#!/home/pablo/Spymovil/python/proyectos/venv_ml/bin/python3

import math
import sys
sys.path.insert(0,'/home/pablo/Spymovil/python/proyectos/TRADING/Scripts/')
import pandas_ta as ta
import numpy as np
import pandas as pd
from bokeh.plotting import figure
from bokeh.models import Band, ColumnDataSource
from bokeh.models import HoverTool
from bokeh.models import LinearAxis, Range1d, DataRange1d
#from bokeh.palettes import Category10 as palette
from bokeh.palettes import Dark2_5 as palette
import itertools
from termcolor import colored as cl
import trade_utils as tdu

class Trade_strategy:

    def __init__(self):
        '''
        df: contiene los datos base leidos de la BD
        wdf: contiene la columna Close y las de las señales
        '''
        self.tdutils = tdu.Tutils()
        self.ticket = None
        self.df = None
        self.wdf = None
        self.backtest_results = None

    def set_ticket(self, ticket):
        self.ticket = ticket

    def read_data(self):
        '''
        Lee de la BD los datos del ticket dado
        '''
        if self.ticket is None:
            print('ERROR: ticket is None')
            return
        self.df = self.tdutils.get_history(ticket=self.ticket)

    def get_df(self):
        return self.df
    
    def set_wdf(self, wdf):
        self.wdf = wdf

    def get_wdf(self):
        return self.wdf
    
    def get_trade_book(self):
        return self.trade_book
    
    def backtest(self,start=0,end=-1,inversion=1000,verbose=False):
        """
        La columna 'Trade' me indica si compro o vendo.
        La señal de trade es: 0-vendo, 1-compro, np.nan: no hago nada
        En 1 indica que COMPRE ( y pase a modo vendedor)
        En 0 indica que VENDI ( y pase a modo comprador)
        Voy generando un libro de transacciones
        Compro acciones completas (enteras) !!
        La primera transaccion debe ser comprar
        """
        cash = inversion
        stocks = 0
        l_ops = []
        l_price = []
        l_stocks = []
        l_cash = []
        l_dates = []

        if self.wdf is None:
            print('ERROR: debe proveer un df !!')
            return
        
        assert( 'Trade' in self.wdf.columns )
        assert( 'Close' in self.wdf.columns )

        if end == -1:
            end = len(self.wdf)

        starting = True
        for i in range(start, end):
            # Busco la primer transacción que sea comprar.
            if starting is True and self.wdf['Trade'].iloc[i] != 1:
                next
           
            starting = False
            if self.wdf['Trade'].iloc[i] == 1:
                #if verbose:
                #    print('BUY')
                # COMPRO: Paso todo el dinero a acciones (enteras)
                price = self.wdf['Close'].iloc[i]
                stocks = math.floor(cash / price)
                cash = cash - stocks*price
                l_ops.append('BUY')
                l_price.append(price)
                l_stocks.append(stocks)
                l_cash.append(cash)
                l_dates.append(self.wdf.index[i])
                    
            elif self.wdf['Trade'].iloc[i] == 0:
                #if verbose:
                #    print('SELL')
                # VENDO: Paso todas las acciones a dinero
                price = self.wdf['Close'].iloc[i]
                cash = cash + stocks*price
                stocks = 0
                l_ops.append('SELL')
                l_price.append(price)
                l_stocks.append(stocks)
                l_cash.append(cash)
                l_dates.append(self.wdf.index[i])
                #
            #  
        #
        self.backtest_results = {}
        #
        # Armo el libro de transacciones
        trade_book = pd.DataFrame( index=l_dates, data = {'Op':l_ops,'price':l_price,'stocks':l_stocks,'cash':l_cash })
        trade_book.index.name = 'Date'
        self.backtest_results['trade_book'] = trade_book
        #
        #print('Trade Book')
        #print(self.trade_book)
        # Calculo el ROI
        if len(trade_book) > 0:
            last_price = self.wdf['Close'].iloc[end - 1]
            if trade_book.iloc[-1].loc['stocks'] > 0:
                # Vendo todo y paso a cash
                stocks = trade_book.iloc[-1].loc['stocks']
                cash = trade_book.iloc[-1].loc['cash']
                cash = cash + stocks*last_price
            else:
                cash = trade_book.iloc[-1].loc['cash']     
        
            roi = round( (cash / inversion -1 )* 100, 2)
        else:
            roi = 0
            print('Trade book ERROR')

        self.backtest_results['inversion'] = inversion
        self.backtest_results['last_price'] = last_price
        self.backtest_results['cash'] = cash
        self.backtest_results['ROI'] = roi
        return self.backtest_results
    
    def backtest_train(self,split=0.8,inversion=1000,verbose=False):
        # Separo los df en train y test
        if self.wdf is None:
            print('ERROR: wdf is None')
            return
        
        idx_start = 0
        idx_end = int( split * len(self.wdf))
        start_date = self.wdf.index[idx_start].strftime('%Y-%m-%d')
        end_date = self.wdf.index[idx_end].strftime('%Y-%m-%d')
        if verbose:
            print(f'Train: Start date({idx_start})={start_date}, end date({idx_end})={end_date}')
        
        bktstresults = self.backtest(start=idx_start, end=idx_end,inversion=inversion, verbose=verbose)
        return bktstresults

    def backtest_test(self,split=0.8,inversion=1000,verbose=False):
        # Separo los df en train y test
        if self.wdf is None:
            print('ERROR: wdf is None')
            return
        
        idx_start = int( split * len(self.wdf))
        idx_end = len(self.wdf)
        start_date = self.wdf.index[idx_start].strftime('%Y-%m-%d')
        end_date = self.wdf.index[-1].strftime('%Y-%m-%d')
        if verbose:
            print(f'Test: Start date({idx_start})={start_date}, end date({idx_end})={end_date}')
        
        bktstresults = self.backtest(start=idx_start, end=idx_end,inversion=inversion, verbose=verbose)
        return bktstresults

    def plot_wdf(self,start=0,end=-1,title=None,columns_to_plot=['Close'] ):
        """
        Genero una figura bokeh del self.wdf.
        Este debe haber sido modificado por las estrategias en particular agregando 
        al menos las columnas Buy_Price, Sell_Price, Signal.
        """
        if end == -1:
            end = len(self.wdf)
        source = ColumnDataSource(self.wdf[start:end])
        hoover_tooltips=[  ("Date", "@Date{%F}") ]
        hoover_formatters={ "@Date": 'datetime'}        
        for name in columns_to_plot:
            hoover_tooltips.append( (name, f"@{name}"+ '{%0.2f}'))
            hoover_formatters[f"@{name}"] = 'printf'

        hover = HoverTool( 
            tooltips=hoover_tooltips,
            formatters=hoover_formatters
        )
        tools = "pan,wheel_zoom,box_zoom,reset, box_select, lasso_select, crosshair"
        colors = itertools.cycle(palette) 
        if title is None:
            title = self.ticket
        p = figure(
            width=900, 
            height=500, 
            x_axis_type='datetime',
            x_axis_label='Fecha',
            y_axis_label='Precios',
            tools=tools, 
            title=title,
            # Es importante el parametro y_range (tupla) si voy a usar un segundo eje !!!
            #y_range=(0,yMax) 
            )
        #
        # EJE SECUNDARIO PARA LA SEÑAL.
        # Para la segunda grafica, debo explicitar el rango del eje y que va a usar
        # Creo un rango de valores con un nombre para luego referenciarlo.
        # Con esto creo un 'named range' que queda disponible para mapear coordenadas y y crear ejes adicionales.
        p.extra_y_ranges = {"y2": Range1d(start = 0, end = 1.1 ) }
        # Para agregar el nuevo eje, debo usar 'layouts'
        # El método 'add_layout' agrega un objeto en determinado lugar.<br>
        # El objeto 'LineasAxis' es un eje de escala lineal, al que le asignamos el rango de valores a tomar.
        p.add_layout(LinearAxis(y_range_name = "y2"), 'right')
    
        # Grafico las series
        for serie,color  in zip(columns_to_plot,colors):
            if serie == 'Buy_Price':
                p.scatter(x='Date', y=serie, source=source, marker='triangle', color='green', size=10, legend_label=serie)
            elif serie == 'Sell_Price':
                 p.scatter(x='Date', y=serie, source=source, marker='inverted_triangle', color='red', size=10, legend_label=serie)
            elif serie == 'Signal':
                p.line(x='Date', y=serie, source=source, color=color, legend_label=serie, y_range_name = "y2")
            else:
                p.line(x='Date', y=serie, source=source, color=color, legend_label=serie)
        #
        p.add_tools(hover)
        p.legend.click_policy="hide"
        p.legend.location = "top_left"

        return p
    
    def plot_wdf_train(self,split=0.8,title=None,columns_to_plot=['Close'] ):
        idx_start = 0
        idx_end = int( split * len(self.wdf))
        start_date = self.wdf.index[idx_start].strftime('%Y-%m-%d')
        end_date = self.wdf.index[idx_end].strftime('%Y-%m-%d')
        print(f'Plot wdf_train: Start date({idx_start})={start_date}, end date({idx_end})={end_date}')
        
        p = self.plot_wdf(start=idx_start, end=idx_end, title=title, columns_to_plot=columns_to_plot)
        return p       

    def plot_wdf_test(self,split=0.8,title=None,columns_to_plot=['Close'] ):
        
        idx_start = int( split * len(self.wdf))
        idx_end = len(self.wdf)
        start_date = self.wdf.index[idx_start].strftime('%Y-%m-%d')
        end_date = self.wdf.index[-1].strftime('%Y-%m-%d')
        print(f'Plot wdf_test: Start date({idx_start})={start_date}, end date({idx_end})={end_date}')
        
        p = self.plot_wdf(start=idx_start, end=idx_end, title=title, columns_to_plot=columns_to_plot)
        return p 
    
    def plot_trading_book( self, title=None ):

        """
        Genero una figura bokeh
        Solo tengo 3 columnas: price, stocks y cash.
        Como stocks y cash alternan con valores 0, las represento con Vbars.
        Uso escalas diferentes para c/u.
        """
        if self.backtest_results['trade_book'] is None:
            print('ERROR: Trade book is none')
            return 
        source = ColumnDataSource(self.backtest_results['trade_book'])
        hoover_tooltips=[  ("Date", "@Date{%F}") ]
        hoover_formatters={ "@Date": 'datetime'}       
        for name in ['price', 'stocks', 'cash']:
            hoover_tooltips.append( (name, f"@{name}"+ '{%0.2f}'))
            hoover_formatters[f"@{name}"] = 'printf'

        hover = HoverTool( 
            tooltips=hoover_tooltips,
            formatters=hoover_formatters
        )
        if title is None:
            title = self.ticket
        tools = "pan,wheel_zoom,box_zoom,reset, box_select, lasso_select, crosshair"
        colors = itertools.cycle(palette) 
        p = figure(
            width=900, 
            height=500, 
            x_axis_type='datetime',
            x_axis_label='Fecha',
            y_axis_label='Cash',
            tools=tools, 
            title=f'Trading Book {title}'
            )
        #   

        p.extra_y_ranges["y2"] = Range1d(start = 0, end = np.max(self.backtest_results['trade_book']['price']) )
        p.extra_y_ranges["y3"] = Range1d(start = 0, end = np.max(self.backtest_results['trade_book']['stocks']) )
        
        p.add_layout(LinearAxis(y_range_name = "y2"), 'right')
        p.add_layout(LinearAxis(y_range_name = "y3"), 'right')
    
        # Grafico las series
        p.vbar(x='Date', top='stocks', source=source, bottom=0, width=0.5, color='orange', legend_label='stocks', y_range_name = "y3")
        p.vbar(x='Date', top='cash', source=source, bottom=0, width=0.5, color='blue', legend_label='cash')
        p.line(x='Date', y='price', source=source, color='red', legend_label='price',y_range_name = "y2")
        #
        p.add_tools(hover)
        p.legend.click_policy="hide"
        p.legend.location = "top_left"
        return p
    
    def print_trade_book(self, inversion=1000, verbose=False):

        trade_book = self.backtest_results['trade_book']
        trade_book.reset_index(inplace=True)
        for index,row in trade_book.iterrows():
            fecha = row['Date'].strftime('%Y-%m-%d')
            op = row['Op']
            price = row['price']
            stocks = row['stocks']
            cash = row['cash']
            if verbose:
                if op == 'BUY':
                    print(cl('BUY:  ',color='green',attrs=['bold']), f'{index:03d} {fecha} stocks={stocks:04d},price={price:0.3f},cash={cash:0.3f}')
                elif op == 'SELL':
                    print(cl('SELL: ',color='red'  ,attrs=['bold']), f'{index:03d} {fecha} stocks={stocks:04d},price={price:0.3f},cash={cash:0.3f}')
        #        
        print(f"START_CASH={self.backtest_results['inversion']}")
        print(f"LAST_PRICE={self.backtest_results['last_price']}")
        print(f"END_CASH={self.backtest_results['cash']}")
        roi = round( cash / inversion * 100, 2)
        print(cl(f"ROI: {self.backtest_results['ROI']}%", attrs = ['bold']))

    def gen_data(self, window=20, std=2):
        """
        Calcula la banda central y las 2 bandas inferior y superior
        y las pone como columnas en el df original
        El parámetro window debe ser entero y hay veces que en un registro numpy
        si se mezclan enteros y float, numpy pone todo del mismo tipo superior: float.
        """
        if self.df is None:
            print('ERROR: Debe indicar un df. !!')
            return None
        
        std = 1.0 * std
        self.wdf = self.df['Close'].copy().to_frame()

        self.wdf['MAVG'] = self.wdf['Close'].rolling(int(window)).mean()
        self.wdf['STD'] = self.wdf['Close'].rolling(int(window)).std()
        self.wdf['BBupperBand'] = self.wdf['MAVG'] + ( self.wdf['STD'] * std )
        self.wdf['BBlowerBand'] = self.wdf['MAVG'] - ( self.wdf['STD'] * std )
        #
        '''
        Podemos hacerlo con pandas-ta
        BBL_name = f'BBL_{window}_{std}'
        BBU_name = f'BBU_{window}_{std}'
        bb = ta.bbands(self.wdf['Close'], length=window, std=2)
        self.wdf['BBupperBand'] = bb[BBU_name]
        self.wdf['BBlowerBand'] = bb[BBL_name]
        '''

        self.wdf.dropna(axis=0,inplace=True)
        return self.wdf
     
    def gen_trade_signal(self):
        """
        La estrategia de BBolinger es:
        - Si precio supera la BBupper y estoy en modo vendedor :vendo
        - Si el precio baja la BBlower y estoy en modo comprador: compro
         El self.wdf debe tener las señales de BBupperBand y BBlowerBand.
        - La señal de trade es: 0: vendo, 1: compro, np.nan: no hago nada
        """
        signalBuy = []    # Lista con los valores a los que compro ( Close)
        signalSell = []   # Lista con los valores a los que vendo  (Close)
        signalTrade = []  # Lista con marcas cuando compro/vendo   (0,1,np.nan)
        signal =[]        # Lista con los niveles de compra/venta  (0,1)
    
        position = 'COMPRADOR' # Arranco en poscion compradora
    
        if self.wdf is None:
            return None
        
        assert( 'BBupperBand' in self.wdf.columns )
        assert( 'BBlowerBand' in self.wdf.columns )
        
        for i in range (len(self.wdf)):
            # precio supera la BBupper ?
            if self.wdf['Close'].iloc[i] > self.wdf['BBupperBand'].iloc[i]:
                # Estoy en posicion vendedor ( tengo acciones )
                if position == 'VENDEDOR':
                    signalBuy.append(np.nan)
                    signalSell.append(self.wdf['Close'].iloc[i])
                    signalTrade.append(0)
                    position = 'COMPRADOR' # Vendí y paso a posicion comprador
                else:
                    signalBuy.append(np.nan)
                    signalSell.append(np.nan)
                    signalTrade.append(np.nan)

            # precio baja de BBlower ?
            elif self.wdf['Close'].iloc[i] < self.wdf['BBlowerBand'].iloc[i]:
                # Estoy en posicion compradora ( solo tengo cash )
                if position == 'COMPRADOR':
                    signalSell.append(np.nan)
                    signalBuy.append(self.wdf['Close'].iloc[i])
                    signalTrade.append(1)
                    position = 'VENDEDOR' # compro y paso a posicion vendedor
                else:
                    signalBuy.append(np.nan)
                    signalSell.append(np.nan)
                    signalTrade.append(np.nan)
            else:
                signalBuy.append(np.nan)
                signalSell.append(np.nan)
                signalTrade.append(np.nan)
            #

            if position == 'COMPRADOR':
                signal.append(0) # No tengo acciones: solo cash
            elif position == 'VENDEDOR':
                signal.append(1) # Tengo acciones
            else:
                signal.append(-1)
        #
        self.wdf['Buy_Price'] = signalBuy
        self.wdf['Sell_Price'] = signalSell
        self.wdf['Signal'] = signal
        self.wdf['Trade'] = signalTrade
        return self.wdf
      
