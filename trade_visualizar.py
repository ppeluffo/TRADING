#!/home/pablo/Spymovil/python/proyectos/venv_ml/bin/python3

from bokeh.plotting import figure
from bokeh.models import Band, ColumnDataSource
from bokeh.models import HoverTool
from bokeh.models import LinearAxis, Range1d, DataRange1d
#from bokeh.palettes import Category10 as palette
from bokeh.palettes import Dark2_5 as palette
from bokeh.io import output_notebook,show
import itertools
from termcolor import colored as cl
import numpy as np

class Tplot:

    def __init__(self):
        self.df = None
        self.trade_book = None

    def make_plot(self, df, title='GE', columns_to_plot=['Close'] ):
        """
        Genero una figura bokeh
        """
        self.df = df.copy()
        source = ColumnDataSource(self.df)
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
                p.scatter(x='Date', y=serie, source=source, marker='triangle', color='red', size=10, legend_label=serie)
            elif serie == 'Sell_Price':
                 p.scatter(x='Date', y=serie, source=source, marker='inverted_triangle', color='green', size=10, legend_label=serie)
            elif serie == 'Signal':
                p.line(x='Date', y=serie, source=source, color=color, legend_label=serie, y_range_name = "y2")
            else:
                p.line(x='Date', y=serie, source=source, color=color, legend_label=serie)
        #
        p.add_tools(hover)
        p.legend.click_policy="hide"
        p.legend.location = "top_left"
        return p
    
    def plot_trading_book( self, df_tbook, title='GE' ):
        """
        Genero una figura bokeh
        Solo tengo 3 columnas: price, stocks y cash.
        Como stocks y cash alternan con valores 0, las represento con Vbars.
        Uso escalas diferentes para c/u.
        """
        self.trade_book = df_tbook.copy()
        source = ColumnDataSource(self.trade_book)
        hoover_tooltips=[  ("Date", "@Date{%F}") ]
        hoover_formatters={ "@Date": 'datetime'}        
        for name in df_tbook.columns:
            hoover_tooltips.append( (name, f"@{name}"+ '{%0.2f}'))
            hoover_formatters[f"@{name}"] = 'printf'

        hover = HoverTool( 
            tooltips=hoover_tooltips,
            formatters=hoover_formatters
        )
        tools = "pan,wheel_zoom,box_zoom,reset, box_select, lasso_select, crosshair"
        colors = itertools.cycle(palette) 
        p = figure(
            width=900, 
            height=500, 
            x_axis_type='datetime',
            x_axis_label='Fecha',
            y_axis_label='Precios',
            tools=tools, 
            title=f'Trading Book {title}'
            )
        #   
        p.extra_y_ranges = {"y2": Range1d(start = 0, end = np.max(self.trade_book['price']) ) }
        p.add_layout(LinearAxis(y_range_name = "y2"), 'right')
    
        # Grafico las series
        p.vbar(x='Date', top='stocks', source=source, bottom=0, width=0.5, color='orange', legend_label='stocks')
        p.vbar(x='Date', top='cash', source=source, bottom=0, width=0.5, color='blue', legend_label='cash')
        p.line(x='Date', y='price', source=source, color='red', legend_label='price',y_range_name = "y2")
        p.line(x='Date', y='price', source=source, color='red', legend_label='price')
        #
        p.add_tools(hover)
        p.legend.click_policy="hide"
        p.legend.location = "top_left"
        return p
    
    def print_trade_book(self, df, df_tbook, inversion=1000, verbose=False):
  
        self.df = df.copy()
        self.trade_book = df_tbook.copy()

        self.trade_book.reset_index(inplace=True)
        for index,row in self.trade_book.iterrows():
            fecha = row['Date']
            op = row['Op']
            price = row['price']
            stocks = row['stocks']
            cash = row['cash']
            if verbose:
                if op == 'BUY':
                    print(cl('BUY: ',color='green',attrs=['bold']), f'{index}:stocks={stocks},price={price:0.3f},cash={cash:0.3f}')
                elif op == 'SELL':
                    print(cl('SELL: ',color='red',attrs=['bold']), f'{index}:stocks={stocks},price={price:0.3f},cash={cash:0.3f}')
        #
        """
        Si al final tengo acciones, las vendo
        """
        last_price = self.df['Close'].iloc[-1]
        if  self.trade_book.iloc[-1].loc['stocks'] > 0:
            # Vendo todo y paso a cash
            stocks = self.trade_book.iloc[-1].loc['stocks']
            cash = self.trade_book.iloc[-1].loc['cash']
            cash = cash + stocks*last_price
        else:
            cash = self.trade_book.iloc[-1].loc['cash']
        
        print(f'START_CASH={inversion}')
        print(f'LAST_PRICE={last_price}')
        print(f'END_CASH={cash}')
        roi = round( cash / inversion * 100, 2)
        print(cl(f'ROI: {roi}%', attrs = ['bold']))

    