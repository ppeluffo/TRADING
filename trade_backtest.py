#!/home/pablo/Spymovil/python/proyectos/venv_ml/bin/python3

import math
import numpy as np
import pandas as pd

class Backtest:

    def __init__(self):
        self.df = None
        self.trade_book = None

    def get_trade_book(self):
        return self.trade_book
    
    def get_df(self):
        return self.df
    
    def set_df(self, df):
        self.df = df.copy()

    def backtest(self, df, inversion=1000, verbose=True):
        """
        La columna 'Trade' me indica si compro o vendo.
        En 1 indica que COMPRE ( y pase a modo vendedor)
        En 0 indica que VENDI ( y pase a modo comprador)
        Voy generando un libro de transacciones
        Compro acciones completas (enteras) !!
        El parámetro split permite dividir los datos en training y test.
        """
        self.df = df.copy()

        cash = inversion
        stocks = 0
        l_ops = []
        l_price = []
        l_stocks = []
        l_cash = []
        l_dates = []
        #
        for i in range(len(self.df)):
            if not np.isnan( self.df['Trade'].iloc[i]):
                price = self.df['Close'].iloc[i]
                if self.df['Trade'].iloc[i] == 1:
                    # COMPRE: Pase todo el dinero a acciones (enteras)
                    stocks = math.floor(cash / price)
                    cash = cash - stocks*price
                    l_ops.append('BUY')
                    l_price.append(price)
                    l_stocks.append(stocks)
                    l_cash.append(cash)
                    l_dates.append(df.index[i])
                    
                elif self.df['Trade'].iloc[i] == 0:
                    # VENDI: Pase todas las acciones a dinero
                    cash = cash + stocks*price
                    stocks = 0
                    l_ops.append('SELL')
                    l_price.append(price)
                    l_stocks.append(stocks)
                    l_cash.append(cash)
                    l_dates.append(df.index[i])
                #
            #  
        #
        # Armo el libro de transacciones
        self.trade_book = pd.DataFrame( index=l_dates, data = {'Op':l_ops,'price':l_price,'stocks':l_stocks,'cash':l_cash })
        self.trade_book.index.name = 'Date'
        #
        # Calculo el ROI
        if len(self.trade_book) > 0:
            last_price = self.df['Close'].iloc[-1]
            if  self.trade_book.iloc[-1].loc['stocks'] > 0:
                # Vendo todo y paso a cash
                stocks = self.trade_book.iloc[-1].loc['stocks']
                cash = self.trade_book.iloc[-1].loc['cash']
                cash = cash + stocks*last_price
            else:
                cash = self.trade_book.iloc[-1].loc['cash']     
        
            roi = round( cash / inversion * 100, 2)
        else:
            roi = 0
            print('Trade book ERROR')

        return {'inversion':inversion, 'last_price':last_price, 'cash':cash, 'ROI':roi, 'trade_book': self.trade_book }
    
    