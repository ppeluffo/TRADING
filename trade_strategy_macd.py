#!/home/pablo/Spymovil/python/proyectos/venv_ml/bin/python3
"""
Implemento los pasos necesarios para la estragegia MACD

https://tradingstrategy.ai/docs/_modules/pandas_ta/overlap/ema.html#ema
https://medium.com/@ethan_johnson03/mastering-the-macd-indicator-b7b6713b2aae

"""

import sys
sys.path.insert(0,'/home/pablo/Spymovil/python/proyectos/TRADING/Scripts/')
import pandas_ta as ta
import numpy as np
import pandas as pd
import trade_backtest
from trade_utils import split_df

class MACDstrategy:

    def __init__(self):
        self.df = None
        self.wdf = None

        self.wdf_train = None
        self.wdf_test = None

    def get_df(self):
        return self.df
    
    def get_wdf(self):
        return self.wdf
    
    def get_df_train(self):
        return self.wdf_train
    
    def get_df_test(self):
        return self.wdf_test
      
    def set_df(self, df):
        self.df = df.copy()

    def MACD_get_bktst_res(self):
        return self._d_bktst_res
    
    def MACD_gen_data(self, fast=12, slow=26, signal=9):
        """
        Calcula la linea MACD, la señal MACD y el histograma
        """
        if self.df is None:
            print('ERROR: Debe indicar un df. !!')
            return None
        
        self.wdf = self.df['Close'].copy().to_frame()
        macd = ta.macd(self.wdf['Close'], fast=fast, slow=slow, signal=signal)
        colnames = macd.columns
        self.wdf['MACDline'] = macd[colnames[0]]
        self.wdf['MACDsignal'] = macd[colnames[2]]
        self.wdf['MACDhist'] = macd[colnames[1]]

        return self.wdf
     
    def MACD_gen_trade_signal(self):
        """
        La estrategia de MACD es:
        - si la linea supera la señal, estoy en tendencia alcista entonces COMPRO
        - si la linea baja de la señal, se viene una reversion de tendencia entonces VENDO
        * En realida hay mas casos que se pueden ver en:
        https://medium.com/@ethan_johnson03/mastering-the-macd-indicator-b7b6713b2aae

        """
        signalBuy = []    # Lista con los valores a los que compro ( Close)
        signalSell = []   # Lista con los valores a los que vendo  (Close)
        signalTrade = []  # Lista con marcas cuando compro/vendo   (0,1,np.nan)
        signal =[]        # Lista con los niveles de compra/venta  (0,1)
    
        position = 'COMPRADOR' # Arranco en poscion compradora
    
        if self.wdf is None:
            return None
        
        assert( 'MACDline' in self.wdf.columns )
        assert( 'MACDsignal' in self.wdf.columns )
        assert( 'MACDhist' in self.wdf.columns )
        
        for i in range (len(self.wdf)):
            # Linea supera a señal ?
            if self.wdf['MACDline'].iloc[i] > self.wdf['MACDsignal'].iloc[i]:
                # Estoy en posicion compradora
                if position == 'COMPRADOR':
                    signalBuy.append(self.wdf['Close'].iloc[i])
                    signalSell.append(np.nan)
                    signalTrade.append(1)
                    position = 'VENDEDOR' # Compre y paso a posicion vendedora
                else:
                    signalBuy.append(np.nan)
                    signalSell.append(np.nan)
                    signalTrade.append(np.nan)

            # Linea cae debajo de la señal ?
            elif self.wdf['MACDline'].iloc[i] < self.wdf['MACDsignal'].iloc[i]:
                # Estoy en posicion vendedora
                if position == 'VENDEDOR':
                    signalBuy.append(np.nan)
                    signalSell.append(self.wdf['Close'].iloc[i])
                    signalTrade.append(0)
                    position = 'COMPRADOR' # Vendo y paso a posicion comprador
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
                signal.append(0) # No tengo acciones
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
      
    def MACD_backtest(self, inversion=1000, split=1.0, modo='Train', verbose=True):

        '''
        El backtest consiste invertir una suma inicial de acuerdo a la señal generado
        por la estragegia y ver el resultado al final del período.
        La señal indica cuando comprar o vender y a que precio lo hacemos.
        '''
        # Separo los df en train y test
        self.wdf_train, self.wdf_test = split_df(self.wdf, split)
        tbktst =  trade_backtest.Backtest()
        if modo == 'Train':
            try:
                results = tbktst.backtest(self.wdf_train, inversion=inversion, verbose=verbose )
            except Exception as e:
                print(f'Exception {e}')
                results = None
        elif modo == 'Test':
            try:
                results = tbktst.backtest(self.wdf_test, inversion=inversion, verbose=verbose )
            except Exception as e:
                print(f'Exception {e}')
                results = None
        else:
            results = None
        return results

    def MACD_tuner( self, MACD_fast_range=[], MACD_slow_range=[],  MACD_signal_range=[], split=1.0, verbose=False ):
        """
        Hace el backtest con combinaciones de valores de entrada para encontrar
        el mejor
        """
        l_fast = []
        l_slow = []
        l_signal = []
        l_roi = []
        for fast in MACD_fast_range:
            for slow in MACD_slow_range:
                if slow >= fast:
                    next
                for signal in MACD_signal_range:  
                    if signal >= slow:
                        next  
                    _ = self.MACD_gen_data( fast=fast, slow=slow, signal=signal)
                    _ = self.MACD_gen_trade_signal()

                    results = self.MACD_backtest(split=split, modo='Train')
                    if results is not None:
                        roi = results['ROI']
                    else:
                        print(f'ERROR: fast={fast},slow={slow},signal={signal}')  
                        roi = 0
                    #
                    l_fast.append(fast)
                    l_slow.append(slow)
                    l_signal.append(signal)
                    if verbose:
                        print(f'{fast}, {slow}, {signal}, {roi}')
                    #
                #
            #
        results = pd.DataFrame(data = {'Fast':l_fast, 'Slow':l_slow, 'Signal':l_signal, 'Roi':l_roi})
        if verbose:
            print(f"BEST VALUES: {results.iloc[np.argmax(results['Roi'])]}")
        
        return results