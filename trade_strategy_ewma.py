#!/home/pablo/Spymovil/python/proyectos/venv_ml/bin/python3
"""
Implemento los pasos necesarios para la estragegia EWSMA 
Calculo las medias móviles exponenciales
Genero las señales de compra y venta
Creo el optimizador

https://tradingstrategy.ai/docs/_modules/pandas_ta/overlap/ema.html#ema

"""

import sys
sys.path.insert(0,'/home/pablo/Spymovil/python/proyectos/TRADING/Scripts/')
import pandas_ta as ta
import numpy as np
import pandas as pd
import trade_backtest
from trade_utils import split_df

class EWMAstrategy:

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

    def SMA_get_bktst_res(self):
        return self._d_bktst_res
    
    def EWMA_gen_data(self, short_days=20, long_days=100):
        """
        Calcula las medias móviles haciendo windows de short_days y otra de long_days
        y las pone como columnas en el df original
        Se puede calcular con la librería pandas-ta o directamente con el método
        ewm de los dataframes.
        """
        if self.df is None:
            print('ERROR: Debe indicar un df. !!')
            return None
        
        self.wdf = self.df['Close'].copy().to_frame()
        self.wdf['EWMA_SHORT'] = ta.ema(self.wdf['Close'], short_days)
        self.wdf['EWMA_LONG'] = ta.ema(self.wdf['Close'],long_days)
        return self.wdf
     
    def EWMA_gen_trade_signal(self):
        """
        La estrategia de EWMA es la misma que para SMA
        El self.wdf debe tener las señales de EWMA_SHORT y EWMA_LONG.
        """
        signalBuy = []    # Lista con los valores a los que compro ( Close)
        signalSell = []   # Lista con los valores a los que vendo  (Close)
        signalTrade = []  # Lista con marcas cuando compro/vendo   (0,1,np.nan)
        signal =[]        # Lista con los niveles de compra/venta  (0,1)
    
        position = 'COMPRADOR' # Arranco en poscion compradora
    
        if self.wdf is None:
            return None
        
        assert( 'EWMA_SHORT' in self.wdf.columns )
        assert( 'EWMA_LONG' in self.wdf.columns )
        
        for i in range (len(self.wdf)):
            # EWMA corto supera al EWMA largo ?
            if self.wdf['EWMA_SHORT'].iloc[i] > self.wdf['EWMA_LONG'].iloc[i]:
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

            # EWMA corto baja al EWMA largo ?
            elif self.wdf['EWMA_SHORT'].iloc[i] < self.wdf['EWMA_LONG'].iloc[i]:
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
      
    def EWMA_backtest(self, inversion=1000, split=1.0, modo='Train', verbose=True):

        '''
        El backtest consiste invertir una suma inicial de acuerdo a la señal generado
        por la estragegia y ver el resultado al final del período.
        La señal indica cuando comprar o vender y a que precio lo hacemos.
        '''
        # Separo los df en train y test
        self.wdf_train, self.wdf_test = split_df(self.wdf, split)

        tbktst =  trade_backtest.Backtest()
        if modo == 'Train':
            results = tbktst.backtest(self.wdf_train, inversion=inversion, verbose=verbose )
        elif modo == 'Test':
            results = tbktst.backtest(self.wdf_test, inversion=inversion, verbose=verbose )
        else:
            results = None
        return results

    def EWMA_tuner( self, EWMA_short_range=[], EWMA_long_range=[], split=1.0, verbose=False ):
        """
        Hace el backtest con combinaciones de valores de entrada para encontrar
        el mejor
        """
        l_short_days = []
        l_long_days = []
        l_roi = []
        for short_days in EWMA_short_range:
            for long_days in EWMA_long_range:
                _ = self.EWMA_gen_data( short_days=short_days, long_days=long_days)
                _ = self.EWMA_gen_trade_signal()

                results = self.EWMA_backtest(split=split, modo='Train')
                roi = results['ROI']
                #
                l_short_days.append(short_days)
                l_long_days.append(long_days)
                l_roi.append(roi)
                if verbose:
                    print(f'{short_days}, {long_days}, {roi}')
            #
        #
        results = pd.DataFrame(data = {'Short_days':l_short_days, 'Long_days':l_long_days, 'Roi':l_roi})
        if verbose:
            print(f"BEST VALUES: {results.iloc[np.argmax(results['Roi'])]}")
        
        return results