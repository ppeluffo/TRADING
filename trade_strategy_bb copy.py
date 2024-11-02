#!/home/pablo/Spymovil/python/proyectos/venv_ml/bin/python3
"""
Implemento los pasos necesarios para la estragegia de BANDAS DE BOLLINGER
Calculo las bandas
Genero las señales de compra y venta
Creo el optimizador
"""

import sys
sys.path.insert(0,'/home/pablo/Spymovil/python/proyectos/TRADING/Scripts/')
import pandas_ta as ta
import numpy as np
import pandas as pd
import trade_backtest
from trade_utils import split_df

class BBstrategy:

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

    def BB_get_bktst_res(self):
        return self._d_bktst_res
    
    def BB_gen_data(self, window=20, std=2):
        """
        Calcula la banda central y las 2 bandas inferior y superior
        y las pone como columnas en el df original
        """
        if self.df is None:
            print('ERROR: Debe indicar un df. !!')
            return None
        
        std = 1.0 * std
        self.wdf = self.df['Close'].copy().to_frame()
        self.wdf['MAVG'] = self.wdf['Close'].rolling(window).mean()
        self.wdf['STD'] = self.wdf['Close'].rolling(window).std()
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
     
    def BB_gen_trade_signal(self):
        """
        La estrategia de BBolinger es:
        - Si precio supera la BBupper y estoy en modo vendedor :vendo
        - Si el precio baja la BBlower y estoy en modo comprador: compro
         El self.wdf debe tener las señales de BBupperBand y BBlowerBand.
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
                # Estoy en posicion vendedor
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
                # Estoy en posicion compradora
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
      
    def BB_backtest(self, inversion=1000, split=1.0, modo='Train', verbose=True):

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

    def BB_tuner( self, BB_windows=[], BB_stds=[], split=1.0, verbose=False ):
        """
        Hace el backtest con combinaciones de valores de entrada para encontrar
        el mejor
        """
        l_windows = []
        l_stds = []
        l_roi = []
        for window in BB_windows:
            for std in BB_stds:
                _ = self.BB_gen_data( window=window, std=std)
                _ = self.BB_gen_trade_signal()

                results = self.BB_backtest(split=split, modo='Train')
                roi = results['ROI']
                #
                l_windows.append(window)
                l_stds.append(std)
                l_roi.append(roi)
                if verbose:
                    print(f'{window}, {std}, {roi}')
            #
        #
        results = pd.DataFrame(data = {'Window':l_windows, 'Std':l_stds, 'Roi':l_roi})
        if verbose:
            print(f"BEST VALUES: {results.iloc[np.argmax(results['Roi'])]}")
        
        return results
           