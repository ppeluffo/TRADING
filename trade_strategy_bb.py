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
from Scripts.trade_strategy import Trade_strategy
from tqdm.notebook import tqdm_notebook

class BBstrategy(Trade_strategy):

    def __init__(self):
        Trade_strategy.__init__(self)
    
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
      
    def tuner( self, BB_windows=[10,15,20,25,30,35,40], BB_stds=[1.0,1.5,2.0], split=0.8, verbose=False ):
        """
        Hace el backtest con combinaciones de valores de entrada para encontrar
        el mejor.
        Utiliza el wdf_train con el split de 0.8
        """
        l_windows = []
        l_stds = []
        l_roi = []
        nro_test = len(BB_windows) * len(BB_stds)
        pbar=tqdm_notebook(total=nro_test, desc = 'Iter:', colour='red')
        for window in BB_windows:
            for std in BB_stds:
                pbar.update(1)
                #print(f'tuner params: {window},{std}')
                _ = self.gen_data( window=window, std=std)
                _ = self.gen_trade_signal()
                results = self.backtest_train(split=split,verbose=verbose)

                roi = results.get('ROI',None)
                if roi is None:
                    print("ERROR: backtest_train ROI = None")
                l_windows.append(window)
                l_stds.append(std)
                l_roi.append(roi)
                if verbose:
                    print(f'{window}, {std}, {roi}')
            #
        #
        pbar.close()
        results = pd.DataFrame(data = {'Window':l_windows, 'Std':l_stds, 'Roi':l_roi})
        #if verbose:
        print(f"BB Tuner BEST VALUES:\n{results.iloc[np.argmax(results['Roi'])]}")
        
        return results
           