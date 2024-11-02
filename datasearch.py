#!/home/pablo/Spymovil/python/proyectos/venv_ml/bin/python3
"""
Implementacion de la clase que se encarga de buscar informacion en la web
de los tickets y almacenarla localmente.
Utilizamos yfinance para esto.
De cada ticket traemos 2 tipos de informacion: info() que es un diccionario
con los datos particulares de la empresa y values() que son los precios ( pd.Dataframe)
Ambos los guardamos con pickle en un repositorio local.
"""

import glob
import os
import random
import time
import pandas as pd
import yfinance as yf
import pickle
import datetime as dt

PATH_DATASTORE = '/home/pablo/Spymovil/python/proyectos/TRADING/datastore'

class DataSearch:

    def __init__(self):
        self.version = '1.0.0'
        self.ticket = None
        self.d_info = None
        self.df_values = None
        self.symbols = None

    def get_version(self):
        return self.version
    
    def get_ticket(self):
        return self.ticket
    
    def set_ticket(self, ticket=None):
        self.ticket = ticket

    def download(self, ticket=None, verbose=False):
        '''
        Baja con yfinance los datos del ticket y los guarda en el datastore
        en formato pickle.
        También los retorna
        Return: tuple->dict,pdDataframe (info, values)
        '''
        if ticket is None and self.ticket is None:
            print('ERROR dwnl001: Debe indicarse un ticket !!')
            return None
        
        if ticket is not None:
            self.ticket = ticket

        data = yf.Ticker(self.ticket)

        # Download valores.
        if verbose:
            print(f'Downloading {self.ticket}')

        try:
            self.df_values = data.history(period='max', raise_errors=True)
        except Exception as e:
            print(f'ERROR dwnl002: {self.ticket} {e}')
            return None
        # Download info
        self.d_info = data.info

        return self.d_info, self.df_values

    def save(self):

        if self.ticket is None:
            print('ERROR save001: Debe indicarse un ticket !!')
            return False
        
        now = dt.datetime.now().strftime('%Y%m%d%H%M')
        filename = os.path.join( PATH_DATASTORE, f'{self.ticket}_{now}_info.pkf' )
        #print(f'FILENAME={filename}')

        try:
            fh = open(filename, 'wb')
        except Exception as e:
            print(f'ERROR save002: abriendo archivo {filename} !! [{e}]')
            return False
           
        pickle.dump(self.d_info, fh)
        fh.close()

        filename = os.path.join( PATH_DATASTORE, f'{self.ticket}_{now}_values.pkf' )
        try:
            fh = open(filename, 'wb')
        except Exception as e:
            print(f'ERROR save003: abriendo archivo {filename} !! [{e}]')
            return False
        
        pickle.dump(self.df_values, fh)
        fh.close()
        return True

    def load(self, ticket=None):
        """
        Pueden haber varios archivos similares que difieren solo en la fecha
        Debemos ordenarlos y quedarnos con el mas reciente.
        """
        if ticket is None and self.ticket is None:
            print('ERROR load001: Debe indicarse un ticket !!')
            return None
        
        if ticket is not None:
            self.ticket = ticket

        # Archivo INFO
        fn_pattern = os.path.join( PATH_DATASTORE, f'{self.ticket}_*_info.pkf' )
        fn_list = glob.glob(fn_pattern)
        fn_list.sort(key=lambda x: os.path.getmtime(x), reverse=True)
        filename = fn_list[0]
        if not os.path.isfile(filename):
            print(f'ERROR load002: archivo {filename} no existe !!')
            return
        
        try:
            fh = open(filename, 'rb')
        except Exception as e:
            print(f'ERROR load003: abriendo archivo {filename} !! [{e}]')
            return 
        self.d_info = pickle.load(fh)
        fh.close()

        # Archivo VALUES
        fn_pattern = os.path.join( PATH_DATASTORE, f'{self.ticket}_*_values.pkf' )
        fn_list = glob.glob(fn_pattern)
        fn_list.sort(key=lambda x: os.path.getmtime(x), reverse=True)
        filename = fn_list[0]
        if not os.path.isfile(filename):
            print(f'ERROR load004: archivo {filename} no existe !!')
            return
        
        try:
            fh = open(filename, 'rb')
        except Exception as e:
            print(f'ERROR load005: abriendo archivo {filename} !! [{e}]')
            return 
        
        self.df_values = pickle.load(fh)
        fh.close()

        return self.d_info, self.df_values
    
    def read_tickets_names(self, filename=None, filter=None, order_by_mcap=None, max_tickets=0):
        '''
        Busca en el datastore los archivos con nombre nasdaq_screener_.... y se
        queda con el más nuevo.
        Lo abre en un dataframe y se queda con la lista de Symbol
        '''
        if filename is None:
            fn_pattern = os.path.join( PATH_DATASTORE, f'nasdaq_screener_*.csv' )
            # Busco todos los archivos con el pattern
            fn_list = glob.glob(fn_pattern)
            # Ordeno la lista 
            fn_list.sort(key=lambda x: os.path.getmtime(x), reverse=True)
            # Me quedo con el mas reciente
            filename = fn_list[0]
        
        df = pd.read_csv(filename)
        if order_by_mcap == 'descending':
            df.sort_values(['Market Cap'], ascending=False, inplace=True)
        elif order_by_mcap == 'ascending':
            df.sort_values(['Market Cap'], ascending=True, inplace=True)

        self.symbols = df['Symbol'].to_list()
        
        if max_tickets > 0 and max_tickets < len(self.symbols):
            self.symbols = self.symbols[:max_tickets]
        return self.symbols
    
    def scrape(self, symbols=None, max_requests_per_minute=6, random_scrape=False, max_tickets=-1, verbose=True ):
        '''
        Lee la lista de tickets y la recorre recuperando datos de c/u.
        Los tickets que dan error se guardan en una lista.
        '''

        if symbols is None and self.symbols is None:
            print('ERROR scrape001: Debe indicarse una lista de tickets !!')
            return None
        
        if symbols is not None:
            self.symbols = symbols

        # Obtengo la lista de tickets a recorrer
        if random_scrape:
            random.shuffle(self.symbols)

        nro_tickets = 0
        for ticket in self.symbols:
            if verbose:
                now = dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                print(f'{now} Ticket ({nro_tickets}):{ticket}')
            if self.download(ticket=ticket) is None:
                print(f'  {ticket} download ERROR !!')
            else:
                _ = self.save()
                nro_tickets += 1
            #
            if nro_tickets == max_tickets:
                break
            # Genero una espera aleatoria para el siguiente de modo de cumplir los requestxmin
            sleep_time = random.randint(3,10)
            if verbose:
                print(f'Sleeping({sleep_time})...')
            time.sleep(sleep_time)



        

                  


