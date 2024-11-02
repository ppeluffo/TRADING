#!/home/pablo/Spymovil/python/proyectos/venv_ml/bin/python3
"""
https://stackoverflow.com/questions/62595156/how-do-i-use-the-sqlalchemy-orm-to-do-an-insert-with-a-subquery-moving-data-fro

TRUNCATE public.tb_empresas RESTART IDENTITY CASCADE

SELECT pg_terminate_backend(pid) FROM pg_stat_activity
WHERE datname = 'dbpruebas'
AND pid <> pg_backend_pid()
AND state in ('idle');


https://stackoverflow.com/questions/33307250/postgresql-on-conflict-in-sqlalchemy
https://www.programcreek.com/python/example/105995/sqlalchemy.dialects.postgresql.insert
https://docs.sqlalchemy.org/en/20/dialects/postgresql.html#postgresql-insert-on-conflict

Logs:
https://blog.sentry.io/logging-in-python-a-developers-guide/

Implemento la clase Screener
Esta lee un archivo nasdaq_screener, lo abre y filtra las industrias, sectores, tickets y nombres de empresa en listas.
Luego recorre c/u y almacena los datos en la BD.

En el insert, si quiero usar el on_conflict debo importarlo desde los dialectos de postgres
y usarlo no como método de la tabla sino la tabla como parámetro de la clase !!!.

"""
import glob
import logging.handlers
import os
import pandas as pd
import datetime
from sqlalchemy import select, bindparam
from sqlalchemy.dialects.postgresql import insert
from base_datos import Bd
import schemas as scm
import logging

PATH='/home/pablo/Descargas/'
file='nasdaq_screener_1723721184048.csv'

class Screener:
    """
    Esta clase se encarga de leer un archivo de screener de Nasdaq en un dataframe
    y procesarlo generando listas de: industrias, sectores, tickets.
    Filtra los tickets para quedarnos con solo aquellos que no tienen caracteres extraños.
    Rellena la tabla de sectores_nasdaq, industrias_nasdaq, simbolos, empresas.
    
    """
    def __init__(self):
        self.df = None
        self.l_sectores = []
        self.l_industrias = []
        self.l_simbolos = []
        self.bd = Bd()
        self.tables = scm

        self.bd.connect()
        self.tables.metadata.create_all(self.bd.get_engine())
        self.bd.close()
        self.logging_start()

    def logging_start(self):
        # Fijo el logsize en 10M
        maxBytes = 10000000 
        # Guardo hasta 10 archivos de log
        backupCount = 10
        logging.basicConfig(level=logging.INFO, filename="trading_log.log",filemode="w",
                            format="%(asctime)s %(levelname)s %(message)s")
        logging.handlers.RotatingFileHandler( filename="trading_log.log", maxBytes=maxBytes, backupCount=backupCount)
        logging.info("Starting...")

    def get_conn(self):
        return self.bd.conn
    
    def get_df(self):
        return self.df
    
    def get_sectores(self):
        return self.l_sectores
    
    def get_industrias(self):
        return self.l_industrias
    
    def get_simbolos(self):
        return self.l_simbolos
    
    def read(self, filename=None):
        "Leo el archivo en un df"

        # Resulevo el filename
        if filename is None:
            fn_pattern = os.path.join( PATH, f'nasdaq_screener_*.csv' )
            # Busco todos los archivos con el pattern
            fn_list = glob.glob(fn_pattern)
            # Ordeno la lista 
            fn_list.sort(key=lambda x: os.path.getmtime(x), reverse=True)
            # Me quedo con el mas reciente
            filename = fn_list[0]
        else:
            filename = os.path.join(PATH,file)
        #
        # Leo el csv en un dataframe
        self.df = pd.read_csv(filename)
        return self.df
    
    def process(self):
        """
        Genera una lista de sectores, otra de industrias y otra de simbolos
        Return: dict { l_industrias, l_sectores, l_simbolos }
        """

        if self.df is not None:
            self.l_sectores = [ s for s in self.df['Sector'].unique() if isinstance(s,str) ]
            self.l_sectores.sort()
            self.l_industrias = [ s for s in self.df['Industry'].unique() if isinstance(s,str) ]
            self.l_industrias.sort()
            self.l_simbolos = [ s for s in self.df['Symbol'].unique() if isinstance(s,str) ]
            self.l_simbolos.sort()
        return {'industries': self.l_industrias, 'sectores': self.l_sectores, 'simbolos': self.l_simbolos }
  
    def filter_simbols(self):
        """
        Filtra todos los simbolos que tienen caracteres no letras o numeros
        """
        filtered_simbols = [ s for s in self.l_simbolos  if (isinstance(s, str ) and s.isalnum()) ]
        return filtered_simbols
    
    def polulate_sectores_nasdaq(self, verbose=False):
        """
        Inserta los nombres de sectores en la tabla correspondinte.
        """
        self.bd.connect()
        if self.bd.conn is None:
            print ("ERROR: Debe conectase a la BD primero.")
            self.bd.close()
            return
        
        #insert_stmt = self.tables.tb_sectores_nasdaq.insert().values(sector_nasdaq_str = bindparam("sector"))
        insert_stmt = insert(self.tables.tb_sectores_nasdaq).values(sector_nasdaq_str = bindparam("sector"))
        do_nothing_stmt = insert_stmt.on_conflict_do_nothing( index_elements=['sector_nasdaq_id'] )
        for i, sector in enumerate(self.l_sectores):    
            if verbose:
                print(f'{i},{sector}')

            try:
                self.bd.conn.execute(do_nothing_stmt,{'sector':sector})
            except Exception as ex:
                #print(f'POLULATE_SECTORES_NADAQ EXCEPTION: {ex}')
                logging.error(f'POLULATE_SECTORES_NADAQ EXCEPTION: {ex}')

            if ((i + 1) % 10 == 0 ):
                self.bd.commit()
        #
        self.bd.commit()   
        self.bd.close()
        #
            
    def polulate_industrias_nasdaq(self, verbose=False):
        """"""
        self.bd.connect()
        if self.bd.conn is None:
            print ("ERROR: Debe conectase a la BD primero.")
            self.bd.close()
            return

        #ins = self.tables.tb_industrias_nasdaq.insert().values(industria_nasdaq_str = bindparam("industria"))
        insert_stmt = insert(self.tables.tb_industrias_nasdaq).values(industria_nasdaq_str = bindparam("industria"))
        do_nothing_stmt = insert_stmt.on_conflict_do_nothing( index_elements=['industria_nasdaq_id'] )
        for i, industria in enumerate(self.l_industrias):
            if verbose:
                print(f'{i},{industria}')

            try:
                self.bd.conn.execute(do_nothing_stmt,{'industria':industria})
            except Exception as ex:
                #print(f'POLULATE_INDUSTRIAS_NADAQ EXCEPTION: {ex}')
                logging.error(f'POLULATE_INDUSTRIAS_NADAQ EXCEPTION: {ex}')

            if ((i + 1) % 10 == 0 ):
                self.bd.commit()
        #
        self.bd.commit()
        self.bd.close()
        #

    def polulate_simbolos(self, verbose=False):
        """"""
        self.bd.connect()
        if self.bd.conn is None:
            print ("ERROR: Debe conectase a la BD primero.")
            self.bd.close()
            return

        #ins = self.tables.tb_simbolos.insert().values(simbolo_str = bindparam("simbolo"))
        insert_stmt = insert(self.tables.tb_simbolos).values(simbolo_str = bindparam("simbolo"))
        do_nothing_stmt = insert_stmt.on_conflict_do_nothing( index_elements=['simbolo_id'] )
        for i,simbolo in enumerate(self.l_simbolos):
            if verbose:
                print(f'{i},{simbolo}')
            try:
                self.bd.conn.execute(do_nothing_stmt,{'simbolo':simbolo})
            except Exception as ex:
                #print(f'POLULATE_SIMBOLOS EXCEPTION: {ex}')
                logging.error(f'POLULATE_SIMBOLOS EXCEPTION: {ex}')

            if ((i + 1) % 10 == 0 ):
                self.bd.commit()
        #
        self.bd.commit()
        self.bd.close()

    def polulate_empresas(self, verbose=False):
        """
        Primero filtro el df para solo quedarme con los simbolos válidos.
        Luego hago un itertuples y en c/u me quedo con nombre, sector, industria, mcap.
        Genero el insert.
        Utilizo subqueries
        """
        self.bd.connect()
        l_valid_simbols = self.filter_simbols()
        filtered_df = self.df[self.df['Symbol'].isin(l_valid_simbols)]
        # La columna 'Market Cap' tiene spaces por lo que no funciona con itertuples !!
        filtered_df.rename(columns={'Market Cap':'mcap'}, inplace=True)
        
        # Creo el template de la consulta
        scalar_subq_simbolo = ( 
            select(self.tables.tb_simbolos.c.simbolo_id).where(
                self.tables.tb_simbolos.c.simbolo_str == bindparam("simbolo")
                ).scalar_subquery()
            )
        scalar_subq_sector = ( 
            select(self.tables.tb_sectores_nasdaq.c.sector_nasdaq_id).where(
                self.tables.tb_sectores_nasdaq.c.sector_nasdaq_str == bindparam("sector")
                ).scalar_subquery()
            )
        scalar_subq_industria = ( 
            select(self.tables.tb_industrias_nasdaq.c.industria_nasdaq_id).where(
                self.tables.tb_industrias_nasdaq.c.industria_nasdaq_str == bindparam("industria")
                ).scalar_subquery()
            )
        #
        insert_stmt = insert(self.tables.tb_empresas).values( 
            nombre = bindparam("name"),
            simbolo_id = scalar_subq_simbolo, 
            sector_nasdaq_id = scalar_subq_sector, 
            industria_nasdaq_id = scalar_subq_industria, 
            market_cap = bindparam("mcap")
        )
        do_nothing_stmt = insert_stmt.on_conflict_do_nothing( index_elements=['empresa_id'] )
        
        i = 1
        for row in filtered_df.itertuples():
            i += 1
            simbolo = row.Symbol
            nombre_empresa = row.Name
            nombre_empresa = nombre_empresa.replace("'","")
            nombre_empresa = nombre_empresa.replace('"',"")
            nombre_empresa = nombre_empresa[:30]
            sector = row.Sector
            industry = row.Industry
            mcap = row.mcap
            #
            if verbose:
                print(f'{i},{simbolo},{nombre_empresa}')
            try:
                self.bd.conn.execute(do_nothing_stmt,{'simbolo':simbolo, 'sector':sector, 'industria': industry, 'name':nombre_empresa,'mcap':mcap })
            except Exception as ex:
                #print(f'POLULATE_EMPRESAS EXCEPTION: {ex}')
                logging.error(f'POLULATE_EMPRESAS EXCEPTION: {ex}')

            if ((i + 1) % 10 == 0 ):
                self.bd.commit()
        #
        self.bd.commit()
        self.bd.close()

if __name__ == "__main__":

    scr = Screener()

    _ = scr.read()
    _ = scr.process()

    # Polulamos las tablas.
    scr.polulate_sectores_nasdaq()
    scr.polulate_industrias_nasdaq()
    scr.polulate_simbolos()
    scr.polulate_empresas()


    