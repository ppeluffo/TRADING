#!/home/pablo/Spymovil/python/proyectos/venv_ml/bin/python3
"""
https://stackoverflow.com/questions/62595156/how-do-i-use-the-sqlalchemy-orm-to-do-an-insert-with-a-subquery-moving-data-fro

TRUNCATE public.tb_empresas RESTART IDENTITY CASCADE

"""
import glob
import os
import pandas as pd
from sqlalchemy import select, desc, bindparam
from sqlalchemy.dialects.postgresql import insert
from base_datos import Bd
import schemas as scm
import yfinance as yf
import pickle as pkl
import time
import logging

class Yfscraper:

    def __init__(self):
        self.bd = Bd()
        self.tables = scm
        self.l_simbolos = []
        
        _= self.bd.connect()
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

    def read_simbolos(self, sort_by=None):
        """
        Lee la tabla de simbolos de la bd.
        Los puede leer ordenados por Nombre, MarketCap, Sector
        """       
        _= self.bd.connect()
        select_stmt = select( self.tables.tb_simbolos.c.simbolo_str ).select_from( 
            self.tables.tb_simbolos.join(self.tables.tb_empresas)
            ).where( self.tables.tb_simbolos.c.simbolo_id == self.tables.tb_empresas.c.simbolo_id )

        # "SELECT (tb_simbolos.simbolo_str) FROM tb_empresas,tb_simbolos WHERE ( tb_simbolos.simbolo_id = tb_empresas.simbolo_id)"
        if sort_by == 'Nombre':
            select_stmt = select_stmt.order_by( desc(self.tables.tb_empresas.c.simbolo_id))
        elif sort_by == 'MarketCap':
            select_stmt = select_stmt.order_by( desc(self.tables.tb_empresas.c.market_cap))
        elif sort_by == 'Sector':
            select_stmt = select_stmt.order_by( desc(self.tables.tb_empresas.c.sector_nasdaq_id))
        #
        #print(select_stmt)
        try:
            rp = self.bd.conn.execute(select_stmt)
        except Exception as ex:
            #print(f'POLULATE_SECTORES_NADAQ EXCEPTION: {ex}')
            logging.error(f'READ SIMBOLOS EXCEPTION: {ex}')
            self.bd.close()
            return None

        self.l_simbolos = []
        for item in rp:
            self.l_simbolos.append(item[0])
        #
        self.bd.close()
        return self.l_simbolos
    
    def yfscrape(self):

        _= self.bd.connect()

        if self.bd.conn is None:
            print ("ERROR: Debe conectase a la BD primero.")
            self.bd.close()
            return
        
        scalar_subq_sectorid = ( 
            select(self.tables.tb_simbolos.c.simbolo_id).where(
                self.tables.tb_simbolos.c.simbolo_str == bindparam("simbolo_str")
                ).scalar_subquery()
            )
        insert_info_stmt = insert(self.tables.tb_info).values( simbolo_id = scalar_subq_sectorid, info = bindparam("pkinfo"))
        insert_history_stmt = insert(self.tables.tb_history).values( 
            simbolo_id = scalar_subq_sectorid, 
            history = bindparam("pkhistory"), 
            last_price_date = bindparam("last_price_date"))

        i = 0
        for ticket in self.l_simbolos:
            print(f"{i},{ticket}")
            i += 1
            tkt = yf.Ticker(ticket)
            try:
                history = tkt.history(period='max', raise_errors=True)
            except Exception as ex:
                logging.error(f'YFSCRAPE {ticket} HISTORY ERROR: {ex}')
                next

            try:    
                info = tkt.info
            except Exception as ex:
                logging.error(f'YFSCRAPE {ticket} INFO ERROR: {ex}')
                next

            last_price_date = history.index[-1]
            pkinfo = pkl.dumps(info)
            pkhistory = pkl.dumps(history)
            #
            # PKINFO
            try:
                self.bd.conn.execute( insert_info_stmt, {'simbolo_str':ticket, 'pkinfo':pkinfo} )
                self.bd.commit()
            except Exception as ex:
                logging.error(f'YFSCRAPE {ticket} INFO EXCEPTION: {ex}')
            
            #HISTORY
            try:
                self.bd.conn.execute( insert_history_stmt, {'simbolo_str':ticket, 'pkhistory':pkhistory, 'last_price_date':last_price_date} )
                self.bd.commit()
            except Exception as ex:
                logging.error(f'YFSCRAPE {ticket} INFO EXCEPTION: {ex}')

            # Espero X secs para el proximo para evitar spoofing
            time.sleep(5)
        #
        self.bd.close()

if __name__ == "__main__":

    yfs = Yfscraper()  
    l_simbolos = yfs.read_simbolos(sort_by='MarketCap')
    yfs.yfscrape()

