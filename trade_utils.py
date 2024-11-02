#!/home/pablo/Spymovil/python/proyectos/venv_ml/bin/python3

import pandas as pd
from sqlalchemy import select, bindparam, desc
from base_datos import Bd
import schemas as scm
import pickle
import logging


class Tutils:

    def __init__(self):
        self.bd = Bd()
        self.tables = scm
    
        _= self.bd.connect()
        self.tables.metadata.create_all(self.bd.get_engine())
        self.bd.close()

        self.d_results = None

    def get_d_results(self):
        return self.d_results
    
    def get_ticket_names(self, sort_by=None, limit=None):
        """
        Lee la tabla de nombres de simbolos y devuelve una lista ordenada
        """
        _= self.bd.connect()

        if self.bd.conn is None:
            print ("ERROR: Debe conectase a la BD primero.")
            self.bd.close()
            return
        
        sel  = select( self.tables.tb_simbolos.c.simbolo_str,self.tables.tb_empresas.c.nombre )
        sel = sel.where(self.tables.tb_empresas.c.simbolo_id == self.tables.tb_simbolos.c.simbolo_id)
        sel = sel.order_by(desc(self.tables.tb_empresas.c.market_cap))
        if limit is not None:
            sel = sel.limit(limit)

        try:
            rp = self.bd.conn.execute( sel )
        except Exception as ex:
            print(f'ERROR DATA EXCEPTION: {ex}')
            self.bd.close()
            return None

        data = rp.fetchall()
        self.bd.close()
        return data
        
    def get_data(self, ticket=None):

        _= self.bd.connect()

        if self.bd.conn is None:
            print ("ERROR: Debe conectase a la BD primero.")
            self.bd.close()
            return

        if ticket is None:
            print( "ERROR: Debe proporcionar un nombre de ticket")
            return
        
        scalar_subq_sectorid = ( 
            select(self.tables.tb_simbolos.c.simbolo_id).where(
                self.tables.tb_simbolos.c.simbolo_str == bindparam("ticket")
                ).scalar_subquery()
            )
        # INFO
        sel_info = select( self.tables.tb_info.c.info, self.tables.tb_info.c.updated_on).where( self.tables.tb_info.c.simbolo_id == scalar_subq_sectorid)
        try:
            rp = self.bd.conn.execute( sel_info, {'ticket': ticket} )
        except Exception as ex:
            print(f'ERROR {ticket} INFO EXCEPTION: {ex}')
            self.bd.close()
            return None
        #
        ( pkinfo, info_updated_on ) = rp.first()
        info = pickle.loads(pkinfo)

        # HISTORY
        sel_history = select( self.tables.tb_history.c.history, self.tables.tb_history.c.updated_on).where( self.tables.tb_history.c.simbolo_id == scalar_subq_sectorid)
        try:
            rp = self.bd.conn.execute( sel_history, {'ticket': ticket} )
        except Exception as ex:
            print(f'ERROR {ticket} HISTORY EXCEPTION: {ex}')
            self.bd.close()
            return None
        ( pkhistory, history_updated_on ) = rp.first()
        history = pickle.loads(pkhistory)

        self.bd.close()
        self.d_results = {'info':info, 'info_updated_on': info_updated_on, 'history': history, 'history_updated_on': history_updated_on } 
        return self.d_results
    
    def get_history(self, ticket=None,  start_date=None):
        _ = self.get_data(ticket=ticket)

        if start_date is None:
            d_res = self.d_results['history']
        else:
            d_res = self.d_results['history'][start_date:]
        return d_res

    def split_df(self, df=None, split=1.0):
        '''
        Separo los df en train y test
        '''
        df_train = df[ :int( split * len(df))].copy()
        df_test = df[int( split * len(df)): ].copy()
        return df_train, df_test  
       
if __name__ == "__main__":

    pass   
