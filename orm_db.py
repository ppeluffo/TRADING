#!/home/pablo/Spymovil/python/proyectos/venv_ml/bin/python3
"""
https://stackoverflow.com/questions/52075642/how-to-handle-unique-data-in-sqlalchemy-flask-python

"""

import os
from datetime import datetime
from sqlalchemy import create_engine
from sqlalchemy.orm import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy import Column, Integer, String, Float, Double, DateTime, ForeignKey, LargeBinary


PGSQL_HOST = os.environ.get('PGSQL_HOST','127.0.0.1')
PGSQL_PORT = os.environ.get('PGSQL_PORT', '5433')
PGSQL_USER = os.environ.get('PGSQL_USER', 'admin')
PGSQL_PASSWD = os.environ.get('PGSQL_PASSWD','pexco599')
PGSQL_BD = os.environ.get('PGSQL_BD', 'dbpruebas')

BD_URL = f'postgresql+psycopg2://{PGSQL_USER}:{PGSQL_PASSWD}@{PGSQL_HOST}:{PGSQL_PORT}/{PGSQL_BD}'
# BD_URL = 'sqlite:///anep.sqlite'

Base = declarative_base()

class DataAccessLayer:

    def __init__(self):
        self.engine = None
        self.url = BD_URL
        self.Session = None     # Factoria de sesiones asociadas al engine
        self.session = None     # Sesion de trabajo
        self.conn = None

    def connect(self):
        self.engine = create_engine(self.url)
        self.Session = sessionmaker(bind=self.engine)
        self.session = self.Session()
        self.conn = self.engine.connect()
        """
        Cuando me conecto creo todo si no existe
        """
        Base.metadata.create_all(self.engine)

    def close(self):
        pass

class Simbolos(Base):
    __tablename__ = 'tb_simbolos'

    simbolo_id = Column(Integer(), primary_key=True)
    simbolo_str = Column(String(10), nullable=False, unique=True, index=True)

    def __repr__(self):
        return f'Simbolo: {self.simbolo_str}'
    
class Industrias_nasdaq(Base):
    __tablename__ = 'tb_industrias_nasdaq'

    industria_nasdaq_id = Column(Integer(), primary_key=True)
    industria_nasdaq_str = Column(String(100), nullable=False, unique=True, index=True)

    def __repr__(self):
        return f'Industria Nasdaq: {self.industria_nasdaq_str}'
    
class Sectores_nasdaq(Base):
    __tablename__ = 'tb_sectores_nasdaq'

    sector_nasdaq_id = Column(Integer(), primary_key=True)
    sector_nasdaq_str = Column(String(100), nullable=False, unique=True, index=True)

    def __repr__(self):
        return f'Sectors Nasdaq: {self.sector_nasdaq_str}'
    
class Industrias(Base):
    __tablename__ = 'tb_industrias'

    industria_id = Column(Integer(), primary_key=True)
    industria_str = Column(String(100), nullable=False, unique=True, index=True)

    def __repr__(self):
        return f'Industria: {self.industria_str}'

class Sectores(Base):
    __tablename__ = 'tb_sectores'

    sector_id = Column(Integer(), primary_key=True)
    sector_str = Column(String(100), nullable=False, unique=True, index=True)

    def __repr__(self):
        return f'Sector: {self.sector_str}'
    
class Empresas(Base):
    __tablename__ = 'tb_empresas'

    empresa_id = Column(Integer(), primary_key=True)
    nombre = Column(String(100))
    simbolo_id = Column(Integer(), ForeignKey('tb_simbolos.simbolo_id'))
    sector_id = Column(Integer(), ForeignKey('tb_sectores.sector_id'))
    industria_id = Column(Integer(), ForeignKey('tb_industrias.industria_id'))
    sector_nasdaq_id = Column(Integer(), ForeignKey('tb_sectores_nasdaq.sector_nasdaq_id'))
    industria_nasdaq_id = Column(Integer(), ForeignKey('tb_industrias_nasdaq.industria_nasdaq_id'))
    market_cap = Column( Double())

    def __repr__(self):
        return f'Empresa: {self.nombre}'

class Info(Base):
    __tablename__ = 'tb_info'

    info_id = Column(Integer(), primary_key=True)
    simbolo_id = Column(Integer(), ForeignKey('tb_simbolos.simbolo_id'))
    info = Column( LargeBinary())
    updated_on = Column(DateTime(), default=datetime.now, onupdate=datetime.now )

    def __repr__(self):
        return f'Info: {self.info_id}'
        
class History(Base):
    __tablename__ = 'tb_history'

    history_id = Column(Integer(), primary_key=True)
    simbolo_id = Column(Integer(), ForeignKey('tb_simbolos.simbolo_id'))
    history = Column( LargeBinary())
    updated_on = Column(DateTime(), default=datetime.now, onupdate=datetime.now )
    last_price_date = Column(DateTime(), nullable=False)

    def __repr__(self):
        return f'History: {self.history_id}'
    
"""
Este objeto me da el acceso a todo el control de la BD
"""
dal = DataAccessLayer()


if __name__ == '__main__':
    "Creamos las tablas si no existen"
    dal.connect()
    