#!/home/pablo/Spymovil/python/proyectos/venv_ml/bin/python3
"""
Defino las tablas
"""

from datetime import datetime
from sqlalchemy import Column, Integer, Double, DateTime, LargeBinary, Text
from sqlalchemy import Table, Column, MetaData, ForeignKey

metadata = MetaData()

tb_simbolos = Table('tb_simbolos', metadata,
    Column('simbolo_id', Integer(), primary_key=True),
    Column('simbolo_str', Text() )
    )

tb_sectores_nasdaq = Table('tb_sectores_nasdaq', metadata,
    Column('sector_nasdaq_id', Integer(), primary_key=True),
    Column('sector_nasdaq_str', Text(100), nullable=False, unique=True, index=True)
    )

tb_industrias_nasdaq = Table('tb_industrias_nasdaq', metadata,
    Column('industria_nasdaq_id', Integer(), primary_key=True),
    Column('industria_nasdaq_str', Text(100), nullable=False, unique=True, index=True)
    )

tb_empresas = Table('tb_empresas', metadata,
    Column('empresa_id', Integer(), primary_key=True),
    Column('nombre', Text(100)),
    Column('simbolo_id', Integer(), ForeignKey('tb_simbolos.simbolo_id')),
    Column('sector_id', Integer(), ForeignKey('tb_sectores.sector_id')),
    Column('industria_id', Integer(), ForeignKey('tb_industrias.industria_id')),
    Column('sector_nasdaq_id', Integer(), ForeignKey('tb_sectores_nasdaq.sector_nasdaq_id')),
    Column('industria_nasdaq_id', Integer(), ForeignKey('tb_industrias_nasdaq.industria_nasdaq_id')),
    Column('market_cap', Double())
    )

tb_info = Table('tb_info', metadata,
    Column('info_id',Integer(), primary_key=True),
    Column('simbolo_id',Integer(), ForeignKey('tb_simbolos.simbolo_id')),
    Column('info', LargeBinary()),
    Column('updated_on',DateTime(), default=datetime.now, onupdate=datetime.now )
    )

tb_history = Table('tb_history', metadata,
    Column('history_id',Integer(), primary_key=True),
    Column('simbolo_id',Integer(), ForeignKey('tb_simbolos.simbolo_id')),
    Column('history', LargeBinary()),
    Column('updated_on',DateTime(), default=datetime.now, onupdate=datetime.now ),
    Column('last_price_date',DateTime() ),
    )






