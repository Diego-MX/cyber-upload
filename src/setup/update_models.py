# Diego Villamil, EPIC
# CDMX, 8 de abril de 2022

import pandas as pd

from src.utilities import tools
from src.lake_connector import LakehouseConnect
from config import (ConfigEnviron, SITE, ENV, SERVER)


xls_def  = SITE/"refs/catalogs/CX - Strategy & Communications.xlsx.lnk"
ref_path = SITE/"refs/catalogs"

# Meta (meta) descriptions are confusing. 
# MSGS are queried from a table. 
# MSGS ATTRS are there corresponding columns. 
# This table lists thems as rows, and therefore their columns, call them metas.  


metas_0 = ['nombre', 'gold_name', 'dtipo', 'filtro', 
    'comunicaciones', 'comparador', 'database', 'silver_name']

metas_1 = ['nombre', 'filtro', 'comunicaciones', 'database', 'dtipo', 
    'comparador', '_middlename']

msg_attrs_0 = (tools.read_excel_table(xls_def, 'columnas', 'mensajes_cols')
    .loc[:, metas_0])

tables_props = { # call:  (meta_database, table_name, _middlename)
    'contracts':   ('database',  'bronze.loan_contracts'), 
    'collections': ('gold_name', 'gold.loan_contracts', 'silver_name')}



# Base de tablas.
atributos_0 = ["nombre", "gold_name", "gold_aux", "dtipo", "filtro", 
    "comunicaciones", "comparador", "database", "en_gold", "en_gold2"]

msg_cols_0  = (tools.read_excel_table(xls_def, "columnas", "mensajes_cols")
    .loc[:, atributos_0]
    .astype({'gold_aux': str}))

msg_cols_0.to_feather(ref_path/"collectionsCX.feather")



#%% DBase Preparation.  

secretter = ConfigEnviron(ENV, SERVER)
lakehouser = LakehouseConnect(secretter)

db_conn = lakehouser.get_connection()

describe_qry = 'DESCRIBE TABLE {0};'


#%% Loan Contracts SAP
key = 'contracts'
(dbase_meta, table_name) = tables_props['contracts']

# No es igual de automático para KEY1 y KEY2.  
# Pero sólo se usan los parametros de TABLES_PROPS. 

from_dbks = pd.read_sql(describe_qry.format(table_name), db_conn)
attrs_in_dbks = msg_attrs_0[dbase_meta].isin(from_dbks['col_name'])
attrs_to_call = (msg_attrs_0
    .assign(_middlename = lambda df: df[dbase_meta])
    .loc[attrs_in_dbks, metas_1]
    .reset_index(drop=True))

# Upload Somewhere. 
attrs_to_call.to_feather(ref_path/"contracts.feather")
attrs_to_call.to_csv(ref_path/"contracts.csv", index=False)


#%% CallType Collections

key = 'collections'
(dbase_meta, table_name, mid_meta) = tables_props['collections']

# No es igual de automático para KEY1 y KEY2.  
# Pero sólo se usan los parametros de TABLES_PROPS. 

from_dbks = pd.read_sql(describe_qry.format(table_name), db_conn)

attrs_to_call = (msg_attrs_0
    .assign(_middlename = lambda df: df[dbase_meta]
        .where(df[dbase_meta].isin(from_dbks['col_name']), other=df[mid_meta]))
    .drop(columns=['database'])
    .rename(columns={dbase_meta: 'database'})
    .loc[lambda df: df['_middlename'].isin(from_dbks['col_name']), metas_1]
    .reset_index(drop=True))

attrs_to_call.to_feather(ref_path/"collections.feather")
attrs_to_call.to_csv(ref_path/"collections.csv", index=False)

db_conn.close()


