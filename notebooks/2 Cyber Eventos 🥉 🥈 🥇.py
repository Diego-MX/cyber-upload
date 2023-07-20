# Databricks notebook source
# MAGIC %md 
# MAGIC
# MAGIC # Preparación
# MAGIC
# MAGIC * Las modificaciones `silver` se hacen en las tablas base, y se verifican los tipos de columnas desde el lado de la fuente. 
# MAGIC * La preparación `gold` consiste en unir las `silver`, y se utilizan los tipos de columnas especificados para crear el _output_.

# COMMAND ----------

# MAGIC %pip install -q -r ../reqs_dbks.txt

# COMMAND ----------

import subprocess
import yaml

with open("../user_databricks.yml", 'r') as _f: 
    u_dbks = yaml.safe_load(_f)

epicpy = {
    'url'   : 'github.com/Bineo2/data-python-tools.git', 
    'branch': 'dev-diego', 
    'token' : dbutils.secrets.get(u_dbks['dbks_scope'], u_dbks['dbks_token'])}

url_call = "git+https://{token}@{url}@{branch}".format(**epicpy)
subprocess.check_call(['pip', 'install', url_call])

# COMMAND ----------

from collections import OrderedDict
from datetime import date
from json import dumps
import pandas as pd
from pandas import DataFrame as pd_DF
from pathlib import Path
from pyspark.sql import (functions as F, SparkSession)
import re

# COMMAND ----------

from epic_py.delta import EpicDF, EpicDataBuilder

from src.data_managers import CyberData
from src.platform_resources import AzureResourcer
from src.utilities import tools


from config import (app_agent, app_resources,
    DBKS_TABLES as tables, cyber_handler, 
    ConfigEnviron, ENV, SERVER, DBKS_TABLES)


tables      = DBKS_TABLES[ENV]['items']
app_environ = ConfigEnviron(ENV, SERVER, spark)
az_manager  = AzureResourcer(app_environ)

at_storage = az_manager.get_storage()
az_manager.set_dbks_permissions(at_storage)

brz_path   = f"abfss://bronze@{at_storage}.dfs.core.windows.net/ops/core-banking/" 
gold_path  = f"abfss://gold@{at_storage}.dfs.core.windows.net/cx/collections/cyber"
specs_path = "cx/collections/cyber/spec_files"
tmp_downer = "/dbfs/FileStore/cyber/specs"

cyber_central = CyberData(spark)
cyber_builder = EpicDataBuilder(typehandler=cyber_handler)

def dumps2(an_obj, **kwargs): 
    dump1 = dumps(an_obj, **kwargs)
    dump2 = re.sub(r'(,)\n *', r'\1 ', dump1)
    return dump2

# COMMAND ----------

# MAGIC %md 
# MAGIC # Modificaciones Silver 🥈

# COMMAND ----------

# MAGIC %md
# MAGIC Solo 4 requieren modificación  
# MAGIC
# MAGIC * `Loans Contract`:  se filtran los prestamos, modifican fechas, y agregan algunas columnas auxiliares.  
# MAGIC * `Person Set`: tiene algunas modificaciones personalizadas.
# MAGIC * `Balances`, `Open Items`: sí se tienen que abordar a fondo.
# MAGIC * `Transaction Set`:  tiene agrupado por contrato, y separado por fechas. 

# COMMAND ----------

# MAGIC %md
# MAGIC ## Mandar llamar todos. 

# COMMAND ----------

balances = EpicDF(spark, f"{brz_path}/loan-contract/aux/balances-wide")


# COMMAND ----------

# Revisar especificación en ~/refs/catalogs/cyber_txns.xlsx
# O en User Story, o en Correos enviados.  

balances = cyber_central.prepare_source('balances', 
    path=f"{brz_path}/loan-contract/aux/balances-wide")
    
open_items_long = cyber_central.prepare_source('open-items-long', 
    path=f"{brz_path}/loan-contract/chains/open-items")

open_items_wide = cyber_central.prepare_source('open-items-wide', 
    path=f"{brz_path}/loan-contract/aux/open-items-wide")

loan_contracts = cyber_central.prepare_source('loan-contracts', 
    path=f"{brz_path}/loan-contract/data", 
    open_items = open_items_wide)

persons = cyber_central.prepare_source('person-set', 
    path=f"{brz_path}/person-set/chains/person")

the_txns = cyber_central.prepare_source('txns-set', 
    path=f"{brz_path}/transaction-set/data")

txn_pmts = cyber_central.prepare_source('txns-grp', 
    path=f"{brz_path}/transaction-set/data")

# COMMAND ----------

# MAGIC %md 
# MAGIC # Preparación Gold 🥇 

# COMMAND ----------

# Las llaves de las tablas se deben mantener, ya que se toman de las especificaciones del usuario. 

tables_dict = {
    "BalancesWide" : balances,
    "ContractSet"  : loan_contracts, 
    "OpenItems"    : open_items_wide, 
    "OpenItemsLong": open_items_long,
    "PersonSet"    : persons, 
    "TxnsGrouped"  : txn_pmts, 
    "TxnsPayments" : the_txns}


print("The COUNT stat in each table is:")
for kk, vv in tables_dict.items(): 
    print(kk, vv.count())
    

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tabla de instrucciones
# MAGIC
# MAGIC Leemos los archivos `specs` y `joins` que se compilaron a partir de las definiciones en Excel.  
# MAGIC Y de ahí, se preparan los archivos. 
# MAGIC

# COMMAND ----------

def read_cyber_specs(task_key: str): 
    # Usa TMP_DOWNER, SPECS_PATH, 
    specs_file = f"{tmp_downer}/{task_key}.feather"
    joins_file = f"{tmp_downer}/{task_key}_joins.csv"
    specs_blob = f"{specs_path}/{task_key}_specs_latest.feather"
    joins_blob = f"{specs_path}/{task_key}_joins_latest.csv"
    
    az_manager.download_storage_blob(specs_file, specs_blob, 'gold', verbose=1)
    az_manager.download_storage_blob(joins_file, joins_blob, 'gold', verbose=1)
    
    specs_df = cyber_central.specs_setup_0(specs_file)
    
    if Path(joins_file).is_file(): 
        join_df = (pd.read_csv(joins_file)
            .set_index('tabla'))
        
        joins_dict = OrderedDict()
        for tabla, rr in join_df.iterrows():
            if not tools.is_na(rr['join_cols']):
                # OldCol1=new_col_1,OldCol2=new_col_2,...
                joiners_0 = rr['join_cols'].split(',')
                joiners_1 = (jj.split('=') for jj in joiners_0)
                joiners_2 = [F.col(j0).alias(j1) for j0, j1 in joiners_1]
                joins_dict[tabla] = joiners_2
    else: 
        joins_dict = None
        
    return specs_df, joins_dict

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Master Join and Fixed-Value Columns  
# MAGIC
# MAGIC 1. Definir tipos de _Spark_, y los valores nulos para cada uno de ellos.  
# MAGIC 2. Crear columnas para los valores fijos definidos.  
# MAGIC 3. Convertir a los tipos definidos en (1).  
# MAGIC 4. Las fechas se manejan por separado.  
# MAGIC
# MAGIC ### Explicit conversion to string
# MAGIC Aplicamos las definiciones anteriores de acuerdo al tipo de columna `str`, `date`, `dbl`, `int`.  
# MAGIC - `STR`: Aplicar formato `c_format` y dejar ASCII.   
# MAGIC - `DATE`: Convertir los `1900-01-01` capturados previamente y aplicar `date_format`.  
# MAGIC - `DBL`: Aplicar `c_format` y quitar decimal.  
# MAGIC - `INT`: Aplicar `c_format`.  
# MAGIC
# MAGIC Post-formatos, aplicar el `s-format`, concatenar.  

# COMMAND ----------

# MAGIC %md
# MAGIC ## Execution

# COMMAND ----------

# MAGIC %md 
# MAGIC ### SAP Pagos

# COMMAND ----------

missing_cols = {}
the_tables = {}

# COMMAND ----------

task = 'sap_pagos'

specs_df, spec_joins = read_cyber_specs(task)

specs_df_2 = (specs_df
    .reset_index()
    .rename(columns={
        'Longitud': 'len', 'Posición inicial': 'pos', 
        'nombre': 'name', 'PyType': 'pytype'}))

specs_dict = cyber_central.specs_reader_1(specs_df, tables_dict)
# Tiene: [readers, missing, fix_vals]

missing_cols[task] = specs_dict['missing']

gold_3 = cyber_central.master_join_2(spec_joins, specs_dict, tables_dict)

fxd_selector = cyber_builder.get_loader(specs_df_2, 'fixed-width')

gold_2 = (gold_3
    .with_column_plus(fxd_selector))

gold_2.display()

one_select = (F.concat(*specs_df['nombre'])
    .alias('~'.join(specs_df['nombre'])))

gold_pagos = gold_2.select(one_select)
the_tables[task] = (gold_path, gold_pagos)

cyber_central.save_task_3(task, gold_path, gold_pagos)
print(f"\tRows: {gold_pagos.count()}")


# COMMAND ----------

# MAGIC %md 
# MAGIC ### SAP Estatus
# MAGIC
# MAGIC

# COMMAND ----------

task = 'sap_estatus'

specs_df, spec_joins = read_cyber_specs(task)

specs_df_2 = (specs_df
    .reset_index()
    .rename(columns={
        'Longitud': 'len', 'Posición inicial': 'pos', 
        'nombre'  : 'name','PyType': 'pytype'}))

specs_dict = cyber_central.specs_reader_1(specs_df, tables_dict)
# Tiene: [readers, missing, fix_vals]

missing_cols[task] = specs_dict['missing']

gold_3 = cyber_central.master_join_2(spec_joins, specs_dict, tables_dict)

# fxd_widther = cyber_central.fxw_converter(specs_df)
#     [0-fill, 1-cast, 2-select]
fxd_selector = cyber_builder.get_loader(specs_df_2, 'fixed-width')

gold_2 = (gold_3
    .with_column_plus(fxd_selector))

one_select = F.concat(*specs_df['nombre']).alias('~'.join(specs_df['nombre']))

gold_estatus = gold_2.select(one_select)
the_tables[task] = (gold_path, gold_estatus)

cyber_central.save_task_3(task, gold_path, gold_estatus)
print(f"\tRows: {gold_estatus.count()}")

# COMMAND ----------

# MAGIC %md 
# MAGIC ### SAP Saldos

# COMMAND ----------

task = 'sap_saldos'

specs_df, spec_joins = read_cyber_specs(task)

specs_df_2 = (specs_df
    .reset_index()
    .rename(columns={
        'Longitud': 'len', 'Posición inicial': 'pos', 
        'nombre'  : 'name','PyType': 'pytype'}))

specs_dict = cyber_central.specs_reader_1(specs_df, tables_dict)
# Tiene: [readers, missing, fix_vals]

missing_cols[task] = specs_dict['missing']

gold_3 = cyber_central.master_join_2(spec_joins, specs_dict, tables_dict)

# fxd_widther = cyber_central.fxw_converter(specs_df)
# [0-fill, 1-cast, 2-select]
fxd_selector = cyber_builder.get_loader(specs_df_2, 'fixed-width')

gold_2 = (gold_3
    .select_plus(fxd_selector))

gold_2.display()

one_select = F.concat(*specs_df['nombre']).alias('~'.join(specs_df['nombre']))

gold_saldos = gold_2.select(one_select)
the_tables[task] = (gold_path, gold_saldos)

cyber_central.save_task_3(task, gold_path, gold_saldos)
print(f"\tRows: {gold_saldos.count()}")

# COMMAND ----------

missing_cols

# COMMAND ----------

#print(the_tables['sap_saldos'][0])
the_tables['sap_pagos'][1].display()
