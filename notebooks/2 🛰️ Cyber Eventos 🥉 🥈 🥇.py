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

read_specs_from = 'blob'  # blob, repo
# Repo es la forma formal, como se lee en PRD. 
# Blob es la forma rápida, que se actualiza desde local, sin necesidad de Github PUSH. 

# COMMAND ----------

from collections import OrderedDict
from datetime import date
from json import dumps
import os
import pandas as pd
from pathlib import Path
from pyspark.sql import (functions as F, SparkSession)
from pyspark.dbutils import DBUtils
import re
from subprocess import check_call
import yaml

spark = SparkSession.builder.getOrCreate()
dbutils = DBUtils(spark)

with open("../user_databricks.yml", 'r') as _f: 
    u_dbks = yaml.safe_load(_f)

epicpy_load = {
    'url'   : 'github.com/Bineo2/data-python-tools.git', 
    'branch': 'dev-diego', 
    'token' : dbutils.secrets.get(u_dbks['dbks_scope'], u_dbks['dbks_token']) }  

url_call = "git+https://{token}@{url}@{branch}".format(**epicpy_load)
check_call(['pip', 'install', url_call])

# COMMAND ----------

from importlib import reload
from src import data_managers; reload(data_managers)
import config; reload(config)

from epic_py.delta import EpicDF, EpicDataBuilder

from src.data_managers import CyberData
from src.utilities import tools

from config import (app_agent, app_resourcer, cyber_handler, cyber_rename)

stg_account = app_resourcer['storage']
stg_permissions = app_agent.prep_dbks_permissions(stg_account, 'gen2')
app_resourcer.set_dbks_permissions(stg_permissions)

λ_path = (lambda cc, pp: app_resourcer.get_resource_url(
        'abfss', 'storage', container=cc, blob_path=pp))

brz_path  = λ_path('bronze', 'ops/core-banking')  
gold_path = λ_path('gold', 'cx/collections/cyber') 

specs_path = "cx/collections/cyber/spec_files"  # @Blob Storage
tmp_downer = "/FileStore/cyber/specs"   # @local (dbks) driver node ≠ DBFS 

cyber_central = CyberData(spark)
cyber_builder = EpicDataBuilder(typehandler=cyber_handler)

def dumps2(an_obj, **kwargs): 
    dump1 = dumps(an_obj, **kwargs)
    dump2 = re.sub(r'(,)\n *', r'\1 ', dump1)
    return dump2

if not os.path.isdir(tmp_downer): 
    os.makedirs(tmp_downer)
    # dbutils.fs.mkdirs(f"file://{tmp_downer}")

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
# MAGIC ## Construir pre-tablas 

# COMMAND ----------

# Revisar especificación en ~/refs/catalogs/cyber_txns.xlsx
# O en User Story, o en Correos enviados.  

balances = cyber_central.prepare_source('balances', 
    path=f"{brz_path}/loan-contract/aux/balances-wide")
    
open_items_long = cyber_central.prepare_source('open-items-long', 
    path=f"{brz_path}/loan-contract/chains/open-items")

open_items_wide = cyber_central.prepare_source('open-items-wide', 
    path=f"{brz_path}/loan-contract/aux/open-items-wide")
    # tiene muchos CURRENT_AMOUNT : NULL

loan_contracts = cyber_central.prepare_source('loan-contracts', 
    path=f"{brz_path}/loan-contract/data", 
    open_items=open_items_wide)

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

from pathlib import Path
from collections import OrderedDict

def read_cyber_specs(task_key: str, downer='blob'): 
    # Usa TMP_DOWNER, SPECS_PATH, 
    dir_at = tmp_downer if downer == 'blob' else '../refs/catalogs'
    file_nm = task_key if downer == 'blob' else f'cyber_{task_key}'

    specs_file = f"{dir_at}/{file_nm}.feather"
    joins_file = f"{dir_at}/{file_nm}_joins.csv"
    
    if downer == 'blob': 
        specs_blob = f"{dir_at}/{file_nm}_specs_latest.feather"
        joins_blob = f"{dir_at}/{file_nm}_joins_latest.csv"
        app_resourcer.download_storage_blob(specs_file, specs_blob, 'gold', verbose=1)
        app_resourcer.download_storage_blob(joins_file, joins_blob, 'gold', verbose=1)
    
    specs_df = cyber_central.specs_setup_0(specs_file)
    
    if Path(joins_file).is_file(): 
        join_df = (pd.read_csv(joins_file)
            .set_index('tabla'))
        
        joins_dict = OrderedDict()
        for tabla, rr in join_df.iterrows():
            if not tools.is_na(rr['join_cols']):
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

cyber_tasks = ['sap_pagos', 'sap_estatus', 'sap_saldos']  
re_col = r"Column<'CAST\((.*) AS [A-Z]*\)'>"

exportar = True

the_tables   = {}
missing_cols = {}

# COMMAND ----------

# MAGIC %md
# MAGIC ### SAP Saldos

# COMMAND ----------

task = 'sap_saldos'

specs_df, spec_joins = read_cyber_specs(task, read_specs_from)
specs_df_2 = specs_df.rename(columns=cyber_rename)
specs_dict = cyber_central.specs_reader_1(specs_df, tables_dict)
# Tiene: [readers, missing, fix_vals]

missing_cols[task] = specs_dict['missing']
one_select = F.concat(*specs_df['nombre']).alias('~'.join(specs_df['nombre']))

widther_2 = cyber_builder.get_loader(specs_df_2, 'fixed-width')

gold_3 = cyber_central.master_join_2(spec_joins, specs_dict, tables_dict)
gold_2 = gold_3.with_column_plus(widther_2)

gold_saldos = gold_2.select(one_select)
the_tables[task] = gold_saldos
cyber_central.save_task_3(task, gold_path, gold_saldos)

if exportar:
    print(f"\tRows: {gold_saldos.count()}")
    gold_saldos.display()

# COMMAND ----------

# MAGIC %md 
# MAGIC ### SAP Estatus

# COMMAND ----------

task = 'sap_estatus'
specs_df, spec_joins = read_cyber_specs(task, read_specs_from)

specs_df_2 = specs_df.rename(columns=cyber_rename)

specs_dict = cyber_central.specs_reader_1(specs_df, tables_dict)
# Tiene: [readers, missing, fix_vals]
missing_cols[task] = specs_dict['missing']

widther_2 = cyber_builder.get_loader(specs_df_2, 'fixed-width')

gold_3 = cyber_central.master_join_2(spec_joins, specs_dict, tables_dict)

gold_2 = gold_3.with_column_plus(widther_2)

one_select = F.concat(*specs_df['nombre']).alias('~'.join(specs_df['nombre']))
gold_estatus = gold_2.select(one_select)
the_tables[task] = gold_estatus
cyber_central.save_task_3(task, gold_path, gold_estatus)
print(f"\tRows: {gold_estatus.count()}")
if exportar: 
    gold_estatus.display()
     

# COMMAND ----------

gold_estatus.display()

# COMMAND ----------

# MAGIC %md 
# MAGIC ### SAP Pagos

# COMMAND ----------

task = 'sap_pagos'
specs_df, spec_joins = read_cyber_specs(task, read_specs_fromt)

specs_df_2 = specs_df.rename(columns=cyber_rename)

specs_dict = cyber_central.specs_reader_1(specs_df, tables_dict)
# Tiene: [readers, missing, fix_vals]
missing_cols[task] = specs_dict['missing']

widther_2 = cyber_builder.get_loader(specs_df_2, 'fixed-width')

gold_3 = cyber_central.master_join_2(spec_joins, specs_dict, tables_dict)

gold_2 = gold_3.with_column_plus(widther_2)

one_select = F.concat(*specs_df['nombre']).alias('~'.join(specs_df['nombre']))
gold_pagos = gold_2.select(one_select)
the_tables[task] = gold_pagos
cyber_central.save_task_3(task, gold_path, gold_pagos)
print(f"\tRows: {gold_pagos.count()}")
gold_pagos.display()

# COMMAND ----------

print(f"Missing columns are: {dumps2(missing_cols, indent=2)}")
the_tables['sap_estatus'].display()
