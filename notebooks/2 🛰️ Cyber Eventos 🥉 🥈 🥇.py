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

read_specs_from = 'repo'
# Puede ser:  {blob, repo}
# REPO es la forma formal, como se lee en PRD. 
# BLOB es la forma rápida, que se actualiza desde local, sin necesidad de Github PUSH. 

epicpy_tag = 'v1.1.18'      # dev-diego
to_display = False

# COMMAND ----------

# pylint: disable=wrong-import-position,wrong-import-order
# pylint: disable=ungrouped-imports
from collections import OrderedDict
from datetime import datetime as dt
from json import dumps
from operator import methodcaller as ϱ
import os
from pathlib import Path
from pytz import timezone as tz
import re
from subprocess import check_call

import pandas as pd
from pyspark.sql import functions as F, SparkSession
from pyspark.dbutils import DBUtils     # pylint: disable=import-error,no-name-in-module
from toolz import compose_left, pipe
import yaml

spark = SparkSession.builder.getOrCreate()
dbutils = DBUtils(spark)

with open("../user_databricks.yml", 'r') as _f:     # pylint: disable=unspecified-encoding
    u_dbks = yaml.safe_load(_f)

epicpy_load = {
    'url'   : 'github.com/Bineo2/data-python-tools.git', 
    'branch': epicpy_tag, 
    'token' : dbutils.secrets.get(u_dbks['dbks_scope'], u_dbks['dbks_token']) }  

url_call = "git+https://{token}@{url}@{branch}".format(**epicpy_load)
check_call(['pip', 'install', url_call])

# COMMAND ----------

from importlib import reload
from src import data_managers; reload(data_managers)
import config; reload(config)

from epic_py.delta import EpicDataBuilder, F_latinize
from epic_py.tools import packed, partial2
from src.data_managers import CyberData
from src.utilities import tools

from config import app_agent, app_resourcer, cyber_handler, specs_rename

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

def set_specs_file(task_key: str, downer='blob'):
     # Usa TMP_DOWNER, SPECS_PATH,
    if downer == 'blob':
        dir_at = tmp_downer # and no prefix
        specs_file = f"{dir_at}/{task_key}.feather"
        joins_file = f"{tmp_downer}/{task_key}_joins.csv"
        specs_blob = f"{specs_path}/{task_key}_specs_latest.feather"
        joins_blob = f"{specs_path}/{task_key}_joins_latest.csv"
        app_resourcer.download_storage_blob(specs_file, specs_blob, 'gold', verbose=1)
        app_resourcer.download_storage_blob(joins_file, joins_blob, 'gold', verbose=1)
    elif downer == 'repo':
        dir_at = "../refs/catalogs"  # prefix: "cyber_"
        specs_file = f"{dir_at}/cyber_{task_key}.feather"
        joins_file = f"{dir_at}/cyber_{task_key}_joins.csv"
    return (specs_file, joins_file)


def df_joiner(join_df) -> OrderedDict:
    λ_col_alias = lambda cc_aa: F.col(cc_aa[0]).alias(cc_aa[1])

    splitter = compose_left(
        ϱ('split', ','), 
        partial2(map, ϱ('split', '=')), 
        partial2(map, λ_col_alias), 
        list)
    joiner = OrderedDict((rr['tabla'], splitter(rr['join_cols']))
        for _, rr in join_df.iterrows()
        if not tools.is_na(rr['join_cols']))
    return joiner


def read_cyber_specs(task_key: str, downer='blob'):
    specs_file, joins_file = set_specs_file(task_key, downer)
    a_specs = cyber_central.specs_setup_0(specs_file)
    if Path(joins_file).is_file():
        joins_dict = df_joiner(pd.read_csv(joins_file))
    else:
        joins_dict = None

    return a_specs, joins_dict

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

the_tables = {}
missing_cols = {}

# COMMAND ----------

# MAGIC %md
# MAGIC ### SAP Saldos

# COMMAND ----------

task = 'sap_saldos'     # pylint: disable=invalid-name

specs_df, spec_joins = read_cyber_specs(task, read_specs_from)
specs_df_ii = specs_df.rename(columns=specs_rename)
specs_dict = cyber_central.specs_reader_1(specs_df, tables_dict)
# Tiene: [readers, missing, fix_vals]

missing_cols[task] = specs_dict['missing']
the_names = specs_df['nombre']
one_select = pipe(the_names,
    packed(F.concat),
    F_latinize,
    ϱ('alias', '~'.join(the_names)))

widther = cyber_builder.get_loader(specs_df_ii, 'fixed-width')

saldos_2 = cyber_central.master_join_2(spec_joins, specs_dict, tables_dict)
saldos_3 = saldos_2.with_column_plus(widther)

gold_saldos = saldos_3.select(one_select)

the_tables[task] = gold_saldos

cyber_central.save_task_3(task, gold_path, gold_saldos)

if to_display:
    print(f"\tRows: {gold_saldos.count()}")
    gold_saldos.display()

# COMMAND ----------

# MAGIC %md 
# MAGIC ### SAP Estatus

# COMMAND ----------

task = 'sap_estatus'        # pylint: disable=invalid-name
specs_df, spec_joins = read_cyber_specs(task, read_specs_from)
the_names = specs_df['nombre']

one_select = pipe(the_names,
    packed(F.concat),
    F_latinize,
    ϱ('alias', '~'.join(the_names))) 

specs_df_2 = specs_df.rename(columns=specs_rename)

specs_dict = cyber_central.specs_reader_1(specs_df, tables_dict)
# Tiene: [readers, missing, fix_vals]

missing_cols[task] = specs_dict['missing']

widther_2 = cyber_builder.get_loader(specs_df_2, 'fixed-width')

estatus_2 = cyber_central.master_join_2(spec_joins, specs_dict, tables_dict)

estatus_3 = estatus_2.with_column_plus(widther_2)

gold_estatus = estatus_3.select(one_select)
the_tables[task] = gold_estatus
cyber_central.save_task_3(task, gold_path, gold_estatus)
print(f"\tRows: {gold_estatus.count()}")
if to_display:
    gold_estatus.display()

# COMMAND ----------

# MAGIC %md 
# MAGIC ### SAP Pagos

# COMMAND ----------

task = 'sap_pagos'      # pylint: disable=invalid-name
specs_df, spec_joins = read_cyber_specs(task, read_specs_from)
the_names = specs_df['nombre']
one_select = pipe(the_names,
    packed(F.concat),
    F_latinize,
    ϱ('alias', '~'.join(the_names))) 

specs_df_2 = specs_df.rename(columns=specs_rename)

specs_dict = cyber_central.specs_reader_1(specs_df, tables_dict)
# Tiene: [readers, missing, fix_vals]

missing_cols[task] = specs_dict['missing']

widther_2 = cyber_builder.get_loader(specs_df_2, 'fixed-width')

gold_3 = cyber_central.master_join_2(spec_joins, specs_dict, tables_dict)
gold_2 = gold_3.with_column_plus(widther_2)

gold_pagos = gold_2.select(one_select)
the_tables[task] = gold_pagos
cyber_central.save_task_3(task, gold_path, gold_pagos)

print(f"\tRows: {gold_pagos.count()}")
if to_display:
    gold_pagos.display()

# COMMAND ----------

print(f"Missing columns are: {dumps2(missing_cols, indent=2)}")

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Exploración de archivos

# COMMAND ----------

a_dir = f"{gold_path}/recent"
tz_mx = tz('America/Mexico_City')
print(a_dir)
for x in dbutils.fs.ls(a_dir): 
    x_time = pipe(x.modificationTime/1000, 
        dt.fromtimestamp, 
        ϱ('astimezone', tz_mx), 
        ϱ('strftime', "%d %b '%y %H:%M"))
    print(f"{x.name}\t=> {x_time}")
