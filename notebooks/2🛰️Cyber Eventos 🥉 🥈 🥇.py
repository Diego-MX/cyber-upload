# Databricks notebook source
# MAGIC %md 
# MAGIC
# MAGIC # Preparación
# MAGIC
# MAGIC * Las modificaciones `silver` se hacen en las tablas base, y se verifican 
# MAGIC los tipos de columnas desde el lado de la fuente. 
# MAGIC * La preparación `gold` consiste en unir las `silver`, y se utilizan los tipos 
# MAGIC de columnas especificados para crear el _output_.

# COMMAND ----------

from src.setup import dependencies as deps
deps.from_reqsfile("../reqs_dbks.txt")
deps.gh_epicpy("gh-1.6", "../user_databricks.yml", False, True)

# COMMAND ----------

TO_DISPLAY = True
READ_SPECS_FROM = 'repo'
# Puede ser:  {blob, repo}
# REPO es la forma formal, como se lee en PRD.
# BLOB es la forma rápida, que se actualiza desde local, sin necesidad de Github PUSH.

# pylint: disable=multiple-statements
# pylint: disable=no-value-for-parameter
# pylint: disable=ungrouped-imports
# pylint: disable=wrong-import-position,wrong-import-order

# COMMAND ----------

from collections import OrderedDict
from json import dumps
from operator import methodcaller as ϱ, attrgetter as ɣ
import os
from pathlib import Path
import re

import pandas as pd
from pyspark.sql import functions as F, SparkSession    # pylint: disable=import-error
from pyspark.dbutils import DBUtils     # pylint: disable=import-error,no-name-in-module
from toolz import compose_left, pipe
from toolz.curried import map as map_z, valmap

spark = SparkSession.builder.getOrCreate()
dbutils = DBUtils(spark)

# COMMAND ----------

from epic_py.delta import EpicDataBuilder, F_latinize, column_name
from epic_py.tools import msec_strftime, packed
from src.data_managers import CyberData
from src.utilities import tools

from config2 import app_agent, app_resourcer, cyber_handler, cyber_rename

stg_permissions = app_agent.prep_dbks_permissions(app_resourcer['storage'], 'gen2')
app_resourcer.set_dbks_permissions(stg_permissions)

brz_path  = app_resourcer.get_resource_url(
        'abfss', 'storage', container='bronze', blob_path='ops/core-banking-x')
gold_path = app_resourcer.get_resource_url(
        'abfss', 'storage', container='gold', blob_path='cx/collections/cyber')

specs_path = "cx/collections/cyber/spec_files"  # @Blob Storage # pylint: disable=invalid-name
tmp_downer = "/FileStore/cyber/specs"   # @local (dbks) driver node ≠ DBFS  # pylint: disable=invalid-name

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
# MAGIC * `Loans Contract`:  se filtran los prestamos, modifican fechas, y agregan 
# MAGIC algunas columnas auxiliares.  
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

open_items = cyber_central.prepare_source('open-items',
    path=f"{brz_path}/loan-contract/chains/open-items")

loan_contracts = cyber_central.prepare_source('loan-contracts', 
    path=f"{brz_path}/loan-contract/data", 
    open_items=open_items)

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
    "OpenItemsLong": open_items,
    "PersonSet"    : persons, 
    "TxnsGrouped"  : txn_pmts, 
    "TxnsPayments" : the_txns}

print("The COUNT stat in each table is:")
for kk, vv in tables_dict.items():
    print(kk, vv.count())

# COMMAND ----------

# MAGIC %md 
# MAGIC Las columnas de Open Items (debug = `True`) son:  
# MAGIC ```python
# MAGIC [   'ReceivableType', 'DueDate', 'ContractID', 'OpenItemID', 'Amount', 
# MAGIC     'Currency', 'StatusCategory', 'ReceivableDescription', 'ReceivableTypeTxt',  
# MAGIC     'StatusTxt', 'Status', 'sap_AccountID', 'sap_EventID',  
# MAGIC     'sap_EventDateTime', 'epic_id', 'epic_date', 'rank_item', 'n_item', 
# MAGIC     'DueDateShift', 'yesterday', 'ID', 'due_date_', 'is_default', 'is_capital',  
# MAGIC     'is_iva', 'is_interest', 'is_recvble', 'cleared', 'dds_default', 
# MAGIC     'uncleared', 'is_min_dds']
# MAGIC ```
# MAGIC El código que usamos para inspeccionar los _open items_ es: 
# MAGIC
# MAGIC ```python
# MAGIC open_items_d = cyber_central.prepare_source('open-items',
# MAGIC     path=f"{brz_path}/loan-contract/chains/open-items", debug=True)
# MAGIC
# MAGIC (open_items_d
# MAGIC     .filter(F.col('ID') == "03017114357-444-MX")
# MAGIC     .select('ID', 'OpenItemID', 'DueDateShift', F.col('StatusCategory').alias('s_cat'), 
# MAGIC         'ReceivableType'.alias('rec_type'), 'cleared', 'uncleared', 
# MAGIC         'dds_default', 'is_min_dds', 'is_default', 'is_recvble', 
# MAGIC         'Amount', 'ReceivableDescription', 'DueDate', 'StatusTxt')
# MAGIC     .display())
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tabla de instrucciones
# MAGIC
# MAGIC Leemos los archivos `specs` y `joins` que se compilaron a partir de las 
# MAGIC definiciones en Excel.  Y de ahí, se preparan los archivos.  
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
        map_z(ϱ('split', '=')), 
        map_z(λ_col_alias), 
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
    return (a_specs, joins_dict)

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
# MAGIC Aplicamos las definiciones anteriores de acuerdo al tipo de columna 
# MAGIC `str`, `date`, `dbl`, `int`.  
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

def one_column(names, header=True):
    an_alias = '~'.join(names) if header else 'one-fixed-width'
    the_col = pipe(names,
        packed(F.concat),
        F_latinize,
        ϱ('alias', an_alias))
    return the_col

# COMMAND ----------

# MAGIC %md
# MAGIC ### SAP Saldos

# COMMAND ----------

task = 'sap_saldos'     # pylint: disable=invalid-name

specs_df, spec_joins = read_cyber_specs(task, READ_SPECS_FROM)
specs_df_ii = specs_df.rename(columns=cyber_rename)
specs_dict = cyber_central.specs_reader_1(specs_df, tables_dict)

missing_cols[task] = specs_dict['missing']
one_select = one_column(specs_df['nombre'])

widther = cyber_builder.get_loader(specs_df_ii, 'fixed-width')

saldos_2 = cyber_central.master_join_2(spec_joins, specs_dict, tables_dict)
saldos_3 = saldos_2.with_column_plus(widther)

gold_saldos = saldos_3.select(one_select)

the_tables[task] = gold_saldos

cyber_central.save_task_3(task, gold_path, gold_saldos)

if TO_DISPLAY:
    print(f"\tRows: {gold_saldos.count()}")
    gold_saldos.display()

saldos_col = column_name(one_select)
saldos_len = specs_df['width'].sum()

# COMMAND ----------

# MAGIC %md
# MAGIC ### SAP Estatus

# COMMAND ----------

task = 'sap_estatus'        #    pylint: disable=invalid-name
specs_df, spec_joins = read_cyber_specs(task, READ_SPECS_FROM)
one_select = one_column(specs_df['nombre'])

specs_df_2 = specs_df.rename(columns=cyber_rename)

specs_dict = cyber_central.specs_reader_1(specs_df, tables_dict)

missing_cols[task] = specs_dict['missing']

widther_2 = cyber_builder.get_loader(specs_df_2, 'fixed-width')

estatus_2 = cyber_central.master_join_2(spec_joins, specs_dict, tables_dict)

estatus_3 = estatus_2.with_column_plus(widther_2)

gold_estatus = estatus_3.select(one_select)
the_tables[task] = gold_estatus
cyber_central.save_task_3(task, gold_path, gold_estatus)
print(f"\tRows: {gold_estatus.count()}")
if TO_DISPLAY:
    gold_estatus.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### SAP Pagos

# COMMAND ----------

task = 'sap_pagos'      # pylint: disable=invalid-name
specs_df, spec_joins = read_cyber_specs(task, READ_SPECS_FROM)

one_select = one_column(specs_df['nombre'])

specs_df_2 = specs_df.rename(columns=cyber_rename)

specs_dict = cyber_central.specs_reader_1(specs_df, tables_dict)

missing_cols[task] = specs_dict['missing']

widther_2 = cyber_builder.get_loader(specs_df_2, 'fixed-width')

gold_3 = cyber_central.master_join_2(spec_joins, specs_dict, tables_dict)
gold_2 = gold_3.with_column_plus(widther_2)

gold_pagos = gold_2.select(one_select)
the_tables[task] = gold_pagos
cyber_central.save_task_3(task, gold_path, gold_pagos)

print(f"\tRows: {gold_pagos.count()}")
if TO_DISPLAY:
    gold_pagos.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exploración de archivos

# COMMAND ----------

a_dir = f"{gold_path}/recent"
print(a_dir)

file_infos = pipe(dbutils.fs.ls(a_dir), 
    map_z(ɣ('name', 'modificationTime')), dict, 
    valmap(msec_strftime), ϱ('items'), 
    map_z(packed("\t{} → {}".format)))

for ff in file_infos: 
    print(ff)

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Checks unitarios
# MAGIC
# MAGIC Por ejemplo, se reportó que algunos saldos vienen desfasados en el ancho fijo. 

# COMMAND ----------

import re

precheck_1 = (gold_saldos
    .withColumn('length', F.length(saldos_col))
    .filter(F.col('length') != saldos_len)
    .collect())

if len(precheck_1) > 0: 
    spark.DataFrame(precheck_1).display()
    raise RuntimeError("Saldos tiene filas de longitud errónea.")

