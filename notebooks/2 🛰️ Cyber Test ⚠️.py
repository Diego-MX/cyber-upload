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

# COMMAND ----------

from collections import OrderedDict
from datetime import date
from json import dumps
import os
from pathlib import Path
import re
from subprocess import check_call

import pandas as pd
from pyspark.sql import (functions as F, SparkSession, Window as W)
from pyspark.dbutils import DBUtils
from toolz import compose_left, curried
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
from epic_py.tools import method_getter, partial2
from src.data_managers import CyberData     # , set_specs_file, read_cyber_specs
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

cyber_tasks = ['sap_pagos', 'sap_estatus', 'sap_saldos']  

exportar = True

the_tables = {}
missing_cols = {}


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
    cols_by = ['tabla', 'join_cols']
    λ_col_alias = lambda cc_aa: F.col(cc_aa[0]).alias(cc_aa[1])

    splitter = compose_left(
        method_getter('split', ','), 
        partial2(map, method_getter('split', '=')), 
        partial2(map, λ_col_alias), 
        list)
    joiner = OrderedDict((rr['tabla'], splitter(rr['join_cols']))
        for _, rr in join_df.iterrows()
        if not tools.is_na(rr['join_cols']))
    return joiner
    

def read_cyber_specs(task_key: str, downer='blob'): 
    specs_file, joins_file = set_specs_file(task_key, downer)
    specs_df = cyber_central.specs_setup_0(specs_file)
    if Path(joins_file).is_file():
        joins_dict = df_joiner(pd.read_csv(joins_file))
    else: 
        joins_dict = None

    return specs_df, joins_dict

# COMMAND ----------

task = 'sap_saldos'

specs_df, spec_joins = read_cyber_specs(task, read_specs_from)
specs_df_2 = specs_df.rename(columns=specs_rename)
specs_dict = cyber_central.specs_reader_1(specs_df, tables_dict)

missing_cols[task] = specs_dict['missing']
one_select = F.concat(*specs_df['nombre']).alias('~'.join(specs_df['nombre']))

widther_2 = cyber_builder.get_loader(specs_df_2, 'fixed-width')

saldos_tbls = (cyber_central
    .master_join_2(spec_joins, specs_dict, tables_dict, tables_only=True))
    


# COMMAND ----------

saldos_tbls.display()

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC # Exploration
# MAGIC

# COMMAND ----------

# MAGIC %md 
# MAGIC ## PersonSet

# COMMAND ----------

person_weird0 = saldos_tbls.filter(F.col('PersonSet').isNull())

person_weird1 = (EpicDF(spark, f"{brz_path}/person-set/chains/person")
    .withColumn('person_id', F.col('ID'))
    .join(person_weird0, on='person_id', how='semi'))

person_weird2 = (EpicDF(spark, f"{brz_path}/loan-contract/data")
    .withColumn('loan_id', F.col('ID'))
    .join(person_weird0, on='loan_id', how='semi'))

person_weird2.display()


# COMMAND ----------

person_weird1.display()

# COMMAND ----------

# MAGIC %md 
# MAGIC ## BalancesWide
# MAGIC
# MAGIC Este está OK, lo dejamos por completez. 

# COMMAND ----------

balances_weird0 = saldos_tbls.filter(F.col('BalancesWide').isNull())

balances_weird1 = (EpicDF(spark, f"{brz_path}/loan-contract/aux/balances-wide")
    .withColumn('loan_id', F.col('ID'))
    .join(balances_weird0, on='loan_id', how='semi'))

balances_weird2 = (EpicDF(spark, f"{brz_path}/loan-contract/data")
    .withColumn('loan_id', F.col('ID'))
    .join(balances_weird0, on='loan_id', how='semi'))

balances_weird2.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## OpenItems
# MAGIC

# COMMAND ----------

items_weird0 = saldos_tbls.filter(F.col('OpenItems').isNull())

items_weird1 = (EpicDF(spark, f"{brz_path}/loan-contract/aux/open-items-wide")
    .withColumn('loan_id', F.col('ContractID'))
    .join(items_weird0, on='loan_id', how='semi'))

items_weird2 = (EpicDF(spark, f"{brz_path}/loan-contract/data")
    .withColumn('loan_id', F.col('ID'))
    .join(items_weird0, on='loan_id', how='semi'))

items_weird2.display()

# COMMAND ----------

items_weird1.display()

# COMMAND ----------

# MAGIC %md 
# MAGIC ## TxnsGrouped
# MAGIC

# COMMAND ----------

tgrp_weird0 = saldos_tbls.filter(F.col('TxnsGrouped').isNull())

tgrp_weird1 = (EpicDF(spark, f"{brz_path}/transaction-set/data")
    .withColumn('loan_id', F.col('AccountID'))
    .join(tgrp_weird0, on='loan_id', how='semi'))

tgrp_weird2 = (EpicDF(spark, f"{brz_path}/loan-contract/data")
    .withColumn('loan_id', F.col('ID'))
    .join(tgrp_weird0, on='loan_id', how='semi'))

tgrp_weird2.display()

# COMMAND ----------

tgrp_weird1.display()

# COMMAND ----------

# MAGIC %md 
# MAGIC ## OpenItemsLong

# COMMAND ----------

items_l_weird0 = saldos_tbls.filter(F.col('OpenItemsLong').isNull())

items_l_weird1 = (EpicDF(spark, f"{brz_path}/loan-contract/chains/open-items")
    .withColumn('loan_id', F.col('ContractID'))
    .join(items_l_weird0, on='loan_id', how='semi'))

items_l_weird2 = (EpicDF(spark, f"{brz_path}/loan-contract/data")
    .withColumn('loan_id', F.col('ID'))
    .join(items_l_weird0, on='loan_id', how='semi'))

items_l_weird2.display()
    

# COMMAND ----------

items_l_weird1.display()
