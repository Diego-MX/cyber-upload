# Databricks notebook source    # pylint: disable=missing-module-docstring,invalid-name
# MAGIC %md 
# MAGIC
# MAGIC # Preparación
# MAGIC
# MAGIC * Las modificaciones `silver` se hacen en las tablas base, y se verifican 
# MAGIC   los tipos de columnas desde el lado de la fuente. 
# MAGIC * La preparación `gold` consiste en unir las `silver`, y se utilizan los 
# MAGIC   tipos de columnas especificados para crear el _output_.

# COMMAND ----------

from importlib import reload
from src.setup import pkg_epicpy; reload(pkg_epicpy)    # pylint: disable=multiple-statements
pkg_epicpy.install_it()

# COMMAND ----------

# balances, open-items-long, open-items-wide, loan-contracts, person-set, txns-set, txns-grp
TEST_TABLE = 'loan-contracts'

# match, null-ids
WHICH_TEST = 'match'

# COMMAND ----------

# pylint: disable=multiple-statements
# pylint: disable=ungrouped-imports
# pylint: disable=unspecified-encoding
# pylint: disable=wrong-import-position,wrong-import-order

# COMMAND ----------

from collections import OrderedDict
from operator import itemgetter as ɣ, methodcaller as ϱ 
from pathlib import Path

import pandas as pd
from pyspark.sql import functions as F, SparkSession
from pyspark.dbutils import DBUtils   # pylint: disable=import-error,no-name-in-module
from toolz import complement, compose_left, pipe
from toolz.curried import map as map_z, valfilter, valmap

spark = SparkSession.builder.getOrCreate()
dbutils = DBUtils(spark)

# COMMAND ----------

from src import data_managers; reload(data_managers)
import config2; reload(config2)

from epic_py.delta import EpicDF, EpicDataBuilder
from epic_py.partners.apis_core import SAPSession
from epic_py.tools import packed
from src.data_managers import CyberData
from src.utilities import tools

from config2 import app_agent, app_resourcer, prep_core, cyber_handler, cyber_rename

stg_account = app_resourcer['storage']
stg_permissions = app_agent.prep_dbks_permissions(stg_account, 'gen2')
app_resourcer.set_dbks_permissions(stg_permissions)
core_session = SAPSession(prep_core)

brz_path = app_resourcer.get_resource_url('abfss', 'storage', 
    container='bronze', blob_path='ops/core-banking')  

specs_path = "cx/collections/cyber/spec_files"  # @Blob Storage
tmp_downer = "/FileStore/cyber/specs"           # @local (dbks) driver node ≠ DBFS 

cyber_central = CyberData(spark)
cyber_builder = EpicDataBuilder(typehandler=cyber_handler)

# COMMAND ----------

balances = cyber_central.prepare_source('balances', 
    path=f"{brz_path}/loan-contract/aux/balances-wide")
    
open_items = cyber_central.prepare_source('open-items', 
    path=f"{brz_path}/loan-contract/chains/open-items")

open_items_wide = cyber_central.prepare_source('open-items-wide', 
    path=f"{brz_path}/loan-contract/aux/open-items-wide")

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
    "OpenItemsLong": open_items,
    "PersonSet"    : persons, 
    "TxnsGrouped"  : txn_pmts, 
    "TxnsPayments" : the_txns}

print("The COUNT stat in each table is:")
for kk, vv in tables_dict.items(): 
    print(kk, vv.count())
    
# COMMAND ----------

cyber_tasks = ['sap_pagos', 'sap_estatus', 'sap_saldos']  

the_tables = {}
missing_cols = {}

def read_cyber_specs(task_key: str, downer='blob'): 
    specs_file, joins_file = _set_specs_file(task_key, downer)
    the_specs = cyber_central.specs_setup_0(specs_file)
    if Path(joins_file).is_file():
        the_joins = _df_joiner(pd.read_csv(joins_file))
    else: 
        the_joins = None
    return (the_specs, the_joins)


def _set_specs_file(task_key: str, downer='blob'): 
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


def _df_joiner(join_df) -> OrderedDict: 
    λ_col_alias = lambda cc, aa: F.col(cc).alias(aa)

    splitter = compose_left(ϱ('split', ','), 
        map_z(ϱ('split', '=')), 
        map_z(packed(λ_col_alias)), 
        list)
    joiner = pipe(join_df.iterrows(), 
        map_z(ɣ('tabla', 'join_cols')), dict, 
        valfilter(complement(tools.is_na)), 
        valmap(splitter))
    return joiner
    
# COMMAND ----------

# MAGIC %md 
# MAGIC # Exploration
# MAGIC

# COMMAND ----------

specs_df, spec_joins = read_cyber_specs('sap_saldos', 'repo')
specs_df_ii = specs_df.rename(columns=cyber_rename)
specs_dict = cyber_central.specs_reader_1(specs_df, tables_dict)

widther = cyber_builder.get_loader(specs_df_ii, 'fixed-width')

saldos_2 = cyber_central.master_join_2(spec_joins, specs_dict, tables_dict)
saldos_tbls = saldos_2.with_column_plus(widther)

# COMMAND ----------

# MAGIC %md 
# MAGIC ## PersonSet

# COMMAND ----------

person_set = core_session.call_data_api('person-set', )

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

person_call = core_session.call_data_api('person-set', '')


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

an_open_item = (cyber_central.prepare_source('open-items',
        path=f"{brz_path}/loan-contract/chains/open-items")
    .filter(F.col('ContractID') == "03017114357-444-MX"))
an_open_item.display()

# COMMAND ----------

open_items_d = (cyber_central.prepare_source('open-items',
        path=f"{brz_path}/loan-contract/chains/open-items", debug=True)
    .filter(F.col('ContractID') == "03017114357-444-MX")
    .select('ID', 'OpenItemID', 'DueDateShift', F.col('StatusCategory').alias('s_cat'), 
        F.col('ReceivableType').alias('rec_type'), 'cleared', 'uncleared', 
        'dds_default', 'is_min_dds', 'is_default', 'is_capital', 'is_recvble', 
        'Amount', 'ReceivableDescription', 'DueDate', 'StatusTxt'))
    
open_items_d.display()

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
