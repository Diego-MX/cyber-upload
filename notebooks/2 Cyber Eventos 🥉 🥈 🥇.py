# Databricks notebook source
# MAGIC %md 
# MAGIC
# MAGIC # Preparación
# MAGIC
# MAGIC * Las modificaciones `silver` se hacen en las tablas base, y se verifican los tipos de columnas desde el lado de la fuente. 
# MAGIC * La preparación `gold` consiste en unir las `silver`, y se utilizan los tipos de columnas especificados para crear el _output_.

# COMMAND ----------

# MAGIC %pip install -r ../reqs_dbks.txt

# COMMAND ----------

from collections import defaultdict, OrderedDict
from datetime import datetime as dt, date, timedelta as delta
from json import dumps

import numpy as np
from os import makedirs, path, environ
import pandas as pd
from pandas import DataFrame as pd_DF
from pathlib import Path
from pyspark.sql import (functions as F, types as T, 
    Window as W, Column, DataFrame as spk_DF)
from pytz import timezone
import re
from toolz.functoolz import reduce
from typing import Union

# COMMAND ----------

from importlib import reload
from src.utilities import tools; reload(tools)

import epic_py; reload(epic_py)
from src import data_managers as epic_data; reload(epic_data)


# COMMAND ----------

from src.utilities import tools
from src.platform_resources import AzureResourcer
from src import data_managers as epic_data
from src.data_managers import EpicDF, CyberData

from config import ConfigEnviron, ENV, SERVER, DBKS_TABLES


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

the_loans = EpicDF(spark, f"{brz_path}/loan-contract/data")
the_loans.display()

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

cyber_tasks = ['sap_pagos', 'sap_estatus', 'sap_saldos']  

the_tables   = {}
missing_cols = {}
for task in cyber_tasks: 
    specs_df, spec_joins = read_cyber_specs(task)
    
    specs_dict = cyber_central.specs_reader_1(specs_df, tables_dict)
    # Tiene: [readers, missing, fix_vals]
    missing_cols[task] = specs_dict['missing']
    
    fxd_widther = cyber_central.fxw_converter(specs_df)
    
    gold_3 = cyber_central.master_join_2(spec_joins, specs_dict, tables_dict)
    gold_2 = gold_3.as_fixed_width(fxd_widther)
    
    one_select = F.concat(*gold_2.columns).alias('|'.join(gold_2.columns))
    gold_1 = gold_2.select(one_select)
    the_tables[task] = gold_1
    cyber_central.save_task_3(task, gold_path, gold_1)
    print(f"\tRows: {gold_1.count()}")
    
print(f"Missing columns are: {dumps2(missing_cols, indent=2)}")
