# Databricks notebook source
# MAGIC %md 
# MAGIC 
# MAGIC # Descripción
# MAGIC Este _notebook_ fue escrito originalmente por Jacobo.  
# MAGIC Para llevarlo de DEV a QAs, le hice (Diego) algunas factorizaciones:  
# MAGIC - Indicar tablas a partir de un diccionario en `CONFIG.PY`.  
# MAGIC - Agrupar el código por celdas de acuerdo a las tablas que se procesan.
# MAGIC - Encadenar las instrucciones de las tablas en una sola, cuando es posible. 

# COMMAND ----------

# MAGIC %pip install -r ../reqs_dbks.txt

# COMMAND ----------

import numpy as np
import pandas as pd
from pyspark.sql import functions as F, types as T, Window as W
from datetime import datetime as dt
import re

from src.platform_resources import AzureResourcer
from config import ConfigEnviron, ENV, SERVER, DBKS_TABLES

tables = DBKS_TABLES[ENV]['names']
app_environ = ConfigEnviron(ENV, SERVER, spark)
az_manager  = AzureResourcer(app_environ)

at_storage = az_manager.get_storage()
az_manager.set_dbks_permissions(at_storage)

# Sustituye el placeholder AT_STORAGE, aunque mantiene STAGE para sustituirse después. 
base_location = f"abfss://{{stage}}@{at_storage}.dfs.core.windows.net/ops/core-banking-batch-updates"


# COMMAND ----------

# MAGIC %md 
# MAGIC # Modificaciones Silver 🥈

# COMMAND ----------

# MAGIC %md
# MAGIC ## Solo 3 requieren modificación 

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Person Set 

# COMMAND ----------

# Person Set

persons = (spark.read.table(tables['brz_persons'])
    .withColumn('split'    , F.split('LastName', ' ', 2))
    .withColumn('LastNameP', F.col('split').getItem(0))
    .withColumn('LastNameM', F.col('split').getItem(1))
    .withColumnRenamed('Address', 'Address2')
    .withColumn('address' , F.concat_ws(' ', 'AddressStreet', 'AddressHouseID', 'AddressRoomID')))

display(persons)

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Balances

# COMMAND ----------

# Agrupar por ID, Currency, BalancesTS;
# Pivotear por Code: sum(Amount)

balances = (spark.read.table('bronze.loan_balances')
    # Set types
    .withColumn('Code', F.col('Code').cast(T.IntegerType()))
    .withColumn('Amount', F.col('Amount').cast(T.DoubleType()))
    .withColumn('BalancesTS', F.col('BalancesTS').cast(T.DateType()))
    # Pivot Code/Amount
    .groupBy(['ID', 'Currency', 'BalancesTS']).pivot('Code')
        .agg(F.round(F.sum(F.col('Amount')), 2)))

display(balances)

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Open Items

# COMMAND ----------

### ReceivableType
# 511080  Reemb.parcial.créd.(esp.)
# 511200  Comisión
# 990004  IVA Interés
# 990006  IVA Comisión
# 991100  Int. No Gravado
# 511100  Int. Nominal
# 511010  Capital

### Status
# 01 Creado    1
# 01 Creado    2
# 01 Creado    3
# 86 Suprimido 1

open_items = (spark.read.table("bronze.loan_open_items")
    # Set Types
    .withColumn('OpenItemTS',  F.to_date(F.col('OpenItemTS'), 'yyyy-MM-dd'))
    .withColumn('DueDate',     F.to_date(F.col('DueDate'),    'yyyyMMdd'))
    .withColumn('Amount',      F.col('Amount').cast(T.DoubleType()))
    # Aux Columns
    .withColumn('cleared',     F.regexp_extract('ReceivableDescription', r"Cleared: ([\d\.]+)", 1)
                                .cast(T.DoubleType())).fillna(0, subset=['cleared'])
    .withColumn('uncleared',   F.round(F.col('Amount') - F.col('cleared'), 2))
    .withColumn('vencido',    (F.col('DueDate') < F.col('OpenItemTS'))
                             & F.col('StatusCategory').isin([2, 3]))
    .withColumn('local/fgn',   F.when(F.col('Currency') == 'MXN', 'local').otherwise('foreign'))
    .withColumn('interes/iva', F.when(F.col('ReceivableType').isin([991100, 511100]), 'interes')
                                .when(F.col('ReceivableType') == 990004, 'iva'))
    .withColumn('chk_vencido', ( F.col('vencido') & (F.col('uncleared') >0))
                              |(~F.col('vencido') & (F.col('uncleared')==0))))

# Pivot on Interes/IVA, Local/Foreign and Uncleared. 
uncleared = (open_items
    .filter(F.col('Vencido') & F.col('Interes/IVA').isNotNull())
    .withColumn('pivoter', F.concat_ws('_', 'interes/iva', 'local/fgn'))
    .groupBy(['OpenItemTS', 'ContractID']).pivot('pivoter')
        .agg(F.round(F.sum(F.col('Uncleared')), 2)))

# Display for experimentation. 
display(uncleared)
#display(open_items.filter(~F.col('chk_vencido')))

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Preparación Gold 🥇 

# COMMAND ----------

# MAGIC %md
# MAGIC Algunas funciones auxiliares:

# COMMAND ----------

from typing import List
from pyspark.sql.dataframe import DataFrame as SparkDataFrame
from pyspark.sql.column import Column as SparkColumn

def select_ls_from_dict(rename_dict: dict) -> List[SparkColumn]: 
    the_list = [F.col(vv).alias(kk) for kk, vv in rename_dict.items()]
    return the_list



# COMMAND ----------

persons_name    = "bronze.persons_set"
loans_name      = "bronze.loan_contracts"    
lqan_name       = "bronze.loan_qan_contracts"
balances_name   = "bronze.loan_balances"     
opens_name      = "bronze.loan_open_items"   

tables_dict = {
    "bronze.persons_set"        : persons, 
    "bronze.loan_contracts"     : spark.read.table("bronze.loan_contracts"), 
    "bronze.loan_qan_contracts" : spark.read.table("bronze.loan_qan_contracts"),
    "bronze.loan_balances"      : balances,
    "bronze.loan_open_items"    : uncleared}



# COMMAND ----------

# MAGIC %md
# MAGIC ### Tabla de instrucciones

# COMMAND ----------

sap_cols_0 = pd.read_feather("../refs/catalogs/cyber_columns.feather")

campo = sap_cols_0['campo']
tipo_campo = pd.Series(np.where(campo.isnull(), 1,
                    np.where(campo.notnull() & (campo.str.slice(stop=1) != '='), 2, 3)))
            
sap_cols_1 = (sap_cols_0
    .assign(**{
        'tipo_campo': tipo_campo, 
        'la_columna': campo.where(tipo_campo.isin([1, 2]), 
                sap_cols_0['calc'].str.extract(r"\(.*, (.*)\)", expand=False))}))

sap_cols_1['in_sap'] = np.nan
for name in tables_dict: 
    sub_idx = (sap_cols_1['tabla_origen'] == name).isin([True])
    sap_cols_1.loc[sub_idx, 'in_sap'] = \
            sap_cols_1['la_columna'][sub_idx].isin(tables_dict[name].columns)

keep_rows = (sap_cols_1['tipo_campo'] == 1) | sap_cols_1['in_sap'] 
sap_cols = sap_cols_1[keep_rows]

#cols_1 = ['nombre', 'valor_fijo', 'tabla_origen', 'campo', 'calc', 'tipo_campo', 'la_columna'] 
cols_1 = ['nombre', 'valor_fijo', 'longitud2', 'decimales', ] #'tipo_campo', 'la_columna'
with pd.option_context('display.max_rows', None, 'display.max_columns', None, 'display.width', 220):
    # print(sap_cols[cols_1])
    # print(sap_cols.loc[sap_cols['tabla_origen'] == 'bronze.loan_contracts', cols_1])
    print(sap_cols_0.loc[sap_cols_0['valor_fijo'].notnull() & (sap_cols_0['Tipo de dato'] == 'NUMBER'), cols_1])


# COMMAND ----------

# MAGIC %md 
# MAGIC ### Tablas individuales

# COMMAND ----------

persons_select  = {a_row['nombre']: a_row['la_columna'] 
        for _, a_row in sap_cols[sap_cols['tabla_origen'] == 'bronze.persons_set'].iterrows()}
loans_select    = {a_row['nombre']: a_row['la_columna'] 
        for _, a_row in sap_cols[sap_cols['tabla_origen'] == 'bronze.loan_contracts'].iterrows()}
lqan_select     = {a_row['nombre']: a_row['la_columna'] 
        for _, a_row in sap_cols[sap_cols['tabla_origen'] == 'bronze.loan_qan_contracts'].iterrows()}
balances_select = {a_row['nombre']: a_row['la_columna'] 
        for _, a_row in sap_cols[sap_cols['tabla_origen'] == 'bronze.loan_balances'].iterrows()}
opens_select    = {a_row['nombre']: a_row['la_columna'] 
        for _, a_row in sap_cols[sap_cols['tabla_origen'] == 'bronze.loan_open_items'].iterrows()}

# With Join columns. 
persons_tbl  = tables_dict['bronze.persons_set'].select(*select_ls_from_dict(persons_select), F.col('ID').alias('person_id'))
loans_tbl    = tables_dict['bronze.loan_contracts'].select(*select_ls_from_dict(loans_select), F.col('ID').alias('loan_id'), F.col('BorrowerID').alias('person_id'))
lqan_tbl     = tables_dict['bronze.loan_qan_contracts'].select(*select_ls_from_dict(lqan_select), F.col('ID').alias('loan_id'))
balances_tbl = tables_dict['bronze.loan_balances'].select(*select_ls_from_dict(balances_select), F.col('ID').alias('loan_id'))
opens_tbl    = tables_dict['bronze.loan_open_items'].select(*select_ls_from_dict(opens_select), F.col('ContractID').alias('loan_id'))



# COMMAND ----------

# MAGIC %md 
# MAGIC ### Master Join and Fixed-Value Columns

# COMMAND ----------

fixed_slct = [F.lit(a_row['valor_fijo']).alias(a_row['nombre'])
    for _, a_row in sap_cols[sap_cols['tipo_campo'] == 1].iterrows()]

fix_chars = sap_cols['nombre'][
    (sap_cols['tipo_campo'] == 1) & (sap_cols['Tipo de dato'] == 'VARCHAR2')]

golden_1 = (persons_tbl
    .join(loans_tbl   , how='right', on='person_id')
    .join(lqan_tbl    , how='left' , on='loan_id')
    .join(balances_tbl, how='left' , on='loan_id') 
    .join(opens_tbl   , how='left' , on='loan_id')
    .drop(*['person_id', 'loan_id'])
    .select('*', *fixed_slct)
    .fillna('N/A', fix_chars.tolist()))

display(golden_1)


# COMMAND ----------

# MAGIC %md
# MAGIC ### Explicit conversion to string

# COMMAND ----------

fstring_keys = {
    'str': 's', 'date': 's', 'int': 'd', 'dbl': 'f'
}

sap_cols_fmt_0 = sap_cols.assign(**{
    'spk_type' : np.where(sap_cols['Tipo de dato'] == 'VARCHAR2', 'str', 
               np.where(sap_cols['Tipo de dato'] == 'DATE', 'date', 
             np.where(sap_cols['decimales'] > 0, 'dbl', 'int')))})

fill_zeros   = pd.Series(np.where(sap_cols_fmt_0['Tipo de dato'] == 'NUMBER', '0', ''))
fill_space   = pd.Series(np.where(sap_cols_fmt_0['Tipo de dato'] == 'NUMBER', ' ', ''))
old_longitud = (sap_cols_fmt_0['longitud2'] + sap_cols_fmt_0['decimales']).apply(str)
its_fkey     = pd.Series([fstring_keys[a_spk] for a_spk in sap_cols_fmt_0['spk_type']])
smp_longitud = sap_cols_fmt_0['longitud2'].apply(str)


sap_cols_fmt = sap_cols_fmt_0.assign(**{
    'f_string' : '%' + fill_zeros + old_longitud + its_fkey, 
    's_string' : '%' + smp_longitud + 's'}) 
    
fmt_cols = ['nombre', 'Posición inicial', 'longitud2', 'decimales', 'spk_type', 'Tipo de dato', 'f_string', 's_string']
with pd.option_context('display.max_rows', None, 'display.max_columns', None, 'display.width', 220):
    print(sap_cols_fmt.loc[sap_cols_fmt['Tipo de dato'] == 'NUMBER', fmt_cols])
    

# COMMAND ----------

int_select

# COMMAND ----------

# The types:  
# 1. Make sure to cast first. 
# 2. Each type is converted to string in its terms. 
# 3. Apply fixed width condition ...
# 4. Select columns in order. 

cast_types = {
    'str' : T.StringType(), 
    'int' : T.DoubleType(), 
    'dbl' : T.DoubleType(), 
    'date': T.DateType()
}

## 1. 
types_select = [F.col(a_row['nombre']).cast(cast_types[a_row['spk_type']]).alias(a_row['nombre'])
    for _, a_row in sap_cols_fmt.iterrows()]

## 2 and 3.  
str_select = [F.col(a_row['nombre']) 
    for _, a_row in sap_cols_fmt.iterrows() if a_row['spk_type'] == 'str']

date_select = [F.date_format(a_row['nombre'], 'MMddyyyy').alias(a_row['nombre']) 
    for _, a_row in sap_cols_fmt.iterrows() if a_row['spk_type'] == 'date']

#---  Assume decimales < 10. 
dbl_select = [F.number_format(a_row['nombre'], int(10*a_row['decimales'])).alias(a_row['nombre']) 
    for _, a_row in sap_cols_fmt.iterrows() if a_row['spk_type'] == 'dbl']

# int_select = [F.format_string(str(a_row['f_string']), a_row['nombre']).alias(a_row['nombre'])
#     for _, a_row in sap_cols_fmt.iterrows() if a_row['spk_type'] == 'int']

int_select = [F.format_string(str(a_row['f_string']), a_row['nombre']).alias(a_row['nombre'])
    for _, a_row in sap_cols_fmt.iterrows() if a_row['spk_type'] == 'int']


## 3. 
fxd_width_select = [F.format_string(str(a_row['s_string']), F.col(a_row['nombre'])).alias(a_row['nombre'])
    for _, a_row in sap_cols_fmt.iterrows()]

golden_2 = (golden_1.select(*types_select)
    .select(*str_select, *dbl_select, *date_select) )  #  , *int_select
#    .select(*fxd_width_select))
#    .select(F.concat(*sap_cols_fmt['nombre']).alias('one_mega_string')))
#    .select(*sap_cols_fmt['nombre'], F.concat(*sap_cols_fmt['nombre']).alias('one_mega_string')))

display(golden_2)
