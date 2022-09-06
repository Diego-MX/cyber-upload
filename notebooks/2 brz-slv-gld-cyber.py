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

def pd_print(a_df: pd.DataFrame, width=180): 
    with pd.option_context('display.max_rows', None, 'display.max_columns', None, 'display.width', width):
        print(a_df)


# COMMAND ----------

# MAGIC %md 
# MAGIC # Modificaciones Silver 🥈

# COMMAND ----------

# MAGIC %md
# MAGIC ## Solo 4 requieren modificación  
# MAGIC 
# MAGIC * `Loans Contract`:  es para filtrar
# MAGIC * `Person Set`: tiene una modificación de dirección
# MAGIC * `Balances`, `Open Items`: sí se tiene que analizar a fondo

# COMMAND ----------

# MAGIC %md
# MAGIC ### Loan Contract (Contract Set)

# COMMAND ----------

loan_contract = (spark.read.table(tables['brz_loans'])
    .filter(F.col('LifeCycleStatusTxt') == 'activos, utilizados'))
display(loan_contract)

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

persons_name    = "bronze.persons_set"
loans_name      = "bronze.loan_contracts"    
lqan_name       = "bronze.loan_qan_contracts"
balances_name   = "bronze.loan_balances"     
opens_name      = "bronze.loan_open_items"   

tables_dict = {
    "bronze.persons_set"        : persons, 
    "bronze.loan_contracts"     : loan_contract, 
    "bronze.loan_qan_contracts" : spark.read.table("bronze.loan_qan_contracts"),
    "bronze.loan_balances"      : balances,
    "bronze.loan_open_items"    : uncleared}


# COMMAND ----------

# MAGIC %md 
# MAGIC ### Tablas individuales

# COMMAND ----------

the_sap_tbls = pd.read_feather("../refs/catalogs/cyber_sap.feather")
the_sap_tbls

# COMMAND ----------

# Only on SAP_COLS

# joins_dict = {a_row['tabla_origen']: dict(a_row['join_cols'].split(',')) 
#               for _, a_row in the_sap_tbls.iterrows()}

persons_select  = [F.col(a_row['la_columna']).alias(a_row['nombre']) 
        for _, a_row in sap_cols.iterrows() if a_row['tabla_origen'] == 'bronze.persons_set']
loans_select    = [F.col(a_row['la_columna']).alias(a_row['nombre']) 
        for _, a_row in sap_cols.iterrows() if a_row['tabla_origen'] ==  'bronze.loan_contracts']
lqan_select     = [F.col(a_row['la_columna']).alias(a_row['nombre']) 
        for _, a_row in sap_cols.iterrows() if a_row['tabla_origen'] == 'bronze.loan_qan_contracts']
balances_select = [F.col(a_row['la_columna']).alias(a_row['nombre']) 
        for _, a_row in sap_cols.iterrows() if a_row['tabla_origen'] == 'bronze.loan_balances']
opens_select    = [F.col(a_row['la_columna']).alias(a_row['nombre']) 
        for _, a_row in sap_cols.iterrows() if a_row['tabla_origen'] ==  'bronze.loan_open_items']

# With Join columns. 
persons_tbl  = tables_dict['bronze.persons_set'].select(*persons_select, F.col('ID').alias('person_id'))
loans_tbl    = tables_dict['bronze.loan_contracts'].select(*loans_select, F.col('ID').alias('loan_id'), F.col('BorrowerID').alias('person_id'))
lqan_tbl     = tables_dict['bronze.loan_qan_contracts'].select(*lqan_select, F.col('ID').alias('loan_id'))
balances_tbl = tables_dict['bronze.loan_balances'].select(*balances_select, F.col('ID').alias('loan_id'))
opens_tbl    = tables_dict['bronze.loan_open_items'].select(*opens_select, F.col('ContractID').alias('loan_id'))



# COMMAND ----------

# MAGIC %md
# MAGIC ### Tabla de instrucciones

# COMMAND ----------

all_cols_1 = (pd.read_feather("../refs/catalogs/cyber_columns.feather")
    .drop(columns=['index']))

valor_fijo = all_cols_1['valor_fijo']
campo      = all_cols_1['campo']
tipo_calc  = all_cols_1['tipo_calc']
ref_col    = all_cols_1['ref_col']

all_cols_0 = (all_cols_1
    .assign(**{
        'tabla_origen': all_cols_1['tabla_origen'].fillna(''), 
        'valor_fijo': np.where((valor_fijo == 'N/A') | valor_fijo.isnull(), None, valor_fijo), 
        'la_columna': np.where(tipo_calc == 1, campo, 
                    np.where(tipo_calc == 2, ref_col.str.extract(r"\(.*, (.*)\)", expand=False), 
                  np.where(tipo_calc == 3, None, None)))}))

all_cols_0['in_sap'] = False
for name in tables_dict: 
    sub_idx = (all_cols_1['tabla_origen'] == name).isin([True])
    all_cols_0.loc[sub_idx, 'in_sap'] = \
            all_cols_0['la_columna'][sub_idx].isin(tables_dict[name].columns)

all_cols = all_cols_0.assign(**{
    'spk_type' : np.where(all_cols_0['Tipo de dato'] == 'VARCHAR2', 'str', 
               np.where(all_cols_0['Tipo de dato'] == 'DATE', 'date', 
             np.where(all_cols_0['decimales'] > 0, 'dbl', 'int')))})

sap_cols = all_cols[all_cols['in_sap']]

# [ 'nombre', 'Posición inicial', 'longitud2', 'decimales', 'valor_fijo',
#   'Tipo de dato', 'tabla_origen', 'campo', 'calc', 'tipo_calc', 'ref_col']
# += ['la_columna', 'in_sap'] 
cols_1 = ['nombre', 'tipo_calc', 'Tipo de dato', 'valor_fijo', 'la_columna', 'in_sap', 'spk_type'] 
with pd.option_context('display.max_rows', None, 'display.max_columns', None, 'display.width', 220):
    print(all_cols.loc[all_cols['tipo_calc'] == 3, cols_1])
    


# COMMAND ----------

# MAGIC %md 
# MAGIC ### Master Join and Fixed-Value Columns

# COMMAND ----------

cast_types = {
    'str' : T.StringType(), 
    'int' : T.IntegerType(), 
    'dbl' : T.DoubleType(), 
    'date': T.DateType()
}
# VALOR_FIJO tiene 'N/A' != None. 

fixed_slct = [ F.lit(a_row['valor_fijo'])
                .cast(cast_types[a_row['spk_type']])
                .alias(a_row['nombre'])
    for _, a_row in all_cols[all_cols['tipo_calc'] == 3].iterrows()]

missing_cols = [F.lit(None).alias(a_row['nombre'])
    for _, a_row in all_cols[all_cols['tipo_calc'] == 4].iterrows()]

missing_sap = [F.lit(None).alias(a_row['nombre'])
    for _, a_row in all_cols[all_cols['tipo_calc'].isin([1, 2]) & ~all_cols['in_sap']].iterrows()]

null_as_zeros = [a_row['nombre'] for _, a_row in all_cols.iterrows() 
            if (a_row['spk_type'] in ['dbl', 'int'])]
null_as_blank = [a_row['nombre'] for _, a_row in all_cols.iterrows() 
            if (a_row['spk_type'] in ['date', 'str'])]


golden_1 = (persons_tbl
    .join(loans_tbl   , how='right', on='person_id')
    .join(lqan_tbl    , how='left' , on='loan_id')
    .join(balances_tbl, how='left' , on='loan_id') 
    .join(opens_tbl   , how='left' , on='loan_id')
    .drop(*['person_id', 'loan_id'])
    .select('*', *missing_cols, *missing_sap, *fixed_slct)
    .fillna(0, subset=null_as_zeros)
    .fillna('', subset=null_as_blank))

display(golden_1)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Explicit conversion to string

# COMMAND ----------

longitud_f = (all_cols['longitud2'].astype('float') 
            + all_cols['decimales']/10).astype(str)
longitud_i =  all_cols['longitud2'].astype(str)

pre_format = np.where(all_cols['spk_type'] == 'str', longitud_i + 's', 
             np.where(all_cols['spk_type'] == 'date',longitud_i + 's', 
             np.where(all_cols['spk_type'] == 'dbl', longitud_f + 'f', '0' + longitud_i + 'd')))

all_cols_fmt = all_cols.assign(**{
    'f_string' : '%' + pre_format,
    's_string' : '%' + longitud_i + 's'}) 

# [ 'nombre', 'Posición inicial', 'longitud2', 'decimales', 'valor_fijo',
#   'Tipo de dato', 'tabla_origen', 'campo', 'calc', 'tipo_calc', 'ref_col',
#   'la_columna', 'in_sap', 'spk_type']
# ... 'f_string', 's_string']
fmt_cols = ['nombre', 'Posición inicial', 'longitud2', 'decimales', 'spk_type', 'Tipo de dato', 'f_string', 's_string']
with pd.option_context('display.max_rows', None, 'display.max_columns', None, 'display.width', 220):
    print(all_cols_fmt.loc[all_cols_fmt['Tipo de dato'] == 'NUMBER', fmt_cols])
    

# COMMAND ----------

# The types:  
# 1. Let's cast to type first ;) 
# 2. Each type is converted to string in its terms. 
# 3. Apply fixed width condition ...
# 4. Select columns in order. 


## 1. 
types_select = [
    F.col(a_row['nombre']).cast(cast_types[a_row['spk_type']]).alias(a_row['nombre'])
        for _, a_row in all_cols_fmt.iterrows()]
## 2. 
str_select = [F.col(a_row['nombre']) 
    for _, a_row in all_cols_fmt.iterrows() if a_row['spk_type'] == 'str']

date_select = [F.date_format(a_row['nombre'], 'MMddyyyy').alias(a_row['nombre']) 
    for _, a_row in all_cols_fmt.iterrows() if a_row['spk_type'] == 'date']

dbl_select = [F.format_number(a_row['nombre'], int(a_row['decimales'])).alias(a_row['nombre']) 
    for _, a_row in all_cols_fmt.iterrows() if a_row['spk_type'] == 'dbl']

# int_select = [F.format_string(str(a_row['f_string']), a_row['nombre']).alias(a_row['nombre'])
#     for _, a_row in sap_cols_fmt.iterrows() if a_row['spk_type'] == 'int']

int_select = [F.format_string(str(a_row['f_string']), a_row['nombre']).alias(a_row['nombre'])
    for _, a_row in all_cols_fmt.iterrows() if a_row['spk_type'] == 'int']

## 3. 
fxd_width_select = [F.format_string(str(a_row['s_string']), F.col(a_row['nombre'])).alias(a_row['nombre'])
    for _, a_row in all_cols_fmt.iterrows()]

golden_2 = (golden_1.select(*types_select)
    .select(*int_select, *str_select, *dbl_select, *date_select)   
    .select(*fxd_width_select)
    .select(F.concat(*sap_cols_fmt['nombre']).alias('one_mega_string'))
#    .select(*sap_cols_fmt['nombre'], F.concat(*sap_cols_fmt['nombre']).alias('one_mega_string'))
)

display(golden_2)
