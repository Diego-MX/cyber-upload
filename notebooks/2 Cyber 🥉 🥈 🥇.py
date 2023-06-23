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
from datetime import datetime as dt, date
from functools import reduce
import numpy as np
import pandas as pd
from pandas import DataFrame as pd_DF
from pathlib import Path
from pyspark.sql import (functions as F, types as T, 
    Window as W, Column as C, DataFrame as spk_DF)
from pytz import timezone
import re
from typing import Union

# COMMAND ----------

from importlib import reload
import config; reload(config)

from config import ConfigEnviron, ENV, SERVER, DBKS_TABLES
from src.platform_resources import AzureResourcer

tables      = DBKS_TABLES[ENV]['items']
app_environ = ConfigEnviron(ENV, SERVER, spark)
az_manager  = AzureResourcer(app_environ)

at_storage = az_manager.get_storage()
az_manager.set_dbks_permissions(at_storage)

gold_dir   = f"abfss://gold@{at_storage}.dfs.core.windows.net/cx/collections/cyber"
specs_path = "cx/collections/cyber/spec_files"
tmp_downer = "/dbfs/FileStore/cyber/specs"

cyber_names = {
    'sap_saldos'    : ('C8BD1374',  'core_balance' ), 
    'sap_estatus'   : ('C8BD1343',  'core_status'  ), 
    'sap_pagos'     : ('C8BD1353',  'core_payments'), 
    'fiserv_saldos' : ('C8BD10000', 'cms_balance'  ),
    'fiserv_estatus': ('C8BD10001', 'cms_status'   ), 
    'fiserv_pagos'  : ('C8BD10002', 'cms_payments' ),}

now_mx = dt.now(timezone('America/Mexico_City'))
days_back = 3 if now_mx.weekday() == 0 else 1


# COMMAND ----------

# MAGIC %md 
# MAGIC Algunas instrucciones se ejecutaron una vez, y se tienen que automatizar.  
# MAGIC Principalmente en lo referente a carpetas tanto de reportes como del datalake. 

# COMMAND ----------

@F.pandas_udf(T.StringType())
def udf_toascii(x_str):   
    # IEC-8859-1 es lo más parecido a ANSI que encontramos
    y_str = x_str.str.normalize('NFKD').map(lambda xx: xx.encode('ascii', 'ignore')) 
    # y_str = normalize('NFKD', x_str).encode('ascii', 'ignore').decode('ascii')
    return y_str


def with_columns(a_df, cols_dict): 
    func = lambda dfx, col_kv: dfx.withColumn(*col_kv)
    return reduce(func, cols_dict.items(), a_df)
    
    
def pd_print(a_df: pd.DataFrame, width=180): 
    with pd.option_context('display.max_rows', None, 'display.max_columns', None, 'display.width', width):
        print(a_df)

        


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
# MAGIC ## Transaction Payments

# COMMAND ----------

pymt_codes  = [
    '92703',  '92704',  '92710',  '92711',  '92798',  '92799', 
    '500021', '500022', '500023', '500401', '500412', '500908', 
    '550002', '550021', '550022', 
    '650710', '650712', '650713', '650716'
    '850003', '850005', '950401', '950404']

by_acct_new = W.partitionBy('AccountID').orderBy(F.col('ValueDate').desc())
by_acct_old = W.partitionBy('AccountID').orderBy(F.col('ValueDate'))

pmts_prep = (spark.read.table(tables['brz_txns'][0])
    .filter(F.col('TransactionTypeCode').isin(pymt_codes))
    .filter(F.col('ValueDate') == F.current_date())
    .withColumn('by_acct_new', F.row_number().over(by_acct_new))
    .withColumn('by_acct_old', F.row_number().over(by_acct_old)))

last_pmts = (pmts_prep
    .filter(F.col('by_acct_new') == 1)
    .select('AccountID', 
        F.col('ValueDate').alias('last_date'), 
        F.col('Amount').alias('last_amount'), 
        F.col('AmountAc').alias('last_amount_local')))
                
txns_grpd = (pmts_prep
    .filter(F.col('by_acct_old') == 1)
    .select('AccountID', F.col('ValueDate').alias('first_date')) 
    .join(last_pmts, on='AccountID', how='inner'))

display(pmts_prep)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Loan Contract

# COMMAND ----------

loan_contract_0 = (spark.read.table(tables['brz_loans'][0])
    .filter(F.col('LifeCycleStatusTxt') == 'activos, utilizados')
    .withColumn('today', F.current_date())
    .withColumn('yesterday', F.date_add(F.current_date(), -1))
    .withColumn('status_2', 
        F.when((F.col('LifeCycleStatus').isin([20, 30])) & (F.col('OverDueDays') == 0), 'VIGENTE')
         .when((F.col('LifeCycleStatus').isin([20, 30])) & (F.col('OverDueDays') >  0), 'VENCIDO')
         .when( F.col('LifeCycleStatus') == 50, 'LIQUIDADO')) )

def spk_sapdate(str_col, dt_type): 
    if dt_type == '/Date': 
        dt_col = F.to_timestamp(F.regexp_extract(F.col(str_col), '\d+', 0)/1000)
    elif dt_type == 'ymd': 
        dt_col = F.to_date(F.col(str_col), 'yyyyMMdd')
    return dt_col.alias(str_col)

date_1_cols = ['CreationDateTime', 'LastChangeDateTime']

date_2_cols = ['StartDate', 'CurrentPostingDate', 'EvaluationDate',
    'TermSpecificationStartDate', 'TermSpecificationEndDate',
    'TermAgreementFixingPeriodStartDate', 'TermAgreementFixingPeriodEndDate',
    'PaymentPlanStartDate', 'PaymentPlanEndDate',
    'EffectiveYieldValidityStartDate',
    'EffectiveYieldCalculationPeriodStartDate', 'EffectiveYieldCalculationPeriodEndDate']

dt_1_select = [ F.to_timestamp(F.regexp_extract(a_col, '\d+', 0)/1000).alias(a_col) 
    for a_col in date_1_cols]
dt_2_select = [ F.to_date(F.col(a_col), 'yyyyMMdd').alias(a_col) 
    for a_col in date_2_cols]

new_dates = (loan_contract_0
    .select('ID', *(dt_1_select + dt_2_select)))

loan_contract_nonfix = (loan_contract_0
    .drop(*(date_1_cols + date_2_cols))
    .join(new_dates, on='ID'))

loan_contract = (loan_contract_nonfix
    .withColumn('ContractID', F.col('ID'))
    .withColumn('person_id', F.col('BorrowerID')))

display(loan_contract)

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Person Set 

# COMMAND ----------

# Person Set
states_str = [
      "AGU,1",  "BCN,2",  "BCS,3",  "CAM,4",  "COA,5",  "COL,6",  "CHH,8",  "CHP,7", 
      "CMX,9",  "DUR,10", "GUA,11", "GRO,12", "HID,13", "JAL,14", "MEX,15", "MIC,16", 
      "MOR,17", "NAY,18", "NLE,19", "OAX,20", "PUE,21", "QUE,22", "ROO,23", "SLP,24", 
      "SIN,25", "SON,26", "TAB,27", "TAM,28", "TLA,29", "VER,30", "YUC,31", "ZAC,32"]

states_data = [ {'AddressRegion': each_split[0], 'state_key': each_split[1]} 
    for each_split in map(lambda x: x.split(','), states_str)]

states_df = spark.createDataFrame(states_data)

persons_cols = OrderedDict({    
    'split'     : F.split('LastName', ' ', 2), 
    'LastNameP' : F.col('split').getItem(0),
    'LastNameM' : F.col('split').getItem(1), 
    'full_name1': F.concat_ws(' ', 'FirstName', 'MiddleName', 'LastName', 'LastName2'), 
    'full_name' : F.regexp_replace(F.col('full_name1'), ' +', ' '), 
    'address2'  : F.concat_ws(' ', 'AddressStreet', 'AddressHouseID', 'AddressRoomID')})

persons_0 = (spark.read.table("din_clients.brz_ops_persons_set")
    .join(states_df, how='left', on='AddressRegion'))

persons = with_columns(persons_0, persons_cols)

display(persons)

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Loan Contract QAN

# COMMAND ----------

loans_qan = (spark.read
    .table(tables['brz_loan_analyzers'][0])
    .withColumn('ID', F.col('LoanContractID')))  #Fixer

display(loans_qan)

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Balances

# COMMAND ----------

# Agrupar por ID, Currency, BalancesTS;
# Pivotear por Code: sum(Amount)

balances = (spark.read.table(tables['brz_loan_balances'][0])
    # Set types
    .withColumn('Code', F.concat(F.lit('x'), F.col('Code')))
    .withColumn('Amount', F.col('Amount').cast(T.DoubleType()))
    .withColumn('BalancesTS', F.col('BalancesTS').cast(T.DateType()))
    # Pivot Code/Amount
    .groupBy(['ID', 'Currency', 'BalancesTS']).pivot('Code')
        .agg(F.round(F.sum(F.col('Amount')), 2)))

display(balances)

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Open Items

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

open_cols = OrderedDict({
    'OpenItemTS' : F.to_date(F.col('OpenItemTS'), 'yyyy-MM-dd'), 
    'DueDate'    : F.to_date(F.col('DueDate'),    'yyyyMMdd'),
    'Amount'     : F.col('Amount').cast(T.DoubleType()),
    # Aux Columns
    'cleared_na' : F.regexp_extract('ReceivableDescription', r"Cleared: ([\d\.]+)", 1)
                    .cast(T.DoubleType()), 
    'cleared'    : F.when(F.col('cleared_na').isNull(), 0).otherwise(F.col('cleared_na')), 
    'uncleared'  : F.round(F.col('Amount') - F.col('cleared'), 2), 
    'vencido'    :(F.col('DueDate') < F.col('OpenItemTS'))
                 & F.col('StatusCategory').isin([2, 3]), 
    'local/fgn'  : F.when(F.col('Currency') == 'MXN', 'local').otherwise('foreign'),
    'interes/iva': F.when(F.col('ReceivableType').isin([991100, 511100]), 'interes')
                    .when(F.col('ReceivableType') == 990004, 'iva'), 
    'chk_vencido':(F.col('vencido') & (F.col('uncleared') >0))
                |(~F.col('vencido') & (F.col('uncleared')==0))
})

open_items_0 = spark.read.table(tables['brz_loan_open_items'][0])

uncleared = (with_columns(open_items_0, open_cols)
    .filter(F.col('vencido') & F.col('interes/iva').isNotNull())
    .withColumn('pivoter', F.concat_ws('_', 'interes/iva', 'local/fgn'))
    .groupBy(['OpenItemTS', 'ContractID']).pivot('pivoter')
        .agg(F.round(F.sum(F.col('Uncleared')), 2))
    .withColumn('ID', F.col('ContractID')))

# Display for experimentation. 
display(uncleared)


# COMMAND ----------

# MAGIC %md 
# MAGIC # Preparación Gold 🥇 

# COMMAND ----------

tables_dict = {
    "PersonSet"    : persons, 
    "ContractSet"  : loan_contract, 
    "Lacqan"       : loans_qan,
    "BalancesWide" : balances,
    "OpenItemsUncleared" : uncleared, 
    "TxnsGrouped"  : txns_grpd, 
    "TxnsPayments" : pmts_prep}

null_values = {
    'str' : '', 
    'int' : 0, 
    'dbl' : 0, 
    'date': '1900-01-01'}  # Posteriormente cambia a '0000-00-00'

cast_types = {
    'str' : T.StringType,   # ''
    'int' : T.IntegerType,  # 0
    'dbl' : T.DoubleType,   # 0
    'date': T.DateType}     # date(1900, 1, 1)

c_formats = {
    'dbl': '%0{}.{}f', 'int': '%0{}d', 'dec' : '%0{}.{}d',  # Puede ser '%0{}.{}f'
    'str': '%-{}.{}s', 'date': '%8.8d', 'long': '%0{}d'}


def column_call(col: C) -> str: 
    reg_alias = r"Column\<'(.*) AS (.*)'\>"
    non_alias = r"Column\<'(.*)'\>"
    a_match = re.match(reg_alias, str(col))
    n_match = re.match(non_alias, str(col))
    
    the_call = (a_match.group(2) if a_match is not None 
           else n_match.group(1)) 
    return the_call
 

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tabla de instrucciones
# MAGIC
# MAGIC Leemos los archivos `specs` y `joins` que se compilaron a partir de las definiciones en Excel.  
# MAGIC Y de ahí, se preparan los archivos. 

# COMMAND ----------

def is_na(val: Union[str, float]): 
    if isinstance(val, str): 
        is_it = (val is None)
    elif isinstance(val, float): 
        is_it = np.isnan(val)
    return is_it
        

def read_cyber_specs(task_key: str): 
    # Usa TMP_DOWNER, SPECS_PATH, 
    specs_file = f"{tmp_downer}/{task_key}.feather"
    joins_file = f"{tmp_downer}/{task_key}_joins.csv"
    specs_blob = f"{specs_path}/{task_key}_specs_latest.feather"
    joins_blob = f"{specs_path}/{task_key}_joins_latest.csv"
    
    az_manager.download_storage_blob(specs_file, specs_blob, 'gold', verbose=1)
    az_manager.download_storage_blob(joins_file, joins_blob, 'gold', verbose=1)
    
    specs_0 = (pd.read_feather(specs_file)
        .set_index('nombre'))

    specs_df = specs_0.assign(
        width       = specs_0['Longitud'].astype(float).astype(int), 
        width_1     = lambda df: df['width'].where(df['PyType'] != 'dbl', df['width'] + 1),
        precision_1 = specs_0['Longitud'].str.split('.').str[1], 
        precision   = lambda df: np.where(df['PyType'] == 'dbl', 
                                 df['precision_1'], df['width']), 
        is_na = ( specs_0['columna_valor'].isnull() 
                | specs_0['columna_valor'].isna() 
                |(specs_0['columna_valor'] == 'N/A').isin([True])),
        x_format = lambda df: [
            c_formats[rr['PyType']].format(rr['width_1'], rr['precision']) 
            for _, rr in df.iterrows()], 
        y_format = lambda df: df['x_format'].str.replace(r'\.\d*', '', regex=True),
        c_format = lambda df: df['y_format'].where(df['PyType'] == 'int', df['x_format']), 
        s_format = lambda df: ["%{}.{}s".format(wth, wth) for wth in df['width']])

    if Path(joins_file).is_file(): 
        join_df = (pd.read_csv(joins_file)
            .set_index('tabla'))
        
        joins_dict = OrderedDict()
        for tabla, rr in join_df.iterrows():
            if not is_na(rr['join_cols']):
                # OldCol1=new_col_1,OldCol2=new_col_2,...
                joiners_0 = rr['join_cols'].split(',')
                joiners_1 = (jj.split('=') for jj in joiners_0)
                joiners_2 = [F.col(j0).alias(j1) for j0, j1 in joiners_1]
                joins_dict[tabla] = joiners_2
    else: 
        joins_dict = None
        
    return specs_df, joins_dict

# COMMAND ----------

# Usa TABLES_0, NULL_VALUES, CAST_TYPES. 
def get_reader_specs(specs_df: pd_DF) -> dict: 
    readers  = defaultdict(list)
    missing  = defaultdict(list)
    fix_vals = []
    for name, rr in specs_df.iterrows(): 
        # Reading and Missing
        r_type = rr['PyType']
        if rr['tabla'] in tables_dict: 
            if rr['columna_valor'] in tables_dict[rr['tabla']].columns: 
                call_as = F.col(rr['columna_valor']).alias(name)
                readers[rr['tabla']].append(call_as)
            else: 
                # Fixing missing columns as NA. 
                missing[rr['tabla']].append(rr['columna_valor'])
                r_value = null_values[r_type]
                r_col = F.lit(r_value).cast(cast_types[r_type]()).alias(name)
                fix_vals.append(r_col)
        else: 
            if rr['is_na']: 
                r_value = null_values[r_type]
            else: 
                r_value = rr['columna_valor']
            fix_vals.append(F.lit(r_value).cast(cast_types[r_type]()).alias(name))

    result_specs = {
        'readers' : readers, 
        'missing' : missing, 
        'fix_vals': fix_vals}
    return result_specs

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


def _col_string_format(name, col_type, c_format, s_format): 
    if   col_type == 'str': 
        format_1 = F.format_string(c_format, F.col(name))
    elif col_type == 'date': 
        format_1 = F.when(F.col(name) == date(1900, 1, 1), F.lit('00000000')
                   ).otherwise(F.date_format(F.col(name), 'MMddyyyy'))
    elif col_type == 'dbl': 
        format_1 = F.regexp_replace(
            F.format_string(c_format, F.col(name)), '[\.,]', '')
    elif col_type == 'int': 
        format_1 = F.format_string(str(c_format), name)
    
    format_2 = F.format_string(s_format, format_1).alias(name)
    return format_2


def master_join_specs(spec_joins, specs_dict): 
    readers = specs_dict['readers']
    
    joiner = iter(spec_joins)
    key_0  = next(joiner)
    main_tbl = (tables_dict[key_0]
        .select(*readers[key_0], *spec_joins[key_0]))
    
    n_key = next(joiner, None)
    while n_key is not None: 
        next_tbl = (tables_dict[n_key]
            .select(*readers[n_key], *spec_joins[n_key]))
        main_tbl = main_tbl.join(next_tbl, how='left', 
            on=[column_call(col) for col in spec_joins[n_key]])
        n_key = next(joiner, None)
    
    master_tbl = main_tbl.select('*', *specs_dict['fix_vals'])
    return master_tbl


def gold_to_fixed_width(gold_df: spk_DF, specs_df: pd_DF) -> spk_DF: 
    # Coercing Nulls. 
    _date_col = (lambda nm:
        F.when(F.col(nm).isNull(), date(1900, 1, 1)).otherwise(F.col(nm)))
    date_cols = { name: _date_col(name)
        for name in specs_df.index[specs_df['PyType'] == 'date']}
    
    str_cols = specs_df.index[specs_df['PyType'] == 'str' ].tolist()
    num_cols = specs_df.index[specs_df['PyType'].isin(['dbl', 'int'])].tolist()
    typ_cols = [ 
        F.col(name).cast(cast_types[rr['PyType']]()).alias(name) 
        for name, rr in specs_df.iterrows()]
    fxw_cols  = [ 
        _col_string_format(name, rr['PyType'], rr['c_format'], rr['s_format'])
        for name, rr in specs_df.iterrows()]
    
    fixed_0 = (gold_df
        .fillna('', str_cols)
        .fillna(0,  num_cols)
        .select(*typ_cols))
    
    fixed_1 = (with_columns(fixed_0, date_cols)
        .select(*fxw_cols))
    return fixed_1


# COMMAND ----------

# Uses GOLD_DIR, CYBER_NAMES and calls NOW()

def save_as_cyber(gold_df, cyber_task, explore): 
    time_format = '%Y-%m-%d_%H%M' if explore else '%Y-%m-%d_0000' # 
    file_header = 'true' if explore else 'false'
    
    now_time = dt.now(tz=timezone('America/Mexico_City')).strftime(time_format)
    
    cyber_key, cyber_name = cyber_names[cyber_task]
    report_dir = f"{gold_dir}/{cyber_name}/_spark/{now_time}"
    report_recent = f"{gold_dir}/recent/{cyber_key}.txt"
    report_history = f"{gold_dir}/history/{cyber_name}/{cyber_key}_{now_time}.txt"
    
    print(f"{now_time}_{cyber_key}_{cyber_task}.txt")
    save_as_file(gold_df, report_dir, report_recent)
    save_as_file(gold_df, report_dir, report_history)
    


# COMMAND ----------

# MAGIC %md
# MAGIC ## Execution

# COMMAND ----------

cyber_tasks = ['sap_saldos', 'sap_pagos', 'sap_estatus']
explore = False

for task in cyber_tasks: 
    specs_df, spec_joins = read_cyber_specs(task)
    specs_dict = get_reader_specs(specs_df)

    gold_3 = master_join_specs(spec_joins, specs_dict)
    gold_2 = gold_to_fixed_width(gold_3, specs_df)
    one_select = F.concat(*gold_2.columns).alias('|'.join(gold_2.columns))
    gold_1 = gold_2.select(one_select)
    save_as_cyber(gold_1, task, explore)
