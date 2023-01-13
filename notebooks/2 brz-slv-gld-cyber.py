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

from base64 import b64encode
from datetime import datetime as dt, date
from functools import reduce
import numpy as np
import pandas as pd
from pandas.core.frame import DataFrame as pd_DF
from pyspark.sql import (functions as F, types as T, Window as W)
from pyspark.sql.dataframe import DataFrame as spk_DF
from pytz import timezone
from typing import Tuple
from unicodedata import normalize

from src.platform_resources import AzureResourcer
from config import ConfigEnviron, ENV, SERVER, DBKS_TABLES

tables = DBKS_TABLES[ENV]['items']
app_environ = ConfigEnviron(ENV, SERVER, spark)
az_manager  = AzureResourcer(app_environ)

at_storage = az_manager.get_storage()
az_manager.set_dbks_permissions(at_storage)

# Sustituye el placeholder AT_STORAGE, aunque mantiene STAGE para sustituirse después. 
base_location = f"abfss://gold@lakehylia.dfs.core.windows.net/cx/collections/cyber"

cyber_terminology = {
    'core_balance' : 'C8BD1374', 'cms_balance'  : 'C8BD10000',
    'core_status'  : 'C8BD1343', 'cms_status'   : 'C8BD10001', 
    'core_payment' : 'C8BD1353', 'cms_payment'  : 'C8BD10002'}


# COMMAND ----------

@F.pandas_udf(T.StringType())
def udf_toascii(x_str):   # IEC-8859-1 es lo más parecido a ANSI que encontramos
    y_str = x_str.str.normalize('NFKD').map(lambda xx: xx.encode('ascii', 'ignore')) 
    # y_str = normalize('NFKD', x_str).encode('ascii', 'ignore').decode('ascii')
    return y_str


def with_columns(a_df, cols_dict): 
    func = lambda df1, col_kv: df1.withColumn(*col_kv)
    return reduce(func, cols_dict.items(), a_df)
    
    
def pd_print(a_df: pd.DataFrame, width=180): 
    with pd.option_context('display.max_rows', None, 'display.max_columns', None, 'display.width', width):
        print(a_df)

        
def save_as_file(a_df: DF.DataFrame, path_dir, path_file, **kwargs):
    std_args = {
        'mode': 'overwrite', 
        'header': 'false'}
    std_args.update(kwargs)
    
    (a_df.coalesce(1).write
         .mode(std_args['mode'])
         .option('header', std_args['header'])
         .text(path_dir))
    
    f_extras = [filish for filish in dbutils.fs.ls(path_dir) 
            if filish.name.startswith('part-')]
    
    if len(f_extras) != 1: 
        raise "Expect only one file starting with 'PART'"
    
    dbutils.fs.mv(f_extras[0].path, path_file)

# COMMAND ----------

# MAGIC %md 
# MAGIC # Modificaciones Silver 🥈

# COMMAND ----------

# MAGIC %md
# MAGIC ## Solo 4 requieren modificación  
# MAGIC 
# MAGIC * `Loans Contract`:  se filtran los prestamos, modifican fechas, y agregan algunas columnas auxiliares.  
# MAGIC * `Person Set`: tiene algunas modificaciones personalizadas.
# MAGIC * `Balances`, `Open Items`: sí se tienen que abordar a fondo. 

# COMMAND ----------

# MAGIC %md
# MAGIC ### Transaction Payments

# COMMAND ----------

pymt_codes  = ['92703', '92704', '92709', '92710', '92711', '92798', '92799', 
    '500021', '500022', '500023', '500401', '500412', '500908', 
    '550002', '550021', '550022', '950401', '950404']

by_acct_new = W.partitionBy('AccountID').orderBy(F.col('ValueDate').desc())
by_acct_old = W.partitionBy('AccountID').orderBy(F.col('ValueDate'))

pmts_prep = (spark.read.table('farore_transactions.brz_ops_transactions_set')
    .filter(F.col('TransactionTypeCode').isin(pymt_codes))
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

display(txns_grpd)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Loan Contract

# COMMAND ----------

loan_contract_0 = (spark.read.table(tables['brz_loans'][0])
    .filter(F.col('LifeCycleStatusTxt') == 'activos, utilizados')
    .withColumn('yesterday', F.date_add(F.current_date(), -1))
    .withColumn('status_2', F.when((F.col('LifeCycleStatus').isin([20, 30])) & (F.col('OverDueDays') == 0), 'VIGENTE')
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

loan_contract = (loan_contract_0
    .drop(*(date_1_cols + date_2_cols))
    .join(new_dates, on='ID'))

display(loan_contract)

# COMMAND ----------

non_strings = [a_col for a_col in loan_contract.dtypes if a_col[1] != 'string']
non_strings
loan_non_strings = loan_contract.select(*[non_str[0] for non_str in non_strings])
display(loan_non_strings)

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Person Set 

# COMMAND ----------

# Person Set
states_str = ["AGU,1",  "BCN,2",  "BCS,3",  "CAM,4",  "COA,5",  "COL,6",  "CHH,8",  "CHP,7", 
              "CMX,9",  "DUR,10", "GUA,11", "GRO,12", "HID,13", "JAL,14", "MEX,15", "MIC,16", 
              "MOR,17", "NAY,18", "NLE,19", "OAX,20", "PUE,21", "QUE,22", "ROO,23", "SLP,24", 
              "SIN,25", "SON,26", "TAB,27", "TAM,28", "TLA,29", "VER,30", "YUC,31", "ZAC,32"]

states_data = [ {'AddressRegion': each_split[0], 'state_key': each_split[1]} 
    for each_split in map(lambda x: x.split(','), states_str)]

states_df = spark.createDataFrame(states_data)

persons = (spark.read.table("din_clients.brz_ops_persons_set")
    .withColumn('split'    , F.split('LastName', ' ', 2))
    .withColumn('LastNameP', F.col('split').getItem(0))
    .withColumn('LastNameM', F.col('split').getItem(1))
    .withColumn('full_name1',F.concat_ws(' ', 'FirstName', 'MiddleName', 'LastName', 'LastName2'))
    .withColumn('full_name' ,F.regexp_replace(F.col('full_name1'), ' +', ' '))
    .withColumn('address2',  F.concat_ws(' ', 'AddressStreet', 'AddressHouseID', 'AddressRoomID'))
    .join(states_df, how='left', on='AddressRegion'))

display(persons)

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Loan Contract QAN

# COMMAND ----------

loans_qan = (spark.read.table("bronze.loan_qan_contracts"))

display(loans_qan)

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

open_items = (spark.read.table("nayru_accounts.brz_ops_loan_open_items")
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
# MAGIC Algunas variables auxiliares:

# COMMAND ----------

tables_dict = {
    "PersonSet"    : persons, 
    "ContractSet"  : loan_contract, 
    "Lacqan"       : loans_qan,
    "BalancesWide" : balances,
    "OpenItemsUncleared" : uncleared, 
    "TxnsGrouped"  : txns_grpd}


# COMMAND ----------

# MAGIC %md 
# MAGIC Esta tabla de tablas no se usa mucho. 

# COMMAND ----------

the_sap_tbls = pd.read_feather("../refs/catalogs/cyber_sap.feather")

join_dict = {}
for _, t_row in the_sap_tbls.iterrows(): 
    if t_row['join_cols']: 
        the_cols = t_row['join_cols'].split(',')
        t_dict = dict(key_entry.split('=') for key_entry in the_cols)
        join_dict[t_row['datalake']] = t_dict

join_select = {}
for key, v_dict in join_dict.items(): 
    join_select[key] = [F.col(k).alias(v) for k, v in v_dict.items()] 
    
the_sap_tbls

# COMMAND ----------

# MAGIC %md
# MAGIC ### Tabla de instrucciones
# MAGIC 
# MAGIC Leemos los archivos `feather` que se compilaron a partir de las definiciones en Excel.  
# MAGIC Hacemos la preparación técnica de la tabla correspondiente.  
# MAGIC * `all_cols` es la tabla con las instrucciones técnicas.  
# MAGIC * `sap_cols` es el subconjunto de aquellas que confirmadas (deberían ser todas, pero no) de SAP.  

# COMMAND ----------

# ['nombre', 'Posición inicial', 'Longitud', 'Tipo de dato', 'aux_nombre',
#  'valor_fijo', 'tabla_origen', 'tipo_calc', 'ref_col', 'ref_type', 'ref_format']

all_cols_1 = (pd.read_feather("../refs/catalogs/cyber_columns.feather")
    .drop(columns=['index']))

longitud   = all_cols_1['Longitud'  ]
valor_fijo = all_cols_1['valor_fijo']
tipo_calc  = all_cols_1['tipo_calc' ]
ref_col    = all_cols_1['ref_col'   ]

all_cols_0 = (all_cols_1
    .assign(
        la_columna   = np.where(tipo_calc == 1, ref_col, 
                     np.where(tipo_calc == 2, ref_col.str.extract(r"\(.*, (.*)\)", expand=False), 
                   np.where(tipo_calc == 3, None, None))), 
        valor_fijo   = np.where(valor_fijo.isnull() | valor_fijo.isna() | (valor_fijo == 'N/A'), None, valor_fijo),
        width        = longitud.astype(float).astype(int), 
        width_1      = lambda df: df['width'].where(df['ref_type'] != 'dbl', df['width'] + 1),
        precision_1  = longitud.str.split('.').str[1], 
        precision    = lambda df: np.where(df['ref_type'] == 'dbl', df['precision_1'], df['width']),
        tabla_origen = lambda df: df['tabla_origen'].fillna(''))
    .set_index('nombre'))

all_cols_0['in_sap'] = False
for name in tables_dict:
    tbl_cols = tables_dict[name].columns
    sub_idx  = (all_cols_0['tabla_origen'] == name).isin([True])
    in_sap   = all_cols_0['la_columna'][sub_idx].isin(tbl_cols)
    all_cols_0.loc[sub_idx, 'in_sap'] = in_sap

all_cols = (all_cols_0.
    assign(
        x_format = [each['ref_format'].format(each['width_1'], each['precision']) 
                    for _, each in all_cols_0.iterrows()], 
        y_format = lambda df: df['x_format'].str.replace(r'\.\d*', '', regex=True),
        c_format = lambda df: np.where(df['ref_type'] == 'int', df['y_format'], df['x_format']), 
        s_format = ["%{}.{}s".format(wth, wth) for wth in all_cols_0['width']],
        is_fixed = (all_cols_0['tipo_calc'].isin([3, 4]) | 
                       ~all_cols_0['in_sap']).isin([True])))

sap_cols = all_cols[all_cols['in_sap']]

# ['nombre', 'Posición inicial', 'Longitud', 'Tipo de dato', 'aux_nombre',
#        'valor_fijo', 'tabla_origen', 'tipo_calc', 'ref_col', 'ref_type',
#        'ref_format', 'in_sap']
# += ['la_columna', 'in_sap'] 

some_rows = all_cols['is_fixed']
some_cols = ['tipo_calc', 'in_sap', 'tabla_origen', 'la_columna', 'ref_format', 's_format', 'c_format'] 

pd_print(all_cols.loc[:, some_cols], 180)
# pd_print(all_cols_1.loc[some_rows, some_cols])
    


# COMMAND ----------

# MAGIC %md 
# MAGIC * De `sap_cols` obtenemos las columnas confirmadas de SAP.  
# MAGIC * Creamos tablas intermedias.  

# COMMAND ----------

# Only on SAP_COLS

persons_select  = [F.col(a_row['la_columna']).alias(name) 
        for name, a_row in sap_cols.iterrows() if a_row['tabla_origen'] == 'bronze.persons_set']
loans_select    = [F.col(a_row['la_columna']).alias(name) 
        for name, a_row in sap_cols.iterrows() if a_row['tabla_origen'] == 'bronze.loan_contracts']
lqan_select     = [F.col(a_row['la_columna']).alias(name) 
        for name, a_row in sap_cols.iterrows() if a_row['tabla_origen'] == 'bronze.loan_qan_contracts']
balances_select = [F.col(a_row['la_columna']).alias(name) 
        for name, a_row in sap_cols.iterrows() if a_row['tabla_origen'] == 'bronze.loan_balances']
opens_select    = [F.col(a_row['la_columna']).alias(name) 
        for name, a_row in sap_cols.iterrows() if a_row['tabla_origen'] == 'bronze.loan_open_items']

# With Join columns. 
persons_tbl  = tables_dict['bronze.persons_set'].select(*persons_select, F.col('ID').alias('person_id'))
loans_tbl    = (tables_dict['bronze.loan_contracts']
    .select(*loans_select, F.col('ID').alias('loan_id'), F.col('BorrowerID').alias('person_id')))
lqan_tbl     = tables_dict['bronze.loan_qan_contracts'].select(*lqan_select, F.col('ID').alias('loan_id'))
balances_tbl = tables_dict['bronze.loan_balances'     ].select(*balances_select, F.col('ID').alias('loan_id'))
opens_tbl    = tables_dict['bronze.loan_open_items'   ].select(*opens_select, F.col('ContractID').alias('loan_id'))

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Master Join and Fixed-Value Columns

# COMMAND ----------

# MAGIC %md
# MAGIC 1. Definir tipos de _Spark_, y los valores nulos para cada uno de ellos.  
# MAGIC 2. Crear columnas para los valores fijos definidos.  
# MAGIC 3. Convertir a los tipos definidos en (1).  
# MAGIC 4. Las fechas se manejan por separado.  

# COMMAND ----------

cast_types = {
    'str' : T.StringType(),   # ''
    'int' : T.IntegerType(),  # 0
    'dbl' : T.DoubleType(),   # 0
    'date': T.DateType()}     # date(1900, 1, 1)

null_values = {
    'str' : '', 
    'int' : 0, 
    'dbl' : 0, 
    'date': '1900-01-01'}  # Posteriormente cambia a '0000-00-00'

fixed_values = { name: 
    a_row['valor_fijo'] if a_row['valor_fijo'] else null_values[a_row['ref_type']]
    for name, a_row in all_cols.iterrows() if a_row['is_fixed']}

fixed_select = [ F.lit(fixed_values[name]).cast(cast_types[a_row['ref_type']]).alias(name)
    for name, a_row in all_cols[all_cols['is_fixed']].iterrows()]

types_select = [ F.col(name).cast(cast_types[a_row['ref_type']]).alias(name)
    for name, a_row in all_cols.iterrows()]

dates_dict = { name: F.when(F.col(name).isNull(), date(1900, 1, 1)).otherwise(F.col(name))
    for name, a_row in all_cols.iterrows() if a_row['ref_type'] == 'date'}

golden_0 = (persons_tbl
    .join(loans_tbl   , how='right', on='person_id')
    .join(lqan_tbl    , how='left' , on='loan_id')
    .join(balances_tbl, how='left' , on='loan_id') 
    .join(opens_tbl   , how='left' , on='loan_id') 
    .drop(*['person_id', 'loan_id'])
    .select('*', *fixed_select)
    .fillna('', all_cols.index[all_cols['ref_type'] == 'str' ].tolist())
    .fillna(0,  all_cols.index[all_cols['ref_type'].isin(['dbl', 'int'])].tolist())
    .select(*types_select))

golden = (with_columns(golden_0, dates_dict))

display(golden)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Explicit conversion to string

# COMMAND ----------

# MAGIC %md 
# MAGIC Aplicamos las definiciones anteriores de acuerdo al tipo de columna `str`, `date`, `dbl`, `int`.  
# MAGIC - `STR`: Aplicar formato `c_format`, y dejar ASCII.   
# MAGIC - `DATE`: Convertir los `1900-1-1` capturados previamente y aplicar `date-format`.  
# MAGIC - `DBL`: Aplicar `c_format` y quitar decimal.  
# MAGIC - `INT`: Aplicar `c_format`.  
# MAGIC 
# MAGIC Post-formatos, aplicar el `s-format`, concatenar.  

# COMMAND ----------

str_select  = [F.format_string(a_row['c_format'], F.col(name)).alias(name)
    for name, a_row in all_cols[all_cols['ref_type'] == 'str'].iterrows()]

date_select = [F.when(F.col(name) == date(1900, 1, 1), F.lit('00000000'))
        .otherwise(F.date_format(F.col(name), 'MMddyyyy')).alias(name)
    for name in all_cols.index[all_cols['ref_type'] == 'date']]

dbl_select  = [F.regexp_replace(
            F.format_string(a_row['c_format'], name), '[\.,]', '').alias(name) 
    for name, a_row in all_cols.iterrows() if a_row['ref_type'] == 'dbl']

int_select = [F.format_string(str(a_row['c_format']), name).alias(name)
    for name, a_row in all_cols.iterrows() if a_row['ref_type'] == 'int']

## 3. 
fxw_select = [F.format_string(a_row['s_format'], F.col(name)).alias(name)
    for name, a_row in all_cols.iterrows()]

pre_fixed = (golden.select(*types_select)
    .select(*str_select, *dbl_select, *date_select, *int_select)  
    .select(*fxw_select))

fxw_core_balances = (pre_fixed
    .select(F.concat(*all_cols.index).alias('|'.join(all_cols['aux_nombre'])) ))
        
display(fxw_core_balances)

# COMMAND ----------

display(fxw_core_balances)

# COMMAND ----------

explore = False

if explore: 
    time_format = '%Y-%m-%d_%H-%M'
    file_header = 'true'
else: 
    time_format = '%Y-%m-%d_00-00'
    file_header = 'false'
    
now_time = dt.now(tz=timezone('America/Mexico_City')).strftime(time_format)

report1_pre = f"{base_location}/spark/core_balance/{now_time}"
report1_pos = f"{base_location}/core_balance/{now_time}.txt"

save_as_file(fxw_core_balances, report1_pre, report1_pos)
