# Databricks notebook source
# MAGIC %md 
# MAGIC # Descripción
# MAGIC Este _notebook_ fue escrito originalmente por Jacobo.  
# MAGIC Para llevarlo de DEV a QAs, le hice (Diego) algunas factorizaciones:  
# MAGIC - Indicar tablas a partir de un diccionario en `CONFIG.PY`.  
# MAGIC - Agrupar el código por celdas de acuerdo a las tablas que se procesan.
# MAGIC - Encadenar las instrucciones de las tablas en una sola, cuando es posible. 

# COMMAND ----------

# MAGIC %pip install -r ../reqs_dbks.txt

# COMMAND ----------

from datetime import datetime as dt
from delta.tables import DeltaTable as Δ
from pyspark.sql import (functions as F, SparkSession, types as T, Window as W)

from config import ConfigEnviron, ENV, SERVER, DBKS_TABLES
from src.platform_resources import AzureResourcer

spark = SparkSession.builder.getOrCreate()

tbl_items = DBKS_TABLES[ENV]['items']

app_environ = ConfigEnviron(ENV, SERVER, spark)
az_manager  = AzureResourcer(app_environ)

at_storage = az_manager.get_storage()
az_manager.set_dbks_permissions(at_storage)

# Sustituye el placeholder AT_STORAGE, aunque mantiene STAGE para sustituirse después. 
base_location = DBKS_TABLES[ENV]['base']
pms_location = DBKS_TABLES[ENV]['promises']


# COMMAND ----------

def spk_sapdate(str_col, dt_type): 
    if dt_type == '/Date': 
        dt_col = F.to_timestamp(F.regexp_extract(F.col(str_col), '\d+', 0)/1000)    #pylint: disable=anomalous-backslash-in-string
    elif dt_type == 'ymd': 
        dt_col = F.to_date(F.col(str_col), 'yyyyMMdd')
    return dt_col.alias(str_col)


def segregate_lastnames(s, pos):
    if len(s.split(' ')) > 1:
        return s.split(' ')[pos]
    
    if pos == 1:
        return ''
    else:
        return s
    

def date_format(s):
    if s != '':
        the_date = dt.strptime(s, '%Y%m%d').date()
    else:
        the_date = dt.strptime('20000101', '%Y%m%d').date()
    return the_date

    
def write_dataframe(spk_df, tbl_loc, write_mode='overwrite'): 
    (spk_df.write.mode(write_mode)
        .option('overwriteSchema', True)
        .format('delta')
        .save(tbl_loc))
    return None
      
    
segregate_udf   = F.udf(segregate_lastnames, T.StringType())
date_format_udf = F.udf(date_format, T.DateType())



# COMMAND ----------

# MAGIC %md 
# MAGIC # Bronze 🥉 to Silver 🥈

# COMMAND ----------

# MAGIC %md
# MAGIC The flow is as follows: 
# MAGIC 1. Setup Persons Set.  
# MAGIC 2. Continue with Loans (Contracts), and then:  
# MAGIC    2.a) Balances  
# MAGIC    2.b) Open Items  
# MAGIC    2.c) Payment Plans  
# MAGIC 3. Write the tables  

# COMMAND ----------

abfss_brz = base_location.format(stage='bronze', storage=at_storage)
abfss_slv = base_location.format(stage='silver', storage=at_storage)
abfss_gld = base_location.format(stage='gold', storage=at_storage)
at_promise = pms_location.format(stage='silver', storage=at_storage) 

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Person Set

# COMMAND ----------

# Person Set
person_cols = ['FirstName', 'LastName', 'LastName2', 
    'AddressRegion', 'AddressCity', 'AddressDistrictName', 
    'AddressStreet', 'AddressHouseID', 'AddressPostalCode', 
    'Gender', 'PhoneNumber', 'ID']


person_set_0 = (spark.read.format('delta')
    .load(f"{abfss_brz}/{tbl_items['brz_persons'][1]}")
    .select(*person_cols)
    .withColumnRenamed('PhoneNumber', 'phone_number')
    .withColumn('LastName2', segregate_udf(F.col('LastName'),  F.lit(1)))
    .withColumn('LastName',  segregate_udf(F.col('LastName'),  F.lit(0)))
    .withColumn('phone_number', F.col('phone_number').cast(T.LongType()))
    .dropDuplicates())

slv_persons = f"{abfss_slv}/{tbl_items['slv_persons'][1]}"
if Δ.isDeltaTable(spark, slv_persons): 
    person_silver = spark.read.format('delta').load(slv_persons)

if Δ.isDeltaTable(spark, slv_persons): 
    print("Person Set Delta Table")
    person_slv_0 = spark.read.format('delta').load(slv_persons)

    # Anti Join Bronze - Silver
    person_slv = (person_set_0
        .join(person_slv_0[['ID']], how='left', 
              on = person_set_0['ID']==person_slv_0['ID'] )
        .filter(person_slv_0['ID'].isNull())
        .drop(person_slv_0['ID']))
else: 
    person_slv = person_set_0

    


# COMMAND ----------

# MAGIC %md
# MAGIC ## Loan Contracts

# COMMAND ----------

# Loan Contracts

loan_cols = ['ID', 'BankAccountID', 'InitialLoanAmount',
    'TermSpecificationStartDate', 'ClabeAccount', 
    'RepaymentFrequency', 'TermSpecificationValidityPeriodDurationM',
    'NominalInterestRate', 'BorrowerID', 'OverdueDays', 'LifeCycleStatusTxt',
    'PaymentPlanStartDate']

loan_contract_slv = (spark.read
    .load(f"{abfss_brz}/{tbl_items['brz_loans'][1]}")
    .select(*loan_cols)
    .withColumn('InitialLoanAmount', F.col('InitialLoanAmount').cast(T.DoubleType()))
    .withColumn('TermSpecificationValidityPeriodDurationM', F.col('TermSpecificationValidityPeriodDurationM').cast(T.IntegerType()))
    .withColumn('NominalInterestRate', F.round(F.col('NominalInterestRate').cast(T.DoubleType()), 2))
    .withColumn('TermSpecificationStartDate', spk_sapdate('TermSpecificationStartDate', 'ymd'))
    .withColumn('DaysToPayment', F.datediff(spk_sapdate('PaymentPlanStartDate', 'ymd'), F.current_date()))
    .dropDuplicates())


# COMMAND ----------

# MAGIC %md 
# MAGIC ## Balances

# COMMAND ----------

fixed_cols  = ['ID'] # , 'Currency', 'BalancesTS'

code_cols = {
  'ord_interes'      : F.abs(F.col('3')), 
  'comisiones'       : F.abs(F.col('4')), 
  'monto_principal'  : F.col('5'), 
  'monto_liquidacion': F.abs(F.col('3')) + F.abs(F.col('4')) + F.col('5')}

code_select = [(vv).alias(kk) for kk, vv in code_cols.items()]

# ['ID', 'Code', 'Name', 'Amount', 'Currency', 'BalancesTS']
loan_balance_slv = (spark.read.format('delta')
    .load(f"{abfss_brz}/{tbl_items['brz_loan_balances'][1]}")
    # Set types
    .withColumn('Code', F.col('Code').cast(T.IntegerType()))
    .withColumn('Amount', F.col('Amount').cast(T.DoubleType()))
    .withColumn('BalancesTS', F.col('BalancesTS').cast(T.DateType()))
    # Pivot Code/Amount
    .groupBy(fixed_cols).pivot('Code')
    .agg(F.round(F.sum(F.col('Amount')), 2))
    .select(*fixed_cols, *code_select))


# COMMAND ----------

# MAGIC %md 
# MAGIC ## Open Loans 
# MAGIC 
# MAGIC Manipulamos la tabla Open Items, para la cual tenemos la siguiente información relacionada.  
# MAGIC 
# MAGIC Sobre estatus y categorías:  
# MAGIC 
# MAGIC | Status | StatusTxt | StatusCategory |
# MAGIC |--------|-----------|---|
# MAGIC | 01     | Created   | 1 |
# MAGIC | 01     | Created   | 2 |
# MAGIC | 01     | Created   | 3 |
# MAGIC | 86     | Suprimido | 1 |
# MAGIC 
# MAGIC | ReceivableType | ReceivableTypeTxt |
# MAGIC |--------|-----------|
# MAGIC | 511010 | Capital   |
# MAGIC | 511080 | Reemb.parcial.créd.(esp.)|
# MAGIC | 511100 | Int. Nominal |
# MAGIC | 991100 | Int. No Gravado |
# MAGIC | 990004 | IVA Interés |
# MAGIC | 511200 | Comisión |
# MAGIC | 990006 | IVA. Comisión |
# MAGIC 
# MAGIC Y generamos las variables intermedias: 
# MAGIC * `es_vencido`: `StatusCategory in (2, 3) AND (DueDate < Timestamp)`  
# MAGIC * `recibible` : `511010 => capital`, `(991100, 990004) => impuesto`,  
# MAGIC   `(511200, 990006) => comision`
# MAGIC 
# MAGIC Para finalmente obtener las medidas:  
# MAGIC 
# MAGIC | Medida | recibible | StatusCat. | es_vencido |
# MAGIC |--------|-----------|---|--|
# MAGIC | Parcialidades pagadas | capital   | 1 |  |
# MAGIC | Parcialidades vencidas| capital   | 2 |  |
# MAGIC | Principal vencido     | capital, impuesto | 2, 3 | True |
# MAGIC | Interes ord vencido   | capital, impuesto, comision | 2, 3 | True |

# COMMAND ----------

# ['OpenItemsTS', 'ContractID', 'OpenItemID', 'Status', 'StatusTxt', 'StatusCategory', 'DueDate', 
#  'ReceivableType', 'ReceivableTypeTxt', 'ReceivableDescription', 'Amount', 'Currency']

# Columnas Finales. 
open_items_cols = {
    'parcialidades_pagadas' : F.col('capital_pagado_n'), 
    'parcialidades_vencidas': F.col('capital_vencido_n'), 
    'principal_vencido'     : F.col('capital_vencido_monto'), 
    'interes_ord_vencido'   : F.col('impuesto_vencido_monto'), 
    'monto_vencido'         : F.col('capital_vencido_monto') 
            + F.col('impuesto_vencido_monto') + F.col('comision_vencido_monto')}

pre_open_cols = [{
    'OpenItemsTS'   : F.to_date('OpenItemsTS', 'yyyy-MM-dd'),
    'DueDate'   : F.to_date('DueDate', 'yyyyMMdd'),
    'Amount'    : F.col('Amount').cast(T.DoubleType()),
    'cleared'   : F.regexp_extract('ReceivableDescription', r"Cleared: ([\d\.]+)", 1)
                   .cast(T.DoubleType()).fillna(0, subset=['cleared']),
    'local/fgn' : F.when(F.col('Currency') == 'MXN', 'local')
                   .when(F.col('Currency').isNotNull(), 'foreign')
    }, {
    'uncleared' : F.round(F.col('Amount') - F.col('cleared'), 2),
    'recibible' : F.when(F.col('ReceivableType') == 511010, 'capital')  
                   .when(F.col('ReceivableType').isin([511200, 990006]), 'comision')
                   .when(F.col('ReceivableType').isin([511100, 991100, 990004]), 'impuesto'), 
    'estatus_2' : F.when(F.col('StatusCategory') == 1, 'pagado')
                   .when((F.col('DueDate') < F.col('OpenItemsTS')) 
                        & F.col('StatusCategory').isin([2, 3]), 'vencido')
}]

pre_open_items = (spark.read.format('delta')
    .load(f"{abfss_brz}/{tbl_items['brz_loan_open_items'][1]}")
    .with_column_plus(pre_open_cols))



open_items_slct = [vv.alias(kk) for kk, vv in open_items_cols.items()]

loan_open_slv = (pre_open_items
    .withColumn('pivoter', F.concat_ws('_', 'recibible', 'estatus_2'))
    .groupBy(*['ContractID']).pivot('pivoter')
        .agg(F.round(F.sum(F.col('Uncleared')), 2).alias('monto'), 
             F.countDistinct(F.col('OpenItemID')).alias('n'))
    .select(*['ContractID'], *open_items_slct)
    .fillna(value=0))

display(loan_open_slv)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Payment Plans

# COMMAND ----------

experiment_refactor = False
if experiment_refactor: 
    # Category, TXT
    # 1  Pago regular ***
    # 4  Pago a capital
    # 5  Interés gravado
    # 80 IVA de los intereses
    # 80 Pago a capital
    # 81 Interés exento
    # E  No se creó el plan de pago; no existe ningún acuerdo de pago
    # E  No existe ningún pago
    # E  De fecha 21.09.2022 o A fecha 16.09.2022 no es correcta
    # E  Fecha inicio plan de pagos es posterior a fin de fijación de condiciones
    
    # sig_pago, monto_a_pagar, parcialidades_plan
    
    pymt_select = [
        F.col('ContractID'), 
        F.col('Date').alias('sig_pago'), 
        F.col('Amount').alias('monto_a_pagar'), 
        F.col('parcialidades_plan')
    ]
    
    by_id = W.partitionBy('ContractID')
    by_date = by_id.orderBy(F.asc(F.col('Date')))
    
    
    ## Filter, Cast types, Prepare aux.
    
    # ['ContractID', 'ItemID', 'Date', 'Category', 'CategoryTxt', 'Amount', 'Currency', 'RemainingDebitAmount', 'PaymentPlanTS']
    loan_payment_0 = (spark.read.format('delta').load(f"{abfss_brz}/{tbl_items['brz_loan_payments'][1]}")
        .filter(F.col('Category') == 1)
        .withColumn('Date', spk_sapdate('Date', 'ymd'))
        .withColumn('PaymentPlanTS', F.col('PaymentPlanTS').cast(T.DateType()))
        .withColumn('Amount',        F.col('Amount').cast(T.DoubleType()))
        .dropDuplicates()
        .withColumn('next_date', F.when(F.col('Date') > F.col('PaymentPlanTS'), 
                                        F.row_number().over(by_date)))
        .withColumn('is_next', F.col('next_date') == F.min(F.col('next_date')).over(by_id)))
    
    loan_payment_df = (loan_payment_0
        .filter(F.col('is_next').isin([True]))
        .join(how='left', on='ContractID', other=loan_payment_0
            .groupBy(['ContractID'])
            .agg(F.countDistinct(F.col('ItemID')).alias('parcialidades_1')))
        .join(how='left', on='ContractID', other=loan_open_slv
            .select('ContractID', F.col('parcialidades_vencidas').alias('parcialidades_2')))
        .withColumn('parcialidades_plan', F.col('parcialidades_1') + F.col('parcialidades_2'))
        .select(pymt_select)
        .fillna(value=0))

    display(loan_payment_df)

# COMMAND ----------

loan_payment_0 = (spark.read.format('delta')
    .load(f"{abfss_brz}/{tbl_items['brz_loan_payments'][1]}"))
loan_payment_0.display()

# COMMAND ----------

# Loan Payment Plans

payment_cols = ['ItemID', 'ContractID', 'Date', 'Category', 'Amount', 'PaymentPlanTS']

loan_payment_0 = (spark.read.format('delta')
    .load(f"{abfss_brz}/{tbl_items['brz_loan_payments'][1]}")
    .select(*payment_cols)
    .filter(F.col('Category') == 1)
    .withColumn('Date',  date_format_udf(F.col('Date')))
    .withColumn('PaymentPlanTS', F.col('PaymentPlanTS').cast(T.DateType()))
    .withColumn('Amount',        F.col('Amount'  ).cast(T.DoubleType()))
    .withColumn('Category',      F.col('Category').cast(T.IntegerType()))
    .dropDuplicates())

window_payment_plan = W.partitionBy('ContractID').orderBy(F.asc(F.col('Date')))

latest_payment_plan = (loan_payment_0
    .filter(F.col('Date') > F.col('PaymentPlanTS'))
    .withColumn('rank', F.dense_rank().over(window_payment_plan))
    .filter(F.col('rank') == 1)
    .dropDuplicates()
    .select(
        F.col('ContractID'), 
        F.col('Date').alias('sig_pago'), 
        F.col('Amount').alias('monto_a_pagar')))

parcialidades_payment_plan = (loan_payment_0
    .select(F.col('ContractID'), F.col('ItemID'))
    .groupBy(F.col('ContractID'))
    .agg(F.countDistinct(F.col('ItemID')).alias('parcialidades_plan')))

parcialidades_payment_plan = (parcialidades_payment_plan
    .join(loan_open_slv, how='left', 
        on=parcialidades_payment_plan.ContractID == loan_open_slv.ContractID)
    .select(
        parcialidades_payment_plan.ContractID, 
        parcialidades_payment_plan.parcialidades_plan, 
        loan_open_slv.parcialidades_vencidas)
    .fillna(value=0)
    .withColumn('parcialidades_plan', F.col('parcialidades_plan') + F.col('parcialidades_vencidas'))
    .drop(F.col('parcialidades_vencidas')))

loan_payment_slv = (latest_payment_plan
    .join(parcialidades_payment_plan, how='inner', 
        on=latest_payment_plan.ContractID == parcialidades_payment_plan.ContractID)
    .drop(parcialidades_payment_plan.ContractID))

display(loan_payment_slv)

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Write tables

# COMMAND ----------

# Person Set utiliza Append, el resto 'overwrite' (default).
write_dataframe(person_slv, f"{abfss_slv}/{tbl_items['slv_persons'][1]}", 'append')

write_dataframe(loan_payment_slv,  f"{abfss_slv}/{tbl_items['slv_loan_payments'][1]}")
write_dataframe(loan_balance_slv,  f"{abfss_slv}/{tbl_items['slv_loan_balances'][1]}")
write_dataframe(loan_contract_slv, f"{abfss_slv}/{tbl_items['slv_loans'][1]}")
write_dataframe(loan_open_slv,     f"{abfss_slv}/{tbl_items['slv_loan_open_items'][1]}")



# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Silver 🥈 to Gold 🥇

# COMMAND ----------

# Do a Super JOIN. 

windowSpec = (W.partitionBy('attribute_loan_id')
    .orderBy(['attribute_due_date', 'promise_id']))

persons_slv = (spark.read.format('delta')
    .load(f"{abfss_slv}/{tbl_items['slv_persons'][1]}")
    .drop(*['last_login', 'phone_number']))

promises_slv = (spark.read.format('delta')
    .load(f"{at_promise}/{tbl_items['slv_promises'][1]}") 
    .withColumnRenamed('id', 'promise_id')
    .filter((F.col('attribute_processed') == False) & (F.col('attribute_accomplished') == False))
    .withColumn('rank', F.dense_rank().over(windowSpec))
    .filter(F.col('rank') == 1)
    .drop(F.col('rank')))


# COMMAND ----------

loan_contracts     = (spark.read.format('delta')
    .load(f"{abfss_slv}/{tbl_items['slv_loans'][1]}"))
loan_balances      = (spark.read.format('delta')
    .load(f"{abfss_slv}/{tbl_items['slv_loan_balances'][1]}"))
loan_open_items    = (spark.read.format('delta')
    .load(f"{abfss_slv}/{tbl_items['slv_loan_open_items'][1]}"))
loan_payment_plans = (spark.read.format('delta')
    .load(f"{abfss_slv}/{tbl_items['slv_loan_payments'][1]}"))

base_df = (loan_contracts
    .join(loan_balances, how='left', on=loan_contracts['ID'] == loan_balances['ID'])
    .drop(loan_balances['ID'])
    .join(persons_slv, how='inner', on=loan_contracts['BorrowerID'] == persons_slv['ID'])
    .drop(persons_slv['ID']))

base_open = (base_df
    .join(loan_open_items, how='left', 
          on=base_df['ID'] == loan_open_items['ContractID'])
    .drop(loan_open_items['ContractID'])
    .fillna(value=0))

display(base_open)

# COMMAND ----------

full_fields = (base_open
    .join(loan_payment_plans, how='left', 
        on=base_open['ID'] == loan_payment_plans['ContractID'])
    .drop(loan_payment_plans['ContractID'])
    .fillna(value=0)
    .join(promises_slv, how='left', on=base_open['ID'] == promises_slv['attribute_loan_id'])
    .drop(promises_slv['attribute_loan_id'])
    .dropDuplicates())

the_cols = ['TermSpecificationStartDate', 'sig_pago', 'attribute_due_date']

for a_col in the_cols:
    full_fields = full_fields.withColumn(a_col, F.date_format(F.col(a_col), 'dd-MM-yyyy'))

display(full_fields)

# COMMAND ----------

write_dataframe(full_fields, f"{abfss_gld}/{tbl_items['gld_loans'][1]}")
