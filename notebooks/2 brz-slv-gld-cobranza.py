# Databricks notebook source
# MAGIC %md 
# MAGIC ## Acerca de
# MAGIC Este _notebook_ fue escrito originalmente por Jacob.  
# MAGIC Para llevarlo de DEV a QAs, se le hicieron algunas factorizaciones:  
# MAGIC - Indicar tablas en un diccionario inicial.  
# MAGIC - Agrupar el código por celdas de acuerdo a las tablas que se procesan.
# MAGIC - Continuar 

# COMMAND ----------

# MAGIC %pip install -r ../reqs_dbks.txt

# COMMAND ----------

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
base_location = f"abfss://{at_storage}.dfs.core.windows.net/ops/core-banking-batch-updates"

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Bronze 🥉 to Silver 🥈

# COMMAND ----------

# MAGIC %md
# MAGIC The flow is as follows: 
# MAGIC 1. Prepare a few functions. 
# MAGIC 2. Setup Persons Set. 
# MAGIC 3. Continue with Loans (Contracts), and then:  
# MAGIC    3.a) Balances  
# MAGIC    3.b) Open Items  
# MAGIC    3.c) Payment Plans  
# MAGIC 4. Write the tables  

# COMMAND ----------

def segregate_lastNames(s, pos):
    if len(s.split(' ')) > 1:
        return s.split(' ')[pos]
    else:
      if pos == 1:
          return ''
      else:
          return s

def date_format(s):
    if s != '':
        return dt.strptime(s, '%Y%m%d').date()
    else:
        return dt.strptime('20000101', '%Y%m%d').date()

def sum_codes(l):
    return sum(l)

def real_amount(am, am_desc):
    if am_desc == '':
        return am
    else:
        expr = r'[0-9\.]+'
        match = re.search(expr, am_desc).group(0)
        return round(float(am) - float(match),2)

def overwrite_dataframe(spk_df, tbl_name): 
    (spk_df.write.mode('overwrite')
        .option('overwriteSchema', True)
        .format('delta')
        .saveAsTable(tbl_name))
    return None
      
segregate_udf   = udf(segregate_lastNames, T.StringType())
date_format_udf = udf(date_format, T.DateType())
sum_codes_udf   = udf(sum_codes,   T.DoubleType())
udf_ra          = udf(real_amount, T.DoubleType())


# COMMAND ----------

# MAGIC %md 
# MAGIC ### Person Set

# COMMAND ----------

# Person Set

person_cols = ['FirstName', 'LastName', 'LastName2', 
    'AddressRegion', 'AddressCity', 'AddressDistrictName', 'AddressStreet', 'AddressHouseID', 'AddressPostalCode', 
    'Gender', 'PhoneNumber', 'ID']

person_silver = spark.read.table(tables['slv_persons'])

person_set_0 = (spark.read.table(tables['brz_persons'])
    .select(*person_cols)
    .withColumnRenamed('PhoneNumber', 'phone_number')
    .withColumn('LastName2', segregate_udf(F.col('LastName'), F.lit(1)))
    .withColumn('LastName', segregate_udf(F.col('LastName'), F.lit(0)))
    .withColumn('phone_number', F.col('phone_number').cast(T.LongType()))
    .dropDuplicates())

person_set_df = (person_set_0
    .join(person_silver[['ID']], person_silver.ID == person_set_0.ID, how='left')
    .filter(person_silver.ID.isNull())
    .drop(person_silver.ID))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Loan Contracts

# COMMAND ----------

# Loan Contracts

loan_cols = ['ID', 'BankAccountID', 'InitialLoanAmount',
      'TermSpecificationStartDate', 'ClabeAccount',
      'RepaymentFrequency', 'TermSpecificationValidityPeriodDurationM',
      'NominalInterestRate', 'BorrowerID', 'OverdueDays', 'LifeCycleStatusTxt']

loan_contract_df = (spark.read.table(tables['brz_loans'])
    .select(*loan_cols)
    .withColumn('InitialLoanAmount', F.col('InitialLoanAmount').cast(T.DoubleType()))
    .withColumn('TermSpecificationValidityPeriodDurationM', F.col('TermSpecificationValidityPeriodDurationM').cast(T.IntegerType()))
    .withColumn('NominalInterestRate', F.round(F.col('NominalInterestRate').cast(T.DoubleType()), 2))
    .withColumn('TermSpecificationStartDate', date_format_udf(F.col('TermSpecificationStartDate')))
    .dropDuplicates())

# COMMAND ----------

# MAGIC %md 
# MAGIC #### Balances

# COMMAND ----------

# Loan Balances

loan_balance_0 = (spark.read.table(tables['brz_loan_balances'])
    .select(*['Amount', 'Code', 'ID'])
    .filter(F.col('Code').isin(['3', '4', '5']))
    .withColumn('Amount', F.col('Amount').cast(T.DoubleType()))
    .dropDuplicates())


# Maybe try PIVOT. 

ord_interes_df = (loan_balance_0
    .select(loan_balance_0.Amount.alias('ord_interes'), loan_balance_0.ID)
    .filter(loan_balance_0.Code == F.lit(3)))

comisiones_df = (loan_balance_0
    .select(loan_balance_0.Amount.alias('comisiones'), loan_balance_0.ID)
    .filter(loan_balance_0.Code == F.lit(4)))

monto_principal_df = (loan_balance_0
    .select(loan_balance_0.Amount.alias('monto_principal'), loan_balance_0.ID)
    .filter(loan_balance_0.Code == F.lit(5)))

ord_comisiones = (ord_interes_df
    .join(other=comisiones_df, how='inner', on=ord_interes_df.ID == comisiones_df.ID)
    .select(ord_interes_df.ID, ord_interes_df.ord_interes, comisiones_df.comisiones))

loan_balance_1 = (ord_comisiones
    .join(other=monto_principal_df, how='inner',
          on=ord_comisiones.ID == monto_principal_df.ID)
    .select(ord_comisiones.ID, ord_comisiones.ord_interes,
          ord_comisiones.comisiones, monto_principal_df.monto_principal))

two_columns = ['ord_interes', 'comisiones']

loan_balance_2 = loan_balance_1
for a_col in two_columns:
    loan_balance_2 = loan_balance_2.withColumn(a_col, F.abs(F.col(a_col)))
    
loan_balance_df = (loan_balance_1
    .withColumn('monto_liquidacion', F.col('comisiones') + F.col('ord_interes') + F.col('monto_principal')))

# COMMAND ----------

# MAGIC %md 
# MAGIC #### Open Loans 

# COMMAND ----------

# Open Loans

rec_types_mv  = [511100, 991100, 511010, 990004, 511200, 990006]
rec_types_iov = [511100, 991100, 990004]

open_items_cols = ['OpenItemID', 'Amount', 'ReceivableDescription',
    'StatusCategory', 'ReceivableType', 'DueDate', 'OpenItemTS',
    'ContractID']

loan_open_0 = (spark.read.table(tables['brz_loan_open_items'])
    .select(*open_items_cols)
    .withColumn('DueDate', date_format_udf(F.col('DueDate')))
    .withColumn('OpenItemTS', F.col('OpenItemTS').cast(T.DateType()))
    .withColumn('Amount', F.col('Amount').cast(T.DoubleType()))
    .dropDuplicates()
    .withColumn('Amount', udf_ra(F.col('Amount'), F.col('ReceivableDescription'))))

all_loans_open = loan_open_0.select(F.col('ContractID')).distinct()

parcialidades_pagadas = (loan_open_0
    .filter((F.col('ReceivableType') == 511010) & (F.col('StatusCategory') == 1))
    .groupBy(F.col('ContractID'))
    .agg(F.countDistinct(F.col('OpenItemID')).alias('parcialidades_pagadas')))

parcialidades_vencidas = (loan_open_0
    .filter((F.col('ReceivableType') == 511010) 
            & ((F.col('StatusCategory') == 2) | (F.col('StatusCategory') == 3)) 
            & (F.col('DueDate') < F.col('OpenItemTS')))
    .groupBy(F.col('ContractID'))
    .agg(F.countDistinct(F.col('OpenItemID')).alias('parcialidades_vencidas')))

monto_vencido = (loan_open_0
    .filter((F.col('ReceivableType').isin(rec_types_mv)) 
            & ((F.col('StatusCategory') == 2) | (F.col('StatusCategory') == 3)) 
            & (F.col('DueDate') < F.col('OpenItemTS')))
    .groupBy(F.col('ContractID'))
    .agg(F.sum(F.col('Amount')).alias('monto_vencido'))
    .withColumn('monto_vencido', F.round(F.col('monto_vencido'),2)))

principal_vencido = (loan_open_0
    .filter((F.col('ReceivableType') == 511010) 
            & ((F.col('StatusCategory') == 2) | (F.col('StatusCategory') == 3)) 
            & (F.col('DueDate') < F.col('OpenItemTS')))
    .groupBy(F.col('ContractID'))
    .agg(F.sum(F.col('Amount')).alias('principal_vencido'))
    .withColumn('principal_vencido', F.round(F.col('principal_vencido'),2)))

interes_ord_vencido = (loan_open_0
    .filter((F.col('ReceivableType').isin(rec_types_iov)) 
            & ((F.col('StatusCategory') == 2) | (F.col('StatusCategory') == 3)) 
            & (F.col('DueDate') < F.col('OpenItemTS')))
    .groupBy(F.col('ContractID'))
    .agg(F.sum(F.col('Amount')).alias('interes_ord_vencido'))
    .withColumn('interes_ord_vencido', F.round(F.col('interes_ord_vencido'), 2)))

loan_open_df = (all_loans_open
    .join(parcialidades_pagadas, how='left', 
          on=all_loans_open.ContractID == parcialidades_pagadas.ContractID)
    .select(all_loans_open.ContractID, parcialidades_pagadas.parcialidades_pagadas)
    .join(parcialidades_vencidas, how='left',
          on=all_loans_open.ContractID == parcialidades_vencidas.ContractID)
    .select(all_loans_open.ContractID, parcialidades_pagadas.parcialidades_pagadas, parcialidades_vencidas.parcialidades_vencidas)
    .join(monto_vencido, how='left', 
          on=all_loans_open.ContractID == monto_vencido.ContractID)
    .select(all_loans_open.ContractID, parcialidades_pagadas.parcialidades_pagadas, 
        parcialidades_vencidas.parcialidades_vencidas, monto_vencido.monto_vencido)
    .join(principal_vencido, how='left', 
          on=all_loans_open.ContractID == principal_vencido.ContractID)
    .select(
        all_loans_open.ContractID, parcialidades_pagadas.parcialidades_pagadas,
        parcialidades_vencidas.parcialidades_vencidas, monto_vencido.monto_vencido,
        principal_vencido.principal_vencido)
    .join(interes_ord_vencido, how='left', 
          on=all_loans_open.ContractID == interes_ord_vencido.ContractID)
    .select(all_loans_open.ContractID, 
        parcialidades_pagadas.parcialidades_pagadas, 
        parcialidades_vencidas.parcialidades_vencidas, 
        monto_vencido.monto_vencido, 
        principal_vencido.principal_vencido, 
        interes_ord_vencido.interes_ord_vencido)
    .fillna(value=0))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Payment Plans

# COMMAND ----------

# Loan Payment Plans

payment_cols = ['ItemID', 'ContractID', 'Date', 'Category', 'Amount', 'PaymentPlanTS']

loan_payment_0 = (spark.read.table(tables['brz_loan_payments'])
    .select(*payment_cols)
    .filter(F.col('Category') == 1)
    .withColumn('Date', date_format_udf(F.col('Date')))
    .withColumn('PaymentPlanTS', F.col('PaymentPlanTS').cast(T.DateType()))
    .withColumn('Amount',        F.col('Amount'  ).cast(T.DoubleType()))
    .withColumn('Category',      F.col('Category').cast(T.IntegerType()))
    .dropDuplicates())

window_payment_plan = W.partitionBy('ContractID').orderBy(F.asc(F.col('Date')))

ranked_loan_payment = (loan_payment_0
    .filter(F.col('Date') > F.col('PaymentPlanTS'))
    .withColumn('rank', F.dense_rank().over(window_payment_plan))
    .filter(F.col('rank') == 1)
    .withColumn('DaysToPayment', F.datediff(F.col('Date'), F.current_date()))
    .dropDuplicates())

window_payment_plan = W.partitionBy('ContractID').orderBy(F.asc(F.col('Date')))

ranked_loan_payment = (loan_payment_0
    .filter(F.col('Date') > F.col('PaymentPlanTS'))
    .withColumn('rank', F.dense_rank().over(window_payment_plan))
    .filter(F.col('rank') == 1)
    .withColumn('DaysToPayment', F.datediff(F.col('Date'), F.current_date()))
    .dropDuplicates())

latest_payment_plan = ranked_loan_payment.select(
        F.col('ContractID'), 
        F.col('Date').alias('sig_pago'), 
        F.col('Amount').alias('monto_a_pagar'), 
        F.col('DaysToPayment'))

parcialidades_payment_plan = (loan_payment_0
    .select(F.col('ContractID'), F.col('ItemID'))
    .groupBy(F.col('ContractID'))
    .agg(F.countDistinct(F.col('ItemID')).alias('parcialidades_plan')))

parcialidades_payment_plan = (parcialidades_payment_plan
    .join(loan_open_df, how='left', 
        on=parcialidades_payment_plan.ContractID == loan_open_df.ContractID)
    .select(
        parcialidades_payment_plan.ContractID, 
        parcialidades_payment_plan.parcialidades_plan, 
        loan_open_df.parcialidades_vencidas)
    .fillna(value=0)
    .withColumn('parcialidades_plan', F.col('parcialidades_plan') + F.col('parcialidades_vencidas'))
    .drop(F.col('parcialidades_vencidas')))

loan_payment_df = (latest_payment_plan
    .join(parcialidades_payment_plan, how='inner', 
        on=latest_payment_plan.ContractID == parcialidades_payment_plan.ContractID)
    .drop(parcialidades_payment_plan.ContractID))

# COMMAND ----------

# Write the Silver Tables

overwrite_dataframe(person_set_df,    tables['slv_persons'])
overwrite_dataframe(loan_payment_df,  tables['slv_loan_payments'])
overwrite_dataframe(loan_balance_df,  tables['slv_loan_balances'])
overwrite_dataframe(loan_contract_df, tables['slv_loans'])
overwrite_dataframe(loan_open_df,     tables['slv_loan_open_items'])

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Silver 🥈 to Gold 🥇

# COMMAND ----------

# Do a Super JOIN. 

windowSpec = (W.partitionBy('attribute_loan_id')
    .orderBy(['attribute_due_date', 'promise_id']))

persons_set = (spark.table(tables['slv_persons'])
    .drop(*['last_login', 'phone_number']))

promises = (spark.table(tables['slv_promises'])
    .withColumnRenamed('id', 'promise_id')
    .filter((F.col('attribute_processed') == False) & (F.col('attribute_accomplished') == False))
    .withColumn('rank', F.dense_rank().over(windowSpec))
    .filter(F.col('rank') == 1)
    .drop(F.col('rank')))

# COMMAND ----------

loan_contracts     = spark.table(tables['slv_loans'])
loan_balances      = spark.table(tables['slv_loan_balances'])
loan_open_items    = spark.table(tables['slv_loan_open_items'])
loan_payment_plans = spark.table(tables['slv_loan_payments'])

base_df = (loan_contracts
    .join(loan_balances, how='left', on=loan_contracts.ID == loan_balances.ID)
    .drop(loan_balances.ID)
    .join(persons_set, how='inner', on=loan_contracts.BorrowerID == persons_set.ID)
    .drop(persons_set.ID))

base_open = (base_df
    .join(loan_open_items, how='left', on=base_df.ID == loan_open_items.ContractID)
    .drop(loan_open_items.ContractID)
    .fillna(value=0))

# COMMAND ----------

full_fields = (base_open
    .join(loan_payment_plans, how='left', on=base_open.ID == loan_payment_plans.ContractID)
    .drop(loan_payment_plans.ContractID)
    .fillna(value=0)
    .join(promises, how='left', on=base_open.ID == promises.attribute_loan_id)
    .drop(promises.attribute_loan_id)
    .dropDuplicates())

the_cols = ['TermSpecificationStartDate', 'sig_pago', 'attribute_due_date']

for a_col in the_cols:
    full_fields = full_fields.withColumn(a_col, F.date_format(F.col(a_col), 'dd-MM-yyyy'))

# COMMAND ----------

overwrite_dataframe(full_fields, tables['gld_loans'])
