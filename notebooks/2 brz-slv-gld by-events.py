# Databricks notebook source
# MAGIC %md 
# MAGIC # Descripción
# MAGIC Este _notebook_ fue escrito originalmente por Jacobo.  
# MAGIC Para llevarlo de DEV a QAs, le hice (Diego) algunas factorizaciones:  
# MAGIC - Indicar tablas a partir de un diccionario en `CONFIG.PY`.  
# MAGIC - Agrupar el código por celdas de acuerdo a las tablas que se procesan.
# MAGIC - Encadenar las instrucciones de las tablas en una sola, cuando es posible. 

# COMMAND ----------

# MAGIC %pip install -q -r ../reqs_dbks.txt

# COMMAND ----------

from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.dbutils import DBUtils
import subprocess
import yaml

epicpy_load = {
    'url'   : 'github.com/Bineo2/data-python-tools.git', 
    'branch': 'dev-diego'}

with open("../user_databricks.yml", 'r') as _f: 
    u_dbks = yaml.safe_load(_f)


spark = SparkSession.builder.getOrCreate()
dbutils = DBUtils(spark)

epicpy_load['token'] = dbutils.secrets.get(u_dbks['dbks_scope'], u_dbks['dbks_token'])
url_call = "git+https://{token}@{url}@{branch}".format(**epicpy_load)
subprocess.check_call(['pip', 'install', url_call])

# COMMAND ----------

from collections import OrderedDict
from datetime import datetime as dt, date, timedelta as delta
from delta.tables import DeltaTable as Δ
from pyspark.sql import functions as F, types as T, Window as W
import re

# COMMAND ----------

from src.utilities import tools
from src.data_managers import EpicDF
from src.platform_resources import AzureResourcer
from config import (ConfigEnviron, ENV, SERVER, DBKS_TABLES)

tables = DBKS_TABLES[ENV]

app_environ = ConfigEnviron(ENV, SERVER, spark)
az_manager  = AzureResourcer(app_environ)
at_storage  = az_manager.get_storage()
az_manager.set_dbks_permissions(at_storage)

events_brz = tables['events'].format(stage='bronze', storage=at_storage)
abfss_slv  = tables['base'  ].format(stage='silver', storage=at_storage)
abfss_gld  = tables['base'  ].format(stage='gold',   storage=at_storage)
at_promise = tables['promises'].format(stage='silver', storage=at_storage) 

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

# MAGIC %md 
# MAGIC ## Person Set

# COMMAND ----------

# Person Set
persons_cols = [F.col('ID').alias('BorrowerID'), 
    'FirstName', 'LastName', 'LastName2', 
    'AddressRegion', 'AddressCity', 'AddressDistrictName', 
    'AddressStreet', 'AddressHouseID', 'AddressPostalCode', 
    'Gender', 'PhoneNumber', 'MobileNumber', 
    F.col('epic_date').alias('person_date')]

persons_slv = (EpicDF(spark, f"{events_brz}/person-set/chains/person")
    .select(*persons_cols)
    .withColumn('phone_number', F.coalesce('PhoneNumber', 'MobileNumber')))

display(persons_slv)

# COMMAND ----------

from epic_py.delta import EpicDF
from inspect import getsource
a_slv = EpicDF(spark, f"{events_brz}/person-set/chains/person")
print(getsource(a_slv.select))

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Current Account
# MAGIC

# COMMAND ----------

account_cols = [F.col('ID').alias('ContractID'), 
    # En Webapp se obtienen de Loan-Contract. 
    'ClabeAccount', 
    F.col('StatementFrequencyName').alias('RepaymentFrequency')]

curr_account = (EpicDF(spark, f"{events_brz}/current-account/data")
    .select(*account_cols))


# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Loan Contracts

# COMMAND ----------

# Loan Contracts

# A veces sirve filtrarlas. 
loan_cols_0 = [F.col('ID').alias('ContractID'), 
    'BankAccountID', 'InitialLoanAmount',
    'TermSpecificationStartDate', 
    'TermSpecificationValidityPeriodDurationM',
    'NominalInterestRate', 'BorrowerID', 'LifeCycleStatusTxt',
    'PaymentPlanStartDate', 
    'sap_EventDateTime', 
    F.col('epic_date').alias('loan_date')]

loans_slv = (EpicDF(spark, f"{events_brz}/loan-contract/data")
    .select(loan_cols_0))


display(loans_slv)

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Balances

# COMMAND ----------

# MAGIC %md
# MAGIC |Code |	Name |
# MAGIC |-----|------|
# MAGIC | 1 |Amount that is already disbursed to the borrower   |
# MAGIC | 2	|Amount that is not yet been disbursed to the borrower  |
# MAGIC | 3	|Interest settled that is yet to be paid by the borrower  |  
# MAGIC | 4	|Charge settled that is yet to be paid by the borrower  |
# MAGIC | 5	|Remaining principal based on payments received (inclusive of redraw) as of today   |
# MAGIC | 17|The initial loan contracted amount   |
# MAGIC | 18|Initial loan contracted amount plus any capital reductions and increases that have already taken place  |
# MAGIC | 19|An amount for which there is a binding agreement between the bank and the borrower for a term agreement fixing period  |
# MAGIC | 20|An amount for which there is a binding agreement between the bank and the borrower for a term agreement fixing period plus any capital reductions and increases that have already taken place  |
# MAGIC | 21|Remaining principal based on the original payment plan simulated by date  |
# MAGIC | 22|Remaining principal based on the original payment plan as of today  | 
# MAGIC | 25|Interest accrued so far, but not yet settled  |
# MAGIC | 31|Interest paid by the borrower so far  |
# MAGIC | 95|Outstanding Charges VAT  |
# MAGIC | 96|Interests not taxed  |
# MAGIC | 97|Outstanding Interest VAT  |
# MAGIC | 98|Redraw balance (principal paid ahead of original payment plan)  |
# MAGIC | 99|Outstanding balance  |
# MAGIC

# COMMAND ----------

code_cols = OrderedDict({
    'ord_interes'      : F.abs(F.col('code_3')), 
    'comisiones'       : F.abs(F.col('code_4')), 
    'monto_principal'  : F.col('code_5'), 
    'monto_liquidacion': F.abs('code_3') + F.abs('code_4') + F.col('code_5')})

balance_cols = [*(vv.alias(kk) for kk, vv in code_cols.items()), 
    F.col('ID').alias('ContractID'), 
    F.col('epic_date').alias('loan_date')]

balance_0 = (EpicDF(spark, f"{events_brz}/loan-contract/aux/balances-wide")
    .filter(F.col('Currency') != F.lit("")))

balance_slv = (balance_0
    .select(*balance_cols))

balance_slv.display()

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Open Items 
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
# MAGIC | 511080 | * Reemb.parcial.créd.(esp.)|
# MAGIC | 511100 | * Int. Nominal |
# MAGIC | 511200 | Comisión |
# MAGIC | 990004 | IVA Interés |
# MAGIC | 991100 | Int. No Gravado |
# MAGIC | 990006 | IVA. Comisión |
# MAGIC
# MAGIC Y generamos las variables intermedias: 
# MAGIC * `es_vencido`: `StatusCategory in (2, 3) AND (DueDate < Timestamp)`  
# MAGIC * `recibible` : `511010 => capital`,  
# MAGIC &emsp;&emsp;&emsp;`(990004, 991100) => impuesto`,  
# MAGIC &emsp;&emsp;&emsp;`(511200, 990006) => comision`
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

ids_cols = ['ContractID', 'loan_date']

w_next = (W.partitionBy(ids_cols)
    .orderBy(F.col('DueDate').asc()))
w_past = (W.partitionBy(ids_cols)
    .orderBy(F.col('DueDate').desc()))

items_cols = OrderedDict({
    'current_date': F.date_add(F.current_date(), -1),
    'days_diff'   : F.datediff('current_date', 'DueDate'), 
    'capital'     : F.col('511010'),
    'impuesto'    : F.col('991100') + F.col('990004'),
    'comision'    : F.col('511200') + F.col('990006'), 
    'is_past'     : F.col('current_date') > F.col('DueDate'), 
    'is_default'  : F.col('StatusCategory').isin([2, 3]) &  F.col('is_past'), 
    'is_paid'     :(F.col('StatusCategory') == F.lit(1)) &  F.col('is_past'),
    'is_due'      :(F.col('StatusCategory') == F.lit(1)) & ~F.col('is_past'), 
    'is_next_due' :(F.when(F.col('is_due'),     F.row_number().over(w_next)) == 1), 
    'last_overdue':(F.when(F.col('is_default'), F.row_number().over(w_past)) == 1)})

grp_cols = OrderedDict({
    'parcialidades_plan'    : F.count('DueDate'), 
    'parcialidades_pagadas' : F.sum( F.col('is_paid').cast(T.DoubleType())), 
    'parcialidades_vencidas': F.sum( F.col('is_default').cast(T.DoubleType())), 
    'monto_vencido'         : F.sum( F.col('amount' )*F.col('is_default').cast(T.DecimalType())), 
    'principal_vencido'     : F.sum( F.col('capital')*F.col('is_default').cast(T.DecimalType())), 
    'interes_ord_vencido'   : F.sum( F.col('capital')*F.col('is_default').cast(T.DecimalType())),
})

next_cols = [*ids_cols, 
    F.col('DueDate').alias('sig_pago'), 
    F.col('amount' ).alias('monto_a_pagar'), 
    F.col('days_diff').alias('DaysToPayment')]

overdue_cols = [*ids_cols, 
    F.col('days_diff').alias('OverdueDays')]


### Execute All. 
# rec_types = [c_nm for c_nm in open_items_0.columns if c_nm.isnumeric()]
rec_types = ['511010', '511080', '511100', '511200', '990004', '990006', '991100']
    
# Falta sap_EventDateTime en OPEN_ITEMS_WIDE, entonces copy de Loan_SLV. 
loan_date_df = (loans_slv
    .select('loan_date', 'ContractID', 'sap_EventDateTime')) 

open_items_1 = (EpicDF(spark, f"{events_brz}/loan-contract/aux/open-items-wide") 
    .fillna(0, subset=rec_types)
    .withColumnRenamed('epic_date', 'loan_date')
    .join(how='left', on=ids_cols, other=loan_date_df)
    .with_columns(items_cols))

open_items_2a = (open_items_1
    .groupBy(ids_cols)
    .agg(*(vv.alias(kk) for kk,vv in grp_cols.items()) ))

# Puede ser que no haya ninguno. 
open_items_2b = (open_items_1
    .filter(F.col('is_next_due'))  
    .select(next_cols))

open_items_2c = (open_items_1
    .filter(F.col('last_overdue'))  
    .select(overdue_cols))

open_items_slv = (open_items_2a
    .join(open_items_2b, how='left', on=ids_cols)
    .join(open_items_2c, how='left', on=ids_cols))

open_items_slv.display()


# COMMAND ----------

# MAGIC %md
# MAGIC ## Promises

# COMMAND ----------

# Do a Super JOIN. 
w_promises = (W.partitionBy('attribute_loan_id')
    .orderBy(F.col('attribute_due_date').desc(), 'promise_id'))

promises_slv = (EpicDF(spark, f"{at_promise}/promises") 
    .withColumnRenamed('id', 'promise_id')
    .filter(~F.col('attribute_processed') 
          & ~F.col('attribute_accomplished'))
    .withColumn('rk_promise', F.row_number().over(w_promises))
    .filter(F.col('rk_promise') == 1)
    .drop(  F.col('rk_promise')))

display(promises_slv)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Silver 🥈 to Gold 🥇

# COMMAND ----------

golden_join = (loans_slv
    .join(how='inner', on='BorrowerID', 
        other=persons_slv.withColumnRenamed('ID', 'BorrowerID'))
    .join(how='left', other=curr_account, on= 'ContractID')
    .join(how='left', other=balance_slv  , on=['ContractID', 'loan_date'])
    .join(how='left', other=open_items_slv, on=['ContractID', 'loan_date'])
    .join(how='left', on='ContractID', other=promises_slv
          .withColumnRenamed('attribute_loan_id', 'ContractID')))

display(golden_join)


# COMMAND ----------

# MAGIC %md 
# MAGIC ## Write Gold

# COMMAND ----------

(golden_join.write
     .mode('overwrite')
     .option('overwriteSchema', True)
     .save(f"{abfss_gld}/loan-contracts"))
