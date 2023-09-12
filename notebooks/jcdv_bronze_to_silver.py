# Databricks notebook source
# MAGIC %md
# MAGIC # Descripción
# MAGIC 
# MAGIC Este es una copia el 4 de abril del archivo de Jacobo.   
# MAGIC El _notebook_ original está en su carpeta de usuario,  
# MAGIC en la carpeta de `MotorCobranza`.  
# MAGIC Le hice algunas modificaciones personales de formato.  

# COMMAND ----------

from datetime import datetime as dt
from pyspark.sql import functions as F, types as T
from pyspark.sql.window import Window as W
import re
from urllib.parse import unquote

# COMMAND ----------

def segregate_lastNames(s, pos):
    the_split = s.split(' ')
    if len(the_split) >= pos:
        return the_split[pos]
    elif pos == 1:
        return ''
    else:
      return s

segregate_udf = F.udf(segregate_lastNames, T.StringType())

def date_format(s):
  if s != '':
    return dt.strptime(s, '%Y%m%d').date()
  else:
    return dt.strptime('20000101', '%Y%m%d').date()

date_format_udf = F.udf(date_format, T.DateType())

def sum_codes(l):
  return sum(l)

sum_codes_udf = F.udf(sum_codes, T.DoubleType())

# COMMAND ----------

loan_contract_df = (spark.sql("""
    select
      ID,
      BankAccountID,
      InitialLoanAmount,
      TermSpecificationStartDate,
      ClabeAccount,
      RepaymentFrequency,
      TermSpecificationValidityPeriodDurationM,
      NominalInterestRate,
      BorrowerID,
      OverdueDays
    from bronze.loan_contracts
    """)
    .withColumn("InitialLoanAmount", F.col("InitialLoanAmount").cast(T.DoubleType()))
    .withColumn("TermSpecificationValidityPeriodDurationM", F.col("TermSpecificationValidityPeriodDurationM").cast(T.IntegerType()))
    .withColumn("NominalInterestRate", F.round(F.col("NominalInterestRate").cast(T.DoubleType()), 2))
    .withColumn("TermSpecificationStartDate", date_format_udf(F.col("TermSpecificationStartDate"))) )

# COMMAND ----------

person_set_df = (spark.sql("""
    select
      FirstName,
      LastName,
      LastName2,
      AddressRegion,
      AddressCity,
      AddressDistrictName,
      AddressStreet,
      AddressHouseID,
      AddressPostalCode,
      Gender,
      PhoneNumber as phone_number,
      ID
    from bronze.persons_set
    """)
    .withColumn("LastName2",    segregate_udf(F.col("LastName"), F.lit(1)))
    .withColumn("LastName",     segregate_udf(F.col("LastName"), F.lit(0)))
    .withColumn("phone_number", F.col("phone_number").cast(T.LongType())))

# COMMAND ----------

person_silver = spark.sql("""
    select *
    from silver.persons_set
    """)

person_set_df = (person_set_df
    .join(person_silver[["ID"]], person_silver.ID == person_set_df.ID, 'left')
    .filter(person_silver.ID.isNull())
    .drop(person_silver.ID))

# Este JOIN es básicamente un ANTI-JOIN (BRONZE - SILVER). 

# COMMAND ----------

loan_balance_df = (spark.sql("""
    select
        Amount,
        Code,
        ID, 
        BalancesTS
    from bronze.loan_balances
    where code in ('3', '4', '5')
    """)
    .withColumn('maxTS', F.max('balancesTS').over(W.partitionBy(['ID', 'code'])))
    .filter(F.col('BalancesTS') == F.col('maxTS'))
    .drop(*['BalancesTS', 'maxTS'])
    .withColumn('Amount', F.col('Amount').cast(T.DoubleType())))

# COMMAND ----------

# PIVOT:  CODE, AMOUNT. 
ord_interes_df = (loan_balance_df
    .select(loan_balance_df.Amount.alias("ord_interes"), loan_balance_df.ID)
    .filter(loan_balance_df.Code == F.lit(3)))

comisiones_df = (loan_balance_df
    .select(loan_balance_df.Amount.alias("comisiones"), loan_balance_df.ID)
    .filter(loan_balance_df.Code == F.lit(4)))

monto_principal_df = (loan_balance_df
    .select(loan_balance_df.Amount.alias("monto_principal"), loan_balance_df.ID)
    .filter(loan_balance_df.Code == F.lit(5)))

# COMMAND ----------

ord_comisiones = (ord_interes_df
    .join(other=comisiones_df, how='inner', on=(ord_interes_df.ID == comisiones_df.ID))
    .select(ord_interes_df.ID, ord_interes_df.ord_interes, comisiones_df.comisiones))

loan_balance_df = (ord_comisiones
    .join(other=monto_principal_df, how='inner', on=(ord_comisiones.ID == monto_principal_df.ID))
    .select(ord_comisiones.ID, ord_comisiones.ord_interes, 
            ord_comisiones.comisiones, monto_principal_df.monto_principal)
    .withColumn("monto_liquidacion", F.col("comisiones") + F.col("ord_interes") + F.col("monto_principal")))

# COMMAND ----------

loan_open_df = spark.sql("""
    select
      OpenItemID,
      Amount,
      StatusCategory,
      ReceivableType,
      DueDate,
      OpenItemTS,
      ContractID
    from bronze.loan_open_items
    """)

# COMMAND ----------

rec_types_mv  = [511100, 991100, 511010, 990004, 511200, 990006]
rec_types_iov = [511100, 991100, 990004]

# I see a pattern: 
# filtros = {
#     'parcialidades_pagadas' : [['ReceivableType', 511010], ['StatusCategory', 1]], 
#     'parcialidades_vencidas' : [['ReceivableType', 511010], ['StatusCategory', (2, 3)]],
#     'monto_vencido' : [['ReceivableType', rec_tpyes_mv.astuple()], ['StatusCategory', (2, 3)], ['DueDate', 'OpenItemTS']], 
#     'principal_vencido': [['RecivableType', 511010], ['StatusCategory', (2,3)], ['DueDate', 'OpenItemTS']],
#     'interes_ord_vencido': [['RecievableType', rec_types_iov.astuple()], ['StatusCategory', (2,3)], ['DueDate', 'OpenItemTS']]
# }

# def one_filter(fltr_ls): 
#     type_encode = type(fltr_ls[1])
#     if isinstance(fltr_ls[1], tuple): 
#         encode = quote(F.col(fltr_ls[0]) == fltr_ls[1])
#     elif isinstance(fltr_ls[1], string):
#         encode = quote(F.col(fltr_ls[0]) < F.col(fltr_ls[1]))
#     elif isinstance(fltr_ls[1], tuple):
#         encode = quote(F.col(fltr_ls[0]).isin(fltr_ls[1]))
#     else: 
#         encode = None

# def many_filters(one_fltr_ls): 
#     and_op = lambda fltr_1, q_fltr: F.and(fltr_1, unquote(q_fltr))
#     reduce( and_op, one_fltr_ls, F.lit(1) == F.lit(1))

parcialidades_pagadas = (loan_open_df
    .filter((F.col("ReceivableType") == 511010) & (F.col("StatusCategory") == 1))
    .groupBy(F.col("ContractID"))
    .agg(F.countDistinct(F.col("OpenItemID")).alias("parcialidades_pagadas")))

parcialidades_vencidas = (loan_open_df
    .filter((F.col("ReceivableType") == 511010) 
          & ((F.col("StatusCategory") == 2) | (F.col("StatusCategory") == 3)) 
          & (F.col("DueDate") < F.col("OpenItemTS")))
    .groupBy(F.col("ContractID"))
    .agg(F.countDistinct(F.col("OpenItemID")).alias("parcialidades_vencidas")))

monto_vencido = (loan_open_df\
    .filter((F.col("ReceivableType").isin(rec_types_mv)) 
          & ((F.col("StatusCategory") == 2) | (F.col("StatusCategory") == 3))
          & (F.col("DueDate") < F.col("OpenItemTS")))
    .groupBy(F.col("ContractID"))
    .agg(F.sum(F.col("Amount")).alias("monto_vencido")))

monto_vencido = (monto_vencido
    .withColumn("monto_vencido", F.round(F.col("monto_vencido"), 2)))

principal_vencido = (loan_open_df
    .filter( (F.col("ReceivableType") == 511010) 
          & ((F.col("StatusCategory") == 2) | (F.col("StatusCategory") == 3)) 
          & ( F.col("DueDate") < F.col("OpenItemTS")))
    .groupBy( F.col("ContractID"))
    .agg(F.sum(F.col("Amount")).alias("principal_vencido")))

principal_vencido = (principal_vencido
    .withColumn("principal_vencido", F.round(F.col("principal_vencido"), 2)))

interes_ord_vencido = loan_open_df\
    .filter((F.col("ReceivableType").isin(rec_types_iov)) 
          & ((F.col("StatusCategory") == 2) | (F.col("StatusCategory") == 3)) 
          & (F.col("DueDate") < F.col("OpenItemTS")))\
    .groupBy(F.col("ContractID"))\
    .agg(F.sum(F.col("Amount")).alias("interes_ord_vencido"))

interes_ord_vencido = (interes_ord_vencido
    .withColumn("interes_ord_vencido", F.round(F.col("interes_ord_vencido"), 2)))

# COMMAND ----------

loan_open_df = (loan_open_df
    .withColumn("DueDate", date_format_udf(F.col("DueDate")))
    .withColumn("OpenItemTS", F.col("OpenItemTS").cast(T.DateType()))
    .withColumn("Amount", F.col("Amount").cast(T.DoubleType())))

# COMMAND ----------

all_loans_open = loan_open_df.select(F.col("ContractID")).distinct()

# COMMAND ----------

loan_open_df = (all_loans_open
    .join(parcialidades_pagadas, how='left', 
          on=(all_loans_open.ContractID == parcialidades_pagadas.ContractID))
    .select(all_loans_open.ContractID, parcialidades_pagadas.parcialidades_pagadas)
    .join(parcialidades_vencidas, how='left', 
          on=(all_loans_open.ContractID == parcialidades_vencidas.ContractID))
    .select(all_loans_open.ContractID, parcialidades_pagadas.parcialidades_pagadas, 
            parcialidades_vencidas.parcialidades_vencidas)
    .join(monto_vencido, how='left', 
          on=(all_loans_open.ContractID == monto_vencido.ContractID))
    .select(all_loans_open.ContractID, parcialidades_pagadas.parcialidades_pagadas, 
            parcialidades_vencidas.parcialidades_vencidas, monto_vencido.monto_vencido)
    .join(principal_vencido, how='left', 
          on=(all_loans_open.ContractID == principal_vencido.ContractID))
    .select(all_loans_open.ContractID, parcialidades_pagadas.parcialidades_pagadas, 
            parcialidades_vencidas.parcialidades_vencidas, monto_vencido.monto_vencido, 
            principal_vencido.principal_vencido)
    .join(interes_ord_vencido, how='left', 
          on=(all_loans_open.ContractID == interes_ord_vencido.ContractID))
    .select(all_loans_open.ContractID, parcialidades_pagadas.parcialidades_pagadas, 
            parcialidades_vencidas.parcialidades_vencidas, monto_vencido.monto_vencido, 
            principal_vencido.principal_vencido, interes_ord_vencido.interes_ord_vencido)
    .fillna(value=0))

# COMMAND ----------

loan_payment_df = (spark.sql("""
    select
      ItemID,
      ContractID,
      Date,
      Category,
      Amount,
      PaymentPlanTS
    from bronze.loan_payment_plans
    where Category = 1
    """)
    .withColumn("Date", date_format_udf(F.col("Date")))
    .withColumn("PaymentPlanTS", F.col("PaymentPlanTS").cast(T.DateType()))
    .withColumn("Amount", F.col("Amount").cast(T.DoubleType()))
    .withColumn("Category", F.col("Category").cast(T.IntegerType())))

# COMMAND ----------

window_payment_plan = W.partitionBy("ContractID")
ranked_loan_payment = (loan_payment_df
    .filter(F.col("Date") < F.col("PaymentPlanTS")) 
    .withColumn("rank", F.dense_rank().over(window_payment_plan.orderBy(F.asc(F.col("Date")))))
    .filter(F.col("rank") == 1)
    .withColumn("DaysToPayment", F.datediff(F.current_date(), F.col('Date'))))

# COMMAND ----------

latest_payemnt_plan = (ranked_loan_payment
    .withColumn("OverdueDays2", )
    .select(F.col("ContractID"), 
            F.col("Date").alias("sig_pago"), 
            F.col("Amount").alias("monto_a_pagar")))

parcialidades_payment_plan = (loan_payment_df
    .select(F.col("ContractID"), F.col("ItemID"))
    .groupBy(F.col("ContractID"))
    .agg(F.countDistinct(F.col("ItemID")).alias("parcialidades_plan")))

parcialidades_payment_plan = (parcialidades_payment_plan
    .join(loan_open_df, parcialidades_payment_plan.ContractID == loan_open_df.ContractID, 'left')
    .select(parcialidades_payment_plan.ContractID, 
            parcialidades_payment_plan.parcialidades_plan, loan_open_df.parcialidades_vencidas)
    .fillna(value=0))

# COMMAND ----------

parcialidades_payment_plan = (parcialidades_payment_plan
    .withColumn("parcialidades_plan", F.col("parcialidades_plan") + F.col("parcialidades_vencidas"))
    .drop(F.col("parcialidades_vencidas")))

# COMMAND ----------

loan_payment_df = (latest_payemnt_plan
  .join(parcialidades_payment_plan, latest_payemnt_plan.ContractID == parcialidades_payment_plan.ContractID, 'inner')
  .drop(parcialidades_payment_plan.ContractID))

# COMMAND ----------

# DBTITLE 1,Save Tables
loan_payment_df.write.mode("overwrite").format("delta").saveAsTable("silver.loan_payment_plans")
loan_balance_df.write.mode("overwrite").format("delta").saveAsTable("silver.loan_balances")
loan_contract_df.write.mode("overwrite").format("delta").saveAsTable("silver.loan_contracts")
loan_open_df.write.mode("overwrite").format("delta").saveAsTable("silver.loan_open_items")
person_set_df.write.mode("append").format("delta").saveAsTable("silver.persons_set")
