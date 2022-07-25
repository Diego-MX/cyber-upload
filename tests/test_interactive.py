# Databricks notebook source
from json import loads, dumps
from pyspark.sql import functions as F, types as T, Window as W

# COMMAND ----------

gold_loans = spark.read.table('gold.loan_contracts')
one_account = gold_loans.filter(F.col('BankAccountID')=='03017012378')
display(one_account)

# COMMAND ----------

one_json = loads(one_account.toJSON().collect()[0])
print(dumps(one_json, indent=2))


# COMMAND ----------

brz_loans = spark.read.table('nayru_accounts.brz_ops_loan_contracts')
one_bronze = brz_loans.filter(F.col('BankAccountID')=='03017012378')
brz_json = loads(one_bronze.toJSON().collect()[0])
print(dumps(brz_json, indent=2))


# COMMAND ----------

# MAGIC %sql 
# MAGIC DESCRIBE TABLE EXTENDED bronze.loan_contracts
