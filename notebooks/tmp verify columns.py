# Databricks notebook source
contracts = spark.table('bronze.loan_contracts')
contract_names = contracts.schema.names
print(contract_names)

# COMMAND ----------

status_names = [name for name in contract_names if 'Status' in name]
print(status_names)

# COMMAND ----------

lifecycle = contracts.select('LifeCycleStatusTxt').distinct().collect()
print(lifecycle)
