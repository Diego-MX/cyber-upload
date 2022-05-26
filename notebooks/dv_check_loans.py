# Databricks notebook source
# MAGIC %md
# MAGIC # Descripción
# MAGIC Sólo para comparar las tablas de `loan_contracts` y su correspondiente `loan_contracts_qan` que corresponde 
# MAGIC al _call_ de 'Quick Analyzer'. 

# COMMAND ----------

import pyspark.pandas as ps

# COMMAND ----------

loans = ps.read_table('bronze.loan_contracts')
print(f'Loan Entries: {loans.shape[0]}')
display(loans)

# COMMAND ----------

loans_qan = ps.read_table("bronze.loan_qan_contracts")
print(f"Loan entries: {loans_qan.shape[0]}")
display(loans_qan)
