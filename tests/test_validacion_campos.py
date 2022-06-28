# Databricks notebook source
import pyspark.pandas as ps

# COMMAND ----------

prestamos = ps.read_table("gold.loan_contracts")
display(prestamos)

# COMMAND ----------


