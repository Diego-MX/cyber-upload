# Databricks notebook source
# MAGIC %md 
# MAGIC # Objetivo
# MAGIC 
# MAGIC Al crear las tablas Δ, no se había puesto atención en la ubicación física... (que no es tan física, pero es lo más físico a lo que aspiran las ubicaciones de tablas).  
# MAGIC Después de algunos meses se descubrió que tienen que estar en cierta ubicación.  
# MAGIC En este _notebook_ movemos las tablas al Lago Hylia. 

# COMMAND ----------

# MAGIC %sql 
# MAGIC CREATE TABLE nayru_accounts.brz_ops_loan_balances USING DELTA
# MAGIC LOCATION "abfss://bronze@lakehylia.dfs.core.windows.net/ops/core-banking/batch-updates/loan-balances";
