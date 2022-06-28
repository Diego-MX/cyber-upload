# Databricks notebook source
# MAGIC %md 
# MAGIC # Objetivo
# MAGIC 
# MAGIC Al crear las tablas Δ, no se habían definido una ubicación física específica.  
# MAGIC Pero las tablas deben vivir en el _datalake_, entonces les hacemos clones a las mismas para aprovisionarlas de una dirección física.  
# MAGIC Adicional a esto, en el _notebook_ `0 clone_at_metastore` las recreamos para que sean accesibles desde los dos _metastores_ correspondientes. 

# COMMAND ----------

# MAGIC %sql 
# MAGIC CREATE TABLE delta.`abfss://gold@lakehylia.dfs.core.windows.net/ops/core-banking/batch-updates/loan-contracts` CLONE gold.loan_contracts;

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE gold.loan_contracts SET LOCATION 'abfss://gold@lakehylia.dfs.core.windows.net/ops/core-banking/batch-updates/loan-contracts';
