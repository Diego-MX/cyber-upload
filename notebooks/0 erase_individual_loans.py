# Databricks notebook source
# MAGIC %md
# MAGIC # Descripción
# MAGIC En algún momento se crearon tablas que venían indexadas por usuario.  
# MAGIC Algo como:  `loan_contract_transactions_01000000032_777_mx`  
# MAGIC 
# MAGIC Se borran. 

# COMMAND ----------

to_erase = spark.sql("""
    SHOW TABLES FROM bronze LIKE 'loan_contract_transactions_[0-9]{11}_[0-9]{3}_mx'
    """)
display(to_erase)

# COMMAND ----------

for iter_erase in to_erase.collect():
    spark.sql(f"DROP TABLE IF EXISTS bronze.{iter_erase['tableName']}")

# COMMAND ----------

# MAGIC %sql 
# MAGIC DESCRIBE TABLE bronze.loan_contracts; 
