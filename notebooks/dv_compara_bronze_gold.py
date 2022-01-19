# Databricks notebook source
# MAGIC %md 
# MAGIC 
# MAGIC # Introducción
# MAGIC 
# MAGIC Las tablas de `bronze` 🥉 son casi tal como las recibimos de las fuentes.  
# MAGIC En las primeras versiones de la aplicación utilizamos estas tablas para regresar resultados. 
# MAGIC 
# MAGIC A su vez, las tablas en `gold` 🥇 incorporan los nombres proporcionados por el equipo de CX.  
# MAGIC De forma que tienen cercanía con las áreas funcionales.  
# MAGIC 
# MAGIC En este _notebook_ nos aseguramos que las columnas por utilizar estén completas,  
# MAGIC y diseñamos lo que se requiera diseñar para hacer los matches de una versión a otra. 

# COMMAND ----------

gold = spark.table("gold.motor_cobranza")
display(gold)

# COMMAND ----------

display(loan_contracts.filter(loan_contracts.LoanContractID in gold.num_prestamo))


# COMMAND ----------



