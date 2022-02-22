# Databricks notebook source
display(dbutils.fs.ls("/mnt/lakehylia2-raw/eh-ops-core-banking-dev"))

# COMMAND ----------

data = spark.read.format("avro").load("/mnt/lakehylia2-raw/eh-ops-core-banking-dev/person-set")


# COMMAND ----------

data.count()
