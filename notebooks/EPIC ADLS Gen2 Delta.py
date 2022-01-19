# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC ## Montado Azure Data Lake Storage Gen2
# MAGIC 1. Se utiliza la autenticación OAuth 2.0 
# MAGIC 1. Se establece el punto de montado a través de Databricks API

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": "c24684d2-ab07-44cc-97a3-93879a05cdea",
          "fs.azure.account.oauth2.client.secret": "j-zMz.~EXfb817CLQlX-jS14x55OLgs8.j",   # sin usar key vault
          "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/a0fcc570-3dbb-47c8-8d90-ffa096affa17/oauth2/token"}

# Optionally, you can add <directory-name> to the source URI of your mount point.
dbutils.fs.mount(
  source = "abfss://appbackup@dbrickscoachdl.dfs.core.windows.net/",
  mount_point = "/mnt/adls",
  extra_configs = configs)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ##Desmontado
# MAGIC Si se desea desmontar se ejecuta el siguiente comando

# COMMAND ----------

#dbutils.fs.unmount("/mnt/adls")

# COMMAND ----------

# MAGIC %md
# MAGIC Se visualiza el contenido que se montó

# COMMAND ----------

dbutils.fs.ls("/mnt/adls")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS bronze;

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Bronze table
# MAGIC Se genera la tabla bonze, la cual contiene los datos de origen sin ninguna modificación

# COMMAND ----------

# MAGIC %sql 
# MAGIC DROP TABLE IF EXISTS bronze.tabla_log_1;
# MAGIC CREATE TABLE bronze.tabla_log_1 USING text OPTIONS (
# MAGIC   path "dbfs:/mnt/adls/api-temsyork_202011070257.log",
# MAGIC   inferSchema "true"
# MAGIC );

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Vista previa
# MAGIC Se hace una vista previa de tabla creada

# COMMAND ----------

# MAGIC %sql DESCRIBE EXTENDED bronze.tabla_log_1

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   *
# MAGIC FROM
# MAGIC   bronze.tabla_log_1 TABLESAMPLE (5 ROWS)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Creación tabla Delta Silver
# MAGIC Se crea la tabla Delta que contiene los siguientes elementos:
# MAGIC 1. Archivos Delta
# MAGIC 1. Log de transacciones Delta
# MAGIC 1. Metastore

# COMMAND ----------

# MAGIC %sql CREATE
# MAGIC OR REPLACE TABLE tabla_log_silver USING DELTA LOCATION '/mnt/adls/DeltaTables/Silver' AS(
# MAGIC   SELECT
# MAGIC     *
# MAGIC   FROM
# MAGIC     bronze.tabla_log_1
# MAGIC ) 

# COMMAND ----------

# MAGIC %md
# MAGIC Se usa `DESCRBE DETAIL` para visalizar los detalles de la tabla `delta`

# COMMAND ----------

# MAGIC %sql DESCRIBE DETAIL tabla_log_silver

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Nuevos datos
# MAGIC Se leen nuevos datos para incorporarse a los existentes

# COMMAND ----------

# MAGIC %sql DROP TABLE IF EXISTS bronze.tabla_log_2;
# MAGIC CREATE TABLE bronze.tabla_log_2 USING text OPTIONS (
# MAGIC   path "/mnt/adls/tems-york_202011070245.log",
# MAGIC   inferSchema "true"
# MAGIC );

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Incorporar datos
# MAGIC Se incorporan los nuevos datos a la tabla delta silver

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO 
# MAGIC tabla_log_silver
# MAGIC SELECT * FROM bronze.tabla_log_2

# COMMAND ----------

# MAGIC %md
# MAGIC Se cuentan los registros de la tabla en su versión inicial

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM tabla_log_silver VERSION AS OF 0

# COMMAND ----------

# MAGIC %md
# MAGIC Se cuentan los registros de la tabla actual

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM tabla_log_silver

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Creación tabla Delta Gold

# COMMAND ----------

# MAGIC %sql CREATE
# MAGIC OR REPLACE TABLE tabla_log_gold USING DELTA LOCATION '/mnt/adls/DeltaTables/Gold' AS(
# MAGIC   SELECT
# MAGIC     UCASE(value) AS detalle
# MAGIC   FROM
# MAGIC     default.tabla_log_silver
# MAGIC ) 

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC *
# MAGIC FROM
# MAGIC tabla_log_gold

# COMMAND ----------

spark.read.format('delta').load('/mnt/adls/DeltaTables/Gold').show()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM delta.`/mnt/adls/DeltaTables/Silver`

# COMMAND ----------

display(dbutils.fs.ls('/mnt/adls/DeltaTables/Silver/_delta_log'))

# COMMAND ----------

j0 = spark.read.json("dbfs:/mnt/adls/DeltaTables/Silver/_delta_log/00000000000000000001.json")

# COMMAND ----------

display(j0.select('commitInfo').where("commitInfo is not null"))
