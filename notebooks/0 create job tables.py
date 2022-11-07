# Databricks notebook source
# MAGIC %md 
# MAGIC # Objetivo
# MAGIC 
# MAGIC El archivo `config.py` contiene un objeto de las tablas que se usan en este repositorio.   
# MAGIC En este _notebook_ verificamos que cada una de ellas exista con las configuraciones correspondientes:  
# MAGIC - En primer lugar, la ubicación en el _datalake_. 
# MAGIC - En segundo lugar, el apuntador del _metastore_ cuando éste lo requiere.  
# MAGIC   Cabe considerar las tablas que sólo se utilizan dentro de este repositorio, para las que no necesitamos agregar al _metastore_.  
# MAGIC   Se identifican mediante el indicador en `config.py` pero mientras no se requiera, no se definen en el metastore.  

# COMMAND ----------

# MAGIC %pip install -r ../reqs_dbks.txt

# COMMAND ----------

from datetime import datetime as dt
from src.core_banking import SAPSession
from src.platform_resources import AzureResourcer
from config import ConfigEnviron, ENV, SERVER, DBKS_TABLES, CORE_ENV

app_environ = ConfigEnviron(ENV, SERVER, spark)
az_manager  = AzureResourcer(app_environ)
core_session = SAPSession(CORE_ENV, az_manager)

at_storage = az_manager.get_storage()
az_manager.set_dbks_permissions(at_storage)

create_clause = "CREATE TABLE {} USING DELTA LOCATION '{}'"

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA nayru_accounts

# COMMAND ----------

table_tuple = ("nayru_accounts.gld_cx_collections_loans", 
    "abfss://gold@stlakehyliastg.dfs.core.windows.net/ops/core-banking/batch-updates/loan-contracts")

dbutils.fs.ls(table_tuple[1])

# COMMAND ----------

print(    create_clause.format(*table_tuple))
spark.sql(create_clause.format(*table_tuple))
