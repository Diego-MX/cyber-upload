# Databricks notebook source
# MAGIC %md 
# MAGIC # Objetivo
# MAGIC 
# MAGIC Al crear las tablas Δ, no se había puesto atención en la ubicación física... (que no es tan física, pero es lo más físico a lo que aspiran las ubicaciones de tablas).  
# MAGIC Después de algunos meses se descubrió que tienen que estar en cierta ubicación.  
# MAGIC En este _notebook_ movemos las tablas al Lago Hylia. 

# COMMAND ----------

from datetime import datetime as dt
import re
from delta.tables import DeltaTable
from pyspark.sql import functions as F, types as T, Window as W

from src.platform_resources import AzureResourcer
from config import ConfigEnviron, ENV, SERVER, DBKS_TABLES

tbl_items = DBKS_TABLES[ENV]['items']

app_environ = ConfigEnviron(ENV, SERVER, spark)
az_manager  = AzureResourcer(app_environ)

at_storage = az_manager.get_storage()
az_manager.set_dbks_permissions(at_storage)

# Sustituye el placeholder AT_STORAGE, aunque mantiene STAGE para sustituirse después. 
base_location = DBKS_TABLES[ENV]['base']
abfss_loc = base_location.format(stage='gold', storage =at_storage)


# COMMAND ----------

# MAGIC %sql 
# MAGIC CREATE TABLE nayru_accounts.gld_cx_collections_loans USING DELTA
# MAGIC LOCATION "abfss://gold@stlakehyliaqas.dfs.core.windows.net/ops/core-banking/batch-updates/loan-contracts";
