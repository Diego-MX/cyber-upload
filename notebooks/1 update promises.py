# Databricks notebook source
# MAGIC %md 
# MAGIC ## Description
# MAGIC 
# MAGIC The Databricks table `bronze.crm_payment_promises` is updated every hour via this script.   
# MAGIC We require access to:  
# MAGIC - Keyvault `kv-resource-access-dbks` via Databricks scope of the same name.
# MAGIC - This in turn yields keys towards a Service Principal, which in turn gives acces to other secrets. 
# MAGIC 
# MAGIC The corresponding key names are found in `config.py`.

# COMMAND ----------

# MAGIC %md 
# MAGIC #### Libraries and Basic Functions

# COMMAND ----------

# MAGIC  %pip install -r ../reqs_dbks.txt

# COMMAND ----------

# MAGIC %md 
# MAGIC # Descripción
# MAGIC ... 

# COMMAND ----------

from importlib import reload
from src import crm_platform
reload(crm_platform)


# COMMAND ----------

from config import ConfigEnviron, ENV, SERVER, CRM_ENV, DBKS_TABLES
from src.platform_resources import AzureResourcer
from src.crm_platform import ZendeskSession

secretter = ConfigEnviron(ENV, SERVER, spark)
azure_getter = AzureResourcer(secretter)
zendesker = ZendeskSession(CRM_ENV, azure_getter)

at_storage = azure_getter.get_storage()

abfss_loc = DBKS_TABLES[ENV]['promises'].format(stage='bronze', storage=at_storage)
tbl_items = DBKS_TABLES[ENV]['items']


# COMMAND ----------

# MAGIC %md
# MAGIC # ToDo
# MAGIC 1. Hacer el query incremental. 
# MAGIC 2. Poner la tabla en Datalake. 

# COMMAND ----------

promises_meta = tbl_items['brz_promises']
promises_tbl = promises_meta[2] if len(promises_meta) > 2 else promises_meta[0]

promises_df = zendesker.get_promises().drop(columns='external_id')
promises_spk = spark.createDataFrame(promises_df)
(promises_spk.write.mode('overwrite')
        .format('delta')
        .save(f"{abfss_loc}/promises"))

