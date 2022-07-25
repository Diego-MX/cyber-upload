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

from config import ConfigEnviron, ENV, SERVER
from src.platform_resources import AzureResourcer
from src.crm_platform import ZendeskSession

secretter = ConfigEnviron(ENV, SERVER, spark)
azurer_getter = AzureResourcer(secretter)
zendesk = ZendeskSession('sandbox', azurer_getter)


# COMMAND ----------

# MAGIC %md
# MAGIC # ToDo
# MAGIC 1. Hacer el query incremental. 
# MAGIC 2. Poner la tabla en Datalake. 

# COMMAND ----------

promises_df = zendesk.get_promises().drop(columns='external_id')
promises_spk = spark.createDataFrame(promises_df)
promises_spk.write.mode('overwrite').saveAsTable('bronze.crm_payment_promises')


# COMMAND ----------

# MAGIC %sql 
# MAGIC DESCRIBE TABLE EXTENDED bronze.crm_payment_promises
