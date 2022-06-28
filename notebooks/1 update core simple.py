# Databricks notebook source
# MAGIC %md 
# MAGIC ## Description
# MAGIC 
# MAGIC This notebook is tied to Databricks job that runs every hour.  
# MAGIC 
# MAGIC It requires the following accesses:  
# MAGIC - Keyvault `kv-resource-access-dbks` through its corresponding Databricks scope of the same name.  
# MAGIC - Access keys in there are set to then accesss a Secret Scope, that has access to secondary keyvault. 
# MAGIC   The names are stored in config file:  `./config.py`.

# COMMAND ----------

# MAGIC %pip install -r ../requirements.txt

# COMMAND ----------

from datetime import datetime as dt
from src.core_banking import SAPSession
from src.platform_resources import AzureResourcer
from config import VaultSetter

secretter = VaultSetter('dbks')
azure_getter = AzureResourcer('local', secretter)
core_session = SAPSession('qas', azure_getter)

# COMMAND ----------

loans_df   = core_session.get_loans("all")
persons_df = core_session.get_persons()

loans_spk   = spark.createDataFrame(loans_df)
persons_spk = spark.createDataFrame(persons_df)

loans_spk.write.mode("overwrite").saveAsTable("bronze.loan_contracts")
persons_spk.write.mode("overwrite").saveAsTable("bronze.persons_set")

