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

from importlib import reload
from src import platform_resources
reload(platform_resources)

# COMMAND ----------

from datetime import datetime as dt
from src.core_banking import SAPSession
from src.platform_resources import AzureResourcer
from config import ConfigEnviron, ENV, SERVER

app_environ = ConfigEnviron(ENV, SERVER, spark)
azure_getter = AzureResourcer(app_environ)
core_session = SAPSession('qas-sap', azure_getter)

# COMMAND ----------

persons_df = core_session.get_persons()
persons_spk = spark.createDataFrame(persons_df)
display(persons_spk)


# COMMAND ----------

persons_spk.write.format('delta').mode('overwrite').saveAsTable("din_clients.brz_core_persons_set")


# COMMAND ----------

loans_df  = core_session.get_loans()
loans_spk = spark.createDataFrame(loans_df)

display(loans_spk)


# COMMAND ----------

loans_spk.write.mode("overwrite").saveAsTable("nayru_accounts.brz_ops_loan_contracts")
