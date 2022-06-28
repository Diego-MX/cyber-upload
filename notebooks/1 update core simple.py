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

import os
os.environ['ENV_TYPE'] = 'dev'
os.environ['SERVER_TYPE'] = 'dbks'

# COMMAND ----------

from importlib import reload
import config
reload(config)
from src import core_banking
reload(core_banking)

# COMMAND ----------

from config import CORE_KEYS
the_access = CORE_KEYS['qas-sap']['main']['access']
the_access.keys()


# COMMAND ----------

CORE_KEYS['qas-sap']['main']['access']

# COMMAND ----------

from datetime import datetime as dt
from src.core_banking import SAPSession
from src.platform_resources import AzureResourcer
from config import ConfigEnviron, ENV, SERVER

app_environ = ConfigEnviron(ENV, SERVER, spark)
azure_getter = AzureResourcer(app_environ)
core_session = SAPSession('qas-sap', azure_getter)

# COMMAND ----------

persons_spk.count()

# COMMAND ----------

persons_df = core_session.get_persons()
persons_spk = spark.createDataFrame(persons_df)
display(persons_spk)


# COMMAND ----------

persons_spk.write.format('delta').mode('overwrite').saveAsTable("din_clients.brz_core_persons_set")


# COMMAND ----------

loans_df  = core_session.get_loans("all")
loans_spk = spark.createDataFrame(loans_df)

display(loans_spk)


# COMMAND ----------

loans_spk.write.mode("overwrite").saveAsTable("bronze.loan_contracts")
