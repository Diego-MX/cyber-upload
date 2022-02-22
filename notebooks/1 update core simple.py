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

# MAGIC %pip install azure-identity azure-keyvault-secrets python-dotenv

# COMMAND ----------

import os, sys
import pandas as pd
from pathlib import Path
from datetime import datetime as dt
import pyspark.sql.types as T
from itertools import product

from requests import get as rq_get, post

from azure.identity import ClientSecretCredential, DefaultAzureCredential
from azure.keyvault.secrets import SecretClient

# from src import core_banking as core
from src.utilities import tools
from src.core_banking import SAPSession

from config import ConfigEnviron, ENV, CORE_KEYS

environ = ConfigEnviron(ENV)
core_session = SAPSession(CORE_KEYS['qas'])

# Notice QAS refers to CORE Banking access, not the proper development. 


# COMMAND ----------

# MAGIC %md
# MAGIC #### Key Vaults and Env Variables

# COMMAND ----------

from src.core_banking import SAPSession
from src.platform_resources import AzureResourcer
from config import CORE_KEYS, ENV

the_secretter = ConfigEnviron(ENV)
the_resourcer = AzureResourcer(the_secretter)
core_session  = SAPSession('qas', the_resourcer)

# COMMAND ----------

# MAGIC %md 
# MAGIC #### Execute commands

# COMMAND ----------

loans_df    = core_session.get_loans("all")
persons_df  = core_session.get_persons()



# COMMAND ----------

loans_spk    = spark.createDataFrame(loans_df)
persons_spk  = spark.createDataFrame(persons_df)


# COMMAND ----------

loans_spk.write.mode("overwrite").saveAsTable("bronze.loan_contracts")
persons_spk.write.mode("overwrite").saveAsTable("bronze.persons_set")


