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
os.environ['ENV'] = 'dbks'

# COMMAND ----------

from datetime import datetime as dt
from src.core_banking import SAPSession
from src.platform_resources import AzureResourcer
from config import ConfigEnviron

secretter = ConfigEnviron('dbks', spark=spark)
azure_getter = AzureResourcer('local', secretter)
core_session = SAPSession('qas', azure_getter)

# COMMAND ----------

loans_df     = core_session.get_loans()
loans_qan_df = core_session.get_loans_qan()
persons_df   = core_session.get_persons()




# COMMAND ----------

loans_spk   = spark.createDataFrame(loans_df)
loans_qan   = spark.createDataFrame(loans_qan_df)
persons_spk = spark.createDataFrame(persons_df)



# COMMAND ----------


loans_spk.write.mode("overwrite").saveAsTable("bronze.loan_contracts")
loans_spk.write.mode("overwrite").saveAsTable("bronze.loan_qan_contracts")
persons_spk.write.mode("overwrite").saveAsTable("bronze.persons_set")
