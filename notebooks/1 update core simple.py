# Databricks notebook source
# MAGIC %md 
# MAGIC ## Description
# MAGIC 
# MAGIC This notebook is tied to Databricks job that runs every hour.   
# MAGIC 0. Preparation of variables `ENV_TYPE` (`dev`, `qas`, `stg`, ...) and `SERVER_TYPE` (`dbks`, `local`, `wap`).  
# MAGIC    This is usually done at a server level, but can also be handled in a script or notebook.  
# MAGIC    `import os; os.environ['ENV_TYPE'] = 'qas'`
# MAGIC 
# MAGIC 1. Check `config.py` for configuration options.  As may be anticipated, some depend on `ENV_TYPE` and `SERVER_TYPE`.  
# MAGIC    One thing to note, the service principal in `SETUP_KEYS` must have been previously given access to the resources in `PLATFORM_KEYS`.  
# MAGIC    Moreover, specific resource configuration may need to be handled along the way;  
# MAGIC    Eg.1 Key Vault Access Policies for the service principal to read secrets.  
# MAGIC    Eg.2 May require fine grained permissions in datalake. 
# MAGIC 
# MAGIC 2. Object classes `SAPSession`, `AzureResourcer`, `ConfigEnviron` use `config.py` under the hood.  
# MAGIC     ... and so does this notebook.  

# COMMAND ----------

# MAGIC %pip install -r ../reqs_dbks.txt

# COMMAND ----------

from datetime import datetime as dt
from src.core_banking import SAPSession
from src.platform_resources import AzureResourcer
from config import ConfigEnviron, ENV, SERVER, DBKS_TABLES

app_environ = ConfigEnviron(ENV, SERVER, spark)
az_manager  = AzureResourcer(app_environ)
core_session = SAPSession('qas-sap', az_manager)

at_storage = az_manager.get_storage()
az_manager.set_dbks_permissions(at_storage)

base_location = f"abfss://bronze@{at_storage}.dfs.core.windows.net/ops/core-banking-batch-updates"


# COMMAND ----------

at_storage = az_manager.get_storage() # Just ... again. 

first_time = False 

persons_dict = DBKS_TABLES[ENV]['brz_persons']
loans_dict   = DBKS_TABLES[ENV]['brz_loans']

def set_table_delta(a_dict, spk_session):
    abfss_loc = f"abfss://bronze@{at_storage}.dfs.core.windows.net/{a_dict['location']}"
    the_clause = f"CREATE TABLE {a_dict['name']} USING DELTA LOCATION \"{abfss_loc}\";"
    spk_session.sql(the_clause)


# COMMAND ----------

persons_df = core_session.get_persons()
persons_spk = spark.createDataFrame(persons_df)

loans_df  = core_session.get_loans()
loans_spk = spark.createDataFrame(loans_df)


# COMMAND ----------

(persons_spk.write
    .format('delta').mode('overwrite')
    .option('overwriteSchema', True)
    .save(f"abfss://bronze@{at_storage}.dfs.core.windows.net/{persons_dict['location']}"))

(loans_spk.write
    .format('delta').mode('overwrite')
    .option('overwriteSchema', True)
    .save(f"abfss://bronze@{at_storage}.dfs.core.windows.net/{loans_dict['location']}"))

if first_time: 
    set_table_delta(persons_dict)
    set_table_delta(loans_dict  )
 

