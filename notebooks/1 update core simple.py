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

from importlib import reload
import config
reload(config)

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
base_location = f"abfss://{at_storage}.dfs.core.windows.net/ops/core-banking-batch-updates"


# COMMAND ----------

first_time = False 

persons_dict = DBKS_TABLES[ENV]['brz_persons']
loans_dict   = DBKS_TABLES[ENV]['brz_loans']

loc_2_delta = """CREATE TABLE {name} USING DELTA LOCATION "abfss://bronze@{location}";"""

# COMMAND ----------

persons_df = core_session.get_persons()
persons_spk = spark.createDataFrame(persons_df)

loans_df  = core_session.get_loans()
loans_spk = spark.createDataFrame(loans_df)


# COMMAND ----------

first_time = False 
az_storage = az_manager.get_storage()

loc_2_delta = """CREATE TABLE {name} USING DELTA LOCATION "abfss://bronze@{az_storage}.dfs.core.windows.net/{location}";"""

if first_time: 
    (persons_spk.write
         .format('delta').mode('overwrite')
         .save(f"abfss://bronze@{persons_dict['location']}"))

    (loans_spk.write
         .format('delta').mode('overwrite')
         .save(f"abfss://bronze@{az_storage}.dfs.core.windows.net/{loans_dict['location']}"))

if first_time: 
    spark.sql(loc_2_delta.format(**persons_dict))
    spark.sql(loc_2_delta.format(**loans_dict))

else: 
    (persons_spk.write
        .format('delta').mode('overwrite')
        .option('overwriteSchema', True)
        .save(f"abfss://bronze@{az_storage}.dfs.core.windows.net/{persons_dict['location']}"))
              
    (loans_spk.write
        .format('delta').mode('overwrite')
        .option('overwriteSchema', True)
        .save(f"abfss://bronze@{az_storage}.dfs.core.windows.net/{loans_dict['location']}"))

# COMMAND ----------

loans_dict['location']

# COMMAND ----------

dbutils.fs.ls(f"abfss://bronze@{az_storage}.dfs.core.windows.net/{loans_dict['location']}")
