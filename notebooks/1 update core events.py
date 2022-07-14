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

from datetime import datetime as dt, timedelta as delta
from src.core_banking import SAPSession
from src.platform_resources import AzureResourcer
from config import ConfigEnviron, ENV, SERVER

app_environ = ConfigEnviron(ENV, SERVER, spark)
az_manager  = AzureResourcer(app_environ)
core_session = SAPSession('qas-sap', az_manager)

at_storage = az_manager.get_storage()
az_manager.set_dbks_permissions(at_storage)
base_location = f"{at_storage}.dfs.core.windows.net/ops/core-banking-batch-events"

# COMMAND ----------

first_time = False 

persons_dict = {
    'name'  : "din_clients.brz_ops_persons_inc", 
    'stage' : "bronze",
    'location' : f"{base_location}/persons"}

accounts_dict = {
    'name'  : "nayru_accounts.brz_ops_accounts_inc", 
    'stage' : "bronze",
    'location' : f"{base_location}/accounts"}

txns_dict = {
    'name'  : "farore_transactions.brz_ops_transactions_inc", 
    'stage' : "bronze",
    'location' : f"{base_location}/transactions"}

loc_2_delta = """CREATE TABLE {name} USING DELTA LOCATION "abfss://{stage}@{location}";"""

# COMMAND ----------

month_ago = dt.now() - delta(days=30)

transactions_df = core_session.get_events(event_type='transaction', date_from=month_ago)



# COMMAND ----------

transactions_spk = spark.createDataFrame(transactions_df)


# COMMAND ----------

(transactions_spk.write
         .format('delta').mode('overwrite')
         .save(f"abfss://{txns_dict['stage']}@{txns_dict['location']}"))

if first_time: 
    spark.sql(loc_2_delta.format(**txns_dict))
    

