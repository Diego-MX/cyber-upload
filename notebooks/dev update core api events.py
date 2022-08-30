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
from src import core_banking
reload(core_banking)

from datetime import datetime as dt, timedelta as delta
from src.core_banking import SAPSession, update_dataframe, create_delta
from src.platform_resources import AzureResourcer
from config import ConfigEnviron, ENV, SERVER

app_environ = ConfigEnviron(ENV, SERVER, spark)
az_manager = AzureResourcer(app_environ)
core_session = SAPSession('qas-sap', az_manager)

spark.conf.set('spark.databricks.delta.schema.autoMerge.enabled', 'true')

# COMMAND ----------

at_storage = az_manager.get_storage()
az_manager.set_dbks_permissions(at_storage)
base_location = f"{at_storage}.dfs.core.windows.net/ops/core-banking-batch-events"

# COMMAND ----------

first_time = False 

events_dict = {
    'name'  : "farore_transactions.brz_ops_events_inc", 
    'stage' : "bronze",
    'location' : f"{base_location}/events"}

persons_dict = {
    'name'  : "din_clients.brz_ops_persons_inc", 
    'stage' : "bronze",
    'location' : f"{base_location}/persons"}

accounts_dict = {
    'name'  : "nayru_accounts.brz_ops_accounts_inc", 
    'stage' : "bronze",
    'location' : f"{base_location}/accounts"}

txns_dict = {
    'name'     : "farore_transactions.brz_ops_transactions_inc", 
    'stage'    : "bronze",
    'location' : f"{base_location}/transactions"}

events_path = f"abfss://{events_dict['stage']}@{events_dict['location']}"

# It's run every hour, but give we give it some room. 
prev_update = dt.now() - delta(days=10)

# COMMAND ----------

print(data_df.text)

# COMMAND ----------

accounts_path   = f"abfss://{accounts_dict['stage']}@{accounts_dict['location']}"

data_df = core_session.get_events(
    event_type='accounts', date_lim=prev_update, output='Response')



# COMMAND ----------

persons_path   = f"abfss://{persons_dict['stage']}@{persons_dict['location']}"

(data_df, events_df) = core_session.get_events(
    event_type='persons', date_lim=prev_update, output='WithMetadata')

update_dataframe(spark, data_df, persons_path, 'ID')

update_dataframe(spark, events_df, events_path, 'EventID')

# COMMAND ----------

print(data_df.columns)
print(older.columns)

# COMMAND ----------

txns_path = f"abfss://{txns_dict['stage']}@{txns_dict['location']}"

(data_df, events_df) = core_session.get_events(
    event_type='transactions', date_lim=prev_update, output='WithMetadata')

update_dataframe(spark, data_df, txns_path, 'TransactionID')

update_dataframe(spark, events_df, events_path, 'EventID')


# COMMAND ----------

data_df
