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

# COMMAND ----------

from datetime import datetime as dt, timedelta as delta
from delta.tables import DeltaTable

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

loc_2_delta = (lambda a_dict: 
    """CREATE TABLE {name} USING DELTA LOCATION "abfss://{stage}@{location}";""".format(**a_dict))

# COMMAND ----------

(data_df, events_df) = core_session.get_events(event_type='transactions', date_from=prev_update, output='WithMetadata')


# COMMAND ----------

event_date = events_df['EventDateTime'].str.extract(r"/Date\(([0-9]*)\)/")
type(event_date)

# COMMAND ----------

events_df

# COMMAND ----------

mod_time = (events_df
    .assign(EventDateTime2 = lambda df: df['EventDateTime'].str.extract(r"/Date\(([0-9]*)\)/"))
    .assign(EventDateTime3 = lambda df: pd.to_datetime(df['EventDateTime2'], unit='ms')) )
mod_time

# COMMAND ----------

prev_update = dt.now() - delta(days=2)

transactions_df = core_session.get_events(event_type='transactions', date_from=prev_update)


# COMMAND ----------



# COMMAND ----------

new_data = spark.createDataFrame(data_df)

prev_data = DeltaTable.forPath(spark, f"abfss://{txns_dict['stage']}@{txns_dict['location']}")
tbl_cols = {a_col : f'updates.{a_col}' for a_col in new_data.columns}

# Confirmar que sí escribe la tabla de regreso:  todo pinta bien con el EXECUTE. 
# Sólo aplica para transacciones por la columna 'TransactionID'
(prev_data.alias('prev')
    .merge(new_data.alias('new'), 'prev.TransactionID = new.TransactionID')
    .whenMatchedUpdate(set = tbl_cols)
    .whenNotMatchedInsert(values = tbl_cols)
    .execute())

# COMMAND ----------

if False: 
    txn_df = spark.read.load(f"abfss://{txns_dict['stage']}@{txns_dict['location']}")
    display(txn_df)

# COMMAND ----------

first_time = True  # Create Table
if first_time: 
    spark.sql(loc_2_delta(events_dict))
