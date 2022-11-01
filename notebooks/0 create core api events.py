# Databricks notebook source
# MAGIC %md 
# MAGIC ## Description
# MAGIC 
# MAGIC This notebook is related to the corresponding `1 update core api events`.  
# MAGIC This is used for initiation, and maintenance of the correpsonding updates, 
# MAGIC while the other is run as an hourly Job.  

# COMMAND ----------

from importlib import reload
from src import core_banking
import config
reload(core_banking)
reload(config)

# COMMAND ----------

import pandas as pd
from datetime import datetime as dt, timedelta as delta, date
from delta.tables import DeltaTable
from json import dumps
from src.core_banking import SAPSession, update_dataframe, str_error
from src.platform_resources import AzureResourcer
from config import ConfigEnviron, ENV, SERVER

app_environ = ConfigEnviron(ENV, SERVER, spark)
az_manager  = AzureResourcer(app_environ)
core_session = SAPSession('qas-sap', az_manager)

# COMMAND ----------

# Toman tiempo y son independientes de los objetos del Core. 
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

dt_str = lambda a_dt: a_dt.strftime('%b-%d,%Y@ %H:%M')

# COMMAND ----------

# Update everyday. 
running_date = dt(2022, 7, 1)
base_skipper = 1
end_date = dt.today()

txns_path   = "abfss://{stage}@{location}".format(**txns_dict)
events_path = "abfss://{stage}@{location}".format(**events_dict)

live_loop = True
while running_date <= end_date and live_loop: 
    run_skipper = base_skipper
    
    live_loop = False
    print(f"Run date {dt_str(running_date)} and skipper {run_skipper} days, let's do it!")
    while (1/24 < run_skipper < 60): 
        try_date = running_date + delta(days=run_skipper)
        try: 
            (data_df, events_df) = core_session.get_events(
                    event_type='transactions', 
                    date_lim  =[running_date, try_date], 
                    output    ='WithMetadata')
            print(f"¡¡¡Succesful import!!!")
  
            update_dataframe(data_df, txns_path, 'TransactionID')
            update_dataframe(events_df, events_path, 'EventID')
            
            running_date = try_date
            live_loop = True
            break
        except TypeError: 
            run_skipper *= 3/2
            print(f"\tLarger Delta: {run_skipper:3.3f}".expandtabs(2))
            continue
        except Exception as err: 
            
            print(f"\tOther error: {str(err)}".expandtabs(2))
            break
    
    if not(1/24 < run_skipper < 60): 
        print(f"\tDate doesn't work: {dt_str(running_date)}".expandtabs(2))
        break

    

# COMMAND ----------

first_time = False  # Create Table
if first_time: 
    data_spk = spark.createDataFrame(events_df)
    
    (data_spk.write
         .format('delta').mode('overwrite')
         .save(f"abfss://{events_dict['stage']}@{events_dict['location']}"))
    
    spark.sql(loc_2_delta(events_dict))
    

# COMMAND ----------

txns_set = spark.read.table('farore_transactions.brz_ops_transactions_inc')
display(txns_set)
