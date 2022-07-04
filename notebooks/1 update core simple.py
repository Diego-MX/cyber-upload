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
from config import ConfigEnviron, ENV, SERVER

app_environ = ConfigEnviron(ENV, SERVER, spark)
azure_getter = AzureResourcer(app_environ)
core_session = SAPSession('qas-sap', azure_getter)

# COMMAND ----------

persons_df = core_session.get_persons()
persons_spk = spark.createDataFrame(persons_df)

loans_df  = core_session.get_loans()
loans_spk = spark.createDataFrame(loans_df)


# COMMAND ----------

first_time = False 

loc_2_delta = """CREATE TABLE {name} USING DELTA LOCATION "abfss://{stage}@{location}";"""

persons_dict = {
    'name'  : "din_clients.brz_ops_persons_set", 
    'stage' : "bronze",
    'location' : "stlakehyliaqas.dfs.core.windows.net/ops/core-banking/batch-updates/persons-set"}
loans_dict = {
    'name'  : "nayru_accounts.brz_ops_loan_contracts", 
    'stage' : "bronze",
    'location' : "stlakehyliaqas.dfs.core.windows.net/ops/core-banking/batch-updates/loan-contracts"}



# COMMAND ----------

if first_time: 
    (persons_spk.write
         .format('delta').mode('overwrite')
         .save(f"abfss://{persons_dict['stage']}@{persons_dict['location']}"))
    spark.sql(loc_2_delta.format(**persons_dict))
    
    (loans_spk.write
         .format('delta').mode('overwrite')
         .save(f"abfss://{loans_dict['stage']}@{loans_dict['location']}"))
    spark.sql(loc_2_delta.format(**loans_dict))
else: 
    (persons_spk.write
        .format('delta').mode('overwrite')
        .saveAsTable(persons_dict['name']))
              
    (loans_spk.write
         .format('delta').mode('overwrite')
         .saveAsTable(loans_dict['name']))

# COMMAND ----------


