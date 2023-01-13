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
from config import ConfigEnviron, ENV, SERVER, DBKS_TABLES, CORE_ENV

app_environ = ConfigEnviron(ENV, SERVER, spark)
az_manager  = AzureResourcer(app_environ)
core_session = SAPSession(CORE_ENV, az_manager)

at_storage = az_manager.get_storage()
az_manager.set_dbks_permissions(at_storage)

at_base = DBKS_TABLES[ENV]['base']  # con placeholders STAGE, STORAGE. 
table_items = DBKS_TABLES[ENV]['items'] 

abfss_loc = at_base.format(stage='bronze', storage=at_storage)

# COMMAND ----------

persons_df = core_session.get_persons()
persons_spk = spark.createDataFrame(persons_df)

loans_df  = core_session.get_loans()
loans_spk = spark.createDataFrame(loans_df)

lqan_df = core_session.get_loans_qan()
lqan_spk = spark.createDataFrame(lqan_df)

# COMMAND ----------

(persons_spk.write
    .format('delta').mode('overwrite')
    .option('overwriteSchema', True)
    .save(f"{abfss_loc}/{table_items['brz_persons'][1]}"))

(loans_spk.write
    .format('delta').mode('overwrite')
    .option('overwriteSchema', True)
    .save(f"{abfss_loc}/{table_items['brz_loans'][1]}"))
 
(lqan_spk.write
    .format('delta').mode('overwrite')
    .option('overwriteSchema', True)
    .save(f"{abfss_loc}/{table_items['brz_loans'][1]}"))


# COMMAND ----------

loans_brz = (spark.read.format('delta')
    .load(f"{abfss_loc}/{table_items['brz_loans'][1]}"))
display(loans_brz)

# COMMAND ----------

persons_brz = (spark.read.format('delta')
    .load(f"{abfss_loc}/{table_items['brz_persons'][1]}"))
display(persons_brz)
