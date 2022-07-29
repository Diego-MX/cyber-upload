# Databricks notebook source
# MAGIC %md 
# MAGIC # Objetivo
# MAGIC 
# MAGIC Al crear las tablas Δ, no se habían definido una ubicación física específica.  
# MAGIC Pero las tablas deben vivir en el _datalake_, entonces les hacemos clones a las mismas para aprovisionarlas de una dirección física.  
# MAGIC Adicional a esto, en el _notebook_ `0 clone_at_metastore` las recreamos para que sean accesibles desde los dos _metastores_ correspondientes. 

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

# COMMAND ----------

from importlib import reload
import config
reload(config)

# COMMAND ----------

exclude_tbls = set('slv_promises')

base_dir = DBKS_TABLES[ENV]['base']  # {stage} : checkout placeholder. 

create_clause = 'CREATE TABLE IF NOT EXISTS delta.`{}` CLONE {}'

stage_keys = {
    'brz': 'bronze',
    'slv': 'silver',
    'gld': 'gold'}

# COMMAND ----------

tbl_items = DBKS_TABLES[ENV]['items']
 
for tbl_key in set(tbl_items).difference(exclude_tbls):
    name, delta, old_name = tbl_items[tbl_key]
    abfss_dir = base_dir.format(stage=stage_keys[tbl_key[0:3]])
    tbl_loctn = f"{abfss_dir}/{delta}"
    sql_clause = create_clause.format(tbl_loctn, old_name) 
    print(sql_clause)
    try: 
        spark.sql(sql_clause)
    except Exception: 
        pass
    
