# Databricks notebook source
# MAGIC %md 
# MAGIC # Objetivo
# MAGIC 
# MAGIC El archivo `config.py` contiene un objeto de las tablas que se usan en este repositorio.   
# MAGIC En este _notebook_ verificamos que cada una de ellas exista con las configuraciones correspondientes:  
# MAGIC - En primer lugar, la ubicación en el _datalake_. 
# MAGIC - En segundo lugar, el apuntador del _metastore_ cuando éste lo requiere.  
# MAGIC   Cabe considerar las tablas que sólo se utilizan dentro de este repositorio, para las que no necesitamos agregar al _metastore_.  
# MAGIC   Se identifican mediante el indicador en `config.py` pero mientras no se requiera, no se definen en el metastore.  

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

# COMMAND ----------

exclude_tbls = set(['slv_promises'])

base_dir = DBKS_TABLES[ENV]['base']  # {stage, storage} : checkout placeholder. 
tbl_items = DBKS_TABLES[ENV]['items']

# ... IF NOT EXISTS ... no funciona con tablas (datalake + metastore)
create_clause = "CREATE TABLE {} LOCATION '{}'"
alter_clause = "ALTER TABLE {} SET LOCATION '{}'"

stage_keys = {
    'brz': 'bronze',
    'slv': 'silver',
    'gld': 'gold'}

# COMMAND ----------

table_tuple = ("nayru_accounts.gld_cx_collections_loans", 
    "abfss://gold@stlakehyliaprd.dfs.core.windows.net/ops/core-banking/batch-updates/loan-contracts")

print(    create_clause.format(*table_tuple))
spark.sql(create_clause.format(*table_tuple))

# COMMAND ----------

experimental = False
if experimental: 
    for tbl_key in set(tbl_items).difference(exclude_tbls):
        name, the_delta = tbl_items[tbl_key]
        stg_container = stage_keys[tbl_key[0:3]]
        abfss_dir = base_dir.format(stage=stg_container, storage=at_storage)
        tbl_loctn = f"{abfss_dir}/{the_delta}"
        sql_create = create_clause.format(name, tbl_loctn) 
        print(sql_alter)
        try: 
            spark.sql(sql_alter)
        except Exception: 
            pass

    
