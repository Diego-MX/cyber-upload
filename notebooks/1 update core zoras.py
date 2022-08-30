# Databricks notebook source
# MAGIC %md 
# MAGIC ## Description
# MAGIC 
# MAGIC This is a copy of notebook `1 update core simple`.  
# MAGIC Due to technical consistency (unfortunately) and can only be executed in the original Zoras workspace in its corresponding cluster. 
# MAGIC 
# MAGIC After moving `Person Set` and `Loan Contract` to both metastore/datalake and QAs, there was rupture in the data tables.  
# MAGIC The purpose of this notebook is to allow us to temporarily set back the previous changes, for the big event of Collections Testing.  
# MAGIC The only difference between this and its original, is the writing location of the tables.  
# MAGIC That is to say, this writes them as tables persé and in the cluster corresponding to DBFS;  
# MAGIC whereas the newer modified one writes them directly in their delta location in the datalake.   

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

at_base     = DBKS_TABLES[ENV]['base']
table_items = DBKS_TABLES[ENV]['items'] 


# COMMAND ----------

first_time = False 

abfss_loc = at_base.format(stage='bronze')

# table_tuple:  new_name, location, old_name
def set_table_delta(a_tuple, spk_session):
    table_loc = f"{abfss_loc}/{a_tuple[1]}"
    the_clause = f"CREATE TABLE {a_dict['name']} USING DELTA LOCATION \"{abfss_loc}\";"
    print(the_clause)
    #spk_session.sql(the_clause)


# COMMAND ----------

persons_df = core_session.get_persons()
persons_spk = spark.createDataFrame(persons_df)

loans_df  = core_session.get_loans()
loans_spk = spark.createDataFrame(loans_df)


# COMMAND ----------

(persons_spk.write
    .format('delta').mode('overwrite')
    .option('overwriteSchema', True)
    .saveAsTable(table_items['brz_persons'][2]))

(loans_spk.write
    .format('delta').mode('overwrite')
    .option('overwriteSchema', True)
    .saveAsTable(table_items['brz_loans'][2]))

if first_time: 
    set_table_delta(persons_dict, spark)
    set_table_delta(loans_dict, spark)
 

