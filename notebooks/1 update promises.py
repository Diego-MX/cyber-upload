# Databricks notebook source
# MAGIC %md 
# MAGIC ## Description
# MAGIC
# MAGIC The Databricks table `bronze.crm_payment_promises` is updated every hour via this script.   
# MAGIC We require access to:  
# MAGIC - Keyvault `kv-resource-access-dbks` via Databricks scope of the same name.
# MAGIC - This in turn yields keys towards a Service Principal, which in turn gives acces to other secrets. 
# MAGIC
# MAGIC The corresponding key names are found in `config.py`.

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Libraries and Basic Functions

# COMMAND ----------

# MAGIC   %pip install -q -r ../reqs_dbks.txt

# COMMAND ----------

from datetime import datetime as dt
import json
from pyspark.sql import (functions as F, types as T, 
    SparkSession)
from pyspark.dbutils import DBUtils
import re
import subprocess
import yaml

spark = SparkSession.builder.getOrCreate()
dbks_secrets = DBUtils(spark).secrets

with open("../user_databricks.yml", 'r') as _f: 
    u_dbks = yaml.safe_load(_f)

epicpy_load = {
    'url'   : 'github.com/Bineo2/data-python-tools.git', 
    'branch': 'dev-diego', 
    'token' :  dbks_secrets.get(u_dbks['dbks_scope'], u_dbks['dbks_token'])}

url_call = "git+https://{token}@{url}@{branch}".format(**epicpy_load)
subprocess.check_call(['pip', 'install', url_call])

# COMMAND ----------

from importlib import reload
import config; reload(config)

from src.crm_platform import ZendeskSession
from src.platform_resources import AzureResourcer
from config import (app_agent, app_resources, DATA_2, 
    ConfigEnviron, 
    ENV, SERVER, CRM_ENV, DBKS_TABLES, )

data_paths  = DATA_2['paths']
data_tables = DATA_2['tables']

stg_permissions = app_agent.prep_dbks_permissions(app_resources['storage'], 'gen2')
app_resources.set_dbks_permissions(stg_permissions)

abfss_brz = app_resources.get_resource_url('abfss', 'storage', 
    container='bronze', blob_path=data_paths['collections']) 
abfss_slv = app_resources.get_resource_url('abfss', 'storage', 
    container='silver', blob_path=data_paths['collections'])     

# Para cambiar estos elementos, requerimos habilitar CRMSession en EpicPy. 
secretter = ConfigEnviron(ENV, SERVER, spark)
azure_getter = AzureResourcer(secretter)
zendesker = ZendeskSession(CRM_ENV, azure_getter)



# COMMAND ----------

def unnest(c, s):
    unn = (None if c == '' 
            else json.loads(c)[s])
    return unn

udf_unnest = F.udf(unnest, T.IntegerType())

def date_format(c):
    pattern1 = re.compile("^[0-9]+/[0-9]+/[0-9]+$")
    pattern2 = re.compile("^[0-9]+-[0-9]+-[0-9]+T[0-9]+:[0-9]+:[0-9]+.[0-9]+Z$")
    pattern3 = re.compile("^[0-9]+-[0-9]+-[0-9]+T[0-9]+:[0-9]+:[0-9]+Z$")
    pattern4 = re.compile("^[0-9]+-[0-9]+-[0-9]+T[0-9]+:[0-9]+:[0-9]+.[0-9]+z$")
    if pattern1.match(c):
        return dt.strptime(c, "%d/%m/%Y").strftime("%Y-%m-%d")
    elif pattern2.match(c):
        return dt.strptime(c, "%Y-%m-%dT%H:%M:%S.%fZ").strftime("%Y-%m-%d")
    elif pattern3.match(c):
        return dt.strptime(c, "%Y-%m-%dT%H:%M:%SZ").strftime("%Y-%m-%d")
    elif pattern4.match(c):
        return dt.strptime(c, "%Y-%m-%dT%H:%M:%S.%fz").strftime("%Y-%m-%d")
    else:
        return None

udf_date_format = F.udf(date_format, T.StringType())

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Raw to Bronze

# COMMAND ----------

promises_tbl = data_tables['brz_promises'][0]

promises_df = (zendesker
    .get_promises()
    .drop(columns='external_id'))

promises_spk = spark.createDataFrame(promises_df) # type: ignore

(promises_spk.write.mode('overwrite')
    .format('delta')
    .save(f"{abfss_brz}/promises"))

# COMMAND ----------

# MAGIC %md 
# MAGIC # Bronze to Silver 

# COMMAND ----------

slv_promises_0 = promises_spk

cols_unnest = ['comission', 'interest', 'principal']
for a_col in cols_unnest:
    slv_promises_0 = (slv_promises_0
        .withColumn(a_col, udf_unnest('attribute_compensation', F.lit(a_col))))

slv_promises = (slv_promises_0
    .withColumn('created_at', F.col('created_at').cast(T.TimestampType()))
    .withColumn('updated_at', F.col('updated_at').cast(T.TimestampType()))
    .withColumn('attribute_due_date', udf_date_format('attribute_due_date'))
    .withColumn('attribute_due_date', F.col('attribute_due_date').cast(T.DateType()))
    .drop(F.col('attribute_compensation')))


# COMMAND ----------

# Cambiemos a tablas Δ
(slv_promises.write
    .mode('overwrite')
    .save(f"{abfss_slv}/promises"))
