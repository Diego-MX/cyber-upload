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

from pyspark.sql import SparkSession
from pyspark.dbutils import DBUtils
import subprocess
import yaml

spark = SparkSession.builder.getOrCreate()
dbutils = DBUtils(spark)



with open("../user_databricks.yml", 'r') as _f: 
    u_dbks = yaml.safe_load(_f)

epicpy_load = {
    'url'   : 'github.com/Bineo2/data-python-tools.git', 
    'branch': 'dev-diego', 
    'token' :  dbutils.secrets.get(u_dbks['dbks_scope'], u_dbks['dbks_token'])}

url_call = "git+https://{token}@{url}@{branch}".format(**epicpy_load)
subprocess.check_call(['pip', 'install', url_call])

# COMMAND ----------

from datetime import datetime as dt
import json
from pyspark.sql import functions as F, types as T
import re


# COMMAND ----------

from importlib import(reload)
import epic_py; reload(epic_py)
import src; reload(src)
import config; reload(config)

from src.platform_resources import AzureResourcer
from src.crm_platform import ZendeskSession
from config import (ConfigEnviron, 
    ENV, SERVER, CRM_ENV, DBKS_TABLES, 
    app_agent, app_resources)

a_storage = app_resources['storage']
stg_permissions = app_agent.prep_dbks_permissions(a_storage, 'gen2')
app_resources.set_dbks_permissions(stg_permissions)

secretter = ConfigEnviron(ENV, SERVER, spark)
azure_getter = AzureResourcer(secretter)

at_storage = azure_getter.get_storage()
azure_getter.set_dbks_permissions(at_storage)

zendesker = ZendeskSession(CRM_ENV, azure_getter)
abfss_brz = DBKS_TABLES[ENV]['promises'].format(stage='bronze', storage=at_storage) # type: ignore
abfss_slv = DBKS_TABLES[ENV]['promises'].format(stage='silver', storage=at_storage) # type: ignore

tbl_items = DBKS_TABLES[ENV]['items'] # type: ignore


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

promises_meta = tbl_items['brz_promises']
promises_tbl = (promises_meta[2] if len(promises_meta) > 2 
        else promises_meta[0])

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
