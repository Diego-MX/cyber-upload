# Databricks notebook source
# MAGIC %md 
# MAGIC Instalando requerimientos de librerías para los _notebooks_. 

# COMMAND ----------

# MAGIC %pip install -q -r ../reqs_dbks.txt

# COMMAND ----------

epicpy_ref = 'gh-1.8' # 'v1.1.19'      # dev-diego
# pylint: disable=wrong-import-position,wrong-import-order
# pylint: disable=ungrouped-imports

# COMMAND ----------

from subprocess import check_call
from pyspark.sql import SparkSession
from pyspark.dbutils import DBUtils     # pylint: disable=import-error,no-name-in-module
import yaml

spark = SparkSession.builder.getOrCreate()
dbutils = DBUtils(spark)

with open("../user_databricks.yml", 'r') as _f:     # pylint: disable=unspecified-encoding
    u_dbks = yaml.safe_load(_f)

epicpy_load = {
    'url'   : 'github.com/Bineo2/data-python-tools.git', 
    'ref'   : epicpy_ref, 
    'token' : dbutils.secrets.get(u_dbks['dbks_scope'], u_dbks['dbks_token']) }  

url_call = "git+https://{token}@{url}@{ref}".format(**epicpy_load)
check_call(['pip', 'install', url_call])

# COMMAND ----------

import epic_py
print(f"""
Epic Ref → {epicpy_ref}
Epic Ver → {epic_py.__version__}"""[1:])
