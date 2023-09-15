# Databricks notebook source
# MAGIC %pip install -q -r ../reqs_dbks.txt

# COMMAND ----------

epicpy_tag = 'v1.1.19'      # dev-diego

# COMMAND ----------

# pylint: disable=wrong-import-position,wrong-import-order
# pylint: disable=ungrouped-imports
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
    'branch': epicpy_tag, 
    'token' : dbutils.secrets.get(u_dbks['dbks_scope'], u_dbks['dbks_token']) }  

url_call = "git+https://{token}@{url}@{branch}".format(**epicpy_load)
check_call(['pip', 'install', url_call])
