import json 
from pyspark.sql import SparkSession
  
   
from epic_py.tools import (MatchCase, read_excel_table, str_camel_2_snake, str_plus, dict_plus)

encode64 = lambda a_str : str_plus(a_str).encode64()


def get_dbutils(spark:SparkSession=None):
    try: 
        from pyspark.dbutils import DBUtils
        dbutils = DBUtils(spark)
    except ModuleNotFoundError:
        dbutils = None
    return dbutils 
