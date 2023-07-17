import numpy as np
from pyspark.sql import SparkSession
from typing import Union
   
from epic_py.tools import (MatchCase, read_excel_table, str_plus, dict_plus)

encode64 = lambda a_str: str_plus(a_str).encode64()

dict_minus = lambda a_dict, keys: dict_plus(a_dict).difference(keys)

dict_get2 = lambda a_dict, *keys: dict_plus(a_dict).get_plus(*keys)

str_snake_to_camel = lambda a_str: str_plus(a_str).to_camel()


def get_dbutils(spark:SparkSession=None):
    try: 
        from pyspark.dbutils import DBUtils
        dbutils = DBUtils(spark)
    except ModuleNotFoundError:
        dbutils = None
    return dbutils 


def is_na(val: Union[str, float]): 
    if isinstance(val, str): 
        is_it = (val is None)
    elif isinstance(val, float): 
        is_it = np.isnan(val)
    return is_it
