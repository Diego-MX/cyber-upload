# Databricks notebook source
# MAGIC %md 
# MAGIC # Setup Machine
# MAGIC
# MAGIC Files in Repos now allows us to import files, and run requirements which are setup for both Databricks Jobs and local development. 

# COMMAND ----------

# MAGIC %pip install -r ../reqs_dbks.txt

# COMMAND ----------

from collections import Counter
from datetime import datetime as dt
from itertools import product
import pandas as pd
from time import time

import asyncio
from httpx import Limits, Timeout
from pyspark.sql import functions as F, types as T, Window as W

# COMMAND ----------

from importlib import reload

from src.utilities import tools; reload(tools)
import config; reload(config)
from src import platform_resources; reload(platform_resources)
from src import core_banking; reload(core_banking)

# COMMAND ----------

from src.core_banking import SAPSessionAsync
from src.platform_resources import AzureResourcer
from src.utilities.tools import str_snake_to_camel
from config import (ConfigEnviron, 
    ENV, SERVER, DBKS_TABLES, CORE_ENV)

secretter = ConfigEnviron(ENV, SERVER, spark)
azure_getter = AzureResourcer(secretter)

at_base     = DBKS_TABLES[ENV]['base']
table_items = DBKS_TABLES[ENV]['items'] 

at_storage = azure_getter.get_storage()
azure_getter.set_dbks_permissions(at_storage)

abfss_brz = at_base.format(stage='bronze', storage=at_storage)
abfss_slv = at_base.format(stage='silver', storage=at_storage)

api_types = ['open_items', 'payment_plan', 'balances']
loans_ids = (spark.read.format('delta')
    .load(f"{abfss_brz}/{table_items['brz_loans'][1]}")
    .select('ID').toPandas()['ID'].tolist())

# COMMAND ----------

# MAGIC %md 
# MAGIC # Async Tools
# MAGIC
# MAGIC Following [Real Python's courses](https://realpython.com/python-concurrency/) we setup basic functionality to call the SAP engine with a thread pool executor. 

# COMMAND ----------

# MAGIC %md 
# MAGIC Las APIs de `Balance`, `Open Items` y `Payment Plan`, no están diseñadas para descargar muchos datos, sino por consulta individual.   
# MAGIC Entonces, las llamamaos asíncronamente cuenta por cuenta.  
# MAGIC

# COMMAND ----------

# Balance, OpenItems, PaymentPlan....;  cada ID, LoanIDs.  

async def call_an_api(a_session, in_params): 
    (api_type, loan_id) = in_params
    maybe_df = await a_session.get_by_api(api_type, loan_id)
    return (in_params, maybe_df)

async def call_all_apis(api_calls, ids_lists):
    sap_limits = Limits(max_keepalive_connections=10, max_connections=50)
    sap_timeouts = Timeout(10, pool=50, read=50)
    
    async with SAPSessionAsync(CORE_ENV, 
            azure_getter, limits=sap_limits, timeout=sap_timeouts
            ) as core_client: 
        tasks = []
        for rev_params in product(ids_lists, api_calls): 
            params = rev_params[::-1]
            a_task = asyncio.create_task(call_an_api(core_client, params))
            tasks.append(a_task)
        pre_calls = await asyncio.gather(*tasks, return_exceptions=True)
    return pre_calls

# COMMAND ----------

# MAGIC %md 
# MAGIC # Execution
# MAGIC
# MAGIC Run the async, and print the execution time. 

# COMMAND ----------

from json import dumps
k_only = 20 # len(loans_ids)

tic = time()
pre_results = asyncio.run(
    call_all_apis(api_types, loans_ids[:k_only]))
toc = time() - tic

# COMMAND ----------

okay = sum(1 for a_result in pre_results 
    if isinstance(a_result, tuple) and isinstance(a_result[1], pd.DataFrame))
not_okay_1 = [repr(a_result) for a_result in pre_results 
    if not isinstance(a_result, tuple) or not isinstance(a_result[1], pd.DataFrame)]
not_okay = Counter(not_okay_1)

print(f'''
    Absolute totals : {len(api_types)} × {len(loans_ids)} 
    Test considered : {len(api_types)} × {k_only}
      Correct calls : {okay} out of {k_only*len(api_types)}
         Time spent : {toc:5.4} seconds''')

print(dumps(not_okay))


# COMMAND ----------

calls_dicts = {api: {} for api in api_types}
for call_result in pre_results: 
    if (isinstance(call_result, tuple) 
        and isinstance(call_result[1], pd.DataFrame)): 
        ((api_type, loan_id), result) = call_result
        calls_dicts[api_type][loan_id] = result
        
print(f"""Lengths:
    open_items:    {len(calls_dicts['open_items'])  : 4}, 
    payment_plans: {len(calls_dicts['payment_plan']): 4}, 
    balances:      {len(calls_dicts['balances'])    : 4}""")

# COMMAND ----------

df_cols = {
    'balances'   : ['ID', 'Code','Name','Amount','Currency','BalancesTS'], 
    'open_items' : ['OpenItemTS', 'ContractID', 'OpenItemID', 'Status', 'StatusTxt', 'StatusCategory', 
        'DueDate', 'ReceivableType', 'ReceivableTypeTxt', 'ReceivableDescription', 'Amount', 'Currency'], 
    'payment_plan': ['PaymentPlanTS', 'ContractID', 'ItemID', 'Date', 'Category', 
        'CategoryTxt', 'Amount', 'Currency', 'RemainingDebitAmount'] }

item_keys = {
    'balances'     : 'brz_loan_balances', 
    'open_items'   : 'brz_loan_open_items', 
    'payment_plan' : 'brz_loan_payments' }

spark_tables = {}
for table_type in ['balances', 'open_items', 'payment_plan']: 
    any_dicts = sum(len(a_df) 
            for a_df in calls_dicts[table_type].values())
    if any_dicts: 
        pre_pandas = pd.concat(a_df for a_df in calls_dicts[table_type].values() 
                if len(a_df) > 0)
        pre_spark = spark.createDataFrame(pre_pandas)
    else: 
        the_cols = df_cols[table_type]
        the_schema = T.StructType([T.StructField(a_col, T.StringType(), True) 
                for a_col in the_cols])
        pre_spark = spark.createDataFrame([], schema=the_schema)
    
    ts_name = str_snake_to_camel(table_type, True) + 'TS'
    table_key = table_items[item_keys[table_type]][1]
    table_loc = f"{abfss_brz}/{table_key}_history"
    
    spark_tables[table_type] = pre_spark.withColumnRenamed('ts_call', ts_name)
    
    (pre_spark.write.format('delta')
         .mode('append').option('mergeSchema', True)
         .save(table_loc))


# COMMAND ----------

# ID, CODE
balances_user = (spark_tables['balances']
    .withColumn('maxTS', F.max('balancesTS').over(W.partitionBy(['ID', 'code'])))
    .filter(F.col('BalancesTS') == F.col('maxTS'))
    .withColumn('balancesTS', F.substring('balancesTS', 0, 10))
    .drop('maxTS'))

# ContractID, DueDate, ReceivableType
openitems_user = (spark_tables['open_items']
    .withColumn('maxTS', F.max('OpenItemsTS').over(W.partitionBy(['ContractID', 'DueDate', 'ReceivableType'])))
    .filter(F.col('OpenItemsTS') == F.col('maxTS'))
    .withColumn('OpenItemsTS', F.substring('OpenItemsTS', 0, 10))
    .drop('maxTS'))

# ContractID, Category
pymntplans_user = (spark_tables['payment_plan']
    .withColumn('maxTS', F.max('PaymentPlanTS').over(W.partitionBy(['ContractID', 'Category'])))
    .filter(F.col('PaymentPlanTS') == F.col('maxTS'))
    .withColumn('PaymentPlanTS', F.substring('PaymentPlanTS', 0, 10))
    .drop('maxTS'))

# COMMAND ----------

(balances_user.write.format('delta')
     .mode('overwrite').option('overwriteSchema', True)
     .save(f"{abfss_brz}/{table_items['brz_loan_balances'][1]}"))

(openitems_user.write.format('delta')
     .mode('overwrite').option('overwriteSchema', True)
     .save(f"{abfss_brz}/{table_items['brz_loan_open_items'][1]}"))

(pymntplans_user.write.format('delta')
     .mode('overwrite')
     .save(f"{abfss_brz}/{table_items['brz_loan_payments'][1]}"))

