# Databricks notebook source
# MAGIC %md 
# MAGIC # Setup Machine
# MAGIC 
# MAGIC Files in Repos now allows us to import files, and run requirements which are setup for both Databricks Jobs and local development. 

# COMMAND ----------

# MAGIC %pip install -r ../reqs_dbks.txt

# COMMAND ----------

from datetime import datetime as dt
from time import time
import pandas as pd
from itertools import product
from collections import Counter
from pyspark.sql import functions as F, types as T, Window as W

import asyncio
from httpx import Limits, Timeout
from config import ConfigEnviron, ENV, SERVER, DBKS_TABLES, CORE_ENV
from src.core_banking import SAPSessionAsync
from src.platform_resources import AzureResourcer


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

async def call_an_api(a_session, in_params): 
    (api_type, type_id) = in_params
    maybe_df = await a_session.get_by_api(api_type, type_id, tries=10)
    return (in_params, maybe_df)

async def call_all_apis(api_calls, ids_lists):
    sap_limits = Limits(max_keepalive_connections=10, max_connections=50)
    sap_timeouts = Timeout(10, pool=50, read=50)
    
    async with SAPSessionAsync(CORE_ENV, azure_getter, limits=sap_limits, timeout=sap_timeouts) as core_client: 
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
k_only = len(loans_ids)

tic = time()
pre_results = asyncio.run(call_all_apis(api_types, loans_ids[:k_only]))
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


balances_df   = (pd.concat(calls_dicts["balances"].values())
    .rename(columns={"ts_call": "BalancesTS"}))
openitems_df  = (pd.concat(calls_dicts["open_items"].values())
    .rename(columns={"ts_call": "OpenItemTS"}))
pymntplans_df = (pd.concat(calls_dicts["payment_plan"].values())
    .rename(columns={"ts_call": "PaymentPlanTS"}))

balances_spk   = spark.createDataFrame(balances_df  )
openitems_spk  = spark.createDataFrame(openitems_df )
pymntplans_spk = spark.createDataFrame(pymntplans_df)

(balances_spk.write.format('delta').mode('append')
     .save(f"{abfss_brz}/{table_items['brz_loan_balances'  ][1]}_history"))
(openitems_spk.write.format('delta').mode('append')
     .save(f"{abfss_brz}/{table_items['brz_loan_open_items'][1]}_history"))
(pymntplans_spk.write.format('delta').mode('append')
     .save(f"{abfss_brz}/{table_items['brz_loan_payments'  ][1]}_history") )


# COMMAND ----------

# ID, CODE
balances_user = (balances_spk
    .withColumn('maxTS', F.max('balancesTS').over(W.partitionBy(['ID', 'code'])))
    .filter(F.col('BalancesTS') == F.col('maxTS'))
    .withColumn('balancesTS', F.substring('balancesTS', 0, 10))
    .drop('maxTS'))

# ContractID, DueDate, ReceivableType
openitems_user = (openitems_spk
    .withColumn('maxTS', F.max('OpenItemTS').over(W.partitionBy(['ContractID', 'DueDate', 'ReceivableType'])))
    .filter(F.col('OpenItemTS') == F.col('maxTS'))
    .withColumn('OpenItemTS', F.substring('OpenItemTS', 0, 10))
    .drop('maxTS'))

# ContractID, Category
pymntplans_user = (pymntplans_spk
    .withColumn('maxTS', F.max('PaymentPlanTS').over(W.partitionBy(['ContractID', 'Category'])))
    .filter(F.col('PaymentPlanTS') == F.col('maxTS'))
    .withColumn('PaymentPlanTS', F.substring('PaymentPlanTS', 0, 10))
    .drop('maxTS'))

# COMMAND ----------

(balances_user.write.format('delta').mode('overwrite')
     .save(f"{abfss_brz}/{table_items['brz_loan_balances'][1]}"))

(openitems_user.write.format('delta').mode('overwrite')
     .save(f"{abfss_brz}/{table_items['brz_loan_open_items'][1]}"))

(pymntplans_user.write.format('delta').mode('overwrite')
     .save(f"{abfss_brz}/{table_items['brz_loan_payments'][1]}"))
