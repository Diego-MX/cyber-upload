# Databricks notebook source
# MAGIC %md 
# MAGIC # Requirements and Main Packages
# MAGIC 
# MAGIC Files in Repos now allows us to import files, and run requirements which are setup for both Databricks Jobs and local development. 

# COMMAND ----------

# MAGIC %pip install -r ../requirements.txt

# COMMAND ----------

from time import time
from itertools import product
from src.core_banking import SAPSession
from src.platform_resources import AzureResourcer
from config import ConfigEnviron

secretter = ConfigEnviron('dbks')
azure_getter = AzureResourcer('local', secretter)


# COMMAND ----------

# MAGIC %md 
# MAGIC # Threading Tools
# MAGIC 
# MAGIC Following [Real Python's courses](https://realpython.com/python-concurrency/) we setup basic functionality to call the SAP engine with a thread pool executor. 

# COMMAND ----------

from concurrent.futures import ThreadPoolExecutor
import threading

calls_dicts = {}
failed_calls = []

thread_local = threading.local()

def get_session():
    if not hasattr(thread_local, 'session'): 
        thread_local.session = SAPSession('qas', azure_getter)
    return thread_local.session


def call_an_api(in_params): 
    global calls_dicts, failed_calls
    api_type, type_id = in_params
    a_session = get_session()
    
    api_df = a_session.get_by_api(api_type, type_id)
    if api_df is not None: 
        calls_dicts[api_type][type_id] = api_df
    else: 
        failed_calls.append((api_type, type_id))

        
def call_all_apis(api_calls, ids_lists, k_workers=20):
    global calls_dicts, failed_calls
    calls_dicts = {a_call: {} for a_call in api_types}
    failed_calls = []
    
    with ThreadPoolExecutor(max_workers=k_workers) as executor: 
        executor.map(call_an_api, product(api_types, ids_lists))
    
    return (calls_dicts, failed_calls)


api_types = ['open_items', 'payment_plan', 'balances']
loans_ids = spark.table('bronze.loan_contracts').select('ID').toPandas()['ID'].tolist()

# COMMAND ----------

# MAGIC %md 
# MAGIC # Execution
# MAGIC 
# MAGIC Run the multithreading, and print the execution time. 

# COMMAND ----------

tic = time()
(core_calls, unserved) = call_all_apis(api_types, loans_ids, 20)
toc = time() - tic

print(f'{len(loans_ids)*len(api_types)}, {len(unserved)} in {toc:5.2} seconds')
print(f"Lengths, open_items: {len(api_dicts['open_items'])}, payment_plans: {len(api_dicts['payment_plan'])}, balances: {len(api_dicts['balances'])}")

# COMMAND ----------

balances_df = pd.concat(core_calls["balances"].values()).rename(columns={"ts_call": "BalancesTS"})
openitems_df = pd.concat(core_calls["open_items"].values()).rename(columns={"ts_call": "OpenItemTS"})
pymntplans_df = pd.concat(core_calls["payment_plan"].values()).rename(columns={"ts_call": "PaymentPlanTS"})

balances_spk = spark.createDataFrame(balances_df)
openitems_spk = spark.createDataFrame(openitems_df)
pymntplans_spk = spark.createDataFrame(pymntplans_df)

balances_spk.write.mode("overwrite").saveAsTable("bronze.loan_balances")
openitems_spk.write.mode("overwrite").saveAsTable("bronze.loan_open_items")
pymntplans_spk.write.mode("overwrite").saveAsTable("bronze.loan_payment_plans")

