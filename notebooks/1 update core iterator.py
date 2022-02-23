# Databricks notebook source
# MAGIC %md 
# MAGIC ## Description
# MAGIC 
# MAGIC Place Holder

# COMMAND ----------

# MAGIC %md 
# MAGIC #### Libraries and Basic Functions

# COMMAND ----------

# MAGIC %pip install -r ../requirements.txt

# COMMAND ----------

from datetime import datetime as dt
from src.core_banking import SAPSession
from src.platform_resources import AzureResourcer
from config import ConfigEnviron

secretter = ConfigEnviron('dbks')
azure_getter = AzureResourcer('local', secretter)
core_session = SAPSession('qas', azure_getter)

# def flatten(t):
#     return [item for sublist in t for item in sublist]

# COMMAND ----------

loans_ids = spark.table('bronze.loan_contracts').select('ID')
display(loans_ids)

# COMMAND ----------

core_calls = {
    "balances"     : dict(), 
    "open_items"   : dict(), 
    "payment_plan" : dict()}

unserved = []

for ((k, each_id), table_type) in product(enumerate(loans_ids), core_calls.keys()): 
    if (table_type == "balances") and (k in [5, 10, 15, 20, 50, 100, 150, 200, 250, 300, 350, 400, 450, 500, 600, 700]):
        print(f"Running {k} out of {len(loans_ids)}")
    for k in range(3): 
        try:
            call_df = core_session.get_by_api()
            
            get_sap_api(table_type, each_id)
            core_calls[table_type][each_id] = call_df
            break
        except: 
            pass
    else:
        unserved.append((table_type, each_id))
    
print(f"{len(unserved)} calls didn't work.")




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

