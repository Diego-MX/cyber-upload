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
# MAGIC #### Libraries and Basic Functions

# COMMAND ----------

# MAGIC %pip install azure-identity azure-keyvault-secrets python-dotenv

# COMMAND ----------

import os, sys
from requests import get as rq_get, auth

import pandas as pd
import pyspark.sql.types as T

from azure.identity import ClientSecretCredential
from azure.keyvault.secrets import SecretClient

from config import SITE, ENV_KEYS, URLS

def secret_by_key(a_key, scope="kv-resource-access-dbks"):
    the_secret = dbutils.secrets.get(scope=scope, key=a_key)
    return the_secret


# COMMAND ----------

# MAGIC %md
# MAGIC #### Key Vaults and Env Variables

# COMMAND ----------

urls = URLS["sap-calls"]["qa"]
AUTH_URL = urls["auth"]
APIS_URL = urls["apis"]

# Access to Auth. 
azure_keys  = ENV_KEYS["platform"]

az_creds = ClientSecretCredential(**{k: secret_by_key(v) 
        for (k, v) in azure_keys["databricks"].items() })

vault_client = SecretClient(vault_url=azure_keys["key-vault"]["url"], credential=az_creds)
get_secret = lambda name: vault_client.get_secret(name).value


# COMMAND ----------

crm_env = "sandbox"

CRM_URL   = get_secret("crm-api-url") # 'https://bineo1633010523.zendesk.com/api' # URLS["crm-call"][crm_env]
CRM_USER  = get_secret("crm-api-user")
CRM_TOKEN = get_secret("crm-api-token")


promises = rq_get(f"{CRM_URL}/sunshine/objects/records", 
    auth=auth.HTTPBasicAuth(f"{CRM_USER}/token", CRM_TOKEN),
    params={"type": "payment_promise"})


# COMMAND ----------


promises_ls = promises.json()["data"]

for prms_dict in promises_ls: 
    attributes = prms_dict.pop("attributes")
    prms_dict.update(attributes)

promises_df = pd.DataFrame(promises_ls).drop(columns = "external_id")
promises_spk = spark.createDataFrame(promises_df)
promises_spk.write.mode("overwrite").saveAsTable("bronze.crm_payment_promises")

