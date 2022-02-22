# Databricks notebook source
# MAGIC %md 
# MAGIC # Descripción 
# MAGIC 
# MAGIC Hacemos los llamados a Zendesk tipo `cron`, es decir a las horas especificadas se llaman las APIs de Zendesk y se le envían los datos de los filtros de mensajes.  
# MAGIC Para más información, la copia del correo está en `refs/correo-request.md`. 

# COMMAND ----------

# MAGIC %pip install python-dotenv azure-identity azure-keyvault-secrets

# COMMAND ----------

crm_calls = {
    'Email' : ('8b8057aa-806c-11ec-bb5a-97eb7d99a45a', 9), 
    'Whatsapp' : ('8ba5ba29-806c-11ec-b252-43df5e7d5d44', 13), 
    'SMS' : ('8bc3a1f4-806c-11ec-ab3b-410d32afd7c3', 17)
}
the_call = 'Email'

# COMMAND ----------

from json import dumps
from requests import get as rq_get, post
from requests.auth import HTTPBasicAuth

from azure.identity import ClientSecretCredential 
from azure.keyvault.secrets import SecretClient
from config import ENV_KEYS


def secret_by_key(a_key, scope='kv-resource-access-dbks'):
    the_secret = dbutils.secrets.get(scope=scope, key=a_key)
    return the_secret

azure_keys = ENV_KEYS['platform']
az_creds = ClientSecretCredential(**{k: secret_by_key(v) 
        for (k, v) in azure_keys['databricks'].items() })

vault_client = SecretClient(vault_url=azure_keys['key-vault']['url'], credential=az_creds)
get_secret = lambda name: vault_client.get_secret(name).value

CRM_URL   = get_secret('crm-api-url') # 'https://bineo1633010523.zendesk.com/api' # URLS["crm-call"][crm_env]
CRM_USER  = get_secret('crm-api-user')
CRM_TOKEN = get_secret('crm-api-token')

ZIS_ID   = get_secret('crm-zis-id')
ZIS_USER = get_secret('crm-zis-user')
ZIS_PASS = get_secret('crm-zis-pass')


# COMMAND ----------

an_id = crm_calls[the_call][0]

filtros = rq_get(f'{CRM_URL}/sunshine/objects/records/{an_id}', 
        auth=HTTPBasicAuth(f"{CRM_USER}/token", CRM_TOKEN))

the_data = filtros.json()['data']


# COMMAND ----------

post_params = {
    'url'  : f'{CRM_URL}/services/zis/inbound_webhooks/generic/ingest/{ZIS_ID}', 
    'auth' : HTTPBasicAuth(ZIS_USER, ZIS_PASS), 
    'data' : dumps({'data': [the_data]})
}

communications = post(**post_params)
