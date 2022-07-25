# Databricks notebook source
# MAGIC %md 
# MAGIC # Descripción 
# MAGIC 
# MAGIC Hacemos los llamados a Zendesk tipo `cron`, es decir a las horas especificadas se llaman las APIs de Zendesk y se le envían los datos de los filtros de mensajes.  
# MAGIC Para más información, la copia del correo está en `refs/correo-request.md`. 

# COMMAND ----------

# MAGIC %pip install -r ../reqs_dbks.txt

# COMMAND ----------

from pytz import timezone as tz
from datetime import datetime as dt

this_hour = dt.now(tz('America/Mexico_City')).hour

# COMMAND ----------

crm_calls = {
    'Email'    : ('8b8057aa-806c-11ec-bb5a-97eb7d99a45a',  9), 
    'Whatsapp' : ('8ba5ba29-806c-11ec-b252-43df5e7d5d44', 13), 
    'SMS'      : ('8bc3a1f4-806c-11ec-ab3b-410d32afd7c3', 17)
}

hourly_calls = {an_hr: [] for an_hr in range(24)}
for (call_name, (call_id, call_hr)) in crm_calls.items():
    hourly_calls[call_hr].append(call_id)

# COMMAND ----------

from config import ConfigEnviron, ENV, SERVER
from src.platform_resources import AzureResourcer
from src.crm_platform import ZendeskSession

secretter = ConfigEnviron(ENV, SERVER, spark=spark)
az_resourcer = AzureResourcer(secretter)
zendesk = ZendeskSession('sandbox', az_resourcer)

for an_id in hourly_calls[this_hour]: 
    zendesk.post_filter(an_id)
