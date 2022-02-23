# Databricks notebook source
# MAGIC %md 
# MAGIC # Descripción 
# MAGIC 
# MAGIC Hacemos los llamados a Zendesk tipo `cron`, es decir a las horas especificadas se llaman las APIs de Zendesk y se le envían los datos de los filtros de mensajes.  
# MAGIC Para más información, la copia del correo está en `refs/correo-request.md`. 

# COMMAND ----------

# MAGIC %pip install -r ../requirements.txt

# COMMAND ----------

crm_calls = {
    'Email' : ('8b8057aa-806c-11ec-bb5a-97eb7d99a45a', 9), 
    'Whatsapp' : ('8ba5ba29-806c-11ec-b252-43df5e7d5d44', 13), 
    'SMS'   : ('8bc3a1f4-806c-11ec-ab3b-410d32afd7c3', 17)
}
an_id = crm_calls['SMS'][0]

# COMMAND ----------

from config import ConfigEnviron
from src.platform_resources import AzureResourcer
from src.crm_platform import ZendeskSession

secretter = ConfigEnviron('dbks')
az_resourcer = AzureResourcer('local', secretter)
zendesk = ZendeskSession('sandbox', az_resourcer)

zendesk.post_filter(an_id)
