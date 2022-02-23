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

# MAGIC  %pip install -r ../requirements.txt

# COMMAND ----------

from config import ConfigEnviron
from src.platform_resources import AzureResourcer
from src.crm_platform import ZendeskSession

secretter = ConfigEnviron('dbks')
azurer_getter = AzureResourcer('local', secretter)
zendesk = ZendeskSession('sandbox', azurer_getter)


# COMMAND ----------

promises_df = zendesk.get_promises()

# COMMAND ----------


promises_1 = promises_df.drop(columns = "external_id")
promises_spk = spark.createDataFrame(promises_1)
promises_spk.write.mode("overwrite").saveAsTable("bronze.crm_payment_promises")

