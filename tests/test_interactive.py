# Databricks notebook source
# MAGIC %md
# MAGIC # Descripción
# MAGIC 
# MAGIC Este _notebook_ tiene algunas pruebas 'unitarias' con base en los _bugs_ de Jira.  
# MAGIC Tiene las siguientes secciones:  
# MAGIC * Preparación
# MAGIC * Prueba de un préstamo específico actualizado
# MAGIC * Prueba de tabla `gold.loan_contracts` con suficientes renglones

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Preparación

# COMMAND ----------

import pandas as pd

from json import loads, dumps
from pyspark.sql import functions as F, types as T, Window as W
from datetime import datetime as dt, timedelta as delta, date
from delta.tables import DeltaTable

from src.core_banking import SAPSession, update_dataframe, str_error
from src.platform_resources import AzureResourcer
from config import ConfigEnviron, ENV, SERVER

app_environ = ConfigEnviron(ENV, SERVER, spark)
az_manager  = AzureResourcer(app_environ)
core_session = SAPSession('qas-sap', az_manager)

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Prueba gold.loan_contracts

# COMMAND ----------

gold_loans = spark.read.table('gold.loan_contracts')
display(gold_loans)


# COMMAND ----------

# MAGIC %md 
# MAGIC ## Prueba de préstamo específico

# COMMAND ----------

one_account = gold_loans.filter(F.col('BankAccountID')=='03017012378')
one_json = loads(one_account.toJSON().collect()[0])
print(dumps(one_json, indent=2))


# COMMAND ----------

brz_loans = spark.read.table('nayru_accounts.brz_ops_loan_contracts')
one_bronze = brz_loans.filter(F.col('BankAccountID')=='03017012378')
brz_json = loads(one_bronze.toJSON().collect()[0])
print(dumps(brz_json, indent=2))

