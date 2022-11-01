# Databricks notebook source
# MAGIC %md 
# MAGIC # Validación de Préstamos
# MAGIC 
# MAGIC Revisar los préstamos faltantes. 

# COMMAND ----------

import os; os.environ['ENV'] = 'dbks'

from datetime import datetime as dt, date
import pandas as pd
import pyspark.pandas as ps
from pyspark.sql import functions as F

from config import ConfigEnviron
from src.platform_resources import AzureResourcer
from src.core_banking import SAPSession
   
secretter = ConfigEnviron('dbks', spark=spark)
azure_getter = AzureResourcer('local', secretter)

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Filas en tablas Bronze, Silver, Gold
# MAGIC Simplemente calcular el número de filas y comparar. 

# COMMAND ----------

contracts_brz  = ps.read_table("bronze.loan_contracts")
qan_brz        = ps.read_table("bronze.loan_qan_contracts")
balances_brz   = ps.read_table("bronze.loan_balances")
open_items_brz = ps.read_table("bronze.loan_open_items")
payments_brz   = ps.read_table("bronze.loan_payment_plans")

# COMMAND ----------

balances.select[]

# COMMAND ----------

# MAGIC %md
# MAGIC Filas en las tablas principales de bronce 🥉

# COMMAND ----------

print(f"""Filas: 
    Contracts  : {contracts_brz.shape[0]}
    QAN        : {qan_brz.shape[0]}
    Balances   : {balances_brz.shape[0]}
    Open Items : {open_items_brz.shape[0]}
    Pymt Plans : {payments_brz.shape[0]}
    """)

# COMMAND ----------

# MAGIC %md 
# MAGIC Ahora en Silver 🥈.   
# MAGIC Revisamos la fecha de edición de las tablas y vemos que  
# MAGIC `loan_contract_{balances, open_items, payment_plan}` tienen la última en diciembre,  
# MAGIC al igual que `loan_contract` (≠ `loan_contracts`) y `...smart_analyzer`.  
# MAGIC Es decir, que estas ya no se están usando.  Listos para desecharlas. 

# COMMAND ----------

contracts_slv  = ps.read_table("silver.loan_contracts")
balances_slv   = ps.read_table("silver.loan_balances")
payments_slv   = ps.read_table("silver.loan_contract_payment_plan")
txns_slv       = ps.read_table("silver.loan_contract_transactions")

smarts       = ps.read_table("silver.loan_contract_smart_analyzer")
open_items   = ps.read_table("silver.loan_contract_open_items")
balances2    = ps.read_table("silver.loan_contract_balances")
open_items2  = ps.read_table("silver.loan_open_items")
payments2    = ps.read_table("silver.loan_payment_plans")

print(f"""Filas: 
    Contracts   : {contracts.shape[0]}
    Balances    : {balances.shape[0]}
    Open Items  : {open_items.shape[0]}
    Pymt Plans  : {payments.shape[0]}
    Transactions: {txns.shape[0]}
    --- ya no sirven ---
    Smart Anlzr : {smarts.shape[0]}
    Balances 2  : {balances2.shape[0]}
    Open Items 2: {open_items2.shape[0]}
    Pymt Plans 2: {payments2.shape[0]}
    
    """)

# COMMAND ----------

# MAGIC %md
# MAGIC Y ahora en Gold 🥇.  
# MAGIC Igualmente `motor_cobranza` ya no se usa desde diciembre 2021. 

# COMMAND ----------

contracts_gld = ps.read_table("gold.loan_contracts")
cobranza_gld  = ps.read_table("gold.motor_cobranza")

print(f"""Filas: 
    Contracts : {contracts_gld.shape[0]}
    Cobranza  : {cobranza_gld.shape[0]}
    """)

# COMMAND ----------

# MAGIC %md 
# MAGIC ## ¿Por qué en Gold 🥇 son menos?  
# MAGIC Inicialmente la tabla de `gold.loan_contracts` tenía menos entradas que las correspondientes de `silver` y `bronze`  
# MAGIC debido a que se hacía un _inner join_ con la tabla de `silver.loan_balances`, y que como se ve tiene muchos menos (299 en mayo) préstamos.   
# MAGIC   
# MAGIC Esto se arregló haciendo un _full join_, pero deja abierta la cuestión de por qué hay tantos faltantes en `balance`.  
# MAGIC Hay dos hipótesis:
# MAGIC     - El servicio no responde bien, de modo que tiene muchas fallas: error técnico de Datos. 
# MAGIC     - Hay préstamos activos y que, en efecto, no cuentan con un balance: clarificación con SAP. 
# MAGIC 
# MAGIC Exploramos las dos causas. 

# COMMAND ----------

ids_prestamos = contracts_brz.shape[0]
ids_balances = balances_brz['ID'].unique().to_numpy()
print(f"""
    Hay {ids_prestamos} en Prestamos.
    Hay {len(ids_balances)} diferentes en Balances.""")

# COMMAND ----------

wout_balance = contracts_brz[~contracts_brz['ID'].isin(ids_balances)]
display(wout_balance)

# COMMAND ----------



