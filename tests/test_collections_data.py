# Databricks notebook source
# MAGIC %md 
# MAGIC # Pruebas de Consistencias
# MAGIC Este _notebook_ es para establecer pruebas que se ejecuten antes de subir código.  
# MAGIC 
# MAGIC Los preparativos:

# COMMAND ----------

from datetime import datetime as dt, date
import pyspark.pandas as ps
from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test Gold
# MAGIC La tabla final 🥇 es: 
# MAGIC - `loan_contracts`
# MAGIC 
# MAGIC Los tests son: 
# MAGIC 1. Cada préstamo aparece una sola vez. 
# MAGIC 2. Los campos que se utilizan son los que se definieron en Excel. 
# MAGIC 3. No hay datos faltantes en los siguientes campos (por definir) 
# MAGIC 
# MAGIC Iniciamos con una pequeña sección de exploración. 

# COMMAND ----------

gold = ps.read_table("gold.loan_contracts")
display(gold)

# COMMAND ----------

# MAGIC %md 
# MAGIC Hacemos el `count` básico de algunas columnas iniciales. 

# COMMAND ----------

# Contar préstamos
print(f"""
Prestamos en Gold : {gold.shape[0]}
IDs               : {gold['ID'].nunique()}
BankAccountID     : {gold['BankAccountID'].nunique()}
""")

# COMMAND ----------

# MAGIC %md
# MAGIC Y ahora exploramos los duplicados. 

# COMMAND ----------

ids_series = loans.groupby('ID').size()
ids_dups = ids_series[ids_series > 1].index.tolist()
loans_dups = loans[loans['ID'].isin(ids_dups)]
display(loans_dups)

# COMMAND ----------

# MAGIC %md 
# MAGIC **Conclusión**  
# MAGIC Tras examinar los repetidos, encontramos que una causa de que se repitan es cuando tienen varias promesas.  
# MAGIC Entonces ajustamos las promesas para que sólo se tome en cuenta una de ellas.  
# MAGIC 
# MAGIC Entonces nos vamos a la exploración de la fuente en `silver`.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test Silver
# MAGIC Las tablas intermedias 🥈 son: 
# MAGIC - `loan_payment_plans`
# MAGIC - `loan_balances`
# MAGIC - `loan_contracts`
# MAGIC - `loan_open_items`
# MAGIC - `persons_set`
# MAGIC - `zendesk_promises`
# MAGIC 
# MAGIC Hacemos el testing de la tablas de promesas por dos razones.  
# MAGIC Para ajustar la característica que nos menciona el equipo de CX, sobre _processed_ y _accomplished_;  
# MAGIC y una vez implementado ese análisis, evaluamos la opción de quitar repetidos. 

# COMMAND ----------

a_table = ps.read_table('silver.zendesk_promises')
', '.join(a_table.columns.tolist())
# '10000004557-111-MX'
b_table = a_table[a_table['attribute_loan_id'] == '10000004557-111-MX']
display(b_table)

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC ### Zendesk Promises
# MAGIC En el siguiente bloque: 
# MAGIC 1. Se define si es o no activa la promesa. 
# MAGIC 2. Se cuentan las promesas activas por préstamo, y encontramos la más próxima.   
# MAGIC   `due_date` es mínimo. 
# MAGIC 3. Se guardan sólo las promesas activas 

# COMMAND ----------

promises = (ps.read_table('silver.zendesk_promises')
    .assign(is_active = lambda df: ~df['attribute_processed']))

# Se tiene IS_ACTIVE en el índice, y como columna calculada
# Renombramos la columna calculada, después restablecemos el índice y hacemos cálculos. 
min_due_dates = (promises
    .groupby(['attribute_loan_id', 'is_active'])
    .agg({'is_active': 'count', 'attribute_due_date': 'min'})
    .rename(columns={'is_active': 'n_active_promises'})  
    .reset_index()  
    .query('is_active')
    .drop(columns=['is_active']))

by_loans = (promises
    .astype({'attribute_accomplished': int})
    .groupby(['attribute_loan_id'])
    .agg({'attribute_loan_id': 'count', 'attribute_accomplished': 'sum'})
    .rename(columns={'attribute_loan_id': 'n_promises', 'attribute_accomplished': 'n_accomplished'})
    .reset_index()
    .merge(min_due_dates, on='attribute_loan_id', how='left')
    .fillna({'n_active_promises': 0}))

# Contiene la promesa con DUE_DATE minimo, así como los totales calculados. 
promises_loan = (promises
    .merge(by_loans, on=['attribute_loan_id', 'attribute_due_date'], how='left')
    .query('n_promises is not null')
    .drop(columns=['is_active'])
    .groupby('attribute_loan_id')
    .first()
    .reset_index())

display(promises_loan)


# COMMAND ----------

# Contar promesas
print(f"""
Prestamos en Gold : {promises.shape[0]}
IDs               : {promises['attribute_loan_id'].nunique()}
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tests Específicos

# COMMAND ----------

'; '.join(gold.columns)

# COMMAND ----------

brz_persons = ps.read_table('bronze.persons_set')
display(brz_persons.query("AddressPostalCode == '00020'"))

# COMMAND ----------

slv_persons = ps.read_table('silver.persons_set')
display(slv_loans.query("ID == '0001002192'"))

# COMMAND ----------

payment_plans = ps.read_table('bronze.') 
