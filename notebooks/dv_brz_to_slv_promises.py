# Databricks notebook source
# MAGIC %md
# MAGIC # Descripción
# MAGIC Este _notebook_ es una copia del de Jacobo en su _workspace_.  
# MAGIC Lo copié el 6 de abril de 2022, para revisión de flujos con CX.  
# MAGIC También hice algunos cambios de formato.  

# COMMAND ----------

from pyspark.sql.functions import F
from pyspark.sql.types import T
from datetime import datetime
import json
import re

# COMMAND ----------

def unnest(c, s):
    if c == '':
        return None
    else:
        c = json.loads(c)
        return c[s]

udf_unnest = udf(unnest, IntegerType())

def date_format(c):
    pattern1 = re.compile("^[0-9]+/[0-9]+/[0-9]+$")
    pattern2 = re.compile("^[0-9]+-[0-9]+-[0-9]+T[0-9]+:[0-9]+:[0-9]+.[0-9]+Z$")
    pattern3 = re.compile("^[0-9]+-[0-9]+-[0-9]+T[0-9]+:[0-9]+:[0-9]+Z$")
    pattern4 = re.compile("^[0-9]+-[0-9]+-[0-9]+T[0-9]+:[0-9]+:[0-9]+.[0-9]+z$")
    if pattern1.match(c):
        return datetime.strptime(c, "%d/%m/%Y").strftime("%Y-%m-%d")
    elif pattern2.match(c):
        return datetime.strptime(c, "%Y-%m-%dT%H:%M:%S.%fZ").strftime("%Y-%m-%d")
    elif pattern3.match(c):
        return datetime.strptime(c, "%Y-%m-%dT%H:%M:%SZ").strftime("%Y-%m-%d")
    elif pattern4.match(c):
        return datetime.strptime(c, "%Y-%m-%dT%H:%M:%S.%fz").strftime("%Y-%m-%d")
    else:
        return None

udf_date_format = udf(date_format, StringType())

# COMMAND ----------

brz_promises = spark.sql("""
    select *
    from bronze.crm_payment_promises
    """)

# COMMAND ----------

column_unnest = ['comission', 'interest', 'principal']
for a_col in column_unnest:
    brz_promises = brz_promises.withColumn(a_col, udf_unnest('attribute_compensation', F.lit(a_col)))

# COMMAND ----------

brz_promises = (brz_promises
    .withColumn('created_at', F.col('created_at').cast(TimestampType()))
    .withColumn('updated_at', F.col('updated_at').cast(TimestampType()))
    .withColumn('attribute_due_date', udf_date_format('attribute_due_date'))
    .withColumn('attribute_due_date', F.col('attribute_due_date').cast(T.DateType()))
    .drop(F.col('attribute_compensation')))

# COMMAND ----------

brz_promises.write.mode("overwrite").format("delta").saveAsTable("silver.zendesk_promises")
