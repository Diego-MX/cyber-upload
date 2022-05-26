# Databricks notebook source
# MAGIC %md 
# MAGIC En el Workspace de Jacobo el _notebook_ se llama `tests`. 

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window imprt Window as W
from pyspark.sql.functions import countDistinct, col, dense_rank
from pyspark.sql.window import Window

# COMMAND ----------

loan_contracts = spark.table("silver.loan_contracts")
loan_payment_plans = spark.table("silver.loan_payment_plans")
loan_balances = spark.table("silver.loan_balances")
loan_open_items = spark.table("silver.loan_open_items")
persons_set = spark.table("silver.persons_set")
promises = spark.table("silver.zendesk_promises")

# COMMAND ----------

persons_set = persons_set.drop(*['last_login','phone_number'])
promises = (promises
    .withColumnRenamed('id','promise_id')
    .filter((F.col('attribute_processed') == False) & (F.col('attribute_accomplished') == False)))

# COMMAND ----------

windowSpec = Window.partitionBy("attribute_loan_id").orderBy("attribute_due_date")
promises = (promises
    .withColumn('rank', F.dense_rank().over(windowSpec))
    .filter(F.col('rank') == 1).drop(F.col('rank')))

# COMMAND ----------

base_df = (loan_contracts
    .join(loan_balances, loan_contracts.ID == loan_balances.ID, 'inner')
    .drop(loan_balances.ID)
    .join(persons_set,loan_contracts.BorrowerID == persons_set.ID,'inner')
    .drop(persons_set.ID))

# COMMAND ----------

base_open = (base_df
    .join(loan_open_items,base_df.ID == loan_open_items.ContractID,'left')
    .drop(loan_open_items.ContractID)
    .fillna(value=0))

# COMMAND ----------

full_fields = (base_open
    .join(loan_payment_plans,base_open.ID == loan_payment_plans.ContractID,'left')
    .drop(loan_payment_plans.ContractID)
    .fillna(value=0))

# COMMAND ----------

full_fields = (full_fields
    .join(promises, full_fields.ID == promises.attribute_loan_id, 'left')
    .drop(promises.attribute_loan_id))

# COMMAND ----------

(full_fields.write.mode("overwrite").format("delta")
     .option("overwriteSchema", True).saveAsTable("gold.loan_contracts"))

# COMMAND ----------

motor = spark.table("gold.loan_contracts")

# COMMAND ----------

display(motor.groupBy('ID').count().filter(col('count') > 1))

# COMMAND ----------

display(motor.filter(col('ID') == '10000004557-111-MX').dropDuplicates())
