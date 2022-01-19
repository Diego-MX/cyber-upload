# Databricks notebook source
# MAGIC %md ##Se cargan librerías 

# COMMAND ----------

import requests
import json
from requests.auth import HTTPDigestAuth
import pandas as pd
from pandas.io.json import json_normalize

spark.sql("set spart.databricks.delta.preview.enabled=true")
spark.sql("set spart.databricks.delta.retentionDutationCheck.preview.enabled=false")

# COMMAND ----------

# MAGIC %md ##Definición de funciones

# COMMAND ----------

def removeEmptyElements(d):
    """recursively remove empty lists, empty dicts, or None elements from a dictionary"""

    def empty(x):
        return x is None or x == {} or x == []

    if not isinstance(d, (dict, list)):
        return d
    elif isinstance(d, list):
        return [v for v in (removeEmptyElements(v) for v in d) if not empty(v)]
    else:
        return {k: v for k, v in ((k, removeEmptyElements(v)) for k, v in d.items()) if not empty(v)}

# COMMAND ----------

def saveRequestDeltaTable(nameTable, pathData):    # Se recibe como parámetros: el nombre que se quieres para la tabla delta, la posición en el JSON en donde se encuentran los datos
  normalized = pd.json_normalize(pathData)
  spark.sql("DROP TABLE IF EXISTS " + nameTable)
  spark_df = spark.createDataFrame(normalized)
  spark_df.toPandas().info()
  spark_df.write.format("delta").mode("overwrite").saveAsTable(nameTable)

# COMMAND ----------

# MAGIC %md ##Obtención del token

# COMMAND ----------

url = "https://latp-apim.prod.apimanagement.us20.hana.ondemand.com/oauth2/token"

payload='grant_type=password&username=<user>&password=<password>'   # Modificar user y password
headers = {
  'Authorization': 'Basic d05XR053QW1UUVA1enRHS3h3eHh4N2R6cEdtWkFkNTk6TkZvcHlRYUxwM2tjRGRhdw==',
  'Content-Type': 'application/x-www-form-urlencoded'
}

responseToken = requests.request("POST", url, headers=headers, data=payload)
json_data = json.loads(responseToken.text)
access_token = json_data.get('access_token')

print(responseToken.text)

# COMMAND ----------

# MAGIC %md ##Loan Contract

# COMMAND ----------

url = "https://sbx-latp-apim.prod.apimanagement.us20.hana.ondemand.com/s4b/v1/lacovr/ContractSet('<NumeroContrato>')?$format=json"   # Agregar número de contrato

headers = {
  'Authorization': 'Bearer '+ access_token
}

#session = requests.Session()
#session.headers.update({'Authorization': 'Bearer {access_token}'})

responseLoanContract = requests.get(url, headers = headers)

print(json.dumps(responseLoanContract.json(), indent=3))

# COMMAND ----------

jsonLoanContract = responseLoanContract.json()
dataLoanContract = jsonLoanContract['d']

# COMMAND ----------

saveRequestDeltaTable("<NombreTabla>", dataLoanContract)   # Agregar nombre tabla

# COMMAND ----------

# MAGIC %sql SELECT * FROM    /* Agregar nombre de la tabla

# COMMAND ----------

# MAGIC %sql DESCRIBE DETAIL  /* Agregar nombre de la tabla

# COMMAND ----------

# MAGIC %md ## Loan Contract Payment Plan

# COMMAND ----------

url = "https://sbx-latp-apim.prod.apimanagement.us20.hana.ondemand.com/s4b/v1/lacovr/ContractSet('<NumeroContrato>')/PaymentPlan?$format=json"   # Agregar número de contrato

headers = {
  'Accept-Encoding': 'gzip, deflate',
  'Authorization': 'Bearer '+ access_token,
  'Cookie': 'SAP_SESSIONID_L4B_900=AZiOof0O6-Kpw72hc3YWF1fVcY3xcxHrvtRCAQqeAAQ%3d; sap-usercontext=sap-client=900'
}

responseLoanContractPaymentPlan = requests.get(url, headers=headers)

print(json.dumps(responseLoanContractPaymentPlan.json(), indent=3))


# COMMAND ----------

jsonLoanContractPaymentPlan = responseLoanContractPaymentPlan.json()
dataLoanContractPaymentPlan = jsonLoanContractPaymentPlan['d']['results']

# COMMAND ----------

saveRequestDeltaTable("<NombreTabla>", dataLoanContractPaymentPlan)   # Nombre de la tabla

# COMMAND ----------

# MAGIC %sql SELECT * FROM    /* Nombre tabla

# COMMAND ----------

# MAGIC %md ##Loan Contract Balances

# COMMAND ----------

url = "https://sbx-latp-apim.prod.apimanagement.us20.hana.ondemand.com/s4b/v1/lacovr/ContractSet('<NumeroContrato>')/Balances?$format=json"   # Número de contrato

headers = {
  'Accept-Encoding': 'gzip, deflate',
  'Authorization': 'Bearer '+ access_token
}

responseLoanContractBalances = requests.get(url, headers=headers)

print(json.dumps(responseLoanContractBalances.json(), indent=3))

# COMMAND ----------

jsonLoanContractBalances = responseLoanContractBalances.json()
dataLoanContractBalances = jsonLoanContractBalances['d']['results']

# COMMAND ----------

saveRequestDeltaTable("<NombreTabla>", dataLoanContractBalances)  # Nombre tabla

# COMMAND ----------

# MAGIC %sql SELECT * FROM    /* Nombre tabla

# COMMAND ----------

# MAGIC %md ##Loan Contract Open Items

# COMMAND ----------

url = "https://sbx-latp-apim.prod.apimanagement.us20.hana.ondemand.com/s4b/v1/lacovr/ContractSet('<NumeroContrato>')/OpenItems?$format=json"   # Número contrato

headers = {
  'Accept-Encoding': 'gzip, deflate',
  'Authorization': 'Bearer '+ access_token
}

responseLoanContractOpenItems = requests.get(url, headers=headers)

print(json.dumps(responseLoanContractOpenItems.json(), indent=3))

# COMMAND ----------

jsonLoanContractOpenItems = responseLoanContractOpenItems.json()
dataLoanContractOpenItems = jsonLoanContractOpenItems['d']['results']

# COMMAND ----------

saveRequestDeltaTable("<NombreTabla>", dataLoanContractOpenItems)   # Nombre tabla

# COMMAND ----------

# MAGIC %sql SELECT * FROM    /* Nombre tabla

# COMMAND ----------

# MAGIC %md ##Customer Person Set

# COMMAND ----------

url = "https://sbx-latp-apim.prod.apimanagement.us20.hana.ondemand.com/s4b/v15/bp/PersonSet('<IdentificadorCliente>')?$format=json"    # Identificador cliente

headers = {
  'Accept-Encoding': 'gzip, deflate',
  'Authorization': 'Bearer '+ access_token
}

responseCustomerPersonSet = requests.get(url, headers=headers)

print(json.dumps(responseCustomerPersonSet.json(), indent=3))

# COMMAND ----------

jsonCustomerPersonSet = responseCustomerPersonSet.json()
dataCustomerPersonSet = jsonCustomerPersonSet['d']

# COMMAND ----------

saveRequestDeltaTable("NombreTabla", dataCustomerPersonSet)   # Nombre tabla

# COMMAND ----------

# MAGIC %sql SELECT * FROM     /* Nombre Tabla

# COMMAND ----------

# MAGIC %md ##Loans in Arrears - Flat List

# COMMAND ----------

url = "https://sbx-latp-apim.prod.apimanagement.us20.hana.ondemand.com/s4b/v1/lacqan/ContractSet?$select=BorrowerID,LoanContractID,CurrentOpenItemsAmount,CurrentOpenItemsCounter,CurrentOldestDueDate,CurrentOverdueDays,BorrowerName&$format=json"  # Seleccinar las propiedades deseadas en el documento "SAP Cloud for Banking - API Documentation  - v1.15.pdf" página 114

headers = {
  'Accept-Encoding': 'gzip, deflate',
  'Authorization': 'Bearer '+ access_token
}

responseLoansArrearsFlatList = requests.get(url, headers=headers)

print(json.dumps(responseLoansArrearsFlatList.json(), indent=3))

# COMMAND ----------

jsonLoansArrearsFlatList = responseLoansArrearsFlatList.json()
dataLoansArrearsFlatList = jsonLoansArrearsFlatList['d']['results']

# COMMAND ----------

saveRequestDeltaTable("<NombreTabla>", dataLoansArrearsFlatList)   # Nombre tabla

# COMMAND ----------

# MAGIC %sql SELECT * FROM     /* Nombre tabla

# COMMAND ----------

# MAGIC %md ##Bank Employee - Bank Account & Contract Overview Read Accounts

# COMMAND ----------

url = "https://sbx-latp-apim.prod.apimanagement.us20.hana.ondemand.com/s4b/v1/bacovr/AccountSet?$filter=AccountHolderID eq '<AccountHolder>'&$format=json"   # Indicar Account Holder

headers = {
  'Accept-Encoding': 'gzip, deflate',
  'Authorization': 'Bearer '+ access_token
}

responseBankEmployeeAccountContractOverviewReadAccounts = requests.get(url, headers=headers)

print(json.dumps(responseBankEmployeeAccountContractOverviewReadAccounts.json(), indent=3))

# COMMAND ----------

jsonBankEmployeeAccountContractOverviewReadAccounts = responseBankEmployeeAccountContractOverviewReadAccounts.json()
dataBankEmployeeAccountContractOverviewReadAccounts = jsonBankEmployeeAccountContractOverviewReadAccounts['d']['results']

# COMMAND ----------

saveRequestDeltaTable("<NombreTabla>", dataBankEmployeeAccountContractOverviewReadAccounts)   # Nombre Tabla

# COMMAND ----------

# MAGIC %sql SELECT * FROM   /* Nombre tabla

# COMMAND ----------

# MAGIC %md ##Bank Employee - Bank Account & Contract Overview read transaction a single account

# COMMAND ----------

url = "https://sbx-latp-apim.prod.apimanagement.us20.hana.ondemand.com/s4b/v1/bacovr/AccountSet('<NumeroContrato>')/Transactions?$format=json"   # Número de contrato

headers = {
  'Authorization': 'Bearer '+ access_token
}

responseBankEmployeeAccountContractOverviewTransactionSingleAccount = requests.get(url, headers=headers)

print(json.dumps(responseBankEmployeeAccountContractOverviewTransactionSingleAccount.json(), indent=3))

# COMMAND ----------

jsonBankEmployeeAccountContractOverviewTransactionSingleAccount = responseBankEmployeeAccountContractOverviewTransactionSingleAccount.json()
dataBankEmployeeAccountContractOverviewTransactionSingleAccount = jsonBankEmployeeAccountContractOverviewTransactionSingleAccount['d']['results']

# COMMAND ----------

dataCleanBankEmployeeAccountContractOverviewTransactionSingleAccount = removeEmptyElements(dataBankEmployeeAccountContractOverviewTransactionSingleAccount)

# COMMAND ----------

saveRequestDeltaTable("<NombreTabla>", dataCleanBankEmployeeAccountContractOverviewTransactionSingleAccount)   # Nombre tabla

# COMMAND ----------

# MAGIC %sql SELECT * FROM    /* Nombre tabla

# COMMAND ----------

# MAGIC %md ##Bank Employee - Bank Account & Contract Overview read transaction a single loan account

# COMMAND ----------

url = "https://sbx-latp-apim.prod.apimanagement.us20.hana.ondemand.com/s4b/v1/bacovr/AccountSet('<NumeroContrato>')/Transactions?$format=json"  # Número contrato

headers = {
  'Authorization': 'Bearer '+ access_token
}

responseBankEmployeeAccountContractOverviewTransactionSingleLoanAccount = requests.get(url, headers=headers)

print(json.dumps(responseBankEmployeeAccountContractOverviewTransactionSingleLoanAccount.json(), indent=3))

# COMMAND ----------

jsonBankEmployeeAccountContractOverviewTransactionSingleLoanAccount = responseBankEmployeeAccountContractOverviewTransactionSingleLoanAccount.json()
dataBankEmployeeAccountContractOverviewTransactionSingleLoanAccount = jsonBankEmployeeAccountContractOverviewTransactionSingleLoanAccount['d']['results']

# COMMAND ----------

dataCleanBankEmployeeAccountContractOverviewTransactionSingleLoanAccount = removeEmptyElements(dataBankEmployeeAccountContractOverviewTransactionSingleLoanAccount)

# COMMAND ----------

saveRequestDeltaTable("<NombreTabla>", dataCleanBankEmployeeAccountContractOverviewTransactionSingleLoanAccount)   # Nombre tabla

# COMMAND ----------

# MAGIC %sql SELECT * FROM    /* Nombre tabla

# COMMAND ----------

# MAGIC %md ##Read Loan Contract Transactions

# COMMAND ----------

url = "https://sbx-latp-apim.prod.apimanagement.us20.hana.ondemand.com/s4b/v1/lacovr/ContractSet('<NumeroContrato>')/Transactions?$format=json"   # Número contrato

headers = {
  'Authorization': 'Bearer '+ access_token
}

responseLoanContractTransactions = requests.get(url, headers=headers)

print(json.dumps(responseLoanContractTransactions.json(), indent=3))

# COMMAND ----------

jsonLoanContractTransactions = responseLoanContractTransactions.json()
dataLoanContractTransactions = jsonLoanContractTransactions['d']['results']

# COMMAND ----------

dataCleanLoanContractTransactions = removeEmptyElements(dataLoanContractTransactions)

# COMMAND ----------

saveRequestDeltaTable('<NombreTabla>', dataCleanLoanContractTransactions)   # Nombre tabla

# COMMAND ----------

# MAGIC %sql SELECT * FROM    /* Nombre tabla

# COMMAND ----------

# MAGIC %md ##Loan Contracts of a Customer

# COMMAND ----------

url = "https://sbx-latp-apim.prod.apimanagement.us20.hana.ondemand.com/s4b/v1/lacovr/ContractSet?$format=json&$filter=BorrowerID eq '<NumeroCliente>'"   # Número cliente

headers = {
  'Accept-Encoding': 'gzip, deflate',
  'Authorization': 'Bearer ' + access_token
}

responseLoanContractsCustomer = requests.get(url, headers=headers)

print(json.dumps(responseLoanContractsCustomer.json(), indent=3))

# COMMAND ----------

jsonLoanContractsCustomer = responseLoanContractsCustomer.json()
dataLoanContractsCustomer = jsonLoanContractsCustomer['d']['results']

# COMMAND ----------

saveRequestDeltaTable('<NombreTabla>', dataLoanContractsCustomer)   # Nombre tabla

# COMMAND ----------

# MAGIC %sql SELECT * FROM    /* Nombre tabla
