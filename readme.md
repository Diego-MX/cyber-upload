

# Introduction

This repo is closely related to `data-cx-collections`.  
While the latter contains the web app being called by CX team in order to return data, 
the code within this repo prepares the data for later calls.  

It has two stages:  
- Initial/sporadic configuration ...   
- Regular data updates ...  

And it also sets some common references for the other given repo with datalake/cosmosDB.  


## Runbook 

Some variable names are predefined as to match the ones in earlier stage of development.  
To suggest a change, consider redefining it in the code.   

Previous requirements: 
- Databricks secrets scope (`cx-collections`) with service principal keys:  
  `sp-collections-client`, `sp-collections-secret`, `aad-tenant-id`, `sp-collections-subscription`  
  This works as an identity for the running application.  
  En `qas` se usaron: `sp-core-events-(client|secret|subscription)`

- Azure key vault (`kv-cx-data-<env>`);  (`kv-cx-collections-dev`)
  Give access to the service principal above and set keys:  
  SAP keys: `core-api-key`, `core-api-secret`, `core-api-user`, `core-api-pass`  
  CRM keys:  `crm-api-user`, `crm-api-token`, 
      `crm-zis-id`, `crm-zis-user`, `crm-zis-pass`  

- Databricks Tables and access.  
  Give access to the service principal above to datalake, e.g. storage container `stlakehylia<qas>` 
  and its corresponding 'folders' (`bronze`, `silver`, `gold`) at `.../ops/core-banking/batch-updates/...`  
  `persons_set`, `loan_contracts`, `loan_contracts`,  
  `loan_balances`, `loan_open_items`, `loan_payments`,   
  `zendesk_promises`  
  
- File `config.py`, consider the following checks:  
  Environment variables `ENV_TYPE` (`dev`, `qas`, `stg`, `prd`)  
  `SERVER_TYPE=dbks` (in other places we may use `wap`, `local-win`, `local-mac`)  
  `CORE_ENV=<ENV>-sap` (it is not always the case that our environment stage is the same as the core's)  
  `CRM_ENV=<ENV>-zd` (same as above, and consider that the names may differ, eg. `sandbox` instead of `dev`)
  
- Also in `config.py` check following configurations:   
  `CORE_KEYS[$CORE_ENV] = ... main[base-url], calls[auth, url]`  
  `CRM_KEYS[$CRM_ENV]   = ... main[url]`
  Azure resources: `key-vault`, `storage`, (`app-id`). 
  
- Inside Databricks, not readily automized:   
  0. Check configuration: metastore and other mothers.  
  1. Create tables.  
  2. Create jobs from notebooks.  
  
  
- For the twin repository `data-collections-webapp`, also setup:  
  Key vault secrets:  `dbks-wap-token`, `dbks-odbc-host`, `dbks-odbc-http`  
  corresponding to -obviously- Databricks connection parameters.  
  
  
  

## Jobs/Notebooks: 

- `1 update core iterator` calls SAP functions to be iterated by user.   
  This is not optimal, as the number of users grow.  Its setup every hour.  

- `1 update core simple zoras` calls SAP functions to be called once for all users.   
  Also updated every hour, which does work.   

- `1 update promises` calls Zendesk functions to update promises every hour.   


Other notebooks that are called on a regular basis and stored elsewhere ... _en el_ 
workspace de Jacobo:   

- `bronze_to_silver_motor_cobranza`   
  Reads `bronze`: `loan_contracts`, `persons_set`, `loan_balances`, 
    `loan_open_items`, `loan_payment_plans`.   
  Writes `silver`: `loan_payment_plans`, `loan_balances`, `loan_contracts`, 
    `loan_open_items`, `persons_set`. 
- `brz_to_slv_promises`
  Reads `bronze.crm_payment_promises`
  Writes `silver.zendesk_promises`

- `tests`
  Reads `silver`: `loan_contracts`, `loan_payment_plans`,  `loan_balances`, 
    `loan_open_items`, `persons_set`
  Writes `gold.loan_contracts`



## Scripts 

The scripts that are used are:  

- `./config.py` keeps all the parameters for the connections as well as the main 
  class object to get them, i.e. `VaultSetter`.  
  Along with a given _key vault_, this object sets the credentials in order to 
  access it; in addition, it prepares the secret-getter in order to create other connectors.   

- `src/core_banking.py` with the help of a _Vault Setter_ object, here we define a
  an object that calls SAP's APIs, the `SAPSession`.   
  One thing to note is that at the beginning it used to be based on regular `requests` sessions, 
  and they evolved into `httpx` async clients, but kept the _Session_ nomenclature.  

- `src/crm_platform.py` similar to the one defined before, the `ZendeskSession` calls
  the Zendesk APIs, and stores whatever credentials and cookies are needed.  
  Different from it -however- this hasn't been upgraded to its async version.  

-  `src/platform_resources.py`, besides the standard credentials in the _Vault Setter_
  object, an `AzureResourcer` allows us to further expand on getting such like 
  resources.  
  Some examples are:  _storage containers_, _cosmos database_, etc. 

Notice that these objects are constructed in their own script files, but their 
configuration options are set in the firstly mentioned `./config.py` file.  

# Flow  

In order to establish the reference and data consistency, we use the following work flow.  

1. `CX - Strategy & Communications.xlsx` is the first source of reference.   
  It holds the variables to be used by the Collections Engine, and different annotations
  from the users.  
  However this is a functional (Excel) file, and therefore its 
  use in the on-running processes is limited.  

2. From brother repo `cx-collections`, the sporadic process `src.setup_columns.py` is
  run in order to match the columns in the given Excel file and its counterparts
  in the deltalake.  
  This results in reference tables within CosmosDB (to implement, but previously 
  setup as feather files in the running repo... not ideal). 

3. Using the reference tables, the _update_ notebooks are run every hour or so, and update the data. 

4. Also the _silver_ and _gold_ notebooks are run, and update the consultation delta table. 

5. Brother repo `cx-collections` runs a web-app that is open for requests by CX Team to the given 
  delta table.  
  The web-app interestingly sets up a connection to the deltalake. 

6. Everything works honky dory.  =D










  
