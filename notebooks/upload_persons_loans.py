# Databricks notebook source
# MAGIC %md 
# MAGIC ## Runbook
# MAGIC 
# MAGIC This notebook is a rescripture of the script `src/refresh_core.py`, as well as its dependencies to be compiled as one. 
# MAGIC 
# MAGIC It should be associated with a databricks-job to be executed **every hour** or less. 
# MAGIC 
# MAGIC Access to the key-vault `kv-resource-access-dbks` is required, where key-values are obtained towards a service-principal, which in turn gives access to a second key-vault.
# MAGIC 
# MAGIC The latter's name, `kv-collections-data-dev`, is not as relevant since it can be obtained from the (secret-less) configuration file `./config.py`. 

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC ## Preparation
# MAGIC 
# MAGIC The cells are executed as follows:   
# MAGIC 1. Setup Libraries, utility functions, and a path token for caching.  
# MAGIC 
# MAGIC 2. Define some Env variables:  
# MAGIC     a. From Config File,  
# MAGIC     b. Using Databricks secrets, Key Vault is for Databricks access.  
# MAGIC     c. To then connect with a Service Principal,   
# MAGIC     d. And use a specific Key Vault, which is defined specific for Collections app.   
# MAGIC     
# MAGIC 3. Core Banking functions to retrieve Loan Contracts, Person Set, and Tokens in between. 
# MAGIC 
# MAGIC 4. Execution. 

# COMMAND ----------

# MAGIC %md 
# MAGIC #### Libraries and Basic Functions

# COMMAND ----------

import os, sys, pandas as pd
#sys.path.append("/Workspace/Repos/diego.v@bineo.com/cx-collections")
from pathlib import Path
from datetime import datetime as dt
from pyarrow import feather

from requests import get as rq_get, post

from azure.identity import ClientSecretCredential, DefaultAzureCredential
from azure.keyvault.secrets import SecretClient
from azure.storage.blob import BlobServiceClient

from config import SITE, ENV_KEYS, URLS

def secret_by_key(a_key, scope="kv-resource-access-dbks"):
    the_secret = dbutils.secrets.get(scope=scope, key=a_key)
    return the_secret

sap_token_file = SITE/"refs/config/SAP_token.json"


# COMMAND ----------

# MAGIC %r
# MAGIC library(lubridate)
# MAGIC library(tidyverse)
# MAGIC library(arrow)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Tools from `tools.py`

# COMMAND ----------

from requests import auth
from base64 import b64encode as enc64

class BearerAuth(auth.AuthBase):
    def __init__(self, token):
        self.token = token

    def __call__(self, req):
        req.headers["authorization"] = f"Bearer {self.token}"
        return req

def dict_minus(a_dict, keys, copy=True): 
    b_dict = a_dict.copy() if copy else a_dict
    if isinstance(keys, str): 
        keys = [keys]
    for key in keys:
        b_dict.pop(key)
    return b_dict

def encode64(a_str): 
    encoded = enc64(a_str.encode('ascii')).decode('ascii')
    return encoded


# COMMAND ----------

# MAGIC %md
# MAGIC #### Key Vaults and Env Variables

# COMMAND ----------

urls = URLS["sap-calls"]["qa"]
AUTH_URL = urls["auth"]
APIS_URL = urls["apis"]

# Access to Auth. 
core_keys   = "qa-vault" 
core_access = ENV_KEYS["core"][core_keys]  

azure_keys = ENV_KEYS["platform"]

az_creds = ClientSecretCredential(**{k: secret_by_key(v) 
        for (k, v) in azure_keys["databricks"].items() })

vault_client = SecretClient(vault_url=azure_keys["key-vault"]["url"], 
        credential=az_creds)
get_secret = lambda name: vault_client.get_secret(name).value


KEY      = get_secret(core_access["key"])
USER     = get_secret(core_access["user"])
PASS     = get_secret(core_access["pass"])
SECRET   = get_secret(core_access["secret"])


# COMMAND ----------

# MAGIC %md 
# MAGIC #### Core Banking Functions

# COMMAND ----------

def get_token(auth_method=None):
    if auth_method == "basic":  # Auth as a POST argument. 
        auth_kwargs = {"auth" : HTTPBasicAuth(KEY, SECRET)} 
    else: 
        auth_kwargs = {}        # Auth in Headers. 

    post_kwargs = {
        "headers" : get_headers("token", auth_method),
        "data"    : {
            "grant_type" : "password", 
            "username"   : USER, 
            "password"   : PASS
    }}
    post_kwargs.update(auth_kwargs)

    the_resp  = post(AUTH_URL, **post_kwargs)
    the_token = the_resp.json()
    return the_token["access_token"]


def get_headers(call=None, auth_method="header"):
    if call == "apis":
        the_headers = {
                "format"          : "json",
                "Accept-Encoding" : "gzip, deflate",
                "Accept"          : "application/json" }
                # "Authorization"   : f"Bearer {token}"  
                # La autorización se resuelve con la clase de BearerAuth.
    elif call == "token":
        if auth_method   == "basic":
            raise "Auth Method not used."
        elif auth_method == "header":
            auth_enc    = encode64(f"{KEY}:{SECRET}")
            the_headers = { 
                    "Content-Type"  : "application/x-www-form-urlencoded",
                    "Authorization" :f"Basic {auth_enc}"}
    return the_headers
    
    
def get_sap_loans(attrs_indicator=None):
    the_token    = get_token("header")
    
    the_hdrs     = get_headers("apis", "headers")
    select_attrs = attributes_from_column(attrs_indicator)
    the_params   = { "$select" : ",".join(select_attrs) }

    the_resp     = rq_get(f"{APIS_URL}/v1/lacqan/ContractSet", 
        auth=BearerAuth(the_token), headers=the_hdrs, params=the_params)

    loans_ls     = the_resp.json()["d"]["results"]  # [metadata : [id, uri, type], borrowerName]
    post_loans   = [ dict_minus(a_loan, "__metadata") for a_loan in loans_ls ]
    loans_df     = pd.DataFrame(post_loans)
    return loans_df 


def get_person_set():
    the_token    = get_token("header")
    the_hdrs     = get_headers("apis", "headers")

    the_resp     = rq_get(f"{APIS_URL}/v15/bp/PersonSet", 
        auth=BearerAuth(the_token), headers=the_hdrs)

    persons_ls     = the_resp.json()["d"]["results"]  # [metadata : [id, uri, type], borrowerName]
    dict_keys      = ["__metadata", "Roles", "TaxNumbers", "Relation", "Partner", "Correspondence"]
    post_persons   = [ dict_minus(a_loan, dict_keys) for a_loan in persons_ls ]
    persons_df     = (pd.DataFrame(post_persons)
        .assign(ID = lambda df: df.ID.str.pad(10, "left", "0")))

    return persons_df

def attributes_from_column(attrs_indicator=None):
    sap_attr = pd.read_feather(SITE/"refs/catalogs/sap_attributes.feather")
    possible_columns = list(sap_attr.columns) + ["all"]

    # en_cx, ejemplo, default, postman_default. 
    if attrs_indicator is None: 
        attrs_indicator = "postman_default"
    
    if attrs_indicator not in possible_columns: 
        raise("COLUMN INDICATOR must be one in SAP_ATTR or 'all'.")

    if attrs_indicator == "all":
        attr_df = sap_attr
    else: 
        attr_df = sap_attr.query(f"{attrs_indicator} == 1")
    
    return attr_df["atributo"]




# COMMAND ----------

# MAGIC %md 
# MAGIC #### Execute commands

# COMMAND ----------


loans_df    = get_sap_loans("all")
persons_df  = get_person_set()

loans_df.to_csv("/tmp/loan_contracts.csv", index=False)


# COMMAND ----------

loans_df.shape


# COMMAND ----------

# MAGIC %r
# MAGIC promesas_cols <- c("num_prestamo", "promesa_id", "promesa_activa", "promesa_descuento_principal", 
# MAGIC               "promesa_descuento_intereses", "promesa_descuento_comisiones", "promesa_monto", "promesa_fecha_inicio", 
# MAGIC               "promesa_fecha_vencimiento", "promesa_procesada", "promesa_cumplida", "promesa_ejecutivo")
# MAGIC 
# MAGIC loans_df   <- read_csv("/tmp/loan_contracts.csv", col_types=cols())
# MAGIC k_promesas <- nrow(loans_df)/2
# MAGIC 
# MAGIC set.seed(42)
# MAGIC promesas_df <- tibble(
# MAGIC     num_prestamo   = sample(loans_df$LoanContractID, k_promesas),
# MAGIC     promesa_id     = sample(10000:99999, k_promesas), 
# MAGIC     promesa_activa = runif(k_promesas) < 0.2, 
# MAGIC     promesa_descuento_principal  = sample(100*(1:10000), k_promesas, replace=TRUE), 
# MAGIC     promesa_descuento_intereses  = promesa_descuento_principal*0.15, 
# MAGIC     promesa_descuento_comisiones = promesa_descuento_principal*0.40, 
# MAGIC     promesa_monto                = promesa_descuento_principal*4.25, 
# MAGIC     promesa_fecha_inicio         = today() - sample(50, k_promesas, replace=TRUE), 
# MAGIC     promesa_fecha_vencimiento    = promesa_fecha_inicio + 100, 
# MAGIC     promesa_procesada            = runif(k_promesas) < 0.4, 
# MAGIC     promesa_cumplida             = runif(k_promesas) < 0.2, 
# MAGIC     promesa_ejecutivo            = sample(1000:9999, k_promesas))
# MAGIC     
# MAGIC write_csv(promesas_df, "/tmp/promesas.csv")

# COMMAND ----------

loans_cols   = {"LoanContractID": "num_prestamo", "DisbursedCapital" : "monto_dispersado", 
    "PaymentPlanStartDate" : "fecha_inicio", "BankRoutingID" : "clabe", "CurrentOverdueDays" : "dias_atraso", 
    "TermSpecificationValidityPeriodDuration" : "periodicidad_pago", "NominalInterestRate": "tasa_interes"}

persons_cols = {"BorrowerName" : "nombre", "UserLastname": "apellido", "UserMaternal": "apellido_2", 
    "BorrowerCity": "estado",  "AddressCounty" : "municipio", "AddressNeighborhood": "colonia", 
    "AddressStreet": "calle", "AddressExterior": "num_ext", "AddressInterior": "num_int", 
    "AddressZipcode": "zipcode", "UserAge":"edad", "UserGender": "genero"}

promesas_dtypes = {
    "num_prestamo"      : object,                   "promesa_id": object, 
    "promesa_activa"    : bool,                     "promesa_descuento_principal" : "float64", 
    "promesa_descuento_intereses" : "float64",      "promesa_descuento_comisiones" : "float64", 
    "promesa_monto"     : "float64",                "promesa_fecha_inicio" : "datetime64[ns]", 
    "promesa_fecha_vencimiento" : "datetime64[ns]", "promesa_procesada" : bool,
    "promesa_cumplida" : bool,                      "promesa_ejecutivo" : object}

promises_df = pd.read_csv("/tmp/promesas.csv")

gold_df = (loans_df
    .join(persons_df.set_index("ID"), on="BorrowerID", lsuffix="", rsuffix="_person")
    .join(promises_df.set_index("num_prestamo"), on="LoanContractID", lsuffix="", rsuffix="_person")
    .rename(columns=loans_cols).rename(columns=persons_cols)
    .astype(promesas_dtypes))
               

# COMMAND ----------


gold_df

# COMMAND ----------

spark.conf.set("spark.sql.parquet.mergeSchema", True)
promises_spk= spark.createDataFrame(promises_df)
loans_spk   = spark.createDataFrame(loans_df)
gold_spk    = spark.createDataFrame(gold_df)

# COMMAND ----------


loans_spk.write.mode("overwrite").saveAsTable("bronze.loan_contracts")
persons_spk.write.mode("overwrite").saveAsTable("bronze.persons_set")
gold_spk.write.saveAsTable("gold.collections_test_promises")

