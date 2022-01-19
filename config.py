import os, sys
from pathlib import Path
from dotenv import load_dotenv
load_dotenv(override=True)

## Main configuration 
SITE = Path(__file__).parent if "__file__" in globals() else Path(os.getcwd())

DEFAULT_ENV = "staging"
AZ_IDENTITY = "managed"       # managed   service-principal   local
CALL_DBKS   = "Docker"     # Docker    Windows

VERSION     = "1.0.40"

## Parameters. 
PAGE_MAX = 1000


## Keys, and parameters Values, and mappers. 
URLS = {
    "api-call"  : {
        "local-flask"  : "http://localhost:5000",
        "local-fastapi": "http://localhost:80",
        "staging"      : "https://wap-cx-collections-dev.azurewebsites.net",
        "qa"           : "https://apim-crosschannel-tech-dev.azure-api.net/data-"
    },  
    "sap-calls" : {
        "pre-qa"    : {
            "auth"  : "https://latp-apim.prod.apimanagement.us20.hana.ondemand.com/oauth2/token", 
            "apis"  : "https://sbx-latp-apim.prod.apimanagement.us20.hana.ondemand.com/s4b"}, 
        "qa"        : {
            "auth"  : "https://apiqas.apimanagement.us21.hana.ondemand.com/oauth2/token", 
            "apis"  : "https://apiqas.apimanagement.us21.hana.ondemand.com/s4b"
    }   }, 
    "crm-call"      : {
        "sandbox"   : "https://bineo1633010523.zendesk.com/api"
    },
    "api-call-pre"  : { # Se quitó el versionamiento del Back End para hacerlo desde API Mgmt. 
        "local"     : "http://localhost:5000/v1/get-loan-messages", 
        "staging"   : "https://wap-cx-collections-dev.azurewebsites.net/v1/get-loan-messages"
}   }

ENV_KEYS = {
    "platform" : {
        "key-vault" : {
            "name"  : "kv-collections-data-dev", 
            "url"   : "https://kv-collections-data-dev.vault.azure.net/"}, 
        "storage"   : {
            "name"  : "lakehylia", 
            "url"   : "https://lakehylia.blob.core.windows.net/"}, 
        "app-id"    : "cx-collections-id",
        "service-principal" : {
            "client_id"       : "SP_APP_ID", 
            "client_secret"   : "SP_APP_SECRET", 
            "tenant_id"       : "AZ_TENANT", 
            "subscription_id" : "AZ_SUBSCRIPTION"}, 
        "databricks": {
            "client_id"       : "sp-lakehylia-app-id", 
            "client_secret"   : "sp-lakehylia-secret", 
            "tenant_id"       : "aad-tenant-id", 
            "subscription_id" : "aad-subscription-id"}, },
    "core"  : {
        "qa-vault"  : {
            "key"   : "core-api-key", 
            "secret": "core-api-secret", 
            "user"  : "core-api-user", 
            "pass"  : "core-api-password"}, 
        "qa"        : {
            "key"   : "SAP_KEY", 
            "secret": "SAP_SCRT", 
            "user"  : "SAP_USER", 
            "pass"  : "SAP_PASS"},
        "pre-qa"    : {
            "key"   : "SAP_KEY_PRE", 
            "secret": "SAP_SRCT_PRE", 
            "user"  : "SAP_USER_PRE", 
            "pass"  : "SAP_PASS_PRE"}   
}   }


SPARK_DBKS = {
    # Se indica como (1, ENV) cuando el valor se guarda en variable de ambiente. 
    "Windows": {
        "DSN": (1, "DBKS_ODBC_DSN")
    },
    "Docker" : {
        "Driver"         : "/opt/simba/spark/lib/64/libsparkodbc_sb64.so",
        "HOST"           : (1, "dbks-odbc-host"),
        "PORT"           : "443",
        "Schema"         : "default",
        "SparkServerType": "3",
        "AuthMech"       : "3",
        "UID"            : "token",
        "PWD"            : (1, "dbks-wap-token"),
        "ThriftTransport": "2",
        "SSL"            : "1",
        "HTTPPath"       : (1, "dbks-odbc-http")
    }
}

DBKS_TABLAS = {  # NOMBRE_DBKS, COLUMNA_EXCEL
    "contracts"   : "bronze.loan_contracts", 
    "collections" : "gold.loan_contracts"
}

## More specific configurations

# Prepare Azure Identity
if "ipykernel" not in sys.modules:
    
    from azure.identity import ClientSecretCredential, DefaultAzureCredential
    from azure.keyvault.secrets import SecretClient

    azure_keys = ENV_KEYS["platform"]
    if  AZ_IDENTITY == "managed":
        credential = DefaultAzureCredential()
        print()
    elif AZ_IDENTITY == "service-principal": 
        credential = ClientSecretCredential(**{k: os.getenv(v) 
            for (k, v) in azure_keys["service-principal"].items() })
    elif AZ_IDENTITY == "local": 
        credential = ClientSecretCredential(**{k: os.getenv(v) 
            for (k, v) in azure_keys["service-principal"].items() })

    vault_client = SecretClient(vault_url=azure_keys["key-vault"]["url"], credential=credential)

    ## Prepare connections. 
    secret_getters = {
        "Windows" : os.getenv, 
        "Docker"  : lambda name: vault_client.get_secret(name).value
    }
    get_secret = secret_getters[CALL_DBKS]

    # map_val = lambda val: val if not isinstance(val, tuple) else get_secret(val[1])

    # DBKS_CONN = ";".join( f"{k}={map_val(v)}" for (k,v) in SPARK_DBKS[CALL_DBKS].items())
    