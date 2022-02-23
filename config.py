
import os, sys
import re
from pathlib import Path
from dotenv import load_dotenv
# from pydantic import SecretStr

in_dbks = 'ipykernel' in sys.modules
ENV = 'dbks' if in_dbks else os.environ.get('ENV', 'local')
SITE = Path(__file__).parent if '__file__' in globals() else Path(os.getcwd())

if in_dbks: 
    from pyspark.sql import SparkSession
    from pyspark.dbutils import DBUtils
    spark = SparkSession.builder.getOrCreate()
    dbutils = DBUtils(spark)



URLS = {
    'api-call'  : {
        'local'     : 'http://localhost:80',
        'staging'   : 'https://wap-cx-collections-dev.azurewebsites.net',
        'qa'        : 'https://apim-crosschannel-tech-dev.azure-api.net/data-'},  
    
    'api-call-pre'  : { 
        # Se quitó el versionamiento del Back End para hacerlo desde API Mgmt. 
        'local'     : 'http://localhost:5000/v1/get-loan-messages', 
        'staging'   : 'https://wap-cx-collections-dev.azurewebsites.net/v1/get-loan-messages'
} }


PLATFORM_KEYS = {
    'local': {        
        'key-vault' : {
            'name'  : 'kv-collections-data-dev', 
            'url'   : 'https://kv-collections-data-dev.vault.azure.net/'}, 
        'storage'   : {
            'name'  : 'lakehylia', 
            'url'   : 'https://lakehylia.blob.core.windows.net/'}, 
        'app-id'    : 'cx-collections-id',
        'service-principal' : {
            'client_id'       : 'SP_APP_ID', 
            'client_secret'   : 'SP_APP_SECRET', 
            'tenant_id'       : 'AZ_TENANT', 
            'subscription_id' : 'AZ_SUBSCRIPTION'}, 
        'dbks': {
            'client_id'       : 'sp-lakehylia-app-id', 
            'client_secret'   : 'sp-lakehylia-secret', 
            'tenant_id'       : 'aad-tenant-id', 
            'subscription_id' : 'aad-subscription-id'}}, 
    'dbks': {        'key-vault' : {
            'name'  : 'kv-collections-data-dev', 
            'url'   : 'https://kv-collections-data-dev.vault.azure.net/'}, 
        'storage'   : {
            'name'  : 'lakehylia', 
            'url'   : 'https://lakehylia.blob.core.windows.net/'}, 
        'app-id'    : 'cx-collections-id',
        'service-principal' : {
            'client_id'       : 'SP_APP_ID', 
            'client_secret'   : 'SP_APP_SECRET', 
            'tenant_id'       : 'AZ_TENANT', 
            'subscription_id' : 'AZ_SUBSCRIPTION'}, 
        'dbks': {
            'client_id'       : 'sp-lakehylia-app-id', 
            'client_secret'   : 'sp-lakehylia-secret', 
            'tenant_id'       : 'aad-tenant-id', 
            'subscription_id' : 'aad-subscription-id'}},
    'dev': {        
        'key-vault' : {
            'name'  : 'kv-collections-data-dev', 
            'url'   : 'https://kv-collections-data-dev.vault.azure.net/'}, 
        'storage'   : {
            'name'  : 'lakehylia', 
            'url'   : 'https://lakehylia.blob.core.windows.net/'}, 
        'app-id'    : 'cx-collections-id',
        'service-principal' : {
            'client_id'       : 'SP_APP_ID', 
            'client_secret'   : 'SP_APP_SECRET', 
            'tenant_id'       : 'AZ_TENANT', 
            'subscription_id' : 'AZ_SUBSCRIPTION'}, 
        'databricks': {
            'client_id'       : 'sp-lakehylia-app-id', 
            'client_secret'   : 'sp-lakehylia-secret', 
            'tenant_id'       : 'aad-tenant-id', 
            'subscription_id' : 'aad-subscription-id'}}, 
    'qas': {        
        'key-vault' : {
            'name'  : 'kv-collections-data-dev', 
            'url'   : 'https://kv-collections-data-dev.vault.azure.net/'}, 
        'storage'   : {
            'name'  : 'lakehylia', 
            'url'   : 'https://lakehylia.blob.core.windows.net/'}, 
        'app-id'    : 'cx-collections-id',
        'service-principal' : {
            'client_id'       : 'SP_APP_ID', 
            'client_secret'   : 'SP_APP_SECRET', 
            'tenant_id'       : 'AZ_TENANT', 
            'subscription_id' : 'AZ_SUBSCRIPTION'}, 
        'databricks': {
            'client_id'       : 'sp-lakehylia-app-id', 
            'client_secret'   : 'sp-lakehylia-secret', 
            'tenant_id'       : 'aad-tenant-id', 
            'subscription_id' : 'aad-subscription-id'}}}


CORE_KEYS = {
    'dev':{
        'main' : { 
            'headers' : {
                'format'          : 'json',
                'Accept-Encoding' : 'gzip, deflate',
                'Accept'          : 'application/json' }, 
            'base-url' : 'https://sbx-latp-apim.prod.apimanagement.us20.hana.ondemand.com/s4b'},
            'access' : {
                'username': (1, 'core-api-key'), 
                'password': (1, 'core-api-secret')}, 
        'calls' : {
            'auth' : {
                'url' : 'https://latp-apim.prod.apimanagement.us20.hana.ondemand.com/oauth2/token', 
                'data': {
                    'grant_type' : 'password', 
                    'username'   : (1, 'core-api-user'), 
                    'password'   : (1, 'core-api-password')}
            },
            'contract-set' : {'sub-url' : 'v1/lacovr/ContractSet'}, 
            'person-set'   : {'sub-url' : 'v15/bp/PersonSet'}
        }           
    }, 
    'qas':{
        'main'  : {
            'headers' : {
                    'format'          : 'json',
                    'Accept-Encoding' : 'gzip, deflate',
                    'Accept'          : 'application/json' }, 
            'base-url' : 'https://apiqas.apimanagement.us21.hana.ondemand.com/s4b',
            'access' : {
                'username': (1, 'core-api-key'), 
                'password': (1, 'core-api-secret')}}, 
        'calls' : {
            'auth' : {
                'url' : 'https://apiqas.apimanagement.us21.hana.ondemand.com/oauth2/token', 
                'data': {
                    'grant_type' : 'password', 
                    'username'   : (1, 'core-api-user'), 
                    'password'   : (1, 'core-api-password')}
            },
            'contract-set' : {'sub-url' : 'v1/lacovr/ContractSet'}, 
            'person-set'   : {'sub-url' : 'v15/bp/PersonSet'}
        }
    }
}


CRM_KEYS = {
    'sandbox' : {
        'main' : {
            'url'  : 'https://bineo1633010523.zendesk.com/api',
            'username' : (1, 'crm-api-user'),   # ZNDK_USER_EMAIL
            'password' : (1, 'crm-api-token'),  # ZNDK_API_TOKEN
        }, 
        'zis'  : {
            'id'      : (1, 'crm-zis-id'), 
            'username': (1, 'crm-zis-user'), 
            'password': (1, 'crm-zis-pass')}, 
        'calls' : {
            'promises' : {
                'sub-url' : 'sunshine/objects/records' 
            }, 
            'filters'  : ('sunshine/objects/records',
                    'services/zis/inbound_webhooks/generic/ingest')
            
        }
    }, 
    'qas' : {
        'url': '', 
        'user':'',
        'token': ''
    }
}


DBKS_TABLAS = {  # NOMBRE_DBKS, COLUMNA_EXCEL
    'contracts'   : 'bronze.loan_contracts', 
    'collections' : 'gold.loan_contracts'
}


PAGE_MAX = 1000


class ConfigEnviron():
    '''
    Depending on ENV_TYPE [local, databricks, dev, qas, prod], this class sets 
    methods to access first-tier secrets from the taylored-made dictionary. 
    Upon accessing first-tier secrets, a key vault is reached and other secrets
    can be accessed independently. 
    That is: 
    - local          -> os.getenv(*)
    - databricks     -> dbutils.secrets.get(scope, *)
    - dev, qas, prod -> not needed, as access to keyvault is granted on launch.

    '''
    def __init__(self, env_type):
        self.env = env_type
        self.set_secret_getter()


    def set_secret_getter(self): 
        if  self.env == 'local': 
            load_dotenv('.env', override=True)        
            def get_secret(key): 
                return os.getenv(key)

        elif self.env == 'dbks': 
            the_scope = 'kv-resource-access-dbks'
            def get_secret(a_key): 
                mod_key = re.sub('_', '-', a_key.lower())
                the_val = dbutils.secrets.get(scope=the_scope, key=mod_key)
                return the_val

        self.get_secret = get_secret


    def call_dict(self, a_dict): 
        def pass_val(a_val): 
            is_tuple = isinstance(a_val, tuple)
            to_pass = self.get_secret(a_val[1]) if is_tuple else a_val
            return to_pass

        return {k: pass_val(v) for (k, v) in a_dict.items()}
        
