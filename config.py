
import os
import re
from pathlib import Path
from azure.identity import ClientSecretCredential
from azure.identity._credentials.default import DefaultAzureCredential
try: 
    from dotenv import load_dotenv
except ImportError:
    load_dotenv = None
try: 
    from pyspark.dbutils import DBUtils
except ImportError: 
    DBUtils = None

SITE = Path(__file__).parent if '__file__' in globals() else Path(os.getcwd())
ENV = os.environ.get('ENV_TYPE')        # dev, qas, stg, prod
SERVER = os.environ.get('SERVER_TYPE')  # dbks, local, wap. 


PAGE_MAX = 1000

SETUP_KEYS = {
    'dev' : {
        'service-principal' : {
            'client_id'       : (1, 'sp-lakehylia-app-id'), 
            'client_secret'   : (1, 'sp-lakehylia-secret'), 
            'tenant_id'       : (1, 'aad-tenant-id'), 
            'subscription_id' : (1, 'aad-subscription-id') } , 
        'dbks': {'scope': 'kv-resource-access-dbks'}
    }, 
    'qas' : {}
}


PLATFORM_KEYS = {
    'local' : {
        'key-vault' : {
            'name'  : 'kv-collections-data-dev', 
            'url'   : 'https://kv-collections-data-dev.vault.azure.net/'},
        'storage'   : {
            'name'  : 'lakehylia', 
            'url'   : 'https://lakehylia.blob.core.windows.net/'}, 
        'app-id'    : 'cx-collections-id',
        'service-principal' : {
            'client_id'       : (1, 'SP_LAKEHYLIA_APP_ID'), 
            'client_secret'   : (1, 'SP_LAKEHYLIA_SECRET'), 
            'tenant_id'       : (1, 'AAD_TENANT_ID'), 
            'subscription_id' : (1, 'AAD_SUBSCRIPTION_ID') } },
    'dev': {        
        'key-vault' : {
            'name'  : 'kv-collections-data-dev', 
            'url'   : 'https://kv-collections-data-dev.vault.azure.net/'}, 
        'storage'   : {
            'name'  : 'lakehylia', 
            'url'   : 'https://lakehylia.blob.core.windows.net/'}, 
        'app-id'    : 'cx-collections-id',
        'service-principal' : {
            'client_id'       : 'SP_LAKEHYLIA_APP_ID', 
            'client_secret'   : 'SP_LAKEHYLIA_SECRET', 
            'tenant_id'       : 'AAD_TENANT_ID', 
            'subscription_id' : 'AAD_SUBSCRIPTION_ID' } }, 
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
            'subscription_id' : 'AZ_SUBSCRIPTION' } } }


CORE_KEYS = {
    'dev-sap': {
        'main' : { 
            'base-url' : 'https://sbx-latp-apim.prod.apimanagement.us20.hana.ondemand.com/s4b',
            'access' : {
                'username': (1, 'core-api-key'), 
                'password': (1, 'core-api-secret') }, 
            'headers' : {
                'format'          : 'json',
                'Accept-Encoding' : 'gzip, deflate',
                'Accept'          : 'application/json'} }, 
        'calls' : {
            'auth' : {
                'url' : 'https://latp-apim.prod.apimanagement.us20.hana.ondemand.com/oauth2/token', 
                'data': {
                    'grant_type' : 'password', 
                    'username'   : (1, 'core-api-user'), 
                    'password'   : (1, 'core-api-password') } },
            'contract-set' : {'sub-url' : 'v1/lacovr/ContractSet'}, 
            'person-set'   : {'sub-url' : 'v15/bp/PersonSet'} } }, 
    'qas-sap' : {
        'main' : {
            'headers' : {
                'format'          : 'json',
                'Accept-Encoding' : 'gzip, deflate',
                'Accept'          : 'application/json' }, 
            'base-url' : 'https://apiqas.apimanagement.us21.hana.ondemand.com/s4b',
            'access' : {
                'username': (1, 'core-api-key'), 
                'password': (1, 'core-api-secret') } }, 
        'calls' : {
            'auth' : {
                'url' : 'https://apiqas.apimanagement.us21.hana.ondemand.com/oauth2/token', 
                'data': {
                    'grant_type' : 'password', 
                    'username'   : (1, 'core-api-user'), 
                    'password'   : (1, 'core-api-password') } },
            'contract-set' : {'sub-url' : 'v1/lacovr/ContractSet'}, 
            'person-set'   : {'sub-url' : 'v15/bp/PersonSet'} } } }


CRM_KEYS = {
    'sandbox' : {
        'main' : {
            'url'  : 'https://bineo1633010523.zendesk.com/api',
            'username' : (1, 'crm-api-user'),   # ZNDK_USER_EMAIL
            'password' : (1, 'crm-api-token')},   # ZNDK_API_TOKEN
        'zis' : {
            'id'      : (1, 'crm-zis-id'), 
            'username': (1, 'crm-zis-user'), 
            'password': (1, 'crm-zis-pass')}, 
        'calls' : {
            'promises' : {
                'sub-url' : 'sunshine/objects/records'}, 
            'filters'  : ('sunshine/objects/records',
                    'services/zis/inbound_webhooks/generic/ingest') } } }


DBKS_TABLAS = {  # NOMBRE_DBKS, COLUMNA_EXCEL
    'contracts'   : 'bronze.loan_contracts', 
    'collections' : 'gold.loan_contracts'}


    

class ConfigEnviron():
    '''
    This class sets up the initial authentication object.  It reads its 
    ENV_TYPE (or stage) [dev,qas,prod] and SERVER(_TYPE) (local,dbks,wap). 
    And from then establishes its first secret-getter in order to later 
    establish its identity wether by a managed identity or service principal.  
    From then on, use PlatformResourcer to access other resources. 
    '''
    def __init__(self, env_type, server, spark=None):
        self.env = env_type
        self.spark = spark
        self.config = SETUP_KEYS[env_type]
        self.server = server
        self.set_secret_getter()
        self.set_credential()

    def set_secret_getter(self): 
        if  self.server == 'local':
            if load_dotenv is None: 
                raise("Failed to load library DOTENV.")
            load_dotenv('.env', override=True)        
            def get_secret(key): 
                return os.getenv(key)

        elif self.server == 'dbks': 
            if self.spark is None: 
                raise("Please provide a spark context on ConfigEnviron init.")
            dbutils = DBUtils(self.spark)
            
            def get_secret(a_key): 
                mod_key = re.sub('_', '-', a_key.lower())
                the_val = dbutils.secrets.get(scope=self.config['dbks']['scope'], key=mod_key)
                return the_val
        self.get_secret = get_secret

    def call_dict(self, a_dict): 
        def map_val(a_val): 
            is_tuple = isinstance(a_val, tuple)
            return self.get_secret(a_val[1]) if is_tuple else a_val

        return {k: map_val(v) for (k, v) in a_dict.items()}
    
    
    def set_credential(self):
        if self.get_secret is None: 
            self.set_secret_getter()
            
        if self.server in ['local', 'dbks']: 
            principal_keys = self.call_dict(self.config['service-principal'])
            the_creds = ClientSecretCredential(**principal_keys)
        elif self.server in ['wap']: 
            the_creds = DefaultAzureCredential()
        self.credential = the_creds

        
        
