import os
import re
from pathlib import Path
from dotenv import load_dotenv

SITE = Path(__file__).parent if '__file__' in globals() else Path(os.getcwd())
DESTINATION = SITE.parent/'collections-webapp'

ENV = os.environ.get('ENV', 'local')  # local, dbks, dev, qas.

SETUP_KEYS = {
    'dev' : {
        'service-principal': {
            'subscription-id': '',
            'application-id': '',
            'client-id': '', 
            'client-secret': ''}, 
        'keyvault': {'url': ''}, 
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
            'subscription_id' : (1, 'AAD_SUBSCRIPTION_ID') }, 
        'sqls': {
            'hylia': {
                'driver': "ODBC Driver 18 for SQL Server", 
                'user': (1, 'SQL_CATALOGS_USER'), 
                'password': (1, 'SQL_CATALOGS_PASS'),
                'host': (1, 'SQL_CATALOGS_HOST'), 
                'database': (1, 'SQL_CATALOGS_DBASE')}
        } },
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
    'dev': {
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
    'qas' : {
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


DBKS_KEYS = {
    'dev': {
        'connect': {
            'local': {'DSN' : (1, 'DBKS_ODBC_DSN')},
            'wap': {
                'Driver'         : '/opt/simba/spark/lib/64/libsparkodbc_sb64.so',
                'PORT'           : '443',
                'Schema'         : 'default',
                'SparkServerType': '3',
                'AuthMech'       : '3',
                'UID'            : 'token',
                'ThriftTransport': '2',
                'SSL'            : '1', 
                'PWD'            : (1, 'dbks-wap-token'),
                'HOST'           : (1, 'dbks-odbc-host'),
                'HTTPPath'       : (1, 'dbks-odbc-http')} }, 
        'tables' : {  # NOMBRE_DBKS, COLUMNA_EXCEL
            'contracts'   : 'bronze.loan_contracts', 
            'collections' : 'gold.loan_contracts'} },
    'qas': {
    } 
} 


PAGE_MAX = 1000

in_dbks = 'DATABRICKS_RUNTIME_VERSION' in os.environ



    

class VaultSetter():
    '''
    Depending on ENV_TYPE [local, dbks, dev, qas, prod], this class sets 
    methods to access first-tier secrets from the taylored-made dictionary. 
    Upon accessing first-tier secrets, a key vault is reached and other secrets
    can be accessed independently. 
    That is: 
    - local          -> os.getenv(*)
    - databricks     -> dbutils.secrets.get(a_scope, *)
    - dev, qas, prod -> not needed, as access to keyvault is granted by identity.
    '''
    def __init__(self, env_type, dbutils=None):
        self.env = env_type
        self.set_secret_getter()
        self.dbutils = dbutils


    def set_secret_getter(self): 
        if  self.env == 'local': 
            load_dotenv('.env', override=True)
            get_secret = lambda a_key: os.getenv(a_key) 
            
        elif self.env == 'dbks': 
            the_scope = 'kv-resource-access-dbks'
            get_secret = (lambda a_key: self.dbutils.secrets.get(
                    scope=the_scope, 
                    key=re.sub('_', '-', a_key.lower()) ))
                
        self.get_secret = get_secret


    def call_dict(self, a_dict): 
        if not hasattr(self, 'get_secret'): 
            self.set_secret_getter()

        def map_val(a_val): 
            is_tuple = isinstance(a_val, tuple)
            return self.get_secret(a_val[1]) if is_tuple else a_val

        return {k: map_val(v) for (k, v) in a_dict.items()}
        
