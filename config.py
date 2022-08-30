from os import environ, getcwd, getenv
import re
from pathlib import Path
from azure.identity import ClientSecretCredential
from azure.identity._credentials.default import DefaultAzureCredential
try: 
    from pyspark.dbutils import DBUtils
except ImportError: 
    DBUtils = None
try: 
    from dotenv import load_dotenv
    load_dotenv(override=True)
except ImportError:
    load_dotenv = None

SITE = Path(__file__).parent if '__file__' in globals() else Path(getcwd())
ENV = environ.get('ENV_TYPE', environ.get('ENV', 'qas'))        # dev, qas, stg, prod
SERVER = environ.get('SERVER_TYPE', 'wap')  # dbks, local, wap. 


PAGE_MAX = 1000

SETUP_KEYS = {
    'dev' : {
        'service-principal' : {
            'client_id'       : (1, 'sp-lakehylia-app-id'), 
            'client_secret'   : (1, 'sp-lakehylia-secret'), 
            'tenant_id'       : (1, 'aad-tenant-id'), 
            'subscription_id' : (1, 'aad-subscription-id') } , 
        'dbks': {'scope': 'kv-resource-access-dbks'} }, 
    'qas' : {
        'service-principal' : {
            'client_id'       : (1, 'sp-core-events-client'), 
            'client_secret'   : (1, 'sp-core-events-secret'), 
            'tenant_id'       : (1, 'aad-tenant-id'), 
            'subscription_id' : (1, 'sp-core-events-suscription') } , 
        'dbks': {'scope': 'eh-core-banking'}
    }
}


PLATFORM_KEYS = {
    'dev': {        
        'key-vault' : {
            'name'  : 'kv-collections-data-dev', 
            'url'   : "https://kv-collections-data-dev.vault.azure.net/"}, 
        'storage'   : {
            'name'  : 'lakehylia', 
            'url'   : 'https://lakehylia.blob.core.windows.net/'}, 
        'app-id'    : 'cx-collections-id'}, 
    'qas': {        
        'key-vault' : {
            'name'  : 'kv-collections-data-dev', 
            'url'   : "https://kv-collections-data-dev.vault.azure.net/"}, 
        'storage'   : {
            'name'  : 'lakehylia', 
            'url'   : 'https://lakehylia.blob.core.windows.net/'}, 
        'app-id'    : 'cx-collections-id'} }


CORE_KEYS = {
    'default': {
        'main': {
            'headers' : {
                'format'          : 'json',
                'Accept-Encoding' : "gzip, deflate",
                'Accept'          : "application/json"} }, 
        'calls': {
            'event-set'    : {
                'persons'      : "v15/bp/EventSet", 
                'accounts'     : "v1/cac/EventSet", 
                'transactions' : "v1/bape/EventSet", 
                'prenotes'    : "v1/bapre/EventSet"}, 
            'person-set'   : {'sub-url' : "v15/bp/PersonSet"},
            'contract-set' : {'sub-url' : "v1/lacovr/ContractSet"},
            'contract-qan' : {'sub-url' : "v1/lacqan/ContractSet"} } }, 
    'dev-sap': {
        'main' : { 
            'base-url' : "https://sbx-latp-apim.prod.apimanagement.us20.hana.ondemand.com/s4b",
            'access' : {
                'username': (1, 'core-api-key'), 
                'password': (1, 'core-api-secret') }, 
            'headers' : {
                'format'          : 'json',
                'Accept-Encoding' : "gzip, deflate",
                'Accept'          : "application/json"} }, 
        'calls' : {
            'auth' : {
                'url' : "https://latp-apim.prod.apimanagement.us20.hana.ondemand.com/oauth2/token", 
                'data': {
                    'grant_type' : 'password', 
                    'username'   : (1, 'core-api-user'), 
                    'password'   : (1, 'core-api-password') } },
            'event-set'    : {
                'persons'      : "v15/bp/EventSet", 
                'accounts'     : "v1/cac/EventSet", 
                'transactions' : "v1/bape/EventSet", 
                'prenotes'     : "v1/bapre/EventSet"}, 
            'person-set'   : {'sub-url' : "v15/bp/PersonSet"},
            'contract-set' : {'sub-url' : "v1/lacovr/ContractSet"},
            'contract-qan' : {'sub-url' : "v1/lacqan/ContractSet"} } }, 
    'qas-sap' : {
        'main' : {
            'headers' : {
                'format'          : 'json',
                'Accept-Encoding' : "gzip, deflate",
                'Accept'          : "application/json" }, 
            'base-url' : "https://apiqas.apimanagement.us21.hana.ondemand.com/s4b",
            'access' : {
                'username': (1, 'core-api-key'), 
                'password': (1, 'core-api-secret') } }, 
        'calls' : {
            'auth' : {
                'url' : "https://apiqas.apimanagement.us21.hana.ondemand.com/oauth2/token", 
                'data': {
                    'grant_type' : 'password', 
                    'username'   : (1, 'core-api-user'), 
                    'password'   : (1, 'core-api-password') } },
            'event-set'    : {
                'persons'      : "v15/bp/EventSet", 
                'accounts'     : "v1/cac/EventSet", 
                'transactions' : "v1/bape/EventSet", 
                'prenotes'     : "v1/bapre/EventSet"},
            'contract-set' : {'sub-url' : "v1/lacovr/ContractSet"},
            'contract-qan' : {'sub-url' : "v1/lacqan/ContractSet"}, 
            'person-set'   : {'sub-url' : "v15/bp/PersonSet"} } } }


CRM_KEYS = {
    'sandbox' : {
        'main' : {
            'url'  : "https://bineo1633010523.zendesk.com/api",
            'username' : (1, 'crm-api-user'),   # ZNDK_USER_EMAIL
            'password' : (1, 'crm-api-token')},   # ZNDK_API_TOKEN
        'zis' : {     # Zendesk Integration Services. 
            'id'      : (1, 'crm-zis-id'), 
            'username': (1, 'crm-zis-user'), 
            'password': (1, 'crm-zis-pass')}, 
        'calls' : {
            'promises' : {
                'sub-url' : "sunshine/objects/records"}, 
            'filters'  : ("sunshine/objects/records",
                    "services/zis/inbound_webhooks/generic/ingest") } },
     'prod' : {
        'main' : {
            'url'  : "https://bineo.zendesk.com/api",
            'username' : (1, 'crm-api-user'),   # ZNDK_USER_EMAIL
            'password' : (1, 'crm-api-token')},   # ZNDK_API_TOKEN
        'zis' : {
            'id'      : (1, 'crm-zis-id'), 
            'username': (1, 'crm-zis-user'), 
            'password': (1, 'crm-zis-pass')}, 
        'calls' : {
            'promises' : {
                'sub-url' : "sunshine/objects/records"}, 
            'filters'  : ("sunshine/objects/records",
                    "services/zis/inbound_webhooks/generic/ingest") } }}

DBKS_KEYS = {
    'dev': {
        'connect': {
            'local': {'DSN' : (1, 'DBKS_ODBC_DSN')},
            'wap': {
                'Driver'         : "/opt/simba/spark/lib/64/libsparkodbc_sb64.so",
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
            'contracts'   : "bronze.loan_contracts", 
            'collections' : "gold.loan_contracts"} },
    'qas': { 
    }  
} 
                                    
DBKS_TABLES = {          
    'dev': {
        'base' : 'abfss://{stage}@lakehylia.dfs.core.windows.net/ops/core-banking/batch-updates',
        'items': {  # table, location, old
            'brz_persons'         : 
                ('din_clients.brz_ops_persons_set',        'persons-set',    'bronze.persons_set'),
            'brz_loans'           : 
                ('nayru_accounts.brz_ops_loan_contracts',  'loan-contracts', 'bronze.loan_contracts'), 
            'brz_loan_balances'   : 
                ('nayru_accounts.brz_ops_loan_balances',   'loan-balances',  'bronze.loan_balances'), 
            'brz_loan_open_items' : 
                ('nayru_accounts.brz_ops_loan_open_items', 'loan-open-items', 'bronze.loan_open_items'),  
            'brz_loan_payments'   : 
                ('nayru_accounts.brz_ops_loan_payments',   'loan-payments',   'bronze.loan_payments'),
            'slv_persons'         : 
                ('din_clients.slv_ops_persons_set',        'persons-set',    'silver.persons_set'), 
            'slv_loan_balances'   : 
                ('nayru_accounts.slv_ops_loan_balances',   'loan-balances',  'silver.loan_balances'), 
            'slv_loan_open_items' : 
                ('nayru_accounts.slv_ops_loan_open_items', 'loan-open-items', 'silver.loan_open_items'),  
            'slv_loan_payments'   : 
                ('nayru_accounts.slv_ops_loan_payments',   'loan-payments',  'silver.loan_payments'),
            'slv_promises'        : 
                ('farore_transactions.slv_cx_payment_promises', '', 'silver.zendesk_promises'), 
            'gld_loans'           : 
                ('nayru_accounts.gld_ops_loan_contracts',  'loan-contracts', 'gold.loan_contracts')},
        'names' : {
            'brz_persons'         : 'bronze.persons_set', 
            'brz_loans'           : 'bronze.loan_contracts',        
            'brz_loan_balances'   : 'bronze.loan_balances', 
            'brz_loan_open_items' : 'bronze.loan_open_items',  
            'brz_loan_payments'   : 'bronze.loan_payment_plans',
            'slv_persons'         : 'silver.persons_set',
            'slv_loan_payments'   : 'silver.loan_payment_plans',
            'slv_loan_balances'   : 'silver.loan_balances',
            'slv_loans'           : 'silver.loan_contracts',
            'slv_loan_open_items' : 'silver.loan_open_items',
            'slv_promises'        : 'silver.zendesk_promises', 
            'gld_loans'           : 'gold.loan_contracts'} }, 
    'qas': {
        'brz_persons' : {
            'name'  : "din_clients.brz_ops_persons_set", 
            'location' : "ops/core-banking/batch-updates/persons-set"},
        'brz_loans' : {
            'name'  : "nayru_accounts.brz_ops_loan_contracts", 
            'location' : "ops/core-banking/batch-updates/loan-contracts"}
} }


class ConfigEnviron():
    '''
    This class sets up the initial authentication object.  It reads its 
    ENV_TYPE or cycle [dev,qas,prod] and SERVER(_TYPE) (local,dbks,wap). 
    And from then establishes its first secret-getter in order to later 
    establish its identity wether by a managed identity or service principal.  
    From then on, use PlatformResourcer to access other resources. 
    '''
    def __init__(self, env_type, server, spark=None):
        self.env = env_type
        self.spark = spark
        self.server = server
        self.config = SETUP_KEYS[env_type]
        self.set_secret_getter()
        self.set_credential()

    def set_secret_getter(self): 
        if  self.server == 'local':
            if load_dotenv is None: 
                raise Exception("Failed to load library DOTENV.")
            load_dotenv('.env', override=True)        
            def get_secret(key): 
                mod_key = re.sub('-', '_', key.upper())
                return getenv(mod_key)

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
        if not hasattr(self, 'get_secret'): 
            self.set_secret_getter()

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
