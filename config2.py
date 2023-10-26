"""Infrastructure variables to be defined in the Azure & Databricks environment."""
from datetime import date
from os import environ

from epic_py.delta import TypeHandler
from epic_py.platform import EpicIdentity
from epic_py.tools import dict_plus

# pylint: disable=line-too-long
PAGE_MAX = 1000

SETUP = {
    'dev': {
        'service-principal' : {
            'client_id'       : 'sp-lakehylia-app-id',
            'client_secret'   : 'sp-lakehylia-secret',
            'tenant_id'       : 'aad-tenant-id',
            'subscription_id' : 'aad-subscription-id'} ,
        'databricks-scope' : 'kv-resource-access-dbks'},
    'qas' : {
        'service-principal' : {
            'client_id'       : 'sp-core-events-client',
            'client_secret'   : 'sp-core-events-secret',
            'tenant_id'       : 'aad-tenant-id',
            'subscription_id' : 'sp-core-events-subscription' } ,
        'databricks-scope': 'eh-core-banking'},
    'stg' : {
        'service-principal' : {
            'client_id'       : 'sp-collections-client',
            'client_secret'   : 'sp-collections-secret',
            'tenant_id'       : 'aad-tenant-id',
            'subscription_id' : 'sp-collections-subscription' } ,
        'databricks-scope': 'cx-collections'},
    'prd' : {
        'service-principal' : {
            'client_id'       : 'sp-collections-client',
            'client_secret'   : 'sp-collections-secret',
            'tenant_id'       : 'aad-tenant-id',
            'subscription_id' : 'sp-collections-subscription' } ,
        'databricks-scope': 'cx-collections'} }

PLATFORM = {
    'dev': {
        'key-vault' : 'kv-collections-data-dev',
        'storage'   : 'lakehylia'},
    'qas': {
        'key-vault' : 'kv-cx-data-qas',
        'storage'   : 'stlakehyliaqas'},
    'stg': {
        'key-vault' : 'kv-cx-adm-stg',
        'storage'   : 'stlakehyliastg'},
    'prd': {
        'key-vault' : 'kv-cx-data-prd',
        'storage'   : 'stlakehyliaprd'} }


CORE = {
    'dev-sap': {
        'base_url': "https://sbx-latp-apim.prod.apimanagement.us20.hana.ondemand.com/s4b", 
        'auth_url': "https://latp-apim.prod.apimanagement.us20.hana.ondemand.com/oauth2/token", 
        'client_id'     : 'core-api-key',
        'client_secret' : 'core-api-secret',
        'sap_username'  : 'core-api-user', 
        'sap_password'  : 'core-api-password'}, 
    'qas-sap': {
        'base_url': "https://apiqas.apimanagement.us21.hana.ondemand.com/s4b", 
        'auth_url': "https://apiqas.apimanagement.us21.hana.ondemand.com/oauth2/token", 
        'client_id'     : 'core-api-key',
        'client_secret' : 'core-api-secret',
        'sap_username'  : 'core-api-user', 
        'sap_password'  : 'core-api-password'}, 
    'prd-sap': {
        'base_url': "https://apiprd.apimanagement.us21.hana.ondemand.com/s4b", 
        'auth_url': "https://apiprd.apimanagement.us21.hana.ondemand.com/oauth2/token", 
        'client_id'     : 'core-api-key',
        'client_secret' : 'core-api-secret',
        'sap_username'  : 'core-api-user', 
        'sap_password'  : 'core-api-password'} }


CRM = {
    'sandbox-zd': {
        'url': "https://bineo1633010523.zendesk.com/api",
        'main-user': 'crm-api-user', 
        'main-token': 'crm-api-token',
        'zis-id': 'crm-zis-id', 
        'zis-user': 'crm-zis-user', 
        'zis-pass': 'crm-zis-pass'},
    'prod-zd': {
        'url': "https://bineo.zendesk.com/api",
        'main-user': 'crm-api-user', 
        'main-token': 'crm-api-token',
        'zis-id': 'crm-zis-id', 
        'zis-user': 'crm-zis-user', 
        'zis-pass': 'crm-zis-pass'} }

DATA = {
    'paths': {
        'core': 'ops/core-banking/batch-updates',
        'events': 'ops/core-banking',
        'collections': 'cx/collections/sunshine-objects'},
    'tables': {
        'brz_persons'       :('din_clients.brz_ops_persons_set',    'persons-set'),
        'brz_loan_payments' :('nayru_accounts.brz_ops_loan_payments',   'loan-payments'),
        'brz_loans'     :('nayru_accounts.brz_ops_loan_contracts',      'loan-contracts'),
        'brz_loan_balances' :('nayru_accounts.brz_ops_loan_balances',   'loan-balances'),
        'brz_loan_open_items'   :('nayru_accounts.brz_ops_loan_open_items', 'loan-open-items'),
        'brz_promises'      :('farore_transactions.brz_cx_payment_promises',    'promises'),
        'slv_loans'         :('nayru_accounts.slv_ops_loan_contracts',  'loan-contracts'),
        'slv_loan_balances' :('nayru_accounts.slv_ops_loan_balances',   'loan-balances'),
        'slv_loan_open_items'   :('nayru_accounts.slv_ops_loan_open_items', 'loan-open-items'),
        'slv_loan_payments' :('nayru_accounts.slv_ops_loan_payments',   'loan-payments'),
        'slv_persons'   :('din_clients.slv_ops_persons_set',    'persons-set'),
        'slv_promises'  :('farore_transactions.slv_cx_payment_promises',    'promises'),
        'gld_loans'     :('nayru_accounts.gld_ops_loan_contracts',  'loan-contracts')} }


## Technical objects used accross the project.

ENV = dict_plus(environ).get_plus('ENV_TYPE', 'ENV', 'nan-env')
SERVER   = environ.get('SERVER_TYPE', 'wap')
CORE_ENV = environ.get('CORE_ENV')
CRM_ENV  = environ.get('CRM_ENV')

app_agent = EpicIdentity.create(SERVER, SETUP[ENV])
app_resourcer = app_agent.get_resourcer(PLATFORM[ENV], check_all=False)
prep_secret = app_resourcer.get_vault_secretter()
prep_core = app_agent.prep_sap_connect(CORE[CORE_ENV], prep_secret)

cyber_handler = TypeHandler({
    'date': dict(c_format='%8.8d', NA=date(1900, 1, 1), date_format='MMddyyyy'),
    'dbl' : dict(c_format='%0{}.{}f', NA=0, no_decimal=True),
    'int' : dict(c_format='%0{}d', NA=0), 
    'long': dict(c_format='%0{}d', NA=0), 
    'str' : dict(c_format='%-{}s', NA=0), })

cyber_rename = {
    'nombre'    : 'name', 
    'PyType'    : 'pytype',
    'Longitud'  : 'len', 
    'Posición inicial' : 'pos'}
