# Diego Villamil, EPIC
# CDMX, 4 de noviembre de 2021

from datetime import datetime as dt
import re
from urllib.parse import unquote
import pandas as pd
from requests import Session, auth

#https://medium.com/analytics-vidhya/asyncio-threading-and-multiprocessing-in-python-4f5ff6ca75e8
from aiohttp import ClientSession
import asyncio

from src.utilities import tools
from src.platform_resources import AzureResourcer
from config import ConfigEnviron, SITE, CORE_KEYS, PAGE_MAX


class SAPSessionAsync(ClientSession): 
    def __init__(self, env, secret_env: AzureResourcer): 
        super().__init__()
        self.config     = CORE_KEYS[env]
        self.get_secret = secret_env.get_secret
        self.call_dict  = secret_env.call_dict
        self.set_main()
        self.set_token()

    def set_main(self): 
        main_config = self.config['main']
        self.headers.update(main_config['headers'])
        self.base_url = main_config['base-url']
        

    async def set_token(self, auth_type=None): 
        the_access = self.call_dict(self.config['main']['access'])
        params = self.config['calls']['auth'].copy()
        params['data'] = self.call_dict(params['data'])
        if auth_type == 'basic':
            the_auth = auth.HTTPBasicAuth(**the_access)
            params.update({'auth': the_auth})
        else: 
            auth_enc = tools.encode64("{username}:{password}".format(**the_access))
            params.update({'headers': {
                'Authorization': f'Basic {auth_enc}', 
                'Content-Type' : 'application/x-www-form-urlencoded'}})
        async with self.post(**params) as the_resp: 
            self.token = await the_resp.json()
        

    async def get_loans(self, attrs_indicator, tries=3): 
        if not hasattr(self, 'token'): 
            await self.set_token()
        
        loan_config  = self.config['calls']['contract-set']
        select_attrs = attributes_from_column(attrs_indicator)
        loan_params  = {'$select': ','.join(select_attrs)}

        async for _ in range(tries): 
            async with self.get(f"{self.base_url}/{loan_config['sub-url']}", 
                        auth=tools.BearerAuth(self.token['access_token']), 
                        params=loan_params) as the_resp: 
                if the_resp.status_code == 401: 
                    await self.set_token()
                    continue
                elif the_resp.status_code == 200: 
                    break
        else: 
            return None   

        loans_ls = the_resp.json()['d']['results']  # [metadata : [id, uri, type], borrowerName]
        for loan in loans_ls: 
            loan.pop('__metadata')
        
        return pd.DataFrame(loans_ls)


    async def get_persons(self, params={}, how_many=PAGE_MAX, tries=3): 
        person_conf = self.config['calls']['person-set']
        params_0 = {'$top': how_many, '$skip': 0}
        params_0.update(params)

        post_persons = []

        while True:
            async for _ in range(tries): 
                async with self.get(f"{self.base_url}/{person_conf['sub-url']}", 
                            auth=tools.BearerAuth(self.token['access_token']), 
                            params=params_0) as the_resp:     
                    if the_resp.status_code == 200: 
                        break
                    if the_resp.status_code == 401: 
                        self.set_token()
                        continue
            else:
                raise 'Could not call API.'
            
            persons_ls = the_resp.json()['d']['results']  # [metadata : [id, uri, type], borrowerName]
            post_persons.extend(persons_ls)

            params_0['$skip'] += how_many
            if len(persons_ls) < how_many: 
                break
        
        rm_keys = ['__metadata', 'Roles', 'TaxNumbers', 'Relation', 'Partner', 'Correspondence']
        persons_mod = [tools.dict_minus(a_person, rm_keys) for a_person in post_persons]
        persons_df = (pd.DataFrame(persons_mod)
            .assign(ID = lambda df: df.ID.str.pad(10, 'left', '0')))
        return persons_df


    async def get_by_api(self, api_type, type_id=None, tries=3): 
        type_ref = 'ContractSet' if type_id is None else f"ContractSet('{type_id}')"
        sub_url = tools.str_snake_to_camel(api_type, first_word_too=True)
        the_url = f'{self.base_url}/v1/lacovr/{type_ref}/{sub_url}'

        if not hasattr(self, 'token'): 
            self.set_token()

        async for _ in range(tries): 
            async with self.get(the_url, 
                        auth=tools.BearerAuth(self.token['access_token'])
                        ) as the_resp: 
                if the_resp.status_code == 200: 
                    break
                await self.set_token()
        else: 
            return None
        
        results_ls = the_resp.json()['d']['results']
        for each_result in results_ls: 
            each_result.pop('__metadata')
        
        results_df = (pd.DataFrame(results_ls).
            assign(ts_call = dt.now().strftime('%Y-%m-%d')))
        return results_df


class SAPSession(Session): 
    def __init__(self, env, secret_env: AzureResourcer): 
        super().__init__()
        self.config     = CORE_KEYS[env]
        self.get_secret = secret_env.get_secret
        self.call_dict  = secret_env.call_dict
        self.set_main()
        self.set_token()


    def set_main(self): 
        main_config = self.config['main']
        self.headers.update(main_config['headers'])
        self.base_url = main_config['base-url']
        

    def set_token(self, auth_type=None): 
        the_access = self.call_dict(self.config['main']['access'])
        params = self.config['calls']['auth'].copy()
        params['data'] = self.call_dict(params['data'])
        if auth_type == 'basic':
            the_auth = auth.HTTPBasicAuth(**the_access)
            params.update({'auth': the_auth})
        else: 
            auth_enc = tools.encode64("{username}:{password}".format(**the_access))
            params.update({'headers': {
                'Authorization': f'Basic {auth_enc}', 
                'Content-Type' : 'application/x-www-form-urlencoded'}})
        the_resp = self.post(**params)
        self.token = the_resp.json()


    def get_loans(self, attrs_indicator, tries=3): 
        if not hasattr(self, 'token'): 
            self.set_token()
        
        loan_config  = self.config['calls']['contract-set']
        select_attrs = attributes_from_column(attrs_indicator)
        loan_params  = {'$select': ','.join(select_attrs)}

        for _ in range(tries): 
            the_resp = self.get(f"{self.base_url}/{loan_config['sub-url']}", 
                auth=tools.BearerAuth(self.token['access_token']), 
                params=loan_params)
            
            if the_resp.status_code == 401: 
                self.set_token()
                continue
            elif the_resp.status_code == 200: 
                break
        else: 
            return None   

        loans_ls = the_resp.json()['d']['results']  # [metadata : [id, uri, type], borrowerName]
        for loan in loans_ls: 
            loan.pop('__metadata')
        
        return pd.DataFrame(loans_ls)


    def get_persons(self, params={}, how_many=PAGE_MAX, tries=3): 
        person_conf = self.config['calls']['person-set']
        params_0 = {'$top': how_many, '$skip': 0}
        params_0.update(params)

        post_persons = []

        while True:
            for _ in range(tries): 
                the_resp = self.get(f"{self.base_url}/{person_conf['sub-url']}", 
                        auth=tools.BearerAuth(self.token['access_token']), 
                        params=params_0)
                    
                if the_resp.status_code == 200: 
                    break
                if the_resp.status_code == 401: 
                    self.set_token()
                    continue
            else:
                raise 'Could not call API.'
            
            persons_ls = the_resp.json()['d']['results']  # [metadata : [id, uri, type], borrowerName]
            post_persons.extend(persons_ls)

            params_0['$skip'] += how_many
            if len(persons_ls) < how_many: 
                break
        
        rm_keys = ['__metadata', 'Roles', 'TaxNumbers', 'Relation', 'Partner', 'Correspondence']
        persons_mod = [tools.dict_minus(a_person, rm_keys) for a_person in post_persons]
        persons_df = (pd.DataFrame(persons_mod)
            .assign(ID = lambda df: df.ID.str.pad(10, 'left', '0')))
        return persons_df


    def get_by_api(self, api_type, type_id=None, tries=3): 
        type_ref = 'ContractSet' if type_id is None else f"ContractSet('{type_id}')"
        sub_url = tools.str_snake_to_camel(api_type, first_word_too=True)
        the_url = f'{self.base_url}/v1/lacovr/{type_ref}/{sub_url}'

        if not hasattr(self, 'token'): 
            self.set_token()

        for _ in range(tries): 
            the_resp = self.get(the_url, auth=tools.BearerAuth(self.token['access_token']))
            if the_resp.status_code == 200: 
                break
            self.set_token()
        else: 
            return None
        
        results_ls = the_resp.json()['d']['results']
        for each_result in results_ls: 
            each_result.pop('__metadata')
        
        results_df = (pd.DataFrame(results_ls).
            assign(ts_call = dt.now().strftime('%Y-%m-%d')))
        return results_df



def attributes_from_column(attrs_indicator=None) -> list:
    if attrs_indicator == 'all': 
        the_attrs = ['ID', 'BankAccountID', 'BankRoutingID', 'BankCountryCode', 
          'BorrowerID', 'BorrowerTxt', 'BorrowerName', 'BorrowerAddress', 'ManagerID', 
          'ManagerTxt', 'BankPostingArea', 'BankPostingAreaTxt', 'ProductID', 'ProductTxt', 
          'Purpose', 'ClabeAccount', 'Currency', 'InitialLoanAmount', 'EffectiveCapital', 
          'StartDate', 'LifeCycleStatus', 'LifeCycleStatusTxt', 'CreationDateTime', 
          'CreationUserID', 'LastChangeDateTime', 'LastChangeUserID', 'NominalInterestRate', 
          'RepaymentFrequency', 'RepaymentAmount', 'RepaymentPercentage', 'LimitTotalAmount', 
          'DirectDebitBankAccountHolder', 'DirectDebitBankAccount', 'DirectDebitBankAccountID', 
          'DirectDebitBankRoutingID', 'DirectDebitBankCountryCode', 'TermSpecificationStartDate', 
          'TermSpecificationEndDate', 'TermSpecificationValidityPeriodDuration', 
          'TermSpecificationValidityPeriodDurationM', 'TermAgreementCommittedCapitalAmount', 
          'TermAgreementFixingPeriodStartDate', 'TermAgreementFixingPeriodEndDate', 
          'PaymentPlanStartDate', 'PaymentPlanEndDate', 'PaymentPlanRemainingDebtAmount', 
          'EffectiveYieldPercentage', 'EffectiveYieldCalculationReason', 
          'EffectiveYieldCalculationReasonTxt', 'EffectiveYieldCalculationMethod', 
          'EffectiveYieldCalculationMethodTxt', 'EffectiveYieldValidityStartDate', 
          'EffectiveYieldCalculationPeriodStartDate', 'EffectiveYieldCalculationPeriodEndDate', 
          'CurrentPostingDate', 'CurrentOpenItemsAmount', 'OutstandingBalance', 'AccountLocked', 
          'MasterContractNo', 'MasterContractID', 'CAT', 'PortfolioType', 'PortfolioTypeTxt', 
          'StageLevel', 'StageLevelTxt', 'OverdueDays', 'PendingPayments', 'EvaluationDate', 
          'RolloverAccount', 'ObservationKey', 'ObservationKeyTxt', 'PaymentForm', 
          'PaymentFormTxt', 'EventID']
        return the_attrs

    sap_attr = pd.read_feather(SITE/'refs/catalogs/sap_attributes.feather')
    possible_columns = list(sap_attr.columns) + ['all']

    # en_cx, ejemplo, default, postman_default. 
    if attrs_indicator is None: 
        attrs_indicator = 'postman_default'
    
    if attrs_indicator not in possible_columns: 
        raise("COLUMN INDICATOR must be one in SAP_ATTR or 'all'.")

    if attrs_indicator == 'all':
        attr_df = sap_attr
    else: 
        attr_df = sap_attr.query(f'{attrs_indicator} == 1')
    
    return attr_df['atributo'].to_list()


def d_results(json_item, api_type): 
    api_fields = {
        'loans'         : [ 'BorrowerName'], 
        'payment_plan'  : [ 'ContractID', 'ItemID', 'Date', 'Category', 
                            'CategoryTxt', 'Amount', 'Currency', 'RemainingDebitAmount'], 
        'balances'      : [ 'ID', 'Code', 'Name', 'Amount', 'Currency'], 
        'open_items'    : [ ''] }

    if api_type == 'loans':
        result_0 = json_item['__metadata']      # Contains:  id, uri, type
        re_match = re.search(r"ContractSet\('(.*)'\)", result_0['uri'])
        result_0['ContractID'] = unquote(re_match.groups()[0])
    
    elif api_type == 'payment_plan': 
        pass

    elif api_type == 'balances': 
        for res_dict in json_item: 
            res_dict.pop('__metadata')
        result_0 = pd.DataFrame(json_item)

    else:  # 'balances', 'open_items'
        result_0 = {}        

    return result_0


    
if False: 
    from importlib import reload
    from src import core_banking
    from src import platform_resources
    from src.utilities import tools
    import config
    reload(core_banking)
    reload(platform_resources)
    reload(config)
    reload(tools)

    from time import time
    from itertools import product
    from src.platform_resources import AzureResourcer
    from config import ConfigEnviron

    secretter = ConfigEnviron('local')
    azure_getter = AzureResourcer('local', secretter)
    # core_session = SAPSession('qas', azure_getter)

    from concurrent.futures import ThreadPoolExecutor
    import threading

    thread_local = threading.local()

    def get_session():
        if not hasattr(thread_local, 'session'): 
            thread_local.session = SAPSession('qas', azure_getter)
        return thread_local.session


    def call_an_api(in_params): 
        global the_calls, unserved
        a_session = get_session()

        api_type, type_id = in_params
        api_df = a_session.get_by_api(api_type, type_id)
        if api_df is not None: 
            the_calls[api_type][type_id] = api_df
        else: 
            unserved.append((api_type, type_id))

    def call_all_apis(call_types, ids_lists, k_workers=20): 
        global the_calls, unserved
        the_calls = {a_type: {} for a_type in call_types}
        unserved = []
        with ThreadPoolExecutor(max_workers=k_workers) as executor: 
            executor.map(call_an_api, product(api_types, ids_lists))        
        return (the_calls, unserved)


    ids = ['10000002999-111-MX', '10000003019-111-MX', '10000003021-111-MX', 
        '10000003053-111-MX', '10000003080-111-MX', '10000003118-111-MX', 
        '10000003136-111-MX', '10000003140-111-MX', '10000003188-111-MX', 
        '10000003226-555-MX']
    
    api_types = ['open_items', 'payment_plan', 'balances']
    
    tic = time()
    (calls, unserved) = call_all_apis(api_types, ids)
    toc = time() - tic

    print(f'Total: {len(ids)*len(api_types)}, Missed: {len(unserved)} in {toc:5.2} seconds')