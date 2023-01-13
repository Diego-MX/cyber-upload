# Diego Villamil, EPIC
# CDMX, 4 de noviembre de 2021

from datetime import datetime as dt, timedelta as delta, date
from itertools import product
from json import dumps
import pandas as pd
import re
from urllib.parse import unquote

from typing import Union
from deepdiff import DeepDiff
from requests import Session, auth
from httpx import AsyncClient, Auth as AuthX

from src.utilities import tools
from src.platform_resources import AzureResourcer
from config import CORE_KEYS, PAGE_MAX

try: 
    from delta.tables import DeltaTable
except ImportError: 
    DeltaTable = None

    
def str_error(an_error): 
    try: 
        a_json = an_error.json()
        return dumps(a_json, indent=2)
    except Exception: 
        return str(an_error)

    
def date_2_pandas(sap_srs: pd.Series, mode='/Date') -> pd.Series:
    if mode == '/Date': 
        dt_regex  = r"/Date\(([0-9]*)\)/"
        epoch_srs = sap_srs.str.extract(dt_regex, expand=False)
        pd_date   = pd.to_datetime(epoch_srs, unit='ms')
    elif mode == 'yyyymmdd': 
        pd_date = pd.to_datetime(sap_srs, format='%Y%m%d')
    return pd_date


def datetime_2_filter(sap_dt, range_how=None) -> str: 
    dt_string = (lambda a_dt: 
            "datetime'{}'".format(a_dt.strftime('%Y-%m-%dT%H:%M:%S')))
    if range_how == 'functions': 
        dt_clause = lambda cmp_dt : "{}(EventDateTime,{})".format(*cmp_dt)
    else: 
        dt_clause = lambda cmp_dt : "EventDateTime {} {}".format(*cmp_dt)
        
    if isinstance(sap_dt, (dt, date)):
        sap_filter = dt_clause(['ge', dt_string(sap_dt)])
        return sap_filter
    
    two_cond = (isinstance(sap_dt, (list, tuple)) 
            and (len(sap_dt) == 2) 
            and (sap_dt[0] < sap_dt[1]))
    if not two_cond: 
        raise Exception("SAP_DT may be DATETIME or LIST[DT1, DT2] with DT1 < DT2")
    
    into_clauses = zip(['ge', 'lt'], map(dt_string, sap_dt))
    # ... = (['ge', dtime_string(sap_dt[0])], 
    #        ['lt', dtime_string(sap_dt[1])])
    map_clauses = map(dt_clause, into_clauses)
    if range_how in [None, 'and']: 
        sap_filter = ' and '.join(map_clauses)
    elif range_how == 'expand_list':  
        sap_filter = list(map_clauses)
    elif range_how == 'between':
        sap_filter = ("EventDateTime between datetime'{}' and datetime'{}'"
            .format(*map_clauses))
    elif range_how == 'functions': 
        sap_filter = 'and({},{})'.format(*map_clauses)
    return sap_filter
        

class BearerAuth2(auth.AuthBase, AuthX): 
    def __init__(self, token_str): 
        self.token = token_str 
    
    def auth_flow(self, a_request): 
        a_request.headers['Authorization'] = f"Bearer {self.token}"
        yield a_request
        
    def __call__(self, a_request):
        a_request.headers['Authorization'] = f"Bearer {self.token}"
        return a_request

    
class BearerAuth(auth.AuthBase):
    def __init__(self, token):
        self.token = token
    
    def __call__(self, a_request):
        a_request.headers['Authorization'] = f"Bearer {self.token}"
        return a_request


class BearerAuthX(AuthX): 
    def __init__(self, token_str): 
        self.token = token_str 
    def auth_flow(self, a_request): 
        a_request.headers['Authorization'] = f"Bearer {self.token}"
        yield a_request
        


class SAPSession(Session): 
    def __init__(self, env, secret_env: AzureResourcer): 
        super().__init__()
        self.config     = CORE_KEYS[env]
        self.get_secret = secret_env.get_secret
        self.call_dict  = secret_env.call_dict
        self.set_main()


    def set_main(self): 
        main_config = self.config['main']
        self.headers.update(main_config['headers'])
        self.base_url = main_config['base-url']
        self.set_token()


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
                'Authorization': f"Basic {auth_enc}", 
                'Content-Type' : "application/x-www-form-urlencoded"}})
        the_resp = self.post(**params)
        self.token = the_resp.json()


    def get_events(self, event_type=None, date_lim: dt=None, output=None, tries=3): 
        """
        EVENT_TYPE: [persons, accounts, transactions, prenotes]
        DATE_FROM:  (as datetime.datetime) for recent events
        OUTPUT:     [Response, DataFrame, WithMetadata]
        TRIES:      (not really useful, but included anyway)
        
        """
        
        if not hasattr(self, 'token'): 
            self.set_token()
        
        if date_lim is None: 
            date_lim = dt.now() - delta(days=1)
        
        if output is None: 
            output = 'DataFrame'
        
        event_config = self.config['calls']['event-set']

        data_from = {
            'persons'       : ['dataOld', 'dataNew'], 
            'accounts'      : ['dataOld', 'dataNew'], 
            'transactions'  : ['dataNew'], 
            'prenotes'      : ['dataNew'] }

        to_expand = {
            'persons'       : ['', '/Person', '/Person/Relation', '/Person/Correspondence'],
            'accounts'      : ['', '/Party'],
            'transactions'  : ['', '/PaymentNotes'], 
            'prenotes'      : ['', '/PaymentNotes'] }

        exp_params = [''.join(data_key)  
            for data_key in product(data_from[event_type], to_expand[event_type])]
        
        event_params = {
            '$filter' : datetime_2_filter(date_lim, 'and'), 
            '$expand' : ','.join(exp_params)}
        print(event_params['$filter'])
            
        for _ in range(tries): 
            the_resp = self.get(f"{self.base_url}/{event_config[event_type]}", 
                auth=BearerAuth(self.token['access_token']), 
                params=event_params)            
            if the_resp.status_code == 200: 
                break
            elif the_resp.status_code == 401: 
                self.set_token()
                continue
            else: 
                print(the_resp.text)
                return None
        else: 
            print(f'Number of tries ({tries}) reached.')
            return the_resp
        
        # From here, there is a response. 
        if output == 'Response':
            return the_resp
        
        events_ls = the_resp.json()['d']['results']
        
        if len(events_ls) == 0: 
            raise Exception("Response data is empty.")
            
        data_poppers = {
            'persons'      : {
                'root'   : ('__metadata', 'dataOld'), 
                'dataNew': ('__metadata', 'Correspondence', 'Relation', 
                            'Person', 'Organisation', 'Group', 'TaxNumbers')}, 
            'accounts'     : [], 
            'transactions' : {
                'root'   : ('__metadata', 'dataOld'),
                'dataNew': ('__metadata', 'PaymentNotes')}}
        
        
        
        data_ls = [an_event.pop('dataNew') for an_event in events_ls]
        for pop_field in data_poppers[event_type]['dataNew']: 
            to_discard = [a_data.pop(pop_field) for a_data in data_ls]
        
        date_cols = {
            'persons' : {
                '/Date': ['CreatedAtDateTime', 'LastModifyDateTime']},
            'transactions' : {
                'yyyymmdd': ['PostingDate', 'ValueDate']}
        }
        
        data_df = (pd.DataFrame(data_ls)
            .assign(**{dt_col: lambda df: date_2_pandas(df[dt_col], '/Date') 
                        for dt_col in date_cols[event_type].get('/Date'   , [])})
            .assign(**{dt_col: lambda df: date_2_pandas(df[dt_col], 'yyyymmdd') 
                        for dt_col in date_cols[event_type].get('yyyymmdd', [])}))
        
        if output == 'DataFrame': 
            return data_df
        
        if output == 'WithMetadata': 
            to_discard = [an_event.pop('__metadata') for an_event in events_ls]
            if event_type == 'persons': 
                pass
            if event_type == 'accounts': 
                pass
            if event_type == 'transactions': 
                to_discard = [an_event.pop('dataOld') for an_event in events_ls]
            if event_type == 'prenotes': 
                pass
            
            meta_df = (pd.DataFrame(events_ls)
                .assign(EventDateTime = lambda df: date_2_pandas(df['EventDateTime'])))
            return (data_df, meta_df)
        

    def get_loans(self, tries=3): 
        if not hasattr(self, 'token'): 
            self.set_token()
        
        loan_config  = self.config['calls']['contract-set']
        select_attrs = attributes_from_column('all')
        loan_params  = {'$select': ','.join(select_attrs)}
        
        for _ in range(tries): 
            the_resp = self.get(f"{self.base_url}/{loan_config['sub-url']}", 
                auth=BearerAuth(self.token['access_token']), 
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


    def get_loans_qan(self, tries=3): 
        if not hasattr(self, 'token'): 
            self.set_token()
        
        loan_config  = self.config['calls']['contract-qan']
        select_attrs = attributes_from_column('qan')
        loan_params  = {'$select': ','.join(select_attrs)}

        for _ in range(tries): 
            the_resp = self.get(f"{self.base_url}/{loan_config['sub-url']}", 
                auth=BearerAuth(self.token['access_token']), 
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

    
    def get_persons(self, params_x={}, how_many=PAGE_MAX, tries=3): 
        person_conf = self.config['calls']['person-set']
        params_0 = {'$top': how_many, '$skip': 0}
        params_0.update(params_x)

        post_persons = []

        while True:
            for _ in range(tries): 
                the_resp = self.get(f"{self.base_url}/{person_conf['sub-url']}", 
                        auth=BearerAuth(self.token['access_token']), 
                        params=params_0)
                    
                if the_resp.status_code == 200: 
                    break
                if the_resp.status_code == 401: 
                    self.set_token()
                    continue
            else:
                raise "Could not call API."
            
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



class SAPSessionAsync(AsyncClient):  
    def __init__(self, env, secret_env: AzureResourcer, **kwargs): 
        super().__init__(**kwargs)
        self.get_secret = secret_env.get_secret
        self.call_dict  = secret_env.call_dict
        self.config     = CORE_KEYS[env]
        self.set_main()
                
            
    def set_main(self): 
        main_config = self.config['main']
        self.base_url = main_config['base-url']
        # await self.set_token() no aplica por temas del AWAIT ASYNC  


    async def set_token(self): 
        # auth_params = self.config['calls']['auth']
        # auth_data = self.call_dict(auth_params['data'])
        # self.token = await self.post(auth_params['url'], **auth_data)
        
        the_access = self.call_dict(self.config['main']['access'])
        params = self.config['calls']['auth'].copy()
        params['data'] = self.call_dict(params['data'])
    
        auth_enc = tools.encode64("{username}:{password}".format(**the_access))
        params.update({
            'headers': {
            'Authorization': f"Basic {auth_enc}", 
            'Content-Type' : "application/x-www-form-urlencoded"}})
        the_resp = await self.post(**params)
        self.token = the_resp.json()


    async def get_by_api(self, api_type, type_id=None, tries=3): 
        type_ref = 'ContractSet' if type_id is None else f"ContractSet('{type_id}')"
        sub_url  = tools.str_snake_to_camel(api_type, first_word_too=True)
        the_url  = f"{self.base_url}/v1/lacovr/{type_ref}/{sub_url}"
        main_conf = self.config['main']
        the_hdrs = main_conf['headers']
        
        if not hasattr(self, 'token'): 
            await self.set_token()

        for _ in range(tries): 
            # the_hdrs['Authorization'] = f"Bearer {self.token['access_token']}"
            the_resp = await self.get(the_url, headers=the_hdrs, #) 
                    auth=BearerAuthX(self.token['access_token']))
            if the_resp.status_code == 200: 
                break
            await self.set_token()
        else: 
            return None
        
        results_ls = the_resp.json()['d']['results']
        for each_result in results_ls: 
            each_result.pop('__metadata')
        
        results_df = (pd.DataFrame(results_ls)
            .assign(ts_call = dt.now().strftime("%Y-%m-%d %H:%M:%S")))
        return results_df

    
def create_delta(spark, data_df: pd.DataFrame, df_location: str, tbl_name: str): 
    create_clause =  f"CREATE TABLE {tbl_name} USING DELTA LOCATION \"{df_location}\""
    data_spk = spark.createDataFrame(data_df)
    (data_spk.write
         .format('delta')
         .save(df_location))
    
    spark.sql(create_clause)
    
    
def update_dataframe(spark, new_df: pd.DataFrame, df_location: str, id_column: str, dev=False): 
    new_data = spark.createDataFrame(new_df)
    
    prev_data = DeltaTable.forPath(spark, df_location)
    tbl_cols = {a_col: f'new.{a_col}' for a_col in new_data.columns}
    
    if not dev: 
        (prev_data.alias('prev')
            .merge(source=new_data.alias('new'), condition=f"prev.{id_column} = new.{id_column}")
            .whenMatchedUpdate(set=tbl_cols)
            .whenNotMatchedInsert(values=tbl_cols)
            .execute())
    else: 
        tbl_cols = {a_col: F.coalesce(f'new.{a_col}', f'prev.{a_col}')
                for a_col in new_data.columns}
        tbl_values = {a_col: f'new.{a_col}' for a_col in new_data.columns}
        (prev_data.alias('prev')
            .merge(source=new_data.alias('new'), condition=f"prev.{id_column} = new.{id_column}")
            .whenMatchedUpdate(set=tbl_cols)
            .whenNotMatchedInsert(values=tbl_cols)
            .execute())
    return True
        
    

def attributes_from_column(attrs_indicator=None) -> list:
    if attrs_indicator == 'all': 
        # Estos son para LACOVR, pero en realidad no se necesitan. 
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
            'StageLevel', 'StageLevelTxt ', 'OverdueDays', 'PendingPayments', 'EvaluationDate', 
            'RolloverAccount', 'ObservationKey', 'ObservationKeyTxt', 'PaymentForm', 
            'PaymentFormTxt', 'EventID']

    elif attrs_indicator == 'qan': 
        the_attrs = ['GenerateId', 'LoanContractID', 'BankAccountID', 'BankRoutingID', 
            'BankCountryCode', 'BorrowerID', 'BorrowerTxt', 'BorrowerName', 
            'BorrowerCategory', 'BorrowerCategoryTxt', 'BorrowerCountry', 'BorrowerCountryTxt', 
            'BorrowerRegion', 'BorrowerRegionTxt', 'BorrowerCity', 'ManagerID', 
            'ManagerTxt', 'BankPostingArea', 'BankPostingAreaTxt', 'ProductID', 
            'ProductTxt', 'Currency', 'InitialLoanAmount', 'StartDate', 'LifeCycleStatus', 
            'TermSpecificationValidityPeriodDuration', 'TermAgreementCommittedCapitalAmount', 
            'TermAgreementFixingPeriodStartDate', 'TermAgreementFixingPeriodEndDate', 
            'PaymentPlanStartDate', 'PaymentPlanEndDate', 'EffectiveYieldPercentage', 
            'EffectiveYieldCalculationReason', 'EffectiveYieldCalculationReasonTxt', 
            'EffectiveYieldCalculationMethod', 'EffectiveYieldCalculationMethodTxt', 
            'EffectiveYieldValidityStartDate', 'EffectiveYieldCalculationPeriodStartDate', 
            'EffectiveYieldCalculationPeriodEndDate', 'AccountLocked', 'ContractCapital', 
            'CommitmentCapital', 'CurrentContractCapital', 'CurrentCommitmentCapital', 
            'DisbursedCapital', 'PlannedCapital', 'EffectiveCapital', 'OutstandingInterest', 
            'RemainingCapital', 'OutstandingCharges', 'DisbursementObligation', 
            'InterestPaid', 'AccruedInterest', 'OutstandingBalance', 'RedrawBalance', 
            'CurrentOpenItemsAmount', 'CurrentOpenItemsCounter', 'CurrentOldestDueDate', 
            'CurrentOverdueDays', 'LiASchemaCluster', 'LiASchemaClusterTxt', 
            'SalesProduct', 'SalesProductTxt', 'Warehouse', 'WarehouseTxt', 
            'WarehouseValidFrom', 'DocumentType', 'DocumentTypeTxt', 'EmploymentStatus', 
            'EmploymentStatusTxt', 'MonthsTrading', 'DefaultJudgements', 'HardshipStatus', 
            'HardshipStatusTxt', 'PropertyInPossession', 'PropertyInPossessionTxt', 
            'SplitLoan', 'ArrearsHistory', 'Counter', 'RepaymentAmount', 'RepaymentPercentage', 
            'LimitTotalAmount', 'DirectDebitBankAccountHolder', 
            'DirectDebitBankAccount', 'DirectDebitBankAccountID', 'DirectDebitBankRoutingID', 
            'DirectDebitBankCountryCode', 'TermSpecificationStartDate', 'TermSpecificationEndDate', 
            'LastChangeUserID', 'NominalInterestRate',              
            'LifeCycleStatusTxt', 'CreationDate', 'CreationDateTime', 'CreationUserID']
    return the_attrs



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


if __name__ == '__main__': 
    from config import ENV, SERVER, ConfigEnviron
    
    loans_ids = [
        '10000002999-111-MX', '10000003019-111-MX', '10000003021-111-MX', 
        '10000003053-111-MX', '10000003080-111-MX', '10000003118-111-MX', 
        '10000003136-111-MX', '10000003140-111-MX', '10000003188-111-MX', 
        '10000003226-555-MX']
    
    pre_setup = ConfigEnviron(ENV, SERVER)
    az_manager = AzureResourcer(pre_setup)
    core_runner = SAPSession('qas-sap', az_manager)

    which = 'accounts'
    date_from = dt.now() - delta(days=7)
    event_resp = core_runner.get_events(which, date_from, output='Response')  # L. 123
    # (data_df, events_df) = core_runner.get_events(which, date_from, output='WithMetadata')
    
    
    
    
    
