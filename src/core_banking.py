# Diego Villamil, EPIC
# CDMX, 4 de noviembre de 2021

from datetime import datetime as dt, timedelta as delta, date
from httpx import (AsyncClient, Auth as AuthX, BasicAuth as BasicAuthX, post as postx)
from itertools import product
from json import dumps
import pandas as pd
import re
from requests import Session, auth, post, get
from typing import Union
from urllib.parse import unquote
import xmltodict

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
    

    
class SAPSession(Session):  # REQUESTS. 
    def __init__(self, env, secret_env: AzureResourcer): 
        self.config     = CORE_KEYS[env]
        self.get_secret = secret_env.get_secret
        self.call_dict  = secret_env.call_dict
        
        super().__init__()
        self.base_url = self.config['main']['base-url']
        self.headers.update(self.config['main']['headers'])
        self.update_token()
              
    def update_token(self, auth_type=None): 
        posters = {
            'url' : self.config['calls']['auth']['url'], 
            'data': self.call_dict(self.config['calls']['auth']['data']), 
            'auth': auth.HTTPBasicAuth(**self.call_dict(self.config['main']['access'])), 
            'headers': {'Content-Type': "application/x-www-form-urlencoded"}
        }
        the_resp = post(**posters)
        self.token = the_resp.json()
        self.auth = BearerAuth2(self.token['access_token'])
  

    def get_events(self, event_type=None, date_lim: dt=None, output=None): 
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
        
        eventers = {
            'url'    : f"{self.base_url}/{event_config[event_type]}",
            'params' : {
                '$filter' : datetime_2_filter(date_lim, 'and'), 
                '$expand' : ','.join(exp_params)}}
        
        ii = 0
        the_resp = self.get(**eventers)
        while the_resp.status_code == 401 and ii < 5: 
            self.update_token()
            the_resp = self.get(**eventers)
            ii += 1
            
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
                'yyyymmdd': ['PostingDate', 'ValueDate']}}
        
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
        

    def get_loans(self): 
        select_attrs = attributes_from_column('all')
        loaners = {
            'url': f"{self.base_url}/{self.config['calls']['contract-set']['sub-url']}", 
            'params': {'$select': ','.join(select_attrs)} }
        
        ii = 0
        the_resp = self.get(**loaners)
        while the_resp.status_code == 401 and ii < 5: 
            self.update_token()
            the_resp = self.get(**loaners)
            ii += 1
            
        loans_ls = the_resp.json()['d']['results']  # [metadata : [id, uri, type], borrowerName]
        
        if loans_ls: 
            for loan in loans_ls: 
                loan.pop('__metadata')
            loans_df = pd.DataFrame(loans_ls)
        else: 
            loans_df = pd.DataFrame([], columns=select_attrs)
        return loans_df 


    def get_loans_qan(self): 
        loan_config  = self.config['calls']['contract-qan']
        select_attrs = attributes_from_column('qan')
        
        loaners = {'url': f"{self.base_url}/{loan_config['sub-url']}", 
            'params': {'$select': ','.join(select_attrs)}
        }
        
        ii = 0
        the_resp = self.get(**loaners)
        while the_resp.status_code == 401 and ii < 5: 
            self.update_token()
            the_resp = self.get(**loaners)
            ii += 1
            
        loans_ls = the_resp.json()['d']['results']  # [metadata : [id, uri, type], borrowerName]
        for loan in loans_ls: 
            loan.pop('__metadata')
        return pd.DataFrame(loans_ls)

    
    def get_persons(self, params_x={}, how_many=PAGE_MAX): 
        person_conf = self.config['calls']['person-set']
        params_0 = {'$top': how_many, '$skip': 0}
        params_0.update(params_x)

        post_persons = []
        while True:
            getters = {
                'url': f"{self.base_url}/{person_conf['sub-url']}", 
                'params': params_0}
            
            ii = 0 
            the_resp = self.get(**getters)
            while the_resp.status_code == 401 and ii < 5: 
                self.update_token()  
                the_resp = self.get(**getters)
                ii += 1
              
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

        
    def set_status_hook(self, auth_tries=3): 
        def status_hook(response, *args, **kwargs): 
            response_1 = response
            for ii in range(auth_tries): 
                if response_1.status_code == 200: 
                    break
                elif response_1.status_code == 401: 
                    self.set_token()
                    response_1 = self.send(response.request)
                    continue
            else: 
                response_1.raise_for_status()
            return response_1
        
        self.hooks['response'].append(status_hook)

        
    def hook_d_results(self, response): 
        hook_allowed_types = ['application/json', 'application/atom+xml']
        
        the_type = response.headers['Content-Type']
        if 'application/json' in the_type: 
            the_json = response.json()
            the_results = the_json['d']['results']
        elif 'application/atom+xml' in the_type: 
            the_results = self._xml_results(response.text)
        else: 
            raise Exception(f"Couldn't extract results from response with content type '{the_type}'.")
            
        return the_results

    
    def _xml_results(self, xml_text): 
        get_entry_ds = (lambda entry_dict: 
            {re.sub(r'^d\:', '', k_p): v_p 
            for k_p, v_p in entry_dict.items() if k_p.startswith('d:')})
        
        entry_ls = xmltodict.parse(xml_text)['feed']['entry']
        entry_props = [entry['content']['m:properties'] for entry in entry_ls]
        entry_rslts = [get_entry_ds(prop) for prop in entry_props]
        return entry_rslts

        




class SAPSessionAsync(AsyncClient):  # AsyncOAuth2Client
    def __init__(self, env, secret_env: AzureResourcer, **kwargs): 
        self.config     = CORE_KEYS[env]
        self.get_secret = secret_env.get_secret
        self.call_dict  = secret_env.call_dict
        
        super().__init__(**kwargs)
        self.base_url = self.config['main']['base-url']
        self.headers.update(self.std_headers) 
        self.update_token()
        
        _access  = self.call_dict(self.config['main']['access'])
        clienters = {
            'client_id'    : _access['username'], 
            'client_secret': _access['password'], 
            'token_endpoint_auth_method': 'client_secret_basic' # 'client_secret_post', 'client_secret_jwt'
            } # token_enpoint=self.config['calls']['auth']['url'],    
        authers = {
            'url'    : self.config['calls']['auth']['url'],
            'data'   : self.call_dict(self.config['calls']['auth']['data']), 
            'headers': {
                'Content-Type': "application/x-www-form-urlencoded"}}
        # super().__init__(**clienters, **kwargs)
        # token_endpoint_auth_method=None, 'client_secret_basic' 
        # revocation_endpoint_auth_method=None, scope=None, redirect_uri=None, 
        # token=None, token_placement='header', update_token=None,
        
    std_headers = {
            'Accept-Encoding' : 'gzip, deflate',
            'Content-Type'    : 'application/json',
            'Accept' : 'application/json',
            'Format' : 'json'}
    
    
    async def get_by_api(self, api_type, loan_id=None): 
        type_ref = 'ContractSet' if loan_id is None else f"ContractSet('{loan_id}')"
        url_key = tools.str_snake_to_camel(api_type, first_too=True)
        sub_url = f"/s4b/v1/lacovr/{type_ref}/{url_key}"
        
        ii = 0
        the_resp = await self.get(sub_url)
        while the_resp.status_code == 401 and ii < 5: 
            self.update_token()
            await self.get(sub_url)
            ii += 1
        
        results_ls = the_resp.json()['d']['results']
        for each_result in results_ls: 
            each_result.pop('__metadata')
        
        now_str = dt.now().strftime("%Y-%m-%d %H:%M:%S")
        results_df = (pd.DataFrame(results_ls)
            .assign(ts_call=now_str))
        return results_df

    
    def update_token(self, auth_type=None): 
        # access = call_dict(config['main']['access'])
        # auth_encode = encode64("{username}:{password}".format[**access])
        authers = {
            'url' : self.config['calls']['auth']['url'], 
            'data': self.call_dict(self.config['calls']['auth']['data']), 
            'auth': BasicAuthX(**self.call_dict(self.config['main']['access'])), 
            'headers': { # Authorization: f"Basic {auth_encode}"
                'Content-Type': "application/x-www-form-urlencoded"}
        }
        the_resp = postx(**authers)
        self.token = the_resp.json()
        self.auth  = BearerAuth2(self.token['access_token'])

        
class SAPAuthX(AuthX): 
    def __init__(self, token_str, config): 
        self.config = config
        self.token = token_str 
    
    def auth_flow(self, a_request): 
        response = yield a_request

        if response.status_code == 401:
            posters = {
                'url' : self.config['url' ], 
                'data': self.config['data'], 
                'auth': AuthX(**self.config['access']), 
                'headers': {'Content-Type': "application/x-www-form-urlencoded"}
            }
            the_resp = self.post(**posters)
            self.token = the_resp.json()
            a_request.headers['X-Authentication'] = self.token
            yield a_request
            
    
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
    
    
    
    
    
