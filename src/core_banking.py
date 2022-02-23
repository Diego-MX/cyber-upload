# Diego Villamil, EPIC
# CDMX, 4 de noviembre de 2021
# Copy del archivo en el repo DATA-SAP-EVENTS. 
# No lo hemos importado, sólo copiado-pegado. 

import re
from urllib.parse import unquote
import pandas as pd
from requests import Session, auth

from src.utilities import tools
from src.platform_resources import AzureResourcer
from config import ConfigEnviron, SITE, CORE_KEYS, PAGE_MAX


class SAPSession(Session): 
    def __init__(self, env, secret_env:AzureResourcer): 
        super().__init__()
        self.config     = CORE_KEYS[env]
        self.get_secret = secret_env.get_secret
        self.call_dict  = secret_env.call_dict
        self.set_main()


    def set_main(self): 
        main_config = self.config['main']
        self.headers.update(main_config['headers'])
        self.base_url = main_config['base-url']


    def set_token(self, auth_type=None): 
        access = self.call_dict(self.config['main']['access'])
        params = self.config['calls']['auth'].copy()
        params['data'] = self.call_dict(params['data'])
        if auth_type == 'basic':
            the_auth = auth.HTTPBasicAuth(**self.call_dict(access))
            params.update({'auth': the_auth})
        else: 
            auth_enc = tools.encode64("{username}:{password}".format(**access))
            params.update({'headers': {
                'Authorization': f'Basic {auth_enc}', 
                'Content-Type' : 'application/x-www-form-urlencoded'}})

        the_resp = self.post(**params)
        self.token = the_resp.json()


    def get_loans(self, attrs_indicator): 
        self.set_token()
        loan_config  = self.config['calls']['contract-set']
        select_attrs = attributes_from_column(attrs_indicator)
        loan_params  = {'$select': ','.join(select_attrs)}

        the_resp = self.get(f"{self.base_url}/{loan_config['sub-url']}", 
            auth=tools.BearerAuth(self.token['access_token']), 
            params=loan_params)
        loans_ls = the_resp.json()['d']['results']  # [metadata : [id, uri, type], borrowerName]
        for loan in loans_ls: 
            loan.pop('__metadata')
        
        return pd.DataFrame(loans_ls)


    def get_persons(self, params={}, how_many=PAGE_MAX): 
        self.set_token()
        
        person_conf = self.config['calls']['person-set']
        params_0 = {'$top': how_many, '$skip': 0}
        params_0.update(params)

        post_persons = []
        while True:    
            the_resp = self.get(f"{self.base_url}/{person_conf['sub-url']}", 
                    auth=tools.BearerAuth(self.token['access_token']), params=params_0)
            
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


    
if __name__ == '__main__': 
    from importlib import reload
    from src import core_banking
    from src import platform_resources
    from src.utilities import tools
    import config
    reload(core_banking)
    reload(platform_resources)
    reload(config)
    reload(tools)

    from src.core_banking import SAPSession
    from src.platform_resources import AzureResourcer
    from config import ConfigEnviron

    secretter = ConfigEnviron('local')
    azure_getter = AzureResourcer('local', secretter)
    core_session = SAPSession('qas', azure_getter)

    persons_df = core_session.get_persons()
    print(persons_df.shape)

