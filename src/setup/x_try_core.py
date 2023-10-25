import pandas as pd
from src.core_banking import SAPSession, BearerAuth
from src.platform_resources import AzureResourcer
from config import ConfigEnviron, ENV, SERVER


app_environ = ConfigEnviron(ENV, SERVER)
az_manager  = AzureResourcer(app_environ)
core_session = SAPSession('qas-sap', az_manager)


core_cfg = core_session.config
get_call = lambda a_key: core_cfg['calls'][a_key]['sub-url']
base_url = core_cfg['main']['base-url']

keys = ['person-set', 'contract-set', 'contract-qan', 'contract-current', 'contract-loans']

a_key = 'contract-qan'
core_session.set_token()
an_auth = BearerAuth(core_session.token['access_token'])
a_params = {"$top": 500}

resp = core_session.get(f"{base_url}/{get_call(a_key)}",
    auth=an_auth, params=a_params)
call_ls = resp.json()['d']['results']

nested = set()
for a_call in call_ls: 
    for a_field, its_value in a_call.items(): 
        if isinstance(its_value, dict): 
            nested.add(a_field)

for a_call in call_ls: 
    for n_field in nested: 
        if n_field in a_call: 
            dull = a_call.pop(n_field)

for nest_col in nested: 
    print(nest_col)

calls_df = pd.DataFrame(call_ls)    
calls_df.to_csv(f"refs/catalogs/api_calls/{a_key}.csv", index=False)

