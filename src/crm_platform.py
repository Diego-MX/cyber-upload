# Diego Villamil, EPIC BANK
# CDMX, 7 de enero de 2022

from json import dumps
import pandas as pd
from dotenv import load_dotenv

from requests import Session
from requests.auth import HTTPBasicAuth
from src.platform_resources import AzureResourcer

from config import CRM_KEYS
load_dotenv(override=True)



class ZendeskSession(Session): 
    def __init__(self, env, secret_env: AzureResourcer): 
        super().__init__()
        self.config = CRM_KEYS[env]
        self.get_secret = secret_env.get_secret
        self.call_dict = secret_env.call_dict
        self.set_main()

    
    def set_main(self): 
        main_config = self.call_dict(self.config['main'])      
        main_config['username'] += '/token'
        self.base = main_config.pop('url')
        self.auth = HTTPBasicAuth(**main_config)        
        
    
    def get_promises(self): 
        promise_params = self.config['calls']['promises']
        promise_url = f"{self.base}/{promise_params['sub-url']}"
        promises = self.get(promise_url, params={'type': 'payment_promise'})

        promises_ls = promises.json()['data']
        for prms_dict in promises_ls: 
            attrs = prms_dict.pop('attributes')
            attrs_new = {f'attribute_{k}': v for (k, v) in attrs.items()}
            prms_dict.update(attrs_new)

        return pd.DataFrame(promises_ls)


    def post_filter(self, filter_id): 
        sub_urls = self.config['calls']['filters']
        pre_resp = self.get(f'{self.base}/{sub_urls[0]}/{filter_id}')
        pre_data = pre_resp.json()['data']

        zis_params = self.call_dict(self.config['zis'])
        zis_id = zis_params.pop('id')
        zis_kwargs = {
            'url'  : f'{self.base}/{sub_urls[1]}/{zis_id}', 
            'auth' : HTTPBasicAuth(**zis_params), 
            'data' : dumps({'data': [pre_data]})}
        return self.post(**zis_kwargs)



if __name__ == '__main__': 
    import config
    from src import platform_resources
    
    from importlib import reload
    reload(config)
    reload(platform_resources)
    
    from config import VaultSetter
    from src.platform_resources import AzureResourcer

    secretter = VaultSetter('local')
    azurer_getter = AzureResourcer('local', secretter)
    zendesk = ZendeskSession('sandbox', azurer_getter)

    promises_df = zendesk.get_promises()

