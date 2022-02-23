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
        self.base_url = main_config['url']
        self.auth = HTTPBasicAuth(f"{main_config['user']}/token", main_config['token'])


    def get_promises(self, params): 
        promise_url = f'{self.base_url}/sunshine/objects/records'
        promises = self.get(promise_url, params={'type': 'payment_promise'})

        promises_ls = promises.json()['data']
        for prms_dict in promises_ls: 
            attrs = prms_dict.pop('attributes')
            attrs_new = {f'attribute_{k}': v for (k, v) in attrs.items()}
            prms_dict.update(attrs_new)

        return pd.DataFrame(promises_ls)


    def send_filter(self, filter_id): 
        filter_url = f'{self.base_url}/sunshine/objects/records/{filter_id}'
        pre_resp = self.get(filter_url)
        pre_data = pre_resp.json()['data']

        zis_params = self.call_dict(self.config['zis']).copy()
        zis_id  = zis_params.pop('id')
        zis_url = '/'.join([self.base_url, 'services/zis/inbound_webhooks', 
                'generic/ingest', zis_id]) 
        post_params = {
            'url'  : zis_url, 
            'auth' : HTTPBasicAuth(**zis_params), 
            'data' : dumps({'data': [pre_data]})}
            
        return self.post(**post_params)