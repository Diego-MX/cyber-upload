# Diego Villamil, EPIC BANK
# CDMX, 7 de enero de 2022

from dotenv import load_dotenv
from httpx import (Client, AsyncClient, 
    Auth as AuthX, post as postx, BasicAuth as BasicX)
from json import dumps
import pandas as pd
from requests import Session, auth

from src.platform_resources import AzureResourcer
from config import CRM_KEYS
try: 
    from dotenv import load_dotenv
except ImportError: 
    load_dotenv = None


    
class ZendeskSession(Client): 
    def __init__(self, env, secret_env: AzureResourcer): 
        self.config = CRM_KEYS[env]
        self.get_secret = secret_env.get_secret
        self.call_dict  = secret_env.call_dict
        
        super().__init__()
        main_args = self.call_dict(self.config['main'])
        self.base_url = main_args.pop('url')
        main_args['username'] += '/token'
        self.auth = BasicX(**main_args)


    api_calls = {
        'promises' : "sunshine/objects/records", 
        'filters'  :("sunshine/objects/records",
                "services/zis/inbound_webhooks/generic/ingest")}         
    
    
    def get_promises(self): 
        getters = {'url': self.api_calls['promises'], 
            'params': {'type': 'payment_promise'}}
        p_resp = self.get(**getters)
        p_data = self.data_response(p_resp)
        return p_data
        
        
    def data_response(self, response): 
        d_list = response.json()['data'].copy()
        for d_entry in d_list: 
            attrs = d_entry.pop('attributes')
            attrs_new = {f'attribute_{k}': v for (k, v) in attrs.items()}
            d_entry.update(attrs_new)
        return pd.DataFrame(d_list)
            
    
    def post_filter(self, filter_id): 
        f_urls = self.api_calls['filters']
        resp_0 = self.get(url=f"{f_urls[0]}/{filter_id}")
        zis_auth = self.call_dict(self.config['zis'])
        posters  = {
            'url' : f"{f_urls[1]}/{zis_auth.pop('id')}", 
            'auth': BasicX(**zis_auth), 
            'data': dumps({'data': [resp_0.json()['data']]})}
        return self.post(**posters)
    
   
        
class ZendeskAuth(AuthX): 
    def __init__(self, token, post_args):
        self.token = token
        self.post_args = post_args
    
    def auth_flow(self, request):
        response = yield request
        if response.status_code == 401:
            j_token = postx(**self.post_args).json()
            self.token = j_token['access_token']
            request.headers['Authorization'] = f"Bearer {self.token}"
            yield request
            
        


if __name__ == '__main__': 
    from config import VaultSetter
    from src import platform_resources
    from src.platform_resources import AzureResourcer

    load_dotenv(override = True)
    
    secretter = VaultSetter('local')
    azurer_getter = AzureResourcer('local', secretter)
    zendesk = ZendeskSession('sandbox', azurer_getter)

    promises_df = zendesk.get_promises()

