# Diego Villamil, EPIC BANK
# CDMX, 7 de enero de 2022

import os
import pandas as pd
from dotenv import load_dotenv

from requests import get as rq_get
from requests.auth import HTTPBasicAuth

from config import URLS
load_dotenv(override=True)

crm_env = 'sandbox'

CRM_URL = URLS['crm-call'][crm_env]
CRM_USER  = os.getenv('ZNDK_USER_EMAIL')
CRM_TOKEN = os.getenv('ZNDK_API_TOKEN')



promises = rq_get(f'{CRM_URL}/sunshine/objects/records', 
    auth=HTTPBasicAuth(f'{CRM_USER}/token', CRM_TOKEN),
    params={'type':'payment_promise'})

promises_ls = promises.json()['data']

for prms_dict in promises_ls: 
    attributes = prms_dict.pop('attributes')
    prms_dict.update(attributes)

promises_df = pd.DataFrame(promises_ls)

