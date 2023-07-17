from base64 import b64encode
from datetime import datetime as dt
from json import dumps
from pytz import timezone as tz 
from requests import get, post


horarios = {
    8  : ["20e00c51-96e3-11ed-87a1-e7db5563879a", "37faf717-96e3-11ed-800a-193e2fd80250"], 
    9  : ["1fe87b46-96e5-11ed-9155-2502ea979bbd", "20183d17-96e5-11ed-87a1-210230cc6337"], 
    10 : ["2041e529-96e5-11ed-87a1-775e2a013c9b", "20694408-96e5-11ed-9155-0f2ccaefc928",
          "2082e6a5-96e5-11ed-95fe-ffe93c59474d", "20a5b0ac-96e5-11ed-800a-17dcfcc0410d"], 
    11 : ["20c14efe-96e5-11ed-800a-2376693d5e47", "20e0bd8d-96e5-11ed-acff-4da148bffd73",
          "20f9c422-96e5-11ed-800a-0d1e0963d17b", "2117ac9b-96e5-11ed-95fe-0586fbd32bb9"], 
    12 : ["2135bbb4-96e5-11ed-800a-0123a6a0aeb3", "2155ee1d-96e5-11ed-95fe-df89d029532e",
          "21707adc-96e5-11ed-9155-9f6c00eaf8c6", "219010ce-96e5-11ed-9155-639d0e9d2e47",
          "140a742d-b215-11ed-ba44-7f74b640fac0"]}


# Insertar aquí los id´s de acuerdo con el horario que se defina 
now_time = dt.now(tz=tz("America/Mexico_City"))
ids = horarios[now_time.hour]


# Autenticación de la API de Zendesk
username = 'username/token'
password = 'password'
credentials = f'{username}:{password}'
token_zis = 'token zis'

encoded_credentials = b64encode(credentials.encode('utf-8')).decode('utf-8')

zis_url = 'https://bineo.zendesk.com/api/services/zis/inbound_webhooks/generic/ingest/CNr92PrXCYdf0KEXi14vPcbxM3Xt941Sr8GJG_FcD1nb'
api_url = 'https://bineo.zendesk.com/api/sunshine/objects/records'

# Consultar los IDs en Zendesk
data = []
for obj_id in ids:
    obj_url = f'{api_url}/{obj_id}'
    headers = {
        'Content-Type': 'application/json',
        'Authorization': f'Basic {encoded_credentials}'
    }
    response = get(obj_url, headers=headers)
    
    if response.status_code == 200:
        obj_data = response.json()
        data.append(obj_data['data'])

data = {'data': data}

# Enviar los datos al webhook de Zendesk ZIS
headers = {
        'Content-Type': 'application/json',
        'Authorization': f'Basic {token_zis}'}

response = post(zis_url, headers=headers, data=dumps(data))