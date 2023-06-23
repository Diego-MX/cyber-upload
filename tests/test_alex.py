from datetime import datetime as dt, timedelta as delta
from json import dumps
from requests import post
from pytz import timezone as tz

#Se debe ejecutar a las 08:00 hrs diariamente
yesterday = dt.now(tz=tz("America/Mexico_City")) - delta(days=1)
dueDate = yesterday.strftime('%Y-%m-%d')

url_prd = 'https://bineo.zendesk.com/api/services/zis/inbound_webhooks/generic/ingest/I8VQhJ85KZPw0XNTfB8j5oeQxKj4pNdu69gSnIfe01Rc'
url_stg = 'https://bineo1643240086.zendesk.com/api/services/zis/inbound_webhooks/generic/ingest/LMW9nfqQh7jA18EWVG9gIYSFKKiU-wRvCzwdf4SYU8oA'

data = {"dueDate": dueDate}
payload = dumps(data)

#Configurar las cabeceras de la solicitud
headers_prd= {
    'Content-Type': 'application/json',
    'Authorization': f'Basic {{token_prd}}'
}
headers_stg = {
    'Content-Type':  'application/json',
    'Authorization': f'Basic {{token_stg}}'
}

#Realizar la solicitud POST a la API con la carga útil y cabeceras especificadas
response_prd = post(url_prd, data=payload, headers=headers_prd)
response_stg = post(url_stg, data=payload, headers=headers_stg)