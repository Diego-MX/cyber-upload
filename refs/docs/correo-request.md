Te comparto los datos para la implementación del cronjob para envío de comunicaciones de cobranza.
Para el paso 1, las contraseñas son las habituales de Zendesk. 
Para el paso 2 compartiré por separado los accesos

### Paso 1: Consulta de filtros a enviar

Endpoint: `[GET] https://{{url}}.zendesk.com/api/sunshine/objects/records/{{id}}`
Ejemplo de respuesta:

 
### Paso 2, Envío de comunicaciones

`[POST] https://{{url}}.zendesk.com/api/services/zis/inbound_webhooks/generic/ingest/{{id_ZIZ}}`
Payload: Se envía la respuesta del paso 1, agregando corchetes [] en el bloque “data”


Variables:
-	`url: bineo1633010523`
-	`id`: (Tres para el primer bloque de pruebas)
  o	Test Código Postal - Email 2:  `8b8057aa-806c-11ec-bb5a-97eb7d99a45a` 
  o	Test Código Postal – WhatsApp: `8ba5ba29-806c-11ec-b252-43df5e7d5d44`
  o	Test Código Postal – SMS:      `8bc3a1f4-806c-11ec-ab3b-410d32afd7c3`
-	`Id_ZIZ: CacbuDusaK69cuGfgZgSg_e8XsrWZBuGNR-3RDC31USi`

Estos datos son del ambiente de Sandbox, eventualmente debemos hacer la migración a producción

Reglas de envío:
El envío deberá ejecutarse diariamente, en los siguientes horarios

Test Código Postal - Email 2:  09:00 hrs
Test Código Postal – WhatsApp: 13:00 hrs
Test Código Postal – SMS:      17:00 hrs

Si queda alguna duda, por favor indícame 
