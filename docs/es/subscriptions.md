# Suscripciones SSE {: #sse-subscriptions }

Provisa admite envío en tiempo real mediante Server-Sent Events (SSE). Los clientes reciben un flujo de eventos de cambio sin necesidad de sondeo.

## Orígenes {: #sources }

Las suscripciones apuntan a una **tabla registrada**:

| Origen | Valores de `strategy` disponibles |
| -------- | ------------------------- |
| Tabla (PostgreSQL) | `native` (LISTEN/NOTIFY), `poll` |
| Tabla (RDBMS no PG con un bloque `cdc` de origen) | `debezium`, `kafka`, `poll` |
| Tabla (vista federada / cualquier otro origen) | solo `poll` |

### Instalación automática de triggers en PostgreSQL {: #postgresql-trigger-auto-installation }

Provisa instala automáticamente triggers `AFTER INSERT OR UPDATE OR DELETE` en todas las tablas de PostgreSQL **preaprobadas** al iniciar. Estos triggers invocan `pg_notify('provisa_{table}', ...)` para que las DML sin procesar (no solo las mutaciones de Provisa) sean captadas por las suscripciones.

Si la instalación del trigger falla (por ejemplo, por privilegios insuficientes — el rol de base de datos debe ser propietario de la tabla), Provisa recurre al sondeo por marca de agua para esa tabla, siempre que haya un `watermark_column` configurado. Se registra una advertencia.

### Suscripciones a vistas entre orígenes de datos {: #cross-datasource-view-subscriptions }

Para vistas que combinan (join) varios orígenes de datos mediante el motor de federación, agregue un `watermark_column` al registro de la tabla. La columna debe existir en el SQL de la vista (no es necesario que aparezca en el esquema de GraphQL):

```sql
-- Example: federated view with derived watermark
CREATE OR REPLACE VIEW orders_with_segments AS
SELECT o.*, s.name AS segment_name,
       GREATEST(o.updated_at, s.updated_at) AS _watermark
FROM postgresql.public.orders o
JOIN mysql.crm.customer_segments s ON o.customer_id = s.customer_id;
```

Regístrela con `watermark_column: _watermark`. Provisa sondea usando `WHERE _watermark > <last_seen>`.

### Suscripciones a relaciones anidadas {: #nested-relationship-subscriptions }

Cuando el campo de suscripción selecciona campos de tablas combinadas (mediante relaciones registradas), Provisa observa **todas** las tablas físicas involucradas simultáneamente. Un cambio en cualquier tabla combinada vuelve a disparar la consulta de suscripción.

## Endpoint

Suscribirse a una tabla:

```http
GET /data/subscribe/{table}
Accept: text/event-stream
```

La conexión permanece abierta y emite un evento JSON por cada cambio:

```text
data: {"event":"insert","table":"orders","row":{"id":43,"amount":55.00,"region":"east"}}

data: {"event":"update","table":"orders","row":{"id":42,"amount":199.00,"region":"west"}}
```

## Modos de entrega {: #delivery-modes }

La entrega se selecciona mediante `live.strategy` en la configuración de la tabla:

| `strategy` | Mecanismo | Disponible para | Requiere |
| ------------ | ----------- | --------------- | --------- |
| `native` | `LISTEN`/`NOTIFY` de PostgreSQL, Change Streams de MongoDB | PG, MongoDB | Nada adicional |
| `debezium` | Tema de Kafka desde el conector Debezium | Tablas de RDBMS no PG | Bloque `cdc` a nivel de origen (Debezium + Kafka) |
| `kafka` | Tema delta de Kafka arbitrario | Cualquier tabla alimentada por Kafka | Bloque `cdc` a nivel de origen |
| `poll` | Sondeo basado en marca de agua | Cualquier tabla con marca de agua | `watermark_column` |

### LISTEN/NOTIFY

Provisa emite `LISTEN <channel>` en una conexión persistente a PG. Las mutaciones de Provisa disparan `NOTIFY` automáticamente. Los escritores externos deben invocar `NOTIFY <channel>, '<payload>'` después de cada escritura. No se requiere infraestructura adicional.

### Sondeo {: #polling }

Provisa vuelve a ejecutar periódicamente la consulta de origen, seleccionando solo las filas donde `watermark_column > last_watermark`. Las diferencias se emiten como eventos SSE. El sondeo no puede detectar eliminaciones definitivas (hard deletes) — una fila eliminada no deja ninguna marca de agua que avance. Para que una eliminación sea visible, use una eliminación lógica (soft delete) (por ejemplo, active un indicador `deleted_at`) que incremente la columna de marca de agua; la eliminación llega entonces como un evento de actualización que transporta el indicador de eliminación lógica.

Configuración de sondeo de tabla (en `provisa.yaml`):

```yaml
tables:
  - id: federated_orders
    source_id: federated-source
    live:
      strategy: poll
      watermark_column: updated_at
      poll_interval: 30
      outputs:
        - type: sse
```

### Debezium CDC

Requiere un conector Debezium en ejecución que escriba en Kafka. Provisa consume el tema de Kafka y reenvía los eventos de cambio a los clientes SSE conectados.

El transporte CDC se configura una única vez por origen en un bloque `cdc`; los temas se derivan como `{topic_prefix}.{schema}.{table}` y nunca se repiten por tabla. Cada tabla selecciona entonces `strategy: debezium`:

```yaml
sources:
  - id: sales-mysql
    cdc:
      bootstrap_servers: kafka:9092
      topic_prefix: debezium
      # schema_registry_url: http://schema-registry:8081   # set for Avro; omit for JSON
    tables:
      - id: orders
        live:
          strategy: debezium
```

#### Codificaciones de mensajes {: #message-encodings }

Un tema de Debezium se lee en una de dos codificaciones, y cada mensaje dice cuál es.

| Codificación | Qué lleva el tema | Qué necesita el origen |
|---|---|---|
| JSON | El sobre del convertidor JSON de Debezium, con o sin la envoltura `schema`/`payload` | Nada más allá del bloque `cdc` |
| Avro | El formato de cable del schema registry de Confluent: un byte cero, un id de esquema de cuatro bytes, un dato Avro | `schema_registry_url`, y cómo autenticarse en él |

Los mensajes Avro se decodifican con el esquema que nombra su id, obtenido del registry una vez y conservado. Un tema cuyo esquema ha evolucionado se lee correctamente, porque cada mensaje nombra el esquema con el que se escribió. Los temas Protobuf y JSON Schema no se leen.

Un mensaje Avro nunca se lee como JSON. Cada forma de no poder leer uno se rechaza por su nombre:

| Situación | Rechazo |
|---|---|
| Mensaje Avro, el origen no nombra ningún registry | `subscribe.avro_topic_without_registry`, con el tema |
| Registry caído o sin responder a tiempo | `subscribe.schema_registry_unreachable` |
| El registry rechaza las credenciales del origen | `subscribe.schema_registry_refused_credentials` |
| Id de esquema que el registry no tiene | `subscribe.schema_id_unknown`, con el id |
| El id nombra un esquema Protobuf o JSON | `subscribe.schema_not_avro` |

Un registry caído o que rechaza las credenciales se rechaza cuando empieza la suscripción, antes de que se abra el flujo. Una consulta al registry espera como máximo 5 segundos para conectar y 10 segundos para una respuesta, y menos cuando a la solicitud le queda menos tiempo.

#### Autenticación del schema registry {: #schema-registry-authentication }

`schema_registry_auth` nombra un método; los campos que ese método necesita se comprueban al guardar el origen.

| `schema_registry_auth` | Campos obligatorios | Enviado al registry |
|---|---|---|
| `none` (predeterminado) | — | Sin credenciales |
| `basic` | `schema_registry_username`, `schema_registry_password` | Autenticación HTTP básica |
| `bearer` | `schema_registry_token` | `Authorization: Bearer <token>` (un token estático) |
| `mtls` | `schema_registry_client_cert`, `schema_registry_client_key` | El certificado de cliente, en el handshake TLS |

`schema_registry_ca` es opcional con cualquier método: un paquete de CA en el que confiar cuando el certificado del registry no lo firma una autoridad pública.

La contraseña y el token siguen el contrato de secretos. Un valor escrito en el formulario del origen se guarda en la bóveda de la organización y el origen conserva una referencia `${secret:NAME}`; una referencia escrita en un archivo de configuración (`${env:...}`, `${secret:...}`) se conserva tal cual. El certificado, la clave y el paquete de CA son archivos que abre el proceso del servidor de Provisa, nombrados por ruta absoluta.

```yaml
sources:
  - id: sales-mysql
    cdc:
      bootstrap_servers: kafka:9092
      topic_prefix: debezium
      schema_registry_url: https://schema-registry:8081
      schema_registry_auth: basic
      schema_registry_username: provisa
      schema_registry_password: ${secret:registry_password}
      schema_registry_ca: /etc/provisa/registry-ca.pem
```

## Cuando un flujo termina con un fallo {: #when-a-stream-ends-on-a-failure }

Un flujo que se ha abierto ya respondió `200`, así que un fallo posterior no puede ser un estado HTTP. El suscriptor recibe una última trama que dice por qué, y después el flujo se cierra. Un flujo nunca se cierra por un fallo sin esa trama.

La trama lleva lo mismo con lo que se respondería ese fallo en una solicitud normal:

| Fallo | Cuerpo |
|---|---|
| Un rechazo con código (el de un schema registry, el de una región, un rechazo de gobierno) | `detail`, `code`, `params` |
| El plazo de la solicitud | `detail: "Request timed out"` |
| Cualquier cosa inesperada | `detail: "Internal server error"`, `type` (el nombre del tipo de la excepción) |

Cada cuerpo lleva también `subscription_id`. En un fallo inesperado, el mensaje y la traza están en el registro del servidor bajo ese id, no en la trama.

La suscripción de tabla (`/data/subscribe/{table}`) envía el cuerpo como los datos de un evento llamado `stream_error`:

```text
event: stream_error
data: {"detail": "Topic 'debezium.app.orders' carries Avro messages, and its source names no schema registry to read them with: set the source's schema registry URL", "code": "subscribe.avro_topic_without_registry", "params": {"topic": "debezium.app.orders"}, "subscription_id": "5f0c2a9d41b7e386"}
```

El evento no se llama `error`, porque un `EventSource` lanza su propio evento `error` cuando se cae una conexión y un listener no podría distinguir los dos. Escúchelo por su nombre:

```javascript
source.addEventListener("stream_error", (e) => {
  const { detail, code, params } = JSON.parse(e.data);
  source.close();
});
```

Una suscripción GraphQL mantiene la forma de respuesta de GraphQL, con el código y los params bajo `extensions`:

```json
{"errors": [{"message": "…", "extensions": {"code": "subscribe.avro_topic_without_registry", "params": {"topic": "debezium.app.orders"}, "subscription_id": "5f0c2a9d41b7e386"}}]}
```

Un caso no termina una suscripción GraphQL: cuando falla la relectura tras un solo cambio, esa trama se envía con la misma forma y el flujo sigue con el siguiente cambio. Un fallo de la lectura inicial, del propio feed de cambios o de su arranque es la última trama.

## Redirección a un sink de Kafka {: #kafka-sink-redirect }

Cualquier suscripción de GraphQL puede redirigirse a un tema de Kafka en lugar de transmitirse de vuelta al cliente. Agregue el encabezado `X-Provisa-Sink` a la solicitud de suscripción:

```yaml
POST /data/graphql
Authorization: Bearer <token>
Content-Type: application/json
X-Provisa-Sink: kafka://broker:9092/my-topic
```

El servidor responde `202 Accepted` de inmediato e inicia una tarea en segundo plano que:

1. Observa los cambios de tabla usando la misma resolución de proveedor que SSE (LISTEN/NOTIFY → sondeo asyncpg → sondeo federado)
2. Vuelve a ejecutar la consulta equivalente ante cada cambio
3. Publica el resultado como un mensaje JSON en el tema de Kafka indicado

El sink se ejecuta durante toda la vida del proceso del servidor. Reinicie el servidor para detenerlo.

**Formato de URI:** `kafka://[broker:port]/topic`

- Si se omite `broker:port`, se usa la variable de entorno `KAFKA_BOOTSTRAP_SERVERS` (valor predeterminado: `localhost:9092`)
- `topic` es obligatorio

**Ejemplo (curl):**

```bash
curl -X POST http://localhost:8000/data/graphql \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -H "X-Provisa-Sink: kafka://kafka:9092/orders-live" \
  -d '{"query": "subscription { orders { id status amount } }"}'
# → 202 {"status":"streaming","sink":"kafka://kafka:9092/orders-live","table":"orders"}
```

### Sink de Kafka como segunda salida a nivel de configuración {: #kafka-sink-as-a-config-level-second-output }

Una suscripción de tabla basada en sondeo puede publicar simultáneamente en un tema de Kafka mediante `provisa.yaml`. La suscripción SSE y el sink de Kafka son ambos salidas del mismo Live Query Engine. Cada salida rastrea su marca de agua de forma independiente.

```yaml
tables:
  - id: active-orders
    live:
      strategy: poll
      watermark_column: updated_at
      poll_interval: 30
      outputs:
        - type: sse
        - type: kafka
          topic: provisa.active-orders
          bootstrap_servers: kafka:9092
          key_column: id
          role: analyst           # required: the rows are published governed as this role
```

Una salida de Kafka publica las filas de la tabla gobernadas como el rol que nombra: sus reglas de filas, la visibilidad de columnas y las máscaras se aplican como en una lectura de ese rol. Una salida sin `role` se rechaza al cargar la configuración.

## Seguridad {: #security }

Todos los modos de suscripción aplican el mismo pipeline de seguridad que las consultas habituales:

- Los filtros de seguridad de nivel de fila se aplican a cada fila emitida
- Las columnas enmascaradas aparecen enmascaradas en los eventos
- La autorización de rol se verifica en el momento de la conexión

## Ejemplo de cliente {: #client-example }

```javascript
// Table subscription (LISTEN/NOTIFY)
const source = new EventSource('/data/subscribe/orders', {
  headers: { 'Authorization': 'Bearer <token>' }
});

source.onmessage = (e) => {
  const event = JSON.parse(e.data);
  console.log(event.event, event.row);
};
```

</content>
