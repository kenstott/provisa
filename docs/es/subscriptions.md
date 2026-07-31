# Suscripciones SSE

Provisa admite envío en tiempo real mediante Server-Sent Events (SSE). Los clientes reciben un flujo de eventos de cambio sin necesidad de sondeo. (REQ-258)

## Orígenes

Las suscripciones apuntan a una **tabla registrada**:

| Origen | Valores de `strategy` disponibles |
| -------- | ------------------------- |
| Tabla (PostgreSQL) | `native` (LISTEN/NOTIFY), `poll` |
| Tabla (RDBMS no PG con un bloque `cdc` de origen) | `debezium`, `kafka`, `poll` |
| Tabla (vista federada / cualquier otro origen) | solo `poll` |

### Instalación automática de triggers en PostgreSQL

Provisa instala automáticamente triggers `AFTER INSERT OR UPDATE OR DELETE` en todas las tablas de PostgreSQL **preaprobadas** al iniciar. (REQ-565) Estos triggers invocan `pg_notify('provisa_{table}', ...)` para que las DML sin procesar (no solo las mutaciones de Provisa) sean captadas por las suscripciones. (REQ-565)

Si la instalación del trigger falla (por ejemplo, por privilegios insuficientes — el rol de base de datos debe ser propietario de la tabla), Provisa recurre al sondeo por marca de agua para esa tabla, siempre que haya un `watermark_column` configurado. (REQ-566) Se registra una advertencia. (REQ-566)

### Suscripciones a vistas entre orígenes de datos

Para vistas que combinan (join) varios orígenes de datos mediante el motor de federación, agregue un `watermark_column` al registro de la tabla. (REQ-260, REQ-283) La columna debe existir en el SQL de la vista (no es necesario que aparezca en el esquema de GraphQL):

```sql
-- Example: federated view with derived watermark
CREATE OR REPLACE VIEW orders_with_segments AS
SELECT o.*, s.name AS segment_name,
       GREATEST(o.updated_at, s.updated_at) AS _watermark
FROM postgresql.public.orders o
JOIN mysql.crm.customer_segments s ON o.customer_id = s.customer_id;
```

Regístrela con `watermark_column: _watermark`. Provisa sondea usando `WHERE _watermark > <last_seen>`. (REQ-260)

### Suscripciones a relaciones anidadas

Cuando el campo de suscripción selecciona campos de tablas combinadas (mediante relaciones registradas), Provisa observa **todas** las tablas físicas involucradas simultáneamente. (REQ-567) Un cambio en cualquier tabla combinada vuelve a disparar la consulta de suscripción. (REQ-567)

## Endpoint

Suscribirse a una tabla:

```http
GET /data/subscribe/{table}
Accept: text/event-stream
```

La conexión permanece abierta y emite un evento JSON por cada cambio: (REQ-258, REQ-568)

```text
data: {"event":"insert","table":"orders","row":{"id":43,"amount":55.00,"region":"east"}}

data: {"event":"update","table":"orders","row":{"id":42,"amount":199.00,"region":"west"}}
```

## Modos de entrega

La entrega se selecciona mediante `live.strategy` en la configuración de la tabla: (REQ-813, REQ-814)

| `strategy` | Mecanismo | Disponible para | Requiere |
| ------------ | ----------- | --------------- | --------- |
| `native` | `LISTEN`/`NOTIFY` de PostgreSQL, Change Streams de MongoDB | PG, MongoDB | Nada adicional |
| `debezium` | Tema de Kafka desde el conector Debezium | Tablas de RDBMS no PG | Bloque `cdc` a nivel de origen (Debezium + Kafka) |
| `kafka` | Tema delta de Kafka arbitrario | Cualquier tabla alimentada por Kafka | Bloque `cdc` a nivel de origen |
| `poll` | Sondeo basado en marca de agua | Cualquier tabla con marca de agua | `watermark_column` |

### LISTEN/NOTIFY

Provisa emite `LISTEN <channel>` en una conexión persistente a PG. (REQ-258) Las mutaciones de Provisa disparan `NOTIFY` automáticamente. (REQ-565) Los escritores externos deben invocar `NOTIFY <channel>, '<payload>'` después de cada escritura. No se requiere infraestructura adicional.

### Sondeo

Provisa vuelve a ejecutar periódicamente la consulta de origen, seleccionando solo las filas donde `watermark_column > last_watermark`. (REQ-260) Las diferencias se emiten como eventos SSE. El sondeo no puede detectar eliminaciones definitivas (hard deletes) — una fila eliminada no deja ninguna marca de agua que avance. Para que una eliminación sea visible, use una eliminación lógica (soft delete) (por ejemplo, active un indicador `deleted_at`) que incremente la columna de marca de agua; la eliminación llega entonces como un evento de actualización que transporta el indicador de eliminación lógica. (REQ-260)

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

Requiere un conector Debezium en ejecución que escriba en Kafka. (REQ-261) Provisa consume el tema de Kafka y reenvía los eventos de cambio a los clientes SSE conectados. (REQ-261)

El transporte CDC se configura una única vez por origen en un bloque `cdc`; los temas se derivan como `{topic_prefix}.{schema}.{table}` y nunca se repiten por tabla. (REQ-824) Cada tabla selecciona entonces `strategy: debezium`:

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

## Redirección a un sink de Kafka

Cualquier suscripción de GraphQL puede redirigirse a un tema de Kafka en lugar de transmitirse de vuelta al cliente. (REQ-812) Agregue el encabezado `X-Provisa-Sink` a la solicitud de suscripción:

```yaml
POST /data/graphql
Authorization: Bearer <token>
Content-Type: application/json
X-Provisa-Sink: kafka://broker:9092/my-topic
```

El servidor responde `202 Accepted` de inmediato e inicia una tarea en segundo plano que: (REQ-812)

1. Observa los cambios de tabla usando la misma resolución de proveedor que SSE (LISTEN/NOTIFY → sondeo asyncpg → sondeo federado)
2. Vuelve a ejecutar la consulta equivalente ante cada cambio
3. Publica el resultado como un mensaje JSON en el tema de Kafka indicado

El sink se ejecuta durante toda la vida del proceso del servidor. (REQ-812) Reinicie el servidor para detenerlo (el registro persistente de sinks mediante la API de administración está planificado).

**Formato de URI:** `kafka://[broker:port]/topic`

- Si se omite `broker:port`, se usa la variable de entorno `KAFKA_BOOTSTRAP_SERVERS` (valor predeterminado: `localhost:9092`) (REQ-812)
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

### Sink de Kafka como segunda salida a nivel de configuración

Una suscripción de tabla basada en sondeo puede publicar simultáneamente en un tema de Kafka mediante `provisa.yaml`. (REQ-282, REQ-286) La suscripción SSE y el sink de Kafka son ambos salidas del mismo Live Query Engine. (REQ-282) Cada salida rastrea su marca de agua de forma independiente. (REQ-286)

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
```

Consulte [Kafka Sinks](sources.md) para la referencia completa de configuración de sinks.

## Seguridad

Todos los modos de suscripción aplican el mismo pipeline de seguridad que las consultas habituales: (REQ-258, REQ-038)

- Los filtros de seguridad de nivel de fila se aplican a cada fila emitida (REQ-040)
- Las columnas enmascaradas aparecen enmascaradas en los eventos (REQ-040)
- La autorización de rol se verifica en el momento de la conexión (REQ-258)

## Ejemplo de cliente

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
