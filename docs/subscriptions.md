# SSE Subscriptions

Provisa supports real-time push over Server-Sent Events (SSE). Clients receive a stream of change events without polling.

## Sources

Subscriptions can target either a **registered table** or a **governed query**:

| Source | Delivery modes available |
|--------|-------------------------|
| Table (PostgreSQL) | `listen` (LISTEN/NOTIFY), `cdc` (Debezium), `poll` |
| Table (non-PG, e.g. federated view) | federated poll only |
| Governed query | `poll` only |

A governed query subscription uses the same poll engine as a non-CDC table subscription. `watermark_column` is required for all poll delivery.

### PostgreSQL trigger auto-installation

Provisa automatically installs `AFTER INSERT OR UPDATE OR DELETE` triggers on all **pre-approved** PostgreSQL tables at startup. These triggers call `pg_notify('provisa_{table}', ...)` so that raw DML (not just Provisa mutations) is picked up by subscriptions.

If trigger installation fails (e.g. insufficient privilege — the database role must own the table), Provisa falls back to watermark polling for that table, provided a `watermark_column` is configured. A warning is logged.

### Cross-datasource view subscriptions

For views that join multiple datasources via the federation engine, add a `watermark_column` to the table registration. The column must exist in the view SQL (it need not appear in the GraphQL schema):

```sql
-- Example: federated view with derived watermark
CREATE OR REPLACE VIEW orders_with_segments AS
SELECT o.*, s.name AS segment_name,
       GREATEST(o.updated_at, s.updated_at) AS _watermark
FROM postgresql.public.orders o
JOIN mysql.crm.customer_segments s ON o.customer_id = s.customer_id;
```

Register with `watermark_column: _watermark`. Provisa polls using `WHERE _watermark > <last_seen>`.

### Nested relationship subscriptions

When the subscription field selects fields from joined tables (via registered relationships), Provisa watches **all** involved physical tables simultaneously. A change to any joined table re-fires the subscription query.

## Endpoint

Subscribe to a table:
```
GET /data/subscribe/{table}
Accept: text/event-stream
```

Subscribe to a governed query:
```
GET /data/subscribe/query/{query_id}
Accept: text/event-stream
```

The connection stays open and emits one JSON event per change:
```
data: {"event":"insert","table":"orders","row":{"id":43,"amount":55.00,"region":"east"}}

data: {"event":"update","table":"orders","row":{"id":42,"amount":199.00,"region":"west"}}
```

## Delivery Modes

| Mode | Mechanism | Available for | Requires |
|------|-----------|---------------|---------|
| `listen` | PostgreSQL `LISTEN`/`NOTIFY` | PG tables | Nothing extra |
| `cdc` | Kafka topic from Debezium connector | Non-PG RDBMS tables | Debezium + Kafka |
| `poll` | Watermark-based polling | Any table, any governed query | `watermark_column` |

### LISTEN/NOTIFY

Provisa issues `LISTEN <channel>` on a persistent PG connection. Provisa mutations fire `NOTIFY` automatically. External writers must call `NOTIFY <channel>, '<payload>'` after writes. No additional infrastructure required.

### Polling

Provisa re-executes the source query periodically, selecting only rows where `watermark_column > last_watermark`. Diffs are emitted as SSE events. Deletes require a `soft_delete_column` (`deleted_at` or `is_deleted`) on the source.

Table poll config (in `provisa.yaml`):
```yaml
tables:
  - id: federated_orders
    source_id: federated-source
    live:
      delivery: poll
      watermark_column: updated_at
      soft_delete_column: deleted_at
      poll_interval: 30s
      outputs:
        - type: sse_subscription
```

Governed query poll config:
```yaml
governed_queries:
  - id: active-orders
    query: "{ orders(where: {status: {_eq: \"active\"}}) { id amount updated_at } }"
    live:
      watermark_column: updated_at
      poll_interval: 30s
      outputs:
        - type: sse_subscription
```

### Debezium CDC

Requires a running Debezium connector writing to Kafka. Provisa consumes the Kafka topic and forwards change events to connected SSE clients. Latency is typically sub-second.

Configure the Debezium topic in `config.yaml`:
```yaml
sources:
  - id: sales-mysql
    tables:
      - id: orders
        cdc_topic: debezium.public.orders
```

## Kafka Sink Redirect

Any GraphQL subscription can be redirected to a Kafka topic instead of streaming back to the client. Add the `X-Provisa-Sink` header to the subscription request:

```
POST /data/graphql
Authorization: Bearer <token>
Content-Type: application/json
X-Provisa-Sink: kafka://broker:9092/my-topic
```

The server responds `202 Accepted` immediately and starts a background task that:
1. Watches for table changes using the same provider resolution as SSE (LISTEN/NOTIFY → asyncpg poll → federated poll)
2. Re-executes the equivalent query on each change
3. Publishes the result as a JSON message to the named Kafka topic

The sink runs for the lifetime of the server process. Restart the server to stop it (persistent sink registration via the admin API is planned).

**URI format:** `kafka://[broker:port]/topic`

- If `broker:port` is omitted, `KAFKA_BOOTSTRAP_SERVERS` env var is used (default: `localhost:9092`)
- `topic` is required

**Example (curl):**
```bash
curl -X POST http://localhost:8000/data/graphql \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -H "X-Provisa-Sink: kafka://kafka:9092/orders-live" \
  -d '{"query": "subscription { orders { id status amount } }"}'
# → 202 {"status":"streaming","sink":"kafka://kafka:9092/orders-live","table":"orders"}
```

### Kafka Sink as a Config-Level Second Output

A governed query (or poll-based table subscription) can simultaneously publish to a Kafka topic via `provisa.yaml`. SSE subscription and Kafka sink are both outputs of the same Live Query Engine. Each output tracks its watermark independently.

```yaml
governed_queries:
  - id: active-orders
    live:
      watermark_column: updated_at
      poll_interval: 30s
      outputs:
        - type: sse_subscription
        - type: kafka_sink
          topic: provisa.active-orders
          key_column: id
```

See [Kafka Sinks](./kafka-sinks.md) for full sink configuration reference.

## Security

All subscription modes enforce the same security pipeline as regular queries:
- RLS filters are applied to every emitted row
- Masked columns appear masked in events
- Role authorization is checked at connection time

A client whose role loses access mid-stream receives a `{"event":"unauthorized"}` event and the connection closes.

## Client Example

```javascript
// Table subscription (LISTEN/NOTIFY)
const source = new EventSource('/data/subscribe/orders', {
  headers: { 'Authorization': 'Bearer <token>' }
});

// Governed query subscription (poll)
const source = new EventSource('/data/subscribe/query/active-orders', {
  headers: { 'Authorization': 'Bearer <token>' }
});

source.onmessage = (e) => {
  const event = JSON.parse(e.data);
  console.log(event.event, event.row);
};
```
