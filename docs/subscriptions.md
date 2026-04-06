# SSE Subscriptions

Provisa supports real-time push over Server-Sent Events (SSE). Clients receive a stream of change events without polling.

## Sources

Subscriptions can target either a **registered table** or a **persisted query**:

| Source | Delivery modes available |
|--------|-------------------------|
| Table (PostgreSQL) | `listen` (LISTEN/NOTIFY), `cdc` (Debezium), `poll` |
| Table (non-PG, e.g. Trino-federated) | `poll` only |
| Persisted query | `poll` only |

A persisted query subscription uses the same poll engine as a non-CDC table subscription. `watermark_column` is required for all poll delivery.

## Endpoint

Subscribe to a table:
```
GET /data/subscribe/{table}
Accept: text/event-stream
```

Subscribe to a persisted query:
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
| `poll` | Watermark-based polling | Any table, any persisted query | `watermark_column` |

### LISTEN/NOTIFY

Provisa issues `LISTEN <channel>` on a persistent PG connection. Provisa mutations fire `NOTIFY` automatically. External writers must call `NOTIFY <channel>, '<payload>'` after writes. No additional infrastructure required.

### Polling

Provisa re-executes the source query periodically, selecting only rows where `watermark_column > last_watermark`. Diffs are emitted as SSE events. Deletes require a `soft_delete_column` (`deleted_at` or `is_deleted`) on the source.

Table poll config (in `provisa.yaml`):
```yaml
tables:
  - id: trino_orders
    source_id: trino-federation
    live:
      delivery: poll
      watermark_column: updated_at
      soft_delete_column: deleted_at
      poll_interval: 30s
      outputs:
        - type: sse_subscription
```

Persisted query poll config:
```yaml
persisted_queries:
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

## Kafka Sink as a Second Output

A persisted query (or poll-based table subscription) can simultaneously publish to a Kafka topic. SSE subscription and Kafka sink are both outputs of the same Live Query Engine. Each output tracks its watermark independently.

```yaml
persisted_queries:
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

// Persisted query subscription (poll)
const source = new EventSource('/data/subscribe/query/active-orders', {
  headers: { 'Authorization': 'Bearer <token>' }
});

source.onmessage = (e) => {
  const event = JSON.parse(e.data);
  console.log(event.event, event.row);
};
```
