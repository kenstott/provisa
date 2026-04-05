# SSE Subscriptions

Provisa supports real-time push over Server-Sent Events (SSE). Clients receive a stream of change events without polling.

## Endpoint

```
POST /data/subscribe
Content-Type: application/json
Accept: text/event-stream
```

Request body:
```json
{
  "query": "{ orders { id amount region } }",
  "role": "analyst",
  "mode": "listen"
}
```

The connection stays open and emits one JSON event per change:
```
data: {"event":"update","table":"orders","row":{"id":42,"amount":199.00,"region":"west"}}

data: {"event":"insert","table":"orders","row":{"id":43,"amount":55.00,"region":"east"}}
```

## Delivery Modes

| Mode | Config key | Mechanism | Best for |
|------|-----------|-----------|---------|
| `listen` | `mode: listen` | PostgreSQL `LISTEN`/`NOTIFY` | PG sources with write activity |
| `poll` | `mode: poll` | Re-execute query on interval | Non-PG sources, or when CDC unavailable |
| `cdc` | `mode: cdc` | Kafka topic from Debezium connector | High-frequency change streams |

### LISTEN/NOTIFY

Provisa issues `LISTEN <channel>` on a persistent PG connection. Your application (or Provisa mutations) must call `NOTIFY <channel>, '<payload>'` after writes. No additional infrastructure required.

### Polling

Provisa re-executes the subscription query on a configurable interval and emits a diff (inserts, updates, deletes since last poll).

```json
{
  "query": "{ orders { id amount } }",
  "role": "analyst",
  "mode": "poll",
  "poll_interval_ms": 5000
}
```

### Debezium CDC

Requires a running Debezium connector writing to Kafka. Provisa consumes the Kafka topic and forwards change events to connected SSE clients. Latency is typically sub-second.

Configure the Debezium topic in `config.yaml`:
```yaml
sources:
  - id: sales-pg
    tables:
      - id: orders
        cdc_topic: debezium.public.orders
```

## Security

All subscription modes enforce the same security pipeline as regular queries:
- RLS filters are applied to every emitted row
- Masked columns appear masked in events
- Role authorization is checked at connection time

A client whose role loses access mid-stream receives a `{"event":"unauthorized"}` event and the connection closes.

## Client Example

```javascript
const source = new EventSource('/data/subscribe', {
  method: 'POST',
  headers: { 'Content-Type': 'application/json' },
  body: JSON.stringify({ query: '{ orders { id amount } }', role: 'analyst', mode: 'listen' })
});

source.onmessage = (e) => {
  const event = JSON.parse(e.data);
  console.log(event.event, event.row);
};
```
