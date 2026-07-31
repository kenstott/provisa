# Subscription SSE

Provisa supporta il push in tempo reale tramite Server-Sent Events (SSE). I client ricevono un flusso di eventi di modifica senza polling. (REQ-258)

## Origini

Le subscription hanno come target una **tabella registrata**:

| Origine | Valori di `strategy` disponibili |
|--------|-------------------------|
| Tabella (PostgreSQL) | `native` (LISTEN/NOTIFY), `poll` |
| Tabella (RDBMS non PG con un blocco `cdc` a livello di origine) | `debezium`, `kafka`, `poll` |
| Tabella (vista federata / qualsiasi altra origine) | solo `poll` |

### Installazione automatica dei trigger PostgreSQL

All'avvio, Provisa installa automaticamente trigger `AFTER INSERT OR UPDATE OR DELETE` su tutte le tabelle PostgreSQL **preapprovate**. (REQ-565) Questi trigger richiamano `pg_notify('provisa_{table}', ...)` in modo che le DML non elaborate (non solo le mutazioni di Provisa) vengano rilevate dalle subscription. (REQ-565)

Se l'installazione del trigger non riesce (ad esempio per privilegi insufficienti — il ruolo del database deve essere proprietario della tabella), Provisa passa al polling basato su watermark per quella tabella, purché sia configurato un `watermark_column`. (REQ-566) Viene registrato un avviso nel log. (REQ-566)

### Subscription su viste multi-origine

Per le viste che uniscono più origini dati tramite il motore di federazione, aggiungere un `watermark_column` alla registrazione della tabella. (REQ-260, REQ-283) La colonna deve esistere nell'SQL della vista (non è necessario che compaia nello schema GraphQL):

```sql
-- Example: federated view with derived watermark
CREATE OR REPLACE VIEW orders_with_segments AS
SELECT o.*, s.name AS segment_name,
       GREATEST(o.updated_at, s.updated_at) AS _watermark
FROM postgresql.public.orders o
JOIN mysql.crm.customer_segments s ON o.customer_id = s.customer_id;
```

Registrare con `watermark_column: _watermark`. Provisa esegue il polling usando `WHERE _watermark > <last_seen>`. (REQ-260)

### Subscription su relazioni annidate

Quando il campo della subscription seleziona campi da tabelle unite tramite join (attraverso relazioni registrate), Provisa monitora **tutte** le tabelle fisiche coinvolte simultaneamente. (REQ-567) Una modifica in una qualsiasi tabella unita fa riscattare la query di subscription. (REQ-567)

## Endpoint

Iscriversi a una tabella:
```
GET /data/subscribe/{table}
Accept: text/event-stream
```

La connessione resta aperta ed emette un evento JSON per ogni modifica: (REQ-258, REQ-568)
```
data: {"event":"insert","table":"orders","row":{"id":43,"amount":55.00,"region":"east"}}

data: {"event":"update","table":"orders","row":{"id":42,"amount":199.00,"region":"west"}}
```

## Modalità di consegna

La consegna viene selezionata tramite `live.strategy` nella configurazione della tabella: (REQ-813, REQ-814)

| `strategy` | Meccanismo | Disponibile per | Richiede |
|------------|-----------|---------------|---------|
| `native` | `LISTEN`/`NOTIFY` di PostgreSQL, Change Streams di MongoDB | PG, MongoDB | Nient'altro |
| `debezium` | Topic Kafka dal connettore Debezium | Tabelle RDBMS non PG | Blocco `cdc` a livello di origine (Debezium + Kafka) |
| `kafka` | Topic delta Kafka arbitrario | Qualsiasi tabella alimentata da Kafka | Blocco `cdc` a livello di origine |
| `poll` | Polling basato su watermark | Qualsiasi tabella con watermark | `watermark_column` |

### LISTEN/NOTIFY

Provisa emette `LISTEN <channel>` su una connessione PG persistente. (REQ-258) Le mutazioni di Provisa attivano automaticamente `NOTIFY`. (REQ-565) I writer esterni devono richiamare `NOTIFY <channel>, '<payload>'` dopo ogni scrittura. Non è richiesta infrastruttura aggiuntiva.

### Polling

Provisa riesegue periodicamente la query di origine, selezionando solo le righe in cui `watermark_column > last_watermark`. (REQ-260) Le differenze vengono emesse come eventi SSE. Il polling non è in grado di rilevare le eliminazioni definitive (hard delete) — una riga rimossa non lascia alcun watermark che avanzi. Per rendere visibile un'eliminazione, utilizzare un'eliminazione logica (soft delete) (ad esempio impostando un flag `deleted_at`) che incrementi la colonna watermark; l'eliminazione arriva quindi come evento di aggiornamento che porta con sé il marcatore di eliminazione logica. (REQ-260)

Configurazione del polling della tabella (in `provisa.yaml`):
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

Richiede un connettore Debezium in esecuzione che scriva su Kafka. (REQ-261) Provisa consuma il topic Kafka e inoltra gli eventi di modifica ai client SSE connessi. (REQ-261)

Il trasporto CDC viene configurato una sola volta per origine in un blocco `cdc`; i topic vengono derivati come `{topic_prefix}.{schema}.{table}` e non vengono mai ripetuti per tabella. (REQ-824) Ogni tabella seleziona quindi `strategy: debezium`:
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

## Reindirizzamento a un sink Kafka

Qualsiasi subscription GraphQL può essere reindirizzata a un topic Kafka anziché essere trasmessa in streaming al client. (REQ-812) Aggiungere l'header `X-Provisa-Sink` alla richiesta di subscription:

```
POST /data/graphql
Authorization: Bearer <token>
Content-Type: application/json
X-Provisa-Sink: kafka://broker:9092/my-topic
```

Il server risponde immediatamente con `202 Accepted` e avvia un'attività in background che: (REQ-812)
1. Monitora le modifiche alle tabelle usando la stessa risoluzione del provider di SSE (LISTEN/NOTIFY → polling asyncpg → polling federato)
2. Riesegue la query equivalente a ogni modifica
3. Pubblica il risultato come messaggio JSON nel topic Kafka indicato

Il sink resta attivo per l'intera durata del processo del server. (REQ-812) Riavviare il server per interromperlo (la registrazione persistente dei sink tramite l'API di amministrazione è prevista).

**Formato URI:** `kafka://[broker:port]/topic`

- Se `broker:port` viene omesso, viene usata la variabile d'ambiente `KAFKA_BOOTSTRAP_SERVERS` (valore predefinito: `localhost:9092`) (REQ-812)
- `topic` è obbligatorio

**Esempio (curl):**
```bash
curl -X POST http://localhost:8000/data/graphql \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -H "X-Provisa-Sink: kafka://kafka:9092/orders-live" \
  -d '{"query": "subscription { orders { id status amount } }"}'
# → 202 {"status":"streaming","sink":"kafka://kafka:9092/orders-live","table":"orders"}
```

### Sink Kafka come seconda uscita a livello di configurazione

Una subscription di tabella basata su polling può pubblicare contemporaneamente su un topic Kafka tramite `provisa.yaml`. (REQ-282, REQ-286) La subscription SSE e il sink Kafka sono entrambi output dello stesso Live Query Engine. (REQ-282) Ogni output tiene traccia del proprio watermark in modo indipendente. (REQ-286)

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

Per il riferimento completo di configurazione dei sink, vedere [Kafka Sinks](sources.md).

## Sicurezza

Tutte le modalità di subscription applicano la stessa pipeline di sicurezza delle query ordinarie: (REQ-258, REQ-038)

- I filtri di sicurezza a livello di riga vengono applicati a ogni riga emessa (REQ-040)
- Le colonne mascherate appaiono mascherate negli eventi (REQ-040)
- L'autorizzazione del ruolo viene verificata al momento della connessione (REQ-258)

## Esempio client

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
