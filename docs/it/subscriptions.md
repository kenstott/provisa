# Subscription SSE {: #sse-subscriptions }

Provisa supporta il push in tempo reale tramite Server-Sent Events (SSE). I client ricevono un flusso di eventi di modifica senza polling.

## Origini {: #sources }

Le subscription hanno come target una **tabella registrata**:

| Origine | Valori di `strategy` disponibili |
| -------- | ------------------------- |
| Tabella (PostgreSQL) | `native` (LISTEN/NOTIFY), `poll` |
| Tabella (RDBMS non PG con un blocco `cdc` a livello di origine) | `debezium`, `kafka`, `poll` |
| Tabella (vista federata / qualsiasi altra origine) | solo `poll` |

### Installazione automatica dei trigger PostgreSQL {: #postgresql-trigger-auto-installation }

All'avvio, Provisa installa automaticamente trigger `AFTER INSERT OR UPDATE OR DELETE` su tutte le tabelle PostgreSQL **preapprovate**. Questi trigger richiamano `pg_notify('provisa_{table}', ...)` in modo che le DML non elaborate (non solo le mutazioni di Provisa) vengano rilevate dalle subscription.

Se l'installazione del trigger non riesce (ad esempio per privilegi insufficienti — il ruolo del database deve essere proprietario della tabella), Provisa passa al polling basato su watermark per quella tabella, purché sia configurato un `watermark_column`. Viene registrato un avviso nel log.

### Subscription su viste multi-origine {: #cross-datasource-view-subscriptions }

Per le viste che uniscono più origini dati tramite il motore di federazione, aggiungere un `watermark_column` alla registrazione della tabella. La colonna deve esistere nell'SQL della vista (non è necessario che compaia nello schema GraphQL):

```sql
-- Example: federated view with derived watermark
CREATE OR REPLACE VIEW orders_with_segments AS
SELECT o.*, s.name AS segment_name,
       GREATEST(o.updated_at, s.updated_at) AS _watermark
FROM postgresql.public.orders o
JOIN mysql.crm.customer_segments s ON o.customer_id = s.customer_id;
```

Registrare con `watermark_column: _watermark`. Provisa esegue il polling usando `WHERE _watermark > <last_seen>`.

### Subscription su relazioni annidate {: #nested-relationship-subscriptions }

Quando il campo della subscription seleziona campi da tabelle unite tramite join (attraverso relazioni registrate), Provisa monitora **tutte** le tabelle fisiche coinvolte simultaneamente. Una modifica in una qualsiasi tabella unita fa riscattare la query di subscription.

## Endpoint

Iscriversi a una tabella:

```http
GET /data/subscribe/{table}
Accept: text/event-stream
```

La connessione resta aperta ed emette un evento JSON per ogni modifica:

```text
data: {"event":"insert","table":"orders","row":{"id":43,"amount":55.00,"region":"east"}}

data: {"event":"update","table":"orders","row":{"id":42,"amount":199.00,"region":"west"}}
```

## Modalità di consegna {: #delivery-modes }

La consegna viene selezionata tramite `live.strategy` nella configurazione della tabella:

| `strategy` | Meccanismo | Disponibile per | Richiede |
| ------------ | ----------- | --------------- | --------- |
| `native` | `LISTEN`/`NOTIFY` di PostgreSQL, Change Streams di MongoDB | PG, MongoDB | Nient'altro |
| `debezium` | Topic Kafka dal connettore Debezium | Tabelle RDBMS non PG | Blocco `cdc` a livello di origine (Debezium + Kafka) |
| `kafka` | Topic delta Kafka arbitrario | Qualsiasi tabella alimentata da Kafka | Blocco `cdc` a livello di origine |
| `poll` | Polling basato su watermark | Qualsiasi tabella con watermark | `watermark_column` |

### LISTEN/NOTIFY

Provisa emette `LISTEN <channel>` su una connessione PG persistente. Le mutazioni di Provisa attivano automaticamente `NOTIFY`. I writer esterni devono richiamare `NOTIFY <channel>, '<payload>'` dopo ogni scrittura. Non è richiesta infrastruttura aggiuntiva.

### Polling

Provisa riesegue periodicamente la query di origine, selezionando solo le righe in cui `watermark_column > last_watermark`. Le differenze vengono emesse come eventi SSE. Il polling non è in grado di rilevare le eliminazioni definitive (hard delete) — una riga rimossa non lascia alcun watermark che avanzi. Per rendere visibile un'eliminazione, utilizzare un'eliminazione logica (soft delete) (ad esempio impostando un flag `deleted_at`) che incrementi la colonna watermark; l'eliminazione arriva quindi come evento di aggiornamento che porta con sé il marcatore di eliminazione logica.

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

Richiede un connettore Debezium in esecuzione che scriva su Kafka. Provisa consuma il topic Kafka e inoltra gli eventi di modifica ai client SSE connessi.

Il trasporto CDC viene configurato una sola volta per origine in un blocco `cdc`; i topic vengono derivati come `{topic_prefix}.{schema}.{table}` e non vengono mai ripetuti per tabella. Ogni tabella seleziona quindi `strategy: debezium`:

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

#### Codifiche dei messaggi {: #message-encodings }

Un topic Debezium viene letto in una di due codifiche, e ogni messaggio dice quale.

| Codifica | Cosa porta il topic | Cosa serve alla sorgente |
|---|---|---|
| JSON | L'involucro del convertitore JSON di Debezium, con o senza l'involucro `schema`/`payload` | Nulla oltre al blocco `cdc` |
| Avro | Il formato wire dello schema registry di Confluent: un byte zero, un id di schema di quattro byte, un dato Avro | `schema_registry_url`, e come autenticarsi presso di esso |

I messaggi Avro vengono decodificati con lo schema che il loro id nomina, recuperato una volta dal registry e conservato. Un topic il cui schema è evoluto viene letto correttamente, perché ogni messaggio nomina lo schema che lo ha scritto. I topic Protobuf e JSON Schema non vengono letti.

Un messaggio Avro non viene mai letto come JSON. Ogni modo in cui non si riesce a leggerne uno viene rifiutato per nome:

| Situazione | Rifiuto |
|---|---|
| Messaggio Avro, la sorgente non nomina alcun registry | `subscribe.avro_topic_without_registry`, con il topic |
| Registry non disponibile o che non risponde in tempo | `subscribe.schema_registry_unreachable` |
| Il registry rifiuta le credenziali della sorgente | `subscribe.schema_registry_refused_credentials` |
| Id di schema che il registry non possiede | `subscribe.schema_id_unknown`, con l'id |
| L'id nomina uno schema Protobuf o JSON | `subscribe.schema_not_avro` |

Un registry non disponibile o che rifiuta le credenziali viene rifiutato all'avvio della sottoscrizione, prima che il flusso si apra. Una consultazione del registry attende al massimo 5 secondi per connettersi e 10 secondi per una risposta, e meno quando alla richiesta resta meno tempo.

#### Autenticazione dello schema registry {: #schema-registry-authentication }

`schema_registry_auth` nomina un metodo; i campi di cui quel metodo ha bisogno vengono controllati al salvataggio della sorgente.

| `schema_registry_auth` | Campi obbligatori | Inviato al registry |
|---|---|---|
| `none` (predefinito) | — | Nessuna credenziale |
| `basic` | `schema_registry_username`, `schema_registry_password` | Autenticazione HTTP basic |
| `bearer` | `schema_registry_token` | `Authorization: Bearer <token>` (un token statico) |
| `mtls` | `schema_registry_client_cert`, `schema_registry_client_key` | Il certificato client, nell'handshake TLS |

`schema_registry_ca` è facoltativo con ogni metodo: un bundle di CA di cui fidarsi quando il certificato del registry non è firmato da un'autorità pubblica.

La password e il token seguono il contratto dei segreti. Un valore digitato nel modulo della sorgente viene memorizzato nel vault dell'organizzazione e la sorgente conserva un riferimento `${secret:NAME}`; un riferimento scritto in un file di configurazione (`${env:...}`, `${secret:...}`) viene conservato così com'è. Il certificato, la chiave e il bundle di CA sono file che il processo server di Provisa apre, indicati con percorso assoluto.

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

## Quando un flusso termina con un errore {: #when-a-stream-ends-on-a-failure }

Un flusso che si è aperto ha già risposto `200`, quindi un errore successivo non può essere uno stato HTTP. Il sottoscrittore riceve un ultimo frame che dice perché, poi il flusso si chiude. Un flusso non si chiude mai per un errore senza quel frame.

Il frame porta ciò con cui lo stesso errore verrebbe risposto in una richiesta ordinaria:

| Errore | Corpo |
|---|---|
| Un rifiuto con un codice (quello di uno schema registry, di una regione, un rifiuto di governance) | `detail`, `code`, `params` |
| La scadenza della richiesta | `detail: "Request timed out"` |
| Qualsiasi cosa inattesa | `detail: "Internal server error"`, `type` (il nome del tipo dell'eccezione) |

Ogni corpo porta anche `subscription_id`. Per un errore inatteso, il messaggio e lo stack trace sono nel log del server sotto quell'id, non nel frame.

La sottoscrizione di tabella (`/data/subscribe/{table}`) invia il corpo come dati di un evento chiamato `stream_error`:

```text
event: stream_error
data: {"detail": "Topic 'debezium.app.orders' carries Avro messages, and its source names no schema registry to read them with: set the source's schema registry URL", "code": "subscribe.avro_topic_without_registry", "params": {"topic": "debezium.app.orders"}, "subscription_id": "5f0c2a9d41b7e386"}
```

L'evento non si chiama `error`, perché un `EventSource` genera il proprio evento `error` quando una connessione cade e un listener non potrebbe distinguere i due. Ascoltatelo per nome:

```javascript
source.addEventListener("stream_error", (e) => {
  const { detail, code, params } = JSON.parse(e.data);
  source.close();
});
```

Una sottoscrizione GraphQL mantiene la forma di risposta GraphQL, con il codice e i params sotto `extensions`:

```json
{"errors": [{"message": "…", "extensions": {"code": "subscribe.avro_topic_without_registry", "params": {"topic": "debezium.app.orders"}, "subscription_id": "5f0c2a9d41b7e386"}}]}
```

Un caso non termina una sottoscrizione GraphQL: quando la rilettura dopo una singola modifica fallisce, quel frame viene inviato nella stessa forma e il flusso prosegue con la modifica successiva. Un errore della lettura iniziale, del feed di modifiche stesso o del suo avvio è l'ultimo frame.

## Reindirizzamento a un sink Kafka {: #kafka-sink-redirect }

Qualsiasi subscription GraphQL può essere reindirizzata a un topic Kafka anziché essere trasmessa in streaming al client. Aggiungere l'header `X-Provisa-Sink` alla richiesta di subscription:

```yaml
POST /data/graphql
Authorization: Bearer <token>
Content-Type: application/json
X-Provisa-Sink: kafka://broker:9092/my-topic
```

Il server risponde immediatamente con `202 Accepted` e avvia un'attività in background che:

1. Monitora le modifiche alle tabelle usando la stessa risoluzione del provider di SSE (LISTEN/NOTIFY → polling asyncpg → polling federato)
2. Riesegue la query equivalente a ogni modifica
3. Pubblica il risultato come messaggio JSON nel topic Kafka indicato

Il sink resta attivo per l'intera durata del processo del server. Riavviare il server per interromperlo.

**Formato URI:** `kafka://[broker:port]/topic`

- Se `broker:port` viene omesso, viene usata la variabile d'ambiente `KAFKA_BOOTSTRAP_SERVERS` (valore predefinito: `localhost:9092`)
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

### Sink Kafka come seconda uscita a livello di configurazione {: #kafka-sink-as-a-config-level-second-output }

Una subscription di tabella basata su polling può pubblicare contemporaneamente su un topic Kafka tramite `provisa.yaml`. La subscription SSE e il sink Kafka sono entrambi output dello stesso Live Query Engine. Ogni output tiene traccia del proprio watermark in modo indipendente.

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

Un output Kafka pubblica le righe della tabella governate come il ruolo che nomina: le sue regole di riga, la visibilità delle colonne e le maschere si applicano come a una lettura di quel ruolo. Un output senza `role` viene rifiutato al caricamento della configurazione.

## Sicurezza {: #security }

Tutte le modalità di subscription applicano la stessa pipeline di sicurezza delle query ordinarie:

- I filtri di sicurezza a livello di riga vengono applicati a ogni riga emessa
- Le colonne mascherate appaiono mascherate negli eventi
- L'autorizzazione del ruolo viene verificata al momento della connessione

## Esempio client {: #client-example }

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
