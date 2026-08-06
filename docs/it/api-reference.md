# Riferimento API

## Panoramica

Provisa espone endpoint REST sotto due prefissi: `/data` per l'esecuzione delle query e l'introspezione dello schema, e `/admin` per la gestione della configurazione. (REQ-043) La maggior parte degli endpoint dati richiede un identificatore di ruolo. Le operazioni di configurazione admin usano un'API GraphQL Strawberry su `/admin/graphql`. (REQ-164)

---

## Autenticazione

Quando `auth.provider` è configurato in `provisa.yaml`, tutti gli endpoint tranne `/health` e `/setup/status` richiedono un header `Authorization: Bearer <token>`. (REQ-120) [tool-verified: `provisa/api/app.py`, `provisa/auth/wiring.py`]

Senza autenticazione configurata, il server gira in modalità dev. Ogni richiesta viene trattata come l'identità `anonymous`, che mappa a tutti i ruoli configurati con accesso wildcard ai domini. (REQ-535)

**Login (`POST /auth/login`)** è fornito dal provider di autenticazione attivo quando è configurato `provider: basic`. (REQ-124) Il formato delle credenziali e la risposta dipendono dal provider.

**Introspezione dell'identità:**

```http
GET /auth/me
```

Restituisce id, email, nome visualizzato, appartenenze a org e assegnazioni di ruolo dell'utente autenticato. In modalità dev restituisce `dev_mode: true` con tutti gli ID di ruolo elencati. [tool-verified: `provisa/api/auth_router.py`]

```http
GET /auth/provider-type
```

Restituisce `{"provider": "<name>"}` o `{"provider": null}` quando l'autenticazione non è configurata. [tool-verified: `provisa/api/auth_router.py`]

---

## Endpoint dati

### `POST /data/graphql`

Esegue una query o mutation GraphQL. (REQ-043) [tool-verified: `provisa/api/data/endpoint.py:151`]

**Corpo della richiesta:**

```json
{
  "query": "{ orders(where: {region: {eq: \"us\"}}) { id amount } }",
  "variables": {},
  "role": "admin",
  "extensions": {}
}
```

Il campo `role` viene usato solo in modalità dev (senza autenticazione). Quando l'autenticazione è attiva, viene usato il ruolo dell'utente autenticato e il `role` nel corpo viene ignorato.

Il campo `extensions` supporta il protocollo Automatic Persisted Query (APQ): (REQ-288)

```json
{
  "extensions": {"persistedQuery": {"sha256Hash": "<sha256-of-query>"}}
}
```

**Header:**

- `X-Provisa-Role` — sovrascrive il ruolo (modalità dev)
- `Accept` — formato della risposta (vedi Content Negotiation)
- `Authorization` — `Bearer <token>` quando l'autenticazione è abilitata
- `X-Provisa-Redirect-Format` — tipo MIME per l'output di redirect S3 (REQ-137)
- `X-Provisa-Redirect-Threshold` — conteggio righe sopra il quale il redirect scatta (REQ-137)
- `X-Provisa-Redirect` — `true` per forzare il redirect incondizionatamente (REQ-029)

**Risposta (JSON inline):**

```json
{
  "data": {
    "orders": [
      {"id": 1, "amount": 99.99}
    ]
  }
}
```

**Risposta (redirect):**

```json
{
  "data": {"orders": null},
  "redirect": {
    "redirect_url": "https://...",
    "row_count": 50000,
    "expires_in": 3600,
    "content_type": "application/vnd.apache.parquet"
  }
}
```

**Risposta (multi-root con inline/redirect misti):**

```json
{
  "data": {
    "orders": [{"id": 1}],
    "customers": null
  },
  "redirects": {
    "customers": {
      "redirect_url": "https://...",
      "row_count": 10000,
      "expires_in": 3600,
      "content_type": "application/vnd.apache.parquet"
    }
  }
}
```

Le query multi-root eseguono ogni campo radice indipendentemente. I campi sotto la soglia di redirect vengono restituiti inline; quelli sopra la soglia reindirizzano. La chiave `redirects` (plurale) mappa i nomi dei campi alle informazioni di redirect. (REQ-029) [tool-verified: `provisa/api/data/endpoint.py`]

**Header di cache:**

- `X-Provisa-Cache: HIT|MISS` (REQ-536)
- `X-Provisa-Cache-Age: <seconds>` (su HIT) (REQ-536)

**Capability richieste:** `QUERY_DEVELOPMENT` per tutte le richieste, inclusa l'introspezione. [tool-verified: `provisa/api/data/endpoint.py:186-283`]

---

### Content Negotiation

| Header Accept | Formato |
| --- | --- |
| `application/json` | JSON (default) |
| `application/x-ndjson` | JSON delimitato da newline |
| `text/csv` | CSV |
| `application/vnd.apache.parquet` | Parquet |
| `application/vnd.apache.arrow.stream` | Arrow IPC |

(REQ-047, REQ-048, REQ-049, REQ-050) [tool-verified: `provisa/api/data/endpoint.py:84-90`]

---

### Redirect

I risultati sopra una soglia di righe configurata (o quando `X-Provisa-Redirect: true`) vengono scritti su S3 e viene restituito un URL presigned. (REQ-029, REQ-044)

| Formato redirect | Scritto da | Memoria |
| --- | --- | --- |
| `application/vnd.apache.parquet` | CTAS federato | Nessuna — i dati non passano mai attraverso Provisa |
| `application/x-orc` | CTAS federato | Nessuna — i dati non passano mai attraverso Provisa |
| `application/json` | Provisa | Vincolato dalla memoria |
| `application/x-ndjson` | Provisa | Vincolato dalla memoria |
| `text/csv` | Provisa | Vincolato dalla memoria |
| `application/vnd.apache.arrow.stream` | Provisa | Vincolato dalla memoria |

Per grandi export analitici, usa il redirect Parquet o ORC. Il motore di federazione scrive direttamente su S3 in parallelo — nessun dato passa attraverso Provisa. (REQ-138)

```yaml
X-Provisa-Redirect-Format: application/vnd.apache.parquet
X-Provisa-Redirect-Threshold: 1000
```

---

### `POST /data/sql`

Esegue SQL grezzo attraverso la pipeline di governance Stage 2. (REQ-267) [tool-verified: `provisa/api/data/endpoint_dev.py:62`]

**Corpo della richiesta:**

```json
{
  "sql": "SELECT id, amount FROM orders WHERE region = 'us'",
  "role": "admin"
}
```

**Capability richieste:** `QUERY_DEVELOPMENT`.

Le violazioni di governance su `POST /data/sql` restituiscono HTTP 403. (REQ-002, REQ-266)

**Risposta:** Stesso formato di `/data/graphql` (righe JSON di default, negoziato tramite `Accept`).

---

### `POST /data/query`

Endpoint di query unificato. Accetta GraphQL, SQL o Cypher — la sintassi viene rilevata automaticamente. (REQ-267) [tool-verified: `provisa/api/data/endpoint_dev.py:509`]

Le query Cypher possono anche essere inviate all'endpoint dedicato `POST /query/cypher`. (REQ-345)

**Corpo della richiesta:**

```json
{
  "query": "{ orders { id } }",
  "params": {},
  "variables": {},
  "role": "admin"
}
```

Restituisce `{"data": ...}` per GraphQL, `{"columns": [...], "rows": [...]}` per SQL e Cypher.

---

### `GET /data/rest/{domain_id}/{table_name}`

Endpoint REST semplice auto-generato per ogni tabella registrata. La query string mappa agli argomenti GraphQL e la richiesta viene compilata ed eseguita attraverso la stessa pipeline (RLS, mascheramento, routing) di GraphQL. (REQ-256) [tool-verified: `provisa/api/rest/generator.py:153`]

**Parametri di query:**

- `limit` — righe massime (≥ 1)
- `offset` — righe da saltare (≥ 0)
- `fields` — nomi di colonna separati da virgola (default a tutti i campi scalari)
- `filter` — array JSON di oggetti filtro `{"field", "comparator", "value"}`
- `orderBy` — array JSON di oggetti di ordinamento `{"field", "direction"}`

Il ruolo autenticato è richiesto; le richieste non autenticate restituiscono `401`. Una spec OpenAPI per queste route è servita su `GET /data/rest/openapi.json` con Swagger UI su `GET /data/rest/docs`.

---

### `GET /data/jsonapi/{domain_id}/{table_name}`

Endpoint conforme a [JSON:API](https://jsonapi.org) auto-generato per ogni tabella registrata. Stessi RLS, mascheramento e routing di GraphQL. (REQ-257) [tool-verified: `provisa/api/jsonapi/generator.py:284`]

**Header `Accept`:** deve includere `application/vnd.api+json` (il media type JSON:API) o la richiesta restituisce `406`.

**Parametri di query:**

- `fields[<type>]` — sparse fieldset, es. `?fields[orders]=amount`
- `filter[<col>]` / `filter[<col>][<op>]` — es. `?filter[region]=US`, `?filter[amount][gt]=100`
- `sort` — separati da virgola, prefisso `-` per discendente, es. `?sort=-created_at,amount`
- `page[number]` / `page[size]` — paginazione

Le risposte sono resource object con `type`/`id`/`attributes`. Gli errori seguono la forma dell'error object JSON:API.

---

### `POST /query/nl`

Invia una domanda in linguaggio naturale. Il servizio avvia un job asincrono e restituisce immediatamente `202 Accepted` con un `job_id`. Richiede un provider LLM configurato nella sezione di configurazione `ai_models`. (REQ-354) [tool-verified: `provisa/api/rest/nl_router.py:50`]

**Corpo della richiesta:**

```json
{"q": "How many orders were placed last month?", "role": "admin"}
```

Restituisce `{"job_id": "<id>"}`. Superare il rate limit NL per ruolo restituisce `429` con un header `Retry-After`. (REQ-370)

**Recuperare il risultato:**

- `GET /query/nl/{job_id}` — polling. Restituisce il documento del job.
- `GET /query/nl/{job_id}/stream` — SSE. Un evento `branch` per ogni target di generazione al completamento, poi un evento `done`. (REQ-357, REQ-358)

Tre cicli di generazione (Cypher, GraphQL, SQL) girano in parallelo, ciascuno validato attraverso il compilatore e raffinato in caso di errore. (REQ-355) Il prompt è vincolato allo schema visibile del ruolo. (REQ-356) Il documento del risultato indicizza ogni branch per target: (REQ-357) [tool-verified: `provisa/nl/job.py:69`]

```json
{
  "job_id": "<id>",
  "state": "complete",
  "branches": {
    "cypher":  {"query": "MATCH ...", "result": [...], "error": null},
    "graphql": {"query": "{ ... }",   "result": {...}, "error": null},
    "sql":     {"query": "SELECT ...", "result": [...], "error": null}
  }
}
```

Un branch che esaurisce il proprio limite di iterazioni restituisce `query: null`, `result: null` ed una stringa `error`. Ogni query generata viene eseguita sotto i diritti del consumatore con la governance Stage 2 applicata — il servizio non bypassa mai la governance. (REQ-359)

---

### `GET /data/sdl`

Restituisce l'SDL GraphQL per lo schema di un ruolo. (REQ-008) [tool-verified: `provisa/api/data/sdl.py:137`]

**Header:** `X-Role: <role_id>` (richiesto)

**Parametri di query:**

- `domain` — ID di dominio separati da virgola. Quando impostato, la risposta viene filtrata al/i dominio/i indicato/i e alle tabelle raggiungibili da essi.

**Risposta:** SDL GraphQL `text/plain`.

---

### `GET /data/introspection`

Restituisce il JSON di introspezione GraphQL, opzionalmente filtrato per dominio. [tool-verified: `provisa/api/data/sdl.py:200`]

**Header:** `X-Provisa-Role: <role_id>` (richiesto)

**Parametri di query:** `domain` — ID di dominio separati da virgola.

**Risposta:** risultato di introspezione `application/json`.

---

### `GET /data/graph-schema`

Restituisce la vista a grafo dello schema del ruolo: label dei nodi e i loro tipi di relazione, per client Cypher/grafo. Include `pk_columns` per label di nodo così i chiamanti possono determinare le colonne di chiave primaria. (REQ-398) [tool-verified: `provisa/api/rest/cypher_router.py:689`]

**Risposta:** `application/json` con `node_labels` (ciascuno con `pk`/`pk_columns`) e `relationship_types`.

---

### `GET /data/domains`

Restituisce gli ID di dominio accessibili al ruolo richiedente. [tool-verified: `provisa/api/data/sdl.py:116`]

**Header:** `X-Role: <role_id>` (richiesto)

**Risposta:** `["sales", "support", ...]`

---

### `GET /data/schema-version`

Restituisce la stringa di versione dello schema corrente. Combina un nonce per-boot con un contatore di rebuild. I client la usano per invalidare le cache di schema dopo i riavvii del server. (REQ-537) [tool-verified: `provisa/api/data/sdl.py:102`]

**Risposta:** `{"version": "<boot-id>-<counter>"}`

---

### `GET /data/proto/{role_id}`

Restituisce il file `.proto` auto-generato per un ruolo. [tool-verified: `provisa/api/data/endpoint_dev.py:49`]

**Risposta:** schema protobuf `text/plain`.

Ogni tabella registrata produce un `message` proto. Le relazioni producono campi di messaggio annidati. Mappatura dei tipi: `integer → int32`, `bigint → int64`, `varchar → string`, `decimal → double`, `boolean → bool`, `timestamp → google.protobuf.Timestamp`. (REQ-538)

---

### `GET /data/subscribe/{table}`

Stream Server-Sent Events per notifiche di cambiamento in tempo reale da una tabella. (REQ-219, REQ-258) [tool-verified: `provisa/api/data/subscribe.py:239`]

La consegna delle notifiche usa un provider collegabile scelto per tipo di origine: le origini PostgreSQL usano `LISTEN/NOTIFY` (via asyncpg), le origini MongoDB usano Change Streams (`collection.watch()`), e le origini Kafka usano consumer group. Ogni provider implementa un'interfaccia async comune di watch. Il filtraggio RLS e la validazione dello schema si applicano indipendentemente dal provider. (REQ-258) Sono supportate anche le origini WebSocket e RSS. (REQ-338, REQ-342)

**Header — `X-Provisa-Sink`:** Imposta su un target Kafka (es. `kafka://broker:9092/topic`) per reindirizzare gli eventi di cambiamento verso un sink Kafka invece della risposta SSE. Il server avvia un consumer sink e restituisce `202 Accepted` invece di uno stream aperto. (REQ-812) [tool-verified: `provisa/api/data/subscription_sse.py:137`]

---

## Endpoint REST admin

### Config

#### `GET /admin/config`

Scarica il `provisa.yaml` corrente come `application/x-yaml` con un header `Content-Disposition: attachment`. (REQ-164) [tool-verified: `provisa/api/admin/settings_router.py:19`]

#### `PUT /admin/config`

Carica uno YAML di configurazione rivisto. Il server scrive un backup `.bak`, salva il nuovo file e ricarica tutti gli schemi, le origini e le viste materializzate. (REQ-164) [tool-verified: `provisa/api/admin/settings_router.py:32`]

**Corpo della richiesta:** contenuto YAML grezzo.

**Risposta:**

```json
{"success": true, "message": "Config uploaded and reloaded"}
```

In caso di fallimento del reload: `{"success": false, "message": "<error>"}`.

---

### Settings

#### `GET /admin/settings`

Restituisce le impostazioni correnti della piattaforma come JSON. (REQ-165) [tool-verified: `provisa/api/admin/settings_router.py:50`]

**Risposta:**

```json
{
  "redirect": {
    "enabled": true,
    "threshold": 10000,
    "default_format": "application/vnd.apache.parquet",
    "ttl": 3600
  },
  "sampling": {
    "default_sample_size": 1000
  },
  "cache": {
    "default_ttl": 300
  },
  "naming": {
    "domain_prefix": false,
    "convention": "apollo_graphql"
  },
  "relationships": {
    "auto_track_fk": true
  },
  "otel": {
    "endpoint": "http://otel-collector:4318",
    "service_name": "provisa",
    "sample_rate": 1.0,
    "support_endpoint": "",
    "support_redact_sql_literals": true,
    "support_redact_attributes": []
  }
}
```

#### `PUT /admin/settings`

Aggiorna le impostazioni della piattaforma a runtime. Tutti i campi sono opzionali — vengono aggiornate solo le chiavi presenti nel corpo. (REQ-165) [tool-verified: `provisa/api/admin/settings_router.py:100`]

**Corpo della richiesta (esempio parziale):**

```json
{
  "otel": {
    "support_endpoint": "https://telemetry.vendor.com/v1/traces",
    "support_redact_sql_literals": true,
    "support_redact_attributes": ["db.statement", "user.email"]
  },
  "cache": {"default_ttl": 600}
}
```

Campi aggiornabili per sezione:

- `redirect`: `enabled`, `threshold`, `default_format`, `ttl`
- `sampling`: `default_sample_size`
- `cache`: `default_ttl`
- `naming`: `domain_prefix`, `convention` — scrive sul file di configurazione e innesca il reload dello schema (REQ-253)
- `relationships`: `auto_track_fk`
- `otel`: `endpoint`, `service_name`, `sample_rate`, `support_endpoint`, `support_redact_sql_literals`, `support_redact_attributes`

**Risposta:**

```json
{"success": true, "updated": ["otel.support_endpoint", "cache.default_ttl"]}
```

---

### Osservabilità

#### `GET /admin/traces/recent`

Restituisce fino a N span completati recenti dal buffer di span in memoria. (REQ-302) [tool-verified: `provisa/api/admin/settings_router.py:317`]

**Parametri di query:** `limit` (default 50, max 200)

**Risposta:** `{"traces": [...]}`

#### `POST /admin/query-engine/reload-catalog`

Ricarica a caldo un catalogo nominato nel coordinator del motore di federazione tramite la sua API REST. Riconnette la connessione interna di Provisa e riesegue il DDL OTel. [tool-verified: `provisa/api/admin/settings_router.py:208`]

**Parametri di query:** `catalog` (default `"otel"`)

**Risposta:**

```json
{"success": true, "errors": []}
```

#### `POST /admin/query-engine/restart`

Riavvia il container del motore di federazione (solo dev single-node). [tool-verified: `provisa/api/admin/settings_router.py:287`]

**Parametri di query:** `container` (default dalla variabile d'ambiente `QUERY_ENGINE_CONTAINER`, poi `"trino"`)

---

### Discovery

#### `POST /admin/discover/relationships`

Innesca la discovery delle relazioni. Esegue sempre l'introspezione delle FK dal motore di federazione. (REQ-018) Esegue l'inferenza LLM se `ANTHROPIC_API_KEY` è impostata. (REQ-167) [tool-verified: `provisa/api/admin/discovery.py:55`]

**Corpo della richiesta:**

```json
{
  "scope": "domain",
  "domain_id": "sales"
}
```

`scope` deve essere uno tra `"table"`, `"domain"`, `"cross-domain"`. Per scope `"table"`, `table_id` (integer) è richiesto. Per scope `"domain"`, `domain_id` è richiesto.

**Risposta:** `{"candidates_found": 12, "stored_ids": [1, 2, 3, ...]}`

#### `GET /admin/discover/candidates`

Elenca i candidati di relazione in attesa. [tool-verified: `provisa/api/admin/discovery.py:96`]

#### `POST /admin/discover/candidates/{candidate_id}/accept`

Accetta un candidato e lo registra come relazione. [tool-verified: `provisa/api/admin/discovery.py:103`]

**Corpo della richiesta (opzionale):** `{"name": "custom-relationship-name"}`

#### `POST /admin/discover/candidates/{candidate_id}/reject`

Rifiuta un candidato. [tool-verified: `provisa/api/admin/discovery.py:110`]

**Corpo della richiesta:** `{"reason": "Not a real join"}`

#### `GET /admin/discover/candidates/rejected/count`

Restituisce il conteggio dei candidati rifiutati. [tool-verified: `provisa/api/admin/discovery.py:118`]

#### `DELETE /admin/discover/candidates/rejected`

Elimina tutti i candidati rifiutati. [tool-verified: `provisa/api/admin/discovery.py:128`]

---

### Crawl delle origini

#### `POST /admin/sources/crawl`

Esegue il crawl di un'origine dati per introspezionarne lo schema e registrare le tabelle. (REQ-012) [tool-verified: `provisa/api/admin/crawl_router.py:36`]

---

### Ricerca tabelle origine

#### `GET /admin/sources/{source_id}/tables/search`

Cerca tabelle disponibili (non ancora registrate) in un'origine per nome. [tool-verified: `provisa/api/admin/table_search_router.py:103`]

---

### Profilazione tabelle

#### `POST /admin/tables/{table_id}/profile`

Esegue un profilo di colonna su una tabella registrata — cardinalità, min/max, tassi di null. [tool-verified: `provisa/api/admin/table_profile_router.py:28`]

---

### Descrizioni delle origini

#### `POST /admin/source-meta/db-description`

Genera descrizioni assistite da LLM per le tabelle e colonne di un'origine. [tool-verified: `provisa/api/admin/source_meta_router.py:48`]

---

### Actions (funzioni e webhook)

Tutti gli endpoint sono sotto il prefisso `/admin/actions`. (REQ-205) [tool-verified: `provisa/api/admin/actions_router.py:24`]

Ogni invocazione — da GraphQL, SQL, Cypher, Bolt, Arrow Flight, MCP `run_sql` e Provisa gRPC — passa attraverso un unico executor governato che applica `writable_by` e la governance in modo uniforme. (REQ-1156) [tool-verified: `provisa/api/data/action_exec.py`] Vedi [docs/integrations.md](integrations.md#invocare-comandi-tra-protocolli) per la sintassi di chiamata per protocollo.

#### `GET /admin/actions`

Restituisce tutte le funzioni DB e i webhook tracciati. (REQ-242) [tool-verified: `provisa/api/admin/actions_router.py:104`]

**Risposta:**

```json
{
  "functions": [
    {
      "name": "random_python_set",
      "implKind": "python",
      "binding": {"callable": "demo.py_functions:random_dataset"},
      "returns": "",
      "returnSchema": {
        "type": "array",
        "items": {"type": "object", "properties": {"id": {"type": "integer"}, "region": {"type": "string"}}}
      },
      "arguments": [{"name": "rows", "type": "Int"}, {"name": "seed", "type": "Int"}],
      "visibleTo": ["admin"],
      "writableBy": [],
      "domainId": "pet-store",
      "description": "Demo Python command returning random rows",
      "kind": "query"
    }
  ],
  "webhooks": [
    {
      "name": "add-pet",
      "url": "https://petstore.example.com/pets",
      "method": "POST",
      "kind": "mutation",
      "approved": true
    }
  ]
}
```

Ogni oggetto webhook porta un booleano `approved`. Un webhook viene approvato quando uno steward esegue la sua richiesta di creazione (REQ-209); i webhook dichiarati in configurazione sono auto-approvati. Un webhook non approvato è registrato ma non esposto su nessuna superficie. [tool-verified: `provisa/api/admin/actions_router.py:124-131`]

#### `POST /admin/actions/functions`

Registra una funzione tracciata (command). (REQ-205) [tool-verified: `provisa/api/admin/actions_router.py:117`]

**Campi chiave:**

| Campo | Richiesto | Descrizione |
| --- | --- | --- |
| `name` | Sì | Nome univoco del command |
| `kind` | Sì | `"query"` → campo Query GraphQL; `"mutation"` → campo Mutation |
| `implKind` | No | Come viene eseguito il command — vedi tabella sotto (default `source_procedure`) |
| `binding` | No | Dettagli di connessione specifici per `implKind` (oggetto JSON) |
| `returnSchema` | No | JSON Schema `{type:"array", items:{type:"object", properties:{...}}}` — rende il command set-returning su ogni superficie |
| `arguments` | No | Definizioni di argomento `[{name, type}]`; l'ordine posizionale conta per i chiamanti SQL e Bolt |
| `visibleTo` | No | ID di ruolo che possono chiamare il command |
| `writableBy` | No | ID di ruolo autorizzati a invocarlo come mutation |
| `domainId` | No | Dominio per il posizionamento GraphQL e il controllo di accesso |

**Valori di `implKind`:**

| `implKind` | Cosa esegue | Campi `binding` |
| --- | --- | --- |
| `source_procedure` | Stored procedure su un'origine registrata (default) | `sourceId`, `schemaName`, `functionName` |
| `script` | Script lato server | `script` |
| `http` | Chiamata HTTP in uscita | `url`, `method` |
| `grpc` | Chiamata gRPC in uscita verso un server esterno | `target`, `method` |
| `python` | Callable Python ospitato da Provisa (REQ-885) | `callable` (es. `"demo.py_functions:random_dataset"`) |

I command demo `random_python_set` (`implKind: python`) e `random_grpc_set` (`implKind: grpc`) mostrano in pratica command set-returning con `returnSchema`; entrambi sono in `config/provisa-install.yaml`. [tool-verified: `config/provisa-install.yaml:809-856`]

#### `PUT /admin/actions/functions/{name}`

Aggiorna una funzione tracciata per nome. [tool-verified: `provisa/api/admin/actions_router.py:182`]

#### `DELETE /admin/actions/functions/{name}`

Elimina una funzione tracciata per nome. [tool-verified: `provisa/api/admin/actions_router.py:233`]

#### `POST /admin/actions/webhooks`

Registra un webhook tracciato. (REQ-209) Registrare o aggiornare un webhook accoda una richiesta di approvazione dello steward — il webhook diventa attivo su tutte le superfici solo dopo l'approvazione di uno steward. I webhook dichiarati in configurazione sono auto-approvati. **Campi del corpo della richiesta:** `name`, `url`, `method`, `timeoutMs`, `returns`, `inlineReturnType`, `arguments`, `visibleTo`, `domainId`, `description`, `kind`. [tool-verified: `provisa/api/admin/actions_router.py:132`, `provisa/api/admin/actions_router.py:325-331`]

#### `PUT /admin/actions/webhooks/{name}`

Aggiorna un webhook tracciato per nome. Qualsiasi modifica riporta l'approvazione a pending fino a nuova approvazione. [tool-verified: `provisa/api/admin/actions_router.py:306`]

#### `DELETE /admin/actions/webhooks/{name}`

Elimina un webhook tracciato per nome. [tool-verified: `provisa/api/admin/actions_router.py:355`]

#### `POST /admin/actions/test`

Testa un'azione (funzione o webhook) per nome. (REQ-245) [tool-verified: `provisa/api/admin/actions_router.py:384`]

---

### Ruoli

Tutti gli endpoint sono sotto il prefisso `/admin/roles`. [tool-verified: `provisa/api/admin/roles_router.py:18`]

| Metodo | Path | Descrizione |
| --- | --- | --- |
| `GET` | `/admin/roles/` | Elenca tutti i ruoli |
| `POST` | `/admin/roles/` | Crea un ruolo |
| `PUT` | `/admin/roles/{role_id}` | Aggiorna un ruolo |
| `DELETE` | `/admin/roles/{role_id}` | Elimina un ruolo |

[tool-verified: `provisa/api/admin/roles_router.py`]

---

### Utenti

Tutti gli endpoint sono sotto il prefisso `/admin/users`. [tool-verified: `provisa/api/admin/local_users_router.py:21`]

| Metodo | Path | Descrizione |
| --- | --- | --- |
| `POST` | `/admin/users/` | Crea un utente locale |
| `GET` | `/admin/users/` | Elenca gli utenti locali |
| `GET` | `/admin/users/{user_id}` | Ottiene un utente |
| `PUT` | `/admin/users/{user_id}` | Aggiorna un utente |
| `PATCH` | `/admin/users/{user_id}/password` | Cambia la password |
| `DELETE` | `/admin/users/{user_id}` | Elimina un utente |
| `GET` | `/admin/users/{user_id}/assignments` | Elenca le assegnazioni di ruolo |
| `POST` | `/admin/users/{user_id}/assignments` | Aggiunge un'assegnazione di ruolo |
| `DELETE` | `/admin/users/{user_id}/assignments/{assignment_id}` | Rimuove un'assegnazione di ruolo |

---

### Organizzazioni

Tutti gli endpoint sono sotto `/admin/orgs`. [tool-verified: `provisa/api/admin/orgs_router.py:18`]

| Metodo | Path | Descrizione |
| --- | --- | --- |
| `GET` | `/admin/orgs/` | Elenca le org |
| `POST` | `/admin/orgs/` | Crea un'org |
| `PUT` | `/admin/orgs/{org_id}` | Aggiorna un'org |
| `DELETE` | `/admin/orgs/{org_id}` | Elimina un'org |
| `GET` | `/admin/orgs/{org_id}/members` | Elenca i membri |
| `POST` | `/admin/orgs/{org_id}/members` | Aggiunge un membro |
| `DELETE` | `/admin/orgs/{org_id}/members/{user_id}` | Rimuove un membro |

---

### Inviti

Tutti gli endpoint sono sotto `/admin/invites`. [tool-verified: `provisa/api/admin/invites_router.py:18`]

| Metodo | Path | Descrizione |
| --- | --- | --- |
| `POST` | `/admin/invites/` | Crea un invito |
| `GET` | `/admin/invites/` | Elenca gli inviti in attesa |
| `DELETE` | `/admin/invites/{token}` | Revoca un invito |

---

### GraphQL admin

#### `POST /admin/graphql`

Endpoint GraphQL Strawberry per tutte le operazioni admin: CRUD di origini e tabelle, gestione delle relazioni, configurazione dei domini, regole RLS, controllo della cache, convenzioni di naming, gestione dei task pianificati e compilazione delle query. (REQ-164) [tool-verified: `provisa/api/app.py:2171`]

**Mutation chiave:**

```graphql
# Cache
mutation { update_source_cache(source_id: "sales-pg", enabled: true, ttl: 600) { success } }
mutation { update_table_cache(table_id: 1, ttl: 60) { success } }

# Naming conventions
mutation { update_source_naming(source_id: "legacy-db", convention: "camelCase") { success } }
mutation { update_table_naming(table_id: 1, convention: "PascalCase") { success } }

# Scheduled tasks
mutation { toggle_scheduled_task(name: "daily-report", enabled: false) { success } }

# Compile a query (returns enforcement metadata and routed SQL)
mutation {
  compile_query(input: {role: "admin", query: "{ orders { id } }"}) {
    sql semantic_sql trino_sql direct_sql route route_reason sources root_field
    enforcement { rls_filters_applied columns_excluded masking_applied }
  }
}
```

[tool-verified: `provisa/api/admin/schema.py`, `provisa/api/admin/actions_router.py`]

---

### Setup

#### `GET /setup/status`

Restituisce lo stato del setup al primo avvio. Sempre non autenticato. (REQ-539) [tool-verified: `provisa/api/setup_router.py:100`]

#### `POST /setup/`

Completa il setup al primo avvio. [tool-verified: `provisa/api/setup_router.py:142`]

---

## Health Check

#### `GET /health` o `HEAD /health`

Restituisce `{"status": "ok"}`. Sempre non autenticato. (REQ-539) [tool-verified: `provisa/api/app.py:2258`]

---

## Risposte di errore

| Stato | Significato |
| --- | --- |
| 400 | Query non valida, errore di validazione o errore di parsing SQL |
| 401 | Token di autenticazione mancante o non valido |
| 403 | Capability insufficienti; violazione di governance |
| 404 | Ruolo, risorsa o file di configurazione non trovato |
| 422 | Header richiesto mancante (es. `X-Role`) |
| 503 | Database o origine non connessi; dipendenza non disponibile |
| 504 | Richiesta scaduta per timeout |

Le violazioni di governance su `POST /data/sql` restituiscono HTTP 403 con un corpo strutturato: (REQ-002) [tool-verified: `provisa/api/data/endpoint_dev.py:184-190`]

```json
{
  "detail": {
    "violations": [
      {"code": "V000", "message": "Table 'orders' is not accessible for role 'analyst'"}
    ]
  }
}
```

Tutti gli altri errori usano: `{"detail": "<message>"}`.

---

## Endpoint Arrow Flight

Porta `8815`. Trasporto columnar Arrow nativo su gRPC. (REQ-143, REQ-045) [tool-verified: `provisa/api/flight/server.py`]

Query e discovery del catalogo sono entrambe disponibili sulla stessa connessione. La pipeline di governance completa (RLS, mascheramento, campionamento) viene applicata a ogni query. (REQ-130, REQ-143)

**Formato del ticket** (JSON):

```json
{"query": "{ customers { name email } }", "role": "analyst", "variables": {}}
```

**Utilizzo (Python):**

```python
import pyarrow.flight as flight

client = flight.FlightClient("grpc://localhost:8815")
ticket = flight.Ticket(b'{"query": "{ orders { id amount } }", "role": "admin"}')
# Stream batch-by-batch
for batch in client.do_get(ticket):
    process(batch.data)
# Or read all at once
table = client.do_get(ticket).read_all()
```

Quando il proxy Zaychik Flight SQL è disponibile (porta 8480), i record batch vengono trasmessi in streaming end-to-end senza materializzazione completa. (REQ-144) Ricade sulla materializzazione tramite il layer di query federata se Zaychik non è disponibile. (REQ-146)

---

## Endpoint gRPC Protobuf

Porta `50051` (sovrascrivibile con la variabile d'ambiente `GRPC_PORT` o la configurazione `server.grpc_port`). (REQ-529) [tool-verified: `provisa/grpc/server.py`, `provisa/api/app.py`]

Passa il ruolo nella chiave di metadata gRPC `x-provisa-role`. Se assente, il server abortisce con `UNAUTHENTICATED`. [tool-verified: `provisa/grpc/server.py`]

Scarica il proto specifico per ruolo da `GET /data/proto/{role_id}`. Compaiono solo le tabelle e colonne visibili a quel ruolo. (REQ-039)

```proto
service ProvisaService {
  rpc QueryOrders (QueryOrdersRequest) returns (stream Orders);
  rpc InsertOrders (InsertOrdersRequest) returns (InsertOrdersResponse);
}
```

Ogni tabella produce un RPC di streaming `Query{TypeName}`. Gli RPC `Insert{TypeName}` esistono per simmetria dello schema ma abortiscono con `UNIMPLEMENTED`. [tool-verified: `provisa/grpc/server.py`]

`grpc_reflection.v1alpha` è abilitato per la discovery del servizio senza un proto precompilato. (REQ-529) [tool-verified: `provisa/grpc/reflection.py`]

```bash
grpcurl -plaintext localhost:50051 list
grpcurl -plaintext -H 'x-provisa-role: analyst' \
  -d '{}' localhost:50051 ProvisaService/QueryOrders
```

Il server gRPC si avvia solo quando un proto valido può essere compilato all'avvio. Se il build dello schema fallisce, il server gRPC non si avvia. (REQ-529)

---

## Driver JDBC

Il driver JDBC Provisa (`provisa-jdbc-0.1.0.jar`) espone il catalogo semantico a strumenti BI (Tableau, PowerBI, DBeaver). (REQ-126)

**URL di connessione:** `jdbc:provisa://host:port` (REQ-131)

I domini mappano a schemi JDBC. (REQ-127) Le tabelle usano i loro alias registrati. Le colonne usano gli alias e mostrano le descrizioni come `REMARKS`. (REQ-128) I metodi metadata standard (`getPrimaryKeys`, `getImportedKeys`, `getExportedKeys`) espongono le relazioni semantiche come metadata PK/FK.

**Supporto SQL:** `SELECT * FROM <alias> [WHERE col = 'value']`. (REQ-129)

Il driver richiede di default il redirect Arrow IPC. I risultati vengono trasmessi batch-by-batch tramite `ArrowStreamReader`, vincolati a un record batch in memoria. (REQ-293)

---

## Formato dell'argomento `orderBy`

L'argomento `order_by` usa oggetti `{column: direction}` con un enum di direzione a 6 valori: (REQ-200)

```json
{
  "query": "{ orders(order_by: [{created_at: desc_nulls_last}]) { id created_at } }",
  "role": "admin"
}
```

Direzioni supportate: `asc`, `desc`, `asc_nulls_first`, `asc_nulls_last`, `desc_nulls_first`, `desc_nulls_last`. (REQ-201)

---

## Subscription

Le subscription SSE sono disponibili su `GET /data/subscribe/{table}`. (REQ-219, REQ-258) La consegna delle notifiche usa un provider collegabile selezionato per tipo di origine: le origini PostgreSQL usano `LISTEN/NOTIFY`, le origini MongoDB usano Change Streams, e le origini Kafka usano consumer group. Il filtraggio RLS e la validazione dello schema si applicano indipendentemente dal provider. Sono supportate anche le origini WebSocket e RSS tramite lo stesso endpoint. (REQ-338, REQ-342) [tool-verified: `provisa/api/data/subscribe.py:239`, `provisa/subscriptions/registry.py`, `provisa/api/app.py` `_rebuild_schemas`]
