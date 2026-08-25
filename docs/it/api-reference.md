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

### `POST /data/sql/explain`

Spiega o analizza un'istruzione SQL attraverso la pipeline governata. (REQ-1519) [tool-verified: `provisa/api/data/endpoint_dev.py:328`]

L'endpoint racchiude l'SQL **governato** — l'istruzione che gira davvero sotto il ruolo di chi chiama, dopo RLS e mascheramento — nella sintassi EXPLAIN del dialetto. Ciò che il piano mostra è la versione autorizzata della query, non l'input grezzo.

**Corpo della richiesta:**

```json
{
  "sql": "SELECT id, amount FROM orders",
  "role": "admin",
  "analyze": false
}
```

Impostare `analyze: true` per eseguire EXPLAIN ANALYZE. La query viene eseguita e il piano porta conteggi di righe e tempi reali. Non tutti i dialetti supportano ANALYZE; vedere la tabella in [Piani di query e statistiche](engines.md#query-plans-and-statistics).

**Risposta:** `{"plan": "<plan text or JSON>", "dialect": "trino", "analyzed": false}`

`400` quando il dialetto non supporta EXPLAIN, oppure quando `analyze: true` viene richiesto su un dialetto che non lo supporta (ad esempio SQLite). [tool-verified: `provisa/executor/explain.py:wrap_explain`, `analyze_sql`]

---

### `GET /data/engine/state`

Restituisce lo stato attuale dello shard del motore senza risvegliarlo. (REQ-1516) [tool-verified: `provisa/api/data/endpoint_dev.py:892`]

La UI interroga periodicamente questo endpoint per mostrare un banner di avvio mentre il motore è in fase di cold start. Non provoca mai un risveglio — l'interrogazione è sicura e non conta come attività per il reaper di inattività.

**Risposta:**

```json
{"state": "ready"}
```

Valori possibili:

| Stato | Significato |
| --- | --- |
| `always-on` | Desktop, self-hosted o coordinator proprio — nessuna gestione del ciclo di vita |
| `ready` | Lo shard è attivo e accetta query |
| `starting` | Cold start in corso |
| `stopped` | Lo shard è sceso a zero |

[tool-verified: `provisa/federation/engine_wake.py:engine_state`]

---

### `POST /data/engine/prewarm`

Avvia il risveglio del motore senza eseguire una query. (REQ-1516) [tool-verified: `provisa/api/data/endpoint_dev.py:913`]

Restituisce subito `202 Accepted`. Il risveglio prosegue in background. Va usato per avere il motore pronto prima che arrivi la prima query — per esempio da uno scheduler che eseguirà query qualche minuto dopo.

**Risposta:** `202 Accepted`, corpo `{"started": true}`

[tool-verified: `provisa/federation/engine_wake.py:prewarm_engine`]

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

#### Explorer OpenAPI / Swagger UI

La pagina explorer OpenAPI (`/app/openapi`) incorpora la Swagger UI in un iframe sandboxed. La spec è scopata per ruolo — compaiono solo le tabelle e colonne visibili al ruolo corrente — ed è opzionalmente filtrata per dominio tramite il selettore di dominio. L'interfaccia passa automaticamente tra tema chiaro e scuro. [tool-verified: `provisa-ui/src/pages/OpenApiPage.tsx:20-34`]

La pagina carica l'HTML della spec tramite `fetch()` invece di un `src` diretto dell'iframe, così la richiesta porta il bearer token della sessione e le richieste relative proprie di Swagger UI si risolvono correttamente contro la stessa origine. [tool-verified: `provisa-ui/src/pages/OpenApiPage.tsx:44-69`]

Quando si arriva da un link NL "Open in OpenAPI", la pagina espande automaticamente l'endpoint target, popola i parametri di query dall'URL generato da NL (es. `aggregate`, `groupBy`) e clicca Execute — usando il polling del DOM per garantire che ogni passo si completi prima che scatti il successivo. (REQ-1359) [tool-verified: `provisa-ui/src/pages/OpenApiPage.tsx:94-171`]

---

### `GET /data/jsonapi/{domain_id}/{table_name}`

Endpoint conforme a [JSON:API](https://jsonapi.org) auto-generato per ogni tabella registrata. Stessi RLS, mascheramento e routing di GraphQL. (REQ-257) [tool-verified: `provisa/api/jsonapi/generator.py:284`]

**Header `Accept`:** deve includere `application/vnd.api+json` (il media type JSON:API) o la richiesta restituisce `406`.

**Parametri di query:**

- `fields[<type>]` — sparse fieldset, es. `?fields[orders]=amount`
- `filter[<col>]` / `filter[<col>][<op>]` — es. `?filter[region]=US`, `?filter[amount][gt]=100`
- `sort` — separati da virgola, prefisso `-` per discendente, es. `?sort=-created_at,amount`
- `page[number]` / `page[size]` — paginazione
- `aggregate` — funzioni di aggregazione separate da virgola da eseguire invece del recupero righe: `count`, `sum`, `avg`, `stddev`, `variance`, `min`, `max`. Usa `?aggregate=count,sum` per richiedere un sottoinsieme. Le risposte di aggregazione restituiscono `data: null` con i risultati in `meta.aggregate`. (REQ-1359) [tool-verified: `provisa-ui/src/pages/JsonApiPage.tsx:238`]
- `groupBy` — nomi di colonna separati da virgola; usato con `?aggregate=` per raggruppare i risultati. Sono valide solo le colonne nell'enum `DistinctOnColumn` della tabella; il server restituisce `400` per qualsiasi colonna che il ruolo non può vedere. (REQ-1361) [tool-verified: `provisa-ui/src/pages/JsonApiPage.tsx:447`]
- `includeNodes` — `true` per includere le colonne scalari della tabella base (e gli scalari di dimensione unita nominati in `include=`) dentro l'array `nodes` di ogni riga di gruppo. Richiesto quando una query NL group-by richiede anche dettagli di dimensione. (REQ-1405)

Le risposte sono resource object con `type`/`id`/`attributes`. Gli errori seguono la forma dell'error object JSON:API.

#### Explorer JSON:API

La pagina explorer JSON:API (`/app/jsonapi`) è un'interfaccia browser su questi endpoint. Seleziona una tabella dall'elenco raggruppato per dominio, poi configura:

- **Fields** — scegli quali colonne includere (sparse fieldset); lascia tutto deselezionato per richiedere ogni colonna
- **Relationships** — seleziona i nomi di relazione derivati dalle FK da caricare tramite `?include=`
- **Filter** — campo, operatore (`eq`, `neq`, `gt`, `gte`, `lt`, `lte`, `like`) e valore
- **Sort** — un campo, ascendente o discendente
- **Aggregate** — scegli le colonne di group-by dall'elenco convalidato dal server, poi seleziona una o più funzioni di aggregazione; quando sono selezionate colonne di group-by, una checkbox "Include nodes" aggiunge le colonne scalari della tabella base a ogni riga
- **Page size** — risorse per pagina, con navigazione first/prev/next/last

I risultati vengono resi in una vista di riepilogo formattata (schede risorsa con ancore di relazione cliccabili) o in una scheda JSON grezzo. L'URL della richiesta live viene mostrato e può essere copiato. La selezione della tabella e la dimensione pagina persistono tra le sessioni in `localStorage`. [tool-verified: `provisa-ui/src/pages/JsonApiPage.tsx`]

Quando si arriva da un link NL "Open in JSON:API", l'explorer preseleziona la tabella e inizializza il selettore di aggregazione dai parametri di query generati da NL, poi esegue automaticamente la richiesta. [tool-verified: `provisa-ui/src/pages/JsonApiPage.tsx:460-479`]

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

#### NL Group-By con dettagli di dimensione (REQ-1405)

Quando una query NL group-by proietta anche colonne da una tabella di dimensione unita — ad esempio, "count of inquiries by user with user name and email" — il runner deriva percorsi puntati per-campo (`dim_paths`) dalle colonne di dimensione proiettate nella SELECT. Questi percorsi popolano il parametro `includeNodes=` sugli URL generati dai pannelli JSON:API e OpenAPI, così quei pannelli richiedono gli stessi campi di dimensione unita risolti dai branch SQL e GraphQL. Senza questo, `includeNodes=true` restituirebbe solo i campi scalari propri della tabella di aggregazione base. (REQ-1405) [tool-verified: `docs/arch/requirements.md:REQ-1405`]

Sul pannello gRPC, la `{Type}GroupByRequest` generata porta `include_nodes` (bool) e `include` (stringa ripetuta di nomi di campo relazione). La `{Type}GroupByRow` restituita include un campo `nodes` tipizzato con le righe di dettaglio di dimensione. [tool-verified: `provisa/grpc/query_ir.py:168-196`]

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

Ogni tipo di relazione porta anche `junction_table_name` e `properties` (REQ-1586). Su un arco basato su una giunzione il primo nomina la tabella associativa che attraversa e il secondo elenca le colonne di quella tabella leggibili come `r.attr` e filtrabili in `WHERE`; su un arco basato su chiave esterna il nome è `null` e l'elenco delle proprietà è vuoto, ed è così che un client distingue i due casi. La tabella di giunzione stessa non è mai un'etichetta di nodo — è l'arco, quindi non ha alcuna pillola in un client grafo né una riga in `node_labels`. [tool-verified: `provisa/api/rest/cypher_router.py:797-805`, `provisa/cypher/label_map.py:378-397`]

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

#### `GET /admin/config/live`

Scarica la **configurazione attiva corrente** — la configurazione così come Provisa la scriverebbe oggi, che riflette ogni tabella, relazione, dominio, ruolo e regola RLS creati via admin e accumulati dall'avvio. (REQ-164) [tool-verified: `provisa/api/admin/settings_router.py:67`]

Il file su disco può restare indietro rispetto allo stato attivo se le modifiche sono state fatte tramite l'API Admin senza un caricamento successivo. Questo endpoint colma quel divario: il suo output è ciò che `PUT /admin/config` dovrebbe ricevere perché il file su disco corrisponda allo stato attivo.

Restituisce `application/x-yaml` con `Content-Disposition: attachment; filename=provisa.live.yaml`.

#### `GET /admin/config/diff`

Restituisce entrambi i lati del confronto di configurazione — `original` (la base di riferimento all'avvio) e `current` (lo stato attivo) — normalizzati allo stesso modo, così che il confronto mostri solo le differenze reali e non riordinamenti o spostamenti di commenti. (REQ-164) [tool-verified: `provisa/api/admin/settings_router.py:82`]

**Risposta:**

```json
{"original": "<yaml>", "current": "<yaml>"}
```

#### `POST /admin/config/patch`

Genera una patch in formato unified diff dalla base di riferimento alla configurazione inviata. (REQ-164) [tool-verified: `provisa/api/admin/settings_router.py:93`]

Inviare lo YAML rivisto come corpo della richiesta. La risposta è un file `text/x-patch` (`provisa.config.patch`) consumabile direttamente da `git apply` o `patch` — utile per portare in commit le modifiche di configurazione fatte dalla UI attraverso una pipeline CI/CD.

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
- `relationships`: `auto_track_fk` — governa solo il tracciamento delle chiavi esterne. Una relazione basata su una giunzione viene dichiarata alla registrazione della tabella e non è mai inferita, quindi questa impostazione non la raggiunge. (REQ-1586)
- `otel`: `endpoint`, `service_name`, `sample_rate`, `support_endpoint`, `support_redact_sql_literals`, `support_redact_attributes`

**Risposta:**

```json
{"success": true, "updated": ["otel.support_endpoint", "cache.default_ttl"]}
```

---

### AI Models

#### `GET /admin/ai-models`

Restituisce le assegnazioni dei modelli AI, il registro dei modelli vettoriali e il rate limit NL dell'organizzazione attiva. (REQ-464, REQ-1349) [tool-verified: `provisa/api/admin/ai_models_router.py:58`]

**Risposta:**

```json
{
  "ai_models": {
    "nl": "claude-3-5-sonnet-20241022",
    "embedding": "text-embedding-3-small"
  },
  "vector_models": [...],
  "nl": {"rate_limit": 20},
  "api_keys_set": {"anthropic": true, "openai": false}
}
```

Le chiavi API non vengono mai restituite — `api_keys_set` riporta soltanto se ciascun vendor ha una chiave configurata. Le modifiche hanno effetto alla richiesta successiva; nessun riavvio necessario. (REQ-1349)

#### `PUT /admin/ai-models`

Aggiorna le assegnazioni dei modelli AI, il registro dei modelli vettoriali o il rate limit NL dell'organizzazione. Ha effetto alla richiesta successiva. [tool-verified: `provisa/api/admin/ai_models_router.py:148`]

#### `GET /admin/ai-models/vendors/{vendor}/models`

Restituisce i nomi dei modelli attualmente serviti da un vendor, per il selettore dei modelli. (REQ-1395, REQ-1398, REQ-1409) [tool-verified: `provisa/api/admin/ai_models_router.py:89`]

L'elenco viene letto in tempo reale dall'API di elenco modelli del vendor stesso usando la chiave configurata dall'organizzazione — oppure la credenziale del deployment quando non è impostata alcuna chiave dell'organizzazione. Un modello rilasciato dopo la distribuzione di questa build è selezionabile il giorno stesso in cui il vendor lo serve.

Restituisce `400` quando il vendor non pubblica alcuna API di elenco modelli (in quel caso il nome del modello va inserito direttamente) oppure quando non è disponibile alcuna chiave. [tool-verified: `provisa/api/admin/ai_models_router.py:109-128`]

---

### Motore di federazione

#### `GET /admin/federation-engine`

Restituisce la selezione corrente del motore di federazione, la sua configurazione di connessione e l'intero registro dei motori selezionabili. (REQ-916) [tool-verified: `provisa/api/admin/settings_router.py:730`]

**Risposta:**

```json
{
  "current": "trino",
  "persisted": "trino",
  "registry": [
    {"key": "trino", "label": "Trino (embedded)", "fields": [...]},
    {"key": "duckdb", "label": "DuckDB", "fields": []}
  ],
  "note": "Changing the federation engine takes effect after the service is restarted."
}
```

La chiave `current` è il motore in esecuzione in questo momento; `persisted` è ciò che viene scritto nel file di configurazione e verrà caricato al riavvio successivo. Divergono quando la configurazione è stata cambiata ma il servizio non è ancora stato riavviato.

#### `PUT /admin/federation-engine`

Persiste una selezione del motore di federazione. (REQ-916) [tool-verified: `provisa/api/admin/settings_router.py:774`]

**Corpo della richiesta:**

```json
{"engine": "trino", "federation_engine_url": "http://trino-coordinator:8080"}
```

La selezione viene scritta nella configurazione di piattaforma. Ha effetto dopo il riavvio successivo del servizio — il motore viene scelto una sola volta all'avvio.

---

### Politica dei domini

#### `POST /admin/domain-policy`

Cambia la politica dei domini dell'organizzazione attiva (`use_domains` / `default_domain`). (REQ-165, REQ-1266, REQ-1349) [tool-verified: `provisa/api/admin/settings_router.py:632`]

È un'operazione distruttiva limitata all'organizzazione attiva. Ogni origine, tabella, dominio e relazione registrati vengono eliminati e ricostruiti sotto la nuova politica. Va usata quando si porta un'organizzazione da domini con spazio dei nomi a struttura piatta (o viceversa).

**Corpo della richiesta:**

```json
{
  "use_domains": true,
  "default_domain": "default"
}
```

`use_domains: null` azzera l'override dell'organizzazione e ricade sull'impostazione a livello di deployment. `use_domains: false` richiede `default_domain` (il nome del dominio unico in cui atterrano tutte le tabelle). La ricostruzione del catalogo è sincrona; la risposta ritorna quando gli schemi sono pronti.

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

### Object storage (REQ-1046, REQ-1048, REQ-1049)

#### `GET /admin/org-storage`

Riporta l'occupazione di storage dell'organizzazione attiva rispetto alla sua quota di piattaforma, e se l'organizzazione ha registrato uno store proprio. [tool-verified: `provisa/api/admin/org_storage_router.py:69`]

Quando l'organizzazione ha registrato un DSN proprio, le sue materializzazioni vi atterrano e non vengono più conteggiate sulla quota. Il DSN in sé non viene mai restituito.

#### `PUT /admin/org-storage`

Registra (o azzera) lo store di materializzazione proprio dell'organizzazione. [tool-verified: `provisa/api/admin/org_storage_router.py:81`]

**Corpo della richiesta:**

```json
{"storage_url": "s3://my-bucket/provisa?region=us-east-1&access_key=..."}
```

Il DSN viene validato contro il motore di federazione prima di essere accettato — un DSN inutilizzabile fallisce alla registrazione, non ore dopo durante un aggiornamento. Il valore è cifrato a riposo e non viene mai restituito da GET.

Inviare `storage_url: null` per azzerare lo store proprio dell'organizzazione e riportarne le materializzazioni sullo store (e sulla quota) di piattaforma. Il runtime dell'organizzazione viene ricostruito nella stessa chiamata, quindi il nuovo store è effettivo immediatamente. [tool-verified: `provisa/api/admin/org_storage_router.py:123-138`]

---

### Crittografia dell'organizzazione (REQ-1574)

#### `GET /admin/org-encryption`

Restituisce lo stato corrente della chiave dell'organizzazione: fingerprint, id e provenienza. Non restituisce mai materiale della chiave. [tool-verified: `provisa/api/admin/org_encryption_router.py:53`]

Quando l'organizzazione non ha impostato alcuna chiave, restituisce `{"configured": false}`. Ogni organizzazione nasce in questo stato ed eredita la chiave del deployment.

#### `PUT /admin/org-encryption`

Imposta o ruota la chiave di crittografia a riposo dell'organizzazione. [tool-verified: `provisa/api/admin/org_encryption_router.py:68`]

**Corpo della richiesta:**

```json
{"key_b64": "<32 raw bytes, base64-encoded>"}
```

Omettere `key_b64` perché sia Provisa a generare una chiave — il percorso più sicuro, dato che la chiave non compare mai negli appunti né in un log delle richieste. Fornire `key_b64` significa portare la propria chiave.

La rotazione aggiunge una nuova voce attiva al portachiavi e conserva quella precedente, così i dati scritti sotto la chiave vecchia restano leggibili. La rotazione non è una ricifratura. Non esiste un endpoint di eliminazione: ritirare l'ultima chiave renderebbe illeggibile ogni payload protetto. [tool-verified: `provisa/api/admin/org_encryption_router.py:75`]

Il portachiavi attivo viene rilegato nella stessa chiamata, quindi la scrittura cifrata successiva usa subito la nuova chiave.

---

### Importazione Hasura / DDN (REQ-1483)

#### `POST /admin/import/hasura/preview`

Converte l'archivio di un progetto Hasura v2 o DDN in configurazione Provisa proposta, senza scrivere nulla. [tool-verified: `provisa/api/admin/import_router.py`]

**Corpo della richiesta:**

```json
{
  "filename": "my-project.zip",
  "content_b64": "<base64-encoded archive>",
  "flavor": "auto",
  "domain_map": {"public": "sales"},
  "source_overrides": {}
}
```

`flavor` è `"auto"` (rilevato dalla struttura dell'archivio), `"hasura_v2"` oppure `"ddn"`.

**Risposta:**

```json
{
  "config_yaml": "...",
  "warnings": ["..."],
  "summary": {
    "sources": 1, "domains": 2, "tables": 40,
    "columns": 180, "roles": 3, "relationships": 15, "rls_rules": 6
  }
}
```

Nulla viene persistito. L'anteprima non viene messa in cache lato server; `apply` prende lo YAML fornito, quindi ciò che viene applicato è esattamente ciò che è stato revisionato (ed eventualmente modificato).

#### `POST /admin/import/hasura/apply`

Carica nell'organizzazione attiva una configurazione già vista in anteprima. [tool-verified: `provisa/api/admin/import_router.py`]

**Corpo della richiesta:**

```json
{"config_yaml": "<yaml string>"}
```

Usa lo stesso percorso di hot-reload di `PUT /admin/config`. Catalogo, schemi e pool dell'organizzazione vengono ricostruiti prima che la risposta ritorni.

---

### Interscambio con Apache Ossie (REQ-1316, REQ-1321)

#### `GET /admin/ossie`

Esporta il modello governato dell'organizzazione come documento YAML Apache Ossie (incubating). (REQ-1321) [tool-verified: `provisa/api/admin/ossie_router.py`]

Il documento è derivato dallo stato attivo a ogni richiesta — mai messo in cache — quindi non può essere obsoleto. Le tabelle diventano oggetti `dataset`, le colonne diventano oggetti `field` e le relazioni si mappano su oggetti `relationship` di Ossie.

Restituisce `text/yaml` con `Content-Disposition: attachment; filename=provisa-ossie.yaml`.

#### `POST /admin/ossie/import`

Analizza un documento Ossie YAML o JSON e restituisce le registrazioni proposte di tabelle e relazioni. (REQ-1316) [tool-verified: `provisa/api/admin/ossie_router.py`]

**Corpo della richiesta:** YAML o JSON Ossie grezzo. Il formato viene rilevato automaticamente.

**Risposta:**

```json
{
  "proposals": {
    "tables": [...],
    "relationships": [...]
  }
}
```

Nulla viene registrato. La schermata di revisione della UI Admin consente di accettare o ridurre le proposte prima che scatti qualsiasi mutazione.

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

#### RPC di Aggregate e Group-By (REQ-1359, REQ-1361, REQ-1405)

Quando una tabella ha `enable_aggregates` impostato, il proto generato include due RPC aggiuntive accanto a `Query{TypeName}`:

- **`Query{TypeName}Aggregate`** — restituisce gli scalari di aggregazione per la tabella (`count`; `sum`, `avg`, `stddev`, `variance` per colonna numerica; `min`, `max` per colonna comparabile)
- **`Query{TypeName}GroupBy`** — restituisce una riga per chiave di gruppo con sotto-campi di aggregazione e, opzionalmente, scalari della tabella base e righe di dimensione unita in un campo `nodes`

Entrambe passano attraverso la stessa pipeline compilatore di aggregazione dei campi radice GraphQL `{field}_aggregate` e `{field}_group_by` — nessuna implementazione di aggregazione separata. (REQ-1359) [tool-verified: `provisa/grpc/query_ir.py:133-196`]

**Campo `funcs` (REQ-1361).** Il messaggio di richiesta accetta un campo `funcs` di stringhe ripetute. I valori validi sono `count`, `sum`, `avg`, `stddev`, `variance`, `min` e `max`. Quando `funcs` è omesso, viene richiesta ogni funzione esposta dallo schema per quella tabella. Quando è impostato, compaiono solo le funzioni nominate. Se nessuna delle funzioni nominate si applica ai tipi di colonna della tabella, la query ricade su `count`. [tool-verified: `provisa/grpc/query_ir.py:66`, `provisa/grpc/query_ir.py:75-97`]

**Campi `include_nodes` e `include` (REQ-1405).** Le richieste `Query{TypeName}GroupBy` possono impostare `include_nodes: true` per includere le colonne scalari della tabella base nel campo `nodes` di ogni riga. Il campo `include` di stringhe ripetute nomina i campi di relazione many-to-one le cui colonne scalari sono anch'esse annidate dentro `nodes`. Questo corrisponde al comportamento JSON:API `?includeNodes=` / `?include=`. [tool-verified: `provisa/grpc/query_ir.py:168-195`]

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

---

## Glossario aziendale (REQ-1387)

Il glossario aziendale mappa i nomi fisici dei campi — così come esistono nei database di origine — su un vocabolario umano condiviso. Ogni colonna registrata nel layer semantico ottiene automaticamente un termine. Non è richiesto alcun inserimento manuale per popolare il glossario; i curatori aggiungono definizioni, relazioni ed esperti sopra ciò che il sistema deriva.

### Come vengono derivati i termini

Quando Provisa registra o aggiorna le colonne di una tabella, `normalize_term` (`provisa/core/glossary.py`) viene eseguito su ogni nome di colonna e produce una frase canonica. [tool-verified: `provisa/core/repositories/glossary.py:sync_table_refs`]

La normalizzazione applica cinque regole in sequenza:

1. Divide sui confini camelCase e sui caratteri separatori (`_`, `-`, `.`, `/`, spazio bianco).
2. Converte il risultato in minuscolo.
3. Espande una tabella fissa di abbreviazioni (es. `cust` → `customer`, `amt` → `amount`, `dt` → `date`, `id` → `identifier`, `key` → `identifier`, `guid` → `identifier`).
4. Rimuove un **token proxy** finale (`identifier`, `code`, `index`, o `reference`) — una colonna nominata per la sua chiave o codice punta al concetto sottostante attraverso un valore surrogato, quindi il termine dovrebbe essere il concetto stesso. L'ultimo token rimanente non viene mai rimosso.
5. Qualifica una **frase troppo generica** con il concetto della tabella. Quando la frase normalizzata completa è una parola di attributo nuda (`name`, `identifier`, `date`, `location`, `message`, `first name`, `last name`, e simili), il termine diventa `<concetto tabella> <frase>` — `employees.first_name` → `employee first name`, `orders.id` → `order identifier`. Un unico termine `name` condiviso tra tabelle non correlate fonderebbe significati distinti; la qualificazione collega invece ogni colonna al proprio concetto englobante. Il concetto della tabella è il nome aziendale della tabella, normalizzato con un sostantivo di testa singolare (`order_lines` → `order line`).

Le pseudo-colonne di filtro nativo (prefisso `_nf_`, o qualsiasi colonna che porta `native_filter_type`) sono macchinari da parametro di query, non campi aziendali, e non derivano termini.

Poiché `id`, `key`, `pk` e `sk` si espandono tutti a `identifier` prima del controllo proxy, tre nomi di colonna fisicamente diversi finiscono esattamente sullo stesso termine:

| Nome fisico | Dopo la normalizzazione |
| --- | --- |
| `cust_id` | `customer` |
| `customerId` | `customer` |
| `CUSTOMER_KEY` | `customer` |
| `txn_amt` | `transaction amount` |

I primi tre collassano su un unico termine. `transaction amount` mantiene entrambi i token perché `amount` non è un proxy. Una colonna `id` nuda — senza token precedenti — non può essere rimossa; si normalizza a `identifier` così il termine non è vuoto. [tool-verified: `provisa/core/glossary.py:normalize_term`]

### Ciclo di vita

I termini sono **derivati dall'appartenenza al layer semantico**, non creati su richiesta dagli utenti. Il repository delle tabelle è l'unico percorso di scrittura: `sync_table_refs` viene eseguito dentro ogni upsert di set di colonne, e `sweep_refless_terms` viene eseguito dopo ogni percorso di eliminazione. [tool-verified: `provisa/core/repositories/glossary.py`]

**Quando viene aggiunta una colonna:** Provisa cerca il termine normalizzato per nome. Se esiste già, la colonna ottiene un ref verso di esso (e se il termine era deprecato, viene ripristinato — `deprecated` viene reimpostato a `False`). Se non esiste ancora alcun termine, ne viene creato uno.

**Quando una colonna viene rimossa** (cambio di schema o rimozione della tabella): il suo ref viene eliminato e il termine viene **regolato** secondo una regola rimuovi-o-deprecare. Un termine radicato senza ref rimanenti viene rimosso del tutto — insieme ai suoi archi e alle assegnazioni di esperti — a meno che rimuoverlo lascerebbe un termine astratto disconnesso da tutti i termini radicati (nessun percorso attraverso il grafo dei termini). In tal caso, il termine viene **deprecato** (marcato `deprecated=True`) invece che eliminato, così l'ancora nel grafo del termine astratto sopravvive.

I termini astratti non vengono mai rimossi automaticamente; esistono al di fuori del ciclo di vita fisico e vengono eliminati solo esplicitamente tramite l'API admin.

**Ripristino:** se il nome normalizzato di un termine deprecato riappare (una colonna viene ri-registrata), il termine viene smarcato e i suoi ref riprendono ad accumularsi.

### Endpoint di curazione

Tutti gli endpoint sono sotto `/admin/glossary`. Richiedono accesso `org_admin` e un'org configurata. Ogni mutation innesca una pubblicazione di metadata. [tool-verified: `provisa/api/admin/glossary_router.py`]

| Metodo | Path | Descrizione |
| --- | --- | --- |
| `GET` | `/admin/glossary/terms` | Elenca i termini. Parametri di query: `q` (ricerca su nome/definizione), `include_deprecated` (default `true`) |
| `GET` | `/admin/glossary/terms/{term_id}` | Ottiene il dettaglio del termine: definizione, ref fisici, archi tipizzati, esperti |
| `POST` | `/admin/glossary/terms` | Crea un termine astratto — vocabolario utente senza ref fisici |
| `PATCH` | `/admin/glossary/terms/{term_id}` | Rinomina, imposta la definizione o attiva/disattiva l'esclusione dall'export |
| `DELETE` | `/admin/glossary/terms/{term_id}` | Elimina un termine senza ref fisici |
| `POST` | `/admin/glossary/refs/move` | Sposta un ref fisico verso un altro termine (consolidamento) |
| `POST` | `/admin/glossary/terms/{term_id}/edges` | Aggiunge un arco di relazione tipizzato tra due termini |
| `DELETE` | `/admin/glossary/terms/{term_id}/edges` | Rimuove un arco (parametri di query: `to_term_id`, `rel_type`) |
| `POST` | `/admin/glossary/terms/{term_id}/experts` | Etichetta un utente come esperto o autore di un termine |
| `DELETE` | `/admin/glossary/terms/{term_id}/experts/{user_id}` | Rimuove la designazione di esperto/autore di un utente |
| `POST` | `/admin/glossary/terms/{term_id}/definition/generate` | Bozza una definizione per un termine usando il modello AI dell'org — restituisce solo testo, nulla viene persistito finché non viene salvato |
| `POST` | `/admin/glossary/definitions/generate` | Genera e persiste le definizioni per ogni termine che non ne ha — non sovrascrive mai testo scritto da un umano |
| `POST` | `/admin/glossary/relationships/generate` | Propone e persiste archi tipizzati sull'intero glossario usando il modello AI dell'org |

**Corpo di `POST /admin/glossary/terms`:**

```json
{"name": "revenue", "definition": "Recognized net revenue after returns and discounts."}
```

**Corpo di `POST /admin/glossary/terms/{term_id}/edges`:**

```json
{"to_term_id": 42, "rel_type": "KIND_OF"}
```

Valori validi di `rel_type`: `KIND_OF`, `RELATED_TO`, `PART_OF`, `SYNONYM_OF`. [tool-verified: `provisa/core/glossary.py:TERM_EDGE_TYPES`]

**Corpo di `POST /admin/glossary/terms/{term_id}/experts`:**

```json
{"user_id": "alice@example.com", "kind": "author"}
```

Valori validi di `kind`: `expert`, `author`. [tool-verified: `provisa/core/repositories/glossary.py:add_expert`]

**Corpo di `POST /admin/glossary/refs/move`:**

```json
{"table_id": 7, "column_name": "cust_id", "to_term_id": 12}
```

Spostare un ref regola il termine perdente secondo la regola rimuovi-o-deprecare. Usa questo per consolidare due termini che la normalizzazione ha tenuto separati — ad esempio, dopo che un'origine usa un'abbreviazione non standard rimasta fuori dalla tabella di espansione.

Eliminare un termine radicato (con ref fisici) restituisce `400 glossary.invalid`. Rimuovi o sposta prima tutti i ref.

**`PATCH /admin/glossary/terms/{term_id}` — campo `export_excluded`:**

```json
{"export_excluded": true}
```

Impostare `export_excluded` a `true` trattiene il termine da tutti gli snapshot di export dei metadata, indipendentemente dai suoi ref fisici o dallo stato astratto. Reimpostarlo a `false` ripristina il termine nello snapshot alla prossima pubblicazione. I dati di curazione (definizione, archi, esperti) non sono interessati. [tool-verified: `provisa/core/repositories/glossary.py:set_export_excluded`, `provisa/api/admin/glossary_router.py:update_term`]

### Curazione assistita da AI

Il modello AI configurato dell'org può abbozzare definizioni e proporre archi di relazione sull'intero glossario in un'unica operazione. Entrambe le azioni bulk richiedono accesso `org_admin` e un'org configurata.

**`POST /admin/glossary/definitions/generate`**

Itera ogni termine nel glossario, salta quelli che hanno già una definizione, e chiama il modello AI dell'org per abbozzarne una per ogni termine rimanente. La bozza viene persistita immediatamente — a differenza dell'endpoint di bozza per singolo termine (`POST /admin/glossary/terms/{term_id}/definition/generate`), non c'è un passaggio di editor. Le definizioni scritte da un umano non vengono mai sovrascritte: la protezione è `if summary["definition"]: continue` prima di qualsiasi chiamata al modello. Una singola notifica di pubblicazione copre l'intero batch. [tool-verified: `provisa/api/admin/glossary_router.py:generate_all_definitions`]

Risposta:

```json
{"generated": 12}
```

`generated` è il conteggio dei termini che hanno ricevuto una nuova definizione. È zero quando ogni termine ne ha già una.

**`POST /admin/glossary/relationships/generate`**

Invia l'elenco completo dei termini al modello AI dell'org con un prompt che specifica i dieci tipi di arco consentiti (`KIND_OF`, `PART_OF`, `SYNONYM_OF`, `RELATED_TO`, `VALID_VALUE_OF`, `DERIVED_FROM`, `REPLACES`, `PREFERRED_TERM_FOR`, `TRANSLATION_OF`, `ANTONYM_OF`) e chiede solo proposte con alta confidenza. Il modello risponde con un array JSON; ogni voce viene validata prima di qualsiasi scrittura: nomi di termine sconosciuti, auto-archi e tipi di arco fuori dall'enum chiuso vengono scartati silenziosamente. Le proposte valide vengono upsertate in modo idempotente — rieseguire l'azione non duplica gli archi. Una singola notifica di pubblicazione copre il batch. L'endpoint restituisce `{"added": 0}` immediatamente quando il glossario contiene meno di due termini non deprecati. [tool-verified: `provisa/api/admin/glossary_router.py:generate_relationships`]

Risposta:

```json
{"added": 5}
```

`added` è il conteggio degli archi scritti. Un arco già esistente conta comunque — l'upsert riesce, ma i dati dell'arco non cambiano.

### Tool MCP `search_terms`

```
search_terms(query, role=None, limit=25)
```

Cerca nei nomi e nelle definizioni dei termini con un confronto per sottostringa case-insensitive, fino a `limit` risultati. Ogni risultato è il dettaglio completo del termine: `name`, `definition`, `is_abstract`, `deprecated`, ref fisici (con `source_id`, `schema_name`, `table_name`, `column_name`), archi tipizzati e assegnazioni di esperti. [tool-verified: `provisa/api/mcp/server.py:236-244`, `provisa/core/repositories/glossary.py:search_terms`]

Usa `search_terms` prima di scrivere SQL per trovare ogni campo fisico che rappresenta un concetto per nome. Ad esempio, cercare `"order date"` restituisce il termine e tutte le colonne `order_dt`, `orderDate`, `ORDER_DATE` in ogni tabella registrata.

### Export dei metadata

Il grafo dei termini del glossario è incluso in ogni `MetadataSnapshot` costruito da `build_snapshot`. [tool-verified: `provisa/api/metadata_export/builder.py:_glossary_assets`]

L'export applica gli stessi filtri del resto dello snapshot:

- Un termine marcato `export_excluded` viene trattenuto del tutto — indipendentemente dai suoi ref fisici, dallo stato astratto, o dal fatto che il catalogo dell'org sia configurato. [tool-verified: `provisa/api/metadata_export/builder.py:_glossary_assets`]
- Un termine radicato viene pubblicato solo quando almeno uno dei suoi ref fisici appartiene a una colonna che supera sia il filtro **Data Product** (il flag `data_product` della tabella deve essere `true`) sia il filtro colonna **technical** (le colonne taggate `technical` vengono trattenute).
- Un termine radicato i cui ref sono tutti trattenuti da quei filtri viene trattenuto insieme a loro.
- I termini astratti vengono pubblicati incondizionatamente — sono vocabolario utente, non vincolato a colonne fisiche.
- Un arco tra due termini viene pubblicato solo quando entrambi i termini agli estremi vengono pubblicati.

Ogni adapter vendor pubblica il grafo dei termini in modo nativo, in un container di glossario di proprietà di Provisa che crea in modo idempotente — mai in un glossario di catalogo esistente:

| Provider | Container | Termini | Relazioni | Deprecazione |
| --- | --- | --- | --- | --- |
| Apache Atlas | "Provisa Glossary" (glossary API) | termini di glossario, definizione su `longDescription` | KIND_OF → `isA`, SYNONYM_OF → `synonyms`, RELATED_TO/PART_OF → `seeAlso` | marcatore shortDescription `[DEPRECATED]` |
| Atlan | glossario Provisa per qualifiedName stabile | `longDescription` (mai la `userDescription` modificata da umano) | stessa mappatura Atlas | `certificateStatus = DEPRECATED` |
| DataHub | `urn:li:glossaryNode:provisa.<org>` | aspetto `glossaryTermInfo` per termine | KIND_OF → Inherits, PART_OF → Contains (invertito), RELATED_TO/SYNONYM_OF → related terms | aspetto di deprecazione; le rinomine seguono la successione URN |
| OpenMetadata | glossario Provisa via `/v1/glossaries` | PUT con chiave fqn, rinomine PATCH-rebind per UUID memorizzato | KIND_OF → gerarchia genitore nativa, SYNONYM_OF → `synonyms`, altri → `relatedTerms` | `entityStatus` |
| Collibra | dominio di tipo Glossary "Provisa Glossary" | asset Business Term via la Import API | tipi di relazione Business Term nativi | stato asset |

La proprietà è il binding, non il nome: l'id vendor di ogni termine pubblicato viene catturato in `catalog_bindings` sotto l'URN del termine (`provisa://<org>/terms/<name>`), e Provisa modifica o elimina un elemento di glossario lato vendor solo quando detiene quel binding (o l'elemento vive nel container di proprietà di Provisa che ha creato). Un elemento di glossario senza binding Provisa ha avuto origine nel sistema esterno e non viene mai toccato; gli aggiornamenti fanno read-merge così i campi aggiunti dallo steward sui termini propri di Provisa sopravvivono; nulla viene eliminato quando un termine esce dallo snapshot. Le assegnazioni steward termine-asset restano di proprietà esterna — nessun adapter scrive assegnazioni termine-asset (la pubblicazione delle assegnazioni scritte da Provisa è un follow-on esplicito). Su Collibra in particolare, la sicurezza sotto la semantica REPLACE della Import API si basa sul contenimento: il payload menziona solo asset dentro il dominio di glossario Provisa e istanze di relazione solo tra termini Provisa, così i glossari dello steward e le loro relazioni non sono mai raggiungibili. [tool-verified: `provisa/api/metadata_export/atlan.py`, `provisa/api/metadata_export/datahub.py`, `provisa/api/metadata_export/atlas.py`, `provisa/api/metadata_export/openmetadata.py`]
