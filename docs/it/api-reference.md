# Riferimento API

## Panoramica

Provisa espone endpoint REST sotto due prefissi: `/data` per l'esecuzione delle query e l'introspezione dello schema, e `/admin` per la gestione della configurazione. (REQ-043) La maggior parte degli endpoint dati richiede un identificatore di ruolo. Le operazioni di configurazione admin usano un'API GraphQL Strawberry su `/admin/graphql`. (REQ-164)

---

## Autenticazione

Quando `auth.provider` è configurato in `provisa.yaml`, tutti gli endpoint tranne `/health` e `/setup/status` richiedono un header `Authorization: Bearer <token>`. (REQ-120) [tool-verified: `provisa/api/app.py`, `provisa/auth/wiring.py`]

Senza autenticazione configurata, il server viene eseguito in modalità dev. Ogni richiesta è trattata come l'identità `anonymous`, che mappa su tutti i ruoli configurati con accesso a dominio wildcard. (REQ-535)

**Login (`POST /auth/login`)** è fornito dal provider di autenticazione attivo quando è configurato `provider: basic`. (REQ-124) Il formato delle credenziali e la risposta dipendono dal provider.

**Introspezione dell'identità:**

```http
GET /auth/me
```

Restituisce l'id, l'email, il nome visualizzato, le appartenenze all'org e le assegnazioni di ruolo dell'utente autenticato. In modalità dev restituisce `dev_mode: true` con tutti gli id di ruolo elencati. [tool-verified: `provisa/api/auth_router.py`]

```http
GET /auth/provider-type
```

Restituisce `{"provider": "<name>"}` oppure `{"provider": null}` quando l'autenticazione non è configurata. [tool-verified: `provisa/api/auth_router.py`]

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

Il campo `role` viene usato solo in modalità dev (senza autenticazione). Quando l'autenticazione è attiva, viene usato il ruolo dell'utente autenticato e `role` nel corpo viene ignorato.

Il campo `extensions` supporta il protocollo Automatic Persisted Query (APQ): (REQ-288)

```json
{
  "extensions": {"persistedQuery": {"sha256Hash": "<sha256-of-query>"}}
}
```

**Header:**

- `X-Provisa-Role` — sovrascrive il ruolo (modalità dev)
- `Accept` — formato della risposta (vedi Negoziazione del contenuto)
- `Authorization` — `Bearer <token>` quando l'autenticazione è abilitata
- `X-Provisa-Redirect-Format` — tipo MIME per l'output del redirect S3 (REQ-137)
- `X-Provisa-Redirect-Threshold` — numero di righe sopra il quale scatta il redirect (REQ-137)
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

Le query multi-root eseguono ogni campo root in modo indipendente. I campi sotto la soglia di redirect vengono restituiti inline; i campi sopra la soglia vengono reindirizzati. La chiave `redirects` (plurale) mappa i nomi dei campi alle informazioni di redirect. (REQ-029) [tool-verified: `provisa/api/data/endpoint.py`]

**Header di cache:**

- `X-Provisa-Cache: HIT|MISS` (REQ-536)
- `X-Provisa-Cache-Age: <seconds>` (su HIT) (REQ-536)

**Capability richieste:** `QUERY_DEVELOPMENT` per tutte le richieste, incluse quelle di introspezione. [tool-verified: `provisa/api/data/endpoint.py:186-283`]

---

### Negoziazione del contenuto

| Header Accept | Formato |
| --- | --- |
| `application/json` | JSON (predefinito) |
| `application/x-ndjson` | JSON delimitato da newline |
| `text/csv` | CSV |
| `application/vnd.apache.parquet` | Parquet |
| `application/vnd.apache.arrow.stream` | Arrow IPC |

(REQ-047, REQ-048, REQ-049, REQ-050) [tool-verified: `provisa/api/data/endpoint.py:84-90`]

---

### Redirect

I risultati sopra una soglia di righe configurata (o quando `X-Provisa-Redirect: true`) vengono scritti su S3 e viene restituito un URL presigned. (REQ-029, REQ-044)

| Formato di redirect | Scritto da | Memoria |
| --- | --- | --- |
| `application/vnd.apache.parquet` | CTAS federata | Nessuna — i dati non passano mai attraverso Provisa |
| `application/x-orc` | CTAS federata | Nessuna — i dati non passano mai attraverso Provisa |
| `application/json` | Provisa | Limitata dalla memoria |
| `application/x-ndjson` | Provisa | Limitata dalla memoria |
| `text/csv` | Provisa | Limitata dalla memoria |
| `application/vnd.apache.arrow.stream` | Provisa | Limitata dalla memoria |

Per grandi esportazioni analitiche, usare il redirect Parquet o ORC. Il motore di federazione scrive direttamente su S3 in parallelo — nessun dato passa attraverso Provisa. (REQ-138)

```yaml
X-Provisa-Redirect-Format: application/vnd.apache.parquet
X-Provisa-Redirect-Threshold: 1000
```

---

### `POST /data/sql`

Esegue SQL grezzo attraverso la pipeline di governance dello Stage 2. (REQ-267) [tool-verified: `provisa/api/data/endpoint_dev.py:62`]

**Corpo della richiesta:**

```json
{
  "sql": "SELECT id, amount FROM orders WHERE region = 'us'",
  "role": "admin"
}
```

**Capability richieste:** `QUERY_DEVELOPMENT`.

Le violazioni di governance su `POST /data/sql` restituiscono HTTP 403. (REQ-002, REQ-266)

**Risposta:** Stesso formato di `/data/graphql` (righe JSON per impostazione predefinita, con negoziazione del contenuto tramite `Accept`).

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

L'endpoint racchiude l'SQL **governato** — l'istruzione che viene effettivamente eseguita sotto il ruolo del chiamante, dopo RLS e mascheramento — nella sintassi EXPLAIN del dialetto. Ciò che il piano mostra è la versione autorizzata della query, non l'input grezzo.

**Corpo della richiesta:**

```json
{
  "sql": "SELECT id, amount FROM orders",
  "role": "admin",
  "analyze": false
}
```

Impostare `analyze: true` per eseguire EXPLAIN ANALYZE. La query viene eseguita e il piano riporta conteggi di righe e tempi reali. Non tutti i dialetti supportano ANALYZE; vedi la tabella in [Piani di query e statistiche](engines.md#query-plans-and-statistics).

**Risposta:** `{"plan": "<plan text or JSON>", "dialect": "trino", "analyzed": false}`

`400` quando il dialetto non ha supporto EXPLAIN, o quando viene richiesto `analyze: true` su un dialetto che non lo supporta (es. SQLite). [tool-verified: `provisa/executor/explain.py:wrap_explain`, `analyze_sql`]

---

### `GET /data/engine/state`

Restituisce lo stato corrente dello shard del motore senza risvegliarlo. (REQ-1516) [tool-verified: `provisa/api/data/endpoint_dev.py:892`]

La UI interroga questo endpoint per mostrare un banner di avvio mentre il motore è in cold-start. Non innesca mai un risveglio — il polling è sicuro e non conta come attività per il reaper di inattività.

**Risposta:**

```json
{"state": "ready"}
```

Valori possibili:

| Stato | Significato |
| --- | --- |
| `always-on` | Desktop, self-hosted o coordinator BYO — nessuna gestione del ciclo di vita |
| `ready` | Lo shard è attivo e accetta query |
| `starting` | Cold-start in corso |
| `stopped` | Lo shard è scalato a zero |

[tool-verified: `provisa/federation/engine_wake.py:engine_state`]

---

### `POST /data/engine/prewarm`

Innesca un risveglio del motore senza eseguire una query. (REQ-1516) [tool-verified: `provisa/api/data/endpoint_dev.py:913`]

Restituisce immediatamente `202 Accepted`. Il risveglio viene eseguito in background. Usare questo endpoint per avere il motore pronto prima dell'arrivo della prima query — ad esempio, da uno scheduler che esegue query alcuni minuti dopo.

**Risposta:** `202 Accepted`, corpo `{"started": true}`

[tool-verified: `provisa/federation/engine_wake.py:prewarm_engine`]

---

### `GET /data/rest/{domain_id}/{table_name}`

Endpoint REST semplice auto-generato per ogni tabella registrata. La query string mappa sugli argomenti GraphQL e la richiesta viene compilata ed eseguita attraverso la stessa pipeline (RLS, mascheramento, routing) di GraphQL. (REQ-256) [tool-verified: `provisa/api/rest/generator.py:153`]

**Parametri di query:**

- `limit` — righe massime (≥ 1)
- `offset` — righe da saltare (≥ 0)
- `fields` — nomi di colonna separati da virgola (per impostazione predefinita tutti i campi scalari)
- `filter` — array JSON di oggetti filtro `{"field", "comparator", "value"}`
- `orderBy` — array JSON di oggetti di ordinamento `{"field", "direction"}`

Il ruolo autenticato è obbligatorio; le richieste non autenticate restituiscono `401`. Una specifica OpenAPI per queste route è servita su `GET /data/rest/openapi.json` con Swagger UI su `GET /data/rest/docs`.

#### Explorer OpenAPI / Swagger UI

La pagina explorer OpenAPI (`/app/openapi`) incorpora la Swagger UI in un iframe sandboxed. La specifica è scoped al ruolo — appaiono solo le tabelle e le colonne visibili al ruolo corrente — ed è opzionalmente filtrata per dominio tramite il selettore di dominio. La UI passa automaticamente tra tema chiaro e scuro. [tool-verified: `provisa-ui/src/pages/OpenApiPage.tsx:20-34`]

La pagina carica l'HTML della specifica tramite `fetch()` anziché un `src` diretto dell'iframe, così la richiesta porta il bearer token della sessione e le richieste relative della Swagger UI si risolvono correttamente rispetto alla stessa origine. [tool-verified: `provisa-ui/src/pages/OpenApiPage.tsx:44-69`]

Quando si arriva da un link NL "Open in OpenAPI", la pagina espande automaticamente l'endpoint target, popola i parametri di query dall'URL generato da NL (es. `aggregate`, `groupBy`) e clicca Execute — usando il polling del DOM per assicurarsi che ogni passo si completi prima che scatti il successivo. (REQ-1359) [tool-verified: `provisa-ui/src/pages/OpenApiPage.tsx:94-171`]

---

### `GET /data/jsonapi/{domain_id}/{table_name}`

Endpoint conforme a [JSON:API](https://jsonapi.org) auto-generato per ogni tabella registrata. Stessi RLS, mascheramento e routing di GraphQL. (REQ-257) [tool-verified: `provisa/api/jsonapi/generator.py:284`]

**Header `Accept`:** deve includere `application/vnd.api+json` (il tipo MIME JSON:API) altrimenti la richiesta restituisce `406`.

**Parametri di query:**

- `fields[<type>]` — sparse fieldset, es. `?fields[orders]=amount`
- `filter[<col>]` / `filter[<col>][<op>]` — es. `?filter[region]=US`, `?filter[amount][gt]=100`
- `sort` — separato da virgola, prefisso `-` per ordine decrescente, es. `?sort=-created_at,amount`
- `page[number]` / `page[size]` — paginazione
- `aggregate` — funzioni di aggregazione separate da virgola da eseguire al posto del recupero righe: `count`, `sum`, `avg`, `stddev`, `variance`, `min`, `max`. Usare `?aggregate=count,sum` per richiedere un sottoinsieme. Le risposte di aggregazione restituiscono `data: null` con i risultati in `meta.aggregate`. (REQ-1359) [tool-verified: `provisa-ui/src/pages/JsonApiPage.tsx:238`]
- `groupBy` — nomi di colonna separati da virgola; usato con `?aggregate=` per raggruppare i risultati. Sono valide solo le colonne nell'enum `DistinctOnColumn` della tabella; il server restituisce `400` per qualsiasi colonna che il ruolo non può vedere. (REQ-1361) [tool-verified: `provisa-ui/src/pages/JsonApiPage.tsx:447`]
- `includeNodes` — `true` per includere le colonne scalari della tabella base (e gli scalari della dimensione unita nominati in `include=`) all'interno dell'array `nodes` di ogni riga di gruppo. Richiesto quando una query NL group-by richiede anche dettagli di dimensione. (REQ-1405)

Le risposte sono oggetti risorsa con `type`/`id`/`attributes`. Gli errori seguono la forma dell'oggetto errore JSON:API.

#### Explorer JSON:API

La pagina explorer JSON:API (`/app/jsonapi`) è una UI browser su questi endpoint. Selezionare una tabella dall'elenco raggruppato per dominio, quindi configurare:

- **Fields** — scegliere quali colonne includere (sparse fieldset); lasciare tutto deselezionato per richiedere ogni colonna
- **Relationships** — selezionare i nomi delle relazioni derivate da FK da caricare tramite `?include=`
- **Filter** — campo, operatore (`eq`, `neq`, `gt`, `gte`, `lt`, `lte`, `like`) e valore
- **Sort** — un campo, crescente o decrescente
- **Aggregate** — scegliere le colonne di group-by dall'elenco validato dal server, quindi selezionare una o più funzioni di aggregazione; quando sono selezionate colonne di group-by, una checkbox "Include nodes" aggiunge le colonne scalari della tabella base a ogni riga
- **Page size** — risorse per pagina, con navigazione primo/precedente/successivo/ultimo

I risultati vengono renderizzati in una vista di riepilogo formattata (card di risorsa con ancore di relazione cliccabili) o in una scheda JSON grezzo. L'URL della richiesta live viene mostrato e può essere copiato. La selezione della tabella e la page size persistono tra le sessioni in `localStorage`. [tool-verified: `provisa-ui/src/pages/JsonApiPage.tsx`]

Quando si arriva da un link NL "Open in JSON:API", l'explorer preseleziona la tabella e popola il selettore di aggregazione dai parametri di query generati da NL, poi esegue automaticamente la richiesta. [tool-verified: `provisa-ui/src/pages/JsonApiPage.tsx:460-479`]

---

### `POST /query/nl`

Invia una domanda in linguaggio naturale. Il servizio avvia un job asincrono e restituisce immediatamente `202 Accepted` con un `job_id`. Richiede un provider LLM configurato nella sezione di configurazione `ai_models`. (REQ-354) [tool-verified: `provisa/api/rest/nl_router.py:50`]

**Corpo della richiesta:**

```json
{"q": "How many orders were placed last month?", "role": "admin"}
```

Restituisce `{"job_id": "<id>"}`. Il superamento del rate limit NL per ruolo restituisce `429` con un header `Retry-After`. (REQ-370)

**Recuperare il risultato:**

- `GET /query/nl/{job_id}` — polling. Restituisce il documento del job.
- `GET /query/nl/{job_id}/stream` — SSE. Un evento `branch` per ogni target di generazione man mano che si completa, poi un evento `done`. (REQ-357, REQ-358)

Tre cicli di generazione (Cypher, GraphQL, SQL) vengono eseguiti in parallelo, ciascuno validato attraverso il compilatore e raffinato in caso di errore. (REQ-355) Il prompt è scoped allo schema visibile del ruolo. (REQ-356) Il documento del risultato chiavizza ogni branch per target: (REQ-357) [tool-verified: `provisa/nl/job.py:69`]

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

Un branch che esaurisce il proprio limite di iterazioni restituisce `query: null`, `result: null`, e una stringa `error`. Ogni query generata viene eseguita sotto i diritti del consumer con la governance dello Stage 2 applicata — il servizio non bypassa mai la governance. (REQ-359)

#### NL Group-By con dettagli di dimensione (REQ-1405)

Quando una query NL group-by proietta anche colonne da una tabella dimensione unita — per esempio, "conteggio delle richieste per utente con nome utente ed email" — il runner deriva dot-path per campo (`dim_paths`) dalle colonne di dimensione proiettate nella SELECT. Questi path popolano il parametro `includeNodes=` sugli URL generati per i pannelli JSON:API e OpenAPI, così quei pannelli richiedono gli stessi campi di dimensione unita risolti dai branch SQL e GraphQL. Senza questo, `includeNodes=true` restituirebbe solo i campi scalari propri della tabella di aggregazione base. (REQ-1405) [tool-verified: `docs/arch/requirements.md:REQ-1405`]

Sul pannello gRPC, il `{Type}GroupByRequest` generato porta `include_nodes` (bool) e `include` (stringa ripetuta di nomi di campo relazione). La `{Type}GroupByRow` restituita include un campo `nodes` tipizzato con le righe di dettaglio della dimensione. [tool-verified: `provisa/grpc/query_ir.py:168-196`]

---

### `GET /data/sdl`

Restituisce l'SDL GraphQL per lo schema di un ruolo. (REQ-008) [tool-verified: `provisa/api/data/sdl.py:137`]

**Header:** `X-Role: <role_id>` (obbligatorio)

**Parametri di query:**

- `domain` — id di dominio separati da virgola. Quando impostato, la risposta è filtrata al/ai dominio/i nominato/i e alle tabelle raggiungibili da essi.

**Risposta:** SDL GraphQL `text/plain`.

---

### `GET /data/introspection`

Restituisce il JSON di introspezione GraphQL, opzionalmente filtrato per dominio. [tool-verified: `provisa/api/data/sdl.py:200`]

**Header:** `X-Provisa-Role: <role_id>` (obbligatorio)

**Parametri di query:** `domain` — id di dominio separati da virgola.

**Risposta:** risultato di introspezione `application/json`.

---

### `GET /data/graph-schema`

Restituisce la vista a grafo dello schema del ruolo: le etichette dei nodi e i loro tipi di relazione, per client Cypher/grafo. Include `pk_columns` per etichetta di nodo così i chiamanti possono determinare le colonne di chiave primaria. (REQ-398) [tool-verified: `provisa/api/rest/cypher_router.py:689`]

**Risposta:** `application/json` con `node_labels` (ognuna con `pk`/`pk_columns`) e `relationship_types`.

Ogni tipo di relazione porta anche `junction_table_name` e `properties` (REQ-1586). Su un arco basato su tabella di giunzione, il primo nomina la tabella associativa che attraversa e il secondo elenca le colonne di quella tabella leggibili come `r.attr` e filtrabili in `WHERE`; su un arco basato su chiave esterna, il nome è `null` e l'elenco delle proprietà è vuoto, ed è così che un client distingue i due casi. La tabella di giunzione stessa non è mai un'etichetta di nodo — è l'arco, quindi non ha un pill in un client grafico e nessuna riga in `node_labels`. [tool-verified: `provisa/api/rest/cypher_router.py:797-805`, `provisa/cypher/label_map.py:378-397`]

---

### `GET /data/domains`

Restituisce gli id di dominio accessibili al ruolo richiedente. [tool-verified: `provisa/api/data/sdl.py:116`]

**Header:** `X-Role: <role_id>` (obbligatorio)

**Risposta:** `["sales", "support", ...]`

---

### `GET /data/schema-version`

Restituisce la stringa della versione corrente dello schema. Combina un nonce per-boot con un contatore di rebuild. I client usano questo valore per invalidare le cache dello schema dopo i riavvii del server. (REQ-537) [tool-verified: `provisa/api/data/sdl.py:102`]

**Risposta:** `{"version": "<boot-id>-<counter>"}`

---

### `GET /data/proto/{role_id}`

Restituisce il file `.proto` auto-generato per un ruolo. [tool-verified: `provisa/api/data/endpoint_dev.py:49`]

**Risposta:** schema protobuf `text/plain`.

Ogni tabella registrata produce un `message` proto. Le relazioni producono campi di messaggio annidati. Mapping dei tipi: `integer → int32`, `bigint → int64`, `varchar → string`, `decimal → double`, `boolean → bool`, `timestamp → google.protobuf.Timestamp`. (REQ-538)

---

### `GET /data/subscribe/{table}`

Stream Server-Sent Events per notifiche di cambiamento in tempo reale da una tabella. (REQ-219, REQ-258) [tool-verified: `provisa/api/data/subscribe.py:239`]

La consegna delle notifiche usa un provider pluggable scelto per tipo di origine: le origini PostgreSQL usano `LISTEN/NOTIFY` (via asyncpg), le origini MongoDB usano Change Streams (`collection.watch()`), e le origini Kafka usano consumer group. Ogni provider implementa un'interfaccia comune di watch asincrona. Il filtraggio RLS e la validazione dello schema si applicano indipendentemente dal provider. (REQ-258) Sono supportate anche origini WebSocket e RSS. (REQ-338, REQ-342)

**Header — `X-Provisa-Sink`:** Impostare su un target Kafka (es. `kafka://broker:9092/topic`) per reindirizzare gli eventi di cambiamento verso un sink Kafka invece della risposta SSE. Il server avvia un consumer sink e restituisce `202 Accepted` anziché uno stream aperto. (REQ-812) [tool-verified: `provisa/api/data/subscription_sse.py:137`]

---

## Endpoint REST Admin

### Config

#### `GET /admin/config`

Scarica il `provisa.yaml` corrente come `application/x-yaml` con un header `Content-Disposition: attachment`. (REQ-164) [tool-verified: `provisa/api/admin/settings_router.py:19`]

#### `PUT /admin/config`

Carica uno YAML di configurazione rivisto. Il server scrive un backup `.bak`, salva il nuovo file e ricarica tutti gli schemi, le origini dati e le viste materializzate. (REQ-164) [tool-verified: `provisa/api/admin/settings_router.py:32`]

**Corpo della richiesta:** Contenuto YAML grezzo.

**Risposta:**

```json
{"success": true, "message": "Config uploaded and reloaded"}
```

In caso di errore di reload: `{"success": false, "message": "<error>"}`.

#### `GET /admin/config/live`

Scarica la **configurazione live corrente** — la configurazione così come Provisa la scriverebbe oggi, riflettendo ogni tabella, relazione, dominio, ruolo e regola RLS creati dall'admin e accumulati dall'avvio. (REQ-164) [tool-verified: `provisa/api/admin/settings_router.py:67`]

Il file su disco può essere in ritardo rispetto allo stato live se le modifiche sono state effettuate tramite l'API admin senza un successivo upload. Questo endpoint colma tale divario: il suo output è ciò che `PUT /admin/config` dovrebbe ricevere per allineare il file su disco allo stato live.

Restituisce `application/x-yaml` con `Content-Disposition: attachment; filename=provisa.live.yaml`.

#### `GET /admin/config/diff`

Restituisce entrambi i lati del diff di configurazione — `original` (la baseline di avvio) e `current` (lo stato live) — normalizzati in modo identico, così il confronto mostra solo le modifiche reali, non il riordino o la deriva dei commenti. (REQ-164) [tool-verified: `provisa/api/admin/settings_router.py:82`]

**Risposta:**

```json
{"original": "<yaml>", "current": "<yaml>"}
```

#### `POST /admin/config/patch`

Genera una patch in formato diff unificato dalla baseline alla configurazione inviata. (REQ-164) [tool-verified: `provisa/api/admin/settings_router.py:93`]

Inviare lo YAML rivisto come corpo della richiesta. La risposta è un file `text/x-patch` (`provisa.config.patch`) che `git apply` o `patch` possono consumare direttamente — utile per fare commit delle modifiche di configurazione guidate dalla UI attraverso una pipeline CI/CD.

---

### Settings

#### `GET /admin/settings`

Restituisce le impostazioni della piattaforma correnti in JSON. (REQ-165) [tool-verified: `provisa/api/admin/settings_router.py:50`]

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
- `relationships`: `auto_track_fk` — governa solo il tracciamento delle chiavi esterne. Una relazione basata su tabella di giunzione è dichiarata alla registrazione della tabella e non viene mai inferita, quindi questa impostazione non la riguarda. (REQ-1586)
- `otel`: `endpoint`, `service_name`, `sample_rate`, `support_endpoint`, `support_redact_sql_literals`, `support_redact_attributes`

**Risposta:**

```json
{"success": true, "updated": ["otel.support_endpoint", "cache.default_ttl"]}
```

---

### Modelli IA

#### `GET /admin/ai-models`

Restituisce le assegnazioni di modello IA dell'org attiva, il registro dei modelli vettoriali e il rate limit NL. (REQ-464, REQ-1349) [tool-verified: `provisa/api/admin/ai_models_router.py:58`]

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

Le chiavi API non vengono mai restituite in eco — `api_keys_set` riporta solo se ogni vendor ha una chiave configurata. Le modifiche hanno effetto alla richiesta successiva; non è necessario alcun riavvio. (REQ-1349)

#### `PUT /admin/ai-models`

Aggiorna le assegnazioni di modello IA dell'org, il registro dei modelli vettoriali, o il rate limit NL. Ha effetto alla richiesta successiva. [tool-verified: `provisa/api/admin/ai_models_router.py:148`]

#### `GET /admin/ai-models/vendors/{vendor}/models`

Restituisce i nomi dei modelli attualmente serviti da un vendor, per il selettore di modello. (REQ-1395, REQ-1398, REQ-1409) [tool-verified: `provisa/api/admin/ai_models_router.py:89`]

L'elenco viene letto live dall'API list-models del vendor stesso, usando la chiave configurata dall'org — o la credenziale di deployment quando non è impostata alcuna chiave org. Un modello rilasciato dopo che questa build è stata pubblicata è selezionabile lo stesso giorno in cui il vendor lo serve.

Restituisce `400` quando il vendor non pubblica alcuna API list-models (in tal caso inserire il nome del modello direttamente) o quando non è disponibile alcuna chiave. [tool-verified: `provisa/api/admin/ai_models_router.py:109-128`]

---

### Motore di federazione

#### `GET /admin/federation-engine`

Restituisce la selezione corrente del motore di federazione, la sua configurazione di connessione e il registro completo dei motori selezionabili. (REQ-916) [tool-verified: `provisa/api/admin/settings_router.py:730`]

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

La chiave `current` è il motore in esecuzione in questo momento; `persisted` è ciò che è scritto nel file di configurazione e verrà caricato al prossimo riavvio. Divergono quando la configurazione è stata modificata ma il servizio non è ancora stato riavviato.

#### `PUT /admin/federation-engine`

Persiste una selezione del motore di federazione. (REQ-916) [tool-verified: `provisa/api/admin/settings_router.py:774`]

**Corpo della richiesta:**

```json
{"engine": "trino", "federation_engine_url": "http://trino-coordinator:8080"}
```

La selezione viene scritta nella configurazione della piattaforma. Ha effetto dopo il successivo riavvio del servizio — il motore viene scelto una sola volta all'avvio.

---

### Politica di dominio

#### `POST /admin/domain-policy`

Modifica la politica di dominio dell'org attiva (`use_domains` / `default_domain`). (REQ-165, REQ-1266, REQ-1349) [tool-verified: `provisa/api/admin/settings_router.py:632`]

Questa è un'operazione distruttiva scoped all'org attiva. Ogni origine dati, tabella, dominio e relazione registrati viene eliminato e ricostruito sotto la nuova politica. Usarla quando si passa un'org da domain-namespaced a flat (o viceversa).

**Corpo della richiesta:**

```json
{
  "use_domains": true,
  "default_domain": "default"
}
```

`use_domains: null` cancella l'override dell'org e ritorna all'impostazione a livello di deployment. `use_domains: false` richiede `default_domain` (il singolo nome di dominio in cui atterrano tutte le tabelle). La ricostruzione del catalogo è sincrona; la risposta viene restituita una volta che gli schemi sono pronti.

---

### Osservabilità

#### `GET /admin/traces/recent`

Restituisce fino a N span completati recenti dal buffer di span in memoria. (REQ-302) [tool-verified: `provisa/api/admin/settings_router.py:317`]

**Parametri di query:** `limit` (predefinito 50, massimo 200)

**Risposta:** `{"traces": [...]}`

#### `POST /admin/query-engine/reload-catalog`

Ricarica a caldo un catalogo nominato nel coordinator del motore di federazione tramite la sua API REST. Riconnette la connessione interna di Provisa e riesegue il DDL OTel. [tool-verified: `provisa/api/admin/settings_router.py:208`]

**Parametri di query:** `catalog` (predefinito `"otel"`)

**Risposta:**

```json
{"success": true, "errors": []}
```

#### `POST /admin/query-engine/restart`

Riavvia il container del motore di federazione (solo dev single-node). [tool-verified: `provisa/api/admin/settings_router.py:287`]

**Parametri di query:** `container` (predefinito la variabile d'ambiente `QUERY_ENGINE_CONTAINER`, poi `"trino"`)

---

### Discovery

#### `POST /admin/discover/relationships`

Innesca la discovery delle relazioni. Esegue sempre l'introspezione FK dal motore di federazione. (REQ-018) Esegue l'inferenza LLM se `ANTHROPIC_API_KEY` è impostata. (REQ-167) [tool-verified: `provisa/api/admin/discovery.py:55`]

**Corpo della richiesta:**

```json
{
  "scope": "domain",
  "domain_id": "sales"
}
```

`scope` deve essere uno tra `"table"`, `"domain"`, `"cross-domain"`. Per lo scope `"table"`, è richiesto `table_id` (integer). Per lo scope `"domain"`, è richiesto `domain_id`.

**Risposta:** `{"candidates_found": 12, "stored_ids": [1, 2, 3, ...]}`

#### `GET /admin/discover/candidates`

Elenca i candidati di relazione in sospeso. [tool-verified: `provisa/api/admin/discovery.py:96`]

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

### Crawl delle origini dati

#### `POST /admin/sources/crawl`

Esegue il crawl di un'origine dati per introspezionarne lo schema e registrare le tabelle. (REQ-012) [tool-verified: `provisa/api/admin/crawl_router.py:36`]

---

### Ricerca tabelle dell'origine dati

#### `GET /admin/sources/{source_id}/tables/search`

Cerca per nome le tabelle disponibili (non ancora registrate) in un'origine dati. [tool-verified: `provisa/api/admin/table_search_router.py:103`]

---

### Profilazione tabelle

#### `POST /admin/tables/{table_id}/profile`

Esegue un profilo di colonna su una tabella registrata — cardinalità, min/max, tassi di null. [tool-verified: `provisa/api/admin/table_profile_router.py:28`]

---

### Descrizioni delle origini dati

#### `POST /admin/source-meta/db-description`

Genera descrizioni assistite da LLM per le tabelle e le colonne di un'origine dati. [tool-verified: `provisa/api/admin/source_meta_router.py:48`]

---

### Object Storage (REQ-1046, REQ-1048, REQ-1049)

#### `GET /admin/org-storage`

Riporta l'ingombro di storage dell'org attiva rispetto alla sua allocazione a livello di piattaforma, e se l'org ha registrato un proprio store. [tool-verified: `provisa/api/admin/org_storage_router.py:69`]

Quando l'org ha registrato un proprio DSN, le sue materializzazioni vanno lì e non vengono più conteggiate nell'allocazione. Il DSN stesso non viene mai restituito.

#### `PUT /admin/org-storage`

Registra (o cancella) lo store di materializzazione proprio dell'org. [tool-verified: `provisa/api/admin/org_storage_router.py:81`]

**Corpo della richiesta:**

```json
{"storage_url": "s3://my-bucket/provisa?region=us-east-1&access_key=..."}
```

Il DSN viene validato contro il motore di federazione prima di essere accettato — un DSN inutilizzabile fallisce alla registrazione, non ore dopo durante un refresh. Il valore è cifrato at-rest e non viene mai restituito da GET.

Inviare `storage_url: null` per cancellare lo store proprio dell'org e riportare le sue materializzazioni allo store della piattaforma (e alla relativa allocazione). Il runtime dell'org viene ricostruito nella stessa chiamata, così il nuovo store è effettivo immediatamente. [tool-verified: `provisa/api/admin/org_storage_router.py:123-138`]

---

### Cifratura dell'org (REQ-1574)

#### `GET /admin/org-encryption`

Restituisce lo stato della chiave corrente dell'org: fingerprint, id e provenienza. Non restituisce mai il materiale della chiave. [tool-verified: `provisa/api/admin/org_encryption_router.py:53`]

Quando l'org non ha impostato alcuna chiave, restituisce `{"configured": false}`. Ogni org parte in questo stato ed eredita la chiave del deployment.

#### `PUT /admin/org-encryption`

Imposta o ruota la chiave di cifratura at-rest dell'org. [tool-verified: `provisa/api/admin/org_encryption_router.py:68`]

**Corpo della richiesta:**

```json
{"key_b64": "<32 raw bytes, base64-encoded>"}
```

Omettere `key_b64` per far generare a Provisa una chiave — il percorso più sicuro, poiché la chiave non compare mai in un clipboard o in un log di richiesta. Fornire `key_b64` significa portare la propria chiave.

La rotazione aggiunge una nuova voce attiva al key ring e mantiene quella precedente, così i dati scritti sotto la chiave precedente restano leggibili. La rotazione non è ri-cifratura. Non esiste un endpoint di eliminazione: ritirare l'ultima chiave renderebbe illeggibile ogni payload wrapped. [tool-verified: `provisa/api/admin/org_encryption_router.py:75`]

Il ring live viene ricollegato nella stessa chiamata, così la successiva scrittura cifrata usa immediatamente la nuova chiave.

---

### Import Hasura / DDN (REQ-1483)

#### `POST /admin/import/hasura/preview`

Converte un archivio progetto Hasura v2 o DDN in una configurazione Provisa proposta senza scrivere nulla. [tool-verified: `provisa/api/admin/import_router.py`]

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

`flavor` è `"auto"` (rilevato dalla struttura dell'archivio), `"hasura_v2"`, o `"ddn"`.

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

Non viene persistito nulla. L'anteprima non viene messa in cache lato server; `apply` prende lo YAML fornito, quindi ciò che viene applicato è esattamente ciò che è stato revisionato (ed eventualmente modificato).

#### `POST /admin/import/hasura/apply`

Carica una configurazione precedentemente anteprima nell'org attiva. [tool-verified: `provisa/api/admin/import_router.py`]

**Corpo della richiesta:**

```json
{"config_yaml": "<yaml string>"}
```

Usa lo stesso percorso di hot-reload di `PUT /admin/config`. Il catalogo, gli schemi e i pool dell'org vengono ricostruiti prima che la risposta venga restituita.

---

### Interscambio Apache Ossie (REQ-1316, REQ-1321)

#### `GET /admin/ossie`

Esporta il modello governato dell'org come documento YAML Apache Ossie (incubating). (REQ-1321) [tool-verified: `provisa/api/admin/ossie_router.py`]

Il documento è derivato dallo stato live a ogni richiesta — mai in cache — quindi non può essere obsoleto. Le tabelle diventano oggetti `dataset`, le colonne diventano oggetti `field`, e le relazioni mappano su oggetti `relationship` di Ossie.

Restituisce `text/yaml` con `Content-Disposition: attachment; filename=provisa-ossie.yaml`.

#### `POST /admin/ossie/import`

Analizza un documento Ossie YAML o JSON e restituisce proposte di registrazione per tabelle e relazioni. (REQ-1316) [tool-verified: `provisa/api/admin/ossie_router.py`]

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

Non viene registrato nulla. Usare la schermata di revisione della UI admin per accettare o ridurre le proposte prima che scatti qualsiasi mutazione.

---

### Actions (funzioni e webhook)

Tutti gli endpoint sono sotto il prefisso `/admin/actions`. (REQ-205) [tool-verified: `provisa/api/admin/actions_router.py:24`]

Ogni invocazione — da GraphQL, SQL, Cypher, Bolt, Arrow Flight, MCP `run_sql`, e Provisa gRPC — passa attraverso un unico executor governato che applica `writable_by` e la governance in modo uniforme. (REQ-1156) [tool-verified: `provisa/api/data/action_exec.py`] Vedi [docs/integrations.md](integrations.md#invocare-comandi-tra-protocolli) per la sintassi di chiamata per protocollo.

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

Ogni oggetto webhook porta un booleano `approved`. Un webhook viene approvato quando uno steward esegue la sua richiesta di creazione (REQ-209); i webhook dichiarati in config sono auto-approvati. Un webhook non approvato è registrato ma non esposto su alcuna superficie. [tool-verified: `provisa/api/admin/actions_router.py:124-131`]

#### `POST /admin/actions/functions`

Registra una funzione tracciata (comando). (REQ-205) [tool-verified: `provisa/api/admin/actions_router.py:117`]

**Campi chiave:**

| Campo | Obbligatorio | Descrizione |
| --- | --- | --- |
| `name` | Sì | Nome comando univoco |
| `kind` | Sì | `"query"` → campo GraphQL Query; `"mutation"` → campo Mutation |
| `implKind` | No | Come viene eseguito il comando — vedi la tabella sotto (predefinito `source_procedure`) |
| `binding` | No | Dettagli di connessione specifici per `implKind` (oggetto JSON) |
| `returnSchema` | No | JSON Schema `{type:"array", items:{type:"object", properties:{...}}}` — rende il comando set-returning su ogni superficie |
| `arguments` | No | Definizioni argomento `[{name, type}]`; l'ordine posizionale conta per i chiamanti SQL e Bolt |
| `visibleTo` | No | Id di ruolo che possono chiamare il comando |
| `writableBy` | No | Id di ruolo autorizzati a invocarlo come mutation |
| `domainId` | No | Dominio per il posizionamento GraphQL e il controllo accessi |

**Valori `implKind`:**

| `implKind` | Cosa viene eseguito | Campi `binding` |
| --- | --- | --- |
| `source_procedure` | Stored procedure su un'origine dati registrata (predefinito) | `sourceId`, `schemaName`, `functionName` |
| `script` | Script lato server | `script` |
| `http` | Chiamata HTTP in uscita | `url`, `method` |
| `grpc` | Chiamata gRPC in uscita verso un server esterno | `target`, `method` |
| `python` | Callable Python ospitato da Provisa (REQ-885) | `callable` (es. `"demo.py_functions:random_dataset"`) |

I comandi demo `random_python_set` (`implKind: python`) e `random_grpc_set` (`implKind: grpc`) mostrano nella pratica comandi set-returning con `returnSchema`; entrambi sono in `config/provisa-install.yaml`. [tool-verified: `config/provisa-install.yaml:809-856`]

#### `PUT /admin/actions/functions/{name}`

Aggiorna una funzione tracciata per nome. [tool-verified: `provisa/api/admin/actions_router.py:182`]

#### `DELETE /admin/actions/functions/{name}`

Elimina una funzione tracciata per nome. [tool-verified: `provisa/api/admin/actions_router.py:233`]

#### `POST /admin/actions/webhooks`

Registra un webhook tracciato. (REQ-209) La registrazione o l'aggiornamento di un webhook mette in coda una richiesta di approvazione dello steward — il webhook diventa attivo su tutte le superfici solo dopo che uno steward lo approva. I webhook dichiarati in config sono auto-approvati. **Campi del corpo della richiesta:** `name`, `url`, `method`, `timeoutMs`, `returns`, `inlineReturnType`, `arguments`, `visibleTo`, `domainId`, `description`, `kind`. [tool-verified: `provisa/api/admin/actions_router.py:132`, `provisa/api/admin/actions_router.py:325-331`]

#### `PUT /admin/actions/webhooks/{name}`

Aggiorna un webhook tracciato per nome. Qualsiasi modifica riporta l'approvazione a pending finché non viene riapprovato. [tool-verified: `provisa/api/admin/actions_router.py:306`]

#### `DELETE /admin/actions/webhooks/{name}`

Elimina un webhook tracciato per nome. [tool-verified: `provisa/api/admin/actions_router.py:355`]

#### `POST /admin/actions/test`

Testa un'action (funzione o webhook) per nome. (REQ-245) [tool-verified: `provisa/api/admin/actions_router.py:384`]

---

### Ruoli

Tutti gli endpoint sono sotto il prefisso `/admin/roles`. [tool-verified: `provisa/api/admin/roles_router.py:18`]

| Metodo | Percorso | Descrizione |
| --- | --- | --- |
| `GET` | `/admin/roles/` | Elenca tutti i ruoli |
| `POST` | `/admin/roles/` | Crea un ruolo |
| `PUT` | `/admin/roles/{role_id}` | Aggiorna un ruolo |
| `DELETE` | `/admin/roles/{role_id}` | Elimina un ruolo |

[tool-verified: `provisa/api/admin/roles_router.py`]

---

### Utenti

Tutti gli endpoint sono sotto il prefisso `/admin/users`. [tool-verified: `provisa/api/admin/local_users_router.py:21`]

| Metodo | Percorso | Descrizione |
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

| Metodo | Percorso | Descrizione |
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

| Metodo | Percorso | Descrizione |
| --- | --- | --- |
| `POST` | `/admin/invites/` | Crea un invito |
| `GET` | `/admin/invites/` | Elenca gli inviti in sospeso |
| `DELETE` | `/admin/invites/{token}` | Revoca un invito |

---

### GraphQL Admin

#### `POST /admin/graphql`

Endpoint GraphQL Strawberry per tutte le operazioni admin: CRUD di origini dati e tabelle, gestione delle relazioni, configurazione dei domini, regole RLS, controllo cache, convenzioni di denominazione, gestione dei task pianificati e compilazione delle query. (REQ-164) [tool-verified: `provisa/api/app.py:2171`]

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
| 422 | Header obbligatorio mancante (es. `X-Role`) |
| 503 | Database o origine dati non connessa; dipendenza non disponibile |
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

Query e discovery del catalogo sono entrambe disponibili sulla stessa connessione. L'intera pipeline di governance (RLS, mascheramento, campionamento) viene applicata a ogni query. (REQ-130, REQ-143)

**Formato ticket** (JSON):

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

Quando il proxy Zaychik Flight SQL è disponibile (porta 8480), i record batch vengono trasmessi in streaming end-to-end senza materializzazione completa. (REQ-144) Ripiega sulla materializzazione tramite il livello di query federata se Zaychik non è disponibile. (REQ-146)

---

## Endpoint gRPC Protobuf

Porta `50051` (sovrascrivibile con la variabile d'ambiente `GRPC_PORT` o la configurazione `server.grpc_port`). (REQ-529) [tool-verified: `provisa/grpc/server.py`, `provisa/api/app.py`]

Passare il ruolo nella chiave dei metadati gRPC `x-provisa-role`. Se assente, il server abortisce con `UNAUTHENTICATED`. [tool-verified: `provisa/grpc/server.py`]

Scaricare il proto specifico per ruolo da `GET /data/proto/{role_id}`. Appaiono solo le tabelle e le colonne visibili a quel ruolo. (REQ-039)

```proto
service ProvisaService {
  rpc QueryOrders (QueryOrdersRequest) returns (stream Orders);
  rpc InsertOrders (InsertOrdersRequest) returns (InsertOrdersResponse);
}
```

Ogni tabella produce un RPC streaming `Query{TypeName}`. Gli RPC `Insert{TypeName}` esistono per simmetria dello schema ma abortiscono con `UNIMPLEMENTED`. [tool-verified: `provisa/grpc/server.py`]

`grpc_reflection.v1alpha` è abilitato per la discovery dei servizi senza un proto precompilato. (REQ-529) [tool-verified: `provisa/grpc/reflection.py`]

```bash
grpcurl -plaintext localhost:50051 list
grpcurl -plaintext -H 'x-provisa-role: analyst' \
  -d '{}' localhost:50051 ProvisaService/QueryOrders
```

Il server gRPC si avvia solo quando un proto valido può essere compilato all'avvio. Se la build dello schema fallisce, il server gRPC non si avvia. (REQ-529)

#### RPC di aggregazione e group-by (REQ-1359, REQ-1361, REQ-1405)

Quando una tabella ha `enable_aggregates` impostato, il proto generato include due RPC aggiuntivi accanto a `Query{TypeName}`:

- **`Query{TypeName}Aggregate`** — restituisce scalari di aggregazione per la tabella (`count`; `sum`, `avg`, `stddev`, `variance` per colonna numerica; `min`, `max` per colonna comparabile)
- **`Query{TypeName}GroupBy`** — restituisce una riga per chiave di gruppo con sotto-campi di aggregazione e, opzionalmente, scalari della tabella base e righe di dimensione unita in un campo `nodes`

Entrambi passano attraverso la stessa pipeline di aggregazione del compilatore usata dai campi root GraphQL `{field}_aggregate` e `{field}_group_by` — nessuna implementazione di aggregazione separata. (REQ-1359) [tool-verified: `provisa/grpc/query_ir.py:133-196`]

**Campo `funcs` (REQ-1361).** Il messaggio di richiesta accetta un campo `funcs` di tipo stringa ripetuta. I valori validi sono `count`, `sum`, `avg`, `stddev`, `variance`, `min` e `max`. Quando `funcs` viene omesso, viene richiesta ogni funzione che lo schema espone per quella tabella. Quando è impostato, appaiono solo le funzioni nominate. Se nessuna delle funzioni nominate si applica ai tipi di colonna della tabella, la query ripiega su `count`. [tool-verified: `provisa/grpc/query_ir.py:66`, `provisa/grpc/query_ir.py:75-97`]

**Campi `include_nodes` e `include` (REQ-1405).** Le richieste `Query{TypeName}GroupBy` possono impostare `include_nodes: true` per includere le colonne scalari della tabella base nel campo `nodes` di ogni riga. Il campo stringa ripetuta `include` nomina i campi relazione many-to-one le cui colonne scalari vengono anch'esse annidate dentro `nodes`. Questo corrisponde al comportamento `?includeNodes=` / `?include=` di JSON:API. [tool-verified: `provisa/grpc/query_ir.py:168-195`]

---

## Driver JDBC

Il driver JDBC Provisa (`provisa-jdbc-0.1.0.jar`) espone il catalogo semantico agli strumenti BI (Tableau, PowerBI, DBeaver). (REQ-126)

**URL di connessione:** `jdbc:provisa://host:port` (REQ-131)

I domini mappano su schemi JDBC. (REQ-127) Le tabelle usano i propri alias registrati. Le colonne usano alias ed espongono le descrizioni come `REMARKS`. (REQ-128) I metodi standard di metadati (`getPrimaryKeys`, `getImportedKeys`, `getExportedKeys`) espongono le relazioni semantiche come metadati PK/FK.

**Supporto SQL:** `SELECT * FROM <alias> [WHERE col = 'value']`. (REQ-129)

Il driver richiede il redirect Arrow IPC per impostazione predefinita. I risultati vengono trasmessi in streaming batch-by-batch via `ArrowStreamReader`, limitati a un record batch in memoria. (REQ-293)

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

## Subscriptions

Le subscription SSE sono disponibili su `GET /data/subscribe/{table}`. (REQ-219, REQ-258) La consegna delle notifiche usa un provider pluggable selezionato per tipo di origine: le origini PostgreSQL usano `LISTEN/NOTIFY`, le origini MongoDB usano Change Streams, e le origini Kafka usano consumer group. Il filtraggio RLS e la validazione dello schema si applicano indipendentemente dal provider. Sono supportate anche origini WebSocket e RSS tramite lo stesso endpoint. (REQ-338, REQ-342) [tool-verified: `provisa/api/data/subscribe.py:239`, `provisa/subscriptions/registry.py`, `provisa/api/app.py` `_rebuild_schemas`]

---

## Glossario aziendale (REQ-1387)

Il glossario aziendale mappa i nomi dei campi fisici — così come esistono nei database di origine — su un vocabolario umano condiviso. Ogni colonna registrata nel livello semantico ottiene automaticamente un termine. Non è richiesto alcun inserimento manuale per popolare il glossario; i curatori aggiungono definizioni, relazioni ed esperti sopra ciò che il sistema deriva.

### Come vengono derivati i termini

Quando Provisa registra o aggiorna le colonne di una tabella, `normalize_term` (`provisa/core/glossary.py`) viene eseguito su ogni nome di colonna e produce una frase canonica. [tool-verified: `provisa/core/repositories/glossary.py:sync_table_refs`]

La normalizzazione applica cinque regole in sequenza:

1. Divide sui confini camelCase e sui caratteri separatori (`_`, `-`, `.`, `/`, spazio).
2. Converte il risultato in minuscolo.
3. Espande una tabella fissa di abbreviazioni (es. `cust` → `customer`, `amt` → `amount`, `dt` → `date`, `id` → `identifier`, `key` → `identifier`, `guid` → `identifier`).
4. Rimuove un **token proxy** finale (`identifier`, `code`, `index`, o `reference`) — una colonna nominata per la sua chiave o codice punta al concetto sottostante attraverso un valore surrogato, quindi il termine dovrebbe essere il concetto stesso. L'ultimo token rimanente non viene mai rimosso.
5. Qualifica una **frase troppo generica** con il concetto della tabella. Quando la frase normalizzata completa è una semplice parola attributo (`name`, `identifier`, `date`, `location`, `message`, `first name`, `last name`, e simili), il termine diventa `<concetto tabella> <frase>` — `employees.first_name` → `employee first name`, `orders.id` → `order identifier`. Un unico termine condiviso `name` tra tabelle non correlate fonderebbe significati distinti; la qualificazione collega invece ogni colonna al proprio concetto racchiudente. Il concetto della tabella è il nome di business della tabella, normalizzato con un sostantivo testa singolare (`order_lines` → `order line`).

Le pseudo-colonne di filtro nativo (prefisso `_nf_`, o qualsiasi colonna che porta `native_filter_type`) sono meccanismi di parametro di query, non campi di business, e non derivano alcun termine.

Poiché `id`, `key`, `pk`, e `sk` si espandono tutti in `identifier` prima del controllo proxy, tre nomi di colonna fisicamente diversi atterrano esattamente sullo stesso termine:

| Nome fisico | Dopo la normalizzazione |
| --- | --- |
| `cust_id` | `customer` |
| `customerId` | `customer` |
| `CUSTOMER_KEY` | `customer` |
| `txn_amt` | `transaction amount` |

I primi tre collassano in un unico termine. `transaction amount` mantiene entrambi i token perché `amount` non è un proxy. Una colonna `id` semplice — senza token precedenti — non può essere rimossa; si normalizza in `identifier` così che il termine non sia vuoto. [tool-verified: `provisa/core/glossary.py:normalize_term`]

### Ciclo di vita

I termini sono **derivati dall'appartenenza al livello semantico**, non creati su richiesta dagli utenti. Il repository delle tabelle è l'unico percorso di scrittura: `sync_table_refs` viene eseguito dentro ogni upsert di set di colonne, e `sweep_refless_terms` viene eseguito dopo ogni percorso di eliminazione. [tool-verified: `provisa/core/repositories/glossary.py`]

**Quando viene aggiunta una colonna:** Provisa cerca il termine normalizzato per nome. Se esiste già, la colonna ottiene un riferimento a esso (e se il termine era deprecato, viene ripristinato — `deprecated` viene reimpostato a `False`). Se non esiste ancora alcun termine, ne viene creato uno.

**Quando una colonna esce** (cambio di schema o rimozione della tabella): il suo riferimento viene eliminato e il termine viene **sistemato** secondo una regola rimuovi-o-deprezza. Un termine radicato senza riferimenti rimanenti viene rimosso definitivamente — insieme ai suoi archi e alle assegnazioni di esperti — a meno che rimuoverlo non lasci un termine astratto disconnesso da tutti i termini radicati (nessun percorso attraverso il grafo dei termini). In tal caso, il termine viene **deprecato** (contrassegnato `deprecated=True`) anziché eliminato, così l'ancora del grafo del termine astratto sopravvive.

I termini astratti non vengono mai rimossi automaticamente; esistono al di fuori del ciclo di vita fisico e vengono eliminati solo esplicitamente tramite l'API admin.

**Ripristino:** se il nome normalizzato di un termine deprecato riappare (una colonna viene ri-registrata), il termine viene dedeprecato e i suoi riferimenti riprendono ad accumularsi.

### Endpoint di curatela

Tutti gli endpoint sono sotto `/admin/glossary`. Richiedono accesso `org_admin` e un'org configurata. Ogni mutation innesca una pubblicazione dei metadati. [tool-verified: `provisa/api/admin/glossary_router.py`]

| Metodo | Percorso | Descrizione |
| --- | --- | --- |
| `GET` | `/admin/glossary/terms` | Elenca i termini. Parametri di query: `q` (ricerca su nome/definizione), `include_deprecated` (predefinito `true`) |
| `GET` | `/admin/glossary/terms/{term_id}` | Ottiene il dettaglio del termine: definizione, riferimenti fisici, archi tipizzati, esperti |
| `POST` | `/admin/glossary/terms` | Crea un termine astratto — vocabolario utente senza riferimenti fisici |
| `PATCH` | `/admin/glossary/terms/{term_id}` | Rinomina, imposta la definizione, o attiva/disattiva l'esclusione dall'export |
| `DELETE` | `/admin/glossary/terms/{term_id}` | Elimina un termine senza riferimenti fisici |
| `POST` | `/admin/glossary/refs/move` | Sposta un riferimento fisico su un termine diverso (consolidamento) |
| `POST` | `/admin/glossary/terms/{term_id}/edges` | Aggiunge un arco di relazione tipizzato tra due termini |
| `DELETE` | `/admin/glossary/terms/{term_id}/edges` | Rimuove un arco (parametri di query: `to_term_id`, `rel_type`) |
| `POST` | `/admin/glossary/terms/{term_id}/experts` | Tagga un utente come esperto o autore per un termine |
| `DELETE` | `/admin/glossary/terms/{term_id}/experts/{user_id}` | Rimuove la designazione di esperto/autore di un utente |
| `POST` | `/admin/glossary/terms/{term_id}/definition/generate` | Genera una bozza di definizione per un termine usando il modello IA dell'org — restituisce solo testo, nulla viene persistito finché non viene salvato |
| `POST` | `/admin/glossary/definitions/generate` | Genera e persiste le definizioni per ogni termine che non ne ha — non sovrascrive mai il testo redatto da un umano |
| `POST` | `/admin/glossary/relationships/generate` | Propone e persiste archi tipizzati sull'intero glossario usando il modello IA dell'org |

**Corpo di `POST /admin/glossary/terms`:**

```json
{"name": "revenue", "definition": "Recognized net revenue after returns and discounts."}
```

**Corpo di `POST /admin/glossary/terms/{term_id}/edges`:**

```json
{"to_term_id": 42, "rel_type": "KIND_OF"}
```

Valori `rel_type` validi: `KIND_OF`, `RELATED_TO`, `PART_OF`, `SYNONYM_OF`. [tool-verified: `provisa/core/glossary.py:TERM_EDGE_TYPES`]

**Corpo di `POST /admin/glossary/terms/{term_id}/experts`:**

```json
{"user_id": "alice@example.com", "kind": "author"}
```

Valori `kind` validi: `expert`, `author`. [tool-verified: `provisa/core/repositories/glossary.py:add_expert`]

**Corpo di `POST /admin/glossary/refs/move`:**

```json
{"table_id": 7, "column_name": "cust_id", "to_term_id": 12}
```

Spostare un riferimento sistema il termine perdente secondo la regola rimuovi-o-deprezza. Usare questo endpoint per consolidare due termini che la normalizzazione ha mantenuto separati — per esempio, dopo che un'origine dati usa un'abbreviazione non standard rimasta fuori dalla tabella di espansione.

Eliminare un termine radicato (con riferimenti fisici) restituisce `400 glossary.invalid`. Rimuovere o spostare prima tutti i riferimenti.

**Campo `export_excluded` di `PATCH /admin/glossary/terms/{term_id}`:**

```json
{"export_excluded": true}
```

Impostare `export_excluded` a `true` trattiene il termine da tutti gli snapshot di export dei metadati, indipendentemente dai suoi riferimenti fisici o dal suo stato astratto. Reimpostarlo a `false` ripristina il termine nello snapshot alla successiva pubblicazione. I dati di curatela (definizione, archi, esperti) non vengono influenzati. [tool-verified: `provisa/core/repositories/glossary.py:set_export_excluded`, `provisa/api/admin/glossary_router.py:update_term`]

### Curatela assistita da IA

Il modello IA configurato dall'org può redigere definizioni e proporre archi di relazione sull'intero glossario in un'unica operazione. Entrambe le azioni bulk richiedono accesso `org_admin` e un'org configurata.

**`POST /admin/glossary/definitions/generate`**

Itera ogni termine nel glossario, salta quelli che hanno già una definizione, e chiama il modello IA dell'org per redigerne una per ogni termine rimanente. La bozza viene persistita immediatamente — a differenza dell'endpoint di bozza per singolo termine (`POST /admin/glossary/terms/{term_id}/definition/generate`), non c'è alcun passaggio di editing. Le definizioni redatte da un umano non vengono mai sovrascritte: la guardia è `if summary["definition"]: continue` prima di qualsiasi chiamata al modello. Una singola notifica di pubblicazione copre l'intero batch. [tool-verified: `provisa/api/admin/glossary_router.py:generate_all_definitions`]

Risposta:

```json
{"generated": 12}
```

`generated` è il conteggio dei termini che hanno ricevuto una nuova definizione. È zero quando ogni termine ne ha già una.

**`POST /admin/glossary/relationships/generate`**

Invia l'elenco completo dei termini al modello IA dell'org con un prompt che specifica i dieci tipi di arco consentiti (`KIND_OF`, `PART_OF`, `SYNONYM_OF`, `RELATED_TO`, `VALID_VALUE_OF`, `DERIVED_FROM`, `REPLACES`, `PREFERRED_TERM_FOR`, `TRANSLATION_OF`, `ANTONYM_OF`) e chiede solo proposte con alta confidenza. Il modello risponde con un array JSON; ogni voce viene validata prima di qualsiasi scrittura: nomi di termine sconosciuti, auto-archi e tipi di arco fuori dall'enum chiuso vengono scartati silenziosamente. Le proposte valide vengono upsertate in modo idempotente — rieseguire l'azione non duplica gli archi. Una singola notifica di pubblicazione copre il batch. L'endpoint restituisce `{"added": 0}` immediatamente quando il glossario contiene meno di due termini non deprecati. [tool-verified: `provisa/api/admin/glossary_router.py:generate_relationships`]

Risposta:

```json
{"added": 5}
```

`added` è il conteggio degli archi scritti. Un arco già esistente conta comunque — l'upsert riesce, ma i dati dell'arco non cambiano.

### Strumento MCP `search_terms`

```
search_terms(query, role=None, limit=25)
```

Cerca nei nomi e nelle definizioni dei termini con una corrispondenza substring case-insensitive, fino a `limit` risultati. Ogni risultato è il dettaglio completo del termine: `name`, `definition`, `is_abstract`, `deprecated`, riferimenti fisici (con `source_id`, `schema_name`, `table_name`, `column_name`), archi tipizzati e assegnazioni di esperti. [tool-verified: `provisa/api/mcp/server.py:236-244`, `provisa/core/repositories/glossary.py:search_terms`]

Usare `search_terms` prima di scrivere SQL per trovare ogni campo fisico che rappresenta un concetto per nome. Per esempio, cercare `"order date"` restituisce il termine e tutte le colonne `order_dt`, `orderDate`, `ORDER_DATE` in ogni tabella registrata.

### Export dei metadati

Il grafo dei termini del glossario è incluso in ogni `MetadataSnapshot` costruito da `build_snapshot`. [tool-verified: `provisa/api/metadata_export/builder.py:_glossary_assets`]

L'export applica gli stessi filtri del resto dello snapshot:

- Un termine contrassegnato `export_excluded` viene trattenuto del tutto — indipendentemente dai suoi riferimenti fisici, dal suo stato astratto, o dal fatto che il catalogo dell'org sia configurato. [tool-verified: `provisa/api/metadata_export/builder.py:_glossary_assets`]
- Un termine radicato viene pubblicato solo quando almeno uno dei suoi riferimenti fisici appartiene a una colonna che supera sia il filtro **Data Product** (il flag `data_product` della tabella deve essere `true`) sia il filtro colonna **technical** (le colonne taggate `technical` vengono trattenute).
- Un termine radicato i cui riferimenti sono tutti trattenuti da quei filtri viene trattenuto con essi.
- I termini astratti vengono pubblicati incondizionatamente — sono vocabolario utente, non vincolato a colonne fisiche.
- Un arco tra due termini viene pubblicato solo quando entrambi i termini endpoint vengono pubblicati.

Ogni adapter vendor pubblica il grafo dei termini in modo nativo, in un contenitore glossario di proprietà di Provisa che crea in modo idempotente — mai in un glossario di catalogo esistente:

| Provider | Contenitore | Termini | Relazioni | Deprecazione |
| --- | --- | --- | --- | --- |
| Apache Atlas | "Provisa Glossary" (API glossary) | termini del glossario, definizione su `longDescription` | KIND_OF → `isA`, SYNONYM_OF → `synonyms`, RELATED_TO/PART_OF → `seeAlso` | marcatore `[DEPRECATED]` in shortDescription |
| Atlan | Glossario Provisa per qualifiedName stabile | `longDescription` (mai la `userDescription` modificata dall'uomo) | stesso mapping Atlas | `certificateStatus = DEPRECATED` |
| DataHub | `urn:li:glossaryNode:provisa.<org>` | aspetto `glossaryTermInfo` per termine | KIND_OF → Inherits, PART_OF → Contains (invertito), RELATED_TO/SYNONYM_OF → termini correlati | aspetto di deprecazione; i rename seguono la successione URN |
| OpenMetadata | Glossario Provisa via `/v1/glossaries` | PUT per fqn, i rename fanno PATCH-rebind per UUID memorizzato | KIND_OF → gerarchia genitore nativa, SYNONYM_OF → `synonyms`, altri → `relatedTerms` | `entityStatus` |
| Collibra | Dominio di tipo Glossary "Provisa Glossary" | Asset Business Term via l'Import API | tipi di relazione Business Term nativi | stato dell'asset |

La proprietà è il binding, non il nome: l'id vendor di ogni termine pubblicato viene catturato in `catalog_bindings` sotto l'URN del termine (`provisa://<org>/terms/<name>`), e Provisa modifica o elimina un elemento glossario lato vendor solo quando detiene quel binding (o l'elemento vive nel contenitore di proprietà di Provisa che ha creato). Un elemento glossario senza binding Provisa ha avuto origine nel sistema esterno e non viene mai toccato; gli aggiornamenti fanno read-merge così i campi aggiunti dallo steward sui termini propri di Provisa sopravvivono; nulla viene eliminato quando un termine lascia lo snapshot. Le assegnazioni termine-asset dello steward restano di proprietà esterna — nessun adapter scrive assegnazioni termine-asset (la pubblicazione delle assegnazioni redatte da Provisa è un follow-on esplicito). Su Collibra in particolare, la sicurezza sotto la semantica REPLACE dell'Import API si basa sul contenimento: il payload menziona solo asset all'interno del dominio glossario Provisa e istanze di relazione solo tra termini Provisa, così i glossari dello steward e le loro relazioni non sono mai raggiungibili. [tool-verified: `provisa/api/metadata_export/atlan.py`, `provisa/api/metadata_export/datahub.py`, `provisa/api/metadata_export/atlas.py`, `provisa/api/metadata_export/openmetadata.py`]

---

## Prodotti Dati (REQ-1634)

Un prodotto dati raggruppa tabelle pubblicate insieme per il consumo, di proprietà di esattamente un dominio. I campi seguono il vocabolario ODPS (Open Data Product Standard) laddove Provisa possiede già la fonte di verità. La UI admin espone i Data Products sotto **Admin → Data Products**. [tool-verified: `provisa/core/models.py:318-342`, `provisa/api/admin/schema_mutation.py:949-1017`, `provisa/api/admin/schema_query.py:352-362`]

### Capability

| Capability | Concede |
| --- | --- |
| `data_product_read` | Accesso in lettura al campo di query `data_products` e alla pagina admin Data Products. Assegnata di default a `org_admin`, `analyst`, `developer`, e `modeler`. |
| `data_product_rw` | Mutation di creazione ed eliminazione. Abilita i controlli New / Edit / Delete nella UI. |

[tool-verified: `provisa/api/admin/schema_mutation.py:959,1001`, `provisa/api/admin/schema_query.py:357`]

### GraphQL Admin

Tutte le operazioni sui data product passano attraverso `POST /admin/graphql`.

**Query:**

```graphql
query {
  data_products {
    id
    domain_id
    name
    owner_role
    team_role
    purpose
    limitations
    usage
    version
    status
    sla
    support
    custom_properties
  }
}
```

Richiede `data_product_read`.

**Crea o aggiorna:**

```graphql
mutation {
  create_data_product(input: {
    id: "customer_360"
    domain_id: "sales"
    name: "Customer 360"
    owner_role: "data-product-owner"
    team_role: "sales-analytics"
    purpose: "Single view of a customer across all touchpoints."
    status: "active"
    version: "1.0.0"
  }) {
    success
    message
  }
}
```

`create_data_product` fa upsert — chiamarla con un `id` esistente aggiorna il record. Richiede `data_product_rw`.

**Elimina:**

```graphql
mutation {
  delete_data_product(id: "customer_360") {
    success
    message
  }
}
```

Eliminare un prodotto cancella `product_id` da ogni tabella membro, rimuovendone l'appartenenza. Richiede `data_product_rw`. [tool-verified: `provisa/api/admin/schema_mutation.py:995-1017`]

### Schema dei campi

| Campo | Tipo | Obbligatorio | Note |
| --- | --- | --- | --- |
| `id` | `String` | Sì | Identificatore stabile leggibile dalla macchina, es. `customer_360` |
| `domain_id` | `String` | Sì | Dominio proprietario. Le tabelle membro devono condividere questo `domain_id` — le discordanze vengono rifiutate al salvataggio |
| `name` | `String` | Sì | Nome visualizzato |
| `owner_role` | `String` | No | Ruolo responsabile di questo prodotto; distinto dallo steward di dominio |
| `team_role` | `String` | No | Ruolo i cui titolari mantengono questo prodotto giorno per giorno; si risolve in individui |
| `purpose` | `String` | No | Cosa pubblica questo prodotto e perché |
| `limitations` | `String` | No | Vincoli, avvertenze o esclusioni noti |
| `usage` | `String` | No | Come consumare questo prodotto |
| `version` | `String` | No | es. `1.2.0` |
| `status` | `String` | No | es. `proposed`, `active`, `deprecated`, `retired` |
| `sla` | `String` | No | Impegni a livello di servizio; prosa — un prodotto abbraccia più tabelle e uno SLA strutturato non può nominare senza ambiguità quale membro descrive |
| `support` | `String` | No | Indicazioni di supporto in testo libero |
| `custom_properties` | `JSON` | No | Metadati chiave-valore arbitrari non coperti dai campi standard |

Due campi aggiuntivi esistono sul modello ma non sono esposti nel `DataProductType` / `DataProductInput` di Strawberry — sono specifici di Snowflake Horizon Catalog (REQ-1635):

| Campo | Note |
| --- | --- |
| `support_contact` | Email o URL; richiesto dai manifest di organization listing di Horizon Catalog |
| `publish` | `true` per pubblicare immediatamente i listing Horizon; i nuovi listing sono DRAFT per impostazione predefinita |

[tool-verified: `provisa/core/models.py:338-341`, `provisa/api/admin/types.py:104-118,538-551`]

### Appartenenza delle tabelle

Una tabella si unisce a un prodotto dati impostando il proprio campo `product_id` nel form di modifica tabella. Il selettore è scoped ai prodotti il cui `domain_id` corrisponde al dominio proprio della tabella — una tabella nel dominio `marketing` non viene mai offerta per un prodotto nel dominio `sales`. [tool-verified: `provisa/api/admin/actions_router.py:244-260`, `docs/arch/requirements.yaml:54585-54586`]

Anche i comandi nello stesso dominio possono essere assegnati come membri. [tool-verified: `provisa-ui/src/i18n/locales/en/dataProductsTab.json:commandsLabel`]

### Filtro di export dei metadati

`build_snapshot` applica `data_products_only=True` per ogni pubblicazione su catalogo. Le tabelle senza un `product_id` vengono trattenute dallo snapshot, insieme ai loro archi di relazione, archi di lineage e tag di governance. Le origini dati e i domini vengono sempre pubblicati. I termini del glossario vengono pubblicati solo quando almeno uno dei loro riferimenti fisici appartiene a una tabella esportata (membro di un prodotto). [tool-verified: `provisa/api/metadata_export/builder.py:594,609,641`]

Un prodotto senza membri esportati non costruisce una voce di snapshot — un listing senza membri rappresenterebbe erroneamente il prodotto al catalogo. [tool-verified: `provisa/api/metadata_export/model.py:106-113`]

### Supporto data product per catalogo target

`MetadataSnapshot.data_products` raggiunge ogni adapter, ma solo gli adapter la cui piattaforma ha un concetto nativo di data product lo pubblicano come entità di prima classe; gli altri pubblicano le tabelle membro (già filtrate sopra) senza un raggruppamento di prodotto.

| Target | Rappresentazione del data product |
| --- | --- |
| Snowflake Horizon | Ogni prodotto diventa una `SHARE` sugli indirizzi fisici delle sue tabelle membro, avvolta in una `CREATE ORGANIZATION LISTING` interna — un Data Product nativo di Horizon Catalog. `publish=true` rende il listing live immediatamente; altrimenti atterra come DRAFT. [tool-verified: `provisa/api/metadata_export/snowflake_horizon.py:21-34,389-418`] |
| BigQuery Dataplex | Ogni prodotto diventa un listing di Analytics Hub via `/v1/dataProducts`. [tool-verified: `provisa/api/metadata_export/bigquery_dataplex.py:100,136,159`] |
| OpenMetadata | Ogni prodotto diventa un'entità `DataProduct` nativa (`/api/v1/dataProducts`), con proprietà derivata dal dominio. [tool-verified: `provisa/api/metadata_export/openmetadata.py:326-344,635`] |
| DataHub | Ogni prodotto diventa un'entità `dataProduct` nativa (`urn:li:dataProduct:...`) con propri aspetti `dataProductProperties`/proprietà. [tool-verified: `provisa/api/metadata_export/datahub.py:133-136,443-483`] |
| Collibra | Ogni prodotto diventa un asset di un community type `Data Product`, correlato alle sue tabelle membro tramite una relazione `Data Product groups Table`. [tool-verified: `provisa/api/metadata_export/collibra.py:129-133,371-388`] |
| Atlan | Pubblicato come ipotesi di typedef personalizzato `DataProduct` — Atlan non ha un nome di tipo stabile documentato per questo concetto, quindi il mapping è best-effort. [tool-verified: `provisa/api/metadata_export/atlan.py:60`] |
| Apache Atlas | Pubblicato come typedef personalizzato `provisa_data_product` con una relazione `provisa_data_product_members` — Atlas non ha un tipo di entità data product nativo. [tool-verified: `provisa/api/metadata_export/atlas.py:134-147,191,256-260`] |
| OpenLineage | Non è un'entità di prima classe — le tabelle membro portano un facet personalizzato `provisa_data_product` che nomina il prodotto proprietario. [tool-verified: `provisa/api/metadata_export/openlineage.py:243,348`] |
