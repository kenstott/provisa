# API-Referenz

## Überblick

Provisa stellt REST-Endpunkte unter zwei Präfixen bereit: `/data` für Abfrageausführung und Schema-Introspektion sowie `/admin` für die Konfigurationsverwaltung. (REQ-043) Die meisten Daten-Endpunkte erfordern eine Rollen-ID. Admin-Konfigurationsoperationen verwenden eine Strawberry-GraphQL-API unter `/admin/graphql`. (REQ-164)

---

## Authentifizierung

Wenn `auth.provider` in `provisa.yaml` konfiguriert ist, erfordern alle Endpunkte außer `/health` und `/setup/status` einen `Authorization: Bearer <token>`-Header. (REQ-120) [tool-verified: `provisa/api/app.py`, `provisa/auth/wiring.py`]

Ohne konfigurierte Authentifizierung läuft der Server im Dev-Modus. Jede Anfrage wird als `anonymous`-Identität behandelt, die auf alle konfigurierten Rollen mit Wildcard-Domänenzugriff abgebildet wird. (REQ-535)

**Login (`POST /auth/login`)** wird vom aktiven Auth-Provider bereitgestellt, wenn `provider: basic` konfiguriert ist. (REQ-124) Anmeldeformat und Antwort hängen vom Provider ab.

**Identitäts-Introspektion:**

```http
GET /auth/me
```

Gibt die ID, E-Mail, den Anzeigenamen, die Organisationsmitgliedschaften und Rollenzuweisungen des authentifizierten Benutzers zurück. Im Dev-Modus wird `dev_mode: true` mit allen aufgelisteten Rollen-IDs zurückgegeben. [tool-verified: `provisa/api/auth_router.py`]

```http
GET /auth/provider-type
```

Gibt `{"provider": "<name>"}` oder `{"provider": null}` zurück, wenn keine Authentifizierung konfiguriert ist. [tool-verified: `provisa/api/auth_router.py`]

---

## Daten-Endpunkte

### `POST /data/graphql`

Führt eine GraphQL-Abfrage oder -Mutation aus. (REQ-043) [tool-verified: `provisa/api/data/endpoint.py:151`]

**Request-Body:**

```json
{
  "query": "{ orders(where: {region: {eq: \"us\"}}) { id amount } }",
  "variables": {},
  "role": "admin",
  "extensions": {}
}
```

Das Feld `role` wird nur im Dev-Modus (ohne Authentifizierung) verwendet. Wenn Authentifizierung aktiv ist, wird die Rolle des authentifizierten Benutzers verwendet und `role` im Body wird ignoriert.

Das Feld `extensions` unterstützt das Automatic-Persisted-Query-Protokoll (APQ): (REQ-288)

```json
{
  "extensions": {"persistedQuery": {"sha256Hash": "<sha256-of-query>"}}
}
```

**Headers:**

- `X-Provisa-Role` — Rolle überschreiben (Dev-Modus)
- `Accept` — Antwortformat (siehe Content Negotiation)
- `Authorization` — `Bearer <token>`, wenn Authentifizierung aktiviert ist
- `X-Provisa-Redirect-Format` — MIME-Typ für S3-Redirect-Ausgabe (REQ-137)
- `X-Provisa-Redirect-Threshold` — Zeilenanzahl, ab der ein Redirect ausgelöst wird (REQ-137)
- `X-Provisa-Redirect` — `true`, um einen Redirect bedingungslos zu erzwingen (REQ-029)

**Antwort (JSON inline):**

```json
{
  "data": {
    "orders": [
      {"id": 1, "amount": 99.99}
    ]
  }
}
```

**Antwort (Redirect):**

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

**Antwort (Multi-Root mit gemischtem Inline/Redirect):**

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

Multi-Root-Abfragen führen jedes Root-Feld unabhängig aus. Felder unterhalb des Redirect-Schwellenwerts werden inline zurückgegeben; Felder darüber werden per Redirect ausgeliefert. Der Schlüssel `redirects` (Plural) ordnet Feldnamen den Redirect-Informationen zu. (REQ-029) [tool-verified: `provisa/api/data/endpoint.py`]

**Cache-Header:**

- `X-Provisa-Cache: HIT|MISS` (REQ-536)
- `X-Provisa-Cache-Age: <seconds>` (bei HIT) (REQ-536)

**Erforderliche Capabilities:** `QUERY_DEVELOPMENT` für alle Anfragen einschließlich Introspektion. [tool-verified: `provisa/api/data/endpoint.py:186-283`]

---

### Content Negotiation

| Accept-Header | Format |
| --- | --- |
| `application/json` | JSON (Standard) |
| `application/x-ndjson` | Newline-delimited JSON |
| `text/csv` | CSV |
| `application/vnd.apache.parquet` | Parquet |
| `application/vnd.apache.arrow.stream` | Arrow IPC |

(REQ-047, REQ-048, REQ-049, REQ-050) [tool-verified: `provisa/api/data/endpoint.py:84-90`]

---

### Redirect

Ergebnisse oberhalb eines konfigurierten Zeilen-Schwellenwerts (oder bei `X-Provisa-Redirect: true`) werden nach S3 geschrieben, und es wird eine vorsignierte URL zurückgegeben. (REQ-029, REQ-044)

| Redirect-Format | Geschrieben von | Speicher |
| --- | --- | --- |
| `application/vnd.apache.parquet` | föderiertes CTAS | Keiner — Daten durchlaufen Provisa nie |
| `application/x-orc` | föderiertes CTAS | Keiner — Daten durchlaufen Provisa nie |
| `application/json` | Provisa | Speichergebunden |
| `application/x-ndjson` | Provisa | Speichergebunden |
| `text/csv` | Provisa | Speichergebunden |
| `application/vnd.apache.arrow.stream` | Provisa | Speichergebunden |

Für große analytische Exporte verwenden Sie Parquet- oder ORC-Redirect. Die Föderations-Engine schreibt parallel direkt nach S3 — keine Daten durchlaufen Provisa. (REQ-138)

```yaml
X-Provisa-Redirect-Format: application/vnd.apache.parquet
X-Provisa-Redirect-Threshold: 1000
```

---

### `POST /data/sql`

Führt rohes SQL durch die Stage-2-Governance-Pipeline aus. (REQ-267) [tool-verified: `provisa/api/data/endpoint_dev.py:62`]

**Request-Body:**

```json
{
  "sql": "SELECT id, amount FROM orders WHERE region = 'us'",
  "role": "admin"
}
```

**Erforderliche Capabilities:** `QUERY_DEVELOPMENT`.

Governance-Verstöße bei `POST /data/sql` geben HTTP 403 zurück. (REQ-002, REQ-266)

**Antwort:** Gleiches Format wie `/data/graphql` (standardmäßig JSON-Zeilen, per `Accept` content-negotiated).

---

### `POST /data/query`

Vereinheitlichter Abfrage-Endpunkt. Akzeptiert GraphQL, SQL oder Cypher — die Syntax wird automatisch erkannt. (REQ-267) [tool-verified: `provisa/api/data/endpoint_dev.py:509`]

Cypher-Abfragen können auch an den reinen Cypher-Endpunkt `POST /query/cypher` gesendet werden. (REQ-345)

**Request-Body:**

```json
{
  "query": "{ orders { id } }",
  "params": {},
  "variables": {},
  "role": "admin"
}
```

Gibt `{"data": ...}` für GraphQL und `{"columns": [...], "rows": [...]}` für SQL und Cypher zurück.

---

### `GET /data/rest/{domain_id}/{table_name}`

Automatisch generierter, einfacher REST-Endpunkt für jede registrierte Tabelle. Der Query-String wird auf GraphQL-Argumente abgebildet, und die Anfrage wird durch dieselbe Pipeline (RLS, Maskierung, Routing) wie GraphQL kompiliert und ausgeführt. (REQ-256) [tool-verified: `provisa/api/rest/generator.py:153`]

**Query-Parameter:**

- `limit` — maximale Zeilenanzahl (≥ 1)
- `offset` — Zeilen überspringen (≥ 0)
- `fields` — kommagetrennte Spaltennamen (Standard: alle skalaren Felder)
- `filter` — JSON-Array von `{"field", "comparator", "value"}`-Filterobjekten
- `orderBy` — JSON-Array von `{"field", "direction"}`-Sortierobjekten

Die authentifizierte Rolle ist erforderlich; nicht authentifizierte Anfragen geben `401` zurück. Eine OpenAPI-Spezifikation für diese Routen wird unter `GET /data/rest/openapi.json` bereitgestellt, mit Swagger UI unter `GET /data/rest/docs`.

#### OpenAPI-/Swagger-UI-Explorer

Die OpenAPI-Explorer-Seite (`/app/openapi`) bettet die Swagger UI in einem sandboxed iframe ein. Die Spezifikation ist rollenskopiert — es erscheinen nur Tabellen und Spalten, die für die aktuelle Rolle sichtbar sind — und optional per Domänen-Selektor domänengefiltert. Die UI wechselt automatisch zwischen hellem und dunklem Theme. [tool-verified: `provisa-ui/src/pages/OpenApiPage.tsx:20-34`]

Die Seite lädt die Spezifikations-HTML per `fetch()` statt über eine direkte iframe-`src`, sodass die Anfrage das Bearer-Token der Sitzung mitführt und die eigenen relativen Anfragen der Swagger UI korrekt gegen denselben Ursprung aufgelöst werden. [tool-verified: `provisa-ui/src/pages/OpenApiPage.tsx:44-69`]

Bei Navigation von einem NL-Link "In OpenAPI öffnen" klappt die Seite den Ziel-Endpunkt automatisch auf, befüllt Query-Parameter aus der NL-generierten URL (z. B. `aggregate`, `groupBy`) und klickt Execute — mittels DOM-Polling, um sicherzustellen, dass jeder Schritt abgeschlossen ist, bevor der nächste ausgelöst wird. (REQ-1359) [tool-verified: `provisa-ui/src/pages/OpenApiPage.tsx:94-171`]

---

### `GET /data/jsonapi/{domain_id}/{table_name}`

Automatisch generierter, [JSON:API](https://jsonapi.org)-konformer Endpunkt für jede registrierte Tabelle. Gleiche RLS, Maskierung und Routing wie GraphQL. (REQ-257) [tool-verified: `provisa/api/jsonapi/generator.py:284`]

**`Accept`-Header:** muss `application/vnd.api+json` (den JSON:API-Medientyp) enthalten, sonst gibt die Anfrage `406` zurück.

**Query-Parameter:**

- `fields[<type>]` — Sparse Fieldsets, z. B. `?fields[orders]=amount`
- `filter[<col>]` / `filter[<col>][<op>]` — z. B. `?filter[region]=US`, `?filter[amount][gt]=100`
- `sort` — kommagetrennt, `-`-Präfix für absteigend, z. B. `?sort=-created_at,amount`
- `page[number]` / `page[size]` — Paginierung
- `aggregate` — kommagetrennte Aggregatfunktionen, die statt des Zeilenabrufs ausgeführt werden: `count`, `sum`, `avg`, `stddev`, `variance`, `min`, `max`. Mit `?aggregate=count,sum` lässt sich eine Teilmenge anfordern. Aggregat-Antworten geben `data: null` mit Ergebnissen in `meta.aggregate` zurück. (REQ-1359) [tool-verified: `provisa-ui/src/pages/JsonApiPage.tsx:238`]
- `groupBy` — kommagetrennte Spaltennamen; wird zusammen mit `?aggregate=` verwendet, um Ergebnisse zu gruppieren. Nur Spalten im `DistinctOnColumn`-Enum der Tabelle sind gültig; der Server gibt `400` für jede Spalte zurück, die die Rolle nicht sehen darf. (REQ-1361) [tool-verified: `provisa-ui/src/pages/JsonApiPage.tsx:447`]
- `includeNodes` — `true`, um skalare Spalten der Basistabelle (und in `include=` benannte, verknüpfte Dimensions-Skalare) im `nodes`-Array jeder Gruppenzeile einzuschließen. Erforderlich, wenn eine NL-Group-by-Abfrage auch Dimensionsdetails anfordert. (REQ-1405)

Antworten sind Ressourcenobjekte mit `type`/`id`/`attributes`. Fehler folgen der JSON:API-Fehlerobjektform.

#### JSON:API-Explorer

Die JSON:API-Explorer-Seite (`/app/jsonapi`) ist eine Browser-UI über diese Endpunkte. Wählen Sie eine Tabelle aus der nach Domänen gruppierten Liste, und konfigurieren Sie dann:

- **Felder** — wählen Sie, welche Spalten einbezogen werden (Sparse Fieldset); alles abwählen, um jede Spalte anzufordern
- **Beziehungen** — wählen Sie FK-abgeleitete Beziehungsnamen zum Sideloading per `?include=`
- **Filter** — Feld, Operator (`eq`, `neq`, `gt`, `gte`, `lt`, `lte`, `like`) und Wert
- **Sortierung** — ein Feld, aufsteigend oder absteigend
- **Aggregat** — wählen Sie Group-by-Spalten aus der serverseitig validierten Liste, dann eine oder mehrere Aggregatfunktionen ankreuzen; wenn Group-by-Spalten ausgewählt sind, fügt eine Checkbox "Nodes einschließen" jeder Zeile skalare Spalten der Basistabelle hinzu
- **Seitengröße** — Ressourcen pro Seite, mit Navigation Erste/Vorherige/Nächste/Letzte

Ergebnisse werden in einer formatierten Zusammenfassungsansicht (Ressourcenkarten mit anklickbaren Beziehungs-Ankern) oder einem Raw-JSON-Tab dargestellt. Die aktuelle Anfrage-URL wird angezeigt und kann kopiert werden. Tabellenauswahl und Seitengröße bleiben sitzungsübergreifend in `localStorage` erhalten. [tool-verified: `provisa-ui/src/pages/JsonApiPage.tsx`]

Bei Navigation von einem NL-Link "In JSON:API öffnen" wählt der Explorer die Tabelle vorab aus, befüllt den Aggregat-Picker aus den NL-generierten Query-Parametern und führt die Anfrage dann automatisch aus. [tool-verified: `provisa-ui/src/pages/JsonApiPage.tsx:460-479`]

---

### `POST /query/nl`

Reicht eine natürlichsprachliche Frage ein. Der Dienst startet einen asynchronen Job und gibt sofort `202 Accepted` mit einer `job_id` zurück. Erfordert einen unter dem Konfigurationsabschnitt `ai_models` konfigurierten LLM-Provider. (REQ-354) [tool-verified: `provisa/api/rest/nl_router.py:50`]

**Request-Body:**

```json
{"q": "How many orders were placed last month?", "role": "admin"}
```

Gibt `{"job_id": "<id>"}` zurück. Überschreitet die pro Rolle geltende NL-Ratenbegrenzung, wird `429` mit einem `Retry-After`-Header zurückgegeben. (REQ-370)

**Das Ergebnis abrufen:**

- `GET /query/nl/{job_id}` — Polling. Gibt das Job-Dokument zurück.
- `GET /query/nl/{job_id}/stream` — SSE. Ein `branch`-Ereignis pro abgeschlossenem Generierungsziel, danach ein `done`-Ereignis. (REQ-357, REQ-358)

Drei Generierungsschleifen (Cypher, GraphQL, SQL) laufen parallel, jede wird durch den Compiler validiert und bei Fehlern verfeinert. (REQ-355) Der Prompt ist auf das für die Rolle sichtbare Schema eingeschränkt. (REQ-356) Das Ergebnisdokument schlüsselt jeden Zweig nach Ziel: (REQ-357) [tool-verified: `provisa/nl/job.py:69`]

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

Ein Zweig, der sein Iterationslimit ausschöpft, gibt `query: null`, `result: null` und eine `error`-Zeichenkette zurück. Jede generierte Abfrage wird unter den Rechten des Verbrauchers mit angewendeter Stage-2-Governance ausgeführt — der Dienst umgeht Governance nie. (REQ-359)

#### NL-Group-by mit Dimensionsdetails (REQ-1405)

Wenn eine NL-Group-by-Abfrage auch Spalten aus einer verknüpften Dimensionstabelle projiziert — zum Beispiel "Anzahl der Anfragen nach Benutzer mit Benutzername und E-Mail" —, leitet der Runner aus den per SELECT projizierten Dimensionsspalten Feld-Dot-Pfade (`dim_paths`) ab. Diese Pfade befüllen den Parameter `includeNodes=` in den generierten URLs der JSON:API- und OpenAPI-Panels, sodass diese Panels dieselben verknüpften Dimensionsfelder anfordern, die die SQL- und GraphQL-Zweige aufgelöst haben. Ohne dies würde `includeNodes=true` nur die eigenen skalaren Felder der Basis-Aggregat-Tabelle zurückgeben. (REQ-1405) [tool-verified: `docs/arch/requirements.md:REQ-1405`]

Im gRPC-Panel führt die generierte `{Type}GroupByRequest` `include_nodes` (bool) und `include` (wiederholte Zeichenkette von Beziehungsfeldnamen) mit. Die zurückgegebene `{Type}GroupByRow` enthält ein typisiertes `nodes`-Feld mit den Dimensionsdetail-Zeilen. [tool-verified: `provisa/grpc/query_ir.py:168-196`]

---

### `GET /data/sdl`

Gibt die GraphQL-SDL für das Schema einer Rolle zurück. (REQ-008) [tool-verified: `provisa/api/data/sdl.py:137`]

**Headers:** `X-Role: <role_id>` (erforderlich)

**Query-Parameter:**

- `domain` — kommagetrennte Domänen-IDs. Wenn gesetzt, wird die Antwort auf die genannte(n) Domäne(n) und von ihnen erreichbare Tabellen gefiltert.

**Antwort:** `text/plain` GraphQL-SDL.

---

### `GET /data/introspection`

Gibt GraphQL-Introspektions-JSON zurück, optional domänengefiltert. [tool-verified: `provisa/api/data/sdl.py:200`]

**Headers:** `X-Provisa-Role: <role_id>` (erforderlich)

**Query-Parameter:** `domain` — kommagetrennte Domänen-IDs.

**Antwort:** `application/json`-Introspektionsergebnis.

---

### `GET /data/graph-schema`

Gibt die Graph-Ansicht des Schemas der Rolle zurück: Knotenlabels und ihre Beziehungstypen, für Cypher-/Graph-Clients. Enthält `pk_columns` pro Knotenlabel, damit Aufrufer die Primärschlüsselspalten bestimmen können. (REQ-398) [tool-verified: `provisa/api/rest/cypher_router.py:689`]

**Antwort:** `application/json` mit `node_labels` (jeweils mit `pk`/`pk_columns`) und `relationship_types`.

---

### `GET /data/domains`

Gibt die für die anfragende Rolle zugänglichen Domänen-IDs zurück. [tool-verified: `provisa/api/data/sdl.py:116`]

**Headers:** `X-Role: <role_id>` (erforderlich)

**Antwort:** `["sales", "support", ...]`

---

### `GET /data/schema-version`

Gibt die aktuelle Schema-Versionszeichenkette zurück. Kombiniert eine Nonce pro Boot mit einem Rebuild-Zähler. Clients nutzen dies, um Schema-Caches nach Server-Neustarts zu invalidieren. (REQ-537) [tool-verified: `provisa/api/data/sdl.py:102`]

**Antwort:** `{"version": "<boot-id>-<counter>"}`

---

### `GET /data/proto/{role_id}`

Gibt die automatisch generierte `.proto`-Datei für eine Rolle zurück. [tool-verified: `provisa/api/data/endpoint_dev.py:49`]

**Antwort:** `text/plain`-Protobuf-Schema.

Jede registrierte Tabelle erzeugt eine Proto-`message`. Beziehungen erzeugen verschachtelte Message-Felder. Typabbildung: `integer → int32`, `bigint → int64`, `varchar → string`, `decimal → double`, `boolean → bool`, `timestamp → google.protobuf.Timestamp`. (REQ-538)

---

### `GET /data/subscribe/{table}`

Server-Sent-Events-Stream für Echtzeit-Änderungsbenachrichtigungen aus einer Tabelle. (REQ-219, REQ-258) [tool-verified: `provisa/api/data/subscribe.py:239`]

Die Zustellung von Benachrichtigungen verwendet einen pro Quelltyp gewählten, austauschbaren Provider: PostgreSQL-Quellen verwenden `LISTEN/NOTIFY` (über asyncpg), MongoDB-Quellen verwenden Change Streams (`collection.watch()`), und Kafka-Quellen verwenden Consumer-Gruppen. Jeder Provider implementiert eine gemeinsame asynchrone Watch-Schnittstelle. RLS-Filterung und Schema-Validierung gelten unabhängig vom Provider. (REQ-258) WebSocket- und RSS-Quellen werden ebenfalls unterstützt. (REQ-338, REQ-342)

**Header — `X-Provisa-Sink`:** Auf ein Kafka-Ziel setzen (z. B. `kafka://broker:9092/topic`), um Änderungsereignisse statt an die SSE-Antwort an eine Kafka-Senke umzuleiten. Der Server startet einen Sink-Consumer und gibt `202 Accepted` statt eines offenen Streams zurück. (REQ-812) [tool-verified: `provisa/api/data/subscription_sse.py:137`]

---

## Admin-REST-Endpunkte

### Config

#### `GET /admin/config`

Lädt die aktuelle `provisa.yaml` als `application/x-yaml` mit einem `Content-Disposition: attachment`-Header herunter. (REQ-164) [tool-verified: `provisa/api/admin/settings_router.py:19`]

#### `PUT /admin/config`

Lädt eine überarbeitete Config-YAML hoch. Der Server schreibt ein `.bak`-Backup, speichert die neue Datei und lädt alle Schemas, Quellen und materialisierten Sichten neu. (REQ-164) [tool-verified: `provisa/api/admin/settings_router.py:32`]

**Request-Body:** Roher YAML-Inhalt.

**Antwort:**

```json
{"success": true, "message": "Config uploaded and reloaded"}
```

Bei Reload-Fehler: `{"success": false, "message": "<error>"}`.

---

### Settings

#### `GET /admin/settings`

Gibt die aktuellen Plattform-Einstellungen als JSON zurück. (REQ-165) [tool-verified: `provisa/api/admin/settings_router.py:50`]

**Antwort:**

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

Aktualisiert Plattform-Einstellungen zur Laufzeit. Alle Felder sind optional — nur im Body vorhandene Schlüssel werden aktualisiert. (REQ-165) [tool-verified: `provisa/api/admin/settings_router.py:100`]

**Request-Body (Teilbeispiel):**

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

Aktualisierbare Felder pro Abschnitt:

- `redirect`: `enabled`, `threshold`, `default_format`, `ttl`
- `sampling`: `default_sample_size`
- `cache`: `default_ttl`
- `naming`: `domain_prefix`, `convention` — schreibt in die Config-Datei und löst einen Schema-Reload aus (REQ-253)
- `relationships`: `auto_track_fk`
- `otel`: `endpoint`, `service_name`, `sample_rate`, `support_endpoint`, `support_redact_sql_literals`, `support_redact_attributes`

**Antwort:**

```json
{"success": true, "updated": ["otel.support_endpoint", "cache.default_ttl"]}
```

---

### Observability

#### `GET /admin/traces/recent`

Gibt bis zu N kürzlich abgeschlossene Spans aus dem In-Memory-Span-Puffer zurück. (REQ-302) [tool-verified: `provisa/api/admin/settings_router.py:317`]

**Query-Parameter:** `limit` (Standard 50, maximal 200)

**Antwort:** `{"traces": [...]}`

#### `POST /admin/query-engine/reload-catalog`

Lädt einen benannten Katalog im Föderations-Engine-Coordinator über dessen REST-API per Hot-Reload neu. Verbindet Provisas interne Verbindung neu und führt das OTel-DDL erneut aus. [tool-verified: `provisa/api/admin/settings_router.py:208`]

**Query-Parameter:** `catalog` (Standard `"otel"`)

**Antwort:**

```json
{"success": true, "errors": []}
```

#### `POST /admin/query-engine/restart`

Startet den Föderations-Engine-Container neu (nur Single-Node-Dev). [tool-verified: `provisa/api/admin/settings_router.py:287`]

**Query-Parameter:** `container` (Standard aus der Umgebungsvariable `QUERY_ENGINE_CONTAINER`, sonst `"trino"`)

---

### Discovery

#### `POST /admin/discover/relationships`

Löst die Beziehungserkennung aus. Führt immer eine FK-Introspektion über die Föderations-Engine durch. (REQ-018) Führt LLM-Inferenz aus, wenn `ANTHROPIC_API_KEY` gesetzt ist. (REQ-167) [tool-verified: `provisa/api/admin/discovery.py:55`]

**Request-Body:**

```json
{
  "scope": "domain",
  "domain_id": "sales"
}
```

`scope` muss einer von `"table"`, `"domain"`, `"cross-domain"` sein. Bei Scope `"table"` ist `table_id` (Integer) erforderlich. Bei Scope `"domain"` ist `domain_id` erforderlich.

**Antwort:** `{"candidates_found": 12, "stored_ids": [1, 2, 3, ...]}`

#### `GET /admin/discover/candidates`

Listet ausstehende Beziehungskandidaten auf. [tool-verified: `provisa/api/admin/discovery.py:96`]

#### `POST /admin/discover/candidates/{candidate_id}/accept`

Akzeptiert einen Kandidaten und registriert ihn als Beziehung. [tool-verified: `provisa/api/admin/discovery.py:103`]

**Request-Body (optional):** `{"name": "custom-relationship-name"}`

#### `POST /admin/discover/candidates/{candidate_id}/reject`

Lehnt einen Kandidaten ab. [tool-verified: `provisa/api/admin/discovery.py:110`]

**Request-Body:** `{"reason": "Not a real join"}`

#### `GET /admin/discover/candidates/rejected/count`

Gibt die Anzahl abgelehnter Kandidaten zurück. [tool-verified: `provisa/api/admin/discovery.py:118`]

#### `DELETE /admin/discover/candidates/rejected`

Löscht alle abgelehnten Kandidaten. [tool-verified: `provisa/api/admin/discovery.py:128`]

---

### Source Crawl

#### `POST /admin/sources/crawl`

Durchsucht eine Datenquelle, um ihr Schema zu introspizieren und Tabellen zu registrieren. (REQ-012) [tool-verified: `provisa/api/admin/crawl_router.py:36`]

---

### Source Table Search

#### `GET /admin/sources/{source_id}/tables/search`

Durchsucht verfügbare (noch nicht registrierte) Tabellen in einer Quelle nach Namen. [tool-verified: `provisa/api/admin/table_search_router.py:103`]

---

### Table Profiling

#### `POST /admin/tables/{table_id}/profile`

Führt ein Spaltenprofil auf einer registrierten Tabelle aus — Kardinalität, Min/Max, Null-Raten. [tool-verified: `provisa/api/admin/table_profile_router.py:28`]

---

### Source Descriptions

#### `POST /admin/source-meta/db-description`

Generiert LLM-gestützte Beschreibungen für die Tabellen und Spalten einer Quelle. [tool-verified: `provisa/api/admin/source_meta_router.py:48`]

---

### Actions (Funktionen und Webhooks)

Alle Endpunkte liegen unter dem Präfix `/admin/actions`. (REQ-205) [tool-verified: `provisa/api/admin/actions_router.py:24`]

Jeder Aufruf — von GraphQL, SQL, Cypher, Bolt, Arrow Flight, MCP `run_sql` und Provisa gRPC — läuft durch einen einzigen governten Executor, der `writable_by` und Governance einheitlich durchsetzt. (REQ-1156) [tool-verified: `provisa/api/data/action_exec.py`] Siehe [docs/integrations.md](integrations.md#commands-uber-protokolle-hinweg-aufrufen) für die protokollspezifische Aufrufsyntax.

#### `GET /admin/actions`

Gibt alle erfassten DB-Funktionen und Webhooks zurück. (REQ-242) [tool-verified: `provisa/api/admin/actions_router.py:104`]

**Antwort:**

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

Jedes Webhook-Objekt führt ein `approved`-Boolean mit. Ein Webhook wird genehmigt, sobald ein Steward seinen Erstellungsantrag ausführt (REQ-209); config-deklarierte Webhooks werden automatisch genehmigt. Ein nicht genehmigter Webhook ist registriert, aber auf keiner Oberfläche exponiert. [tool-verified: `provisa/api/admin/actions_router.py:124-131`]

#### `POST /admin/actions/functions`

Registriert eine erfasste Funktion (Command). (REQ-205) [tool-verified: `provisa/api/admin/actions_router.py:117`]

**Wichtige Felder:**

| Feld | Erforderlich | Beschreibung |
| --- | --- | --- |
| `name` | Ja | Eindeutiger Command-Name |
| `kind` | Ja | `"query"` → GraphQL-Query-Feld; `"mutation"` → Mutation-Feld |
| `implKind` | Nein | Wie der Command ausgeführt wird — siehe Tabelle unten (Standard `source_procedure`) |
| `binding` | Nein | `implKind`-spezifische Verbindungsdetails (JSON-Objekt) |
| `returnSchema` | Nein | JSON-Schema `{type:"array", items:{type:"object", properties:{...}}}` — macht den Command auf jeder Oberfläche set-returning |
| `arguments` | Nein | `[{name, type}]`-Argumentdefinitionen; die Reihenfolge ist für SQL- und Bolt-Aufrufer relevant |
| `visibleTo` | Nein | Rollen-IDs, die den Command aufrufen dürfen |
| `writableBy` | Nein | Rollen-IDs, die ihn als Mutation aufrufen dürfen |
| `domainId` | Nein | Domäne für GraphQL-Platzierung und Zugriffskontrolle |

**Werte für `implKind`:**

| `implKind` | Was ausgeführt wird | `binding`-Felder |
| --- | --- | --- |
| `source_procedure` | Gespeicherte Prozedur auf einer registrierten Quelle (Standard) | `sourceId`, `schemaName`, `functionName` |
| `script` | Serverseitiges Skript | `script` |
| `http` | Ausgehender HTTP-Aufruf | `url`, `method` |
| `grpc` | Ausgehender gRPC-Aufruf an einen externen Server | `target`, `method` |
| `python` | Von Provisa gehostetes Python-Callable (REQ-885) | `callable` (z. B. `"demo.py_functions:random_dataset"`) |

Die Demo-Commands `random_python_set` (`implKind: python`) und `random_grpc_set` (`implKind: grpc`) zeigen set-returning Commands mit `returnSchema` in der Praxis; beide befinden sich in `config/provisa-install.yaml`. [tool-verified: `config/provisa-install.yaml:809-856`]

#### `PUT /admin/actions/functions/{name}`

Aktualisiert eine erfasste Funktion nach Name. [tool-verified: `provisa/api/admin/actions_router.py:182`]

#### `DELETE /admin/actions/functions/{name}`

Löscht eine erfasste Funktion nach Name. [tool-verified: `provisa/api/admin/actions_router.py:233`]

#### `POST /admin/actions/webhooks`

Registriert einen erfassten Webhook. (REQ-209) Das Registrieren oder Aktualisieren eines Webhooks reiht eine Steward-Genehmigungsanfrage ein — der Webhook wird auf allen Oberflächen erst aktiv, nachdem ein Steward ihn genehmigt hat. Config-deklarierte Webhooks werden automatisch genehmigt. **Felder des Request-Bodys:** `name`, `url`, `method`, `timeoutMs`, `returns`, `inlineReturnType`, `arguments`, `visibleTo`, `domainId`, `description`, `kind`. [tool-verified: `provisa/api/admin/actions_router.py:132`, `provisa/api/admin/actions_router.py:325-331`]

#### `PUT /admin/actions/webhooks/{name}`

Aktualisiert einen erfassten Webhook nach Name. Jede Bearbeitung setzt die Genehmigung zurück auf "ausstehend", bis erneut genehmigt wird. [tool-verified: `provisa/api/admin/actions_router.py:306`]

#### `DELETE /admin/actions/webhooks/{name}`

Löscht einen erfassten Webhook nach Name. [tool-verified: `provisa/api/admin/actions_router.py:355`]

#### `POST /admin/actions/test`

Testet eine Action (Funktion oder Webhook) nach Name. (REQ-245) [tool-verified: `provisa/api/admin/actions_router.py:384`]

---

### Roles

Alle Endpunkte liegen unter dem Präfix `/admin/roles`. [tool-verified: `provisa/api/admin/roles_router.py:18`]

| Methode | Pfad | Beschreibung |
| --- | --- | --- |
| `GET` | `/admin/roles/` | Alle Rollen auflisten |
| `POST` | `/admin/roles/` | Eine Rolle erstellen |
| `PUT` | `/admin/roles/{role_id}` | Eine Rolle aktualisieren |
| `DELETE` | `/admin/roles/{role_id}` | Eine Rolle löschen |

[tool-verified: `provisa/api/admin/roles_router.py`]

---

### Users

Alle Endpunkte liegen unter dem Präfix `/admin/users`. [tool-verified: `provisa/api/admin/local_users_router.py:21`]

| Methode | Pfad | Beschreibung |
| --- | --- | --- |
| `POST` | `/admin/users/` | Einen lokalen Benutzer erstellen |
| `GET` | `/admin/users/` | Lokale Benutzer auflisten |
| `GET` | `/admin/users/{user_id}` | Einen Benutzer abrufen |
| `PUT` | `/admin/users/{user_id}` | Einen Benutzer aktualisieren |
| `PATCH` | `/admin/users/{user_id}/password` | Passwort ändern |
| `DELETE` | `/admin/users/{user_id}` | Einen Benutzer löschen |
| `GET` | `/admin/users/{user_id}/assignments` | Rollenzuweisungen auflisten |
| `POST` | `/admin/users/{user_id}/assignments` | Eine Rollenzuweisung hinzufügen |
| `DELETE` | `/admin/users/{user_id}/assignments/{assignment_id}` | Eine Rollenzuweisung entfernen |

---

### Organizations

Alle Endpunkte liegen unter `/admin/orgs`. [tool-verified: `provisa/api/admin/orgs_router.py:18`]

| Methode | Pfad | Beschreibung |
| --- | --- | --- |
| `GET` | `/admin/orgs/` | Organisationen auflisten |
| `POST` | `/admin/orgs/` | Eine Organisation erstellen |
| `PUT` | `/admin/orgs/{org_id}` | Eine Organisation aktualisieren |
| `DELETE` | `/admin/orgs/{org_id}` | Eine Organisation löschen |
| `GET` | `/admin/orgs/{org_id}/members` | Mitglieder auflisten |
| `POST` | `/admin/orgs/{org_id}/members` | Ein Mitglied hinzufügen |
| `DELETE` | `/admin/orgs/{org_id}/members/{user_id}` | Ein Mitglied entfernen |

---

### Invites

Alle Endpunkte liegen unter `/admin/invites`. [tool-verified: `provisa/api/admin/invites_router.py:18`]

| Methode | Pfad | Beschreibung |
| --- | --- | --- |
| `POST` | `/admin/invites/` | Eine Einladung erstellen |
| `GET` | `/admin/invites/` | Ausstehende Einladungen auflisten |
| `DELETE` | `/admin/invites/{token}` | Eine Einladung widerrufen |

---

### Admin-GraphQL

#### `POST /admin/graphql`

Strawberry-GraphQL-Endpunkt für alle Admin-Operationen: Quellen- und Tabellen-CRUD, Beziehungsverwaltung, Domänenkonfiguration, RLS-Regeln, Cache-Steuerung, Namenskonventionen, Verwaltung geplanter Aufgaben und Abfragekompilierung. (REQ-164) [tool-verified: `provisa/api/app.py:2171`]

**Wichtige Mutationen:**

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

Gibt den Erstinbetriebnahme-Status zurück. Immer nicht authentifiziert. (REQ-539) [tool-verified: `provisa/api/setup_router.py:100`]

#### `POST /setup/`

Schließt die Erstinbetriebnahme ab. [tool-verified: `provisa/api/setup_router.py:142`]

---

## Health Check

#### `GET /health` oder `HEAD /health`

Gibt `{"status": "ok"}` zurück. Immer nicht authentifiziert. (REQ-539) [tool-verified: `provisa/api/app.py:2258`]

---

## Fehlerantworten

| Status | Bedeutung |
| --- | --- |
| 400 | Ungültige Abfrage, Validierungsfehler oder SQL-Parsefehler |
| 401 | Fehlendes oder ungültiges Auth-Token |
| 403 | Unzureichende Capabilities; Governance-Verstoß |
| 404 | Rolle, Ressource oder Konfigurationsdatei nicht gefunden |
| 422 | Fehlender erforderlicher Header (z. B. `X-Role`) |
| 503 | Datenbank oder Quelle nicht verbunden; Abhängigkeit nicht verfügbar |
| 504 | Anfrage-Timeout |

Governance-Verstöße bei `POST /data/sql` geben HTTP 403 mit einem strukturierten Body zurück: (REQ-002) [tool-verified: `provisa/api/data/endpoint_dev.py:184-190`]

```json
{
  "detail": {
    "violations": [
      {"code": "V000", "message": "Table 'orders' is not accessible for role 'analyst'"}
    ]
  }
}
```

Alle anderen Fehler verwenden: `{"detail": "<message>"}`.

---

## Arrow-Flight-Endpunkt

Port `8815`. Nativer Arrow-Columnar-Transport über gRPC. (REQ-143, REQ-045) [tool-verified: `provisa/api/flight/server.py`]

Abfragen und Katalog-Discovery sind beide über dieselbe Verbindung verfügbar. Die vollständige Governance-Pipeline (RLS, Maskierung, Sampling) wird auf jede Abfrage angewendet. (REQ-130, REQ-143)

**Ticket-Format** (JSON):

```json
{"query": "{ customers { name email } }", "role": "analyst", "variables": {}}
```

**Verwendung (Python):**

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

Wenn der Zaychik-Flight-SQL-Proxy verfügbar ist (Port 8480), werden Record-Batches ohne vollständige Materialisierung durchgängig gestreamt. (REQ-144) Fällt bei nicht verfügbarem Zaychik auf Materialisierung über die föderierte Abfrageschicht zurück. (REQ-146)

---

## Protobuf-gRPC-Endpunkt

Port `50051` (überschreibbar mit der Umgebungsvariable `GRPC_PORT` oder der Config `server.grpc_port`). (REQ-529) [tool-verified: `provisa/grpc/server.py`, `provisa/api/app.py`]

Übergeben Sie die Rolle im gRPC-Metadatenschlüssel `x-provisa-role`. Fehlt dieser, bricht der Server mit `UNAUTHENTICATED` ab. [tool-verified: `provisa/grpc/server.py`]

Laden Sie das rollenspezifische Proto von `GET /data/proto/{role_id}` herunter. Es erscheinen nur Tabellen und Spalten, die für diese Rolle sichtbar sind. (REQ-039)

```proto
service ProvisaService {
  rpc QueryOrders (QueryOrdersRequest) returns (stream Orders);
  rpc InsertOrders (InsertOrdersRequest) returns (InsertOrdersResponse);
}
```

Jede Tabelle erzeugt eine streamende `Query{TypeName}`-RPC. `Insert{TypeName}`-RPCs existieren aus Gründen der Schemasymmetrie, brechen jedoch mit `UNIMPLEMENTED` ab. [tool-verified: `provisa/grpc/server.py`]

`grpc_reflection.v1alpha` ist für die Service-Discovery ohne vorkompiliertes Proto aktiviert. (REQ-529) [tool-verified: `provisa/grpc/reflection.py`]

```bash
grpcurl -plaintext localhost:50051 list
grpcurl -plaintext -H 'x-provisa-role: analyst' \
  -d '{}' localhost:50051 ProvisaService/QueryOrders
```

Der gRPC-Server startet nur, wenn beim Start ein gültiges Proto kompiliert werden kann. Schlägt der Schema-Build fehl, startet der gRPC-Server nicht. (REQ-529)

#### Aggregat- und Group-by-RPCs (REQ-1359, REQ-1361, REQ-1405)

Wenn für eine Tabelle `enable_aggregates` gesetzt ist, enthält das generierte Proto neben `Query{TypeName}` zwei zusätzliche RPCs:

- **`Query{TypeName}Aggregate`** — gibt Aggregat-Skalare für die Tabelle zurück (`count`; `sum`, `avg`, `stddev`, `variance` pro numerischer Spalte; `min`, `max` pro vergleichbarer Spalte)
- **`Query{TypeName}GroupBy`** — gibt eine Zeile pro Gruppenschlüssel mit Aggregat-Unterfeldern und optional Basistabellen-Skalaren sowie verknüpften Dimensionszeilen in einem `nodes`-Feld zurück

Beide laufen durch dieselbe Compiler-Aggregat-Pipeline wie GraphQLs Root-Felder `{field}_aggregate` und `{field}_group_by` — keine separate Aggregat-Implementierung. (REQ-1359) [tool-verified: `provisa/grpc/query_ir.py:133-196`]

**Feld `funcs` (REQ-1361).** Die Request-Message akzeptiert ein wiederholtes String-Feld `funcs`. Gültige Werte sind `count`, `sum`, `avg`, `stddev`, `variance`, `min` und `max`. Wird `funcs` weggelassen, wird jede vom Schema für diese Tabelle exponierte Funktion angefordert. Ist es gesetzt, erscheinen nur die benannten Funktionen. Passt keine der benannten Funktionen zu den Spaltentypen der Tabelle, fällt die Abfrage auf `count` zurück. [tool-verified: `provisa/grpc/query_ir.py:66`, `provisa/grpc/query_ir.py:75-97`]

**Felder `include_nodes` und `include` (REQ-1405).** `Query{TypeName}GroupBy`-Anfragen können `include_nodes: true` setzen, um skalare Spalten der Basistabelle in das `nodes`-Feld jeder Zeile aufzunehmen. Das wiederholte String-Feld `include` benennt Many-to-one-Beziehungsfelder, deren skalare Spalten ebenfalls in `nodes` verschachtelt werden. Dies entspricht dem Verhalten von `?includeNodes=` / `?include=` in JSON:API. [tool-verified: `provisa/grpc/query_ir.py:168-195`]

---

## JDBC-Treiber

Der Provisa-JDBC-Treiber (`provisa-jdbc-0.1.0.jar`) exponiert den semantischen Katalog gegenüber BI-Tools (Tableau, PowerBI, DBeaver). (REQ-126)

**Verbindungs-URL:** `jdbc:provisa://host:port` (REQ-131)

Domänen werden auf JDBC-Schemas abgebildet. (REQ-127) Tabellen verwenden ihre registrierten Aliase. Spalten verwenden Aliase und exponieren Beschreibungen als `REMARKS`. (REQ-128) Standard-Metadatenmethoden (`getPrimaryKeys`, `getImportedKeys`, `getExportedKeys`) exponieren semantische Beziehungen als PK/FK-Metadaten.

**SQL-Unterstützung:** `SELECT * FROM <alias> [WHERE col = 'value']`. (REQ-129)

Der Treiber fordert standardmäßig Arrow-IPC-Redirect an. Ergebnisse werden batchweise über `ArrowStreamReader` gestreamt, begrenzt auf einen Record-Batch im Speicher. (REQ-293)

---

## `orderBy`-Argumentformat

Das Argument `order_by` verwendet `{column: direction}`-Objekte mit einem 6-wertigen Direction-Enum: (REQ-200)

```json
{
  "query": "{ orders(order_by: [{created_at: desc_nulls_last}]) { id created_at } }",
  "role": "admin"
}
```

Unterstützte Richtungen: `asc`, `desc`, `asc_nulls_first`, `asc_nulls_last`, `desc_nulls_first`, `desc_nulls_last`. (REQ-201)

---

## Subscriptions

SSE-Subscriptions sind unter `GET /data/subscribe/{table}` verfügbar. (REQ-219, REQ-258) Die Zustellung von Benachrichtigungen verwendet einen pro Quelltyp gewählten, austauschbaren Provider: PostgreSQL-Quellen verwenden `LISTEN/NOTIFY`, MongoDB-Quellen verwenden Change Streams, und Kafka-Quellen verwenden Consumer-Gruppen. RLS-Filterung und Schema-Validierung gelten unabhängig vom Provider. WebSocket- und RSS-Quellen werden über denselben Endpunkt ebenfalls unterstützt. (REQ-338, REQ-342) [tool-verified: `provisa/api/data/subscribe.py:239`, `provisa/subscriptions/registry.py`, `provisa/api/app.py` `_rebuild_schemas`]

---

## Business Glossary (REQ-1387)

Das Business Glossary bildet physische Feldnamen — wie sie in Quelldatenbanken existieren — auf ein gemeinsames, menschenlesbares Vokabular ab. Jede in der semantischen Schicht registrierte Spalte erhält automatisch einen Begriff. Zur Befüllung des Glossars ist keine manuelle Eingabe erforderlich; Kuratoren fügen Definitionen, Beziehungen und Experten auf dem hinzu, was das System ableitet.

### Wie Begriffe abgeleitet werden

Wenn Provisa die Spalten einer Tabelle registriert oder aktualisiert, läuft `normalize_term` (`provisa/core/glossary.py`) über jeden Spaltennamen und erzeugt eine kanonische Phrase. [tool-verified: `provisa/core/repositories/glossary.py:sync_table_refs`]

Die Normalisierung wendet fünf Regeln in Folge an:

1. Aufteilung an camelCase-Grenzen und Trennzeichen (`_`, `-`, `.`, `/`, Leerraum).
2. Umwandlung des Ergebnisses in Kleinbuchstaben.
3. Erweiterung einer festen Abkürzungstabelle (z. B. `cust` → `customer`, `amt` → `amount`, `dt` → `date`, `id` → `identifier`, `key` → `identifier`, `guid` → `identifier`).
4. Entfernen eines nachgestellten **Proxy-Tokens** (`identifier`, `code`, `index` oder `reference`) — eine Spalte, die nach ihrem Schlüssel oder Code benannt ist, verweist über einen Stellvertreterwert auf das zugrunde liegende Konzept, daher sollte der Begriff das Konzept selbst sein. Das letzte verbleibende Token wird nie entfernt.
5. Qualifizierung einer **zu allgemeinen Phrase** mit dem Konzept der Tabelle. Wenn die vollständige normalisierte Phrase ein bloßes Attributwort ist (`name`, `identifier`, `date`, `location`, `message`, `first name`, `last name` und Ähnliches), wird der Begriff zu `<Tabellenkonzept> <Phrase>` — `employees.first_name` → `employee first name`, `orders.id` → `order identifier`. Ein gemeinsam genutzter `name`-Begriff über nicht zusammenhängende Tabellen hinweg würde unterschiedliche Bedeutungen verschmelzen; die Qualifizierung verbindet stattdessen jede Spalte mit ihrem umschließenden Konzept. Das Tabellenkonzept ist der Geschäftsname der Tabelle, normalisiert mit einem singularen Kopfnomen (`order_lines` → `order line`).

Native-Filter-Pseudospalten (mit `_nf_`-Präfix oder jede Spalte, die `native_filter_type` führt) sind Query-Parameter-Mechanik, keine Geschäftsfelder, und leiten keine Begriffe ab.

Da `id`, `key`, `pk` und `sk` alle vor der Proxy-Prüfung zu `identifier` expandieren, landen drei physisch unterschiedliche Spaltennamen auf genau demselben Begriff:

| Physischer Name | Nach Normalisierung |
| --- | --- |
| `cust_id` | `customer` |
| `customerId` | `customer` |
| `CUSTOMER_KEY` | `customer` |
| `txn_amt` | `transaction amount` |

Die ersten drei fallen zu einem Begriff zusammen. `transaction amount` behält beide Tokens, da `amount` kein Proxy ist. Eine bloße `id`-Spalte — ohne vorangehende Tokens — kann nicht entfernt werden; sie normalisiert zu `identifier`, damit der Begriff nicht leer ist. [tool-verified: `provisa/core/glossary.py:normalize_term`]

### Lifecycle

Begriffe werden **aus der Mitgliedschaft in der semantischen Schicht abgeleitet**, nicht auf Anforderung von Benutzern erstellt. Das Tabellen-Repository ist der einzige Schreibpfad: `sync_table_refs` läuft innerhalb jedes Spaltenmengen-Upserts, und `sweep_refless_terms` läuft nach jedem Löschpfad. [tool-verified: `provisa/core/repositories/glossary.py`]

**Wenn eine Spalte hinzugefügt wird:** Provisa sucht den normalisierten Begriff nach Namen. Existiert er bereits, erhält die Spalte einen Verweis darauf (und war der Begriff als deprecated markiert, wird er wiederbelebt — `deprecated` wird zurück auf `False` gesetzt). Existiert noch kein Begriff, wird einer erstellt.

**Wenn eine Spalte entfällt** (Schemaänderung oder Tabellenentfernung): Ihr Verweis wird gelöscht, und der Begriff wird nach einer Remove-or-Deprecate-Regel **abgeschlossen**. Ein verwurzelter Begriff ohne verbleibende Verweise wird vollständig entfernt — zusammen mit seinen Kanten und Expertenzuweisungen —, es sei denn, seine Entfernung würde einen abstrakten Begriff von allen verwurzelten Begriffen trennen (kein Pfad durch den Begriffsgraphen). In diesem Fall wird der Begriff **als deprecated markiert** (`deprecated=True`) statt gelöscht, damit der Graph-Anker des abstrakten Begriffs erhalten bleibt.

Abstrakte Begriffe werden nie automatisch entfernt; sie existieren außerhalb des physischen Lebenszyklus und werden nur explizit über die Admin-API gelöscht.

**Wiederbelebung:** Taucht der normalisierte Name eines deprecateten Begriffs wieder auf (eine Spalte wird erneut registriert), wird die Markierung des Begriffs aufgehoben, und seine Verweise sammeln sich wieder an.

### Kuratierungs-Endpunkte

Alle Endpunkte liegen unter `/admin/glossary`. Sie erfordern `org_admin`-Zugriff und eine konfigurierte Organisation. Jede Mutation löst eine Metadaten-Publikation aus. [tool-verified: `provisa/api/admin/glossary_router.py`]

| Methode | Pfad | Beschreibung |
| --- | --- | --- |
| `GET` | `/admin/glossary/terms` | Begriffe auflisten. Query-Parameter: `q` (Name-/Definitionssuche), `include_deprecated` (Standard `true`) |
| `GET` | `/admin/glossary/terms/{term_id}` | Begriffsdetail abrufen: Definition, physische Verweise, typisierte Kanten, Experten |
| `POST` | `/admin/glossary/terms` | Einen abstrakten Begriff erstellen — Benutzer-Vokabular ohne physische Verweise |
| `PATCH` | `/admin/glossary/terms/{term_id}` | Umbenennen, Definition setzen oder Export-Ausschluss umschalten |
| `DELETE` | `/admin/glossary/terms/{term_id}` | Einen Begriff ohne physische Verweise löschen |
| `POST` | `/admin/glossary/refs/move` | Einen physischen Verweis auf einen anderen Begriff verschieben (Konsolidierung) |
| `POST` | `/admin/glossary/terms/{term_id}/edges` | Eine typisierte Beziehungskante zwischen zwei Begriffen hinzufügen |
| `DELETE` | `/admin/glossary/terms/{term_id}/edges` | Eine Kante entfernen (Query-Parameter: `to_term_id`, `rel_type`) |
| `POST` | `/admin/glossary/terms/{term_id}/experts` | Einen Benutzer als Experte oder Autor für einen Begriff markieren |
| `DELETE` | `/admin/glossary/terms/{term_id}/experts/{user_id}` | Die Experten-/Autorenkennzeichnung eines Benutzers entfernen |
| `POST` | `/admin/glossary/terms/{term_id}/definition/generate` | Eine Definition für einen Begriff mit dem KI-Modell der Organisation entwerfen — gibt nur Text zurück, nichts wird gespeichert, bis es gesichert wird |
| `POST` | `/admin/glossary/definitions/generate` | Definitionen für jeden Begriff ohne Definition generieren und speichern — überschreibt nie von Menschen verfassten Text |
| `POST` | `/admin/glossary/relationships/generate` | Typisierte Kanten über das gesamte Glossar hinweg mit dem KI-Modell der Organisation vorschlagen und speichern |

**Body für `POST /admin/glossary/terms`:**

```json
{"name": "revenue", "definition": "Recognized net revenue after returns and discounts."}
```

**Body für `POST /admin/glossary/terms/{term_id}/edges`:**

```json
{"to_term_id": 42, "rel_type": "KIND_OF"}
```

Gültige `rel_type`-Werte: `KIND_OF`, `RELATED_TO`, `PART_OF`, `SYNONYM_OF`. [tool-verified: `provisa/core/glossary.py:TERM_EDGE_TYPES`]

**Body für `POST /admin/glossary/terms/{term_id}/experts`:**

```json
{"user_id": "alice@example.com", "kind": "author"}
```

Gültige `kind`-Werte: `expert`, `author`. [tool-verified: `provisa/core/repositories/glossary.py:add_expert`]

**Body für `POST /admin/glossary/refs/move`:**

```json
{"table_id": 7, "column_name": "cust_id", "to_term_id": 12}
```

Das Verschieben eines Verweises schließt den verlierenden Begriff nach der Remove-or-Deprecate-Regel ab. Nutzen Sie dies, um zwei Begriffe zu konsolidieren, die die Normalisierung getrennt gehalten hat — zum Beispiel, wenn eine Quelle eine nicht standardmäßige Abkürzung verwendet, die außerhalb der Expansionstabelle lag.

Das Löschen eines verwurzelten Begriffs (mit physischen Verweisen) gibt `400 glossary.invalid` zurück. Entfernen oder verschieben Sie zuerst alle Verweise.

**`PATCH /admin/glossary/terms/{term_id}` — Feld `export_excluded`:**

```json
{"export_excluded": true}
```

Das Setzen von `export_excluded` auf `true` hält den Begriff von allen Metadaten-Export-Snapshots zurück, unabhängig von seinen physischen Verweisen oder seinem abstrakten Status. Das Zurücksetzen auf `false` stellt den Begriff bei der nächsten Publikation wieder im Snapshot her. Kuratierungsdaten (Definition, Kanten, Experten) sind davon nicht betroffen. [tool-verified: `provisa/core/repositories/glossary.py:set_export_excluded`, `provisa/api/admin/glossary_router.py:update_term`]

### KI-gestützte Kuratierung

Das konfigurierte KI-Modell der Organisation kann Definitionen entwerfen und Beziehungskanten über das gesamte Glossar hinweg in einer einzigen Operation vorschlagen. Beide Sammel-Aktionen erfordern `org_admin`-Zugriff und eine konfigurierte Organisation.

**`POST /admin/glossary/definitions/generate`**

Durchläuft jeden Begriff im Glossar, überspringt jeden, der bereits eine Definition hat, und ruft das KI-Modell der Organisation auf, um für jeden verbleibenden Begriff eine zu entwerfen. Der Entwurf wird sofort gespeichert — anders als beim Pro-Begriff-Entwurfs-Endpunkt (`POST /admin/glossary/terms/{term_id}/definition/generate`) gibt es keinen Editor-Schritt. Von Menschen verfasste Definitionen werden nie überschrieben: die Schutzbedingung ist `if summary["definition"]: continue` vor jedem Modellaufruf. Eine Publikationsbenachrichtigung deckt den gesamten Batch ab. [tool-verified: `provisa/api/admin/glossary_router.py:generate_all_definitions`]

Antwort:

```json
{"generated": 12}
```

`generated` ist die Anzahl der Begriffe, die eine neue Definition erhalten haben. Der Wert ist null, wenn jeder Begriff bereits eine hatte.

**`POST /admin/glossary/relationships/generate`**

Sendet die vollständige Begriffsliste an das KI-Modell der Organisation mit einem Prompt, der die zehn zulässigen Kantentypen (`KIND_OF`, `PART_OF`, `SYNONYM_OF`, `RELATED_TO`, `VALID_VALUE_OF`, `DERIVED_FROM`, `REPLACES`, `PREFERRED_TERM_FOR`, `TRANSLATION_OF`, `ANTONYM_OF`) angibt und nur um sichere Vorschläge bittet. Das Modell antwortet mit einem JSON-Array; jeder Eintrag wird vor jedem Schreibvorgang validiert: unbekannte Begriffsnamen, Selbstkanten und Kantentypen außerhalb des geschlossenen Enums werden stillschweigend verworfen. Gültige Vorschläge werden idempotent upserted — ein erneuter Lauf der Aktion dupliziert keine Kanten. Eine Publikationsbenachrichtigung deckt den Batch ab. Der Endpunkt gibt sofort `{"added": 0}` zurück, wenn das Glossar weniger als zwei nicht deprecatete Begriffe enthält. [tool-verified: `provisa/api/admin/glossary_router.py:generate_relationships`]

Antwort:

```json
{"added": 5}
```

`added` ist die Anzahl der geschriebenen Kanten. Eine bereits existierende Kante wird trotzdem gezählt — der Upsert gelingt, aber die Kantendaten ändern sich nicht.

### MCP-Tool `search_terms`

```
search_terms(query, role=None, limit=25)
```

Durchsucht Begriffsnamen und Definitionen mit einem Groß-/Kleinschreibung ignorierenden Teilstring-Abgleich, bis zu `limit` Ergebnissen. Jedes Ergebnis ist das vollständige Begriffsdetail: `name`, `definition`, `is_abstract`, `deprecated`, physische Verweise (mit `source_id`, `schema_name`, `table_name`, `column_name`), typisierte Kanten und Expertenzuweisungen. [tool-verified: `provisa/api/mcp/server.py:236-244`, `provisa/core/repositories/glossary.py:search_terms`]

Verwenden Sie `search_terms` vor dem Schreiben von SQL, um jedes physische Feld zu finden, das ein Konzept dem Namen nach repräsentiert. Zum Beispiel gibt eine Suche nach `"order date"` den Begriff sowie alle `order_dt`-, `orderDate`-, `ORDER_DATE`-Spalten über jede registrierte Tabelle hinweg zurück.

### Metadaten-Export

Der Begriffsgraph des Glossars ist in jedem von `build_snapshot` erstellten `MetadataSnapshot` enthalten. [tool-verified: `provisa/api/metadata_export/builder.py:_glossary_assets`]

Der Export wendet dieselben Filter an wie der Rest des Snapshots:

- Ein als `export_excluded` markierter Begriff wird vollständig zurückgehalten — unabhängig von seinen physischen Verweisen, abstraktem Status oder ob der Katalog der Organisation konfiguriert ist. [tool-verified: `provisa/api/metadata_export/builder.py:_glossary_assets`]
- Ein verwurzelter Begriff wird nur publiziert, wenn mindestens einer seiner physischen Verweise zu einer Spalte gehört, die sowohl den **Data-Product**-Filter (das `data_product`-Flag der Tabelle muss `true` sein) als auch den **technical**-Spaltenfilter besteht (mit `technical` markierte Spalten werden zurückgehalten).
- Ein verwurzelter Begriff, dessen Verweise alle durch diese Filter zurückgehalten werden, wird mit ihnen zurückgehalten.
- Abstrakte Begriffe werden bedingungslos publiziert — sie sind Benutzer-Vokabular, nicht an physische Spalten gebunden.
- Eine Kante zwischen zwei Begriffen wird nur publiziert, wenn beide Endpunkt-Begriffe publiziert werden.

Jeder Vendor-Adapter publiziert den Begriffsgraphen nativ, in einen Provisa-eigenen Glossar-Container, den er idempotent erstellt — nie in ein bestehendes Katalog-Glossar:

| Provider | Container | Begriffe | Beziehungen | Deprecation |
| --- | --- | --- | --- | --- |
| Apache Atlas | "Provisa Glossary" (Glossary-API) | Glossarbegriffe, Definition in `longDescription` | KIND_OF → `isA`, SYNONYM_OF → `synonyms`, RELATED_TO/PART_OF → `seeAlso` | `[DEPRECATED]`-Markierung in shortDescription |
| Atlan | Provisa-Glossar per stabilem qualifiedName | `longDescription` (nie das von Menschen bearbeitete `userDescription`) | gleiche Atlas-Abbildung | `certificateStatus = DEPRECATED` |
| DataHub | `urn:li:glossaryNode:provisa.<org>` | `glossaryTermInfo`-Aspekt pro Begriff | KIND_OF → Inherits, PART_OF → Contains (invertiert), RELATED_TO/SYNONYM_OF → related terms | Deprecation-Aspekt; Umbenennungen folgen der URN-Sukzession |
| OpenMetadata | Provisa-Glossar über `/v1/glossaries` | fqn-schlüsseltes PUT, Umbenennungen PATCH-rebind per gespeicherter UUID | KIND_OF → native Elternhierarchie, SYNONYM_OF → `synonyms`, andere → `relatedTerms` | `entityStatus` |
| Collibra | Glossary-Typ-Domäne "Provisa Glossary" | Business-Term-Assets über die Import-API | native Business-Term-Beziehungstypen | Asset-Status |

Die Eigentümerschaft ist die Bindung, nicht der Name: Die Vendor-ID jedes publizierten Begriffs wird unter der URN des Begriffs (`provisa://<org>/terms/<name>`) in `catalog_bindings` erfasst, und Provisa ändert oder löscht ein Glossar-Element auf Vendor-Seite nur, wenn es diese Bindung hält (oder das Element im Provisa-eigenen Container liegt, den es erstellt hat). Ein Glossar-Element ohne Provisa-Bindung entstand im externen System und wird nie berührt; Aktualisierungen lesen und mischen, sodass von Stewards hinzugefügte Felder auf Provisas eigenen Begriffen erhalten bleiben; nichts wird gelöscht, wenn ein Begriff den Snapshot verlässt. Steward-Zuweisungen von Begriff zu Asset bleiben extern verwaltet — kein Adapter schreibt Begriff-zu-Asset-Zuweisungen (die Publikation von Provisa-verfassten Zuweisungen ist ein expliziter Folgeschritt). Speziell bei Collibra beruht die Sicherheit unter der REPLACE-Semantik der Import-API auf Containment: Der Payload erwähnt nur Assets innerhalb der Provisa-Glossar-Domäne und Beziehungsinstanzen nur zwischen Provisa-Begriffen, sodass Steward-Glossare und ihre Beziehungen nie erreichbar sind. [tool-verified: `provisa/api/metadata_export/atlan.py`, `provisa/api/metadata_export/datahub.py`, `provisa/api/metadata_export/atlas.py`, `provisa/api/metadata_export/openmetadata.py`]
