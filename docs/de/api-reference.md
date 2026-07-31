# API-Referenz

## Übersicht

Provisa stellt REST-Endpunkte unter zwei Präfixen bereit: `/data` für die Ausführung von Abfragen und die Schema-Introspektion sowie `/admin` für die Konfigurationsverwaltung. (REQ-043) Die meisten Daten-Endpunkte erfordern eine Rollen-ID. Administrative Konfigurationsoperationen verwenden eine Strawberry-GraphQL-API unter `/admin/graphql`. (REQ-164)

---

## Authentifizierung

Wenn `auth.provider` in `provisa.yaml` konfiguriert ist, erfordern alle Endpunkte außer `/health` und `/setup/status` einen `Authorization: Bearer <token>`-Header. (REQ-120) [tool-verified: `provisa/api/app.py`, `provisa/auth/wiring.py`]

Ohne konfigurierte Authentifizierung läuft der Server im Entwicklungsmodus. Jede Anfrage wird als Identität `anonymous` behandelt, die allen konfigurierten Rollen mit Platzhalter-Domänenzugriff zugeordnet ist. (REQ-535)

**Anmeldung (`POST /auth/login`)** wird vom aktiven Authentifizierungsanbieter bereitgestellt, wenn `provider: basic` konfiguriert ist. (REQ-124) Format der Anmeldedaten und Antwort hängen vom Anbieter ab.

**Identitäts-Introspektion:**

```http
GET /auth/me
```

Liefert die ID, E-Mail-Adresse, den Anzeigenamen, die Organisationszugehörigkeiten und Rollenzuweisungen des authentifizierten Benutzers. Im Entwicklungsmodus wird `dev_mode: true` mit allen aufgelisteten Rollen-IDs zurückgegeben. [tool-verified: `provisa/api/auth_router.py`]

```http
GET /auth/provider-type
```

Liefert `{"provider": "<name>"}` oder `{"provider": null}`, wenn keine Authentifizierung konfiguriert ist. [tool-verified: `provisa/api/auth_router.py`]

---

## Daten-Endpunkte

### `POST /data/graphql`

Führt eine GraphQL-Abfrage oder -Mutation aus. (REQ-043) [tool-verified: `provisa/api/data/endpoint.py:151`]

**Anfragetext:**

```json
{
  "query": "{ orders(where: {region: {eq: \"us\"}}) { id amount } }",
  "variables": {},
  "role": "admin",
  "extensions": {}
}
```

Das Feld `role` wird nur im Entwicklungsmodus (ohne Authentifizierung) verwendet. Wenn Authentifizierung aktiv ist, wird die Rolle des authentifizierten Benutzers verwendet und `role` im Anfragetext wird ignoriert.

Das Feld `extensions` unterstützt das Automatic-Persisted-Query-Protokoll (APQ): (REQ-288)

```json
{
  "extensions": {"persistedQuery": {"sha256Hash": "<sha256-of-query>"}}
}
```

**Header:**

- `X-Provisa-Role` — überschreibt die Rolle (Entwicklungsmodus)
- `Accept` — Antwortformat (siehe Content Negotiation)
- `Authorization` — `Bearer <token>`, wenn Authentifizierung aktiviert ist
- `X-Provisa-Redirect-Format` — MIME-Typ für die S3-Redirect-Ausgabe (REQ-137)
- `X-Provisa-Redirect-Threshold` — Zeilenanzahl, oberhalb derer die Weiterleitung ausgelöst wird (REQ-137)
- `X-Provisa-Redirect` — `true`, um die Weiterleitung bedingungslos zu erzwingen (REQ-029)

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

**Antwort (Weiterleitung):**

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

**Antwort (mehrere Wurzeln, gemischt inline/Weiterleitung):**

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

Abfragen mit mehreren Wurzeln führen jedes Wurzelfeld unabhängig aus. Felder unterhalb des Weiterleitungsschwellenwerts werden inline zurückgegeben; Felder oberhalb werden weitergeleitet. Der Schlüssel `redirects` (Plural) ordnet Feldnamen den Weiterleitungsinformationen zu. (REQ-029) [tool-verified: `provisa/api/data/endpoint.py`]

**Cache-Header:**

- `X-Provisa-Cache: HIT|MISS` (REQ-536)
- `X-Provisa-Cache-Age: <seconds>` (bei HIT) (REQ-536)

**Erforderliche Capabilities:** `QUERY_DEVELOPMENT` für alle Anfragen, einschließlich Introspektion. [tool-verified: `provisa/api/data/endpoint.py:186-283`]

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

### Weiterleitung (Redirect)

Ergebnisse oberhalb eines konfigurierten Zeilenschwellenwerts (oder wenn `X-Provisa-Redirect: true`) werden nach S3 geschrieben, und es wird eine vorsignierte URL zurückgegeben. (REQ-029, REQ-044)

| Weiterleitungsformat | Geschrieben von | Speicher |
| --- | --- | --- |
| `application/vnd.apache.parquet` | föderiertes CTAS | Keiner — Daten laufen nie durch Provisa |
| `application/x-orc` | föderiertes CTAS | Keiner — Daten laufen nie durch Provisa |
| `application/json` | Provisa | Speicherabhängig |
| `application/x-ndjson` | Provisa | Speicherabhängig |
| `text/csv` | Provisa | Speicherabhängig |
| `application/vnd.apache.arrow.stream` | Provisa | Speicherabhängig |

Für große analytische Exporte verwenden Sie die Parquet- oder ORC-Weiterleitung. Die Föderations-Engine schreibt parallel direkt nach S3 — keine Daten laufen durch Provisa. (REQ-138)

```yaml
X-Provisa-Redirect-Format: application/vnd.apache.parquet
X-Provisa-Redirect-Threshold: 1000
```

---

### `POST /data/sql`

Führt rohes SQL über die Governance-Pipeline von Stufe 2 aus. (REQ-267) [tool-verified: `provisa/api/data/endpoint_dev.py:62`]

**Anfragetext:**

```json
{
  "sql": "SELECT id, amount FROM orders WHERE region = 'us'",
  "role": "admin",
  "discovery_mode": false
}
```

Das Flag `discovery_mode` erweitert die Sichtbarkeitsprüfung für Tabellen auf alle Tabellen aus allen Kontexten. Nur für interne Werkzeuge. [tool-verified: `provisa/api/data/endpoint_dev.py:148-152`]

**Erforderliche Capabilities:** `QUERY_DEVELOPMENT`.

Governance-Verstöße bei `POST /data/sql` liefern HTTP 403. (REQ-002, REQ-266)

**Antwort:** Gleiches Format wie `/data/graphql` (standardmäßig JSON-Zeilen, per Content Negotiation über `Accept` gesteuert).

---

### `POST /data/query`

Vereinheitlichter Abfrage-Endpunkt. Akzeptiert GraphQL, SQL oder Cypher — die Syntax wird automatisch erkannt. (REQ-267) [tool-verified: `provisa/api/data/endpoint_dev.py:509`]

Cypher-Abfragen können auch an den ausschließlich für Cypher vorgesehenen Endpunkt `POST /query/cypher` gesendet werden. (REQ-345)

**Anfragetext:**

```json
{
  "query": "{ orders { id } }",
  "params": {},
  "variables": {},
  "role": "admin"
}
```

Liefert `{"data": ...}` für GraphQL, `{"columns": [...], "rows": [...]}` für SQL und Cypher.

---

### `GET /data/rest/{domain_id}/{table_name}`

Automatisch generierter einfacher REST-Endpunkt für jede registrierte Tabelle. Der Query-String wird auf GraphQL-Argumente abgebildet, und die Anfrage wird über dieselbe Pipeline (RLS, Maskierung, Routing) wie GraphQL kompiliert und ausgeführt. (REQ-256) [tool-verified: `provisa/api/rest/generator.py:153`]

**Query-Parameter:**

- `limit` — maximale Zeilenanzahl (≥ 1)
- `offset` — zu überspringende Zeilen (≥ 0)
- `fields` — durch Kommas getrennte Spaltennamen (Standard: alle skalaren Felder)
- `filter` — JSON-Array von Filterobjekten `{"field", "comparator", "value"}`
- `orderBy` — JSON-Array von Sortierobjekten `{"field", "direction"}`

Die authentifizierte Rolle ist erforderlich; nicht authentifizierte Anfragen liefern `401`. Eine OpenAPI-Spezifikation für diese Routen wird unter `GET /data/rest/openapi.json` bereitgestellt, mit Swagger UI unter `GET /data/rest/docs`.

---

### `GET /data/jsonapi/{domain_id}/{table_name}`

Automatisch generierter, [JSON:API](https://jsonapi.org)-konformer Endpunkt für jede registrierte Tabelle. Gleiche RLS-, Maskierungs- und Routing-Logik wie GraphQL. (REQ-257) [tool-verified: `provisa/api/jsonapi/generator.py:284`]

**`Accept`-Header:** muss `application/vnd.api+json` (den JSON:API-Medientyp) enthalten, sonst liefert die Anfrage `406`.

**Query-Parameter:**

- `fields[<type>]` — Sparse Fieldsets, z. B. `?fields[orders]=amount`
- `filter[<col>]` / `filter[<col>][<op>]` — z. B. `?filter[region]=US`, `?filter[amount][gt]=100`
- `sort` — durch Kommas getrennt, Präfix `-` für absteigende Sortierung, z. B. `?sort=-created_at,amount`
- `page[number]` / `page[size]` — Paginierung

Antworten sind Ressourcenobjekte mit `type`/`id`/`attributes`. Fehler folgen der JSON:API-Fehlerobjektstruktur.

---

### `POST /query/nl`

Übermittelt eine Frage in natürlicher Sprache. Der Dienst startet einen asynchronen Job und liefert sofort `202 Accepted` mit einer `job_id`. Erfordert einen unter dem Konfigurationsabschnitt `ai_models` konfigurierten LLM-Anbieter. (REQ-354) [tool-verified: `provisa/api/rest/nl_router.py:50`]

**Anfragetext:**

```json
{"q": "How many orders were placed last month?", "role": "admin"}
```

Liefert `{"job_id": "<id>"}`. Das Überschreiten des NL-Ratenlimits pro Rolle liefert `429` mit einem `Retry-After`-Header. (REQ-370)

**Ergebnis abrufen:**

- `GET /query/nl/{job_id}` — Polling. Liefert das Job-Dokument.
- `GET /query/nl/{job_id}/stream` — SSE. Ein `branch`-Ereignis pro Generierungsziel bei dessen Abschluss, gefolgt von einem `done`-Ereignis. (REQ-357, REQ-358)

Drei Generierungsschleifen (Cypher, GraphQL, SQL) laufen parallel, jede über den Compiler validiert und bei Fehlern verfeinert. (REQ-355) Der Prompt ist auf das für die Rolle sichtbare Schema beschränkt. (REQ-356) Das Ergebnisdokument indiziert jeden Zweig nach Ziel: (REQ-357) [tool-verified: `provisa/nl/job.py:69`]

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

Ein Zweig, der sein Iterationslimit ausschöpft, liefert `query: null`, `result: null` und eine `error`-Zeichenkette. Jede generierte Abfrage wird unter den Rechten des Konsumenten mit angewendeter Stufe-2-Governance ausgeführt — der Dienst umgeht Governance niemals. (REQ-359)

---

### `GET /data/sdl`

Liefert das GraphQL-SDL für das Schema einer Rolle. (REQ-008) [tool-verified: `provisa/api/data/sdl.py:137`]

**Header:** `X-Role: <role_id>` (erforderlich)

**Query-Parameter:**

- `domain` — durch Kommas getrennte Domänen-IDs. Wenn gesetzt, wird die Antwort auf die genannte(n) Domäne(n) und die davon erreichbaren Tabellen gefiltert.

**Antwort:** GraphQL-SDL als `text/plain`.

---

### `GET /data/introspection`

Liefert GraphQL-Introspektions-JSON, optional domänengefiltert. [tool-verified: `provisa/api/data/sdl.py:200`]

**Header:** `X-Provisa-Role: <role_id>` (erforderlich)

**Query-Parameter:** `domain` — durch Kommas getrennte Domänen-IDs.

**Antwort:** Introspektionsergebnis als `application/json`.

---

### `GET /data/graph-schema`

Liefert die Graphenansicht des Rollenschemas: Knotenbezeichnungen und deren Beziehungstypen, für Cypher-/Graph-Clients. Enthält `pk_columns` je Knotenbezeichnung, damit Aufrufer die Primärschlüsselspalten bestimmen können. (REQ-398) [tool-verified: `provisa/api/rest/cypher_router.py:689`]

**Antwort:** `application/json` mit `node_labels` (jeweils mit `pk`/`pk_columns`) und `relationship_types`.

---

### `GET /data/domains`

Liefert die für die anfragende Rolle zugänglichen Domänen-IDs. [tool-verified: `provisa/api/data/sdl.py:116`]

**Header:** `X-Role: <role_id>` (erforderlich)

**Antwort:** `["sales", "support", ...]`

---

### `GET /data/schema-version`

Liefert die aktuelle Schema-Versionszeichenkette. Kombiniert eine Nonce pro Systemstart mit einem Rebuild-Zähler. Clients nutzen dies, um Schema-Caches nach Serverneustarts zu invalidieren. (REQ-537) [tool-verified: `provisa/api/data/sdl.py:102`]

**Antwort:** `{"version": "<boot-id>-<counter>"}`

---

### `GET /data/proto/{role_id}`

Liefert die automatisch generierte `.proto`-Datei für eine Rolle. [tool-verified: `provisa/api/data/endpoint_dev.py:49`]

**Antwort:** Protobuf-Schema als `text/plain`.

Jede registrierte Tabelle erzeugt eine proto-`message`. Beziehungen erzeugen verschachtelte Message-Felder. Typzuordnung: `integer → int32`, `bigint → int64`, `varchar → string`, `decimal → double`, `boolean → bool`, `timestamp → google.protobuf.Timestamp`. (REQ-538)

---

### `GET /data/subscribe/{table}`

Server-Sent-Events-Stream für Echtzeit-Änderungsbenachrichtigungen einer Tabelle. (REQ-219, REQ-258) [tool-verified: `provisa/api/data/subscribe.py:239`]

Die Zustellung von Benachrichtigungen nutzt einen austauschbaren Provider, der je nach Quelltyp gewählt wird: PostgreSQL-Quellen verwenden `LISTEN/NOTIFY` (über asyncpg), MongoDB-Quellen verwenden Change Streams (`collection.watch()`), und Kafka-Quellen verwenden Consumer Groups. Jeder Provider implementiert eine gemeinsame asynchrone Watch-Schnittstelle. RLS-Filterung und Schemavalidierung gelten unabhängig vom Provider. (REQ-258) WebSocket- und RSS-Quellen werden ebenfalls unterstützt. (REQ-338, REQ-342)

**Header — `X-Provisa-Sink`:** Auf ein Kafka-Ziel setzen (z. B. `kafka://broker:9092/topic`), um Änderungsereignisse anstelle der SSE-Antwort an eine Kafka-Senke umzuleiten. Der Server startet einen Sink-Consumer und liefert `202 Accepted` anstelle eines offenen Streams. (REQ-812) [tool-verified: `provisa/api/data/subscription_sse.py:137`]

---

## Admin-REST-Endpunkte

### Config

#### `GET /admin/config`

Lädt die aktuelle `provisa.yaml` als `application/x-yaml` mit einem `Content-Disposition: attachment`-Header herunter. (REQ-164) [tool-verified: `provisa/api/admin/settings_router.py:19`]

#### `PUT /admin/config`

Lädt eine überarbeitete Konfigurations-YAML hoch. Der Server schreibt ein `.bak`-Backup, speichert die neue Datei und lädt alle Schemata, Quellen und materialisierten Sichten neu. (REQ-164) [tool-verified: `provisa/api/admin/settings_router.py:32`]

**Anfragetext:** Roher YAML-Inhalt.

**Antwort:**

```json
{"success": true, "message": "Config uploaded and reloaded"}
```

Bei fehlgeschlagenem Neuladen: `{"success": false, "message": "<error>"}`.

---

### Einstellungen (Settings)

#### `GET /admin/settings`

Liefert die aktuellen Plattformeinstellungen als JSON. (REQ-165) [tool-verified: `provisa/api/admin/settings_router.py:50`]

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

Aktualisiert Plattformeinstellungen zur Laufzeit. Alle Felder sind optional — es werden nur die im Anfragetext vorhandenen Schlüssel aktualisiert. (REQ-165) [tool-verified: `provisa/api/admin/settings_router.py:100`]

**Anfragetext (Teilbeispiel):**

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

Aktualisierbare Felder je Abschnitt:

- `redirect`: `enabled`, `threshold`, `default_format`, `ttl`
- `sampling`: `default_sample_size`
- `cache`: `default_ttl`
- `naming`: `domain_prefix`, `convention` — schreibt in die Konfigurationsdatei und löst ein Schema-Reload aus (REQ-253)
- `relationships`: `auto_track_fk`
- `otel`: `endpoint`, `service_name`, `sample_rate`, `support_endpoint`, `support_redact_sql_literals`, `support_redact_attributes`

**Antwort:**

```json
{"success": true, "updated": ["otel.support_endpoint", "cache.default_ttl"]}
```

---

### Observability

#### `GET /admin/traces/recent`

Liefert bis zu N zuletzt abgeschlossene Spans aus dem In-Memory-Span-Puffer. (REQ-302) [tool-verified: `provisa/api/admin/settings_router.py:317`]

**Query-Parameter:** `limit` (Standard 50, maximal 200)

**Antwort:** `{"traces": [...]}`

#### `POST /admin/query-engine/reload-catalog`

Lädt einen benannten Katalog im Koordinator der Föderations-Engine über dessen REST-API im laufenden Betrieb neu. Verbindet die interne Verbindung von Provisa neu und führt das OTel-DDL erneut aus. [tool-verified: `provisa/api/admin/settings_router.py:208`]

**Query-Parameter:** `catalog` (Standard `"otel"`)

**Antwort:**

```json
{"success": true, "errors": []}
```

#### `POST /admin/query-engine/restart`

Startet den Container der Föderations-Engine neu (nur für Einzelknoten-Entwicklung). [tool-verified: `provisa/api/admin/settings_router.py:287`]

**Query-Parameter:** `container` (Standard: Umgebungsvariable `QUERY_ENGINE_CONTAINER`, dann `"trino"`)

---

### Discovery

#### `POST /admin/discover/relationships`

Löst die Beziehungserkennung aus. Führt immer eine Fremdschlüssel-Introspektion von der Föderations-Engine aus. (REQ-018) Führt LLM-Inferenz aus, wenn `ANTHROPIC_API_KEY` gesetzt ist. (REQ-167) [tool-verified: `provisa/api/admin/discovery.py:55`]

**Anfragetext:**

```json
{
  "scope": "domain",
  "domain_id": "sales"
}
```

`scope` muss `"table"`, `"domain"` oder `"cross-domain"` sein. Für den Geltungsbereich `"table"` ist `table_id` (Ganzzahl) erforderlich. Für den Geltungsbereich `"domain"` ist `domain_id` erforderlich.

**Antwort:** `{"candidates_found": 12, "stored_ids": [1, 2, 3, ...]}`

#### `GET /admin/discover/candidates`

Listet ausstehende Beziehungskandidaten auf. [tool-verified: `provisa/api/admin/discovery.py:96`]

#### `POST /admin/discover/candidates/{candidate_id}/accept`

Akzeptiert einen Kandidaten und registriert ihn als Beziehung. [tool-verified: `provisa/api/admin/discovery.py:103`]

**Anfragetext (optional):** `{"name": "custom-relationship-name"}`

#### `POST /admin/discover/candidates/{candidate_id}/reject`

Lehnt einen Kandidaten ab. [tool-verified: `provisa/api/admin/discovery.py:110`]

**Anfragetext:** `{"reason": "Not a real join"}`

#### `GET /admin/discover/candidates/rejected/count`

Liefert die Anzahl abgelehnter Kandidaten. [tool-verified: `provisa/api/admin/discovery.py:118`]

#### `DELETE /admin/discover/candidates/rejected`

Löscht alle abgelehnten Kandidaten. [tool-verified: `provisa/api/admin/discovery.py:128`]

---

### Quellen-Crawling (Source Crawl)

#### `POST /admin/sources/crawl`

Durchsucht (crawlt) eine Datenquelle, um deren Schema zu introspektieren und Tabellen zu registrieren. (REQ-012) [tool-verified: `provisa/api/admin/crawl_router.py:36`]

---

### Suche nach Quelltabellen

#### `GET /admin/sources/{source_id}/tables/search`

Sucht nach Namen in einer Quelle verfügbare (noch nicht registrierte) Tabellen. [tool-verified: `provisa/api/admin/table_search_router.py:103`]

---

### Tabellenprofilierung

#### `POST /admin/tables/{table_id}/profile`

Führt ein Spaltenprofil für eine registrierte Tabelle aus — Kardinalität, Min/Max, Nullwertraten. [tool-verified: `provisa/api/admin/table_profile_router.py:28`]

---

### Quellenbeschreibungen

#### `POST /admin/source-meta/db-description`

Generiert LLM-gestützte Beschreibungen für Tabellen und Spalten einer Quelle. [tool-verified: `provisa/api/admin/source_meta_router.py:48`]

---

### Aktionen (Funktionen und Webhooks)

Alle Endpunkte befinden sich unter dem Präfix `/admin/actions`. (REQ-205) [tool-verified: `provisa/api/admin/actions_router.py:24`]

Jeder Aufruf — von GraphQL, SQL, Cypher, Bolt, Arrow Flight, MCP `run_sql` und Provisa gRPC — wird über einen einzigen governance-gesteuerten Executor geleitet, der `writable_by` und die Governance einheitlich durchsetzt. (REQ-1156) [tool-verified: `provisa/api/data/action_exec.py`] Siehe [docs/integrations.md](integrations.md#befehle-uber-protokolle-hinweg-aufrufen) für die protokollspezifische Aufrufsyntax.

#### `GET /admin/actions`

Liefert alle erfassten DB-Funktionen und Webhooks. (REQ-242) [tool-verified: `provisa/api/admin/actions_router.py:104`]

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

Jedes Webhook-Objekt trägt einen booleschen Wert `approved`. Ein Webhook wird genehmigt, sobald ein Steward dessen Erstellungsanfrage ausführt (REQ-209); in der Konfiguration deklarierte Webhooks werden automatisch genehmigt. Ein nicht genehmigter Webhook wird registriert, aber auf keiner Oberfläche exponiert. [tool-verified: `provisa/api/admin/actions_router.py:124-131`]

#### `POST /admin/actions/functions`

Registriert eine erfasste Funktion (Command). (REQ-205) [tool-verified: `provisa/api/admin/actions_router.py:117`]

**Wichtige Felder:**

| Feld | Erforderlich | Beschreibung |
| --- | --- | --- |
| `name` | Ja | Eindeutiger Command-Name |
| `kind` | Ja | `"query"` → GraphQL-Query-Feld; `"mutation"` → Mutation-Feld |
| `implKind` | Nein | Wie der Command ausgeführt wird — siehe Tabelle unten (Standard `source_procedure`) |
| `binding` | Nein | `implKind`-spezifische Verbindungsdetails (JSON-Objekt) |
| `returnSchema` | Nein | JSON Schema `{type:"array", items:{type:"object", properties:{...}}}` — macht den Command auf jeder Oberfläche mengenwertig |
| `arguments` | Nein | Argumentdefinitionen `[{name, type}]`; die Reihenfolge zählt für SQL- und Bolt-Aufrufer |
| `visibleTo` | Nein | Rollen-IDs, die den Command aufrufen dürfen |
| `writableBy` | Nein | Rollen-IDs, die ihn als Mutation aufrufen dürfen |
| `domainId` | Nein | Domäne für die GraphQL-Platzierung und Zugriffskontrolle |

**Werte von `implKind`:**

| `implKind` | Was ausgeführt wird | Felder von `binding` |
| --- | --- | --- |
| `source_procedure` | Gespeicherte Prozedur auf einer registrierten Quelle (Standard) | `sourceId`, `schemaName`, `functionName` |
| `script` | Serverseitiges Skript | `script` |
| `http` | Ausgehender HTTP-Aufruf | `url`, `method` |
| `grpc` | Ausgehender gRPC-Aufruf an einen externen Server | `target`, `method` |
| `python` | Von Provisa gehostetes Python-Callable (REQ-885) | `callable` (z. B. `"demo.py_functions:random_dataset"`) |

Die Demo-Commands `random_python_set` (`implKind: python`) und `random_grpc_set` (`implKind: grpc`) zeigen in der Praxis mengenwertige Commands mit `returnSchema`; beide befinden sich in `config/provisa-install.yaml`. [tool-verified: `config/provisa-install.yaml:809-856`]

#### `PUT /admin/actions/functions/{name}`

Aktualisiert eine erfasste Funktion anhand ihres Namens. [tool-verified: `provisa/api/admin/actions_router.py:182`]

#### `DELETE /admin/actions/functions/{name}`

Löscht eine erfasste Funktion anhand ihres Namens. [tool-verified: `provisa/api/admin/actions_router.py:233`]

#### `POST /admin/actions/webhooks`

Registriert einen erfassten Webhook. (REQ-209) Das Registrieren oder Aktualisieren eines Webhooks reiht eine Steward-Genehmigungsanfrage ein — der Webhook wird auf allen Oberflächen erst aktiv, nachdem ein Steward ihn genehmigt hat. In der Konfiguration deklarierte Webhooks werden automatisch genehmigt. **Felder des Anfragetexts:** `name`, `url`, `method`, `timeoutMs`, `returns`, `inlineReturnType`, `arguments`, `visibleTo`, `domainId`, `description`, `kind`. [tool-verified: `provisa/api/admin/actions_router.py:132`, `provisa/api/admin/actions_router.py:325-331`]

#### `PUT /admin/actions/webhooks/{name}`

Aktualisiert einen erfassten Webhook anhand seines Namens. Jede Änderung setzt die Genehmigung bis zur erneuten Genehmigung auf ausstehend zurück. [tool-verified: `provisa/api/admin/actions_router.py:306`]

#### `DELETE /admin/actions/webhooks/{name}`

Löscht einen erfassten Webhook anhand seines Namens. [tool-verified: `provisa/api/admin/actions_router.py:355`]

#### `POST /admin/actions/test`

Testet eine Aktion (Funktion oder Webhook) anhand ihres Namens. (REQ-245) [tool-verified: `provisa/api/admin/actions_router.py:384`]

---

### Rollen

Alle Endpunkte befinden sich unter dem Präfix `/admin/roles`. [tool-verified: `provisa/api/admin/roles_router.py:18`]

| Methode | Pfad | Beschreibung |
| --- | --- | --- |
| `GET` | `/admin/roles/` | Listet alle Rollen |
| `POST` | `/admin/roles/` | Erstellt eine Rolle |
| `PUT` | `/admin/roles/{role_id}` | Aktualisiert eine Rolle |
| `DELETE` | `/admin/roles/{role_id}` | Löscht eine Rolle |

[tool-verified: `provisa/api/admin/roles_router.py`]

---

### Benutzer

Alle Endpunkte befinden sich unter dem Präfix `/admin/users`. [tool-verified: `provisa/api/admin/local_users_router.py:21`]

| Methode | Pfad | Beschreibung |
| --- | --- | --- |
| `POST` | `/admin/users/` | Erstellt einen lokalen Benutzer |
| `GET` | `/admin/users/` | Listet lokale Benutzer |
| `GET` | `/admin/users/{user_id}` | Ruft einen Benutzer ab |
| `PUT` | `/admin/users/{user_id}` | Aktualisiert einen Benutzer |
| `PATCH` | `/admin/users/{user_id}/password` | Ändert das Passwort |
| `DELETE` | `/admin/users/{user_id}` | Löscht einen Benutzer |
| `GET` | `/admin/users/{user_id}/assignments` | Listet Rollenzuweisungen |
| `POST` | `/admin/users/{user_id}/assignments` | Fügt eine Rollenzuweisung hinzu |
| `DELETE` | `/admin/users/{user_id}/assignments/{assignment_id}` | Entfernt eine Rollenzuweisung |

---

### Organisationen

Alle Endpunkte befinden sich unter `/admin/orgs`. [tool-verified: `provisa/api/admin/orgs_router.py:18`]

| Methode | Pfad | Beschreibung |
| --- | --- | --- |
| `GET` | `/admin/orgs/` | Listet Organisationen |
| `POST` | `/admin/orgs/` | Erstellt eine Organisation |
| `PUT` | `/admin/orgs/{org_id}` | Aktualisiert eine Organisation |
| `DELETE` | `/admin/orgs/{org_id}` | Löscht eine Organisation |
| `GET` | `/admin/orgs/{org_id}/members` | Listet Mitglieder |
| `POST` | `/admin/orgs/{org_id}/members` | Fügt ein Mitglied hinzu |
| `DELETE` | `/admin/orgs/{org_id}/members/{user_id}` | Entfernt ein Mitglied |

---

### Einladungen

Alle Endpunkte befinden sich unter `/admin/invites`. [tool-verified: `provisa/api/admin/invites_router.py:18`]

| Methode | Pfad | Beschreibung |
| --- | --- | --- |
| `POST` | `/admin/invites/` | Erstellt eine Einladung |
| `GET` | `/admin/invites/` | Listet ausstehende Einladungen |
| `DELETE` | `/admin/invites/{token}` | Widerruft eine Einladung |

---

### Admin-GraphQL

#### `POST /admin/graphql`

Strawberry-GraphQL-Endpunkt für alle Admin-Operationen: CRUD für Quellen und Tabellen, Beziehungsverwaltung, Domänenkonfiguration, RLS-Regeln, Cache-Steuerung, Namenskonventionen, Verwaltung geplanter Aufgaben und Abfragekompilierung. (REQ-164) [tool-verified: `provisa/api/app.py:2171`]

**Wichtige Mutations:**

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

Liefert den Status der Ersteinrichtung. Immer ohne Authentifizierung. (REQ-539) [tool-verified: `provisa/api/setup_router.py:100`]

#### `POST /setup/`

Schließt die Ersteinrichtung ab. [tool-verified: `provisa/api/setup_router.py:142`]

---

## Health Check

#### `GET /health` oder `HEAD /health`

Liefert `{"status": "ok"}`. Immer ohne Authentifizierung. (REQ-539) [tool-verified: `provisa/api/app.py:2258`]

---

## Fehlerantworten

| Status | Bedeutung |
| --- | --- |
| 400 | Ungültige Abfrage, Validierungsfehler oder SQL-Parsing-Fehler |
| 401 | Fehlendes oder ungültiges Authentifizierungstoken |
| 403 | Unzureichende Capabilities; Governance-Verstoß |
| 404 | Rolle, Ressource oder Konfigurationsdatei nicht gefunden |
| 422 | Erforderlicher Header fehlt (z. B. `X-Role`) |
| 503 | Datenbank oder Quelle nicht verbunden; Abhängigkeit nicht verfügbar |
| 504 | Zeitüberschreitung der Anfrage |

Governance-Verstöße bei `POST /data/sql` liefern HTTP 403 mit einem strukturierten Body: (REQ-002) [tool-verified: `provisa/api/data/endpoint_dev.py:184-190`]

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

Port `8815`. Natives, spaltenorientiertes Arrow-Transportprotokoll über gRPC. (REQ-143, REQ-045) [tool-verified: `provisa/api/flight/server.py`]

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

Wenn der Zaychik-Flight-SQL-Proxy verfügbar ist (Port 8480), werden Record Batches durchgängig ohne vollständige Materialisierung gestreamt. (REQ-144) Fällt auf die Materialisierung über die föderierte Abfrageschicht zurück, wenn Zaychik nicht verfügbar ist. (REQ-146)

---

## Protobuf-gRPC-Endpunkt

Port `50051` (überschreibbar mit der Umgebungsvariable `GRPC_PORT` oder der Konfiguration `server.grpc_port`). (REQ-529) [tool-verified: `provisa/grpc/server.py`, `provisa/api/app.py`]

Übergeben Sie die Rolle im gRPC-Metadatenschlüssel `x-provisa-role`. Fehlt dieser, bricht der Server mit `UNAUTHENTICATED` ab. [tool-verified: `provisa/grpc/server.py`]

Laden Sie das rollenspezifische Proto von `GET /data/proto/{role_id}` herunter. Es erscheinen nur Tabellen und Spalten, die für diese Rolle sichtbar sind. (REQ-039)

```proto
service ProvisaService {
  rpc QueryOrders (QueryOrdersRequest) returns (stream Orders);
  rpc InsertOrders (InsertOrdersRequest) returns (InsertOrdersResponse);
}
```

Jede Tabelle erzeugt einen streamenden `Query{TypeName}`-RPC. `Insert{TypeName}`-RPCs existieren aus Gründen der Schemasymmetrie, brechen aber mit `UNIMPLEMENTED` ab. [tool-verified: `provisa/grpc/server.py`]

`grpc_reflection.v1alpha` ist für die Service Discovery ohne vorkompiliertes Proto aktiviert. (REQ-529) [tool-verified: `provisa/grpc/reflection.py`]

```bash
grpcurl -plaintext localhost:50051 list
grpcurl -plaintext -H 'x-provisa-role: analyst' \
  -d '{}' localhost:50051 ProvisaService/QueryOrders
```

Der gRPC-Server startet nur, wenn beim Start ein gültiges Proto kompiliert werden kann. Schlägt der Schema-Build fehl, startet der gRPC-Server nicht. (REQ-529)

---

## JDBC-Treiber

Der Provisa-JDBC-Treiber (`provisa-jdbc-0.1.0.jar`) macht den semantischen Katalog für BI-Tools (Tableau, PowerBI, DBeaver) zugänglich. (REQ-126)

**Verbindungs-URL:** `jdbc:provisa://host:port` (REQ-131)

Domänen werden auf JDBC-Schemata abgebildet. (REQ-127) Tabellen verwenden ihre registrierten Aliase. Spalten verwenden Aliase und zeigen Beschreibungen als `REMARKS` an. (REQ-128) Standard-Metadatenmethoden (`getPrimaryKeys`, `getImportedKeys`, `getExportedKeys`) exponieren semantische Beziehungen als PK/FK-Metadaten.

**SQL-Unterstützung:** `SELECT * FROM <alias> [WHERE col = 'value']`. (REQ-129)

Der Treiber fordert standardmäßig eine Arrow-IPC-Weiterleitung an. Ergebnisse werden über `ArrowStreamReader` batchweise gestreamt, begrenzt auf einen Record Batch im Speicher. (REQ-293)

---

## Format des `orderBy`-Arguments

Das Argument `order_by` verwendet `{column: direction}`-Objekte mit einer 6-wertigen Richtungs-Enumeration: (REQ-200)

```json
{
  "query": "{ orders(order_by: [{created_at: desc_nulls_last}]) { id created_at } }",
  "role": "admin"
}
```

Unterstützte Richtungen: `asc`, `desc`, `asc_nulls_first`, `asc_nulls_last`, `desc_nulls_first`, `desc_nulls_last`. (REQ-201)

---

## Subscriptions

SSE-Subscriptions sind unter `GET /data/subscribe/{table}` verfügbar. (REQ-219, REQ-258) Die Zustellung von Benachrichtigungen nutzt einen austauschbaren Provider, der je nach Quelltyp ausgewählt wird: PostgreSQL-Quellen verwenden `LISTEN/NOTIFY`, MongoDB-Quellen verwenden Change Streams, und Kafka-Quellen verwenden Consumer Groups. RLS-Filterung und Schemavalidierung gelten unabhängig vom Provider. WebSocket- und RSS-Quellen werden ebenfalls über denselben Endpunkt unterstützt. (REQ-338, REQ-342) [tool-verified: `provisa/api/data/subscribe.py:239`, `provisa/subscriptions/registry.py`, `provisa/api/app.py` `_rebuild_schemas`]
