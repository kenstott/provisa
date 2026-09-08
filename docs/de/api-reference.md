# API-Referenz

## Übersicht

Provisa stellt REST-Endpunkte unter zwei Präfixen bereit: `/data` für Abfrageausführung und Schema-Introspektion sowie `/admin` für Konfigurationsverwaltung. (REQ-043) Die meisten Daten-Endpunkte erfordern einen Rollenbezeichner. Admin-Konfigurationsoperationen verwenden eine Strawberry-GraphQL-API unter `/admin/graphql`. (REQ-164)

---

## Authentifizierung

Wenn `auth.provider` in `provisa.yaml` konfiguriert ist, erfordern alle Endpunkte außer `/health` und `/setup/status` einen `Authorization: Bearer <token>`-Header. (REQ-120) [tool-verified: `provisa/api/app.py`, `provisa/auth/wiring.py`]

Ohne konfigurierte Authentifizierung läuft der Server im Dev-Modus. Jede Anfrage wird als `anonymous`-Identität behandelt, die auf alle konfigurierten Rollen mit Wildcard-Domänenzugriff abgebildet wird. (REQ-535)

**Login (`POST /auth/login`)** wird vom aktiven Auth-Provider bereitgestellt, wenn `provider: basic` konfiguriert ist. (REQ-124) Anmeldeformat und Antwort hängen vom Provider ab.

**Identitäts-Introspektion:**

```http
GET /auth/me
```

Gibt die id, E-Mail, den Anzeigenamen, die Org-Mitgliedschaften und Rollenzuweisungen des authentifizierten Benutzers zurück. Im Dev-Modus wird `dev_mode: true` mit allen aufgelisteten Rollen-IDs zurückgegeben. [tool-verified: `provisa/api/auth_router.py`]

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

Das Feld `extensions` unterstützt das Automatic-Persisted-Query-(APQ)-Protokoll: (REQ-288)

```json
{
  "extensions": {"persistedQuery": {"sha256Hash": "<sha256-of-query>"}}
}
```

**Headers:**

- `X-Provisa-Role` — Rolle überschreiben (Dev-Modus)
- `Accept` — Antwortformat (siehe Content Negotiation)
- `Authorization` — `Bearer <token>`, wenn Authentifizierung aktiviert ist
- `X-Provisa-Redirect-Format` — MIME-Typ für die S3-Redirect-Ausgabe (REQ-137)
- `X-Provisa-Redirect-Threshold` — Zeilenanzahl, oberhalb derer der Redirect ausgelöst wird (REQ-137)
- `X-Provisa-Redirect` — `true`, um den Redirect bedingungslos zu erzwingen (REQ-029)

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

Multi-Root-Abfragen führen jedes Root-Feld unabhängig aus. Felder unterhalb des Redirect-Schwellenwerts werden inline zurückgegeben; Felder darüber werden umgeleitet. Der (plurale) Schlüssel `redirects` bildet Feldnamen auf Redirect-Informationen ab. (REQ-029) [tool-verified: `provisa/api/data/endpoint.py`]

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

Ergebnisse oberhalb eines konfigurierten Zeilenschwellenwerts (oder wenn `X-Provisa-Redirect: true`) werden nach S3 geschrieben, und es wird eine presigned URL zurückgegeben. (REQ-029, REQ-044)

| Redirect-Format | Geschrieben von | Speicher |
| --- | --- | --- |
| `application/vnd.apache.parquet` | föderiertes CTAS | Keiner — Daten laufen nie durch Provisa |
| `application/x-orc` | föderiertes CTAS | Keiner — Daten laufen nie durch Provisa |
| `application/json` | Provisa | Speicherbegrenzt |
| `application/x-ndjson` | Provisa | Speicherbegrenzt |
| `text/csv` | Provisa | Speicherbegrenzt |
| `application/vnd.apache.arrow.stream` | Provisa | Speicherbegrenzt |

Für große analytische Exporte verwenden Sie den Parquet- oder ORC-Redirect. Die Föderations-Engine schreibt direkt und parallel nach S3 — keine Daten laufen durch Provisa. (REQ-138)

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

**Antwort:** Gleiches Format wie `/data/graphql` (standardmäßig JSON-Zeilen, contentverhandelt über `Accept`).

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

### `POST /data/sql/explain`

Erklärt oder analysiert eine SQL-Anweisung durch die governte Pipeline. (REQ-1519) [tool-verified: `provisa/api/data/endpoint_dev.py:328`]

Der Endpunkt umschließt das **governte** SQL — die Anweisung, die tatsächlich unter der Rolle des Aufrufers nach RLS und Maskierung läuft — mit der EXPLAIN-Syntax des Dialekts. Was der Plan zeigt, ist die autorisierte Version der Abfrage, nicht die rohe Eingabe.

**Request-Body:**

```json
{
  "sql": "SELECT id, amount FROM orders",
  "role": "admin",
  "analyze": false
}
```

Setzen Sie `analyze: true`, um EXPLAIN ANALYZE auszuführen. Die Abfrage wird ausgeführt, und der Plan trägt reale Zeilenzahlen und Zeitwerte. Nicht jeder Dialekt unterstützt ANALYZE; siehe die Tabelle in [Query plans and statistics](engines.md#query-plans-and-statistics).

**Antwort:** `{"plan": "<plan text or JSON>", "dialect": "trino", "analyzed": false}`

`400`, wenn der Dialekt keine EXPLAIN-Unterstützung hat oder `analyze: true` für einen Dialekt angefordert wird, der dies nicht unterstützt (z. B. SQLite). [tool-verified: `provisa/executor/explain.py:wrap_explain`, `analyze_sql`]

---

### `GET /data/engine/state`

Gibt den aktuellen Zustand des Engine-Shards zurück, ohne ihn zu wecken. (REQ-1516) [tool-verified: `provisa/api/data/endpoint_dev.py:892`]

Die UI pollt diesen Endpunkt, um während des Kaltstarts der Engine ein Startbanner anzuzeigen. Er löst nie ein Aufwecken aus — Polling ist sicher und zählt nicht als Aktivität für den Idle-Reaper.

**Antwort:**

```json
{"state": "ready"}
```

Mögliche Werte:

| Zustand | Bedeutung |
| --- | --- |
| `always-on` | Desktop, self-hosted oder BYO-Coordinator — kein Lifecycle-Management |
| `ready` | Shard ist hochgefahren und nimmt Abfragen an |
| `starting` | Kaltstart läuft |
| `stopped` | Shard ist auf null skaliert |

[tool-verified: `provisa/federation/engine_wake.py:engine_state`]

---

### `POST /data/engine/prewarm`

Löst ein Aufwecken der Engine aus, ohne eine Abfrage auszuführen. (REQ-1516) [tool-verified: `provisa/api/data/endpoint_dev.py:913`]

Gibt sofort `202 Accepted` zurück. Das Aufwecken läuft im Hintergrund. Verwenden Sie dies, wenn die Engine bereit sein soll, bevor die erste Abfrage eintrifft — zum Beispiel von einem Scheduler, der Abfragen erst einige Minuten später ausführt.

**Antwort:** `202 Accepted`, Body `{"started": true}`

[tool-verified: `provisa/federation/engine_wake.py:prewarm_engine`]

---

### `GET /data/rest/{domain_id}/{table_name}`

Automatisch generierter reiner REST-Endpunkt für jede registrierte Tabelle. Der Query-String wird auf GraphQL-Argumente abgebildet, und die Anfrage wird durch dieselbe Pipeline (RLS, Maskierung, Routing) wie GraphQL kompiliert und ausgeführt. (REQ-256) [tool-verified: `provisa/api/rest/generator.py:153`]

**Query-Parameter:**

- `limit` — maximale Zeilenanzahl (≥ 1)
- `offset` — zu überspringende Zeilen (≥ 0)
- `fields` — kommagetrennte Spaltennamen (Standard: alle skalaren Felder)
- `filter` — JSON-Array von `{"field", "comparator", "value"}`-Filterobjekten
- `orderBy` — JSON-Array von `{"field", "direction"}`-Sortierobjekten

Die authentifizierte Rolle ist erforderlich; nicht authentifizierte Anfragen geben `401` zurück. Eine OpenAPI-Spezifikation für diese Routen wird unter `GET /data/rest/openapi.json` bereitgestellt, mit Swagger UI unter `GET /data/rest/docs`.

#### OpenAPI-/Swagger-UI-Explorer

Die OpenAPI-Explorer-Seite (`/app/openapi`) bettet die Swagger UI in einem sandboxed iframe ein. Die Spezifikation ist rollenbezogen — nur für die aktuelle Rolle sichtbare Tabellen und Spalten erscheinen — und optional über den Domänenselektor domänengefiltert. Die UI wechselt automatisch zwischen hellem und dunklem Theme. [tool-verified: `provisa-ui/src/pages/OpenApiPage.tsx:20-34`]

Die Seite lädt das Spec-HTML über `fetch()` statt über ein direktes iframe-`src`, sodass die Anfrage das Bearer-Token der Sitzung trägt und die eigenen relativen Anfragen der Swagger UI korrekt gegen denselben Origin aufgelöst werden. [tool-verified: `provisa-ui/src/pages/OpenApiPage.tsx:44-69`]

Bei Navigation über einen NL-Link "In OpenAPI öffnen" klappt die Seite den Ziel-Endpunkt automatisch auf, füllt Query-Parameter aus der NL-generierten URL (z. B. `aggregate`, `groupBy`) und klickt Execute — mittels DOM-Polling, um sicherzustellen, dass jeder Schritt abgeschlossen ist, bevor der nächste ausgelöst wird. (REQ-1359) [tool-verified: `provisa-ui/src/pages/OpenApiPage.tsx:94-171`]

---

### `GET /data/jsonapi/{domain_id}/{table_name}`

Automatisch generierter, [JSON:API](https://jsonapi.org)-konformer Endpunkt für jede registrierte Tabelle. Gleiche RLS, Maskierung und Routing wie GraphQL. (REQ-257) [tool-verified: `provisa/api/jsonapi/generator.py:284`]

**`Accept`-Header:** muss `application/vnd.api+json` (den JSON:API-Medientyp) enthalten, sonst gibt die Anfrage `406` zurück.

**Query-Parameter:**

- `fields[<type>]` — Sparse Fieldsets, z. B. `?fields[orders]=amount`
- `filter[<col>]` / `filter[<col>][<op>]` — z. B. `?filter[region]=US`, `?filter[amount][gt]=100`
- `sort` — kommagetrennt, `-`-Präfix für absteigend, z. B. `?sort=-created_at,amount`
- `page[number]` / `page[size]` — Paginierung
- `aggregate` — kommagetrennte Aggregatfunktionen, die anstelle des Zeilenabrufs ausgeführt werden: `count`, `sum`, `avg`, `stddev`, `variance`, `min`, `max`. Verwenden Sie `?aggregate=count,sum`, um eine Teilmenge anzufordern. Aggregat-Antworten geben `data: null` mit Ergebnissen in `meta.aggregate` zurück. (REQ-1359) [tool-verified: `provisa-ui/src/pages/JsonApiPage.tsx:238`]
- `groupBy` — kommagetrennte Spaltennamen; wird zusammen mit `?aggregate=` verwendet, um Ergebnisse zu gruppieren. Nur Spalten im `DistinctOnColumn`-Enum der Tabelle sind gültig; der Server gibt `400` für jede Spalte zurück, die die Rolle nicht sehen kann. (REQ-1361) [tool-verified: `provisa-ui/src/pages/JsonApiPage.tsx:447`]
- `includeNodes` — `true`, um skalare Spalten der Basistabelle (und über `include=` verbundene skalare Dimensionsspalten) im `nodes`-Array jeder Gruppenzeile einzuschließen. Erforderlich, wenn eine NL-Group-by-Abfrage auch Dimensionsdetails anfordert. (REQ-1405)

Antworten sind Ressourcenobjekte mit `type`/`id`/`attributes`. Fehler folgen der JSON:API-Fehlerobjekt-Form.

#### JSON:API-Explorer

Die JSON:API-Explorer-Seite (`/app/jsonapi`) ist eine Browser-UI über diesen Endpunkten. Wählen Sie eine Tabelle aus der nach Domäne gruppierten Liste und konfigurieren Sie dann:

- **Fields** — wählen Sie, welche Spalten einbezogen werden (Sparse Fieldset); lassen Sie alle abgewählt, um jede Spalte anzufordern
- **Relationships** — wählen Sie FK-abgeleitete Beziehungsnamen zum Sideloading über `?include=`
- **Filter** — Feld, Operator (`eq`, `neq`, `gt`, `gte`, `lt`, `lte`, `like`) und Wert
- **Sort** — ein Feld, aufsteigend oder absteigend
- **Aggregate** — wählen Sie Group-by-Spalten aus der serverseitig validierten Liste und markieren Sie dann eine oder mehrere Aggregatfunktionen; wenn Group-by-Spalten ausgewählt sind, fügt ein Kontrollkästchen "Include nodes" skalare Spalten der Basistabelle zu jeder Zeile hinzu
- **Page size** — Ressourcen pro Seite, mit Navigation first/prev/next/last

Ergebnisse werden in einer formatierten Zusammenfassungsansicht (Ressourcenkarten mit anklickbaren Beziehungsankern) oder einem rohen JSON-Tab dargestellt. Die Live-Request-URL wird angezeigt und kann kopiert werden. Tabellenauswahl und Seitengröße bleiben sitzungsübergreifend im `localStorage` erhalten. [tool-verified: `provisa-ui/src/pages/JsonApiPage.tsx`]

Bei Navigation über einen NL-Link "In JSON:API öffnen" wählt der Explorer die Tabelle vorab aus und initialisiert den Aggregat-Picker aus den NL-generierten Query-Parametern, dann wird die Anfrage automatisch ausgeführt. [tool-verified: `provisa-ui/src/pages/JsonApiPage.tsx:460-479`]

---

### `POST /query/nl`

Reicht eine natürlichsprachliche Frage ein. Der Dienst startet einen asynchronen Job und gibt sofort `202 Accepted` mit einer `job_id` zurück. Erfordert einen LLM-Provider, der im `ai_models`-Konfigurationsabschnitt konfiguriert ist. (REQ-354) [tool-verified: `provisa/api/rest/nl_router.py:50`]

**Request-Body:**

```json
{"q": "How many orders were placed last month?", "role": "admin"}
```

Gibt `{"job_id": "<id>"}` zurück. Das Überschreiten des NL-Ratenlimits pro Rolle gibt `429` mit einem `Retry-After`-Header zurück. (REQ-370)

**Ergebnis abrufen:**

- `GET /query/nl/{job_id}` — Polling. Gibt das Job-Dokument zurück.
- `GET /query/nl/{job_id}/stream` — SSE. Ein `branch`-Ereignis pro Generierungsziel bei dessen Abschluss, dann ein `done`-Ereignis. (REQ-357, REQ-358)

Drei Generierungsschleifen (Cypher, GraphQL, SQL) laufen parallel, jede über den Compiler validiert und bei Fehlern verfeinert. (REQ-355) Der Prompt ist auf das sichtbare Schema der Rolle beschränkt. (REQ-356) Das Ergebnisdokument schlüsselt jeden Branch nach Ziel: (REQ-357) [tool-verified: `provisa/nl/job.py:69`]

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

Ein Branch, der sein Iterationslimit erschöpft, gibt `query: null`, `result: null` und eine `error`-Zeichenkette zurück. Jede generierte Abfrage wird unter den Rechten des Konsumenten mit angewandter Stage-2-Governance ausgeführt — der Dienst umgeht Governance nie. (REQ-359)

#### NL Group-By mit Dimensionsdetails (REQ-1405)

Wenn eine NL-Group-by-Abfrage auch Spalten aus einer verbundenen Dimensionstabelle projiziert — zum Beispiel "Anzahl der Anfragen nach Benutzer mit Benutzername und E-Mail" — leitet der Runner Pro-Feld-Punktpfade (`dim_paths`) aus den in der SELECT projizierten Dimensionsspalten ab. Diese Pfade befüllen den Parameter `includeNodes=` in den generierten URLs der JSON:API- und OpenAPI-Panels, sodass diese Panels dieselben verbundenen Dimensionsfelder anfordern, die die SQL- und GraphQL-Branches aufgelöst haben. Ohne dies würde `includeNodes=true` nur die eigenen skalaren Felder der Basis-Aggregattabelle zurückgeben. (REQ-1405) [tool-verified: `docs/arch/requirements.md:REQ-1405`]

Im gRPC-Panel trägt die generierte `{Type}GroupByRequest` `include_nodes` (bool) und `include` (repeated string mit Beziehungsfeldnamen). Die zurückgegebene `{Type}GroupByRow` enthält ein typisiertes `nodes`-Feld mit den Dimensionsdetail-Zeilen. [tool-verified: `provisa/grpc/query_ir.py:168-196`]

---

### `GET /data/sdl`

Gibt das GraphQL-SDL für das Schema einer Rolle zurück. (REQ-008) [tool-verified: `provisa/api/data/sdl.py:137`]

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

Gibt die Graphansicht des Schemas der Rolle zurück: Knoten-Labels und ihre Beziehungstypen, für Cypher-/Graph-Clients. Enthält `pk_columns` pro Knoten-Label, sodass Aufrufer Primärschlüsselspalten bestimmen können. (REQ-398) [tool-verified: `provisa/api/rest/cypher_router.py:689`]

**Antwort:** `application/json` mit `node_labels` (jeweils mit `pk`/`pk_columns`) und `relationship_types`.

Jeder Beziehungstyp trägt auch `junction_table_name` und `properties` (REQ-1586). Bei einer über eine Junction realisierten Kante benennt das erste die assoziative Tabelle, die durchlaufen wird, und das zweite listet die Spalten dieser Tabelle auf, die als `r.attr` lesbar und in `WHERE` filterbar sind; bei einer über einen Fremdschlüssel realisierten Kante ist der Name `null` und die Property-Liste leer, woran ein Client die beiden unterscheidet. Die Junction-Tabelle selbst ist nie ein Knoten-Label — sie ist die Kante, hat also keine Pille in einem Graph-Client und keine Zeile in `node_labels`. [tool-verified: `provisa/api/rest/cypher_router.py:797-805`, `provisa/cypher/label_map.py:378-397`]

---

### `GET /data/domains`

Gibt Domänen-IDs zurück, die für die anfragende Rolle zugänglich sind. [tool-verified: `provisa/api/data/sdl.py:116`]

**Headers:** `X-Role: <role_id>` (erforderlich)

**Antwort:** `["sales", "support", ...]`

---

### `GET /data/schema-version`

Gibt die aktuelle Schemaversions-Zeichenkette zurück. Kombiniert eine Boot-Nonce mit einem Rebuild-Zähler. Clients verwenden dies, um Schema-Caches nach Server-Neustarts zu invalidieren. (REQ-537) [tool-verified: `provisa/api/data/sdl.py:102`]

**Antwort:** `{"version": "<boot-id>-<counter>"}`

---

### `GET /data/proto/{role_id}`

Gibt die automatisch generierte `.proto`-Datei für eine Rolle zurück. [tool-verified: `provisa/api/data/endpoint_dev.py:49`]

**Antwort:** `text/plain`-Protobuf-Schema.

Jede registrierte Tabelle erzeugt eine Proto-`message`. Beziehungen erzeugen verschachtelte Message-Felder. Typabbildung: `integer → int32`, `bigint → int64`, `varchar → string`, `decimal → double`, `boolean → bool`, `timestamp → google.protobuf.Timestamp`. (REQ-538)

---

### `GET /data/subscribe/{table}`

Server-Sent-Events-Stream für Echtzeit-Änderungsbenachrichtigungen aus einer Tabelle. (REQ-219, REQ-258) [tool-verified: `provisa/api/data/subscribe.py:239`]

Die Benachrichtigungszustellung verwendet einen pluggable Provider, der je nach Quelltyp ausgewählt wird: PostgreSQL-Quellen verwenden `LISTEN/NOTIFY` (über asyncpg), MongoDB-Quellen verwenden Change Streams (`collection.watch()`), und Kafka-Quellen verwenden Consumer-Groups. Jeder Provider implementiert eine gemeinsame asynchrone Watch-Schnittstelle. RLS-Filterung und Schema-Validierung gelten unabhängig vom Provider. (REQ-258) WebSocket- und RSS-Quellen werden ebenfalls unterstützt. (REQ-338, REQ-342)

**Header — `X-Provisa-Sink`:** Auf ein Kafka-Ziel setzen (z. B. `kafka://broker:9092/topic`), um Änderungsereignisse an eine Kafka-Sink umzuleiten, statt an die SSE-Antwort. Der Server startet einen Sink-Consumer und gibt `202 Accepted` statt eines offenen Streams zurück. (REQ-812) [tool-verified: `provisa/api/data/subscription_sse.py:137`]

---

## Admin-REST-Endpunkte

### Config

#### `GET /admin/config`

Lädt die aktuelle `provisa.yaml` als `application/x-yaml` mit einem `Content-Disposition: attachment`-Header herunter. (REQ-164) [tool-verified: `provisa/api/admin/settings_router.py:19`]

#### `PUT /admin/config`

Lädt ein überarbeitetes Config-YAML hoch. Der Server schreibt ein `.bak`-Backup, speichert die neue Datei und lädt alle Schemas, Quellen und materialisierten Sichten neu. (REQ-164) [tool-verified: `provisa/api/admin/settings_router.py:32`]

**Request-Body:** Roher YAML-Inhalt.

**Antwort:**

```json
{"success": true, "message": "Config uploaded and reloaded"}
```

Bei Reload-Fehler: `{"success": false, "message": "<error>"}`.

#### `GET /admin/config/live`

Lädt die **aktuelle Live-Konfiguration** herunter — die Konfiguration, wie Provisa sie heute schreiben würde, unter Berücksichtigung jeder über den Admin erstellten Tabelle, Beziehung, Domäne, Rolle und RLS-Regel, die sich seit dem Start angesammelt hat. (REQ-164) [tool-verified: `provisa/api/admin/settings_router.py:67`]

Die Datei auf der Festplatte kann dem Live-Zustand hinterherhinken, wenn Änderungen über die Admin-API ohne anschließenden Upload vorgenommen wurden. Dieser Endpunkt schließt diese Lücke: Seine Ausgabe ist das, was `PUT /admin/config` empfangen müsste, damit die Datei auf der Festplatte dem Live-Zustand entspricht.

Gibt `application/x-yaml` mit `Content-Disposition: attachment; filename=provisa.live.yaml` zurück.

#### `GET /admin/config/diff`

Gibt beide Seiten des Config-Diffs zurück — `original` (die Startbaseline) und `current` (Live-Zustand) — identisch normalisiert, sodass der Vergleich nur echte Änderungen zeigt, keine Umordnung oder Kommentar-Drift. (REQ-164) [tool-verified: `provisa/api/admin/settings_router.py:82`]

**Antwort:**

```json
{"original": "<yaml>", "current": "<yaml>"}
```

#### `POST /admin/config/patch`

Erzeugt einen unified-diff-Patch von der Baseline zur eingesendeten Konfiguration. (REQ-164) [tool-verified: `provisa/api/admin/settings_router.py:93`]

Senden Sie das überarbeitete YAML als Request-Body. Die Antwort ist eine `text/x-patch`-Datei (`provisa.config.patch`), die `git apply` oder `patch` direkt verarbeiten kann — nützlich, um UI-getriebene Konfigurationsänderungen über eine CI/CD-Pipeline zu committen.

---

### Settings

#### `GET /admin/settings`

Gibt aktuelle Plattformeinstellungen als JSON zurück. (REQ-165) [tool-verified: `provisa/api/admin/settings_router.py:50`]

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

Aktualisiert Plattformeinstellungen zur Laufzeit. Alle Felder sind optional — nur im Body vorhandene Schlüssel werden aktualisiert. (REQ-165) [tool-verified: `provisa/api/admin/settings_router.py:100`]

**Request-Body (partielles Beispiel):**

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
- `naming`: `domain_prefix`, `convention` — schreibt in die Config-Datei und löst einen Schema-Reload aus (REQ-253)
- `relationships`: `auto_track_fk` — regelt ausschließlich die Fremdschlüssel-Verfolgung. Eine über eine Junction realisierte Beziehung wird bei der Tabellenregistrierung deklariert und nie inferiert, daher betrifft diese Einstellung sie nicht. (REQ-1586)
- `otel`: `endpoint`, `service_name`, `sample_rate`, `support_endpoint`, `support_redact_sql_literals`, `support_redact_attributes`

**Antwort:**

```json
{"success": true, "updated": ["otel.support_endpoint", "cache.default_ttl"]}
```

---

### KI-Modelle

#### `GET /admin/ai-models`

Gibt die KI-Modell-Zuweisungen, die Vektor-Modell-Registry und das NL-Ratenlimit der handelnden Org zurück. (REQ-464, REQ-1349) [tool-verified: `provisa/api/admin/ai_models_router.py:58`]

**Antwort:**

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

API-Schlüssel werden nie zurückgespiegelt — `api_keys_set` meldet nur, ob für jeden Anbieter ein Schlüssel konfiguriert ist. Änderungen wirken sich auf die nächste Anfrage aus; kein Neustart erforderlich. (REQ-1349)

#### `PUT /admin/ai-models`

Aktualisiert die KI-Modell-Zuweisungen, die Vektor-Modell-Registry oder das NL-Ratenlimit der Org. Wird bei der nächsten Anfrage wirksam. [tool-verified: `provisa/api/admin/ai_models_router.py:148`]

#### `GET /admin/ai-models/vendors/{vendor}/models`

Gibt die Modellnamen zurück, die ein Anbieter derzeit bereitstellt, für den Modell-Picker. (REQ-1395, REQ-1398, REQ-1409) [tool-verified: `provisa/api/admin/ai_models_router.py:89`]

Die Liste wird live aus der eigenen List-Models-API des Anbieters gelesen, unter Verwendung des konfigurierten Schlüssels der Org — oder der Deployment-Anmeldedaten, wenn kein Org-Schlüssel gesetzt ist. Ein Modell, das nach dem Ausliefern dieses Builds veröffentlicht wurde, ist am selben Tag auswählbar, an dem der Anbieter es bereitstellt.

Gibt `400` zurück, wenn der Anbieter keine List-Models-API veröffentlicht (in diesem Fall den Modellnamen direkt eingeben) oder wenn kein Schlüssel verfügbar ist. [tool-verified: `provisa/api/admin/ai_models_router.py:109-128`]

---

### Föderations-Engine

#### `GET /admin/federation-engine`

Gibt die aktuelle Auswahl der Föderations-Engine, ihre Verbindungskonfiguration und die vollständige auswählbare Engine-Registry zurück. (REQ-916) [tool-verified: `provisa/api/admin/settings_router.py:730`]

**Antwort:**

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

Der Schlüssel `current` ist die gerade laufende Engine; `persisted` ist das, was in die Config-Datei geschrieben wird und beim nächsten Neustart geladen wird. Sie weichen voneinander ab, wenn die Konfiguration geändert wurde, der Dienst aber noch nicht neu gestartet wurde.

#### `PUT /admin/federation-engine`

Persistiert eine Auswahl der Föderations-Engine. (REQ-916) [tool-verified: `provisa/api/admin/settings_router.py:774`]

**Request-Body:**

```json
{"engine": "trino", "federation_engine_url": "http://trino-coordinator:8080"}
```

Die Auswahl wird in die Plattformkonfiguration geschrieben. Sie wird nach dem nächsten Dienstneustart wirksam — die Engine wird nur einmal beim Boot gewählt.

---

### Domänenrichtlinie

#### `POST /admin/domain-policy`

Ändert die Domänenrichtlinie (`use_domains` / `default_domain`) der handelnden Org. (REQ-165, REQ-1266, REQ-1349) [tool-verified: `provisa/api/admin/settings_router.py:632`]

Dies ist eine destruktive, auf die handelnde Org beschränkte Operation. Jede registrierte Quelle, Tabelle, Domäne und Beziehung wird gelöscht und unter der neuen Richtlinie neu aufgebaut. Verwenden Sie dies, um eine Org von domänen-namensraumbasiert auf flach umzustellen (oder umgekehrt).

**Request-Body:**

```json
{
  "use_domains": true,
  "default_domain": "default"
}
```

`use_domains: null` löscht die Override der Org und fällt auf die Deployment-Ebene-Einstellung zurück. `use_domains: false` erfordert `default_domain` (den einen Domänennamen, in dem alle Tabellen landen). Der Katalog-Rebuild ist synchron; die Antwort kehrt zurück, sobald die Schemas bereit sind.

---

### Observability

#### `GET /admin/traces/recent`

Gibt bis zu N aktuelle abgeschlossene Spans aus dem In-Memory-Span-Puffer zurück. (REQ-302) [tool-verified: `provisa/api/admin/settings_router.py:317`]

**Query-Parameter:** `limit` (Standard 50, max. 200)

**Antwort:** `{"traces": [...]}`

#### `POST /admin/query-engine/reload-catalog`

Lädt einen benannten Katalog in der Koordinator-Instanz der Föderations-Engine über deren REST-API im laufenden Betrieb neu. Verbindet Provisas interne Verbindung neu und führt das OTel-DDL erneut aus. [tool-verified: `provisa/api/admin/settings_router.py:208`]

**Query-Parameter:** `catalog` (Standard `"otel"`)

**Antwort:**

```json
{"success": true, "errors": []}
```

#### `POST /admin/query-engine/restart`

Startet den Föderations-Engine-Container neu (nur Single-Node-Dev). [tool-verified: `provisa/api/admin/settings_router.py:287`]

**Query-Parameter:** `container` (Standard: die Umgebungsvariable `QUERY_ENGINE_CONTAINER`, dann `"trino"`)

---

### Discovery

#### `POST /admin/discover/relationships`

Löst die Beziehungs-Discovery aus. Führt immer die FK-Introspektion aus der Föderations-Engine durch. (REQ-018) Führt LLM-Inferenz aus, wenn `ANTHROPIC_API_KEY` gesetzt ist. (REQ-167) [tool-verified: `provisa/api/admin/discovery.py:55`]

**Request-Body:**

```json
{
  "scope": "domain",
  "domain_id": "sales"
}
```

`scope` muss `"table"`, `"domain"` oder `"cross-domain"` sein. Für den Scope `"table"` ist `table_id` (integer) erforderlich. Für den Scope `"domain"` ist `domain_id` erforderlich.

**Antwort:** `{"candidates_found": 12, "stored_ids": [1, 2, 3, ...]}`

#### `GET /admin/discover/candidates`

Listet ausstehende Beziehungskandidaten auf. [tool-verified: `provisa/api/admin/discovery.py:96`]

#### `POST /admin/discover/candidates/{candidate_id}/accept`

Akzeptiert einen Kandidaten und registriert ihn als Beziehung. [tool-verified: `provisa/api/admin/discovery.py:103`]

**Request-Body (optional):** `{"name": "custom-relationship-name"}`

#### `POST /admin/discover/candidates/{candidate_id}/reject`

Weist einen Kandidaten zurück. [tool-verified: `provisa/api/admin/discovery.py:110`]

**Request-Body:** `{"reason": "Not a real join"}`

#### `GET /admin/discover/candidates/rejected/count`

Gibt die Anzahl zurückgewiesener Kandidaten zurück. [tool-verified: `provisa/api/admin/discovery.py:118`]

#### `DELETE /admin/discover/candidates/rejected`

Löscht alle zurückgewiesenen Kandidaten. [tool-verified: `provisa/api/admin/discovery.py:128`]

---

### Quellen-Crawl

#### `POST /admin/sources/crawl`

Crawlt eine Datenquelle, um ihr Schema zu introspizieren und Tabellen zu registrieren. (REQ-012) [tool-verified: `provisa/api/admin/crawl_router.py:36`]

---

### Quellen-Tabellensuche

#### `GET /admin/sources/{source_id}/tables/search`

Sucht nach Namen in verfügbaren (noch nicht registrierten) Tabellen einer Quelle. [tool-verified: `provisa/api/admin/table_search_router.py:103`]

---

### Tabellenprofiling

#### `POST /admin/tables/{table_id}/profile`

Führt ein Spaltenprofil für eine registrierte Tabelle aus — Kardinalität, Min/Max, Null-Raten. [tool-verified: `provisa/api/admin/table_profile_router.py:28`]

---

### Quellbeschreibungen

#### `POST /admin/source-meta/db-description`

Erzeugt LLM-unterstützte Beschreibungen für die Tabellen und Spalten einer Quelle. [tool-verified: `provisa/api/admin/source_meta_router.py:48`]

---

### Objektspeicher (REQ-1046, REQ-1048, REQ-1049)

#### `GET /admin/org-storage`

Meldet den Speicherfußabdruck der handelnden Org gegenüber ihrer Plattform-Zuteilung und ob die Org einen eigenen Store registriert hat. [tool-verified: `provisa/api/admin/org_storage_router.py:69`]

Wenn die Org ihren eigenen DSN registriert hat, gehen ihre Materialisierungen dorthin und werden nicht mehr gegen die Zuteilung angerechnet. Der DSN selbst wird nie zurückgegeben.

#### `PUT /admin/org-storage`

Registriert (oder löscht) den eigenen Materialisierungsspeicher der Org. [tool-verified: `provisa/api/admin/org_storage_router.py:81`]

**Request-Body:**

```json
{"storage_url": "s3://my-bucket/provisa?region=us-east-1&access_key=..."}
```

Der DSN wird vor der Annahme gegen die Föderations-Engine validiert — ein unbrauchbarer DSN schlägt bei der Registrierung fehl, nicht erst Stunden später bei einem Refresh. Der Wert wird ruhend verschlüsselt und nie von GET zurückgegeben.

Senden Sie `storage_url: null`, um den eigenen Store der Org zu löschen und ihre Materialisierungen zum Plattform-Store (und der Zuteilung) zurückzugeben. Die Laufzeitumgebung der Org wird im selben Aufruf neu aufgebaut, sodass der neue Store sofort wirksam ist. [tool-verified: `provisa/api/admin/org_storage_router.py:123-138`]

---

### Org-Verschlüsselung (REQ-1574)

#### `GET /admin/org-encryption`

Gibt den aktuellen Schlüsselstatus der Org zurück: Fingerprint, ID und Herkunft. Gibt nie das Schlüsselmaterial zurück. [tool-verified: `provisa/api/admin/org_encryption_router.py:53`]

Wenn die Org keinen Schlüssel gesetzt hat, wird `{"configured": false}` zurückgegeben. Jede Org startet in diesem Zustand und erbt den Deployment-Schlüssel.

#### `PUT /admin/org-encryption`

Setzt oder rotiert den At-Rest-Verschlüsselungsschlüssel der Org. [tool-verified: `provisa/api/admin/org_encryption_router.py:68`]

**Request-Body:**

```json
{"key_b64": "<32 raw bytes, base64-encoded>"}
```

`key_b64` weglassen, damit Provisa einen Schlüssel generiert — der sicherste Weg, da der Schlüssel nie in einer Zwischenablage oder einem Request-Log erscheint. Die Angabe von `key_b64` bringt einen eigenen Schlüssel mit.

Die Rotation fügt dem Key Ring einen neuen aktiven Eintrag hinzu und behält den alten bei, sodass unter dem vorherigen Schlüssel geschriebene Daten weiterhin lesbar bleiben. Rotation ist keine Neuverschlüsselung. Es gibt keinen Delete-Endpunkt: Das Entfernen des letzten Schlüssels würde jede gewrappte Payload unlesbar machen. [tool-verified: `provisa/api/admin/org_encryption_router.py:75`]

Der Live-Ring wird im selben Aufruf neu gebunden, sodass der nächste verschlüsselte Schreibvorgang sofort den neuen Schlüssel verwendet.

---

### Hasura-/DDN-Import (REQ-1483)

#### `POST /admin/import/hasura/preview`

Wandelt ein Hasura-v2- oder DDN-Projektarchiv in eine vorgeschlagene Provisa-Konfiguration um, ohne etwas zu schreiben. [tool-verified: `provisa/api/admin/import_router.py`]

**Request-Body:**

```json
{
  "filename": "my-project.zip",
  "content_b64": "<base64-encoded archive>",
  "flavor": "auto",
  "domain_map": {"public": "sales"},
  "source_overrides": {}
}
```

`flavor` ist `"auto"` (aus der Archivstruktur erkannt), `"hasura_v2"` oder `"ddn"`.

**Antwort:**

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

Nichts wird persistiert. Die Vorschau wird serverseitig nicht zwischengespeichert; `apply` übernimmt das von Ihnen bereitgestellte YAML, sodass genau das angewendet wird, was geprüft (und optional bearbeitet) wurde.

#### `POST /admin/import/hasura/apply`

Lädt eine zuvor vorgeschaute Konfiguration in die handelnde Org. [tool-verified: `provisa/api/admin/import_router.py`]

**Request-Body:**

```json
{"config_yaml": "<yaml string>"}
```

Verwendet denselben Hot-Reload-Pfad wie `PUT /admin/config`. Der Katalog, die Schemas und die Pools der Org werden neu aufgebaut, bevor die Antwort zurückgegeben wird.

---

### Apache-Ossie-Interchange (REQ-1316, REQ-1321)

#### `GET /admin/ossie`

Exportiert das governte Modell der Org als Apache-Ossie-(incubating)-YAML-Dokument. (REQ-1321) [tool-verified: `provisa/api/admin/ossie_router.py`]

Das Dokument wird bei jeder Anfrage aus dem Live-Zustand abgeleitet — nie zwischengespeichert — sodass es nie veraltet sein kann. Tabellen werden zu `dataset`-Objekten, Spalten werden zu `field`-Objekten, und Beziehungen werden auf Ossie-`relationship`-Objekte abgebildet.

Gibt `text/yaml` mit `Content-Disposition: attachment; filename=provisa-ossie.yaml` zurück.

#### `POST /admin/ossie/import`

Parst ein Ossie-YAML- oder -JSON-Dokument und gibt vorgeschlagene Tabellen- und Beziehungsregistrierungen zurück. (REQ-1316) [tool-verified: `provisa/api/admin/ossie_router.py`]

**Request-Body:** Rohes Ossie-YAML oder -JSON. Das Format wird automatisch erkannt.

**Antwort:**

```json
{
  "proposals": {
    "tables": [...],
    "relationships": [...]
  }
}
```

Es wird nichts registriert. Verwenden Sie den Prüfbildschirm der Admin-UI, um Vorschläge zu akzeptieren oder zu beschneiden, bevor eine Mutation ausgelöst wird.

---

### Actions (Funktionen und Webhooks)

Alle Endpunkte liegen unter dem Präfix `/admin/actions`. (REQ-205) [tool-verified: `provisa/api/admin/actions_router.py:24`]

Jeder Aufruf — von GraphQL, SQL, Cypher, Bolt, Arrow Flight, MCP `run_sql` und Provisa gRPC — läuft durch einen einzigen governten Executor, der `writable_by` und Governance einheitlich durchsetzt. (REQ-1156) [tool-verified: `provisa/api/data/action_exec.py`] Siehe [docs/integrations.md](integrations.md#commands-uber-protokolle-hinweg-aufrufen) für die protokollspezifische Aufrufsyntax.

#### `GET /admin/actions`

Gibt alle nachverfolgten DB-Funktionen und Webhooks zurück. (REQ-242) [tool-verified: `provisa/api/admin/actions_router.py:104`]

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

Jedes Webhook-Objekt trägt einen booleschen Wert `approved`. Ein Webhook wird genehmigt, sobald ein Steward seine Erstellungsanfrage ausführt (REQ-209); Config-deklarierte Webhooks sind automatisch genehmigt. Ein nicht genehmigter Webhook ist registriert, aber auf keiner Oberfläche exponiert. [tool-verified: `provisa/api/admin/actions_router.py:124-131`]

#### `POST /admin/actions/functions`

Registriert eine nachverfolgte Funktion (Command). (REQ-205) [tool-verified: `provisa/api/admin/actions_router.py:117`]

**Schlüsselfelder:**

| Feld | Erforderlich | Beschreibung |
| --- | --- | --- |
| `name` | Ja | Eindeutiger Command-Name |
| `kind` | Ja | `"query"` → GraphQL-Query-Feld; `"mutation"` → Mutation-Feld |
| `implKind` | Nein | Wie der Command läuft — siehe Tabelle unten (Standard `source_procedure`) |
| `binding` | Nein | `implKind`-spezifische Verbindungsdetails (JSON-Objekt) |
| `returnSchema` | Nein | JSON Schema `{type:"array", items:{type:"object", properties:{...}}}` — macht den Command auf jeder Oberfläche set-returning |
| `arguments` | Nein | `[{name, type}]`-Argumentdefinitionen; die positionale Reihenfolge zählt für SQL- und Bolt-Aufrufer |
| `visibleTo` | Nein | Rollen-IDs, die den Command aufrufen können |
| `writableBy` | Nein | Rollen-IDs, die berechtigt sind, ihn als Mutation aufzurufen |
| `domainId` | Nein | Domäne für GraphQL-Platzierung und Zugriffskontrolle |

**`implKind`-Werte:**

| `implKind` | Was ausgeführt wird | `binding`-Felder |
| --- | --- | --- |
| `source_procedure` | Stored Procedure auf einer registrierten Quelle (Standard) | `sourceId`, `schemaName`, `functionName` |
| `script` | Serverseitiges Skript | `script` |
| `http` | Ausgehender HTTP-Aufruf | `url`, `method` |
| `grpc` | Ausgehender gRPC-Aufruf an einen externen Server | `target`, `method` |
| `python` | Von Provisa gehosteter Python-Callable (REQ-885) | `callable` (z. B. `"demo.py_functions:random_dataset"`) |

Die Demo-Commands `random_python_set` (`implKind: python`) und `random_grpc_set` (`implKind: grpc`) zeigen set-returning Commands mit `returnSchema` in der Praxis; beide befinden sich in `config/provisa-install.yaml`. [tool-verified: `config/provisa-install.yaml:809-856`]

#### `PUT /admin/actions/functions/{name}`

Aktualisiert eine nachverfolgte Funktion nach Namen. [tool-verified: `provisa/api/admin/actions_router.py:182`]

#### `DELETE /admin/actions/functions/{name}`

Löscht eine nachverfolgte Funktion nach Namen. [tool-verified: `provisa/api/admin/actions_router.py:233`]

#### `POST /admin/actions/webhooks`

Registriert einen nachverfolgten Webhook. (REQ-209) Das Registrieren oder Aktualisieren eines Webhooks stellt eine Steward-Genehmigungsanfrage in die Warteschlange — der Webhook wird auf allen Oberflächen erst aktiv, nachdem ihn ein Steward genehmigt hat. Config-deklarierte Webhooks sind automatisch genehmigt. **Request-Body-Felder:** `name`, `url`, `method`, `timeoutMs`, `returns`, `inlineReturnType`, `arguments`, `visibleTo`, `domainId`, `description`, `kind`. [tool-verified: `provisa/api/admin/actions_router.py:132`, `provisa/api/admin/actions_router.py:325-331`]

#### `PUT /admin/actions/webhooks/{name}`

Aktualisiert einen nachverfolgten Webhook nach Namen. Jede Bearbeitung setzt die Genehmigung zurück auf ausstehend, bis erneut genehmigt wird. [tool-verified: `provisa/api/admin/actions_router.py:306`]

#### `DELETE /admin/actions/webhooks/{name}`

Löscht einen nachverfolgten Webhook nach Namen. [tool-verified: `provisa/api/admin/actions_router.py:355`]

#### `POST /admin/actions/test`

Testet eine Action (Funktion oder Webhook) nach Namen. (REQ-245) [tool-verified: `provisa/api/admin/actions_router.py:384`]

---

### Rollen

Alle Endpunkte liegen unter dem Präfix `/admin/roles`. [tool-verified: `provisa/api/admin/roles_router.py:18`]

| Methode | Pfad | Beschreibung |
| --- | --- | --- |
| `GET` | `/admin/roles/` | Alle Rollen auflisten |
| `POST` | `/admin/roles/` | Eine Rolle erstellen |
| `PUT` | `/admin/roles/{role_id}` | Eine Rolle aktualisieren |
| `DELETE` | `/admin/roles/{role_id}` | Eine Rolle löschen |

[tool-verified: `provisa/api/admin/roles_router.py`]

---

### Benutzer

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

### Organisationen

Alle Endpunkte liegen unter `/admin/orgs`. [tool-verified: `provisa/api/admin/orgs_router.py:18`]

| Methode | Pfad | Beschreibung |
| --- | --- | --- |
| `GET` | `/admin/orgs/` | Orgs auflisten |
| `POST` | `/admin/orgs/` | Eine Org erstellen |
| `PUT` | `/admin/orgs/{org_id}` | Eine Org aktualisieren |
| `DELETE` | `/admin/orgs/{org_id}` | Eine Org löschen |
| `GET` | `/admin/orgs/{org_id}/members` | Mitglieder auflisten |
| `POST` | `/admin/orgs/{org_id}/members` | Ein Mitglied hinzufügen |
| `DELETE` | `/admin/orgs/{org_id}/members/{user_id}` | Ein Mitglied entfernen |

---

### Einladungen

Alle Endpunkte liegen unter `/admin/invites`. [tool-verified: `provisa/api/admin/invites_router.py:18`]

| Methode | Pfad | Beschreibung |
| --- | --- | --- |
| `POST` | `/admin/invites/` | Eine Einladung erstellen |
| `GET` | `/admin/invites/` | Ausstehende Einladungen auflisten |
| `DELETE` | `/admin/invites/{token}` | Eine Einladung widerrufen |

---

### Admin-GraphQL

#### `POST /admin/graphql`

Strawberry-GraphQL-Endpunkt für alle Admin-Operationen: Quellen- und Tabellen-CRUD, Beziehungsverwaltung, Domänenkonfiguration, RLS-Regeln, Cache-Steuerung, Namenskonventionen, Verwaltung geplanter Tasks und Abfragekompilierung. (REQ-164) [tool-verified: `provisa/api/app.py:2171`]

**Schlüssel-Mutationen:**

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

Gibt den Erststart-Setup-Status zurück. Immer nicht authentifiziert. (REQ-539) [tool-verified: `provisa/api/setup_router.py:100`]

#### `POST /setup/`

Schließt das Erststart-Setup ab. [tool-verified: `provisa/api/setup_router.py:142`]

---

## Health Check

#### `GET /health` oder `HEAD /health`

Gibt `{"status": "ok"}` zurück. Immer nicht authentifiziert. (REQ-539) [tool-verified: `provisa/api/app.py:2258`]

---

## Fehlerantworten

| Status | Bedeutung |
| --- | --- |
| 400 | Ungültige Abfrage, Validierungsfehler oder SQL-Parse-Fehler |
| 401 | Fehlendes oder ungültiges Auth-Token |
| 403 | Unzureichende Capabilities; Governance-Verstoß |
| 404 | Rolle, Ressource oder Config-Datei nicht gefunden |
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

Abfragen und Katalog-Discovery sind beide auf derselben Verbindung verfügbar. Die vollständige Governance-Pipeline (RLS, Maskierung, Sampling) wird auf jede Abfrage angewendet. (REQ-130, REQ-143)

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

Wenn der Zaychik-Flight-SQL-Proxy verfügbar ist (Port 8480), streamen Record-Batches End-to-End ohne vollständige Materialisierung. (REQ-144) Fällt auf Materialisierung über die föderierte Abfrageschicht zurück, wenn Zaychik nicht verfügbar ist. (REQ-146)

---

## Protobuf-gRPC-Endpunkt

Port `50051` (mit der Umgebungsvariable `GRPC_PORT` oder der Config `server.grpc_port` überschreibbar). (REQ-529) [tool-verified: `provisa/grpc/server.py`, `provisa/api/app.py`]

Übergeben Sie die Rolle im gRPC-Metadatenschlüssel `x-provisa-role`. Fehlt dieser, bricht der Server mit `UNAUTHENTICATED` ab. [tool-verified: `provisa/grpc/server.py`]

Laden Sie das rollenspezifische Proto von `GET /data/proto/{role_id}` herunter. Nur für diese Rolle sichtbare Tabellen und Spalten erscheinen. (REQ-039)

```proto
service ProvisaService {
  rpc QueryOrders (QueryOrdersRequest) returns (stream Orders);
  rpc InsertOrders (InsertOrdersRequest) returns (InsertOrdersResponse);
}
```

Jede Tabelle erzeugt eine streamende RPC `Query{TypeName}`. `Insert{TypeName}`-RPCs existieren aus Gründen der Schema-Symmetrie, brechen aber mit `UNIMPLEMENTED` ab. [tool-verified: `provisa/grpc/server.py`]

`grpc_reflection.v1alpha` ist für die Service-Discovery ohne vorkompiliertes Proto aktiviert. (REQ-529) [tool-verified: `provisa/grpc/reflection.py`]

```bash
grpcurl -plaintext localhost:50051 list
grpcurl -plaintext -H 'x-provisa-role: analyst' \
  -d '{}' localhost:50051 ProvisaService/QueryOrders
```

Der gRPC-Server startet nur, wenn beim Start ein gültiges Proto kompiliert werden kann. Schlägt der Schema-Build fehl, startet der gRPC-Server nicht. (REQ-529)

#### Aggregat- und Group-By-RPCs (REQ-1359, REQ-1361, REQ-1405)

Wenn für eine Tabelle `enable_aggregates` gesetzt ist, enthält das generierte Proto zwei zusätzliche RPCs neben `Query{TypeName}`:

- **`Query{TypeName}Aggregate`** — gibt Aggregat-Skalare für die Tabelle zurück (`count`; `sum`, `avg`, `stddev`, `variance` pro numerischer Spalte; `min`, `max` pro vergleichbarer Spalte)
- **`Query{TypeName}GroupBy`** — gibt eine Zeile pro Gruppenschlüssel mit Aggregat-Unterfeldern und optional Skalaren der Basistabelle sowie verbundenen Dimensionszeilen in einem `nodes`-Feld zurück

Beide laufen durch dieselbe Compiler-Aggregat-Pipeline wie die GraphQL-Root-Felder `{field}_aggregate` und `{field}_group_by` — keine separate Aggregat-Implementierung. (REQ-1359) [tool-verified: `provisa/grpc/query_ir.py:133-196`]

**Feld `funcs` (REQ-1361).** Die Request-Message akzeptiert ein repeated-string-Feld `funcs`. Gültige Werte sind `count`, `sum`, `avg`, `stddev`, `variance`, `min` und `max`. Wenn `funcs` weggelassen wird, wird jede Funktion angefordert, die das Schema für diese Tabelle bereitstellt. Wenn gesetzt, erscheinen nur die genannten Funktionen. Wenn keine der genannten Funktionen auf die Spaltentypen der Tabelle zutrifft, fällt die Abfrage auf `count` zurück. [tool-verified: `provisa/grpc/query_ir.py:66`, `provisa/grpc/query_ir.py:75-97`]

**Felder `include_nodes` und `include` (REQ-1405).** `Query{TypeName}GroupBy`-Anfragen können `include_nodes: true` setzen, um skalare Spalten der Basistabelle im `nodes`-Feld jeder Zeile einzuschließen. Das repeated-string-Feld `include` benennt Many-to-One-Beziehungsfelder, deren skalare Spalten ebenfalls in `nodes` verschachtelt werden. Dies entspricht dem Verhalten von `?includeNodes=` / `?include=` bei JSON:API. [tool-verified: `provisa/grpc/query_ir.py:168-195`]

---

## JDBC-Treiber

Der Provisa-JDBC-Treiber (`provisa-jdbc-0.1.0.jar`) stellt den semantischen Katalog für BI-Tools (Tableau, PowerBI, DBeaver) bereit. (REQ-126)

**Connection-URL:** `jdbc:provisa://host:port` (REQ-131)

Domänen werden auf JDBC-Schemas abgebildet. (REQ-127) Tabellen verwenden ihre registrierten Aliase. Spalten verwenden Aliase und stellen Beschreibungen als `REMARKS` bereit. (REQ-128) Standard-Metadatenmethoden (`getPrimaryKeys`, `getImportedKeys`, `getExportedKeys`) exponieren semantische Beziehungen als PK-/FK-Metadaten.

**SQL-Unterstützung:** `SELECT * FROM <alias> [WHERE col = 'value']`. (REQ-129)

Der Treiber fordert standardmäßig einen Arrow-IPC-Redirect an. Ergebnisse werden batchweise über `ArrowStreamReader` gestreamt, begrenzt auf einen Record-Batch im Speicher. (REQ-293)

---

## Argumentformat `orderBy`

Das Argument `order_by` verwendet `{column: direction}`-Objekte mit einem 6-wertigen Richtungs-Enum: (REQ-200)

```json
{
  "query": "{ orders(order_by: [{created_at: desc_nulls_last}]) { id created_at } }",
  "role": "admin"
}
```

Unterstützte Richtungen: `asc`, `desc`, `asc_nulls_first`, `asc_nulls_last`, `desc_nulls_first`, `desc_nulls_last`. (REQ-201)

---

## Subscriptions

SSE-Subscriptions sind unter `GET /data/subscribe/{table}` verfügbar. (REQ-219, REQ-258) Die Benachrichtigungszustellung verwendet einen pro Quelltyp gewählten pluggable Provider: PostgreSQL-Quellen verwenden `LISTEN/NOTIFY`, MongoDB-Quellen verwenden Change Streams, und Kafka-Quellen verwenden Consumer-Groups. RLS-Filterung und Schema-Validierung gelten unabhängig vom Provider. WebSocket- und RSS-Quellen werden über denselben Endpunkt ebenfalls unterstützt. (REQ-338, REQ-342) [tool-verified: `provisa/api/data/subscribe.py:239`, `provisa/subscriptions/registry.py`, `provisa/api/app.py` `_rebuild_schemas`]

---

## Business-Glossar (REQ-1387)

Das Business-Glossar bildet physische Feldnamen — wie sie in Quelldatenbanken existieren — auf ein gemeinsames menschliches Vokabular ab. Jede in der semantischen Schicht registrierte Spalte erhält automatisch einen Begriff. Es ist keine manuelle Eingabe erforderlich, um das Glossar zu befüllen; Kuratoren fügen Definitionen, Beziehungen und Experten auf dem hinzu, was das System ableitet.

### Wie Begriffe abgeleitet werden

Wenn Provisa die Spalten einer Tabelle registriert oder aktualisiert, läuft `normalize_term` (`provisa/core/glossary.py`) für jeden Spaltennamen und erzeugt eine kanonische Formulierung. [tool-verified: `provisa/core/repositories/glossary.py:sync_table_refs`]

Die Normalisierung wendet fünf Regeln in Reihenfolge an:

1. Aufteilen an camelCase-Grenzen und Trennzeichen (`_`, `-`, `.`, `/`, Leerzeichen).
2. Das Ergebnis in Kleinbuchstaben umwandeln.
3. Eine feste Abkürzungstabelle expandieren (z. B. `cust` → `customer`, `amt` → `amount`, `dt` → `date`, `id` → `identifier`, `key` → `identifier`, `guid` → `identifier`).
4. Ein nachgestelltes **Proxy-Token** entfernen (`identifier`, `code`, `index` oder `reference`) — eine Spalte, die nach ihrem Schlüssel oder Code benannt ist, verweist über einen Platzhalterwert auf das zugrunde liegende Konzept, daher sollte der Begriff das Konzept selbst sein. Das letzte verbleibende Token wird nie entfernt.
5. Eine **zu generische Formulierung** mit dem Konzept der Tabelle qualifizieren. Wenn die vollständig normalisierte Formulierung ein bloßes Attributwort ist (`name`, `identifier`, `date`, `location`, `message`, `first name`, `last name` und Ähnliches), wird der Begriff zu `<Tabellenkonzept> <Formulierung>` — `employees.first_name` → `employee first name`, `orders.id` → `order identifier`. Ein gemeinsamer Begriff `name` über nicht verwandte Tabellen hinweg würde unterschiedliche Bedeutungen zusammenführen; die Qualifizierung verbindet stattdessen jede Spalte mit ihrem umschließenden Konzept. Das Tabellenkonzept ist der Geschäftsname der Tabelle, normalisiert mit einem singularen Kopfnomen (`order_lines` → `order line`).

Native-Filter-Pseudospalten (mit `_nf_`-Präfix, oder jede Spalte, die `native_filter_type` trägt) sind Abfrageparameter-Mechanik, keine Geschäftsfelder, und leiten keine Begriffe ab.

Da `id`, `key`, `pk` und `sk` alle vor der Proxy-Prüfung zu `identifier` expandieren, landen drei physisch unterschiedliche Spaltennamen auf exakt demselben Begriff:

| Physischer Name | Nach Normalisierung |
| --- | --- |
| `cust_id` | `customer` |
| `customerId` | `customer` |
| `CUSTOMER_KEY` | `customer` |
| `txn_amt` | `transaction amount` |

Die ersten drei fallen zu einem Begriff zusammen. `transaction amount` behält beide Tokens, da `amount` kein Proxy ist. Eine bloße `id`-Spalte — ohne vorangehende Tokens — kann nicht entfernt werden; sie normalisiert zu `identifier`, damit der Begriff nicht leer ist. [tool-verified: `provisa/core/glossary.py:normalize_term`]

### Lebenszyklus

Begriffe werden **aus der Mitgliedschaft in der semantischen Schicht abgeleitet**, nicht auf Anforderung von Benutzern erstellt. Das Tabellen-Repository ist der einzige Schreibpfad: `sync_table_refs` läuft bei jedem Spaltensatz-Upsert, und `sweep_refless_terms` läuft nach jedem Löschpfad. [tool-verified: `provisa/core/repositories/glossary.py`]

**Wenn eine Spalte hinzugefügt wird:** Provisa sucht den normalisierten Begriff nach Name. Existiert er bereits, erhält die Spalte eine Referenz darauf (und wurde der Begriff als veraltet markiert, wird er wiederbelebt — `deprecated` wird zurück auf `False` gesetzt). Existiert noch kein Begriff, wird einer erstellt.

**Wenn eine Spalte entfällt** (Schemaänderung oder Tabellenentfernung): Ihre Referenz wird gelöscht, und der Begriff wird nach einer Regel "Entfernen oder als veraltet markieren" **abgeschlossen**. Ein verwurzelter Begriff ohne verbleibende Referenzen wird vollständig entfernt — zusammen mit seinen Kanten und Expertenzuweisungen — es sei denn, das Entfernen würde einen abstrakten Begriff von allen verwurzelten Begriffen trennen (kein Pfad durch den Begriffsgraphen). In diesem Fall wird der Begriff **als veraltet markiert** (`deprecated=True`), statt gelöscht zu werden, damit der Graph-Anker des abstrakten Begriffs erhalten bleibt.

Abstrakte Begriffe werden nie automatisch entfernt; sie existieren außerhalb des physischen Lebenszyklus und werden nur explizit über die Admin-API gelöscht.

**Wiederbelebung:** Erscheint der normalisierte Name eines als veraltet markierten Begriffs erneut (eine Spalte wird neu registriert), wird die Markierung entfernt, und seine Referenzen sammeln sich wieder an.

### Kuratierungs-Endpunkte

Alle Endpunkte liegen unter `/admin/glossary`. Sie erfordern `org_admin`-Zugriff und eine konfigurierte Org. Jede Mutation löst eine Metadaten-Veröffentlichung aus. [tool-verified: `provisa/api/admin/glossary_router.py`]

| Methode | Pfad | Beschreibung |
| --- | --- | --- |
| `GET` | `/admin/glossary/terms` | Begriffe auflisten. Query-Parameter: `q` (Name-/Definitionssuche), `include_deprecated` (Standard `true`) |
| `GET` | `/admin/glossary/terms/{term_id}` | Begriffsdetail abrufen: Definition, physische Referenzen, typisierte Kanten, Experten |
| `POST` | `/admin/glossary/terms` | Einen abstrakten Begriff erstellen — Benutzervokabular ohne physische Referenzen |
| `PATCH` | `/admin/glossary/terms/{term_id}` | Umbenennen, Definition setzen oder Exportausschluss umschalten |
| `DELETE` | `/admin/glossary/terms/{term_id}` | Einen Begriff ohne physische Referenzen löschen |
| `POST` | `/admin/glossary/refs/move` | Eine physische Referenz zu einem anderen Begriff verschieben (Konsolidierung) |
| `POST` | `/admin/glossary/terms/{term_id}/edges` | Eine typisierte Beziehungskante zwischen zwei Begriffen hinzufügen |
| `DELETE` | `/admin/glossary/terms/{term_id}/edges` | Eine Kante entfernen (Query-Parameter: `to_term_id`, `rel_type`) |
| `POST` | `/admin/glossary/terms/{term_id}/experts` | Einen Benutzer als Experte oder Autor für einen Begriff kennzeichnen |
| `DELETE` | `/admin/glossary/terms/{term_id}/experts/{user_id}` | Die Experten-/Autoren-Kennzeichnung eines Benutzers entfernen |
| `POST` | `/admin/glossary/terms/{term_id}/definition/generate` | Einen Definitionsentwurf für einen Begriff mit dem KI-Modell der Org erstellen — gibt nur Text zurück, nichts wird persistiert, bis gespeichert wird |
| `POST` | `/admin/glossary/definitions/generate` | Definitionen für jeden Begriff ohne eine solche generieren und persistieren — überschreibt nie von Menschen verfassten Text |
| `POST` | `/admin/glossary/relationships/generate` | Typisierte Kanten über das gesamte Glossar hinweg mit dem KI-Modell der Org vorschlagen und persistieren |

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

Das Verschieben einer Referenz schließt den verlierenden Begriff nach der Regel "Entfernen oder als veraltet markieren" ab. Verwenden Sie dies, um zwei Begriffe zu konsolidieren, die die Normalisierung getrennt gehalten hat — zum Beispiel, nachdem eine Quelle eine nicht standardmäßige Abkürzung verwendet, die außerhalb der Expansionstabelle lag.

Das Löschen eines verwurzelten Begriffs (mit physischen Referenzen) gibt `400 glossary.invalid` zurück. Entfernen oder verschieben Sie zuerst alle Referenzen.

**`PATCH /admin/glossary/terms/{term_id}` — Feld `export_excluded`:**

```json
{"export_excluded": true}
```

Das Setzen von `export_excluded` auf `true` hält den Begriff von allen Metadaten-Export-Snapshots zurück, unabhängig von seinen physischen Referenzen oder seinem abstrakten Status. Das Zurücksetzen auf `false` stellt den Begriff bei der nächsten Veröffentlichung wieder im Snapshot her. Kuratierungsdaten (Definition, Kanten, Experten) sind davon unberührt. [tool-verified: `provisa/core/repositories/glossary.py:set_export_excluded`, `provisa/api/admin/glossary_router.py:update_term`]

### KI-unterstützte Kuratierung

Das konfigurierte KI-Modell der Org kann Definitionen entwerfen und in einer einzigen Operation Beziehungskanten über das gesamte Glossar hinweg vorschlagen. Beide Batch-Aktionen erfordern `org_admin`-Zugriff und eine konfigurierte Org.

**`POST /admin/glossary/definitions/generate`**

Iteriert über jeden Begriff im Glossar, überspringt jeden, der bereits eine Definition hat, und ruft das KI-Modell der Org auf, um für jeden verbleibenden Begriff einen Entwurf zu erstellen. Der Entwurf wird sofort persistiert — anders als beim Pro-Begriff-Entwurfs-Endpunkt (`POST /admin/glossary/terms/{term_id}/definition/generate`) gibt es keinen Editor-Schritt. Von Menschen verfasste Definitionen werden nie überschrieben: Die Schutzklausel ist `if summary["definition"]: continue` vor jedem Modellaufruf. Eine einzige Veröffentlichungsbenachrichtigung deckt den gesamten Batch ab. [tool-verified: `provisa/api/admin/glossary_router.py:generate_all_definitions`]

Antwort:

```json
{"generated": 12}
```

`generated` ist die Anzahl der Begriffe, die eine neue Definition erhalten haben. Sie ist null, wenn jeder Begriff bereits eine hat.

**`POST /admin/glossary/relationships/generate`**

Sendet die vollständige Begriffsliste an das KI-Modell der Org mit einem Prompt, der die zehn erlaubten Kantentypen (`KIND_OF`, `PART_OF`, `SYNONYM_OF`, `RELATED_TO`, `VALID_VALUE_OF`, `DERIVED_FROM`, `REPLACES`, `PREFERRED_TERM_FOR`, `TRANSLATION_OF`, `ANTONYM_OF`) spezifiziert und nur um sichere Vorschläge bittet. Das Modell antwortet mit einem JSON-Array; jeder Eintrag wird vor jedem Schreibvorgang validiert: unbekannte Begriffsnamen, Selbstkanten und Kantentypen außerhalb des geschlossenen Enums werden stillschweigend verworfen. Gültige Vorschläge werden idempotent upserted — ein erneuter Aufruf der Aktion dupliziert keine Kanten. Eine einzige Veröffentlichungsbenachrichtigung deckt den Batch ab. Der Endpunkt gibt sofort `{"added": 0}` zurück, wenn das Glossar weniger als zwei nicht veraltete Begriffe enthält. [tool-verified: `provisa/api/admin/glossary_router.py:generate_relationships`]

Antwort:

```json
{"added": 5}
```

`added` ist die Anzahl der geschriebenen Kanten. Eine bereits existierende Kante zählt weiterhin mit — der Upsert gelingt, aber die Kantendaten ändern sich nicht.

### MCP-Tool `search_terms`

```
search_terms(query, role=None, limit=25)
```

Durchsucht Begriffsnamen und Definitionen mit einer Groß-/Kleinschreibung ignorierenden Teilstring-Suche, bis zu `limit` Ergebnisse. Jedes Ergebnis ist das vollständige Begriffsdetail: `name`, `definition`, `is_abstract`, `deprecated`, physische Referenzen (mit `source_id`, `schema_name`, `table_name`, `column_name`), typisierte Kanten und Expertenzuweisungen. [tool-verified: `provisa/api/mcp/server.py:236-244`, `provisa/core/repositories/glossary.py:search_terms`]

Verwenden Sie `search_terms` vor dem Schreiben von SQL, um jedes physische Feld zu finden, das ein Konzept namentlich repräsentiert. Zum Beispiel gibt die Suche nach `"order date"` den Begriff und alle Spalten `order_dt`, `orderDate`, `ORDER_DATE` über jede registrierte Tabelle hinweg zurück.

### Metadaten-Export

Der Begriffsgraph des Glossars ist in jedem von `build_snapshot` erstellten `MetadataSnapshot` enthalten. [tool-verified: `provisa/api/metadata_export/builder.py:_glossary_assets`]

Der Export wendet dieselben Filter wie der Rest des Snapshots an:

- Ein als `export_excluded` markierter Begriff wird vollständig zurückgehalten — unabhängig von seinen physischen Referenzen, seinem abstrakten Status oder ob der Katalog der Org konfiguriert ist. [tool-verified: `provisa/api/metadata_export/builder.py:_glossary_assets`]
- Ein verwurzelter Begriff wird nur veröffentlicht, wenn mindestens eine seiner physischen Referenzen zu einer Spalte gehört, die sowohl den **Data-Product**-Filter (das `data_product`-Flag der Tabelle muss `true` sein) als auch den technischen Spaltenfilter (Spalten mit dem Tag `technical` werden zurückgehalten) besteht.
- Ein verwurzelter Begriff, dessen Referenzen alle durch diese Filter zurückgehalten werden, wird mit ihnen zurückgehalten.
- Abstrakte Begriffe werden bedingungslos veröffentlicht — sie sind Benutzervokabular, nicht an physische Spalten gebunden.
- Eine Kante zwischen zwei Begriffen wird nur veröffentlicht, wenn beide Endpunkt-Begriffe veröffentlicht werden.

Jeder Vendor-Adapter veröffentlicht den Begriffsgraphen nativ, in einen von ihm idempotent erstellten, Provisa-eigenen Glossar-Container — nie in ein bestehendes Katalog-Glossar:

| Provider | Container | Begriffe | Beziehungen | Deprecation |
| --- | --- | --- | --- | --- |
| Apache Atlas | "Provisa Glossary" (Glossary-API) | Glossarbegriffe, Definition unter `longDescription` | KIND_OF → `isA`, SYNONYM_OF → `synonyms`, RELATED_TO/PART_OF → `seeAlso` | `[DEPRECATED]`-shortDescription-Markierung |
| Atlan | Provisa-Glossar nach stabilem qualifiedName | `longDescription` (nie das von Menschen bearbeitete `userDescription`) | gleiche Atlas-Abbildung | `certificateStatus = DEPRECATED` |
| DataHub | `urn:li:glossaryNode:provisa.<org>` | `glossaryTermInfo`-Aspekt pro Begriff | KIND_OF → Inherits, PART_OF → Contains (invertiert), RELATED_TO/SYNONYM_OF → verwandte Begriffe | Deprecation-Aspekt; Umbenennungen folgen der URN-Nachfolge |
| OpenMetadata | Provisa-Glossar über `/v1/glossaries` | fqn-keyed PUT, Umbenennungen PATCH-rebind nach gespeicherter UUID | KIND_OF → native Elternhierarchie, SYNONYM_OF → `synonyms`, andere → `relatedTerms` | `entityStatus` |
| Collibra | Glossary-Typ-Domäne "Provisa Glossary" | Business-Term-Assets über die Import-API | native Business-Term-Beziehungstypen | Asset-Status |

Die Eigentümerschaft ist die Bindung, nicht der Name: Die Vendor-ID jedes veröffentlichten Begriffs wird unter der URN des Begriffs (`provisa://<org>/terms/<name>`) in `catalog_bindings` erfasst, und Provisa ändert oder löscht ein vendorseitiges Glossarelement nur, wenn es diese Bindung hält (oder das Element im von ihm erstellten, Provisa-eigenen Container liegt). Ein Glossarelement ohne Provisa-Bindung stammt aus dem externen System und wird nie angefasst; Updates werden read-merge durchgeführt, sodass von Stewards hinzugefügte Felder auf Provisas eigenen Begriffen erhalten bleiben; nichts wird gelöscht, wenn ein Begriff den Snapshot verlässt. Steward-Zuweisungen von Begriff zu Asset bleiben extern verwaltet — kein Adapter schreibt Begriff-zu-Asset-Zuweisungen (die Veröffentlichung von Provisa-verfassten Zuweisungen ist ein expliziter Folgeschritt). Speziell bei Collibra beruht die Sicherheit unter den REPLACE-Semantiken der Import-API auf Containment: Die Payload erwähnt nur Assets innerhalb der Provisa-Glossar-Domäne und Beziehungsinstanzen nur zwischen Provisa-Begriffen, sodass Steward-Glossare und ihre Beziehungen nie erreichbar sind. [tool-verified: `provisa/api/metadata_export/atlan.py`, `provisa/api/metadata_export/datahub.py`, `provisa/api/metadata_export/atlas.py`, `provisa/api/metadata_export/openmetadata.py`]

---

## Datenprodukte (REQ-1634)

Ein Datenprodukt gruppiert Tabellen, die gemeinsam zur Nutzung veröffentlicht werden, im Besitz genau einer Domäne. Die Felder folgen dem ODPS-Vokabular (Open Data Product Standard), wo Provisa bereits die Source of Truth besitzt. Die Admin-UI stellt Datenprodukte unter **Admin → Data Products** bereit. [tool-verified: `provisa/core/models.py:318-342`, `provisa/api/admin/schema_mutation.py:949-1017`, `provisa/api/admin/schema_query.py:352-362`]

### Capabilities

| Capability | Gewährt |
| --- | --- |
| `data_product_read` | Lesezugriff auf das Query-Feld `data_products` und die Data-Products-Admin-Seite. Standardmäßig an `org_admin`, `analyst`, `developer` und `modeler` vergeben. |
| `data_product_rw` | Erstellungs- und Löschmutationen. Aktiviert die Steuerelemente New / Edit / Delete in der UI. |

[tool-verified: `provisa/api/admin/schema_mutation.py:959,1001`, `provisa/api/admin/schema_query.py:357`]

### Admin-GraphQL

Alle Datenprodukt-Operationen laufen über `POST /admin/graphql`.

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

Erfordert `data_product_read`.

**Erstellen oder aktualisieren:**

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

`create_data_product` führt einen Upsert durch — der Aufruf mit einer bestehenden `id` aktualisiert den Datensatz. Erfordert `data_product_rw`.

**Löschen:**

```graphql
mutation {
  delete_data_product(id: "customer_360") {
    success
    message
  }
}
```

Das Löschen eines Produkts entfernt `product_id` von jeder Mitgliedstabelle und hebt damit ihre Mitgliedschaft auf. Erfordert `data_product_rw`. [tool-verified: `provisa/api/admin/schema_mutation.py:995-1017`]

### Feldschema

| Feld | Typ | Erforderlich | Anmerkungen |
| --- | --- | --- | --- |
| `id` | `String` | Ja | Maschinenlesbarer stabiler Bezeichner, z. B. `customer_360` |
| `domain_id` | `String` | Ja | Besitzende Domäne. Mitgliedstabellen müssen diese `domain_id` teilen — Abweichungen werden beim Speichern zurückgewiesen |
| `name` | `String` | Ja | Anzeigename |
| `owner_role` | `String` | Nein | Rolle, die für dieses Produkt rechenschaftspflichtig ist; unterscheidet sich vom Domänen-Steward |
| `team_role` | `String` | Nein | Rolle, deren Inhaber dieses Produkt im Tagesgeschäft pflegen; löst sich zu einzelnen Personen auf |
| `purpose` | `String` | Nein | Was dieses Produkt veröffentlicht und warum |
| `limitations` | `String` | Nein | Bekannte Einschränkungen, Vorbehalte oder Ausschlüsse |
| `usage` | `String` | Nein | Wie dieses Produkt zu konsumieren ist |
| `version` | `String` | Nein | z. B. `1.2.0` |
| `status` | `String` | Nein | z. B. `proposed`, `active`, `deprecated`, `retired` |
| `sla` | `String` | Nein | Service-Level-Zusagen; Fließtext — ein Produkt umfasst mehrere Tabellen, und ein strukturiertes SLA kann nicht eindeutig benennen, welches Mitglied es beschreibt |
| `support` | `String` | Nein | Freitext-Support-Hinweise |
| `custom_properties` | `JSON` | Nein | Beliebige Schlüssel-Wert-Metadaten, die nicht von den Standardfeldern abgedeckt werden |

Zwei zusätzliche Felder existieren im Modell, sind aber nicht im Strawberry-`DataProductType`/`DataProductInput` exponiert — sie sind spezifisch für den Snowflake Horizon Catalog (REQ-1635):

| Feld | Anmerkungen |
| --- | --- |
| `support_contact` | E-Mail oder URL; erforderlich für Organisations-Listing-Manifeste des Horizon Catalog |
| `publish` | `true`, um Horizon-Listings sofort zu veröffentlichen; neue Listings sind standardmäßig DRAFT |

[tool-verified: `provisa/core/models.py:338-341`, `provisa/api/admin/types.py:104-118,538-551`]

### Tabellenmitgliedschaft

Eine Tabelle tritt einem Datenprodukt bei, indem im Tabellen-Bearbeitungsformular ihr Feld `product_id` gesetzt wird. Der Picker ist auf Produkte beschränkt, deren `domain_id` mit der eigenen Domäne der Tabelle übereinstimmt — eine Tabelle in der Domäne `marketing` wird nie ein Produkt in der Domäne `sales` angeboten. [tool-verified: `provisa/api/admin/actions_router.py:244-260`, `docs/arch/requirements.yaml:54585-54586`]

Commands in derselben Domäne können ebenfalls als Mitglieder zugewiesen werden. [tool-verified: `provisa-ui/src/i18n/locales/en/dataProductsTab.json:commandsLabel`]

### Metadaten-Export-Filter

`build_snapshot` wendet für jede Katalog-Veröffentlichung `data_products_only=True` an. Tabellen ohne `product_id` werden aus dem Snapshot zurückgehalten, ebenso ihre Beziehungskanten, Lineage-Kanten und Governance-Tags. Quellen und Domänen werden immer veröffentlicht. Glossarbegriffe werden nur veröffentlicht, wenn mindestens eine ihrer physischen Referenzen zu einer exportierten (Produktmitglieds-)Tabelle gehört. [tool-verified: `provisa/api/metadata_export/builder.py:594,609,641`]

Ein Produkt ohne exportierte Mitglieder erzeugt keinen Snapshot-Eintrag — ein Listing ohne Mitglieder würde das Produkt gegenüber dem Katalog falsch darstellen. [tool-verified: `provisa/api/metadata_export/model.py:106-113`]

### Datenprodukt-Unterstützung nach Katalogziel

`MetadataSnapshot.data_products` erreicht jeden Adapter, aber nur Adapter, deren Plattform ein natives Datenprodukt-Konzept hat, veröffentlichen es als erstklassige Entität; die übrigen veröffentlichen die (bereits oben gefilterten) Mitgliedstabellen ohne Produktgruppierung.

| Ziel | Datenprodukt-Repräsentation |
| --- | --- |
| Snowflake Horizon | Jedes Produkt wird zu einer `SHARE` über die physischen Adressen seiner Mitgliedstabellen, eingebettet in ein internes `CREATE ORGANIZATION LISTING` — ein natives Horizon-Catalog-Datenprodukt. `publish=true` schaltet das Listing sofort live; andernfalls landet es als DRAFT. [tool-verified: `provisa/api/metadata_export/snowflake_horizon.py:21-34,389-418`] |
| BigQuery Dataplex | Jedes Produkt wird zu einem Analytics-Hub-Listing über `/v1/dataProducts`. [tool-verified: `provisa/api/metadata_export/bigquery_dataplex.py:100,136,159`] |
| OpenMetadata | Jedes Produkt wird zu einer nativen `DataProduct`-Entität (`/api/v1/dataProducts`), mit domänenabgeleiteter Ownership. [tool-verified: `provisa/api/metadata_export/openmetadata.py:326-344,635`] |
| DataHub | Jedes Produkt wird zu einer nativen `dataProduct`-Entität (`urn:li:dataProduct:...`) mit eigenen `dataProductProperties`-/Ownership-Aspekten. [tool-verified: `provisa/api/metadata_export/datahub.py:133-136,443-483`] |
| Collibra | Jedes Produkt wird zu einem Asset eines `Data Product`-Community-Typs, verknüpft mit seinen Mitgliedstabellen über eine Beziehung `Data Product groups Table`. [tool-verified: `provisa/api/metadata_export/collibra.py:129-133,371-388`] |
| Atlan | Veröffentlicht als benutzerdefinierte `DataProduct`-Typedef-Vermutung — Atlan hat keinen dokumentierten stabilen Typnamen für dieses Konzept, daher ist die Abbildung Best-effort. [tool-verified: `provisa/api/metadata_export/atlan.py:60`] |
| Apache Atlas | Veröffentlicht als benutzerdefiniertes `provisa_data_product`-Typedef mit einer `provisa_data_product_members`-Beziehung — Atlas hat keinen nativen Datenprodukt-Entitätstyp. [tool-verified: `provisa/api/metadata_export/atlas.py:134-147,191,256-260`] |
| OpenLineage | Keine erstklassige Entität — Mitgliedstabellen tragen eine benutzerdefinierte `provisa_data_product`-Facette, die das besitzende Produkt benennt. [tool-verified: `provisa/api/metadata_export/openlineage.py:243,348`] |
