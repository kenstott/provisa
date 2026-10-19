# API-Referenz {: #api-reference }

## Übersicht {: #overview }

Provisa stellt REST-Endpunkte unter zwei Präfixen bereit: `/data` für Abfrageausführung und Schema-Introspektion sowie `/admin` für Konfigurationsverwaltung. Die meisten Daten-Endpunkte erfordern einen Rollenbezeichner. Admin-Konfigurationsoperationen verwenden eine Strawberry-GraphQL-API unter `/admin/graphql`.

---

## Authentifizierung {: #authentication }

Wenn `auth.provider` in `provisa.yaml` konfiguriert ist, erfordern alle Endpunkte außer `/health` und `/setup/status` einen `Authorization: Bearer <token>`-Header.

Ohne konfigurierte Authentifizierung läuft der Server im Dev-Modus. Jede Anfrage wird als `anonymous`-Identität behandelt, die auf alle konfigurierten Rollen mit Wildcard-Domänenzugriff abgebildet wird.

**Login (`POST /auth/login`)** wird vom aktiven Auth-Provider bereitgestellt, wenn `provider: basic` konfiguriert ist. Anmeldeformat und Antwort hängen vom Provider ab.

**Identitäts-Introspektion:**

```http
GET /auth/me
```

Gibt die id, E-Mail, den Anzeigenamen, die Org-Mitgliedschaften und Rollenzuweisungen des authentifizierten Benutzers zurück. Im Dev-Modus wird `dev_mode: true` mit allen aufgelisteten Rollen-IDs zurückgegeben.

```http
GET /auth/provider-type
```

Gibt `{"provider": "<name>"}` oder `{"provider": null}` zurück, wenn keine Authentifizierung konfiguriert ist.

---

## Daten-Endpunkte {: #data-endpoints }

### `POST /data/graphql`

Führt eine GraphQL-Abfrage oder -Mutation aus.

**Request-Body:**

```json
{
  "query": "{ orders(where: {region: {eq: \"us\"}}) { id amount } }",
  "variables": {},
  "role": "org_admin",
  "extensions": {}
}
```

Die Anfrage läuft als ihre handelnde Rolle: die Rolle des authentifizierten Benutzers, im Dev-Modus (ohne Authentifizierung) der Header `X-Provisa-Role`, `org_admin`, wenn der Header fehlt. Das Feld `role` im Body wählt keine Rolle. Ist es vorhanden, muss es der handelnden Rolle entsprechen; ein anderer Wert liefert `400` mit Code `data.role_mismatch` und nennt beide Rollen. Dieselbe Regel gilt für `POST /data/sql`, `POST /data/query` und `POST /query/nl`.

Das Feld `extensions` unterstützt das Automatic-Persisted-Query-(APQ)-Protokoll:

```json
{
  "extensions": {"persistedQuery": {"sha256Hash": "<sha256-of-query>"}}
}
```

**Headers:**

- `X-Provisa-Role` — die Rolle, als die die Anfrage läuft (Dev-Modus); mit Authentifizierung eine Rollenanfrage, die nur berücksichtigt wird, wenn die Rolle dem Benutzer zugewiesen ist
- `Accept` — Antwortformat (siehe Content Negotiation)
- `Authorization` — `Bearer <token>`, wenn Authentifizierung aktiviert ist
- `X-Provisa-Redirect-Format` — MIME-Typ für die S3-Redirect-Ausgabe
- `X-Provisa-Redirect-Threshold` — Zeilenanzahl, oberhalb derer der Redirect ausgelöst wird
- `X-Provisa-Redirect` — `true`, um den Redirect bedingungslos zu erzwingen

**Antwort-Cache:** opt-in pro Anfrage mit der Operationsdirektive `@cached` (`query @cached(ttl: 60) { ... }`, `ttl` optional); dieselbe Direktive gilt für GraphQL über Arrow Flight. Cypher meldet sich mit einer Zeile `// @provisa cache=true` oder `// @provisa cache_ttl=N` an, gRPC mit den Aufruf-Metadaten `x-provisa-cache: true` oder `x-provisa-cache-ttl: N`. Ohne sie liest und schreibt die Anfrage den Antwort-Cache nicht. Der Antwort-Header `X-Provisa-Cache` meldet `HIT` oder `MISS`. Siehe [Cache](configuration.md#per-request-opt-in).

<a id="response-cache-request-headers"></a>

#### Request-Header für den Antwort-Cache {: #response-cache-request-headers }

`/data/graphql`, `/data/cypher`, die Neo4j Query API (`/db/{database}/query/v2`), `/data/rest` und `/data/jsonapi` nehmen das Opt-in auch als Request-Header entgegen. Jede der folgenden Formen verhält sich wie `@cached(ttl: N)`:

| Header | Entspricht |
| --- | --- |
| `Cache-Control: max-age=N` / `Cache-Control: max-stale=N` | `@cached(ttl: N)` |
| `Cache-Control: max-stale` (ohne Wert) | `@cached` (die TTL des Betreibers) |
| `Cache-Control: no-cache` / `no-store` | kein Cache, wie wenn nichts gesendet wird |
| `x-provisa-cache: true` | `@cached` |
| `x-provisa-cache-ttl: N` | `@cached(ttl: N)` |

Eine Anfrage ohne einen dieser Header und ohne Hinweis in ihrem Text wird nicht gecacht, obwohl einfache HTTP-Caches standardmäßig cachen. Die Cache-Einstellungen des Betreibers gelten weiterhin wie bei `@cached`: Ein Header macht eine Antwort nie frischer, als sie es erlauben, und wird nie abgelehnt, weil er eine längere `ttl` verlangt. Andere `Cache-Control`-Direktiven (`no-transform`, `min-fresh`, ...) werden ignoriert, wie RFC 9111 es von einem Cache verlangt.

Eine Anfrage kann das Opt-in mehrfach angeben: in ihrem Text und in einem Header oder in zwei Headern. Stimmen die Angaben überein, wird einmal wie gewünscht gecacht. Widersprechen sie sich (zum Beispiel `@cached(ttl: 60)` mit `Cache-Control: max-age=5` oder `x-provisa-cache-ttl: 10` mit `Cache-Control: max-age=20`), schlägt die Anfrage mit `400` und dem Code `data.invalid_cache_control` fehl und nennt jede gesendete Form. Ein Wert `N`, der keine ganze Zahl von Sekunden ist, schlägt genauso fehl. Keine der Formen wird bevorzugt.

```bash
curl -H 'Cache-Control: max-age=120' -H 'X-Provisa-Role: analyst' \
  'http://localhost:8001/data/rest/sales/orders?limit=10'
```

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

Multi-Root-Abfragen führen jedes Root-Feld unabhängig aus. Felder unterhalb des Redirect-Schwellenwerts werden inline zurückgegeben; Felder darüber werden umgeleitet. Der (plurale) Schlüssel `redirects` bildet Feldnamen auf Redirect-Informationen ab.

**Cache-Header:**

- `X-Provisa-Cache: HIT|MISS`
- `X-Provisa-Cache-Age: <seconds>` (bei HIT)

**Erforderliche Capabilities:** `QUERY_DEVELOPMENT` für alle Anfragen einschließlich Introspektion.

---

### Content Negotiation

| Accept-Header | Format |
| --- | --- |
| `application/json` | JSON (Standard) |
| `application/x-ndjson` | Newline-delimited JSON |
| `text/csv` | CSV |
| `application/vnd.apache.parquet` | Parquet |
| `application/vnd.apache.arrow.stream` | Arrow IPC |

---

### Redirect

Ergebnisse oberhalb eines konfigurierten Zeilenschwellenwerts (oder wenn `X-Provisa-Redirect: true`) werden nach S3 geschrieben, und es wird eine presigned URL zurückgegeben.

| Redirect-Format | Geschrieben von | Speicher |
| --- | --- | --- |
| `application/vnd.apache.parquet` | föderiertes CTAS | Keiner — Daten laufen nie durch Provisa |
| `application/x-orc` | föderiertes CTAS | Keiner — Daten laufen nie durch Provisa |
| `application/json` | Provisa | Speicherbegrenzt |
| `application/x-ndjson` | Provisa | Speicherbegrenzt |
| `text/csv` | Provisa | Speicherbegrenzt |
| `application/vnd.apache.arrow.stream` | Provisa | Speicherbegrenzt |

Für große analytische Exporte nutzen Sie die Parquet- oder ORC-Umleitung. Die Federation-Engine schreibt parallel direkt nach S3, und keine Daten laufen durch Provisa.

```yaml
X-Provisa-Redirect-Format: application/vnd.apache.parquet
X-Provisa-Redirect-Threshold: 1000
```

---

### `POST /data/sql`

Führt rohes SQL durch die Stage-2-Governance-Pipeline aus.

**Request-Body:**

```json
{
  "sql": "SELECT id, amount FROM orders WHERE region = 'us'",
  "role": "org_admin"
}
```

`params` (optional) trägt die Werte der Platzhalter `$1…$n` der Anweisung, der Reihe nach, getrennt von ihrem Text:

```json
{
  "sql": "SELECT id, amount FROM orders WHERE region = $1 AND amount > $2",
  "params": ["us", 100]
}
```

Die Werte werden beim Ausführen der Anweisung gebunden, von der Engine oder vom Treiber der Quelle, und nie in die Anweisung geschrieben, sodass ein Wert nicht verändern kann, was die Anweisung ist. Sie gehören zu genau einer Anweisung, deren Platzhalter sie genau durchnummerieren. Ein Stapel von Anweisungen, ein Platzhalter ohne Wert oder ein Wert, den kein Platzhalter aufnimmt, wird mit `400` und dem Code `data.sql_parameters_do_not_fit` abgelehnt, bevor die Anweisung gesteuert oder ausgeführt wird. Eine Anweisung ohne Platzhalter wird wie bisher ohne `params` gesendet.

**Erforderliche Capabilities:** `QUERY_DEVELOPMENT`.

Governance-Verstöße bei `POST /data/sql` geben HTTP 403 zurück.

**Antwort:** Gleiches Format wie `/data/graphql` (standardmäßig JSON-Zeilen, contentverhandelt über `Accept`).

---

### `POST /data/query`

Ein Endpunkt für jede Abfragesprache. Er nimmt GraphQL, SQL oder Cypher an und erkennt, was er bekommen hat.

Cypher-Abfragen können auch an den reinen Cypher-Endpunkt `POST /query/cypher` gesendet werden.

**Request-Body:**

```json
{
  "query": "{ orders { id } }",
  "params": {},
  "variables": {},
  "role": "org_admin"
}
```

Gibt `{"data": ...}` für GraphQL und `{"columns": [...], "rows": [...]}` für SQL und Cypher zurück.

---

### `POST /data/sql/explain`

Erklärt oder analysiert eine SQL-Anweisung durch die governte Pipeline.

Der Endpunkt hüllt das **gesteuerte** SQL in die EXPLAIN-Syntax des Dialekts: die Anweisung, die unter der Rolle des Aufrufers tatsächlich läuft, nach RLS und Maskierung. Der Plan, den Sie sehen, gilt also für die autorisierte Fassung der Abfrage, nicht für die Roheingabe.

**Request-Body:**

```json
{
  "sql": "SELECT id, amount FROM orders",
  "role": "org_admin",
  "analyze": false
}
```

Setzen Sie `analyze: true`, um EXPLAIN ANALYZE auszuführen. Die Abfrage wird ausgeführt, und der Plan trägt reale Zeilenzahlen und Zeitwerte. Nicht jeder Dialekt unterstützt ANALYZE; siehe die Tabelle in [Query plans and statistics](engines.md#query-plans-and-statistics).

**Antwort:** `{"plan": "<plan text or JSON>", "dialect": "trino", "analyzed": false}`

`400`, wenn der Dialekt keine EXPLAIN-Unterstützung hat oder `analyze: true` für einen Dialekt angefordert wird, der dies nicht unterstützt (z. B. SQLite).

---

### `GET /data/engine/state`

Gibt den aktuellen Zustand des Engine-Shards zurück, ohne ihn zu wecken.

Die UI fragt diesen Endpunkt ab, um während des Kaltstarts der Engine ein Startbanner zu zeigen. Er weckt die Engine nie, daher ist das Abfragen gefahrlos und zählt für den Leerlauf-Abschalter nicht als Aktivität.

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

---

### `POST /data/engine/prewarm`

Löst ein Aufwecken der Engine aus, ohne eine Abfrage auszuführen.

Gibt sofort `202 Accepted` zurück, und das Wecken läuft im Hintergrund. Nutzen Sie das, wenn die Engine vor der ersten Abfrage bereit sein soll, etwa aus einem Scheduler, der ein paar Minuten später Abfragen ausführt.

**Antwort:** `202 Accepted`, Body `{"started": true}`

---

### `GET /data/rest/{domain_id}/{table_name}`

Automatisch generierter reiner REST-Endpunkt für jede registrierte Tabelle. Der Query-String wird auf GraphQL-Argumente abgebildet, und die Anfrage wird durch dieselbe Pipeline (RLS, Maskierung, Routing) wie GraphQL kompiliert und ausgeführt.

**Query-Parameter:**

- `limit` — maximale Zeilenanzahl (≥ 1)
- `offset` — zu überspringende Zeilen (≥ 0)
- `fields` — kommagetrennte Feldnamen in der Schreibweise des GraphQL-Schemas, unter der Standard-Namenskonvention camelCase, z. B. `?fields=orderId,amount` (Standard: alle skalaren Felder). Feldnamen in `filter` und `orderBy` folgen derselben Schreibweise. JSON:API nimmt stattdessen den physischen Spaltennamen (`fields[orders]=order_id`).
- `filter` — JSON-Array von `{"field", "comparator", "value"}`-Filterobjekten. Ein Wert wird als Typ seiner Spalte gelesen, `"100"` auf einer Zahlenspalte vergleicht also als 100; ein Wert, den der Spaltentyp nicht fassen kann, liefert `400`.
- `orderBy` — JSON-Array von `{"field", "direction"}`-Sortierobjekten
- `params` — die Parameter der Tabelle als ein JSON-Objekt, nach Parametername, z. B. `?params={"region":"EU"}`. Eine über eine API registrierte Tabelle oder eine Abfragetabelle hat für jede Eingabe ihrer Quelle einen Parameter. Fehlt ein Pflichtparameter, liefert die Abfrage `400` mit seinem Namen, ebenso bei einem Namen, für den die Tabelle keinen Parameter hat. Jeder Wert wird gegen den Typ seines Parameters geprüft.

Filter- und Parameterwerte reisen als Variablen neben dem GraphQL-Text, den der Endpunkt baut; ein Wert mit Anführungszeichen wird daher so verglichen, wie er geschrieben ist.

Die authentifizierte Rolle ist erforderlich; nicht authentifizierte Anfragen geben `401` zurück. Eine OpenAPI-Spezifikation für diese Routen wird unter `GET /data/rest/openapi.json` bereitgestellt, mit Swagger UI unter `GET /data/rest/docs`.

**Antwort-Cache:** Opt-in per Request-Header, `Cache-Control: max-age=N` oder `x-provisa-cache: true` (siehe [Request-Header für den Antwort-Cache](#response-cache-request-headers)). Jede Antwort trägt `X-Provisa-Cache: HIT|MISS` und bei einem Treffer `X-Provisa-Cache-Age`.

#### OpenAPI-/Swagger-UI-Explorer

Die OpenAPI-Explorer-Seite (`/app/openapi`) bettet die Swagger UI in einen abgeschotteten iframe ein. Die Spezifikation ist auf die Rolle zugeschnitten, es erscheinen also nur Tabellen und Spalten, die die aktuelle Rolle sehen kann, und die Domänenauswahl kann sie weiter eingrenzen. Die UI wechselt selbständig zwischen hellem und dunklem Design.

Die Seite lädt das Spezifikations-HTML mit `fetch()` statt über ein direktes iframe-`src`. So trägt die Anfrage das Bearer-Token der Sitzung, und die eigenen relativen Anfragen der Swagger UI werden gegen denselben Ursprung aufgelöst.

Über einen NL-Link „In OpenAPI öffnen“ aufgerufen, klappt die Seite den Ziel-Endpunkt auf, füllt die Abfrageparameter aus der NL-generierten URL aus (z. B. `aggregate`, `groupBy`) und klickt auf Execute, wobei sie das DOM abfragt, damit jeder Schritt fertig ist, bevor der nächste beginnt.

---

### `GET /data/jsonapi/{domain_id}/{table_name}`

Automatisch generierter, [JSON:API](https://jsonapi.org)-konformer Endpunkt für jede registrierte Tabelle. Gleiche RLS, Maskierung und Routing wie GraphQL.

**`Accept`-Header:** muss `application/vnd.api+json` (den JSON:API-Medientyp) enthalten, sonst gibt die Anfrage `406` zurück.

**Query-Parameter:**

- `fields[<type>]` — Sparse Fieldsets, z. B. `?fields[orders]=amount`. Die Namen sind die physischen Spaltennamen, z. B. `fields[orders]=order_id`, ebenso bei `filter[...]`, `sort` und den ausgegebenen Attributschlüsseln. REST nimmt stattdessen die GraphQL-Schreibweise (`fields=orderId`).
- `filter[<col>]` / `filter[<col>][<op>]` — z. B. `?filter[region]=US`, `?filter[amount][gt]=100`. Ein Wert wird als Typ seiner Spalte gelesen; ein Wert, den der Typ nicht fassen kann, liefert `400`.
- `params` — die Parameter der Tabelle als ein JSON-Objekt, nach Parametername, z. B. `?params={"region":"EU"}`, wie beim REST-Endpunkt. Ein fehlender Pflichtparameter liefert `400` mit seinem Namen; ein unbekannter Name oder ein Wert falschen Typs liefert `400` mit `source.parameter` gleich `params`. Paginierungslinks behalten den Parameter.
- `sort` — kommagetrennt, `-`-Präfix für absteigend, z. B. `?sort=-created_at,amount`
- `page[number]` / `page[size]` — Paginierung
- `page[total]` — `true` zählt die Zeilen, die der Filter trifft, und liefert `meta.total` und `links.last`. Ohne ihn trägt die Antwort nur `links.self`, `links.first`, `links.prev` und `links.next`, und die Datenbank führt keine Zählung aus. Jeder Wert außer `true` oder `false` liefert `400`. Links in einer Antwort behalten den Parameter.
- `aggregate` — kommagetrennte Aggregatfunktionen, die anstelle des Zeilenabrufs ausgeführt werden: `count`, `sum`, `avg`, `stddev`, `variance`, `min`, `max`. Verwenden Sie `?aggregate=count,sum`, um eine Teilmenge anzufordern. Aggregat-Antworten geben `data: null` mit Ergebnissen in `meta.aggregate` zurück.
- `groupBy` — kommagetrennte Spaltennamen; wird zusammen mit `?aggregate=` verwendet, um Ergebnisse zu gruppieren. Nur Spalten im `DistinctOnColumn`-Enum der Tabelle sind gültig; der Server gibt `400` für jede Spalte zurück, die die Rolle nicht sehen kann.
- `includeNodes` — `true`, um skalare Spalten der Basistabelle (und über `include=` verbundene skalare Dimensionsspalten) im `nodes`-Array jeder Gruppenzeile einzuschließen. Erforderlich, wenn eine NL-Group-by-Abfrage auch Dimensionsdetails anfordert.

- `include` — kommagetrennte Beziehungsnamen in physischer Schreibweise, z. B. `?include=order_items,shipment`: Jede verknüpfte Ressource wird einmal in `included` zurückgegeben, und jede primäre Ressource verlinkt, was sie einschließt, unter `relationships.<name>.data` (eine Liste von Ressourcen-Identifikatoren für eine To-many-Beziehung, ein Identifikator oder `null` für eine To-one-Beziehung).

Antworten sind Ressourcenobjekte mit `type`/`id`/`attributes`. Die `id` einer Ressource ist der Primärschlüssel ihrer Tabelle: der Wert der Schlüsselspalte oder, bei einem zusammengesetzten Schlüssel, die Werte seiner Spalten in Schlüsselreihenfolge, verbunden durch `:` (Position 2 der Bestellung 7 einer Tabelle mit dem Schlüssel `(order_id, line)` ist `7:2`). Ein `:` oder `\` in einem Wert wird mit `\` maskiert. Eine Tabelle ohne deklarierten Primärschlüssel verwendet ihre Spalte `id`; eine Tabelle mit keinem von beiden liefert `400`, ebenso ein `include`, das eine solche erreicht. Fehler folgen der JSON:API-Fehlerobjekt-Form.

**Antwort-Cache:** Opt-in per Request-Header, `Cache-Control: max-age=N` oder `x-provisa-cache: true` (siehe [Request-Header für den Antwort-Cache](#response-cache-request-headers)). Jede Antwort trägt `X-Provisa-Cache: HIT|MISS` und bei einem Treffer `X-Provisa-Cache-Age`. Zwei Formen, die sich widersprechen, liefern `400` als JSON:API-Fehlerobjekt.

#### JSON:API-Explorer

Die JSON:API-Explorer-Seite (`/app/jsonapi`) ist eine Browser-UI über diesen Endpunkten. Wählen Sie eine Tabelle aus der nach Domäne gruppierten Liste und konfigurieren Sie dann:

- **Fields** — wählen Sie, welche Spalten einbezogen werden (Sparse Fieldset); lassen Sie alle abgewählt, um jede Spalte anzufordern
- **Relationships** — wählen Sie FK-abgeleitete Beziehungsnamen zum Sideloading über `?include=`
- **Filter** — Feld, Operator (`eq`, `neq`, `gt`, `gte`, `lt`, `lte`, `like`) und Wert
- **Sort** — ein Feld, aufsteigend oder absteigend
- **Aggregate** — wählen Sie Group-by-Spalten aus der serverseitig validierten Liste und markieren Sie dann eine oder mehrere Aggregatfunktionen; wenn Group-by-Spalten ausgewählt sind, fügt ein Kontrollkästchen "Include nodes" skalare Spalten der Basistabelle zu jeder Zeile hinzu
- **Page size** — Ressourcen pro Seite, mit Navigation first/prev/next/last

Ergebnisse werden in einer formatierten Zusammenfassungsansicht (Ressourcenkarten mit anklickbaren Beziehungsankern) oder einem rohen JSON-Tab dargestellt. Die Live-Request-URL wird angezeigt und kann kopiert werden. Tabellenauswahl und Seitengröße bleiben sitzungsübergreifend im `localStorage` erhalten.

Bei Navigation über einen NL-Link "In JSON:API öffnen" wählt der Explorer die Tabelle vorab aus und initialisiert den Aggregat-Picker aus den NL-generierten Query-Parametern, dann wird die Anfrage automatisch ausgeführt.

---

### `POST /query/nl`

Reicht eine natürlichsprachliche Frage ein. Der Dienst startet einen asynchronen Job und gibt sofort `202 Accepted` mit einer `job_id` zurück. Erfordert einen LLM-Provider, der im `ai_models`-Konfigurationsabschnitt konfiguriert ist.

**Request-Body:**

```json
{"q": "How many orders were placed last month?", "role": "org_admin"}
```

Gibt `{"job_id": "<id>"}` zurück. Das Überschreiten des NL-Ratenlimits pro Rolle gibt `429` mit einem `Retry-After`-Header zurück.

**Ergebnis abrufen:**

- `GET /query/nl/{job_id}` — Polling. Gibt das Job-Dokument zurück.
- `GET /query/nl/{job_id}/stream` — SSE. Ein `branch`-Ereignis pro Generierungsziel bei dessen Abschluss, dann ein `done`-Ereignis.

Drei Generierungsschleifen (Cypher, GraphQL, SQL) laufen parallel, jede über den Compiler validiert und bei Fehlern verfeinert. Der Prompt ist auf das sichtbare Schema der Rolle beschränkt. Das Ergebnisdokument schlüsselt jeden Branch nach Ziel:

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

Ein Zweig, dem die Iterationen ausgehen, liefert `query: null`, `result: null` und einen `error`-String. Jede generierte Abfrage läuft unter den Rechten des Nutzers mit der Governance der Stufe 2; der Dienst umgeht die Governance nie.

#### NL Group-By mit Dimensionsdetails {: #nl-group-by-with-dimension-details }

Nehmen Sie eine NL-Group-by-Abfrage, die auch Spalten aus einer angebundenen Dimensionstabelle projiziert, etwa „Anzahl der Anfragen pro Benutzer mit Benutzername und E-Mail“. Der Runner leitet aus den Dimensionsspalten im SELECT Punktpfade pro Feld ab (`dim_paths`). Diese Pfade füllen den Parameter `includeNodes=` in den URLs, die für die Panels JSON:API und OpenAPI erzeugt werden, sodass diese Panels dieselben Felder der angebundenen Dimension anfordern, die die Zweige SQL und GraphQL aufgelöst haben. Ohne sie würde `includeNodes=true` nur die eigenen Skalarfelder der Basis-Aggregattabelle liefern.

Im gRPC-Panel trägt die generierte `{Type}GroupByRequest` `include_nodes` (bool) und `include` (repeated string mit Beziehungsfeldnamen). Die zurückgegebene `{Type}GroupByRow` enthält ein typisiertes `nodes`-Feld mit den Dimensionsdetail-Zeilen.

---

### `GET /data/sdl`

Gibt das GraphQL-SDL für das Schema einer Rolle zurück.

**Headers:** `X-Provisa-Role: <role_id>` (erforderlich)

**Query-Parameter:**

- `domain` — kommagetrennte Domänen-IDs. Wenn gesetzt, wird die Antwort auf die genannte(n) Domäne(n) und von ihnen erreichbare Tabellen gefiltert.

**Antwort:** `text/plain` GraphQL-SDL.

---

### `GET /data/introspection`

Gibt GraphQL-Introspektions-JSON zurück, optional domänengefiltert.

**Headers:** `X-Provisa-Role: <role_id>` (erforderlich)

**Query-Parameter:** `domain` — kommagetrennte Domänen-IDs.

**Antwort:** `application/json`-Introspektionsergebnis.

---

### `GET /data/graph-schema`

Gibt die Graphansicht des Schemas der Rolle zurück: Knoten-Labels und ihre Beziehungstypen, für Cypher-/Graph-Clients. Enthält `pk_columns` pro Knoten-Label, sodass Aufrufer Primärschlüsselspalten bestimmen können.

**Antwort:** `application/json` mit `node_labels` (jeweils mit `pk`/`pk_columns`) und `relationship_types`.

Jeder Beziehungstyp trägt außerdem `junction_table_name` und `properties`. Bei einer Junction-Kante nennt das erste die assoziative Tabelle, die sie traversiert, und das zweite listet deren Spalten, lesbar als `r.attr` und filterbar in `WHERE`. Bei einer Fremdschlüssel-Kante ist der Name `null` und die Liste leer; daran unterscheidet ein Client die beiden. Die Junction-Tabelle selbst ist nie eine Knotenbezeichnung. Sie ist die Kante, hat also in einem Graph-Client kein Etikett und keine Zeile in `node_labels`.

---

### `GET /data/domains`

Gibt Domänen-IDs zurück, die für die anfragende Rolle zugänglich sind.

**Headers:** `X-Provisa-Role: <role_id>` (erforderlich)

**Antwort:** `["sales", "support", ...]`

---

### `GET /data/schema-version`

Gibt die aktuelle Schemaversions-Zeichenkette zurück. Kombiniert eine Boot-Nonce mit einem Rebuild-Zähler. Clients verwenden dies, um Schema-Caches nach Server-Neustarts zu invalidieren.

**Antwort:** `{"version": "<boot-id>-<counter>"}`

---

### `GET /data/catalog`

Liefert den Katalog der Rolle, als die die Anfrage läuft: die Tabellen, die dieser Rolle bereitgestellt werden, so wie das Arrow-Flight-Listing sie liefert.

**Antwort:** `{"tables": [{"path": ["<domain>", "<table>"], "schema": "<base64>"}]}`. `schema` ist das Arrow-Schema der Tabelle in der Arrow-IPC-Kodierung: jede Spalte unter dem Namen, mit dem SQL sie liest, mit ihrem registrierten Typ, ihrer Beschreibung und ihren Schlüsselmetadaten (`primary_key`, `references`) in den Feldmetadaten. Commands und Metriken werden nicht gelistet.

Die Rolle ist die, die die Auth-Schicht festgestellt hat (`X-Provisa-Role` kann eine gehaltene Rolle oder eine kommagetrennte Menge anfordern). Ein Deployment ohne Auth-Provider listet den ganzen Katalog, wie das Flight-Listing. Der JDBC-Treiber liest diese Route, wenn der Flight-Port nicht erreichbar ist.

---

### `GET /data/proto/{role_id}`

Gibt die automatisch generierte `.proto`-Datei für eine Rolle zurück.

**Antwort:** `text/plain`-Protobuf-Schema.

`{role_id}` ist eine Rolle, die der Aufrufer innehat, oder eine kommagetrennte Menge solcher Rollen (`analyst,org_admin`), die als Meta-Rolle der Menge bedient wird: die Vereinigung dessen, was ihren Mitgliedern gewährt ist. Eine Rolle, die der Aufrufer nicht innehat, wird mit `403 auth.role_not_assigned` abgelehnt, eine direkt genannte Meta-Rollen-ID mit `403 auth.meta_role_named`. `?domains=a,b` beschränkt das Proto auf diese Domänen; eine Domäne, die die Rolle nicht erreicht, wird mit `403 data.domain_not_accessible` abgelehnt. Eine Rolle ohne Datenoberfläche antwortet mit `404 data.no_proto_for_role`, eine unbekannte Rolle mit `404 data.no_role` und ein noch nicht aufgebautes Modell mit `503 data.schema_cache_not_ready`.

Jede Tabelle im Schema der Rolle erzeugt eine Proto-`message`. Beziehungen erzeugen verschachtelte Message-Felder. Der Service listet RPCs nur für die Tabellen an der Wurzel des Schemas der Rolle auf, dieselben Tabellen wie ihr GraphQL-Typ `Query`; eine verknüpfte Tabelle hat ihre Message und wird über ihre Beziehung erreicht.

Mit `?domains=a,b` wird die Datei auf diese Domänen beschränkt. Der Service listet dann RPCs für die eigenen Tabellen der gewählten Domänen auf; damit verknüpfte Tabellen erscheinen als Messages.

Die Datei folgt dem Modell. Sie wird mit dem Schema der Rolle neu erzeugt, sobald sich das Modell oder die Rolle ändert, auch für eine Meta-Rolle. Typabbildung: `integer → int32`, `bigint → int64`, `varchar → string`, `decimal → double`, `boolean → bool`, `timestamp → google.protobuf.Timestamp`.

---

### `GET /data/subscribe/{table}`

Server-Sent-Events-Stream für Echtzeit-Änderungsbenachrichtigungen aus einer Tabelle.

Die Benachrichtigungszustellung verwendet einen pluggable Provider, der je nach Quelltyp ausgewählt wird: PostgreSQL-Quellen verwenden `LISTEN/NOTIFY` (über asyncpg), MongoDB-Quellen verwenden Change Streams (`collection.watch()`), und Kafka-Quellen verwenden Consumer-Groups. Jeder Provider implementiert eine gemeinsame asynchrone Watch-Schnittstelle. RLS-Filterung und Schema-Validierung gelten unabhängig vom Provider. WebSocket- und RSS-Quellen werden ebenfalls unterstützt.

**Header — `X-Provisa-Sink`:** Auf ein Kafka-Ziel setzen (z. B. `kafka://broker:9092/topic`), um Änderungsereignisse an eine Kafka-Sink umzuleiten, statt an die SSE-Antwort. Der Server startet einen Sink-Consumer und gibt `202 Accepted` statt eines offenen Streams zurück.

### `POST /data/sources/{source_id}/signal`

Teilt Provisa mit, dass sich eine Quelle geändert hat, ohne Zeilen: der Empfänger des Änderungssignals `signal`. Jede Tabelle der Quelle, deren Änderungssignal `signal` ist (ihr eigenes, sonst das ihrer Quelle), wird als geschrieben behandelt, wie bei einem Schreibvorgang über Provisa: Ihre zwischengespeicherten Antworten werden verworfen, die materialisierten Sichten darauf veralten, und ein Build ihres Replikats wird angefordert. Der Build einer lastgeschützten Tabelle wartet auf ihr Nebenzeitfenster. `?table=schema.table` signalisiert eine Tabelle.

Die Rolle des Aufrufers braucht das Schreibrecht und muss die Domäne jeder Tabelle erreichen, die sie signalisiert. Die Antwort ist `202 Accepted` mit den signalisierten Tabellen:

```json
{"signalled": ["public.orders"]}
```

Eine nicht existierende Quelle oder eine `table`, die sie nicht hat, antwortet mit `404`; eine Quelle ohne Tabelle auf dem Änderungssignal `signal` antwortet mit `409`, weil nichts aufgefrischt würde.

---

## Admin-REST-Endpunkte {: #admin-rest-endpoints }

### Config

#### `GET /admin/config`

Lädt die aktuelle `provisa.yaml` als `application/x-yaml` mit einem `Content-Disposition: attachment`-Header herunter.

#### `PUT /admin/config`

Lädt ein überarbeitetes Config-YAML hoch. Der Server schreibt ein `.bak`-Backup, speichert die neue Datei, lädt daraus die Einstellungen des Deployments neu und wendet sie als ausdrückliches, einmaliges Seed auf den Model Store an: Was die Datei deklariert, wird über den Model Store hinzugefügt oder aktualisiert, und nichts wird entfernt. Ein späterer Neustart liest die Einstellungen der Datei und wendet ihr Modell nie wieder an; nach dem ersten Start besitzt allein der Model Store das Modell.

**Request-Body:** Roher YAML-Inhalt.

**Antwort:**

```json
{"success": true, "message": "Config uploaded and applied"}
```

Bei Reload-Fehler: `{"success": false, "message": "<error>"}`.

#### `GET /admin/config/live`

Lädt das Modell im aktuellen Zustand als Konfigurationsdatei herunter. Sie enthält die Einstellungen der Deployment-Datei, wobei jeder Modellabschnitt aus dem Model Store gelesen wird: Stores, Regionen, Quellen, Domänen, Rollen, Datenprodukte, Glossarbegriffe, Tabellen und ihre Spalten, Beziehungen, Metriken, Tags und ihre Zuordnungen, Zeilenfilter, Commands, Webhooks, geplante Trigger und Benennungsregeln. Auf einen leeren Model Store angewendet, baut die Datei dasselbe Modell. Was das Deployment in jeden Store einspielt (die eingebauten Quellen und ihre Tabellen, die Systemdomänen, `org_admin` und `platform_admin`), bleibt weg.

**Query-Parameter:** `sections` (optional) — eine kommagetrennte Liste von Modellabschnitten, z. B. `sources,tables`. Nur diese Abschnitte werden geschrieben; ein unbekannter Name antwortet mit 400 `settings.export_unknown_section`.

Der Endpunkt steht in jedem Deployment einem Aufrufer mit dem Recht für Deployment-Einstellungen offen; der Abschnitt Konfigurationsdatei der Admin-Seite bietet ihn als **Modell exportieren** an.

Gibt `application/x-yaml` mit `Content-Disposition: attachment; filename=provisa.live.yaml` zurück.

#### `GET /admin/config/diff`

Gibt beide Seiten des Konfigurationsvergleichs zurück, `original` (die Ausgangsbasis beim Start) und `current` (den aktuellen Zustand), gleich normalisiert, sodass der Vergleich nur echte Änderungen zeigt und Umsortierungen und Kommentaränderungen übergeht.

**Antwort:**

```json
{"original": "<yaml>", "current": "<yaml>"}
```

#### `POST /admin/config/patch`

Erzeugt einen unified-diff-Patch von der Baseline zur eingesendeten Konfiguration.

Senden Sie das überarbeitete YAML als Request-Body. Die Antwort ist eine `text/x-patch`-Datei (`provisa.config.patch`), die `git apply` oder `patch` direkt anwenden kann; praktisch, um in der UI gemachte Konfigurationsänderungen über eine CI/CD-Pipeline einzuchecken.

---

### Settings

#### `GET /admin/settings`

Gibt aktuelle Plattformeinstellungen als JSON zurück.

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

Ändert Plattformeinstellungen zur Laufzeit. Jedes Feld ist optional, und nur die im Body vorhandenen Schlüssel werden geändert.

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
- `naming`: `domain_prefix`, `convention` — schreibt in die Config-Datei und löst einen Schema-Reload aus
- `relationships`: `auto_track_fk` — regelt ausschließlich die Fremdschlüssel-Verfolgung. Eine über eine Junction realisierte Beziehung wird bei der Tabellenregistrierung deklariert und nie inferiert, daher betrifft diese Einstellung sie nicht.
- `otel`: `endpoint`, `service_name`, `sample_rate`, `support_endpoint`, `support_redact_sql_literals`, `support_redact_attributes`

**Antwort:**

```json
{"success": true, "updated": ["otel.support_endpoint", "cache.default_ttl"]}
```

---

### KI-Modelle {: #ai-models }

#### `GET /admin/ai-models`

Gibt die KI-Modell-Zuweisungen, die Vektor-Modell-Registry und das NL-Ratenlimit der handelnden Org zurück.

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

API-Schlüssel werden nie zurückgegeben: `api_keys_set` sagt nur, ob für jeden Anbieter ein Schlüssel eingerichtet ist. Änderungen gelten ab der nächsten Anfrage, ohne Neustart.

#### `PUT /admin/ai-models`

Aktualisiert die KI-Modell-Zuweisungen, die Vektor-Modell-Registry oder das NL-Ratenlimit der Org. Wird bei der nächsten Anfrage wirksam.

#### `GET /admin/ai-models/vendors/{vendor}/models`

Gibt die Modellnamen zurück, die ein Anbieter derzeit bereitstellt, für den Modell-Picker.

Die Liste wird live aus der eigenen List-Models-API des Anbieters gelesen, mit dem Schlüssel der Organisation oder, wenn sie keinen hat, mit der Anmeldung des Deployments. Ein Modell, das nach Auslieferung dieses Builds erscheint, ist am selben Tag auswählbar, an dem der Anbieter es bereitstellt.

Gibt `400` zurück, wenn der Anbieter keine List-Models-API veröffentlicht (in diesem Fall den Modellnamen direkt eingeben) oder wenn kein Schlüssel verfügbar ist.

---

### Föderations-Engine {: #federation-engine }

#### `GET /admin/federation-engine`

Gibt die aktuelle Auswahl der Föderations-Engine, ihre Verbindungskonfiguration und die vollständige auswählbare Engine-Registry zurück.

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

Persistiert eine Auswahl der Föderations-Engine.

**Request-Body:**

```json
{"engine": "trino", "federation_engine_url": "http://trino-coordinator:8080"}
```

Die Auswahl wird in die Plattformkonfiguration geschrieben und gilt nach dem nächsten Neustart des Dienstes, weil die Engine einmal beim Start gewählt wird.

---

### Domänenrichtlinie {: #domain-policy }

#### `POST /admin/domain-policy`

Ändert die Domänenrichtlinie (`use_domains` / `default_domain`) der handelnden Org.

Die Umstellung entfernt nichts. Sie wird mit `409` und Code `settings.domain_policy_catalog_exists` abgelehnt, solange die handelnde Org einen Katalog hat: eine registrierte Tabelle, eine Quelle oder eine Domäne, die das Deployment nicht vorab angelegt hat. Die `params` der Ablehnung zählen jede Art (`tables`, `sources`, `domains`). Löschen Sie diese Objekte zuerst, jedes über seine eigene Aktion, und stellen Sie dann um. Deklariert eine Konfigurationsdatei einen Teil dieses Katalogs, ist der Code `settings.domain_policy_catalog_in_config`: Das nächste Laden würde ihn wiederherstellen, daher wird die Richtlinie in der Konfigurationsdatei gesetzt.

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

Gibt bis zu N aktuelle abgeschlossene Spans aus dem In-Memory-Span-Puffer zurück.

**Query-Parameter:** `limit` (Standard 50, max. 200)

**Antwort:** `{"traces": [...]}`

#### `POST /admin/query-engine/reload-catalog`

Lädt einen benannten Katalog in der Koordinator-Instanz der Föderations-Engine über deren REST-API im laufenden Betrieb neu. Verbindet Provisas interne Verbindung neu und führt das OTel-DDL erneut aus.

**Query-Parameter:** `catalog` (Standard `"otel"`)

**Antwort:**

```json
{"success": true, "errors": []}
```

#### `POST /admin/query-engine/restart`

Startet den Föderations-Engine-Container neu (nur Single-Node-Dev).

**Query-Parameter:** `container` (Standard: die Umgebungsvariable `QUERY_ENGINE_CONTAINER`, dann `"trino"`)

---

### Discovery

#### `POST /admin/discover/relationships`

Löst die Beziehungs-Discovery aus. Führt immer die FK-Introspektion aus der Föderations-Engine durch. Führt LLM-Inferenz aus, wenn `ANTHROPIC_API_KEY` gesetzt ist.

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

Listet ausstehende Beziehungskandidaten auf.

#### `POST /admin/discover/candidates/{candidate_id}/accept`

Akzeptiert einen Kandidaten und registriert ihn als Beziehung.

**Request-Body (optional):** `{"name": "custom-relationship-name"}`

#### `POST /admin/discover/candidates/{candidate_id}/reject`

Weist einen Kandidaten zurück.

**Request-Body:** `{"reason": "Not a real join"}`

#### `GET /admin/discover/candidates/rejected/count`

Gibt die Anzahl zurückgewiesener Kandidaten zurück.

#### `DELETE /admin/discover/candidates/rejected`

Löscht alle zurückgewiesenen Kandidaten.

---

### Quellen-Crawl {: #source-crawl }

#### `POST /admin/sources/crawl`

Crawlt eine Datenquelle, um ihr Schema zu introspizieren und Tabellen zu registrieren.

---

### Quellen-Tabellensuche {: #source-table-search }

#### `GET /admin/sources/{source_id}/tables/search`

Sucht nach Namen in verfügbaren (noch nicht registrierten) Tabellen einer Quelle.

---

### Tabellenprofiling {: #table-profiling }

#### `POST /admin/tables/{table_id}/profile`

Erstellt ein Spaltenprofil für eine registrierte Tabelle: Kardinalität, Min/Max und Null-Anteile.

---

### Quellbeschreibungen {: #source-descriptions }

#### `POST /admin/source-meta/db-description`

Erzeugt LLM-unterstützte Beschreibungen für die Tabellen und Spalten einer Quelle.

---

### Objektspeicher {: #object-storage }

#### `GET /admin/org-storage`

Meldet den Speicherfußabdruck der handelnden Org gegenüber ihrer Plattform-Zuteilung und ob die Org einen eigenen Store registriert hat.

Wenn die Org ihren eigenen DSN registriert hat, gehen ihre Materialisierungen dorthin und werden nicht mehr gegen die Zuteilung angerechnet. Der DSN selbst wird nie zurückgegeben.

#### `PUT /admin/org-storage`

Registriert (oder löscht) den eigenen Materialisierungsspeicher der Org.

**Request-Body:**

```json
{"storage_url": "s3://my-bucket/provisa?region=us-east-1&access_key=..."}
```

Der DSN wird gegen die Federation-Engine geprüft, bevor er angenommen wird; ein unbrauchbarer DSN scheitert also bei der Registrierung und nicht Stunden später bei einer Aktualisierung. Der Wert wird verschlüsselt gespeichert und von GET nie zurückgegeben.

Senden Sie `storage_url: null`, um den eigenen Store der Org zu löschen und ihre Materialisierungen zum Plattform-Store (und der Zuteilung) zurückzugeben. Die Laufzeitumgebung der Org wird im selben Aufruf neu aufgebaut, sodass der neue Store sofort wirksam ist.

---

### Org-Verschlüsselung {: #org-encryption }

#### `GET /admin/org-encryption`

Gibt den aktuellen Schlüsselstatus der Org zurück: Fingerprint, ID und Herkunft. Gibt nie das Schlüsselmaterial zurück.

Wenn die Org keinen Schlüssel gesetzt hat, wird `{"configured": false}` zurückgegeben. Jede Org startet in diesem Zustand und erbt den Deployment-Schlüssel.

#### `PUT /admin/org-encryption`

Setzt oder rotiert den At-Rest-Verschlüsselungsschlüssel der Org.

**Request-Body:**

```json
{"key_b64": "<32 raw bytes, base64-encoded>"}
```

Lassen Sie `key_b64` weg, damit Provisa einen Schlüssel erzeugt. Das ist die sicherste Wahl, weil der Schlüssel nie in einer Zwischenablage oder einem Anfrageprotokoll auftaucht. Mit `key_b64` bringen Sie Ihren eigenen Schlüssel mit.

Die Rotation fügt dem Key Ring einen neuen aktiven Eintrag hinzu und behält den alten bei, sodass unter dem vorherigen Schlüssel geschriebene Daten weiterhin lesbar bleiben. Rotation ist keine Neuverschlüsselung. Es gibt keinen Delete-Endpunkt: Das Entfernen des letzten Schlüssels würde jede gewrappte Payload unlesbar machen.

Der Live-Ring wird im selben Aufruf neu gebunden, sodass der nächste verschlüsselte Schreibvorgang sofort den neuen Schlüssel verwendet.

---

### Hasura-/DDN-Import

#### `POST /admin/import/hasura/preview`

Wandelt ein Hasura-v2- oder DDN-Projektarchiv in eine vorgeschlagene Provisa-Konfiguration um, ohne etwas zu schreiben.

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

Lädt eine zuvor vorgeschaute Konfiguration in die handelnde Org.

**Request-Body:**

```json
{"config_yaml": "<yaml string>"}
```

Ein Import ist ein ausdrückliches, einmaliges Seed des Model Store der handelnden Org, dieselbe Anwendung wie bei `PUT /admin/config`: Er fügt über den Model Store hinzu und aktualisiert, was die importierte Konfiguration deklariert, mit Schutzprüfungen und Versionen, und entfernt nichts, was er nicht erwähnt. Es gibt kein Feld `replace`. Die Engine-Kataloge der deklarierten Quellen, die Pools der Org und ihre Schemas werden neu aufgebaut, bevor die Antwort zurückkommt.

**Antwort:** `{"summary": {...}}` — die Zahlen dessen, was die importierte Konfiguration deklariert.

---

### Apache-Ossie-Interchange

#### `GET /admin/ossie`

Exportiert das governte Modell der Org als Apache-Ossie-(incubating)-YAML-Dokument.

Das Dokument wird bei jeder Anfrage aus dem aktuellen Zustand gebaut und nie zwischengespeichert, kann also nicht veraltet sein. Tabellen werden zu `dataset`-Objekten, Spalten zu `field`-Objekten, und Beziehungen werden auf Ossie-`relationship`-Objekte abgebildet.

Gibt `text/yaml` mit `Content-Disposition: attachment; filename=provisa.ossie.yaml` zurück.

#### `GET /admin/dbt/sources`

Lädt den regulierten Katalog als dbt-`sources.yml` herunter, so wie er einer Rolle bereitgestellt wird.

**Query-Parameter:** `role` — eine Rolle, die der Aufrufer hält, oder jede Rolle für den Inhaber von `access_config`.

Gibt `text/yaml` mit `Content-Disposition: attachment; filename=provisa.sources.yml` zurück. Eine unbekannte Rolle ist ein `404` mit Code `dbt_sources.unknown_role`. Was die Datei enthält, steht unter [Integrationen](integrations.md#dbt-sources-file).

#### `POST /admin/ossie/import`

Parst ein Ossie-YAML- oder -JSON-Dokument und gibt vorgeschlagene Tabellen- und Beziehungsregistrierungen zurück.

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

### Actions (Funktionen und Webhooks) {: #actions-functions-and-webhooks }

Alle Endpunkte liegen unter dem Präfix `/admin/actions`.

Jeder Aufruf, ob aus GraphQL, SQL, Cypher, Bolt, Arrow Flight, MCP `run_sql` oder Provisa gRPC, läuft durch einen gesteuerten Executor. Er setzt die Command-Zuweisung (`visible_to`), das Recht `write` für eine Mutation und die Governance jedes Mal gleich durch. Die Aufrufsyntax je Protokoll steht in [docs/integrations.md](integrations.md#invoking-commands-across-protocols).

#### `GET /admin/actions`

Gibt alle nachverfolgten DB-Funktionen und Webhooks zurück.

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

Jedes Webhook-Objekt trägt einen booleschen Wert `approved`. Ein Webhook wird genehmigt, sobald ein Steward seine Erstellungsanfrage ausführt; Config-deklarierte Webhooks sind automatisch genehmigt. Ein nicht genehmigter Webhook ist registriert, aber auf keiner Oberfläche exponiert.

#### `POST /admin/actions/functions`

Registriert eine nachverfolgte Funktion (Command).

**Schlüsselfelder:**

| Feld | Erforderlich | Beschreibung |
| --- | --- | --- |
| `name` | Ja | Eindeutiger Command-Name |
| `kind` | Ja | `"query"` → GraphQL-Query-Feld; `"mutation"` → Mutation-Feld |
| `implKind` | Nein | Wie der Command läuft — siehe Tabelle unten (Standard `source_procedure`) |
| `binding` | Nein | `implKind`-spezifische Verbindungsdetails (JSON-Objekt) |
| `returnSchema` | Nein | JSON Schema `{type:"array", items:{type:"object", properties:{...}}}` — macht den Command auf jeder Oberfläche set-returning |
| `arguments` | Nein | `[{name, type}]`-Argumentdefinitionen; die positionale Reihenfolge zählt für SQL- und Bolt-Aufrufer |
| `visibleTo` | Nein | Rollen-IDs, denen der Befehl zugewiesen ist (leer: jede Rolle). Eine `mutation` braucht zusätzlich das Recht `write` |
| `domainId` | Nein | Domäne für GraphQL-Platzierung und Zugriffskontrolle |

**`implKind`-Werte:**

| `implKind` | Was ausgeführt wird | `binding`-Felder |
| --- | --- | --- |
| `source_procedure` | Stored Procedure auf einer registrierten Quelle (Standard) | `sourceId`, `schemaName`, `functionName` |
| `script` | Serverseitiges Skript | `script` |
| `http` | Ausgehender HTTP-Aufruf | `url`, `method` |
| `grpc` | Ausgehender gRPC-Aufruf an einen externen Server | `target`, `method` |
| `python` | Von Provisa gehosteter Python-Callable | `callable` (z. B. `"demo.py_functions:random_dataset"`) |

Die Demo-Commands `random_python_set` (`implKind: python`) und `random_grpc_set` (`implKind: grpc`) zeigen set-returning Commands mit `returnSchema` in der Praxis; beide befinden sich in `config/provisa-install.yaml`.

#### `PUT /admin/actions/functions/{name}`

Aktualisiert eine nachverfolgte Funktion nach Namen.

#### `DELETE /admin/actions/functions/{name}`

Löscht eine nachverfolgte Funktion nach Namen.

#### `POST /admin/actions/webhooks`

Registriert einen verfolgten Webhook. Das Registrieren oder Ändern eines Webhooks stellt eine Genehmigungsanfrage an den Steward in die Warteschlange, und der Webhook wird erst auf allen Oberflächen aktiv, wenn ein Steward ihn genehmigt. In der Konfiguration deklarierte Webhooks werden automatisch genehmigt. **Felder im Request-Body:** `name`, `url`, `method`, `timeoutMs`, `returns`, `inlineReturnType`, `arguments`, `visibleTo`, `domainId`, `description`, `kind`.

#### `PUT /admin/actions/webhooks/{name}`

Aktualisiert einen nachverfolgten Webhook nach Namen. Jede Bearbeitung setzt die Genehmigung zurück auf ausstehend, bis erneut genehmigt wird.

#### `DELETE /admin/actions/webhooks/{name}`

Löscht einen nachverfolgten Webhook nach Namen.

#### `POST /admin/actions/test`

Testet eine Action (Funktion oder Webhook) nach Namen.

---

### Rollen {: #roles }

Alle Endpunkte liegen unter dem Präfix `/admin/roles`.

| Methode | Pfad | Beschreibung |
| --- | --- | --- |
| `GET` | `/admin/roles/` | Alle Rollen auflisten |
| `POST` | `/admin/roles/` | Eine Rolle erstellen |
| `PUT` | `/admin/roles/{role_id}` | Eine Rolle aktualisieren |
| `DELETE` | `/admin/roles/{role_id}` | Eine Rolle löschen |

---

### Benutzer {: #users }

Alle Endpunkte liegen unter dem Präfix `/admin/users`.

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

### Organisationen {: #organizations }

Alle Endpunkte liegen unter `/admin/orgs`.

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

### Einladungen {: #invites }

Alle Endpunkte liegen unter `/admin/invites`.

| Methode | Pfad | Beschreibung |
| --- | --- | --- |
| `POST` | `/admin/invites/` | Eine Einladung erstellen |
| `GET` | `/admin/invites/` | Ausstehende Einladungen auflisten |
| `DELETE` | `/admin/invites/{token}` | Eine Einladung widerrufen |

---

### Admin-GraphQL

#### `POST /admin/graphql`

Strawberry-GraphQL-Endpunkt für alle Admin-Operationen: Quellen- und Tabellen-CRUD, Beziehungsverwaltung, Domänenkonfiguration, RLS-Regeln, Cache-Steuerung, Namenskonventionen, Verwaltung geplanter Tasks und Abfragekompilierung.

Die vollständige Schemareferenz — jedes Query-Feld, Mutation-Feld und jeder Input-/Output-Typ — finden Sie in der [Admin-GraphQL-API-Referenz](admin-graphql.md).

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

---

### Setup

#### `GET /setup/status`

Gibt den Erststart-Setup-Status zurück. Immer nicht authentifiziert.

#### `POST /setup/`

Schließt das Erststart-Setup ab.

---

## Health Check

#### `GET /health` oder `HEAD /health` {: #get-health-or-head-health }

Gibt `{"status": "ok"}` zurück. Immer nicht authentifiziert.

---

## Fehlerantworten {: #error-responses }

| Status | Bedeutung |
| --- | --- |
| 400 | Ungültige Abfrage, Validierungsfehler oder SQL-Parse-Fehler |
| 401 | Fehlendes oder ungültiges Auth-Token |
| 403 | Unzureichende Capabilities; Governance-Verstoß |
| 404 | Rolle, Ressource oder Config-Datei nicht gefunden |
| 422 | Fehlender erforderlicher Header (z. B. `X-Provisa-Role`) |
| 503 | Datenbank oder Quelle nicht verbunden; Abhängigkeit nicht verfügbar |
| 504 | Anfrage-Timeout |

Governance-Verstöße bei `POST /data/sql` geben HTTP 403 mit einem strukturierten Body zurück:

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

## Arrow-Flight-Endpunkt {: #arrow-flight-endpoint }

Port `8815`. Nativer Arrow-Columnar-Transport über gRPC.

Abfragen und Katalog-Discovery sind beide auf derselben Verbindung verfügbar. Die vollständige Governance-Pipeline (RLS, Maskierung, Sampling) wird auf jede Abfrage angewendet.

Die Katalog-Discovery (`list_flights`, `get_flight_info`, `get_schema` und ein Ticket ohne `query`) listet, was der Rolle des Aufrufers freigegeben ist. Mit eingeschalteter Authentifizierung nehmen die drei Aufrufe ohne Ticket das Credential aus dem Aufruf-Header `authorization: Bearer` und die Rolle aus `x-provisa-role` und werden ohne Credential abgelehnt; siehe [Integrationen](integrations.md).

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

Wenn der Zaychik-Flight-SQL-Proxy verfügbar ist (Port 8480), streamen Record-Batches End-to-End ohne vollständige Materialisierung. Fällt auf Materialisierung über die föderierte Abfrageschicht zurück, wenn Zaychik nicht verfügbar ist.

**Warnungen.** Was eine Antwort über sich selbst sagt, reist im Stream als `app_metadata` eines Batches ohne Zeilen: `{"provisa_warnings": [{"code", "params", "message"}]}`. Warnungen, die vor dem Lesen der Zeilen bekannt sind, kommen in einem zeilenlosen Batch vor ihnen. Eine Warnung, die erst nach dem Lesen der Zeilen bekannt ist, kommt in einem zweiten zeilenlosen Batch, dem letzten des Streams: `statement.rows_cut` sagt, dass die Antwort an einem Zeilenlimit abgeschnitten wurde und weitere Zeilen passen. Lesen Sie `app_metadata` bei jedem Batch, den letzten eingeschlossen.

---

## Protobuf-gRPC-Endpunkt {: #protobuf-grpc-endpoint }

Port `50051` (mit der Umgebungsvariable `GRPC_PORT` oder der Config `server.grpc_port` überschreibbar).

Übergeben Sie die Rolle im gRPC-Metadatenschlüssel `x-provisa-role`. Fehlt dieser, bricht der Server mit `UNAUTHENTICATED` ab.

Laden Sie das rollenspezifische Proto von `GET /data/proto/{role_id}` herunter. Nur für diese Rolle sichtbare Tabellen und Spalten erscheinen.

```proto
service ProvisaService {
  rpc QueryOrders (QueryOrdersRequest) returns (stream Orders);
  rpc InsertOrders (InsertOrdersRequest) returns (InsertOrdersResponse);
}
```

Jede Tabelle an der Wurzel des Schemas der Rolle erzeugt eine streamende RPC `Query{TypeName}`. Das sind die Tabellen, die der GraphQL-Typ `Query` der Rolle auflistet. Eine Tabelle, die die Rolle nur über eine Beziehung erreicht, hat keine eigene RPC, so wie sie in GraphQL kein Wurzelfeld hat. `Insert{TypeName}`-RPCs existieren aus Gründen der Schema-Symmetrie, brechen aber mit `UNIMPLEMENTED` ab.

**Parameter.** Eine Tabelle mit Parametern (eine über eine API registrierte Tabelle oder eine Abfragetabelle) hat eine Nachricht `{TypeName}Params` mit einem optionalen Feld je Parameter, benannt ohne das Präfix `_nf_`, und `Query{TypeName}Request` trägt sie als `params`. Jeder dort gesetzte Wert erreicht die Quelle als der Parameter, gebunden in die Anweisung; ein Name, der kein Parameter der Tabelle ist, wird mit `INVALID_ARGUMENT` abgewiesen, und ein Lesen ohne einen Pflichtparameter wird unter dessen Namen abgewiesen. Der HTTP-gRPC-Proxy nimmt dieselben Werte als Objekt `params` des Bodys (HTTP 400, Code `data.invalid_parameter`, bei unbekanntem Namen oder falschem JSON-Typ).

Eine Methode, die nicht zum Service der Rolle des Aufrufers gehört, antwortet mit `UNIMPLEMENTED`.

`grpc_reflection.v1alpha` ist für die Service-Discovery ohne vorkompiliertes Proto aktiviert. Reflection beschreibt jedem Aufrufer den Service der Rolle, als die er handelt: dasselbe `.proto`, das diese Rolle über `GET /data/proto/{role_id}` herunterlädt. Sie nennt den Provisa-Service und den Reflection-Service beim Namen und beschreibt die Datei des Provisa-Service.

Ein Reflection-Aufruf wird wie jeder andere gRPC-Aufruf identifiziert. Bei aktivierter Authentifizierung stammt die Rolle aus dem Berechtigungsnachweis des Aufrufers. Ohne Authentifizierung nennen Sie die Rolle in `x-provisa-role`; bedient die Installation mehr als eine Organisation, nennen Sie zusätzlich die Organisation in `x-provisa-org`. Ein Aufruf ohne diese Angaben wird mit `UNAUTHENTICATED` abgelehnt. Ohne Authentifizierung bleibt Reflection deaktiviert, solange `server.grpc_allow_unsecured_reflection` nicht gesetzt ist.

```bash
grpcurl -plaintext -H 'x-provisa-role: analyst' localhost:50051 list
grpcurl -plaintext -H 'x-provisa-role: analyst' \
  -d '{}' localhost:50051 provisa.v1.ProvisaService/QueryOrders
```

Der gRPC-Server startet, wenn das beim Start geladene Modell einen gRPC-Service definiert. Danach wird jeder Aufruf aus dem Modell in seinem aktuellen Stand bedient: Eine Tabelle, die bei laufendem Server registriert oder geändert wird, wird vom nächsten Aufruf bedient und beschrieben, ohne Neustart.

#### Aggregat- und Group-By-RPCs {: #aggregate-and-group-by-rpcs }

Wenn für eine Tabelle `enable_aggregates` gesetzt ist, enthält das generierte Proto zwei zusätzliche RPCs neben `Query{TypeName}`:

- **`Query{TypeName}Aggregate`** — gibt Aggregat-Skalare für die Tabelle zurück (`count`; `sum`, `avg`, `stddev`, `variance` pro numerischer Spalte; `min`, `max` pro vergleichbarer Spalte)
- **`Query{TypeName}GroupBy`** — gibt eine Zeile pro Gruppenschlüssel mit Aggregat-Unterfeldern und optional Skalaren der Basistabelle sowie verbundenen Dimensionszeilen in einem `nodes`-Feld zurück

Beide laufen durch dieselbe Aggregat-Pipeline des Compilers wie die GraphQL-Root-Felder `{field}_aggregate` und `{field}_group_by`; eine eigene Aggregat-Implementierung gibt es nicht.

**Feld `funcs`.** Die Request-Message akzeptiert ein repeated-string-Feld `funcs`. Gültige Werte sind `count`, `sum`, `avg`, `stddev`, `variance`, `min` und `max`. Wenn `funcs` weggelassen wird, wird jede Funktion angefordert, die das Schema für diese Tabelle bereitstellt. Wenn gesetzt, erscheinen nur die genannten Funktionen. Wenn keine der genannten Funktionen auf die Spaltentypen der Tabelle zutrifft, fällt die Abfrage auf `count` zurück.

**Felder `include_nodes` und `include`.** `Query{TypeName}GroupBy`-Anfragen können `include_nodes: true` setzen, um skalare Spalten der Basistabelle im `nodes`-Feld jeder Zeile einzuschließen. Das repeated-string-Feld `include` benennt Many-to-One-Beziehungsfelder, deren skalare Spalten ebenfalls in `nodes` verschachtelt werden. Dies entspricht dem Verhalten von `?includeNodes=` / `?include=` bei JSON:API.

---

## JDBC-Treiber {: #jdbc-driver }

Der Provisa-JDBC-Treiber (`provisa-jdbc-<release>.jar`) stellt den semantischen Katalog für BI-Tools (Tableau, PowerBI, DBeaver) bereit.

**Connection-URL:** `jdbc:provisa://host:port`

Domänen werden auf JDBC-Schemas abgebildet. Tabellen verwenden ihre registrierten Aliase. Spalten verwenden Aliase und stellen Beschreibungen als `REMARKS` bereit. Standard-Metadatenmethoden (`getPrimaryKeys`, `getImportedKeys`, `getExportedKeys`) exponieren semantische Beziehungen als PK-/FK-Metadaten.

**SQL-Unterstützung:** `SELECT * FROM <alias> [WHERE col = 'value']`.

Der Treiber fordert standardmäßig einen Arrow-IPC-Redirect an. Ergebnisse werden batchweise über `ArrowStreamReader` gestreamt, begrenzt auf einen Record-Batch im Speicher.

---

## Argumentformat `orderBy` {: #orderby-argument-format }

Das Argument `order_by` verwendet `{column: direction}`-Objekte mit einem 6-wertigen Richtungs-Enum:

```json
{
  "query": "{ orders(order_by: [{created_at: desc_nulls_last}]) { id created_at } }",
  "role": "admin"
}
```

Unterstützte Richtungen: `asc`, `desc`, `asc_nulls_first`, `asc_nulls_last`, `desc_nulls_first`, `desc_nulls_last`.

---

## Subscriptions

SSE-Subscriptions sind unter `GET /data/subscribe/{table}` verfügbar. Die Benachrichtigungszustellung verwendet einen pro Quelltyp gewählten pluggable Provider: PostgreSQL-Quellen verwenden `LISTEN/NOTIFY`, MongoDB-Quellen verwenden Change Streams, und Kafka-Quellen verwenden Consumer-Groups. RLS-Filterung und Schema-Validierung gelten unabhängig vom Provider. WebSocket- und RSS-Quellen werden über denselben Endpunkt ebenfalls unterstützt.

---

## Business-Glossar {: #business-glossary }

Das Geschäftsglossar bildet physische Feldnamen, wie sie in den Quelldatenbanken stehen, auf ein gemeinsames menschliches Vokabular ab. Jede im Modell registrierte Spalte bekommt automatisch einen Begriff, niemand muss also etwas eintragen, um das Glossar zu füllen. Kuratoren ergänzen Definitionen, Beziehungen und Experten zu dem, was das System ableitet.

### Wie Begriffe abgeleitet werden {: #how-terms-are-derived }

Wenn Provisa die Spalten einer Tabelle registriert oder aktualisiert, läuft `normalize_term` (`provisa/core/glossary.py`) für jeden Spaltennamen und erzeugt eine kanonische Formulierung.

Die Normalisierung wendet fünf Regeln in Reihenfolge an:

1. Aufteilen an camelCase-Grenzen und Trennzeichen (`_`, `-`, `.`, `/`, Leerzeichen).
2. Das Ergebnis in Kleinbuchstaben umwandeln.
3. Eine feste Abkürzungstabelle expandieren (z. B. `cust` → `customer`, `amt` → `amount`, `dt` → `date`, `id` → `identifier`, `key` → `identifier`, `guid` → `identifier`).
4. Ein nachgestelltes **Proxy-Token** entfernen (`identifier`, `code`, `index` oder `reference`) — eine Spalte, die nach ihrem Schlüssel oder Code benannt ist, verweist über einen Platzhalterwert auf das zugrunde liegende Konzept, daher sollte der Begriff das Konzept selbst sein. Das letzte verbleibende Token wird nie entfernt.
5. Eine **zu generische Formulierung** mit dem Konzept der Tabelle qualifizieren. Wenn die vollständig normalisierte Formulierung ein bloßes Attributwort ist (`name`, `identifier`, `date`, `location`, `message`, `first name`, `last name` und Ähnliches), wird der Begriff zu `<table concept> <phrase>` — `employees.first_name` → `employee first name`, `orders.id` → `order identifier`. Ein gemeinsamer Begriff `name` über nicht verwandte Tabellen hinweg würde unterschiedliche Bedeutungen zusammenführen; die Qualifizierung verbindet stattdessen jede Spalte mit ihrem umschließenden Konzept. Das Tabellenkonzept ist der Geschäftsname der Tabelle, normalisiert mit einem singularen Kopfnomen (`order_lines` → `order line`).

Native-Filter-Pseudospalten (mit `_nf_`-Präfix, oder jede Spalte, die `native_filter_type` trägt) sind Abfrageparameter-Mechanik, keine Geschäftsfelder, und leiten keine Begriffe ab.

Da `id`, `key`, `pk` und `sk` alle vor der Proxy-Prüfung zu `identifier` expandieren, landen drei physisch unterschiedliche Spaltennamen auf exakt demselben Begriff:

| Physischer Name | Nach Normalisierung |
| --- | --- |
| `cust_id` | `customer` |
| `customerId` | `customer` |
| `CUSTOMER_KEY` | `customer` |
| `txn_amt` | `transaction amount` |

Die ersten drei fallen zu einem Begriff zusammen. `transaction amount` behält beide Token, weil `amount` kein Platzhalter ist. Eine bloße `id`-Spalte ohne vorangehende Token lässt sich nicht kürzen; sie wird zu `identifier` normalisiert, damit der Begriff nicht leer ist.

### Lebenszyklus {: #lifecycle }

Begriffe werden **aus der Zugehörigkeit zur semantischen Schicht abgeleitet**; Benutzer legen sie nicht nach Bedarf an. Das Tabellen-Repository ist der einzige Schreibweg: `sync_table_refs` läuft in jedem Upsert eines Spaltensatzes, und `sweep_refless_terms` läuft nach jedem Löschpfad.

**Wenn eine Spalte hinzugefügt wird:** Provisa sucht den normalisierten Begriff nach Name. Existiert er bereits, erhält die Spalte eine Referenz darauf (und wurde der Begriff als veraltet markiert, wird er wiederbelebt — `deprecated` wird zurück auf `False` gesetzt). Existiert noch kein Begriff, wird einer erstellt.

**Wenn eine Spalte wegfällt** (Schemaänderung oder Entfernen einer Tabelle), wird ihre Referenz gelöscht und der Begriff nach einer Regel „entfernen oder als veraltet markieren“ **erledigt**. Ein verwurzelter Begriff ohne verbleibende Referenzen wird samt Kanten und Expertenzuordnungen gleich entfernt, außer das Entfernen ließe einen abstrakten Begriff ohne Pfad durch den Begriffsgraphen zu einem verwurzelten Begriff zurück. Dann wird der Begriff **als veraltet markiert** (`deprecated=True`) statt gelöscht, sodass der abstrakte Begriff seinen Anker im Graphen behält.

Abstrakte Begriffe werden nie automatisch entfernt; sie existieren außerhalb des physischen Lebenszyklus und werden nur explizit über die Admin-API gelöscht.

**Wiederbelebung:** Erscheint der normalisierte Name eines als veraltet markierten Begriffs erneut (eine Spalte wird neu registriert), wird die Markierung entfernt, und seine Referenzen sammeln sich wieder an.

### Kuratierungs-Endpunkte {: #curation-endpoints }

Alle Endpunkte liegen unter `/admin/glossary`. Sie erfordern `org_admin`-Zugriff und eine konfigurierte Org. Jede Mutation löst eine Metadaten-Veröffentlichung aus.

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

Gültige `rel_type`-Werte: `KIND_OF`, `RELATED_TO`, `PART_OF`, `SYNONYM_OF`.

**Body für `POST /admin/glossary/terms/{term_id}/experts`:**

```json
{"user_id": "alice@example.com", "kind": "author"}
```

Gültige `kind`-Werte: `expert`, `author`.

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

Das Setzen von `export_excluded` auf `true` hält den Begriff von allen Metadaten-Export-Snapshots zurück, unabhängig von seinen physischen Referenzen oder seinem abstrakten Status. Das Zurücksetzen auf `false` stellt den Begriff bei der nächsten Veröffentlichung wieder im Snapshot her. Kuratierungsdaten (Definition, Kanten, Experten) sind davon unberührt.

### KI-unterstützte Kuratierung {: #ai-assisted-curation }

Das konfigurierte KI-Modell der Org kann Definitionen entwerfen und in einer einzigen Operation Beziehungskanten über das gesamte Glossar hinweg vorschlagen. Beide Batch-Aktionen erfordern `org_admin`-Zugriff und eine konfigurierte Org.

**`POST /admin/glossary/definitions/generate`**

Iteriert über jeden Begriff im Glossar, überspringt jeden, der bereits eine Definition hat, und ruft das KI-Modell der Org auf, um für jeden verbleibenden Begriff einen Entwurf zu erstellen. Der Entwurf wird sofort persistiert — anders als beim Pro-Begriff-Entwurfs-Endpunkt (`POST /admin/glossary/terms/{term_id}/definition/generate`) gibt es keinen Editor-Schritt. Von Menschen verfasste Definitionen werden nie überschrieben: Die Schutzklausel ist `if summary["definition"]: continue` vor jedem Modellaufruf. Eine einzige Veröffentlichungsbenachrichtigung deckt den gesamten Batch ab.

Antwort:

```json
{"generated": 12}
```

`generated` ist die Anzahl der Begriffe, die eine neue Definition erhalten haben. Sie ist null, wenn jeder Begriff bereits eine hat.

**`POST /admin/glossary/relationships/generate`**

Sendet die vollständige Begriffsliste an das KI-Modell der Org mit einem Prompt, der die zehn erlaubten Kantentypen (`KIND_OF`, `PART_OF`, `SYNONYM_OF`, `RELATED_TO`, `VALID_VALUE_OF`, `DERIVED_FROM`, `REPLACES`, `PREFERRED_TERM_FOR`, `TRANSLATION_OF`, `ANTONYM_OF`) spezifiziert und nur um sichere Vorschläge bittet. Das Modell antwortet mit einem JSON-Array; jeder Eintrag wird vor jedem Schreibvorgang validiert: unbekannte Begriffsnamen, Selbstkanten und Kantentypen außerhalb des geschlossenen Enums werden stillschweigend verworfen. Gültige Vorschläge werden idempotent upserted — ein erneuter Aufruf der Aktion dupliziert keine Kanten. Eine einzige Veröffentlichungsbenachrichtigung deckt den Batch ab. Der Endpunkt gibt sofort `{"added": 0}` zurück, wenn das Glossar weniger als zwei nicht veraltete Begriffe enthält.

Antwort:

```json
{"added": 5}
```

`added` ist die Anzahl der geschriebenen Kanten. Eine bereits existierende Kante zählt weiterhin mit — der Upsert gelingt, aber die Kantendaten ändern sich nicht.

### MCP-Tool `search_terms` {: #mcp-search_terms-tool }

```
search_terms(query, role=None, limit=25)
```

Durchsucht Begriffsnamen und Definitionen mit einer Groß-/Kleinschreibung ignorierenden Teilstring-Suche, bis zu `limit` Ergebnisse. Jedes Ergebnis ist das vollständige Begriffsdetail: `name`, `definition`, `is_abstract`, `deprecated`, physische Referenzen (mit `source_id`, `schema_name`, `table_name`, `column_name`), typisierte Kanten und Expertenzuweisungen.

Verwenden Sie `search_terms` vor dem Schreiben von SQL, um jedes physische Feld zu finden, das ein Konzept namentlich repräsentiert. Zum Beispiel gibt die Suche nach `"order date"` den Begriff und alle Spalten `order_dt`, `orderDate`, `ORDER_DATE` über jede registrierte Tabelle hinweg zurück.

### MCP-Statistik-Tools {: #mcp-statistics-tools }

```
describe(sql, role=None)
correlate(sql, columns, method="pearson", role=None)
regress(sql, target, features, kind="linear", role=None)
compare(sql, value, group, role=None)
forecast(sql, time, value, horizon, seasonal_periods=None, role=None)
```

Jedes Tool nimmt einen durch `sql` definierten Datensatz und führt ihn durch dieselbe gesteuerte Pipeline wie `run_sql`, in der Rolle des Aufrufers: Zeilenregeln, Maskierung und Schwärzung gelten, SQL, das die Pipeline ablehnt, scheitert wie bei `run_sql`, und eine Rolle, die der Aufrufer nicht innehat, wird abgelehnt. Jedes gibt Ergebnisse zurück, nie die Zeilen, mit der verwendeten Methode und dem gezählten `n`.

| Tool | Gibt zurück |
| --- | --- |
| `describe` | Pro Spalte: `count`, `missing`, `distinct`; bei einer Zahl `mean`, `sd`, `min`, `max` und `quantiles` (p0, p5, p25, p50, p75, p95, p100); `top_values` (die fünf häufigsten) |
| `correlate` | Die `pearson`- oder `spearman`-Korrelations`matrix` der numerischen `columns`, über die in allen vollständigen Zeilen, und dieses `n` |
| `regress` | `kind="linear"`: Kleinste-Quadrate-`coefficients` (`estimate`, `se`, `p_value`), `r_squared`, `adj_r_squared`. `kind="logistic"`: ein `target` mit zwei Werten, Maximum-Likelihood-`coefficients`, `pseudo_r_squared`, `auc` und der `positive_value`. Ein Text-Feature wird zu einem Indikator je Wert außer dem ersten |
| `compare` | `n`, `mean` und `sd` jeder Gruppe; bei zwei Gruppen Welchs t-Test (`t`, `df`, `p_value`) und Cohens d; bei mehr einfaktorielle ANOVA (`f`, `df_between`, `df_within`, `p_value`) und Eta-Quadrat |
| `forecast` | `horizon` Punkte nach der letzten `time`, jeder mit `value`, `lower` und `upper` (95-%-Prognoseintervall), durch exponentielle Glättung (ETS) mit additivem Fehler, additivem Trend ab 10 Punkten und additiver Saisonalität, wenn `seasonal_periods` angegeben ist |

Die Engine berechnet, was sie kann, über jede Zeile, die die Rolle liest: die Anzahlen, distinkten Anzahlen, Mittelwerte, Standardabweichungen, Minima und Maxima von `describe` sowie `n`, Mittelwert und Varianz jeder Gruppe für `compare`. Ein angepasstes Modell sowie die Quantile und häufigsten Werte von `describe` lesen höchstens `mcp.stats_max_rows` Zeilen (Admin → Settings → Limits; standardmäßig 100.000). Jede Antwort, die Zeilen liest, nennt `rows_read`, `row_cap` und `truncated`, und das eigene Zeilenlimit einer Rolle wird wie bei `run_sql` unter `warnings` gemeldet.

Statistik-Tools geben nur Ergebnisse zurück, sodass die Zeilen hinter einer Analyse nie in den Kontext des Modells gelangen: Datenminimierung standardmäßig. Die Zugriffskontrolle begrenzt, was ein Agent erfahren kann. Um Identitäten außer Reichweite eines Agenten zu halten, erteilen Sie seiner Rolle nur die Spalten, die niemanden identifizieren; eine Rolle, die eine Spalte lesen kann, kann sie aggregieren, und Aggregate über kleine Gruppen können dennoch Einzelpersonen identifizieren. Siehe [Sicherheit](security.md#statistics-over-mcp).

### Metadaten-Export {: #metadata-export }

Der Begriffsgraph des Glossars ist in jedem von `build_snapshot` erstellten `MetadataSnapshot` enthalten.

Der Export wendet dieselben Filter wie der Rest des Snapshots an:

- Ein als `export_excluded` markierter Begriff wird vollständig zurückgehalten — unabhängig von seinen physischen Referenzen, seinem abstrakten Status oder ob der Katalog der Org konfiguriert ist.
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

Die Eigentümerschaft ist die Bindung, nicht der Name: Die Vendor-ID jedes veröffentlichten Begriffs wird unter der URN des Begriffs (`provisa://<org>/terms/<name>`) in `catalog_bindings` erfasst, und Provisa ändert oder löscht ein vendorseitiges Glossarelement nur, wenn es diese Bindung hält (oder das Element im von ihm erstellten, Provisa-eigenen Container liegt). Ein Glossarelement ohne Provisa-Bindung stammt aus dem externen System und wird nie angefasst; Updates werden read-merge durchgeführt, sodass von Stewards hinzugefügte Felder auf Provisas eigenen Begriffen erhalten bleiben; nichts wird gelöscht, wenn ein Begriff den Snapshot verlässt. Steward-Zuweisungen von Begriff zu Asset bleiben extern verwaltet — kein Adapter schreibt Begriff-zu-Asset-Zuweisungen (die Veröffentlichung von Provisa-verfassten Zuweisungen ist ein expliziter Folgeschritt). Speziell bei Collibra beruht die Sicherheit unter den REPLACE-Semantiken der Import-API auf Containment: Die Payload erwähnt nur Assets innerhalb der Provisa-Glossar-Domäne und Beziehungsinstanzen nur zwischen Provisa-Begriffen, sodass Steward-Glossare und ihre Beziehungen nie erreichbar sind.

---

## Datenprodukte {: #data-products }

Ein Datenprodukt gruppiert Tabellen, die gemeinsam zur Nutzung veröffentlicht werden, im Besitz genau einer Domäne. Die Felder folgen dem ODPS-Vokabular (Open Data Product Standard), wo Provisa bereits die Source of Truth besitzt. Die Admin-UI stellt Datenprodukte unter **Admin → Data Products** bereit.

### Capabilities

| Capability | Gewährt |
| --- | --- |
| `data_product_read` | Lesezugriff auf das Query-Feld `data_products` und die Data-Products-Admin-Seite. Standardmäßig an `org_admin`, `analyst`, `developer` und `modeler` vergeben. |
| `data_product_rw` | Erstellungs- und Löschmutationen. Aktiviert die Steuerelemente New / Edit / Delete in der UI. |

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

Das Löschen eines Produkts entfernt `product_id` von jeder Mitgliedstabelle und hebt damit ihre Mitgliedschaft auf. Erfordert `data_product_rw`.

### Feldschema {: #field-schema }

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

Zwei zusätzliche Felder existieren im Modell, sind aber nicht im Strawberry-`DataProductType`/`DataProductInput` exponiert — sie sind spezifisch für den Snowflake Horizon Catalog:

| Feld | Anmerkungen |
| --- | --- |
| `support_contact` | E-Mail oder URL; erforderlich für Organisations-Listing-Manifeste des Horizon Catalog |
| `publish` | `true`, um Horizon-Listings sofort zu veröffentlichen; neue Listings sind standardmäßig DRAFT |

### Tabellenmitgliedschaft {: #table-membership }

Eine Tabelle tritt einem Datenprodukt bei, indem im Tabellen-Bearbeitungsformular ihr Feld `product_id` gesetzt wird. Der Picker ist auf Produkte beschränkt, deren `domain_id` mit der eigenen Domäne der Tabelle übereinstimmt — eine Tabelle in der Domäne `marketing` wird nie ein Produkt in der Domäne `sales` angeboten.

Commands in derselben Domäne können ebenfalls als Mitglieder zugewiesen werden.

### Metadaten-Export-Filter {: #metadata-export-filter }

`build_snapshot` wendet für jede Katalog-Veröffentlichung `data_products_only=True` an. Tabellen ohne `product_id` werden aus dem Snapshot zurückgehalten, ebenso ihre Beziehungskanten, Lineage-Kanten und Governance-Tags. Quellen und Domänen werden immer veröffentlicht. Glossarbegriffe werden nur veröffentlicht, wenn mindestens eine ihrer physischen Referenzen zu einer exportierten (Produktmitglieds-)Tabelle gehört.

Ein Produkt ohne exportierte Mitglieder erzeugt keinen Snapshot-Eintrag — ein Listing ohne Mitglieder würde das Produkt gegenüber dem Katalog falsch darstellen.

### Datenprodukt-Unterstützung nach Katalogziel {: #data-product-support-by-catalog-target }

`MetadataSnapshot.data_products` erreicht jeden Adapter, aber nur Adapter, deren Plattform ein natives Datenprodukt-Konzept hat, veröffentlichen es als erstklassige Entität; die übrigen veröffentlichen die (bereits oben gefilterten) Mitgliedstabellen ohne Produktgruppierung.

| Ziel | Datenprodukt-Repräsentation |
| --- | --- |
| Snowflake Horizon | Jedes Produkt wird zu einer `SHARE` über die physischen Adressen seiner Mitgliedstabellen, eingebettet in ein internes `CREATE ORGANIZATION LISTING` — ein natives Horizon-Catalog-Datenprodukt. `publish=true` schaltet das Listing sofort live; andernfalls landet es als DRAFT. |
| BigQuery Dataplex | Jedes Produkt wird zu einem Analytics-Hub-Listing über `/v1/dataProducts`. |
| OpenMetadata | Jedes Produkt wird zu einer nativen `DataProduct`-Entität (`/api/v1/dataProducts`), mit domänenabgeleiteter Ownership. |
| DataHub | Jedes Produkt wird zu einer nativen `dataProduct`-Entität (`urn:li:dataProduct:...`) mit eigenen `dataProductProperties`-/Ownership-Aspekten. |
| Collibra | Jedes Produkt wird zu einem Asset eines `Data Product`-Community-Typs, verknüpft mit seinen Mitgliedstabellen über eine Beziehung `Data Product groups Table`. |
| Atlan | Veröffentlicht als benutzerdefinierte `DataProduct`-Typedef-Vermutung — Atlan hat keinen dokumentierten stabilen Typnamen für dieses Konzept, daher ist die Abbildung Best-effort. |
| Apache Atlas | Veröffentlicht als benutzerdefiniertes `provisa_data_product`-Typedef mit einer `provisa_data_product_members`-Beziehung — Atlas hat keinen nativen Datenprodukt-Entitätstyp. |
| OpenLineage | Keine erstklassige Entität — Mitgliedstabellen tragen eine benutzerdefinierte `provisa_data_product`-Facette, die das besitzende Produkt benennt. |
