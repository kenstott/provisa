# Admin API

Die Admin API ist ein Strawberry-GraphQL-Endpunkt unter `POST /admin/graphql` (REQ-533). Sie erfordert eine Superuser- oder Admin-Rolle (REQ-125, REQ-060) und ist vom Daten-GraphQL-Endpunkt getrennt (REQ-533).

## Authentifizierung

Übergeben Sie Ihre Anmeldedaten im `Authorization`-Header über den Standard-Auth-Provider von Provisa (REQ-120):

```yaml
Authorization: Bearer <token>
```

Der Admin-Zugriff wird durch die einer Rolle zugewiesene Capability `admin` gesteuert (REQ-060, REQ-042).

### Personal Access Tokens

Ein Personal Access Token wird überall dort akzeptiert, wo ein Bearer-Token akzeptiert wird, auch an diesem Endpunkt. Ausstellen und Widerrufen sind Selbstbedienung — es ist das eigene Credential des Token-Inhabers und liegt deshalb im Profil des Benutzers in der Admin-Oberfläche, nicht unter einer Administrationsseite, direkt neben dem Verlassen einer Organisation und dem Löschen des Kontos. Ein Administrator stellt keine Tokens im Namen anderer aus. (REQ-1263)

| Route | Wirkung |
| ------- | -------- |
| `POST /auth/tokens` | Stellt ein Token für den Aufrufer aus. Body: `name`, optional `role_id`, `scopes`, `expires_in_days` (1–366). Die Antwort ist die einzige Stelle, an der das Geheimnis jemals erscheint |
| `GET /auth/tokens` | Die aktiven Tokens des Aufrufers in dieser Organisation — Anzeigepräfix, Name, Lebenszyklus-Zeitstempel und der Hash, der ein Token für den Widerruf identifiziert. Niemals ein funktionierendes Credential |
| `DELETE /auth/tokens/{token_hash}` | Widerruft eines der Tokens des Aufrufers. 404, wenn es ihm nicht gehört oder bereits widerrufen wurde |

Wird `role_id` weggelassen, löst das Token auf die Rolle auf, die sein Eigentümer innehat; wird eine benannt, engt sie das Token unter seinen Eigentümer ein. Der Widerruf geschieht auch implizit: Wird die Organisationsmitgliedschaft eines Benutzers entfernt, werden seine Tokens für diese Organisation widerrufen. Zum Credential selbst siehe [Sicherheitsmodell](security.md#personal-access-tokens).

## Capabilities

### Konfigurationsverwaltung

Die aktuell laufende Konfiguration herunterladen (REQ-164):

```http
GET /admin/config
```

Liefert die vollständige `config.yaml` als YAML-Datei. Eine neue Konfiguration hochladen (REQ-164):

```http
PUT /admin/config
```

Provisa validiert das YAML, lädt die Kataloge neu und regeneriert die Schemas (REQ-012, REQ-253). Kein Neustart erforderlich.

### Laufzeiteinstellungen

Laufzeit-Plattformeinstellungen lesen und schreiben, ohne die Konfigurationsdatei zu bearbeiten (REQ-165):

```http
GET  /admin/settings
PUT  /admin/settings
```

Die Einstellungsoberfläche umfasst die Umleitung großer Ergebnisse, Standard-Sampling und Zeilenlimit, Response-Cache-TTL, Namenskonvention, automatische FK-Erkennung für Relationships, Materialisierungsspeicher-DSN, Föderations-Engine-Speicher (`jvm_heap_gb`, `query_max_memory`, `query_max_memory_per_node`, `query_max_total_memory`, `fault_tolerant_execution`, `fault_tolerant_task_memory`, `exchange_spool_dir`) sowie die gesamte Tuning-Oberfläche der OpenTelemetry-Tracing-Pipeline (REQ-1082). Remote-GraphQL-Traversierungslimits sowie Warm-Tier-/Read-Cache-Einstellungen werden ebenfalls exponiert (REQ-1081, REQ-1083).

Sicherheitshaltung — `security.mode` (`standard` | `high`) — wird beim Neustart angewendet (REQ-1079):

```http
GET  /admin/security
PUT  /admin/security
```

KI-Modellzuweisungen, die Embedding-/Vektor-Modell-Registry und das NL-Rate-Limit — werden beim Neustart angewendet (REQ-1080):

```http
GET  /admin/ai-models
PUT  /admin/ai-models
```

Der Admin-Tab für Verschlüsselung leitet seine Anbieterliste live aus der Verschlüsselungs-Registry ab; nicht verfügbare Anbieter erscheinen, sind aber nicht auswählbar (REQ-1091).

`GET`/`HEAD /health` und `GET /setup/status` sind immer unauthentifiziert — sie umgehen die Anforderung `Authorization: Bearer` selbst dann, wenn ein Auth-Provider konfiguriert ist (REQ-539).

### Relationship-Editor

Relationships auflisten (REQ-166):

```graphql
query {
  relationships {
    id
    sourceTableId
    targetTableId
    sourceColumn
    targetColumn
    cardinality
    materialize
  }
}
```

Eine Relationship erstellen (REQ-019):

```graphql
mutation {
  upsertRelationship(input: {
    id: "orders-to-customers"
    sourceTableId: "orders"
    targetTableId: "customers"
    sourceColumn: "customer_id"
    targetColumn: "id"
    cardinality: "many_to_one"
  }) {
    success
  }
}
```

### KI-gestützte Relationship-Erkennung

Claude-gestützte FK-Analyse über REST auslösen (REQ-167, REQ-018):

```bash
curl -X POST http://localhost:8001/admin/discover/relationships \
  -H "Content-Type: application/json" \
  -d '{"scope": "domain", "domain_id": "sales"}'
```

Liefert FK-Kandidaten, nach Konfidenz sortiert. Einen Kandidaten annehmen:

```bash
curl -X POST http://localhost:8001/admin/discover/candidates/{id}/accept \
  -H "Content-Type: application/json" \
  -d '{"name": "orders_to_customers"}'
```

### Schema-Introspektion

Veröffentlichte Tabellen über alle Quellen hinweg durchsuchen (REQ-008):

```graphql
query {
  tables {
    id
    sourceId
    columns {
      columnName
      unmaskedTo
      writableBy
    }
  }
}
```

### Spaltenabhängigkeitsprüfung (REQ-1484)

Bevor Sie eine Tabellenänderung speichern, die den SQL-Alias einer Spalte umbenennt oder eine
Spalte löscht, fragen Sie ab, was noch darauf verweist:

```graphql
query {
  columnDependents(tableId: "42", renamed: ["order_total"], removed: ["legacy_code"]) {
    columnName
    dependents { kind name detail breaksOn }
  }
}
```

Das Umbenennen eines Alias bricht jedes Artefakt, das gegen den exponierten Namen geschrieben
wurde — Views, MVs, Metrik-Ausdrücke, RLS-Prädikate, DQ-Contracts. Das Löschen einer Spalte
bricht diese sowie die Artefakte, die den physischen `column_name` speichern: Relationships,
Glossar-Bindungen, Tag-Zuweisungen. `breaksOn` gibt an, welches. Die Tables-Seite führt dies
beim Speichern aus und zeigt das Ergebnis als beratenden Dialog. Siehe [Lineage](lineage.md)
dazu, was die Abfrage abdeckt und was nicht.

### View-Verwaltung

Eine materialisierte View registrieren (REQ-133, REQ-135):

```graphql
mutation {
  registerTable(input: {
    viewSql: "SELECT o.id, o.amount, c.name FROM orders o JOIN customers c ON o.customer_id = c.id"
    mvRefreshInterval: 300
    materialize: true
  }) {
    success
  }
}
```

Eine manuelle Aktualisierung auslösen (REQ-135):

```graphql
mutation {
  refreshMv(mvId: "orders-with-customers") {
    success
  }
}
```

### Registrierung von Graph-Quellen

Neo4j- und SPARQL-Quellen werden über REST-Endpunkte registriert (nicht über die GraphQL-Admin-API) (REQ-295, REQ-297):

**Neo4j:**

```bash
# 1. Register the Neo4j source
curl -X POST http://localhost:8001/admin/sources/neo4j \
  -H "Content-Type: application/json" \
  -d '{"source_id": "graph", "host": "neo4j", "port": 7474, "database": "neo4j"}'

# 2. Preview a Cypher query (validates scalar projections)
curl -X POST http://localhost:8001/admin/sources/neo4j/graph/preview \
  -H "Content-Type: application/json" \
  -d '{"cypher": "MATCH (p:Person) RETURN p.name AS name, p.age AS age"}'

# 3. Register a table (runs preview+validate automatically)
curl -X POST http://localhost:8001/admin/sources/neo4j/graph/tables \
  -H "Content-Type: application/json" \
  -d '{"table_name": "people", "cypher": "MATCH (p:Person) RETURN p.name AS name, p.age AS age", "ttl": 300}'
```

**SPARQL:**

```bash
# 1. Register the SPARQL source
curl -X POST http://localhost:8001/admin/sources/sparql \
  -H "Content-Type: application/json" \
  -d '{"source_id": "kg", "endpoint_url": "http://fuseki:3030/ds/sparql"}'

# 2. Register a table (probes endpoint and infers columns)
curl -X POST http://localhost:8001/admin/sources/sparql/kg/tables \
  -H "Content-Type: application/json" \
  -d '{"table_name": "products", "sparql_query": "SELECT ?name ?category WHERE { ?p a :Product ; :name ?name ; :category ?category . }", "ttl": 600}'
```

Nach der Registrierung erscheinen Tabellen im GraphQL-Schema und sind wie jede andere Quelle abfragbar (REQ-016).

## GraphiQL

Die Admin API wird mit GraphiQL unter `GET /admin/graphql` im Browser ausgeliefert (REQ-622). Nutzen Sie es, um das vollständige Admin-Schema interaktiv zu erkunden.

## Verwaltungsviews der Ops-Domäne (REQ-1386)

Acht SQL-Views werden bei jeder Installation in die integrierte `ops`-Domäne eingebracht. [tool-verified: `provisa/api/startup_seed.py:225-331` `_seed_ops_domain`] Sie exponieren das Query-Audit-Log als governte Tabellen — abfragbar über SQL (pgwire), GraphQL und Cypher unter denselben Domänenzugriffs-, RLS- und Maskierungsregeln wie jede geschäftliche Tabelle.

`org_admin` wird zum Zeitpunkt des Seedings als Steward der Ops-Domäne festgelegt, sodass die Domäne nie als Governance-Lücke in `stale_metadata` erscheint. [tool-verified: `startup_seed.py:326-331`]

| View | Was sie beantwortet |
| --- | --- |
| `usage_ranking` | Abfrageanzahl und eindeutige Nutzer pro registrierter Tabelle; Tabellen ohne Treffer erscheinen als Kandidaten für die Entfernung |
| `deprecated_usage` | Jeder Zugriff auf eine Tabelle oder Spalte mit dem Tag `deprecated` — die aktiven Konsumenten, die eine gefahrlose Entfernung verhindern |
| `pii_access` | Jeder Zugriff auf eine Tabelle oder Spalte mit dem Tag `pii`: wer hat abgefragt, unter welcher Rolle, über welche Oberfläche |
| `policy_denials` | Alle Zugriffsversuche, die die Governance abgelehnt hat (HTTP 401/403) |
| `surface_mix` | Tägliche Abfrageanzahl und eindeutige Nutzer pro Protokolloberfläche (SQL, GraphQL, Cypher, gRPC usw.) |
| `query_health` | Tägliche Fehleranzahl und durchschnittliche/maximale Latenz pro Oberfläche |
| `stale_metadata` | Tabellen und Spalten ohne Beschreibung; Domänen ohne Steward |
| `join_hotspots` | Am häufigsten gemeinsam abgefragte Tabellenpaare — Kandidaten für Materialisierung oder Caching |

Zwei Einschränkungen gelten derzeit. Die Granularität liegt auf Tabellenebene — das Audit-Log erfasst `table_ids`, nicht einzelne abgerufene Spalten. Der Abfragetext ist verschlüsselt (REQ-689) und in keiner der hier gezeigten Views enthalten; er ist nur über den autorisierten Admin-Entschlüsselungspfad zugänglich. [tool-verified: `_meta_views.py:148-162` — comment notes `query_text_enc` exclusion]

Eine Rolle benötigt Zugriff auf die `ops`-Domäne, damit diese Views sichtbar sind. Gewähren Sie ihn genauso wie den Zugriff auf jede andere Domäne.

```sql
-- Which tables have never been queried?
SELECT table_name, domain_id
FROM ops.usage_ranking
WHERE query_count = 0;

-- Who accessed PII-tagged data in the last 7 days?
SELECT user_id, role_id, source, pii_column, logged_at
FROM ops.pii_access
WHERE logged_at >= CURRENT_DATE - INTERVAL '7 days'
ORDER BY logged_at DESC;

-- Where does traffic originate by protocol?
SELECT source, day, query_count, distinct_users
FROM ops.surface_mix
ORDER BY day DESC, query_count DESC;
```

Dieselben Abfragen laufen als GraphQL oder Cypher über jeden governten Transport — pgwire, Arrow Flight oder Bolt. [inferred from governed-surface design]

## Reports-Viewer (REQ-1390)

Der Reports-Viewer befindet sich unter `/admin/reports`. Rollen ohne die Capability `observability` können ihn nicht erreichen.

Das linke Panel listet jede registrierte Tabelle in der `ops`-Domäne, sortiert nach Alias. [tool-verified: `ReportsTab.tsx:46-52` — filters `tables` to `domainId === "ops"`] Die acht eingebrachten Verwaltungsviews erscheinen dort automatisch. Klicken Sie auf einen Report, um ihn im governten Datenviewer rechts zu laden.

**Einen benutzerdefinierten Report hinzufügen.** Die Schaltfläche „Add report" öffnet einen Dialog. Geben Sie einen Namen, eine optionale Beschreibung und ein SELECT-Statement an. Beim Speichern wird die View als governte, abgeleitete Tabelle in der `ops`-Domäne registriert — katalogisiert, zugriffsgesteuert und über jede Oberfläche neben den eingebrachten Views abfragbar. [tool-verified: `ReportsTab.tsx:70-96` — `registerTable` called with `sourceId: DERIVED_SOURCE_ID, domainId: "ops"`]

**Löschen.** Das Papierkorb-Symbol erscheint nur bei benutzerdefinierten Reports. Eingebrachte Verwaltungsviews können über diese Oberfläche nicht gelöscht werden. [tool-verified: `ReportsTab.tsx:151` — `const custom = report.sourceId === DERIVED_SOURCE_ID` gates the delete button]

## Tabellenvorschau (REQ-1392)

Klappen Sie auf der Tables-Seite eine beliebige Tabellenzeile auf. Die Schaltfläche **Preview** öffnet ein Modal mit 90 % Breite und den live governten Daten der Tabelle. [tool-verified: `TablePreviewModal.tsx:24` — `size="90%"`; `GovernedTableViewer.tsx` is the underlying viewer]

Tabellen, die auf APIs mit erforderlichen Pfadparametern basieren, blockieren die Vorschau, bis diese Werte angegeben werden. Ein Inline-Formular sammelt jeden erforderlichen Parameter, bevor die erste Abfrage läuft; optionale Query-Parameter erscheinen im selben Formular. [tool-verified: `GovernedTableViewer.tsx:51-55, 153-155` — `requiredParamColumns` check; "paramsRequired" message shown when `activeParams == null`]

## Governter Datenviewer (REQ-1391)

Dieselbe Viewer-Komponente treibt sowohl das Vorschau-Modal als auch den Reports-Viewer an. Ihr Verhalten ist in beiden Kontexten identisch.

**Serverseitiges Paging.** Jede Seite ist ein eigenes governtes `SELECT *` mit `LIMIT 101 OFFSET n`. 100 Zeilen erscheinen pro Seite; die 101. zeigt an, ob weitere existieren. Der vollständige Datensatz wird nie in den Browser geladen. [tool-verified: `nativeParams.ts:72` — `LIMIT ${pageSize + 1} OFFSET ${page * pageSize}`; `types.ts:74` — `PAGE_SIZE = 100`]

**Pushed-Down-Filter und -Sortierungen.** Jede Spaltenüberschrift hat ein Filterfeld. Filterbegriffe werden zu `WHERE LOWER(CAST(col AS VARCHAR)) LIKE LOWER('%term%')`-Prädikaten; Sortierklicks erzeugen `ORDER BY`-Klauseln. Beide gehen an die Datenbank — ein Filter auf einer Tabelle mit einer Milliarde Zeilen durchsucht die Quelle, nicht die 100-Zeilen-Seite vor Ihnen. [tool-verified: `nativeParams.ts:53-70`]

**Mehrstufiges Group-by.** Das Layers-Symbol in jeder Spaltenüberschrift schaltet diese Spalte in die Gruppierung ein. Gruppenspalten führen die `ORDER BY`-Klausel an, sodass Gruppenmitglieder über Seitengrenzen hinweg auf derselben Seite wie ihre Überschrift landen. Primärschlüsselspalten werden als stabiler Tiebreaker angehängt. [tool-verified: `nativeParams.ts:61-70` — group columns first, then explicit sorts, then PKs] Gruppenüberschrift-Zeilen sind einklappbar; das Einklappen verbirgt Mitglieder, ohne eine neue Abfrage auszulösen. [tool-verified: `useResultsGrid.ts:150-171` — `collapsedGroups` set gates the `build()` recursion]

**Persistente Auswahl.** Filter-, Sortier- und Group-by-Einstellungen werden unter `provisa.grid.table:<domain>.<table>` im `localStorage` gespeichert und beim nächsten Besuch wiederhergestellt. [tool-verified: `useResultsGrid.ts:95-98`, `GovernedTableViewer.tsx:66`]

**Export.** Laden Sie die aktuelle Seite als CSV herunter oder kopieren Sie sie als tabulatorgetrennten Text in die Zwischenablage. Der Export deckt nur die sichtbare Seite ab. [tool-verified: `useResultsGrid.ts:247-274` — both handlers iterate `displayRows`, which in server-paged mode is the current page]
