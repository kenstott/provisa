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

Laden Sie die aktuell laufende Konfiguration herunter (REQ-164):

```http
GET /admin/config
```

Gibt die vollständige `config.yaml` als YAML-Datei zurück. Laden Sie eine neue Konfiguration hoch (REQ-164):

```http
PUT /admin/config
```

Provisa validiert das YAML, lädt die Kataloge neu und generiert die Schemas neu (REQ-012, REQ-253). Kein Neustart erforderlich.

### Laufzeiteinstellungen

Lesen und schreiben Sie Laufzeit-Plattformeinstellungen, ohne die Konfigurationsdatei zu bearbeiten (REQ-165):

```http
GET  /admin/settings
PUT  /admin/settings
```

Die Einstellungsoberfläche umfasst die Umleitung großer Ergebnisse, das Standard-Sampling und das Zeilenlimit, die TTL des Antwort-Caches, die Namenskonvention, das automatische Nachverfolgen von Fremdschlüssel-Beziehungen, den DSN des Materialisierungsspeichers, den Arbeitsspeicher der Federation-Engine (`jvm_heap_gb`, `query_max_memory`, `query_max_memory_per_node`, `query_max_total_memory`, `fault_tolerant_execution`, `fault_tolerant_task_memory`, `exchange_spool_dir`) sowie die gesamte Tuning-Oberfläche der OpenTelemetry-Tracing-Pipeline (REQ-1082). Auch die Limits für den entfernten GraphQL-Traversal sowie die Einstellungen für Warm-Tier/Lese-Cache werden bereitgestellt (REQ-1081, REQ-1083).

Sicherheitsstatus — `security.mode` (`standard` | `high`) — wird beim Neustart angewendet (REQ-1079):

```http
GET  /admin/security
PUT  /admin/security
```

KI-Modellzuweisungen, die Registry der Embedding-/Vektor-Modelle und das NL-Ratenlimit — werden beim Neustart angewendet (REQ-1080):

```http
GET  /admin/ai-models
PUT  /admin/ai-models
```

Der Verschlüsselungs-Tab im Admin-Bereich leitet seine Anbieterliste live aus der Verschlüsselungs-Registry ab; nicht verfügbare Anbieter werden angezeigt, sind aber nicht auswählbar (REQ-1091).

`GET`/`HEAD /health` und `GET /setup/status` sind immer unauthentifiziert erreichbar — sie umgehen die Anforderung `Authorization: Bearer` auch dann, wenn ein Auth-Provider konfiguriert ist (REQ-539).

### Beziehungs-Editor

Beziehungen auflisten (REQ-166):

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

Eine Beziehung erstellen (REQ-019):

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

### KI-gestützte Beziehungserkennung

Lösen Sie die Claude-gestützte Fremdschlüsselanalyse über REST aus (REQ-167, REQ-018):

```bash
curl -X POST http://localhost:8001/admin/discover/relationships \
  -H "Content-Type: application/json" \
  -d '{"scope": "domain", "domain_id": "sales"}'
```

Gibt Fremdschlüssel-Kandidaten sortiert nach Konfidenz zurück. Einen Kandidaten akzeptieren:

```bash
curl -X POST http://localhost:8001/admin/discover/candidates/{id}/accept \
  -H "Content-Type: application/json" \
  -d '{"name": "orders_to_customers"}'
```

### Schema-Introspektion

Durchsuchen Sie veröffentlichte Tabellen über alle Quellen hinweg (REQ-008):

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

### Sichtenverwaltung

Registrieren Sie eine materialisierte Sicht (REQ-133, REQ-135):

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

Nach der Registrierung erscheinen die Tabellen im GraphQL-Schema und sind wie jede andere Quelle abfragbar (REQ-016).

## GraphiQL

Die Admin API liefert GraphiQL unter `GET /admin/graphql` im Browser mit (REQ-622). Nutzen Sie es, um das vollständige Admin-Schema interaktiv zu erkunden.

## Verwaltungssichten der Ops-Domäne (REQ-1386)

Bei jeder Installation werden acht SQL-Sichten in die eingebaute Domäne `ops` eingespielt. [tool-verified: `provisa/api/startup_seed.py:225-331` `_seed_ops_domain`] Sie legen das Query-Audit-Log als regierte Tabellen offen — abfragbar über SQL (pgwire), GraphQL und Cypher, unter denselben Domänenzugriffs-, RLS- und Maskierungsregeln wie jede Geschäftstabelle.

`org_admin` wird beim Seeding als Steward der Ops-Domäne eingesetzt, damit die Domäne nie als Governance-Lücke in `stale_metadata` auftaucht. [tool-verified: `startup_seed.py:326-331`]

| Sicht | Welche Frage sie beantwortet |
| --- | --- |
| `usage_ranking` | Abfragezahl und eindeutige Benutzer je registrierter Tabelle; Tabellen ohne Treffer treten als Kandidaten zur Abkündigung hervor |
| `deprecated_usage` | Jeder Zugriff auf eine Tabelle oder Spalte mit dem Tag `deprecated` — die aktiven Konsumenten, die eine gefahrlose Entfernung blockieren |
| `pii_access` | Jeder Zugriff auf eine Tabelle oder Spalte mit dem Tag `pii`: wer abgefragt hat, unter welcher Rolle, über welche Oberfläche |
| `policy_denials` | Alle Zugriffsversuche, die die Governance abgelehnt hat (HTTP 401/403) |
| `surface_mix` | Tägliche Abfragezahl und eindeutige Benutzer je Protokolloberfläche (SQL, GraphQL, Cypher, gRPC usw.) |
| `query_health` | Tägliche Fehlerzahl sowie durchschnittliche und maximale Latenz je Oberfläche |
| `stale_metadata` | Tabellen und Spalten ohne Beschreibung; Domänen ohne Steward |
| `join_hotspots` | Am häufigsten gemeinsam abgefragte Tabellenpaare — Kandidaten für Materialisierung oder Caching |

Heute gelten zwei Einschränkungen. Die Granularität liegt auf Tabellenebene — das Audit-Log erfasst `table_ids`, nicht die einzeln abgerufenen Spalten. Der Abfragetext ist verschlüsselt (REQ-689) und aus jeder Sicht hier ausgeschlossen; er ist nur über den autorisierten Admin-Entschlüsselungspfad zugänglich. [tool-verified: `_meta_views.py:148-162` — comment notes `query_text_enc` exclusion]

Eine Rolle benötigt Zugriff auf die Domäne `ops`, bevor diese Sichten sichtbar werden. Erteilen Sie ihn genauso wie den Zugriff auf jede andere Domäne.

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

Dieselben Abfragen laufen als GraphQL oder Cypher über jeden regierten Transport — pgwire, Arrow Flight oder Bolt. [inferred from governed-surface design]

## Berichtsansicht (REQ-1390)

Die Berichtsansicht liegt unter `/admin/reports`. Rollen ohne die Capability `observability` erreichen sie nicht.

Die linke Leiste listet jede registrierte Tabelle der Domäne `ops`, nach Alias sortiert. [tool-verified: `ReportsTab.tsx:46-52` — filters `tables` to `domainId === "ops"`] Die acht eingespielten Verwaltungssichten erscheinen dort automatisch. Ein Klick auf einen Bericht lädt ihn rechts im regierten Datenbetrachter.

**Einen eigenen Bericht hinzufügen.** Die Schaltfläche „Bericht hinzufügen" öffnet einen Dialog. Geben Sie einen Namen, optional eine Beschreibung und eine SELECT-Anweisung an. Beim Speichern wird die Sicht als regierte abgeleitete Tabelle in der Domäne `ops` registriert — katalogisiert, zugriffskontrolliert und über jede Oberfläche abfragbar, neben den eingespielten Sichten. [tool-verified: `ReportsTab.tsx:70-96` — `registerTable` called with `sourceId: DERIVED_SOURCE_ID, domainId: "ops"`]

**Löschen.** Das Papierkorbsymbol erscheint nur bei eigenen Berichten. Eingespielte Verwaltungssichten lassen sich über diese Oberfläche nicht löschen. [tool-verified: `ReportsTab.tsx:151` — `const custom = report.sourceId === DERIVED_SOURCE_ID` gates the delete button]

## Tabellenvorschau (REQ-1392)

Klappen Sie auf der Tabellenseite eine beliebige Tabellenzeile auf. Die Schaltfläche **Vorschau** öffnet ein Modal mit 90 % Breite und den regierten Livedaten der Tabelle. [tool-verified: `TablePreviewModal.tsx:24` — `size="90%"`; `GovernedTableViewer.tsx` is the underlying viewer]

Tabellen, die auf APIs mit erforderlichen Pfadparametern beruhen, sperren die Vorschau, bis diese Werte vorliegen. Ein eingebettetes Formular erfasst jeden erforderlichen Parameter, bevor die erste Abfrage läuft; optionale Query-Parameter erscheinen im selben Formular. [tool-verified: `GovernedTableViewer.tsx:51-55, 153-155` — `requiredParamColumns` check; "paramsRequired" message shown when `activeParams == null`]

## Regierter Datenbetrachter (REQ-1391)

Dieselbe Betrachterkomponente treibt das Vorschau-Modal und die Berichtsansicht an. Ihr Verhalten ist in beiden Kontexten identisch.

**Serverseitiges Blättern.** Jede Seite ist ein eigenes regiertes `SELECT *` mit `LIMIT 101 OFFSET n`. Pro Seite erscheinen 100 Zeilen; die 101. zeigt an, ob es weitere gibt. Der vollständige Datenbestand wird nie in den Browser geladen. [tool-verified: `nativeParams.ts:72` — `LIMIT ${pageSize + 1} OFFSET ${page * pageSize}`; `types.ts:74` — `PAGE_SIZE = 100`]

**Heruntergedrückte Filter und Sortierungen.** Jede Spaltenüberschrift hat ein Filterfeld. Filterbegriffe werden zu Prädikaten `WHERE LOWER(CAST(col AS VARCHAR)) LIKE LOWER('%term%')`; Sortierklicks erzeugen `ORDER BY`-Klauseln. Beides geht an die Datenbank — ein Filter auf einer Tabelle mit einer Milliarde Zeilen durchsucht die Quelle, nicht die 100 Zeilen vor Ihnen. [tool-verified: `nativeParams.ts:53-70`]

**Mehrstufiges Gruppieren.** Das Ebenen-Symbol in einer Spaltenüberschrift nimmt diese Spalte in die Gruppierung auf. Gruppenspalten stehen im `ORDER BY` vorn, damit Gruppenmitglieder über Seitengrenzen hinweg auf derselben Seite wie ihre Kopfzeile landen. Primärschlüsselspalten werden als stabiler Gleichstandsbrecher angehängt. [tool-verified: `nativeParams.ts:61-70` — group columns first, then explicit sorts, then PKs] Gruppenkopfzeilen lassen sich einklappen; das Einklappen blendet Mitglieder aus, ohne eine neue Abfrage abzusetzen. [tool-verified: `useResultsGrid.ts:150-171` — `collapsedGroups` set gates the `build()` recursion]

**Dauerhafte Einstellungen.** Filter-, Sortier- und Gruppierungseinstellungen werden im `localStorage` unter `provisa.grid.table:<domain>.<table>` gespeichert und beim nächsten Besuch wiederhergestellt. [tool-verified: `useResultsGrid.ts:95-98`, `GovernedTableViewer.tsx:66`]

**Export.** Laden Sie die aktuelle Seite als CSV herunter oder kopieren Sie sie als tabulatorgetrennten Text in die Zwischenablage. Der Export umfasst nur die sichtbare Seite. [tool-verified: `useResultsGrid.ts:247-274` — both handlers iterate `displayRows`, which in server-paged mode is the current page]
