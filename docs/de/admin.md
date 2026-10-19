# Admin API

Die Admin API ist ein Strawberry-GraphQL-Endpunkt unter `POST /admin/graphql`. Sie erfordert eine Superuser- oder Admin-Rolle und ist vom Daten-GraphQL-Endpunkt getrennt.

## Authentifizierung {: #authentication }

Übergeben Sie Ihre Anmeldedaten im `Authorization`-Header über den Standard-Auth-Provider von Provisa:

```yaml
Authorization: Bearer <token>
```

Der Admin-Zugriff wird durch die einer Rolle zugewiesene Capability `admin` gesteuert.

### Personal Access Tokens

Ein Personal Access Token wird überall akzeptiert, wo ein Bearer-Token akzeptiert wird, auch an diesem Endpunkt. Sie stellen Ihre eigenen aus und widerrufen sie, denn ein Token ist die eigene Anmeldung seines Inhabers. Deshalb liegt es in Ihrem Profil in der Admin-Oberfläche, neben dem Verlassen einer Organisation und dem Löschen Ihres Kontos, und nicht unter einer Administrationsseite. Ein Administrator stellt nie ein Token im Namen eines anderen aus.

| Route | Wirkung |
| ------- | -------- |
| `POST /auth/tokens` | Stellt ein Token für den Aufrufer aus. Body: `name`, optional `role_id`, `scopes`, `expires_in_days` (1–366). Die Antwort ist die einzige Stelle, an der das Geheimnis jemals erscheint |
| `GET /auth/tokens` | Die aktiven Tokens des Aufrufers in dieser Organisation — Anzeigepräfix, Name, Lebenszyklus-Zeitstempel und der Hash, der ein Token für den Widerruf identifiziert. Niemals ein funktionierendes Credential |
| `DELETE /auth/tokens/{token_hash}` | Widerruft eines der Tokens des Aufrufers. 404, wenn es ihm nicht gehört oder bereits widerrufen wurde |

Wird `role_id` weggelassen, löst das Token auf die Rolle auf, die sein Eigentümer innehat; wird eine benannt, engt sie das Token unter seinen Eigentümer ein. Der Widerruf geschieht auch implizit: Wird die Organisationsmitgliedschaft eines Benutzers entfernt, werden seine Tokens für diese Organisation widerrufen. Zum Credential selbst siehe [Sicherheitsmodell](security.md#personal-access-tokens).

## Capabilities

### Konfigurationsverwaltung {: #config-management }

Die aktuell laufende Konfiguration herunterladen:

```http
GET /admin/config
```

Liefert die vollständige `config.yaml` als YAML-Datei. Eine neue Konfiguration hochladen:

```http
PUT /admin/config
```

Provisa validiert das YAML, lädt die Kataloge neu und regeneriert die Schemas. Kein Neustart erforderlich.

### Laufzeiteinstellungen {: #runtime-settings }

Laufzeit-Plattformeinstellungen lesen und schreiben, ohne die Konfigurationsdatei zu bearbeiten:

```http
GET  /admin/settings
PUT  /admin/settings
```

Die Einstellungsoberfläche umfasst die Umleitung großer Ergebnisse, Standard-Sampling und Zeilenlimit, Response-Cache-TTL, Namenskonvention, automatische FK-Erkennung für Relationships, Materialisierungsspeicher-DSN, Föderations-Engine-Speicher (`jvm_heap_gb`, `query_max_memory`, `query_max_memory_per_node`, `query_max_total_memory`, `fault_tolerant_execution`, `fault_tolerant_task_memory`, `exchange_spool_dir`) sowie die gesamte Tuning-Oberfläche der OpenTelemetry-Tracing-Pipeline. Remote-GraphQL-Traversierungslimits sowie Warm-Tier-/Read-Cache-Einstellungen werden ebenfalls exponiert.

Die Karte **Connector-Server** benennt für jeden Connector mit einem mitgelieferten Server (File Crawler, SharePoint, Salesforce, ServiceNow, Cloud Inventory, Splunk, AskAmerica) einen Ordner mit einem für diesen Host erstellten Server-Bundle (`connectors.<connector>.bundle_dir`, Umgebungsvariable `PROVISA_PGWIRE_BUNDLE_<CONNECTOR>`). Ist er leer, wird der Server des Connectors aus dem gepinnten `kenstott/calcite`-Release abgerufen. Ist er gesetzt, wird der Ordner so verwendet, wie er ist: Sein Launcher muss unter `bin/pgwire-<connector>` liegen; fehlt er, wird der Start des Connector-Servers mit einem Fehler abgebrochen, der die Einstellung nennt — das Release wird nie an seiner Stelle abgerufen. Die Einstellung wird bei jedem Start eines Connector-Servers gelesen.

Die Sicherheitsstufe `security.mode` (`standard` | `high`) wird beim Neustart angewendet:

```http
GET  /admin/security
PUT  /admin/security
```

KI-Modellzuweisungen, die Registry der Embedding- und Vektormodelle und das NL-Ratenlimit wirken ab der nächsten Anfrage, ohne Neustart:

```http
GET  /admin/ai-models
PUT  /admin/ai-models
```

Der Admin-Tab für Verschlüsselung leitet seine Anbieterliste live aus der Verschlüsselungs-Registry ab; nicht verfügbare Anbieter erscheinen, sind aber nicht auswählbar.

`GET`/`HEAD /health` und `GET /setup/status` verlangen nie eine Authentifizierung. Sie umgehen die Anforderung `Authorization: Bearer` auch dann, wenn ein Auth-Provider konfiguriert ist.

### Föderations-Engine {: #federation-engine }

Lesen oder ändern Sie, welche Engine das Deployment verwendet:

```http
GET  /admin/federation-engine
PUT  /admin/federation-engine
```

`GET` liefert den Schlüssel der aktiven Engine und die Konfigurationsfelder, die sie braucht. `PUT` akzeptiert einen Body mit `engine` (dem Schlüssel) und beliebigen engine-spezifischen Feldern; die Auswahl wird in der Plattformkonfiguration gespeichert und bindet beim nächsten Neustart des Dienstes.

### Relationship-Editor

Relationships auflisten:

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

Eine Relationship erstellen. Eine bereits vorhandene ID wird abgelehnt; eine Relationship wird mit `updateRelationship` geändert, das dieselbe Eingabe nimmt:

```graphql
mutation {
  createRelationship(input: {
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

Eine Junction-gestützte Beziehung deklarieren:

```graphql
mutation {
  createRelationship(input: {
    id: "pets-bonded-pair"
    sourceTableId: "pets"
    targetTableId: "pets"
    sourceColumn: "id"
    targetColumn: "id"
    cardinality: "one-to-many"
    viaTable: "pet_companions"
    viaSourceColumn: "pet_id"
    viaTargetColumn: "companion_pet_id"
    viaTypeColumn: "companion_type"
    viaTypeValue: "bonded pair"
    viaLabelSource: "column"
  }) {
    success
  }
}
```

Eine Assoziationstabelle deklarieren Sie als Kante; Provisa entdeckt nie eine. `viaTable` nennt eine registrierte Tabelle. Ihre zwei Schlüsselspalten tragen die Kante, und jede weitere Spalte wird ein Attribut der Beziehung, filterbar wie jedes Feld. `viaTypeColumn` / `viaTypeValue` teilen eine Verknüpfungstabelle in mehrere Kantentypen. Zum Beispiel ergeben Zeilen von `pet_companions` mit `companion_type` `bonded pair`, `littermate` oder `shares enclosure` drei verschiedene Beziehungen über dasselbe Tabellenpaar.

`viaLabelSource` legt fest, woher der exponierte Name kommt, und Cypher erhält jede Form in Großbuchstaben mit Unterstrichen. `column` nutzt `viaTypeValue` (`BONDED_PAIR`), `table` den eigenen Namen der Verknüpfungstabelle (`PET_COMPANIONS`) und `fixed` den deklarierten `alias`. Eine so deklarierte Verknüpfungstabelle ist eine Kante. Provisa nimmt sie aus den Knotenlabels, sodass sie in der Graph-UI nie als Knoten-Pill erscheint.

### KI-gestützte Relationship-Erkennung {: #ai-relationship-discovery }

Claude über REST nach FK-Kandidaten fragen:

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

### Schema-Introspektion {: #schema-introspection }

Veröffentlichte Tabellen über alle Quellen hinweg durchsuchen:

```graphql
query {
  tables {
    id
    sourceId
    columns {
      columnName
      unmaskedTo
    }
    writeGrants {
      role
      operation
      columns
      rows
      check
      presets { column source name value }
    }
  }
}
```

### Spaltenabhängigkeitsprüfung {: #column-dependency-check }

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

Das Umbenennen eines Alias bricht jedes Artefakt, das gegen den exponierten Namen geschrieben ist: Views, MVs, Metrik-Ausdrücke, RLS-Prädikate und DQ-Verträge. Das Entfernen einer Spalte bricht diese ebenfalls, dazu die Artefakte, die den physischen `column_name` speichern, also Beziehungen, Glossar-Bindungen und Tag-Zuweisungen. `breaksOn` sagt, welche. Die Seite Tabellen führt diese Prüfung beim Speichern aus und zeigt das Ergebnis als Hinweisdialog. [Lineage](lineage.md) erklärt, was die Abfrage abdeckt und was nicht.

### View-Verwaltung {: #view-management }

Eine materialisierte View registrieren:

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

Eine manuelle Aktualisierung auslösen:

```graphql
mutation {
  refreshMv(mvId: "orders-with-customers") {
    success
  }
}
```

### Registrierung von Graph-Quellen {: #graph-source-registration }

Neo4j- und SPARQL-Quellen werden über REST-Endpunkte registriert (nicht über die GraphQL-Admin-API):

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

Nach der Registrierung erscheinen Tabellen im GraphQL-Schema und sind wie jede andere Quelle abfragbar.

### Hasura-/DDN-Import

Wandeln Sie ein bestehendes Hasura-v2- oder Hasura-DDN-Projekt über die Admin-UI oder die API in Provisa-Konfiguration um, ohne dass etwas landet, bevor Sie es freigeben.

```http
POST /admin/import/hasura/preview
POST /admin/import/hasura/apply
```

**Preview** konvertiert das hochgeladene Archiv und liefert das vorgeschlagene `config_yaml`, eine Liste von Warnungen und eine Zusammenfassung des Gefundenen (Anzahl von Quellen, Domänen, Tabellen, Spalten, Rollen, Relationships und RLS-Regeln). Nichts wird in die Mandantendatenbank geschrieben. Request-Body:

```json
{
  "filename": "my-hasura-project.zip",
  "content_b64": "<base64-encoded archive>",
  "flavor": "auto",
  "domain_map": {"public": "sales"},
  "source_overrides": {}
}
```

`flavor` ist `"auto"` (aus der Archivstruktur erkannt), `"hasura_v2"` oder `"ddn"`.

**Apply** lädt das geprüfte YAML, auf Wunsch bearbeitet, in die handelnde Org. Es nutzt denselben Hot-Reload-Weg wie `PUT /admin/config`. Request-Body: `{"config_yaml": "<yaml string>"}`.

Preview cachet das konvertierte YAML nie serverseitig; Apply nimmt das YAML, das Sie liefern, sodass genau das angewendet wird, was geprüft wurde.

### Apache-Ossie-Austausch {: #apache-ossie-interchange }

Provisa arbeitet mit Apache Ossie (incubating) als Import-/Export-Grenze zusammen.

```http
GET  /admin/ossie
POST /admin/ossie/import
```

**Export** (`GET /admin/ossie`) baut das Ossie-YAML-Dokument bei jeder Anfrage aus dem lebenden gesteuerten Modell. Nichts wird zwischengespeichert, also ist das Dokument nie veraltet. Die Antwort ist `text/yaml` mit dem Header `Content-Disposition: attachment`. Das Dokument ist ein Modell mit `version`, `name`, `datasets`, `relationships` und `metrics` an der Wurzel. Tabellen werden zu `dataset`-Objekten, Spalten zu `field`-Objekten, und Beziehungen werden auf Ossie-`relationship`-Objekte abgebildet. Ein Modell ohne Tabelle wird mit `422` abgelehnt.

**Import** (`POST /admin/ossie/import`) nimmt ein Ossie-Dokument in YAML oder JSON an und erkennt das Format selbst. Es parst das Dokument und gibt vorgeschlagene Registrierungen von Tabellen, Beziehungen und Metriken als JSON-Objekt zurück. Registriert wird nichts. Es liest nur die flache Dokumentform, und ein Dokument, das sein Modell in `semantic_model` einhüllt, wird namentlich abgelehnt. In der Admin-Oberfläche können Sie im Prüfbildschirm Vorschläge annehmen oder kürzen, bevor sich etwas ändert.

### Objektspeicher {: #object-storage }

Lesen oder konfigurieren Sie den Materialisierungsspeicher der Org:

```http
GET  /admin/org-storage
PUT  /admin/org-storage
```

`GET` meldet, wie viel des Plattform-Speicherkontingents die Org nutzt. `PUT` registriert die eigene Speicher-DSN der Org (im Ruhezustand verschlüsselt; nie von GET zurückgegeben). Einmal gesetzt, landen die Materialisierungen der Org in ihrem eigenen Bucket und werden nicht mehr auf das Plattformkontingent angerechnet. `storage_url: null` zu senden löscht sie und bewegt die Org zurück zum Plattformspeicher.

### Org-Verschlüsselung {: #org-encryption }

Setzen oder rotieren Sie den Verschlüsselungsschlüssel der Org für ruhende Daten:

```http
GET  /admin/org-encryption
PUT  /admin/org-encryption
```

`GET` liefert Fingerabdruck, ID und Herkunft des Schlüssels und nie Schlüsselmaterial. `PUT` setzt oder rotiert den Schlüssel. Geben Sie `key_b64` (32 Rohbytes, base64-kodiert) an, um einen eigenen Schlüssel mitzubringen, oder lassen Sie es weg, dann erzeugt Provisa einen. Löschen gibt es nicht, denn das Stilllegen des letzten Schlüssels machte jede von ihm umhüllte Nutzlast unlesbar.

## GraphiQL

Die Admin API wird mit GraphiQL unter `GET /admin/graphql` im Browser ausgeliefert. Damit durchsuchen Sie das vollständige Admin-Schema und probieren Aufrufe aus.

## Verwaltungsviews der Ops-Domäne {: #ops-domain-management-views }

Jede Installation legt acht SQL-Views in der eingebauten Domäne `ops` an. Sie stellen das Abfrage-Audit-Log als gesteuerte Tabellen bereit. Sie fragen sie über SQL (pgwire), GraphQL und Cypher ab, unter denselben Regeln für Domänenzugriff, RLS und Maskierung wie jede Geschäftstabelle.

`org_admin` wird zum Zeitpunkt des Seedings als Steward der Ops-Domäne festgelegt, sodass die Domäne nie als Governance-Lücke in `stale_metadata` erscheint.

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

Die Granularität endet bei der Tabelle. Das Audit-Log zeichnet `table_ids` auf, die einzelnen gelesenen Spalten nicht. Abfragetext ist verschlüsselt und in keiner dieser Views enthalten; nur der autorisierte Admin-Entschlüsselungsweg kann ihn lesen.

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

Da die Views gesteuerte Tabellen sind, können Sie dieselben Abfragen in GraphQL oder Cypher schreiben.

## Reports-Viewer

Der Reports-Viewer befindet sich unter `/admin/reports`. Rollen ohne die Capability `observability` können ihn nicht erreichen.

Das linke Panel listet jede registrierte Tabelle in der `ops`-Domäne, sortiert nach Alias. Die acht eingebrachten Verwaltungsviews erscheinen dort automatisch. Klicken Sie auf einen Report, um ihn im governten Datenviewer rechts zu laden.

**Einen benutzerdefinierten Report hinzufügen.** Die Schaltfläche „Add report“ öffnet einen Dialog, in dem Sie einen Namen, eine optionale Beschreibung und eine SELECT-Anweisung angeben. Beim Speichern wird die View als gesteuerte abgeleitete Tabelle in der Domäne `ops` registriert. Sie ist katalogisiert, zugriffsgesteuert und über jede Oberfläche abfragbar, neben den vorinstallierten Views.

**Löschen.** Das Papierkorb-Symbol erscheint nur bei benutzerdefinierten Reports. Eingebrachte Verwaltungsviews können über diese Oberfläche nicht gelöscht werden.

## Tabellenvorschau {: #table-preview }

Klappen Sie auf der Tables-Seite eine beliebige Tabellenzeile auf. Die Schaltfläche **Preview** öffnet ein Modal mit 90 % Breite und den live governten Daten der Tabelle.

Tabellen, die auf APIs mit erforderlichen Pfadparametern basieren, blockieren die Vorschau, bis diese Werte angegeben werden. Ein Inline-Formular sammelt jeden erforderlichen Parameter, bevor die erste Abfrage läuft; optionale Query-Parameter erscheinen im selben Formular.

## Governter Datenviewer {: #governed-data-viewer }

Dieselbe Viewer-Komponente treibt sowohl das Vorschau-Modal als auch den Reports-Viewer an. Ihr Verhalten ist in beiden Kontexten identisch.

**Serverseitiges Paging.** Jede Seite ist ein eigenes governtes `SELECT *` mit `LIMIT 101 OFFSET n`. 100 Zeilen erscheinen pro Seite; die 101. zeigt an, ob weitere existieren. Der vollständige Datensatz wird nie in den Browser geladen.

**Pushed-Down-Filter und -Sortierungen.** Jeder Spaltenkopf hat ein Filterfeld. Ein Filterbegriff wird zum Prädikat `WHERE LOWER(CAST(col AS VARCHAR)) LIKE LOWER('%term%')`, und ein Klick zum Sortieren erzeugt eine `ORDER BY`-Klausel. Beides läuft in der Datenbank. Ein Filter auf einer Tabelle mit einer Milliarde Zeilen durchsucht die Quelle und ist nie auf die 100-Zeilen-Seite vor Ihnen beschränkt.

**Mehrstufiges Group-by.** Das Layers-Symbol in jeder Spaltenüberschrift schaltet diese Spalte in die Gruppierung ein. Gruppenspalten führen die `ORDER BY`-Klausel an, sodass Gruppenmitglieder über Seitengrenzen hinweg auf derselben Seite wie ihre Überschrift landen. Primärschlüsselspalten werden als stabiler Tiebreaker angehängt. Gruppenüberschrift-Zeilen sind einklappbar; das Einklappen verbirgt Mitglieder, ohne eine neue Abfrage auszulösen.

**Persistente Auswahl.** Filter-, Sortier- und Group-by-Einstellungen werden unter `provisa.grid.table:<domain>.<table>` im `localStorage` gespeichert und beim nächsten Besuch wiederhergestellt.

**Export.** Laden Sie die aktuelle Seite als CSV herunter oder kopieren Sie sie als tabulatorgetrennten Text in die Zwischenablage. Der Export deckt nur die sichtbare Seite ab.
