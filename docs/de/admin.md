# Admin API

Die Admin API ist ein Strawberry-GraphQL-Endpunkt unter `POST /admin/graphql` (REQ-533). Sie erfordert eine Superuser- oder Admin-Rolle (REQ-125, REQ-060) und ist vom Daten-GraphQL-Endpunkt getrennt (REQ-533).

## Authentifizierung

Übergeben Sie Ihre Anmeldedaten im `Authorization`-Header über den Standard-Auth-Provider von Provisa (REQ-120):

```yaml
Authorization: Bearer <token>
```

Der Admin-Zugriff wird durch die einer Rolle zugewiesene Capability `admin` gesteuert (REQ-060, REQ-042).

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
