# Admin API

L'Admin API è un endpoint Strawberry GraphQL disponibile su `POST /admin/graphql` (REQ-533). Richiede un ruolo superuser o admin (REQ-125, REQ-060) ed è distinta dall'endpoint GraphQL dei dati (REQ-533).

## Autenticazione

Passare le credenziali nell'header `Authorization` utilizzando il provider di autenticazione standard di Provisa (REQ-120):
```
Authorization: Bearer <token>
```

L'accesso admin è governato dalla capacità `admin` assegnata a un ruolo (REQ-060, REQ-042).

## Capacità

### Gestione della configurazione

Scaricare la configurazione attualmente in esecuzione (REQ-164):
```
GET /admin/config
```

Restituisce il file `config.yaml` completo in formato YAML. Caricare una nuova configurazione (REQ-164):
```
PUT /admin/config
```

Provisa convalida lo YAML, ricarica i cataloghi e rigenera gli schemi (REQ-012, REQ-253). Non è richiesto alcun riavvio.

### Impostazioni di runtime

Leggere e scrivere le impostazioni della piattaforma a runtime senza modificare il file di configurazione (REQ-165):
```
GET  /admin/settings
PUT  /admin/settings
```

La superficie delle impostazioni copre il reindirizzamento dei risultati di grandi dimensioni, il campionamento predefinito e il limite di righe, il TTL della cache delle risposte, la convenzione di denominazione, il tracciamento automatico delle chiavi esterne delle relazioni, il DSN del datastore di materializzazione, la memoria del motore di federazione (`jvm_heap_gb`, `query_max_memory`, `query_max_memory_per_node`, `query_max_total_memory`, `fault_tolerant_execution`, `fault_tolerant_task_memory`, `exchange_spool_dir`) e l'intera superficie di ottimizzazione della pipeline di tracciamento OpenTelemetry (REQ-1082). Sono inoltre esposti i limiti di attraversamento GraphQL remoto e le impostazioni di warm-tier/cache di lettura (REQ-1081, REQ-1083).

Postura di sicurezza — `security.mode` (`standard` | `high`) — applicata al riavvio (REQ-1079):
```
GET  /admin/security
PUT  /admin/security
```

Assegnazioni dei modelli IA, registro dei modelli di embedding/vettoriali e limite di frequenza NL — applicati al riavvio (REQ-1080):
```
GET  /admin/ai-models
PUT  /admin/ai-models
```

La scheda di crittografia dell'admin deriva il proprio elenco di provider in tempo reale dal registro di crittografia; i provider non disponibili vengono visualizzati ma non sono selezionabili (REQ-1091).

`GET`/`HEAD /health` e `GET /setup/status` non richiedono mai autenticazione — bypassano il requisito `Authorization: Bearer` anche quando è configurato un provider di autenticazione (REQ-539).

### Editor delle relazioni

Elencare le relazioni (REQ-166):
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

Creare una relazione (REQ-019):
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

### Individuazione delle relazioni con IA

Avviare l'analisi delle chiavi esterne basata su Claude tramite REST (REQ-167, REQ-018):

```bash
curl -X POST http://localhost:8001/admin/discover/relationships \
  -H "Content-Type: application/json" \
  -d '{"scope": "domain", "domain_id": "sales"}'
```

Restituisce i candidati chiave esterna classificati per livello di confidenza. Accettare un candidato:

```bash
curl -X POST http://localhost:8001/admin/discover/candidates/{id}/accept \
  -H "Content-Type: application/json" \
  -d '{"name": "orders_to_customers"}'
```

### Introspezione dello schema

Sfogliare le tabelle pubblicate in tutte le origini (REQ-008):
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

### Gestione delle viste

Registrare una vista materializzata (REQ-133, REQ-135):
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

Avviare un aggiornamento manuale (REQ-135):
```graphql
mutation {
  refreshMv(mvId: "orders-with-customers") {
    success
  }
}
```

### Registrazione di origini a grafo

Le origini Neo4j e SPARQL vengono registrate tramite endpoint REST (non l'Admin API GraphQL) (REQ-295, REQ-297):

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

Una volta registrate, le tabelle appaiono nello schema GraphQL e sono interrogabili come qualsiasi altra origine (REQ-016).

## GraphiQL

L'Admin API include GraphiQL su `GET /admin/graphql` nel browser (REQ-622). Utilizzarlo per esplorare in modo interattivo l'intero schema admin.
