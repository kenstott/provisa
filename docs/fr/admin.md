# Admin API

L'Admin API est un endpoint Strawberry GraphQL sur `POST /admin/graphql` (REQ-533). Elle nécessite un rôle superutilisateur ou admin (REQ-125, REQ-060) et est distincte de l'endpoint GraphQL de données (REQ-533).

## Authentification

Transmettez vos identifiants dans l'en-tête `Authorization` en utilisant le fournisseur d'authentification standard de Provisa (REQ-120)&nbsp;:

```yaml
Authorization: Bearer <token>
```

L'accès admin est régi par la capacité `admin` attribuée à un rôle (REQ-060, REQ-042).

## Capacités

### Gestion de la configuration

Téléchargez la configuration en cours d'exécution (REQ-164)&nbsp;:

```http
GET /admin/config
```

Renvoie le fichier `config.yaml` complet au format YAML. Envoyez une nouvelle configuration (REQ-164)&nbsp;:

```http
PUT /admin/config
```

Provisa valide le YAML, recharge les catalogues et régénère les schémas (REQ-012, REQ-253). Aucun redémarrage n'est requis.

### Paramètres d'exécution

Lisez et écrivez les paramètres de la plateforme au moment de l'exécution sans modifier le fichier de configuration (REQ-165)&nbsp;:

```http
GET  /admin/settings
PUT  /admin/settings
```

La surface de paramètres couvre la redirection des résultats volumineux, l'échantillonnage par défaut et la limite de lignes, le TTL du cache de réponses, la convention de nommage, le suivi automatique des clés étrangères de relations, le DSN du stockage de matérialisation, la mémoire du moteur de fédération (`jvm_heap_gb`, `query_max_memory`, `query_max_memory_per_node`, `query_max_total_memory`, `fault_tolerant_execution`, `fault_tolerant_task_memory`, `exchange_spool_dir`), ainsi que toute la surface de réglage du pipeline de traçage OpenTelemetry (REQ-1082). Les limites de parcours GraphQL distant et les paramètres de niveau intermédiaire (warm-tier)/cache de lecture sont également exposés (REQ-1081, REQ-1083).

Posture de sécurité — `security.mode` (`standard` | `high`) — appliquée au redémarrage (REQ-1079)&nbsp;:

```http
GET  /admin/security
PUT  /admin/security
```

Attribution des modèles d'IA, registre des modèles d'embedding/de vecteurs, et limite de débit NL — appliqués au redémarrage (REQ-1080)&nbsp;:

```http
GET  /admin/ai-models
PUT  /admin/ai-models
```

L'onglet de chiffrement de l'admin dérive sa liste de fournisseurs en direct depuis le registre de chiffrement&nbsp;; les fournisseurs indisponibles apparaissent mais ne sont pas sélectionnables (REQ-1091).

`GET`/`HEAD /health` et `GET /setup/status` sont toujours non authentifiés — ils contournent l'exigence `Authorization: Bearer` même lorsqu'un fournisseur d'authentification est configuré (REQ-539).

### Éditeur de relations

Listez les relations (REQ-166)&nbsp;:

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

Créez une relation (REQ-019)&nbsp;:

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

### Découverte de relations par IA

Déclenchez l'analyse des clés étrangères propulsée par Claude via REST (REQ-167, REQ-018)&nbsp;:

```bash
curl -X POST http://localhost:8001/admin/discover/relationships \
  -H "Content-Type: application/json" \
  -d '{"scope": "domain", "domain_id": "sales"}'
```

Renvoie les candidats de clé étrangère classés par niveau de confiance. Acceptez un candidat&nbsp;:

```bash
curl -X POST http://localhost:8001/admin/discover/candidates/{id}/accept \
  -H "Content-Type: application/json" \
  -d '{"name": "orders_to_customers"}'
```

### Introspection de schéma

Parcourez les tables publiées dans toutes les sources (REQ-008)&nbsp;:

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

### Gestion des vues

Enregistrez une vue matérialisée (REQ-133, REQ-135)&nbsp;:

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

Déclenchez une actualisation manuelle (REQ-135)&nbsp;:

```graphql
mutation {
  refreshMv(mvId: "orders-with-customers") {
    success
  }
}
```

### Enregistrement de sources de graphe

Les sources Neo4j et SPARQL sont enregistrées via des endpoints REST (et non l'Admin API GraphQL) (REQ-295, REQ-297)&nbsp;:

**Neo4j&nbsp;:**

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

**SPARQL&nbsp;:**

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

Une fois enregistrées, les tables apparaissent dans le schéma GraphQL et sont interrogeables comme toute autre source (REQ-016).

## GraphiQL

L'Admin API inclut GraphiQL sur `GET /admin/graphql` dans le navigateur (REQ-622). Utilisez-le pour explorer l'ensemble du schéma admin de façon interactive.
