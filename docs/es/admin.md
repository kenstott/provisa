# Admin API

La Admin API es un endpoint de Strawberry GraphQL en `POST /admin/graphql` (REQ-533). Requiere un rol de superusuario o admin (REQ-125, REQ-060) y es independiente del endpoint de GraphQL de datos (REQ-533).

## Autenticación

Pase sus credenciales en el encabezado `Authorization` usando el proveedor de autenticación estándar de Provisa (REQ-120):

```yaml
Authorization: Bearer <token>
```

El acceso admin se rige por la capacidad `admin` asignada a un rol (REQ-060, REQ-042).

## Capacidades

### Gestión de Configuración

Descargue la configuración en ejecución actual (REQ-164):

```http
GET /admin/config
```

Devuelve el `config.yaml` completo como archivo YAML. Suba una nueva configuración (REQ-164):

```http
PUT /admin/config
```

Provisa valida el YAML, recarga los catálogos y regenera los esquemas (REQ-012, REQ-253). No requiere reinicio.

### Configuración en Tiempo de Ejecución

Lea y escriba la configuración de la plataforma en tiempo de ejecución sin editar el archivo de configuración (REQ-165):

```http
GET  /admin/settings
PUT  /admin/settings
```

La superficie de configuración abarca la redirección de resultados grandes, el muestreo predeterminado y el límite de filas, el TTL de la caché de respuestas, la convención de nomenclatura, el auto-rastreo de FK de relaciones, el DSN del almacén de materialización, la memoria del motor de federación (`jvm_heap_gb`, `query_max_memory`, `query_max_memory_per_node`, `query_max_total_memory`, `fault_tolerant_execution`, `fault_tolerant_task_memory`, `exchange_spool_dir`) y toda la superficie de ajuste del pipeline de trazas de OpenTelemetry (REQ-1082). Los límites de recorrido de GraphQL remoto y la configuración de nivel cálido/caché de lectura también se exponen (REQ-1081, REQ-1083).

Postura de seguridad — `security.mode` (`standard` | `high`) — aplicada al reiniciar (REQ-1079):

```http
GET  /admin/security
PUT  /admin/security
```

Asignaciones de modelos de IA, el registro de modelos de embeddings/vectores, y el límite de tasa de NL — aplicados al reiniciar (REQ-1080):

```http
GET  /admin/ai-models
PUT  /admin/ai-models
```

La pestaña de cifrado del admin deriva su lista de proveedores en vivo desde el registro de cifrado; los proveedores no disponibles aparecen pero no son seleccionables (REQ-1091).

`GET`/`HEAD /health` y `GET /setup/status` siempre están sin autenticar — eluden el requisito de `Authorization: Bearer` incluso cuando hay un proveedor de autenticación configurado (REQ-539).

### Editor de Relaciones

Liste las relaciones (REQ-166):

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

Cree una relación (REQ-019):

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

### Descubrimiento de Relaciones con IA

Active el análisis de FK impulsado por Claude vía REST (REQ-167, REQ-018):

```bash
curl -X POST http://localhost:8001/admin/discover/relationships \
  -H "Content-Type: application/json" \
  -d '{"scope": "domain", "domain_id": "sales"}'
```

Devuelve candidatos de FK clasificados por confianza. Acepte un candidato:

```bash
curl -X POST http://localhost:8001/admin/discover/candidates/{id}/accept \
  -H "Content-Type: application/json" \
  -d '{"name": "orders_to_customers"}'
```

### Introspección de Esquemas

Explore las tablas publicadas en todos los orígenes (REQ-008):

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

### Gestión de Vistas

Registre una vista materializada (REQ-133, REQ-135):

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

Active una actualización manual (REQ-135):

```graphql
mutation {
  refreshMv(mvId: "orders-with-customers") {
    success
  }
}
```

### Registro de Orígenes de Grafo

Los orígenes Neo4j y SPARQL se registran mediante endpoints REST (no la Admin API de GraphQL) (REQ-295, REQ-297):

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

Una vez registradas, las tablas aparecen en el esquema de GraphQL y son consultables como cualquier otro origen (REQ-016).

## GraphiQL

La Admin API incluye GraphiQL en `GET /admin/graphql` en el navegador (REQ-622). Úselo para explorar el esquema admin completo de forma interactiva.
