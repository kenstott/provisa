# API de Administração

A API de administração é um endpoint Strawberry GraphQL em `POST /admin/graphql` (REQ-533). Ela exige uma função superuser ou admin (REQ-125, REQ-060) e é separada do endpoint GraphQL de dados (REQ-533).

## Autenticação

Passe suas credenciais no cabeçalho `Authorization` usando o provedor de autenticação padrão do Provisa (REQ-120):
```
Authorization: Bearer <token>
```

O acesso administrativo é governado pela capacidade `admin` atribuída a uma função (REQ-060, REQ-042).

## Capacidades

### Gerenciamento de Config

Baixe a config atualmente em execução (REQ-164):
```
GET /admin/config
```

Retorna o `config.yaml` completo como um arquivo YAML. Envie uma nova config (REQ-164):
```
PUT /admin/config
```

O Provisa valida o YAML, recarrega catálogos, e regenera esquemas (REQ-012, REQ-253). Nenhum reinício necessário.

### Configurações de Runtime

Leia e escreva configurações de plataforma em tempo de execução sem editar o arquivo de config (REQ-165):
```
GET  /admin/settings
PUT  /admin/settings
```

A superfície de configurações cobre redirecionamento de resultado grande, amostragem padrão e limite de linha, TTL de cache de resposta, convenção de nomenclatura, auto-rastreamento de FK de relacionamento, DSN do armazenamento de materialização, memória do motor de federação (`jvm_heap_gb`, `query_max_memory`, `query_max_memory_per_node`, `query_max_total_memory`, `fault_tolerant_execution`, `fault_tolerant_task_memory`, `exchange_spool_dir`), e a superfície completa de ajuste do pipeline de rastreamento OpenTelemetry (REQ-1082). Limites de travessia GraphQL remota e configurações de camada quente/cache de leitura também são expostos (REQ-1081, REQ-1083).

Postura de segurança — `security.mode` (`standard` | `high`) — aplicada no reinício (REQ-1079):
```
GET  /admin/security
PUT  /admin/security
```

Atribuições de modelo de IA, o registro de modelo de embedding/vetor, e o limite de taxa NL — aplicados no reinício (REQ-1080):
```
GET  /admin/ai-models
PUT  /admin/ai-models
```

A aba de criptografia da administração deriva sua lista de provedores ao vivo a partir do registro de criptografia; provedores indisponíveis aparecem mas não são selecionáveis (REQ-1091).

`GET`/`HEAD /health` e `GET /setup/status` são sempre não autenticados — eles contornam a exigência de `Authorization: Bearer` mesmo quando um provedor de autenticação está configurado (REQ-539).

### Editor de Relacionamento

Liste relacionamentos (REQ-166):
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

Crie um relacionamento (REQ-019):
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

### Descoberta de Relacionamento por IA

Dispare a análise de FK alimentada por Claude via REST (REQ-167, REQ-018):

```bash
curl -X POST http://localhost:8001/admin/discover/relationships \
  -H "Content-Type: application/json" \
  -d '{"scope": "domain", "domain_id": "sales"}'
```

Retorna candidatos a FK classificados por confiança. Aceite um candidato:

```bash
curl -X POST http://localhost:8001/admin/discover/candidates/{id}/accept \
  -H "Content-Type: application/json" \
  -d '{"name": "orders_to_customers"}'
```

### Introspecção de Esquema

Navegue por tabelas publicadas em todas as fontes (REQ-008):
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

### Gerenciamento de View

Registre uma view materializada (REQ-133, REQ-135):
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

Dispare uma atualização manual (REQ-135):
```graphql
mutation {
  refreshMv(mvId: "orders-with-customers") {
    success
  }
}
```

### Registro de Fonte de Grafo

Fontes Neo4j e SPARQL são registradas via endpoints REST (não a API de administração GraphQL) (REQ-295, REQ-297):

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

Uma vez registradas, tabelas aparecem no esquema GraphQL e são consultáveis como qualquer outra fonte (REQ-016).

## GraphiQL

A API de administração vem com GraphiQL em `GET /admin/graphql` no navegador (REQ-622). Use-o para explorar o esquema de administração completo interativamente.
