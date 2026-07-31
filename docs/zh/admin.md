# Admin API

Admin API 是一个 Strawberry GraphQL 终结点，位于 `POST /admin/graphql`（REQ-533）。它需要超级用户或管理员角色（REQ-125、REQ-060），并且与数据 GraphQL 终结点是分开的（REQ-533）。

## 身份验证

使用 Provisa 的标准身份验证提供程序（REQ-120），在 `Authorization` 标头中传递您的凭据：

```yaml
Authorization: Bearer <token>
```

管理员访问权限由分配给角色的 `admin` 功能所控制（REQ-060、REQ-042）。

## 功能

### 配置管理

下载当前正在运行的配置（REQ-164）：

```http
GET /admin/config
```

以 YAML 文件形式返回完整的 `config.yaml`。上传新配置（REQ-164）：

```http
PUT /admin/config
```

Provisa 会验证 YAML、重新加载目录，并重新生成架构 (Schema)（REQ-012、REQ-253）。无需重启。

### 运行时设置

在不编辑配置文件的情况下读取和写入运行时平台设置（REQ-165）：

```http
GET  /admin/settings
PUT  /admin/settings
```

设置范围涵盖大结果重定向、默认采样和行数限制、响应缓存的 TTL、命名约定、关联外键的自动追踪、物化存储的 DSN、联邦引擎内存（`jvm_heap_gb`、`query_max_memory`、`query_max_memory_per_node`、`query_max_total_memory`、`fault_tolerant_execution`、`fault_tolerant_task_memory`、`exchange_spool_dir`），以及整个 OpenTelemetry 追踪管道的调优范围（REQ-1082）。远程 GraphQL 遍历限制以及暖层/读取缓存设置也已提供（REQ-1081、REQ-1083）。

安全态势 — `security.mode`（`standard` | `high`）— 在重启时生效（REQ-1079）：

```http
GET  /admin/security
PUT  /admin/security
```

AI 模型分配、嵌入/向量模型注册表，以及自然语言速率限制 — 在重启时生效（REQ-1080）：

```http
GET  /admin/ai-models
PUT  /admin/ai-models
```

管理员加密选项卡会实时从加密注册表获取其提供程序列表；不可用的提供程序会显示，但不可选择（REQ-1091）。

`GET`/`HEAD /health` 和 `GET /setup/status` 始终无需身份验证 — 即使已配置身份验证提供程序，它们也会绕过 `Authorization: Bearer` 的要求（REQ-539）。

### 关联编辑器

列出关联（REQ-166）：

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

创建关联（REQ-019）：

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

### AI 关联发现

通过 REST 触发由 Claude 提供支持的外键分析（REQ-167、REQ-018）：

```bash
curl -X POST http://localhost:8001/admin/discover/relationships \
  -H "Content-Type: application/json" \
  -d '{"scope": "domain", "domain_id": "sales"}'
```

返回按置信度排序的外键候选项。接受候选项：

```bash
curl -X POST http://localhost:8001/admin/discover/candidates/{id}/accept \
  -H "Content-Type: application/json" \
  -d '{"name": "orders_to_customers"}'
```

### 架构内省

浏览所有数据源中已发布的表（REQ-008）：

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

### 视图管理

注册物化视图（REQ-133、REQ-135）：

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

触发手动刷新（REQ-135）：

```graphql
mutation {
  refreshMv(mvId: "orders-with-customers") {
    success
  }
}
```

### 图数据源注册

Neo4j 和 SPARQL 数据源通过 REST 终结点注册（而非 GraphQL 管理员 API）（REQ-295、REQ-297）：

**Neo4j：**

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

**SPARQL：**

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

注册完成后，表会出现在 GraphQL 架构中，并可像任何其他数据源一样进行查询（REQ-016）。

## GraphiQL

Admin API 在浏览器中的 `GET /admin/graphql` 附带 GraphiQL（REQ-622）。可用它以交互方式探索完整的管理员架构。
