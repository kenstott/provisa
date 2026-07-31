# Admin API

Admin API 是一個 Strawberry GraphQL 端點，位於 `POST /admin/graphql`（REQ-533）。它需要超級用戶或管理員角色（REQ-125、REQ-060），並且與數據 GraphQL 端點是分開的（REQ-533）。

## 驗證

使用 Provisa 的標準驗證提供者（REQ-120），在 `Authorization` 標頭中傳遞您的憑證：

```yaml
Authorization: Bearer <token>
```

管理員存取權限由指派給角色的 `admin` 功能所管治（REQ-060、REQ-042）。

## 功能

### 設定管理

下載目前執行中的設定（REQ-164）：

```http
GET /admin/config
```

以 YAML 檔案形式傳回完整的 `config.yaml`。上載新的設定（REQ-164）：

```http
PUT /admin/config
```

Provisa 會驗證 YAML、重新載入目錄，並重新產生結構描述 (Schema)（REQ-012、REQ-253）。無需重新啟動。

### 執行階段設定

在不編輯設定檔的情況下讀取及寫入執行階段平台設定（REQ-165）：

```http
GET  /admin/settings
PUT  /admin/settings
```

設定範圍涵蓋大型結果重新導向、預設取樣及行數限制、回應快取的 TTL、命名慣例、關聯外部索引鍵的自動追蹤、具體化儲存區的 DSN、聯邦引擎記憶體（`jvm_heap_gb`、`query_max_memory`、`query_max_memory_per_node`、`query_max_total_memory`、`fault_tolerant_execution`、`fault_tolerant_task_memory`、`exchange_spool_dir`），以及整個 OpenTelemetry 追蹤管線的調校範圍（REQ-1082）。同時亦提供遠端 GraphQL 走訪限制及暖層／讀取快取設定（REQ-1081、REQ-1083）。

安全狀態 — `security.mode`（`standard` | `high`）— 於重新啟動時套用（REQ-1079）：

```http
GET  /admin/security
PUT  /admin/security
```

AI 模型指派、嵌入／向量模型登錄，以及自然語言速率限制 — 於重新啟動時套用（REQ-1080）：

```http
GET  /admin/ai-models
PUT  /admin/ai-models
```

管理員加密分頁會即時從加密登錄擷取其提供者清單；無法使用的提供者會顯示，但不可選取（REQ-1091）。

`GET`／`HEAD /health` 與 `GET /setup/status` 一律無需驗證 — 即使已設定驗證提供者，它們亦會略過 `Authorization: Bearer` 的要求（REQ-539）。

### 關聯編輯器

列出關聯（REQ-166）：

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

建立關聯（REQ-019）：

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

### AI 關聯探索

透過 REST 觸發由 Claude 提供支援的外部索引鍵分析（REQ-167、REQ-018）：

```bash
curl -X POST http://localhost:8001/admin/discover/relationships \
  -H "Content-Type: application/json" \
  -d '{"scope": "domain", "domain_id": "sales"}'
```

傳回按信心程度排序的外部索引鍵候選項目。接受候選項目：

```bash
curl -X POST http://localhost:8001/admin/discover/candidates/{id}/accept \
  -H "Content-Type: application/json" \
  -d '{"name": "orders_to_customers"}'
```

### 結構描述內省

瀏覽所有數據來源中已發佈的資料表（REQ-008）：

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

### 檢視管理

註冊具體化檢視（REQ-133、REQ-135）：

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

觸發手動重新整理（REQ-135）：

```graphql
mutation {
  refreshMv(mvId: "orders-with-customers") {
    success
  }
}
```

### 圖形數據來源註冊

Neo4j 及 SPARQL 數據來源透過 REST 端點註冊（並非 GraphQL 管理員 API）（REQ-295、REQ-297）：

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

註冊完成後，資料表便會出現在 GraphQL 結構描述中，並可像其他任何數據來源一樣進行查詢（REQ-016）。

## GraphiQL

Admin API 在瀏覽器中的 `GET /admin/graphql` 附帶 GraphiQL（REQ-622）。可用它以互動方式探索完整的管理員結構描述。
