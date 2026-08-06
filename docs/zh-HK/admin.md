# Admin API

Admin API 是一個 Strawberry GraphQL 端點，位於 `POST /admin/graphql`（REQ-533）。它需要超級用戶或管理員角色（REQ-125、REQ-060），並且與數據 GraphQL 端點是分開的（REQ-533）。

## 驗證

使用 Provisa 的標準驗證提供者（REQ-120），在 `Authorization` 標頭中傳遞您的憑證：

```yaml
Authorization: Bearer <token>
```

管理員存取權限由指派給角色的 `admin` 功能所管治（REQ-060、REQ-042）。

### 個人存取權杖

凡接受 bearer 權杖之處皆接受個人存取權杖，本端點亦然。簽發與撤銷均為自助操作——它是持有者的私有憑證，因此位於管理介面的使用者個人資料頁而非管理員頁面，與離開組織、刪除帳戶並列。管理員不會代他人簽發權杖。（REQ-1263）

| 路由 | 作用 |
| ------- | -------- |
| `POST /auth/tokens` | 為呼叫者簽發一個權杖。請求主體：`name`，以及可選的 `role_id`、`scopes`、`expires_in_days`（1–366）。回應是密鑰唯一一次出現的地方 |
| `GET /auth/tokens` | 呼叫者在本組織中的有效權杖——顯示前綴、名稱、生命週期時間戳，以及用於撤銷的權杖識別雜湊。絕不會是可用憑證 |
| `DELETE /auth/tokens/{token_hash}` | 撤銷呼叫者的其中一個權杖。若不屬於呼叫者或已被撤銷則回傳 404 |

省略 `role_id` 時，權杖會解析為其擁有者持有的角色；指定角色則把權杖收窄至低於其擁有者。撤銷亦會隱含發生：移除使用者在某組織中的成員資格會撤銷其針對該組織的所有權杖。憑證本身請參閱[安全模型](security.md#personal-access-tokens)。

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

## ops 網域管理檢視（REQ-1386）

每次安裝都會在內建的 `ops` 網域中植入八個 SQL 檢視。[tool-verified: `provisa/api/startup_seed.py:225-331` `_seed_ops_domain`] 它們把查詢稽核紀錄呈現為受管治的資料表——可經由 SQL（pgwire）、GraphQL 與 Cypher 查詢，並套用與任何業務資料表相同的網域存取規則、資料列層級安全與遮罩。

植入時會把 `org_admin` 設為 ops 網域的管家，因此該網域絕不會在 `stale_metadata` 中顯示為管治缺口。[tool-verified: `startup_seed.py:326-331`]

| 檢視 | 回答什麼問題 |
| --- | --- |
| `usage_ranking` | 每個已註冊資料表的查詢次數與不重複使用者數；無人存取的資料表會浮現為退役候選 |
| `deprecated_usage` | 對帶 `deprecated` 標籤的資料表或資料行的每一次存取——阻礙安全移除的活躍取用方 |
| `pii_access` | 對帶 `pii` 標籤的資料表或資料行的每一次存取：誰查詢的、以何角色、經由哪個介面 |
| `policy_denials` | 管治拒絕的所有存取嘗試（HTTP 401/403） |
| `surface_mix` | 按協定介面（SQL、GraphQL、Cypher、gRPC 等）統計的每日查詢次數與不重複使用者數 |
| `query_health` | 按介面統計的每日錯誤數與平均／最大延遲 |
| `stale_metadata` | 缺少描述的資料表與資料行；缺少管家的網域 |
| `join_hotspots` | 最常一起被查詢的資料表配對——物化或快取的候選 |

目前有兩項限制。粒度為資料表層級——稽核紀錄儲存 `table_ids`，而非實際存取了哪些資料行。查詢文字已加密（REQ-689）並從此處所有檢視中排除；只能經由獲授權的管理解密路徑存取。[tool-verified: `_meta_views.py:148-162` — comment notes `query_text_enc` exclusion]

角色需要 `ops` 網域的存取權限，這些檢視才會可見。授予方式與授予任何其他網域的存取權限相同。

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

同樣的查詢可以作為 GraphQL 或 Cypher 執行於任何受管治的傳輸之上——pgwire、Arrow Flight 或 Bolt。[inferred from governed-surface design]

## 報表檢視器（REQ-1390）

報表檢視器位於 `/admin/reports`。沒有 `observability` 功能的角色無法存取。

左側面板列出 `ops` 網域中每個已註冊的資料表，按別名排序。[tool-verified: `ReportsTab.tsx:46-52` — filters `tables` to `domainId === "ops"`] 八個植入的管理檢視會自動出現在該處。點擊任一報表即可在右側的受管治資料檢視器中載入。

**新增自訂報表。**「新增報表」按鈕會開啟一個對話框。填寫名稱、可選描述以及一段 SELECT 陳述式。儲存會把該檢視註冊為 `ops` 網域中受管治的衍生資料表——已編目、受存取控制，並可與植入檢視一同從所有介面查詢。[tool-verified: `ReportsTab.tsx:70-96` — `registerTable` called with `sourceId: DERIVED_SOURCE_ID, domainId: "ops"`]

**刪除。**垃圾桶圖示僅對自訂報表顯示。植入的管理檢視無法從此介面刪除。[tool-verified: `ReportsTab.tsx:151` — `const custom = report.sourceId === DERIVED_SOURCE_ID` gates the delete button]

## 資料表預覽（REQ-1392）

在「資料表」頁面展開任一列。**預覽**按鈕會開啟一個寬度為 90% 的對話框，顯示該資料表的即時受管治資料。[tool-verified: `TablePreviewModal.tsx:24` — `size="90%"`; `GovernedTableViewer.tsx` is the underlying viewer]

以 API 為後端且帶有必填路徑參數的資料表會封鎖預覽，直到提供那些值。內嵌表單會在首次查詢執行前收集每個必填參數；可選的查詢參數出現在同一表單中。[tool-verified: `GovernedTableViewer.tsx:51-55, 153-155` — `requiredParamColumns` check; "paramsRequired" message shown when `activeParams == null`]

## 受管治資料檢視器（REQ-1391）

同一個檢視器元件同時驅動預覽對話框與報表檢視器。兩種情境下行為一致。

**伺服器端分頁。**每一頁都是獨立的受管治 `SELECT *`，帶 `LIMIT 101 OFFSET n`。每頁顯示 100 列；第 101 列用來判斷是否還有更多。完整資料集絕不會載入瀏覽器。[tool-verified: `nativeParams.ts:72` — `LIMIT ${pageSize + 1} OFFSET ${page * pageSize}`; `types.ts:74` — `PAGE_SIZE = 100`]

**篩選與排序下推至來源。**每個資料行標題都有一個篩選欄位。篩選字詞會變成 `WHERE LOWER(CAST(col AS VARCHAR)) LIKE LOWER('%term%')` 述詞；點擊排序會產生 `ORDER BY` 子句。兩者都會送往資料庫——對十億列的資料表做篩選掃描的是來源，而非你眼前的 100 列。[tool-verified: `nativeParams.ts:53-70`]

**多層分組。**每個資料行標題上的圖層圖示會把該資料行加入分組。分組資料行排在 `ORDER BY` 最前，因此即使跨頁，組成員也會與其組標題落在同一頁。主鍵資料行會附加於末尾，作為穩定的決勝條件。[tool-verified: `nativeParams.ts:61-70` — group columns first, then explicit sorts, then PKs] 組標題列可摺疊；摺疊會隱藏成員而不發出新查詢。[tool-verified: `useResultsGrid.ts:150-171` — `collapsedGroups` set gates the `build()` recursion]

**選擇會保留。**篩選、排序與分組設定儲存在 `localStorage` 的 `provisa.grid.table:<domain>.<table>` 之下，並於下次造訪時還原。[tool-verified: `useResultsGrid.ts:95-98`, `GovernedTableViewer.tsx:66`]

**匯出。**把目前頁面下載為 CSV，或以定位字元分隔的文字複製到剪貼簿。匯出僅涵蓋可見頁面。[tool-verified: `useResultsGrid.ts:247-274` — both handlers iterate `displayRows`, which in server-paged mode is the current page]
