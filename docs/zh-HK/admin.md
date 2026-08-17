# 管理 API

管理 API 是位於 `POST /admin/graphql` 的 Strawberry GraphQL 端點（REQ-533）。它需要 superuser 或 admin 角色（REQ-125、REQ-060），並且與數據 GraphQL 端點分開（REQ-533）。

## 身分驗證

使用標準的 Provisa 身分驗證提供者，在 `Authorization` 標頭中傳遞你的憑證（REQ-120）：

```yaml
Authorization: Bearer <token>
```

管理存取權限由指派給角色的 `admin` 權限控管（REQ-060、REQ-042）。

### 個人存取權杖

個人存取權杖 (personal access token) 可以在任何接受 bearer token 的地方使用，包括此端點。核發及撤銷此類權杖屬於自助服務——因為它是持有人自己的憑證，所以會列在管理介面中使用者本身的個人資料頁面，而非管理頁面下，與離開組織及刪除帳戶並列。管理員不會代替他人核發權杖。（REQ-1263）

| 路由 | 效果 |
| ------- | -------- |
| `POST /auth/tokens` | 為呼叫方核發一個權杖。內文參數：`name`、可選的 `role_id`、`scopes`、`expires_in_days`（1 至 366）。回應內容是密鑰唯一會出現的地方 |
| `GET /auth/tokens` | 呼叫方在此組織中的有效權杖——顯示前綴、名稱、生命週期時間戳記，以及用於撤銷的權杖識別雜湊值。絕不會傳回可用的憑證本身 |
| `DELETE /auth/tokens/{token_hash}` | 撤銷呼叫方其中一個權杖。若該權杖並非其所有，或已被撤銷，則傳回 404 |

省略 `role_id` 會讓權杖解析為其擁有者所持有的任何角色；指定角色則會將權杖的權限收窄至低於其擁有者。撤銷動作亦會隱含發生：移除使用者的組織成員資格，會撤銷其在該組織的所有權杖。有關憑證本身，請參閱[安全模型](security.md#_17)。

## 功能

### 設定管理

下載目前運行中的設定（REQ-164）：

```http
GET /admin/config
```

以 YAML 檔案形式傳回完整的 `config.yaml`。上載新的設定（REQ-164）：

```http
PUT /admin/config
```

Provisa 會驗證該 YAML、重新載入目錄，並重新產生結構描述（schema）（REQ-012、REQ-253）。不需要重新啟動。

### 執行階段設定

無需編輯設定檔即可讀寫執行階段的平台設定（REQ-165）：

```http
GET  /admin/settings
PUT  /admin/settings
```

設定介面涵蓋大量結果重新導向、預設抽樣及資料列上限、回應快取存留時間 (TTL)、命名慣例、關聯外部索引鍵自動追蹤、具體化儲存的 DSN、聯邦引擎記憶體（`jvm_heap_gb`、`query_max_memory`、`query_max_memory_per_node`、`query_max_total_memory`、`fault_tolerant_execution`、`fault_tolerant_task_memory`、`exchange_spool_dir`），以及完整的 OpenTelemetry 追蹤管線調校介面（REQ-1082）。遠端 GraphQL 遍歷限制及暖層 (warm-tier)／讀取快取設定亦一併公開（REQ-1081、REQ-1083）。

安全狀態——`security.mode`（`standard` | `high`）——於重新啟動時套用（REQ-1079）：

```http
GET  /admin/security
PUT  /admin/security
```

AI 模型指派、嵌入／向量模型登記冊，以及自然語言查詢的速率限制——於重新啟動時套用（REQ-1080）：

```http
GET  /admin/ai-models
PUT  /admin/ai-models
```

管理介面的加密分頁會即時從加密登記冊推導其供應商清單；無法使用的供應商會顯示出來，但無法選取（REQ-1091）。

`GET`/`HEAD /health` 及 `GET /setup/status` 一律不需要身分驗證——即使已設定身分驗證提供者，它們仍會繞過 `Authorization: Bearer` 的要求（REQ-539）。

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

建立一個關聯（REQ-019）：

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

### AI 關聯偵測

透過 REST 觸發由 Claude 驅動的外部索引鍵分析（REQ-167、REQ-018）：

```bash
curl -X POST http://localhost:8001/admin/discover/relationships \
  -H "Content-Type: application/json" \
  -d '{"scope": "domain", "domain_id": "sales"}'
```

傳回按信心度排序的外部索引鍵候選項目。接受一個候選項目：

```bash
curl -X POST http://localhost:8001/admin/discover/candidates/{id}/accept \
  -H "Content-Type: application/json" \
  -d '{"name": "orders_to_customers"}'
```

### 結構描述探索

瀏覽所有來源中已發佈的資料表（REQ-008）：

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

### 欄相依性檢查（REQ-1484）

在儲存會重新命名欄的 SQL 別名或刪除欄的資料表編輯之前，先查詢還有什麼東西參照它：

```graphql
query {
  columnDependents(tableId: "42", renamed: ["order_total"], removed: ["legacy_code"]) {
    columnName
    dependents { kind name detail breaksOn }
  }
}
```

重新命名別名，會破壞所有依顯示名稱撰寫的構件——檢視、MV、指標運算式、RLS 判斷式、DQ 合約。刪除一個欄除了破壞上述項目外，還會破壞儲存實體 `column_name` 的構件：關聯、詞彙表綁定、標籤指派。`breaksOn` 會標明是哪一種。Tables 頁面會在儲存時自動執行此查詢，並以提示對話方塊顯示結果。有關此查詢涵蓋的範圍及其限制，請參閱[數據血緣](lineage.md)。

### 檢視管理

註冊一個具體化檢視（REQ-133、REQ-135）：

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

### 圖形來源註冊

Neo4j 及 SPARQL 來源是透過 REST 端點（而非 GraphQL 管理 API）註冊的（REQ-295、REQ-297）：

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

註冊完成後，資料表會出現在 GraphQL 結構描述中，並可像任何其他來源一樣被查詢（REQ-016）。

## GraphiQL

管理 API 內建 GraphiQL，可於瀏覽器中的 `GET /admin/graphql` 存取（REQ-622）。可用它以互動方式探索完整的管理結構描述。

## Ops 網域管理檢視（REQ-1386）

每次安裝時，都會有八個 SQL 檢視植入內建的 `ops` 網域。[tool-verified: `provisa/api/startup_seed.py:225-331` `_seed_ops_domain`] 它們將查詢稽核記錄以受治理資料表的形式公開——可透過 SQL（pgwire）、GraphQL 及 Cypher 查詢，並與任何業務資料表一樣，遵循相同的網域存取、RLS 及遮罩規則。

`org_admin` 在植入時被指定為 ops 網域的數據管家，因此該網域絕不會在 `stale_metadata` 中顯示為治理缺口。[tool-verified: `startup_seed.py:326-331`]

| 檢視 | 回答的問題 |
| --- | --- |
| `usage_ranking` | 每個已註冊資料表的查詢次數及不重複使用者數；零命中的資料表會標示為淘汰候選項目 |
| `deprecated_usage` | 對帶有 `deprecated` 標籤的資料表或欄的每一次存取——即阻礙安全移除的目前使用方 |
| `pii_access` | 對帶有 `pii` 標籤的資料表或欄的每一次存取：誰查詢了它、使用哪個角色、透過哪個介面 |
| `policy_denials` | 所有被治理機制拒絕的存取嘗試（HTTP 401/403） |
| `surface_mix` | 每個通訊協定介面（SQL、GraphQL、Cypher、gRPC 等）的每日查詢次數及不重複使用者數 |
| `query_health` | 每個介面的每日錯誤次數及平均／最大延遲 |
| `stale_metadata` | 缺少描述的資料表及欄；缺少數據管家的網域 |
| `join_hotspots` | 最常一併被查詢的資料表配對——具體化或快取的候選項目 |

目前有兩項限制。粒度停留在資料表層級——稽核記錄記錄的是 `table_ids`，而非個別被存取的欄。查詢文字經過加密（REQ-689），並在此處所有檢視中一律被排除；只能透過經授權的管理員解密路徑存取。[tool-verified: `_meta_views.py:148-162` — comment notes `query_text_enc` exclusion]

角色需要先取得 `ops` 網域的存取權限，才能看到這些檢視。授予方式與授予任何其他網域的存取權限相同。

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

相同的查詢也可以 GraphQL 或 Cypher 形式，透過任何受治理的傳輸方式執行——pgwire、Arrow Flight 或 Bolt。[inferred from governed-surface design]

## 報表檢視器（REQ-1390）

報表檢視器位於 `/admin/reports`。沒有 `observability` 權限的角色無法存取此頁面。

左側面板列出 `ops` 網域中所有已註冊的資料表，按別名排序。[tool-verified: `ReportsTab.tsx:46-52` — filters `tables` to `domainId === "ops"`] 八個植入的管理檢視會自動出現在此處。按一下任何報表，即可在右側的受治理數據檢視器中載入它。

**新增自訂報表。**「Add report」按鈕會開啟一個對話方塊。提供名稱、可選的描述，以及一個 SELECT 陳述式。儲存後會將該檢視以受治理衍生資料表的形式，註冊至 `ops` 網域——與其他植入的檢視一樣，一併納入目錄、存取控管，並可透過所有介面查詢。[tool-verified: `ReportsTab.tsx:70-96` — `registerTable` called with `sourceId: DERIVED_SOURCE_ID, domainId: "ops"`]

**刪除。**垃圾桶圖示只會在自訂報表上出現。植入的管理檢視無法從此介面刪除。[tool-verified: `ReportsTab.tsx:151` — `const custom = report.sourceId === DERIVED_SOURCE_ID` gates the delete button]

## 資料表預覽（REQ-1392）

在 Tables 頁面展開任何一列資料表。**Preview** 按鈕會開啟一個寬度為 90% 的視窗，顯示該資料表的即時受治理數據。[tool-verified: `TablePreviewModal.tsx:24` — `size="90%"`; `GovernedTableViewer.tsx` is the underlying viewer]

由需要路徑參數的 API 支援的資料表，在提供這些值之前會封鎖預覽功能。第一次查詢執行前，會有一個內嵌表單收集每個必要參數；可選的查詢參數會出現在同一個表單中。[tool-verified: `GovernedTableViewer.tsx:51-55, 153-155` — `requiredParamColumns` check; "paramsRequired" message shown when `activeParams == null`]

## 受治理數據檢視器（REQ-1391）

同一個檢視器元件同時支援預覽視窗及報表檢視器，其行為在兩種情境下完全相同。

**伺服器端分頁。**每一頁都是獨立的受治理 `SELECT *`，並帶有 `LIMIT 101 OFFSET n`。每頁顯示 100 列；第 101 列則用來判斷是否還有更多資料。完整數據集絕不會被載入瀏覽器。[tool-verified: `nativeParams.ts:72` — `LIMIT ${pageSize + 1} OFFSET ${page * pageSize}`; `types.ts:74` — `PAGE_SIZE = 100`]

**下推式篩選及排序。**每個欄標題都有一個篩選輸入框。篩選詞會轉換為 `WHERE LOWER(CAST(col AS VARCHAR)) LIKE LOWER('%term%')` 判斷式；點按排序則會產生 `ORDER BY` 子句。兩者都會下推至資料庫執行——對一個十億列資料表的篩選，掃描的是來源本身，而不是你眼前這一頁的 100 列。[tool-verified: `nativeParams.ts:53-70`]

**多層分組。**任何欄標題中的圖層圖示，都可以切換該欄是否納入分組。分組欄會排在 `ORDER BY` 之首，讓同一分組的成員即使跨頁，也能與其標題落在同一頁。主索引鍵欄會被附加作為穩定的排序決勝條件。[tool-verified: `nativeParams.ts:61-70` — group columns first, then explicit sorts, then PKs] 分組標題列可摺疊；摺疊只會隱藏成員，不會發出新的查詢。[tool-verified: `useResultsGrid.ts:150-171` — `collapsedGroups` set gates the `build()` recursion]

**持久化選項。**篩選、排序及分組設定會持久化至 `localStorage`，鍵值為 `provisa.grid.table:<domain>.<table>`，並於下次造訪時還原。[tool-verified: `useResultsGrid.ts:95-98`, `GovernedTableViewer.tsx:66`]

**匯出。**將目前頁面下載為 CSV，或以 tab 分隔文字複製到剪貼簿。匯出範圍僅限於目前可見的頁面。[tool-verified: `useResultsGrid.ts:247-274` — both handlers iterate `displayRows`, which in server-paged mode is the current page]
