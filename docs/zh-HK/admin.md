# 管理 API

管理 API 是位於 `POST /admin/graphql` 的 Strawberry GraphQL 端點（REQ-533）。它需要 superuser 或 admin 角色（REQ-125、REQ-060），且與數據用的 GraphQL 端點分開（REQ-533）。

## 身份驗證

使用標準的 Provisa 驗證提供者，在 `Authorization` 標頭中傳入你的憑證（REQ-120）：

```yaml
Authorization: Bearer <token>
```

管理存取由指派給角色的 `admin` 能力管束（REQ-060、REQ-042）。

### 個人存取權杖

凡接受 bearer 權杖之處都接受個人存取權杖，本端點亦然。簽發與撤銷都是自助的——它是權杖持有人自己的憑證，因此它落在管理 UI 中使用者的個人檔案上，而不在某個管理頁面之下，與退出組織及刪除帳戶並列。管理員不會代他人鑄造權杖。（REQ-1263）

| 路由 | 作用 |
| ------- | -------- |
| `POST /auth/tokens` | 為呼叫者鑄造一個權杖。主體：`name`，選填 `role_id`、`scopes`、`expires_in_days`（1–366）。回應是該密鑰唯一現身之處 |
| `GET /auth/tokens` | 呼叫者在此組織中的有效權杖——顯示前綴、名稱、生命週期時間戳記，以及撤銷時用來指認權杖的雜湊值。絕不是可用的憑證 |
| `DELETE /auth/tokens/{token_hash}` | 撤銷呼叫者的一個權杖。權杖不屬於他或已撤銷時回 404 |

省略 `role_id` 會讓權杖解析到其擁有者所持有的任何角色；指名一個則把權杖收窄到低於其擁有者。撤銷也會隱含發生：移除某使用者的組織成員資格，即撤銷他在該組織的各個權杖。憑證本身見 [安全模型](security.md#personal-access-tokens)。

## 各項能力

### 設定管理

下載目前執行中的設定（REQ-164）：

```http
GET /admin/config
```

以 YAML 檔案傳回完整的 `config.yaml`。上傳新的設定（REQ-164）：

```http
PUT /admin/config
```

Provisa 會驗證該 YAML、重新載入目錄並重新生成結構描述（REQ-012、REQ-253）。毋須重新啟動。

### 執行階段設定

毋須編輯設定檔即可讀寫執行階段的平台設定（REQ-165）：

```http
GET  /admin/settings
PUT  /admin/settings
```

設定介面涵蓋大型結果重新導向、預設取樣與行數上限、回應快取 TTL、命名慣例、關係外部索引鍵自動追蹤、具體化儲存區 DSN、聯邦引擎記憶體（`jvm_heap_gb`、`query_max_memory`、`query_max_memory_per_node`、`query_max_total_memory`、`fault_tolerant_execution`、`fault_tolerant_task_memory`、`exchange_spool_dir`），以及完整的 OpenTelemetry 追蹤管線調校介面（REQ-1082）。遠端 GraphQL 的走訪上限與溫層／讀取快取設定亦一併公開（REQ-1081、REQ-1083）。

安全姿態——`security.mode`（`standard` | `high`）——於重新啟動時套用（REQ-1079）：

```http
GET  /admin/security
PUT  /admin/security
```

AI 模型指派、embedding／向量模型註冊表，以及 NL 速率限制——於下一個請求生效，毋須重新啟動（REQ-1349）：[tool-verified: `provisa/api/admin/ai_models_router.py:38-39`]

```http
GET  /admin/ai-models
PUT  /admin/ai-models
```

管理介面的加密分頁會即時從加密註冊表推導出提供者清單；不可用的提供者會顯示出來，但無法選取（REQ-1091）。

`GET`／`HEAD /health` 與 `GET /setup/status` 一律免驗證——即使設定了驗證提供者，它們也繞過 `Authorization: Bearer` 的要求（REQ-539）。

### 聯邦引擎

讀取或變更此部署所使用的引擎（REQ-916）：

```http
GET  /admin/federation-engine
PUT  /admin/federation-engine
```

`GET` 傳回目前啟用的引擎索引鍵，以及它所需的設定欄位。`PUT` 接受一個帶有 `engine`（索引鍵）及任何引擎專屬欄位的主體；該選擇會保存到平台設定，並於服務下次重新啟動時繫結。[tool-verified: `provisa/api/admin/settings_router.py:730-829`]

### 關係編輯器

列出關係（REQ-166）：

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

建立一個關係（REQ-019）：

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

宣告一個由聯結資料表支撐的關係（REQ-1586）：

```graphql
mutation {
  upsertRelationship(input: {
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

關聯資料表是被宣告為邊的，從不被自動探索。`viaTable` 指定一張已註冊的資料表；它的兩個索引鍵欄位承載這條邊，其餘每一個欄位都會成為該關係的一個屬性，可以像任何其他欄位一樣被過濾。`viaTypeColumn` / `viaTypeValue` 把一張聯結資料表拆成多個邊類型——`pet_companions` 中 `companion_type` 分別為 `bonded pair`、`littermate` 與 `shares enclosure` 的三類資料列，就是同一對資料表之上的三種不同關係。

`viaLabelSource` 指定對外公開的名稱來自哪裡，三種形式都會為 Cypher 轉成大寫蛇形（UPPER_SNAKE_CASE）：`column` 取 `viaTypeValue`（`BONDED_PAIR`），`table` 取聯結資料表自身的名稱（`PET_COMPANIONS`），`fixed` 取宣告的 `alias`。以這種方式宣告的聯結資料表是一條邊而不是一個實體——它會從節點標籤中移除，因此在圖 UI 中永遠不會出現節點膠囊。[tool-verified: `provisa/api/admin/types.py:606-611`, `provisa/api/admin/db_queries.py:47-82`]

### AI 關係探索

經 REST 觸發由 Claude 驅動的外部索引鍵分析（REQ-167、REQ-018）：

```bash
curl -X POST http://localhost:8001/admin/discover/relationships \
  -H "Content-Type: application/json" \
  -d '{"scope": "domain", "domain_id": "sales"}'
```

傳回按信心度排序的外部索引鍵候選。接受某個候選：

```bash
curl -X POST http://localhost:8001/admin/discover/candidates/{id}/accept \
  -H "Content-Type: application/json" \
  -d '{"name": "orders_to_customers"}'
```

### 結構描述內省

瀏覽所有數據來源上已發佈的表（REQ-008）：

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

### 欄位依賴檢查（REQ-1484）

在儲存一項會更改欄位 SQL 別名或刪除欄位的表編輯之前，先問問還有什麼引用它：

```graphql
query {
  columnDependents(tableId: "42", renamed: ["order_total"], removed: ["legacy_code"]) {
    columnName
    dependents { kind name detail breaksOn }
  }
}
```

更改別名會弄壞每個照著對外名稱撰寫的構件——檢視、MV、指標運算式、RLS 謂詞、DQ 合約。刪除欄位除了弄壞上述這些，還會弄壞儲存實體 `column_name` 的那些構件：關係、詞彙表繫結、標籤指派。`breaksOn` 會說明是哪一種。表頁面會在儲存時執行這個查詢，並以建議性對話方塊顯示結果。此查詢涵蓋什麼、又涵蓋不了什麼，見 [血緣](lineage.md)。

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

觸發一次手動重新整理（REQ-135）：

```graphql
mutation {
  refreshMv(mvId: "orders-with-customers") {
    success
  }
}
```

### 圖形數據來源註冊

Neo4j 與 SPARQL 數據來源經 REST 端點註冊（而非 GraphQL 管理 API）（REQ-295、REQ-297）：

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

註冊之後，這些表便出現在 GraphQL 結構描述中，可像任何其他數據來源一樣查詢（REQ-016）。

### Hasura／DDN 匯入（REQ-1483）

經管理 UI 或 API 把既有的 Hasura v2 或 Hasura DDN 專案轉換成 Provisa 設定，在你批准之前不會有任何東西落地。

```http
POST /admin/import/hasura/preview
POST /admin/import/hasura/apply
```

**預覽**會轉換上傳的封存檔，並傳回建議的 `config_yaml`、一份警告清單，以及一份發現內容摘要（數據來源、網域、表、欄位、角色、關係與 RLS 的數量）。不會有任何東西寫入租用戶資料庫。請求主體：

```json
{
  "filename": "my-hasura-project.zip",
  "content_b64": "<base64-encoded archive>",
  "flavor": "auto",
  "domain_map": {"public": "sales"},
  "source_overrides": {}
}
```

`flavor` 為 `"auto"`（由封存檔結構偵測）、`"hasura_v2"` 或 `"ddn"`。

**套用**會取你已審閱（並可能已編輯）的 YAML，載入到操作中的組織——與 `PUT /admin/config` 走同一條熱重載路徑。請求主體：`{"config_yaml": "<yaml string>"}`。

預覽絕不在伺服器端快取轉換後的 YAML；套用取用的是你提供的 YAML，因此被套用的與被審閱的完全一致。[tool-verified: `provisa/api/admin/import_router.py`]

### Apache Ossie 互通（REQ-1316、REQ-1321）

Provisa 以匯入／匯出邊界的形式與 Apache Ossie（孵化中）互通。

```http
GET  /admin/ossie
POST /admin/ossie/import
```

**匯出**（`GET /admin/ossie`）在每次請求時，都從當下受治理的模型推導出 Ossie YAML 文件——它從不快取，所以不可能過時。回應為 `text/yaml`，並帶有 `Content-Disposition: attachment` 標頭。表變成 `dataset` 物件，欄位變成 `field` 物件，關係則對應到 Ossie 的 `relationship` 物件。（REQ-1321）[tool-verified: `provisa/api/admin/ossie_router.py:download_ossie`]

**匯入**（`POST /admin/ossie/import`）接受 Ossie YAML 或 JSON 文件（格式自動偵測）。它剖析該文件，並以 JSON 物件傳回建議的表與關係註冊——不會註冊任何東西。管理 UI 中的審閱畫面讓你在任何變更操作觸發之前，先接受或刪減這些提議。（REQ-1316）[tool-verified: `provisa/api/admin/ossie_router.py:import_ossie`]

### 物件儲存（REQ-1046、REQ-1048、REQ-1049）

讀取或設定該組織的具體化儲存：

```http
GET  /admin/org-storage
PUT  /admin/org-storage
```

`GET` 回報該組織用掉了平台儲存配額的多少。`PUT` 註冊該組織自己的儲存 DSN（靜態加密；GET 絕不回傳）。一經設定，該組織的具體化便落在它自己的貯體中，不再計入平台配額。送出 `storage_url: null` 會清除設定，把該組織移回平台儲存區。[tool-verified: `provisa/api/admin/org_storage_router.py`]

### 組織加密（REQ-1574）

設定或輪替該組織的靜態加密金鑰：

```http
GET  /admin/org-encryption
PUT  /admin/org-encryption
```

`GET` 傳回金鑰的指紋、id 與來歷——絕不傳回金鑰內容。`PUT` 設定或輪替金鑰。提供 `key_b64`（32 個原始位元組，base64 編碼）可自帶金鑰，省略則由 Provisa 生成一把。此處沒有刪除：淘汰最後一把金鑰，會讓它所包覆過的每一份負載都無法讀取。[tool-verified: `provisa/api/admin/org_encryption_router.py`]

## GraphiQL

管理 API 隨附 GraphiQL，可在瀏覽器中經 `GET /admin/graphql` 開啟（REQ-622）。用它可互動式地探索完整的管理結構描述。

## ops 網域的管理檢視（REQ-1386）

每次安裝都會把八個 SQL 檢視播種進內建的 `ops` 網域。[tool-verified: `provisa/api/startup_seed.py:225-331` `_seed_ops_domain`] 它們把查詢審計記錄公開為受治理的表——可經 SQL (pgwire)、GraphQL 與 Cypher 查詢，並適用與任何業務表相同的網域存取、RLS 與遮罩規則。

播種時 `org_admin` 被指定為 ops 網域的數據管家，因此該網域絕不會在 `stale_metadata` 中顯示為治理缺口。[tool-verified: `startup_seed.py:326-331`]

| 檢視 | 它回答什麼 |
| --- | --- |
| `usage_ranking` | 每張已註冊表的查詢次數與不重複使用者數；零命中的表浮現為淘汰候選 |
| `deprecated_usage` | 對帶有 `deprecated` 標籤之表或欄位的每一次存取——即擋住安全移除的那些活躍消費方 |
| `pii_access` | 對帶有 `pii` 標籤之表或欄位的每一次存取：誰查的、以什麼角色、經哪個介面 |
| `policy_denials` | 所有被治理拒絕的存取嘗試（HTTP 401／403） |
| `surface_mix` | 每個傳輸協定介面的每日查詢次數與不重複使用者數（SQL、GraphQL、Cypher、gRPC 等） |
| `query_health` | 每個介面的每日錯誤次數與平均／最大延遲 |
| `stale_metadata` | 缺少說明的表與欄位；缺少數據管家的網域 |
| `join_hotspots` | 最常被一同查詢的表配對——具體化或快取的候選 |

目前有兩項限制。粒度停在表層級——審計記錄記的是 `table_ids`，而非個別被存取的欄位。查詢文字已加密（REQ-689），並被排除於此處每個檢視之外；它只能經授權的管理解密路徑取得。[tool-verified: `_meta_views.py:148-162` — comment notes `query_text_enc` exclusion]

角色須先具備 `ops` 網域存取權，這些檢視才會可見。授予方式與授予任何其他網域的存取權相同。

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

同樣這些查詢也能以 GraphQL 或 Cypher 形式，在任何受治理的傳輸上執行——pgwire、Arrow Flight 或 Bolt。[inferred from governed-surface design]

## 報表檢視器（REQ-1390）

報表檢視器位於 `/admin/reports`。不具 `observability` 能力的角色無法進入。

左側面板列出 `ops` 網域中每一張已註冊的表，按別名排序。[tool-verified: `ReportsTab.tsx:46-52` — filters `tables` to `domainId === "ops"`] 那八個播種的管理檢視會自動出現在那裡。點擊任一報表，即可在右側受治理的數據檢視器中載入它。

**加入自訂報表。** 「加入報表」按鈕會開啟一個對話方塊。填入名稱、選填說明，以及一段 SELECT 陳述式。儲存後，該檢視便以受治理的衍生表形式註冊進 `ops` 網域——已編目、受存取控制，並與那些播種的檢視並列，可經每個介面查詢。[tool-verified: `ReportsTab.tsx:70-96` — `registerTable` called with `sourceId: DERIVED_SOURCE_ID, domainId: "ops"`]

**刪除。** 垃圾桶圖示只對自訂報表出現。播種的管理檢視無法從此介面刪除。[tool-verified: `ReportsTab.tsx:151` — `const custom = report.sourceId === DERIVED_SOURCE_ID` gates the delete button]

## 表預覽（REQ-1392）

在表頁面展開任一表的資料行。**預覽**按鈕會開啟一個佔寬 90% 的強制回應視窗，顯示該表當下受治理的數據。[tool-verified: `TablePreviewModal.tsx:24` — `size="90%"`; `GovernedTableViewer.tsx` is the underlying viewer]

以帶有必填路徑參數的 API 為底的表，在那些值填妥之前不允許預覽。首次查詢執行之前，會有一份行內表單收集每個必填參數；選填的查詢參數也出現在同一份表單中。[tool-verified: `GovernedTableViewer.tsx:51-55, 153-155` — `requiredParamColumns` check; "paramsRequired" message shown when `activeParams == null`]

## 受治理的數據檢視器（REQ-1391）

同一個檢視器元件同時驅動預覽視窗與報表檢視器。它在兩種脈絡下的行為完全一致。

**伺服器端分頁。** 每一頁都是它自己的一次受治理 `SELECT *`，帶 `LIMIT 101 OFFSET n`。每頁顯示 100 行；第 101 行用來指示是否還有更多。完整數據集絕不會載入瀏覽器。[tool-verified: `nativeParams.ts:72` — `LIMIT ${pageSize + 1} OFFSET ${page * pageSize}`; `types.ts:74` — `PAGE_SIZE = 100`]

**下推的篩選與排序。** 每個欄位標頭都有一個篩選輸入框。篩選詞會變成 `WHERE LOWER(CAST(col AS VARCHAR)) LIKE LOWER('%term%')` 謂詞；點擊排序則產生 `ORDER BY` 子句。兩者都送進資料庫——對十億行的表所下的篩選掃的是數據來源，而不是你眼前那 100 行的頁面。[tool-verified: `nativeParams.ts:53-70`]

**多層分組。** 任一欄位標頭中的圖層圖示，可把該欄位切換進分組。分組欄位排在 `ORDER BY` 最前，好讓跨頁時群組成員與其標頭落在同一頁。主索引鍵欄位會被附加在後，作為穩定的決勝依據。[tool-verified: `nativeParams.ts:61-70` — group columns first, then explicit sorts, then PKs] 群組標頭資料行可收合；收合會隱藏成員，且不會發出新的查詢。[tool-verified: `useResultsGrid.ts:150-171` — `collapsedGroups` set gates the `build()` recursion]

**選擇會保留。** 篩選、排序與分組設定會以 `provisa.grid.table:<domain>.<table>` 為索引鍵保存到 `localStorage`，並在下次造訪時還原。[tool-verified: `useResultsGrid.ts:95-98`, `GovernedTableViewer.tsx:66`]

**匯出。** 可把目前這一頁下載為 CSV，或以定位字元分隔的文字複製到剪貼簿。匯出只涵蓋可見的那一頁。[tool-verified: `useResultsGrid.ts:247-274` — both handlers iterate `displayRows`, which in server-paged mode is the current page]
