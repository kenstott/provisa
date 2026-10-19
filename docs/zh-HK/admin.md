# 管理 API {: #admin-api }

管理 API 是位於 `POST /admin/graphql` 的 Strawberry GraphQL 端點。它需要 superuser 或 admin 角色，且與數據用的 GraphQL 端點分開。

## 身份驗證 {: #authentication }

使用標準的 Provisa 驗證提供者，在 `Authorization` 標頭中傳入你的憑證：

```yaml
Authorization: Bearer <token>
```

管理存取由指派給角色的 `admin` 能力管束。

### 個人存取權杖 {: #personal-access-tokens }

凡是接受 bearer 權杖的地方都接受個人存取權杖，本端點亦不例外。你自己簽發及撤銷自己的權杖，因為權杖是持有者本人的憑證。所以它位於管理介面中你的使用者資料頁，與退出組織、刪除帳戶並列，而不在管理員頁面下。管理員從不代他人簽發權杖。

| 路由 | 作用 |
| ------- | -------- |
| `POST /auth/tokens` | 為呼叫者鑄造一個權杖。主體：`name`，選填 `role_id`、`scopes`、`expires_in_days`（1–366）。回應是該密鑰唯一現身之處 |
| `GET /auth/tokens` | 呼叫者在此組織中的有效權杖——顯示前綴、名稱、生命週期時間戳記，以及撤銷時用來指認權杖的雜湊值。絕不是可用的憑證 |
| `DELETE /auth/tokens/{token_hash}` | 撤銷呼叫者的一個權杖。權杖不屬於他或已撤銷時回 404 |

省略 `role_id` 會讓權杖解析到其擁有者所持有的任何角色；指名一個則把權杖收窄到低於其擁有者。撤銷也會隱含發生：移除某使用者的組織成員資格，即撤銷他在該組織的各個權杖。憑證本身見 [安全模型](security.md#personal-access-tokens)。

## 各項能力 {: #capabilities }

### 設定管理 {: #config-management }

下載目前執行中的設定：

```http
GET /admin/config
```

以 YAML 檔案傳回完整的 `config.yaml`。上傳新的設定：

```http
PUT /admin/config
```

Provisa 會驗證該 YAML、重新載入目錄並重新生成結構描述。毋須重新啟動。

### 執行階段設定 {: #runtime-settings }

毋須編輯設定檔即可讀寫執行階段的平台設定：

```http
GET  /admin/settings
PUT  /admin/settings
```

設定介面涵蓋大型結果重新導向、預設取樣與行數上限、回應快取 TTL、命名慣例、關係外部索引鍵自動追蹤、具體化儲存區 DSN、聯邦引擎記憶體（`jvm_heap_gb`、`query_max_memory`、`query_max_memory_per_node`、`query_max_total_memory`、`fault_tolerant_execution`、`fault_tolerant_task_memory`、`exchange_spool_dir`），以及完整的 OpenTelemetry 追蹤管線調校介面。遠端 GraphQL 的走訪上限與溫層／讀取快取設定亦一併公開。

**連接器伺服器**卡片為每個附有隨附伺服器的連接器（File Crawler、SharePoint、Salesforce、ServiceNow、Cloud Inventory、Splunk、AskAmerica）命名一個資料夾，其中存放為此主機建置的伺服器套件包（`connectors.<connector>.bundle_dir`，環境變數 `PROVISA_PGWIRE_BUNDLE_<CONNECTOR>`）。為空時，連接器伺服器從固定的 `kenstott/calcite` release 取得。已設定時，資料夾依原樣使用：其啟動程式必須位於 `bin/pgwire-<connector>`；沒有該啟動程式的資料夾將阻止連接器伺服器啟動，並顯示指出該設定的錯誤 — release 不會在其位置被取得。該設定在每次連接器伺服器啟動時讀取。

安全態勢 `security.mode`（`standard` | `high`）在重新啟動時生效：

```http
GET  /admin/security
PUT  /admin/security
```

AI 模型分配、嵌入與向量模型登錄表以及 NL 速率限制從下一個請求起生效，無須重新啟動：

```http
GET  /admin/ai-models
PUT  /admin/ai-models
```

管理介面的加密分頁會即時從加密註冊表推導出提供者清單；不可用的提供者會顯示出來，但無法選取。

`GET`/`HEAD /health` 及 `GET /setup/status` 從不要求認證。即使設定了認證提供者，它們亦跳過 `Authorization: Bearer` 要求。

### 聯邦引擎 {: #federation-engine }

讀取或變更此部署所使用的引擎：

```http
GET  /admin/federation-engine
PUT  /admin/federation-engine
```

`GET` 傳回目前啟用的引擎索引鍵，以及它所需的設定欄位。`PUT` 接受一個帶有 `engine`（索引鍵）及任何引擎專屬欄位的主體；該選擇會保存到平台設定，並於服務下次重新啟動時繫結。

### 關係編輯器 {: #relationship-editor }

列出關係：

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

建立關係。已存在的 id 會被拒絕；關係透過 `updateRelationship` 修改，其接受相同的輸入：

```graphql
mutation {
  createRelationship(input: {
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

宣告一個由聯結資料表支撐的關係：

```graphql
mutation {
  createRelationship(input: {
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

關聯表由你宣告為邊，Provisa 從不自動發現。`viaTable` 指定一張已登錄的表。它的兩個鍵欄承載這條邊，其餘每一欄都成為該關係的屬性，可以像任何欄位一樣篩選。`viaTypeColumn` / `viaTypeValue` 把一張連接表拆分成多種邊類型。例如，`pet_companions` 中 `companion_type` 為 `bonded pair`、`littermate` 或 `shares enclosure` 的列，會在同一對表之間形成三個不同的關係。

`viaLabelSource` 指定對外名稱從哪裏來，Cypher 拿到的每種形式都是大寫加底線。`column` 使用 `viaTypeValue`（`BONDED_PAIR`），`table` 使用連接表自己的名稱（`PET_COMPANIONS`），`fixed` 使用宣告的 `alias`。這樣宣告的連接表是一條邊。Provisa 會把它從節點標籤中去掉，所以它永遠不會以節點標籤塊出現在圖介面中。

### AI 關係探索 {: #ai-relationship-discovery }

經 REST 觸發由 Claude 驅動的外部索引鍵分析：

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

### 結構描述內省 {: #schema-introspection }

瀏覽所有數據來源上已發佈的表：

```graphql
query {
  tables {
    id
    sourceId
    columns {
      columnName
      unmaskedTo
    }
    writeGrants {
      role
      operation
      columns
      rows
      check
      presets { column source name value }
    }
  }
}
```

### 欄位依賴檢查 {: #column-dependency-check }

在儲存一項會更改欄位 SQL 別名或刪除欄位的表編輯之前，先問問還有什麼引用它：

```graphql
query {
  columnDependents(tableId: "42", renamed: ["order_total"], removed: ["legacy_code"]) {
    columnName
    dependents { kind name detail breaksOn }
  }
}
```

重新命名別名會破壞所有基於對外名稱編寫的製品：視圖、MV、指標運算式、RLS 謂詞及 DQ 契約。刪除欄除了破壞這些之外，還會破壞儲存實體 `column_name` 的製品，即關係、詞彙表綁定及標籤指派。`breaksOn` 會說明是哪一種。「表」頁面在儲存時執行這項檢查，並以提示對話框顯示結果。[血緣](lineage.md) 說明了查詢涵蓋甚麼、不能涵蓋甚麼。

### 檢視管理 {: #view-management }

註冊一個具體化檢視：

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

觸發一次手動重新整理：

```graphql
mutation {
  refreshMv(mvId: "orders-with-customers") {
    success
  }
}
```

### 圖形數據來源註冊 {: #graph-source-registration }

Neo4j 與 SPARQL 數據來源經 REST 端點註冊（而非 GraphQL 管理 API）：

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

註冊之後，這些表便出現在 GraphQL 結構描述中，可像任何其他數據來源一樣查詢。

### Hasura／DDN 匯入 {: #hasura-ddn-import }

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

**套用** 會把你審閱過（如果願意亦可編輯過）的 YAML 載入到目前操作的組織中。它走的是與 `PUT /admin/config` 相同的熱重載路徑。請求主體：`{"config_yaml": "<yaml string>"}`。

預覽絕不在伺服器端快取轉換後的 YAML；套用取用的是你提供的 YAML，因此被套用的與被審閱的完全一致。

### Apache Ossie 互通 {: #apache-ossie-interchange }

Provisa 以匯入／匯出邊界的形式與 Apache Ossie（孵化中）互通。

```http
GET  /admin/ossie
POST /admin/ossie/import
```

**匯出**（`GET /admin/ossie`）在每次請求時都從即時的受治理模型建構 Ossie YAML 文件。不做任何快取，所以文件永遠不會過時。回應為 `text/yaml`，帶有 `Content-Disposition: attachment` 標頭。文件是一個模型，根部包含 `version`、`name`、`datasets`、`relationships` 及 `metrics`。表變為 `dataset` 物件，欄變為 `field` 物件，關係對應為 Ossie 的 `relationship` 物件。沒有任何表的模型會以 `422` 被拒絕。

**匯入**（`POST /admin/ossie/import`）接受 YAML 或 JSON 格式的 Ossie 文件，並自行識別格式。它剖析文件，以 JSON 物件傳回擬議的表、關係及指標登錄。不會登錄任何東西。它只讀取扁平的文件形態，把模型包在 `semantic_model` 中的文件會被點名拒絕。在管理介面中，審閱頁面讓你在發生任何變更之前接受或刪減這些提議。

### 物件儲存 {: #object-storage }

讀取或設定該組織的具體化儲存：

```http
GET  /admin/org-storage
PUT  /admin/org-storage
```

`GET` 回報該組織用掉了平台儲存配額的多少。`PUT` 註冊該組織自己的儲存 DSN（靜態加密；GET 絕不回傳）。一經設定，該組織的具體化便落在它自己的貯體中，不再計入平台配額。送出 `storage_url: null` 會清除設定，把該組織移回平台儲存區。

### 組織加密 {: #org-encryption }

設定或輪替該組織的靜態加密金鑰：

```http
GET  /admin/org-encryption
PUT  /admin/org-encryption
```

`GET` 傳回金鑰的指紋、id 及來源，從不傳回金鑰材料。`PUT` 設定或輪換金鑰。提供 `key_b64`（32 個原始位元組，base64 編碼）即可使用你自己的金鑰；省略它則由 Provisa 產生一個。沒有刪除操作，因為停用最後一把金鑰會令它包裹過的每個負載都無法讀取。

## GraphiQL

管理 API 隨附 GraphiQL，可在瀏覽器中經 `GET /admin/graphql` 開啟。用它可互動式地探索完整的管理結構描述。

## ops 網域的管理檢視 {: #ops-domain-management-views }

每次安裝都會在內置的 `ops` 領域中植入八個 SQL 視圖。它們把查詢稽核日誌以受治理表的形式公開。你可以透過 SQL（pgwire）、GraphQL 及 Cypher 查詢它們，適用的領域存取、RLS 及遮罩規則與任何業務表相同。

播種時 `org_admin` 被指定為 ops 網域的數據管家，因此該網域絕不會在 `stale_metadata` 中顯示為治理缺口。

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

粒度止於表級。稽核日誌記錄 `table_ids`，不記錄查詢讀取的具體欄。查詢文字經過加密，不出現在這裏的任何視圖中；只有經授權的管理員解密路徑才能讀取它。

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

由於這些視圖是受治理表，你亦可以用 GraphQL 或 Cypher 編寫同樣的查詢。

## 報表檢視器 {: #reports-viewer }

報表檢視器位於 `/admin/reports`。不具 `observability` 能力的角色無法進入。

左側面板列出 `ops` 網域中每一張已註冊的表，按別名排序。 那八個播種的管理檢視會自動出現在那裡。點擊任一報表，即可在右側受治理的數據檢視器中載入它。

**加入自訂報表。** 「Add report」按鈕會開啟一個對話框，你在其中填寫名稱、選填的描述及一條 SELECT 陳述式。儲存後，該視圖會作為受治理的衍生表登錄到 `ops` 領域中。它會進入目錄、受存取控制，並可透過所有介面查詢，與預置視圖並列。

**刪除。** 垃圾桶圖示只對自訂報表出現。播種的管理檢視無法從此介面刪除。

## 表預覽 {: #table-preview }

在表頁面展開任一表的資料行。**預覽**按鈕會開啟一個佔寬 90% 的強制回應視窗，顯示該表當下受治理的數據。

以帶有必填路徑參數的 API 為底的表，在那些值填妥之前不允許預覽。首次查詢執行之前，會有一份行內表單收集每個必填參數；選填的查詢參數也出現在同一份表單中。

## 受治理的數據檢視器 {: #governed-data-viewer }

同一個檢視器元件同時驅動預覽視窗與報表檢視器。它在兩種脈絡下的行為完全一致。

**伺服器端分頁。** 每一頁都是它自己的一次受治理 `SELECT *`，帶 `LIMIT 101 OFFSET n`。每頁顯示 100 行；第 101 行用來指示是否還有更多。完整數據集絕不會載入瀏覽器。

**下推的篩選與排序。** 每個欄標題都有一個篩選輸入框。篩選詞會變成 `WHERE LOWER(CAST(col AS VARCHAR)) LIKE LOWER('%term%')` 謂詞，點擊排序會產生 `ORDER BY` 子句。兩者都在資料庫中執行。對十億列的表篩選會掃描資料來源，範圍從不局限於你眼前那一頁的 100 列。

**多層分組。** 任一欄位標頭中的圖層圖示，可把該欄位切換進分組。分組欄位排在 `ORDER BY` 最前，好讓跨頁時群組成員與其標頭落在同一頁。主索引鍵欄位會被附加在後，作為穩定的決勝依據。 群組標頭資料行可收合；收合會隱藏成員，且不會發出新的查詢。

**選擇會保留。** 篩選、排序與分組設定會以 `provisa.grid.table:<domain>.<table>` 為索引鍵保存到 `localStorage`，並在下次造訪時還原。

**匯出。** 可把目前這一頁下載為 CSV，或以定位字元分隔的文字複製到剪貼簿。匯出只涵蓋可見的那一頁。
