# API 參考 {: #api-reference }

## 概觀 {: #overview }

Provisa 在兩個前綴下公開 REST 端點：`/data` 負責查詢執行與結構描述自省，`/admin` 負責組態管理。 大部分數據端點需要角色識別碼。管理員組態操作使用位於 `/admin/graphql` 的 Strawberry GraphQL API。

---

## 驗證 {: #authentication }

當 `provisa.yaml` 中設定了 `auth.provider` 時，除 `/health` 與 `/setup/status` 以外的所有端點都需要 `Authorization: Bearer <token>` 標頭。

未設定驗證時，伺服器以開發模式執行。任何請求都被視為 `anonymous` 身分，該身分對映到所有已設定的角色，並具備萬用字元網域存取權。

**登入（`POST /auth/login`）** 由使用中的驗證提供者在設定 `provider: basic` 時提供。 憑證格式與回應取決於提供者。

**身分自省：**

```http
GET /auth/me
```

傳回已驗證使用者的 id、電郵、顯示名稱、組織成員資格與角色指派。在開發模式下傳回 `dev_mode: true`，並列出所有角色 ID。

```http
GET /auth/provider-type
```

傳回 `{"provider": "<name>"}`，或在未設定驗證時傳回 `{"provider": null}`。

---

## 數據端點 {: #data-endpoints }

### `POST /data/graphql`

執行 GraphQL 查詢或變更操作。

**請求主體：**

```json
{
  "query": "{ orders(where: {region: {eq: \"us\"}}) { id amount } }",
  "variables": {},
  "role": "org_admin",
  "extensions": {}
}
```

請求以其執行角色運作：已驗證使用者的角色，或在開發模式（沒有身分驗證）下取 `X-Provisa-Role` 標頭，沒有時為 `org_admin`。主體中的 `role` 欄位並不選擇角色。若存在，它必須與執行角色一致；不同的值會傳回 `400`，代碼為 `data.role_mismatch`，並指名兩個角色。同一規則適用於 `POST /data/sql`、`POST /data/query` 及 `POST /query/nl`。

`extensions` 欄位支援自動持續化查詢（APQ）通訊協定：

```json
{
  "extensions": {"persistedQuery": {"sha256Hash": "<sha256-of-query>"}}
}
```

**標頭：**

- `X-Provisa-Role` — 要代為執行的角色（開發模式）；啟用身分驗證時為角色要求，只有在該角色已指派給使用者時才會生效
- `Accept` — 回應格式（見內容協商）
- `Authorization` — 啟用驗證時為 `Bearer <token>`
- `X-Provisa-Redirect-Format` — S3 重新導向輸出的 MIME 類型
- `X-Provisa-Redirect-Threshold` — 觸發重新導向的資料列數門檻
- `X-Provisa-Redirect` — 設為 `true` 可無條件強制重新導向

**回應快取：** 透過操作層級指令 `@cached` 按請求選擇啟用（`query @cached(ttl: 60) { ... }`，`ttl` 可選）；同一指令亦適用於經 Arrow Flight 發送的 GraphQL。Cypher 以 `// @provisa cache=true` 或 `// @provisa cache_ttl=N` 行選擇啟用，gRPC 以呼叫元數據 `x-provisa-cache: true` 或 `x-provisa-cache-ttl: N` 選擇啟用。沒有該指令時，請求既不讀取也不寫入回應快取。回應標頭 `X-Provisa-Cache` 報告 `HIT` 或 `MISS`。請參閱[快取](configuration.md#per-request-opt-in)。

<a id="response-cache-request-headers"></a>

#### 回應快取請求標頭 {: #response-cache-request-headers }

`/data/graphql`、`/data/cypher`、Neo4j Query API（`/db/{database}/query/v2`）、`/data/rest` 和 `/data/jsonapi` 也接受以請求標頭形式選擇啟用。下列每種形式的行為都與 `@cached(ttl: N)` 相同：

| 請求標頭 | 等同於 |
| --- | --- |
| `Cache-Control: max-age=N` / `Cache-Control: max-stale=N` | `@cached(ttl: N)` |
| `Cache-Control: max-stale`（無值） | `@cached`（營運方的 TTL） |
| `Cache-Control: no-cache` / `no-store` | 不快取，與不發送任何內容相同 |
| `x-provisa-cache: true` | `@cached` |
| `x-provisa-cache-ttl: N` | `@cached(ttl: N)` |

沒有上述請求標頭、文字中也沒有提示的請求不會被快取，儘管一般 HTTP 快取預設會快取。營運方的快取設定仍與 `@cached` 一樣適用：請求標頭不會令讀取比這些設定允許的更新，也不會因為要求更長的 `ttl` 而被拒絕。其他 `Cache-Control` 指令（`no-transform`、`min-fresh` 等）會被忽略，這是 RFC 9111 對快取的要求。

一個請求可以多次聲明選擇啟用：在其文字中和請求標頭中，或在兩個請求標頭中。若一致，則按要求快取一次。若不一致（例如 `@cached(ttl: 60)` 搭配 `Cache-Control: max-age=5`，或 `x-provisa-cache-ttl: 10` 搭配 `Cache-Control: max-age=20`），請求將以 `400` 和錯誤碼 `data.invalid_cache_control` 失敗，並指明所發送的每種形式。`N` 不是整數秒的值同樣失敗。不會偏向任何一種形式。

```bash
curl -H 'Cache-Control: max-age=120' -H 'X-Provisa-Role: analyst' \
  'http://localhost:8001/data/rest/sales/orders?limit=10'
```

**回應（內嵌 JSON）：**

```json
{
  "data": {
    "orders": [
      {"id": 1, "amount": 99.99}
    ]
  }
}
```

**回應（重新導向）：**

```json
{
  "data": {"orders": null},
  "redirect": {
    "redirect_url": "https://...",
    "row_count": 50000,
    "expires_in": 3600,
    "content_type": "application/vnd.apache.parquet"
  }
}
```

**回應（多根欄位，內嵌與重新導向混合）：**

```json
{
  "data": {
    "orders": [{"id": 1}],
    "customers": null
  },
  "redirects": {
    "customers": {
      "redirect_url": "https://...",
      "row_count": 10000,
      "expires_in": 3600,
      "content_type": "application/vnd.apache.parquet"
    }
  }
}
```

多根欄位查詢會各自獨立執行每個根欄位。低於重新導向門檻的欄位以內嵌方式傳回；高於門檻的欄位則重新導向。`redirects` 索引鍵（複數）將欄位名稱對映到重新導向資訊。

**快取標頭：**

- `X-Provisa-Cache: HIT|MISS`
- `X-Provisa-Cache-Age: <seconds>`（HIT 時）

**必要能力：** 所有請求（包括自省）皆需 `QUERY_DEVELOPMENT`。

---

### 內容協商 {: #content-negotiation }

| Accept 標頭 | 格式 |
| --- | --- |
| `application/json` | JSON（預設） |
| `application/x-ndjson` | 換行分隔的 JSON |
| `text/csv` | CSV |
| `application/vnd.apache.parquet` | Parquet |
| `application/vnd.apache.arrow.stream` | Arrow IPC |

---

### 重新導向 {: #redirect }

超出已設定資料列門檻的結果（或當 `X-Provisa-Redirect: true` 時）會寫入 S3，並傳回預先簽署的 URL。

| 重新導向格式 | 寫入方 | 記憶體 |
| --- | --- | --- |
| `application/vnd.apache.parquet` | 聯邦 CTAS | 無 — 數據從不經過 Provisa |
| `application/x-orc` | 聯邦 CTAS | 無 — 數據從不經過 Provisa |
| `application/json` | Provisa | 受記憶體限制 |
| `application/x-ndjson` | Provisa | 受記憶體限制 |
| `text/csv` | Provisa | 受記憶體限制 |
| `application/vnd.apache.arrow.stream` | Provisa | 受記憶體限制 |

對於大型分析匯出，請使用 Parquet 或 ORC 重新導向。聯邦引擎會並行直接寫入 S3，數據不經過 Provisa。

```yaml
X-Provisa-Redirect-Format: application/vnd.apache.parquet
X-Provisa-Redirect-Threshold: 1000
```

---

### `POST /data/sql`

透過第 2 階段治理管線執行原始 SQL。

**請求主體：**

```json
{
  "sql": "SELECT id, amount FROM orders WHERE region = 'us'",
  "role": "org_admin"
}
```

`params`（可選）按次序攜帶陳述式 `$1…$n` 預留位置的值，與其文字分開：

```json
{
  "sql": "SELECT id, amount FROM orders WHERE region = $1 AND amount > $2",
  "params": ["us", 100]
}
```

這些值在語句執行時由引擎或數據來源的驅動程式繫結，從不寫進語句本身，所以值無法改變語句是甚麼。它們只隨一條語句一起傳送，該語句的預留位置與它們逐一編號對應。語句批次、沒有值的預留位置，或沒有預留位置接收的值，都會在語句受治理或執行之前以 `400` 及代碼 `data.sql_parameters_do_not_fit` 被拒絕。沒有預留位置的語句與以前一樣，不帶 `params` 傳送。

**必要能力：** `QUERY_DEVELOPMENT`。

`POST /data/sql` 上的治理違規會傳回 HTTP 403。

**回應：** 與 `/data/graphql` 格式相同（預設為 JSON 資料列，並透過 `Accept` 進行內容協商）。

---

### `POST /data/query`

所有查詢語言共用一個端點。它接受 GraphQL、SQL 或 Cypher，並自動識別收到的是哪一種。

Cypher 查詢也可以提交到僅限 Cypher 的 `POST /query/cypher` 端點。

**請求主體：**

```json
{
  "query": "{ orders { id } }",
  "params": {},
  "variables": {},
  "role": "org_admin"
}
```

GraphQL 傳回 `{"data": ...}`，SQL 與 Cypher 傳回 `{"columns": [...], "rows": [...]}`。

---

### `POST /data/sql/explain`

透過受治理的管線說明或分析 SQL 陳述式。

該端點把**受治理的** SQL 包進方言的 EXPLAIN 語法中：亦即在呼叫者角色下、經過 RLS 及遮罩之後真正執行的語句。所以您看到的計劃對應的是查詢的授權版本，而不是原始輸入。

**請求主體：**

```json
{
  "sql": "SELECT id, amount FROM orders",
  "role": "org_admin",
  "analyze": false
}
```

設定 `analyze: true` 可執行 EXPLAIN ANALYZE。查詢會實際執行，計劃會帶有真實的資料列數與時間。並非每種方言都支援 ANALYZE；請參閱 [查詢計劃與統計資料](engines.md#query-plans-and-statistics) 中的表格。

**回應：** `{"plan": "<plan text or JSON>", "dialect": "trino", "analyzed": false}`

當方言不支援 EXPLAIN，或在不支援的方言（例如 SQLite）上要求 `analyze: true` 時傳回 `400`。

---

### `GET /data/engine/state`

在不喚醒引擎的情況下傳回引擎分區的目前狀態。

UI 會輪詢該端點，在引擎冷啟動期間顯示啟動橫額。它從不喚醒引擎，所以輪詢是安全的，亦不會被閒置回收機制算作活動。

**回應：**

```json
{"state": "ready"}
```

可能的值：

| 狀態 | 意義 |
| --- | --- |
| `always-on` | 桌面版、自行託管或自備協調器 — 無生命週期管理 |
| `ready` | 分區已啟動並接受查詢 |
| `starting` | 冷啟動進行中 |
| `stopped` | 分區已縮減至零 |

---

### `POST /data/engine/prewarm`

在不執行查詢的情況下觸發引擎喚醒。

立即傳回 `202 Accepted`，喚醒在背景進行。如您希望引擎在第一個查詢到來之前已準備好，就呼叫它，例如從幾分鐘後才執行查詢的排程器中呼叫。

**回應：** `202 Accepted`，主體為 `{"started": true}`

---

### `GET /data/rest/{domain_id}/{table_name}`

為每個已註冊資料表自動產生的純 REST 端點。查詢字串會對映到 GraphQL 引數，請求會透過與 GraphQL 相同的管線（RLS、遮罩、路由）編譯並執行。

**查詢參數：**

- `limit` — 最大資料列數（≥ 1）
- `offset` — 略過的資料列數（≥ 0）
- `fields` — 以逗號分隔的欄位名稱，採用 GraphQL 結構描述的拼寫，在預設命名慣例下為 camelCase，例如 `?fields=orderId,amount`（預設：所有純量欄位）。`filter` 及 `orderBy` 中的欄位名稱遵循相同拼寫。JSON:API 則接受實體欄位名稱（`fields[orders]=order_id`）。
- `filter` — `{"field", "comparator", "value"}` 篩選物件的 JSON 陣列。值按其欄的類型讀取，因此數值欄上的 `"100"` 按 100 比較；欄類型無法容納的值返回 `400`。
- `orderBy` — `{"field", "direction"}` 排序物件的 JSON 陣列
- `params` — 表的參數，作為一個按參數名索引的 JSON 物件，例如 `?params={"region":"EU"}`。基於 API 登記的表或查詢表，其來源接受的每個輸入都有一個參數。缺少必填參數的讀取返回 `400` 並指出其名稱，表中沒有的參數名同樣如此。每個值都按其參數的類型檢查。

篩選值和參數值作為變數隨端點構建的 GraphQL 文本一起傳遞，因此含引號的值按原樣比較。

需要已驗證的角色；未驗證的請求傳回 `401`。這些路由的 OpenAPI 規格由 `GET /data/rest/openapi.json` 提供，Swagger UI 位於 `GET /data/rest/docs`。

**回應快取：** 透過請求標頭選擇啟用，`Cache-Control: max-age=N` 或 `x-provisa-cache: true`（參見[回應快取請求標頭](#response-cache-request-headers)）。每個回應都帶有 `X-Provisa-Cache: HIT|MISS`，命中時還帶有 `X-Provisa-Cache-Age`。

#### OpenAPI / Swagger UI 探索工具 {: #openapi-swagger-ui-explorer }

OpenAPI 瀏覽器頁面（`/app/openapi`）在沙盒化的 iframe 中嵌入 Swagger UI。規格按角色限定，只顯示當前角色能看到的資料表及資料行，亦可以用領域選擇器進一步篩選。UI 會自動在淺色及深色主題之間切換。

頁面用 `fetch()` 載入規格 HTML，而不是直接設定 iframe 的 `src`。這樣請求會攜帶工作階段的 bearer 權杖，Swagger UI 自身的相對請求亦會基於同一來源解析。

從 NL 的「在 OpenAPI 中開啟」連結進入時，頁面會展開目標端點，用 NL 產生的 URL 填好查詢參數（例如 `aggregate`、`groupBy`），然後點擊 Execute，並輪詢 DOM，確保每一步完成後才開始下一步。

---

### `GET /data/jsonapi/{domain_id}/{table_name}`

為每個已註冊資料表自動產生、符合 [JSON:API](https://jsonapi.org) 規範的端點。與 GraphQL 採用相同的 RLS、遮罩與路由。

**`Accept` 標頭：** 必須包含 `application/vnd.api+json`（JSON:API 媒體類型），否則請求傳回 `406`。

**查詢參數：**

- `fields[<type>]` — 稀疏欄位集，例如 `?fields[orders]=amount`。名稱是實體欄位名稱，例如 `fields[orders]=order_id`，與 `filter[...]`、`sort` 以及傳回屬性的鍵相同。REST 則接受 GraphQL 拼寫（`fields=orderId`）。
- `filter[<col>]` / `filter[<col>][<op>]` — 例如 `?filter[region]=US`、`?filter[amount][gt]=100`。值按其欄的類型讀取；類型無法容納的值返回 `400`。
- `params` — 表的參數，作為一個按參數名索引的 JSON 物件，例如 `?params={"region":"EU"}`，與 REST 端點相同。缺少必填參數返回 `400` 並指出其名稱；未知名稱或類型錯誤的值返回 `400`，`source.parameter` 為 `params`。分頁連結保留該參數。
- `sort` — 以逗號分隔，前綴 `-` 表示遞減，例如 `?sort=-created_at,amount`
- `page[number]` / `page[size]` — 分頁
- `page[total]` — `true` 計算與過濾條件相符的資料列數目，並傳回 `meta.total` 及 `links.last`。不帶它時，回應只攜帶 `links.self`、`links.first`、`links.prev` 及 `links.next`，資料庫不會執行計數。`true` 或 `false` 以外的任何值會傳回 `400`。回應中的連結會保留該參數。
- `aggregate` — 以逗號分隔的彙總函式，取代資料列擷取執行：`count`、`sum`、`avg`、`stddev`、`variance`、`min`、`max`。使用 `?aggregate=count,sum` 可要求其中一部分。彙總回應傳回 `data: null`，結果放在 `meta.aggregate` 中。
- `groupBy` — 以逗號分隔的欄位名稱；與 `?aggregate=` 搭配使用以分組結果。只有資料表 `DistinctOnColumn` 列舉中的欄位有效；角色看不到的欄位，伺服器一律傳回 `400`。
- `includeNodes` — 設為 `true` 可在每個群組資料列的 `nodes` 陣列中包含基礎資料表的純量欄位（以及 `include=` 中指名的聯結維度純量）。當自然語言分組查詢同時要求維度明細時為必要。

- `include` — 以逗號分隔的關係名稱，使用實體拼寫，例如 `?include=order_items,shipment`：每個相關資源在 `included` 中只傳回一次，每個主要資源在 `relationships.<name>.data` 下連結它包含的內容（一對多關係為資源識別碼清單，一對一關係為一個識別碼或 `null`）。

回應是帶有 `type`/`id`/`attributes` 的資源物件。資源的 `id` 是其資料表的主鍵：鍵欄位的值；對於複合鍵，則是按鍵順序以 `:` 連接的各欄位值（主鍵為 `(order_id, line)` 的資料表中，訂單 7 的第 2 行是 `7:2`）。值中的 `:` 或 `\` 用 `\` 跳脫。沒有宣告主鍵的資料表使用其 `id` 欄位；兩者都沒有的資料表傳回 `400`，到達這種資料表的 `include` 也是如此。 錯誤遵循 JSON:API 錯誤物件的格式。

**回應快取：** 透過請求標頭選擇啟用，`Cache-Control: max-age=N` 或 `x-provisa-cache: true`（參見[回應快取請求標頭](#response-cache-request-headers)）。每個回應都帶有 `X-Provisa-Cache: HIT|MISS`，命中時還帶有 `X-Provisa-Cache-Age`。兩種形式不一致時，以 JSON:API 錯誤物件傳回 `400`。

#### JSON:API 探索工具 {: #jsonapi-explorer }

JSON:API 探索工具頁面（`/app/jsonapi`）是這些端點之上的瀏覽器 UI。從依網域分組的清單中選取一個資料表，然後設定：

- **Fields** — 選擇要包含哪些欄位（稀疏欄位集）；全部不勾選則要求所有欄位
- **Relationships** — 選取由外部索引鍵衍生的關聯名稱，以 `?include=` 一併載入
- **Filter** — 欄位、運算子（`eq`、`neq`、`gt`、`gte`、`lt`、`lte`、`like`）與值
- **Sort** — 單一欄位，遞增或遞減
- **Aggregate** — 從伺服器驗證過的清單中挑選分組欄位，然後勾選一個或多個彙總函式；選取分組欄位後，「Include nodes」核取方塊會將基礎資料表的純量欄位附加到每一列
- **Page size** — 每頁資源數，附首頁／上一頁／下一頁／末頁導覽

結果以格式化摘要檢視（帶可點擊關聯錨點的資源卡片）或原始 JSON 分頁呈現。即時請求 URL 會顯示出來並可複製。資料表選取與每頁大小會透過 `localStorage` 跨工作階段保留。

從自然語言的「Open in JSON:API」連結導覽而來時，探索工具會預先選取資料表，並依自然語言產生的查詢參數預填彙總挑選器，然後自動執行請求。

---

### `POST /query/nl`

提交自然語言問題。服務會啟動非同步作業，並立即傳回 `202 Accepted` 與一個 `job_id`。需要在 `ai_models` 組態區段下設定 LLM 提供者。

**請求主體：**

```json
{"q": "How many orders were placed last month?", "role": "org_admin"}
```

傳回 `{"job_id": "<id>"}`。超出每個角色的自然語言速率限制時傳回 `429`，並附帶 `Retry-After` 標頭。

**擷取結果：**

- `GET /query/nl/{job_id}` — 輪詢。傳回作業文件。
- `GET /query/nl/{job_id}/stream` — SSE。每個產生目標完成時發出一個 `branch` 事件，最後發出 `done` 事件。

三個產生迴圈（Cypher、GraphQL、SQL）並行執行，各自經編譯器驗證並在出錯時修正。 提示詞的範圍限定在該角色可見的結構描述內。 結果文件依目標為每個分支建立索引鍵：

```json
{
  "job_id": "<id>",
  "state": "complete",
  "branches": {
    "cypher":  {"query": "MATCH ...", "result": [...], "error": null},
    "graphql": {"query": "{ ... }",   "result": {...}, "error": null},
    "sql":     {"query": "SELECT ...", "result": [...], "error": null}
  }
}
```

耗盡迭代次數的分支傳回 `query: null`、`result: null` 及一個 `error` 字串。每個產生的查詢都以使用者的權限執行，並套用第 2 階段治理；服務從不繞過治理。

#### 帶維度明細的自然語言分組查詢 {: #nl-group-by-with-dimension-details }

以一個同時投影所聯結維度表資料行的 NL 分組查詢為例，例如「按使用者統計查詢次數，並顯示使用者名稱及電郵」。執行器會從 SELECT 中的維度資料行推導出每個欄位的點路徑（`dim_paths`）。這些路徑會填入為 JSON:API 及 OpenAPI 面板產生的 URL 中的 `includeNodes=` 參數，令這些面板請求與 SQL、GraphQL 分支所解析的相同維度欄位。沒有它們，`includeNodes=true` 只會傳回基礎彙總表自身的純量欄位。

在 gRPC 面板上，產生的 `{Type}GroupByRequest` 帶有 `include_nodes`（布林值）與 `include`（關聯欄位名稱的重複字串）。傳回的 `{Type}GroupByRow` 包含帶有維度明細資料列的具型別 `nodes` 欄位。

---

### `GET /data/sdl`

傳回某個角色結構描述的 GraphQL SDL。

**標頭：** `X-Provisa-Role: <role_id>`（必要）

**查詢參數：**

- `domain` — 以逗號分隔的網域 ID。設定後，回應會篩選為指名網域及可從其抵達的資料表。

**回應：** `text/plain` GraphQL SDL。

---

### `GET /data/introspection`

傳回 GraphQL 自省 JSON，可選擇依網域篩選。

**標頭：** `X-Provisa-Role: <role_id>`（必要）

**查詢參數：** `domain` — 以逗號分隔的網域 ID。

**回應：** `application/json` 自省結果。

---

### `GET /data/graph-schema`

傳回該角色結構描述的圖形檢視：節點標籤及其關聯類型，供 Cypher／圖形用戶端使用。每個節點標籤包含 `pk_columns`，讓呼叫者可判斷主索引鍵欄位。

**回應：** `application/json`，包含 `node_labels`（各自帶有 `pk`/`pk_columns`）與 `relationship_types`。

每種關係類型還帶有 `junction_table_name` 及 `properties`。對於經由關聯表的邊，前者指明它所穿過的關聯表，後者列出該表可作為 `r.attr` 讀取、可在 `WHERE` 中篩選的資料行。對於基於外鍵的邊，名稱為 `null`，屬性清單為空，用戶端據此區分兩者。關聯表本身永遠不是節點標籤。它就是邊，所以在圖用戶端中沒有標籤，在 `node_labels` 中亦沒有對應列。

---

### `GET /data/domains`

傳回提出請求的角色可存取的網域 ID。

**標頭：** `X-Provisa-Role: <role_id>`（必要）

**回應：** `["sales", "support", ...]`

---

### `GET /data/schema-version`

傳回目前的結構描述版本字串。它結合了每次開機的 nonce 與重建計數器。用戶端據此在伺服器重新啟動後使結構描述快取失效。

**回應：** `{"version": "<boot-id>-<counter>"}`

---

### `GET /data/catalog`

傳回請求所執行角色的目錄：提供給該角色的資料表，與 Arrow Flight 清單所提供的形式相同。

**回應：** `{"tables": [{"path": ["<domain>", "<table>"], "schema": "<base64>"}]}`。`schema` 是該資料表以 Arrow IPC 編碼的 Arrow 結構描述：每個欄位以 SQL 讀取它所用的名稱出現，欄位中繼資料中帶有其註冊類型、描述及鍵中繼資料（`primary_key`、`references`）。命令及指標不會列出。

角色是身分驗證層所確定的角色（`X-Provisa-Role` 可以要求一個已持有的角色或逗號分隔的集合）。沒有身分驗證提供者的部署會列出整個目錄，與 Flight 清單相同。當 Flight 連接埠無法連上時，JDBC 驅動程式會讀取此路由。

---

### `GET /data/proto/{role_id}`

傳回為某角色自動產生的 `.proto` 檔。

**回應：** `text/plain` protobuf 結構描述。

`{role_id}` 是呼叫方持有的一個角色，或以逗號分隔的一組已持有角色（`analyst,org_admin`），該組以其元角色的身分提供服務：即其成員所獲授權的聯集。呼叫方未持有的角色以 `403 auth.role_not_assigned` 拒絕，直接指明的元角色 id 以 `403 auth.meta_role_named` 拒絕。`?domains=a,b` 將 proto 限定於這些網域；角色無法存取的網域以 `403 data.domain_not_accessible` 拒絕。沒有資料介面的角色傳回 `404 data.no_proto_for_role`，未知角色傳回 `404 data.no_role`，模型尚未建立時傳回 `503 data.schema_cache_not_ready`。

角色結構描述中的每張資料表都會產生一個 proto `message`。關係會產生巢狀的 message 欄位。 服務只為角色結構描述根部的資料表列出 RPC，即其 GraphQL `Query` 類型所列的那些資料表；關聯資料表有自己的訊息，並透過其關聯存取。

加入 `?domains=a,b` 可將檔案限制在這些網域內。此時服務為所選網域自身的資料表列出 RPC；與之關聯的資料表以訊息形式出現。

該檔案跟隨模型。每當模型或角色有變，它會隨角色的結構描述一併重新產生，元角色亦然。類型對映：`integer → int32`、`bigint → int64`、`varchar → string`、`decimal → double`、`boolean → bool`、`timestamp → google.protobuf.Timestamp`。

---

### `GET /data/subscribe/{table}`

從資料表接收即時變更通知的 Server-Sent Events 串流。

通知傳遞採用依數據來源類型選擇的可插拔提供者：PostgreSQL 來源使用 `LISTEN/NOTIFY`（透過 asyncpg），MongoDB 來源使用 Change Streams（`collection.watch()`），Kafka 來源使用消費者群組。每個提供者都實作共通的非同步監看介面。無論採用哪個提供者，RLS 篩選與結構描述驗證都會套用。 WebSocket 與 RSS 來源同樣受支援。

**標頭 — `X-Provisa-Sink`：** 設為 Kafka 目標（例如 `kafka://broker:9092/topic`）可將變更事件重新導向到 Kafka 接收端，而非 SSE 回應。伺服器會啟動接收端消費者並傳回 `202 Accepted`，而非開啟串流。

### `POST /data/sources/{source_id}/signal`

在不帶資料列的情況下告訴 Provisa 某個數據來源已經變化：`signal` 變更訊號的接收端。數據來源中變更訊號為 `signal` 的每張資料表（其本身的，或在沒有本身訊號時為數據來源的）都會被視為已寫入，與經由 Provisa 寫入相同：其快取的回應會被捨棄，以它為基礎的具體化檢視會變為過期，並要求建構其副本。受負載保護的資料表的建構會等待其非繁忙時段。`?table=schema.table` 只通知一張資料表。

呼叫者的角色需要寫入權，並且必須涵蓋它所通知的每張資料表的網域。回應為 `202 Accepted`，附帶被通知的資料表：

```json
{"signalled": ["public.orders"]}
```

不存在的數據來源，或它沒有的 `table`，會回應 `404`；`signal` 變更訊號上沒有資料表的數據來源會回應 `409`，因為沒有可重新整理的內容。

---

## 管理員 REST 端點 {: #admin-rest-endpoints }

### 組態 {: #config }

#### `GET /admin/config`

以 `application/x-yaml` 下載目前的 `provisa.yaml`，並附帶 `Content-Disposition: attachment` 標頭。

#### `PUT /admin/config`

上載修訂後的設定 YAML。伺服器寫入 `.bak` 備份，儲存新檔案，從中重新載入部署設定，並把它作為明確的一次性填入套用至模型儲存：檔案宣告的內容會經由模型儲存新增或更新，不會移除任何內容。之後的重新啟動會讀取檔案的設定，但從不再次套用其模型；首次啟動之後，只有模型儲存擁有模型。

**請求主體：** 原始 YAML 內容。

**回應：**

```json
{"success": true, "message": "Config uploaded and applied"}
```

重新載入失敗時：`{"success": false, "message": "<error>"}`。

#### `GET /admin/config/live`

下載當前狀態的模型，寫成設定檔。檔案包含部署檔的設定，每個模型部分都從模型儲存讀取：儲存、區域、數據來源、領域、角色、數據產品、詞彙表術語、資料表及其資料行、關係、指標、標籤及其指派、資料列篩選器、命令、Webhook、排程觸發器及命名規則。把該檔案套用到空的模型儲存，會建構出同樣的模型。部署向每個儲存預置的內容（內建數據來源及其資料表、系統領域、`org_admin` 及 `platform_admin`）不包含在內。

**查詢參數：** `sections`（可選）——以逗號分隔的模型部分清單，例如 `sources,tables`。只會寫出這些部分；未知的名稱會回應 400 `settings.export_unknown_section`。

該端點在任何部署中都對擁有部署設定權利的呼叫者開放；管理頁面的設定檔區域以**匯出模型**提供它。

傳回 `application/x-yaml`，並附帶 `Content-Disposition: attachment; filename=provisa.live.yaml`。

#### `GET /admin/config/diff`

傳回設定差異的兩側：`original`（啟動時的基線）及 `current`（目前即時狀態），兩者以同樣方式正規化，因此比較只顯示真正的變更，略過次序調整及註解變化。

**回應：**

```json
{"original": "<yaml>", "current": "<yaml>"}
```

#### `POST /admin/config/patch`

從基準到所張貼組態產生統一差異修補檔。

把修改後的 YAML 作為請求主體傳送。回應是一個 `text/x-patch` 檔案（`provisa.config.patch`），`git apply` 或 `patch` 可以直接套用，便於把在 UI 中作出的設定變更經 CI/CD 管線提交。

---

### 設定 {: #settings }

#### `GET /admin/settings`

以 JSON 傳回目前的平台設定。

**回應：**

```json
{
  "redirect": {
    "enabled": true,
    "threshold": 10000,
    "default_format": "application/vnd.apache.parquet",
    "ttl": 3600
  },
  "sampling": {
    "default_sample_size": 1000
  },
  "cache": {
    "default_ttl": 300
  },
  "naming": {
    "domain_prefix": false,
    "convention": "apollo_graphql"
  },
  "relationships": {
    "auto_track_fk": true
  },
  "otel": {
    "endpoint": "http://otel-collector:4318",
    "service_name": "provisa",
    "sample_rate": 1.0,
    "support_endpoint": "",
    "support_redact_sql_literals": true,
    "support_redact_attributes": []
  }
}
```

#### `PUT /admin/settings`

在執行時更新平台設定。所有欄位都是可選的，只有請求主體中出現的鍵會被修改。

**請求主體（部分範例）：**

```json
{
  "otel": {
    "support_endpoint": "https://telemetry.vendor.com/v1/traces",
    "support_redact_sql_literals": true,
    "support_redact_attributes": ["db.statement", "user.email"]
  },
  "cache": {"default_ttl": 600}
}
```

各區段可更新的欄位：

- `redirect`：`enabled`、`threshold`、`default_format`、`ttl`
- `sampling`：`default_sample_size`
- `cache`：`default_ttl`
- `naming`：`domain_prefix`、`convention` — 會寫入組態檔並觸發結構描述重新載入
- `relationships`：`auto_track_fk` —— 僅管轄外部索引鍵追蹤。由聯結資料表支撐的關係是在資料表註冊時宣告的，從不被推斷，因此該設定對它不起作用。
- `otel`：`endpoint`、`service_name`、`sample_rate`、`support_endpoint`、`support_redact_sql_literals`、`support_redact_attributes`

**回應：**

```json
{"success": true, "updated": ["otel.support_endpoint", "cache.default_ttl"]}
```

---

### AI 模型 {: #ai-models }

#### `GET /admin/ai-models`

傳回作用中組織的 AI 模型指派、向量模型登錄檔與自然語言速率限制。

**回應：**

```json
{
  "ai_models": {
    "nl": "claude-3-5-sonnet-20241022",
    "embedding": "text-embedding-3-small"
  },
  "vector_models": [...],
  "nl": {"rate_limit": 20},
  "api_keys_set": {"anthropic": true, "openai": false}
}
```

API 金鑰永遠不會傳回：`api_keys_set` 只說明每個供應商是否已設定金鑰。變更由下一個請求起生效，毋須重新啟動。

#### `PUT /admin/ai-models`

更新組織的 AI 模型指派、向量模型登錄檔或自然語言速率限制。在下一個請求生效。

#### `GET /admin/ai-models/vendors/{vendor}/models`

傳回某供應商目前提供的模型名稱，供模型挑選器使用。

該清單即時從供應商自己的模型清單 API 讀取，使用機構設定的金鑰；機構沒有金鑰時使用部署的憑證。在此版本發佈之後才推出的模型，在供應商開始提供它的當天就能選用。

當供應商未發佈 list-models API（此時請直接輸入模型名稱），或沒有可用密鑰時，傳回 `400`。

---

### 聯邦引擎 {: #federation-engine }

#### `GET /admin/federation-engine`

傳回目前的聯邦引擎選擇、其連線組態，以及完整的可選引擎登錄檔。

**回應：**

```json
{
  "current": "trino",
  "persisted": "trino",
  "registry": [
    {"key": "trino", "label": "Trino (embedded)", "fields": [...]},
    {"key": "duckdb", "label": "DuckDB", "fields": []}
  ],
  "note": "Changing the federation engine takes effect after the service is restarted."
}
```

`current` 索引鍵是此刻正在執行的引擎；`persisted` 是寫入組態檔、將在下次重新啟動時載入的引擎。當組態已變更但服務尚未重新啟動時，兩者會不一致。

#### `PUT /admin/federation-engine`

持續保存聯邦引擎的選擇。

**請求主體：**

```json
{"engine": "trino", "federation_engine_url": "http://trino-coordinator:8080"}
```

選擇會寫入平台設定，並在下次服務重新啟動後生效，因為引擎只在啟動時選定一次。

---

### 網域原則 {: #domain-policy }

#### `POST /admin/domain-policy`

變更作用中組織的網域原則（`use_domains` / `default_domain`）。

切換不會刪除任何內容。只要目前的組織擁有目錄——部署沒有自行加入的已註冊資料表、數據來源或網域——它便會以 `409` 及代碼 `settings.domain_policy_catalog_exists` 被拒絕。拒絕的 `params` 會計算每一類（`tables`、`sources`、`domains`）。先以各自的操作移除這些物件，然後才切換。當設定檔宣告了該目錄的一部分時，代碼為 `settings.domain_policy_catalog_in_config`：下一次載入會把它還原，因此該政策在設定檔中設定。

**請求主體：**

```json
{
  "use_domains": true,
  "default_domain": "default"
}
```

`use_domains: null` 會清除組織的覆寫值，改用部署層級的設定。`use_domains: false` 需要 `default_domain`（所有資料表落腳的單一網域名稱）。目錄重建是同步的；回應在結構描述就緒後才傳回。

---

### 可觀測性 {: #observability }

#### `GET /admin/traces/recent`

從記憶體內的 span 緩衝區傳回最多 N 個最近完成的 span。

**查詢參數：** `limit`（預設 50，上限 200）

**回應：** `{"traces": [...]}`

#### `POST /admin/query-engine/reload-catalog`

透過聯邦引擎協調器的 REST API 熱重新載入指名的目錄。會重新連線 Provisa 的內部連線並重新執行 OTel DDL。

**查詢參數：** `catalog`（預設 `"otel"`）

**回應：**

```json
{"success": true, "errors": []}
```

#### `POST /admin/query-engine/restart`

重新啟動聯邦引擎容器（僅限單節點開發環境）。

**查詢參數：** `container`（預設取 `QUERY_ENGINE_CONTAINER` 環境變數，其次為 `"trino"`）

---

### 探索 {: #discovery }

#### `POST /admin/discover/relationships`

觸發關聯探索。一律從聯邦引擎執行外部索引鍵自省。 若已設定 `ANTHROPIC_API_KEY`，則執行 LLM 推論。

**請求主體：**

```json
{
  "scope": "domain",
  "domain_id": "sales"
}
```

`scope` 必須為 `"table"`、`"domain"` 或 `"cross-domain"` 之一。`"table"` 範圍需要 `table_id`（整數）。`"domain"` 範圍需要 `domain_id`。

**回應：** `{"candidates_found": 12, "stored_ids": [1, 2, 3, ...]}`

#### `GET /admin/discover/candidates`

列出待處理的關聯候選項。

#### `POST /admin/discover/candidates/{candidate_id}/accept`

接受候選項並將其註冊為關聯。

**請求主體（選填）：** `{"name": "custom-relationship-name"}`

#### `POST /admin/discover/candidates/{candidate_id}/reject`

拒絕候選項。

**請求主體：** `{"reason": "Not a real join"}`

#### `GET /admin/discover/candidates/rejected/count`

傳回已拒絕候選項的數量。

#### `DELETE /admin/discover/candidates/rejected`

刪除所有已拒絕的候選項。

---

### 數據來源爬取 {: #source-crawl }

#### `POST /admin/sources/crawl`

爬取數據來源以自省其結構描述並註冊資料表。

---

### 數據來源資料表搜尋 {: #source-table-search }

#### `GET /admin/sources/{source_id}/tables/search`

依名稱搜尋數據來源中可用（尚未註冊）的資料表。

---

### 資料表剖析 {: #table-profiling }

#### `POST /admin/tables/{table_id}/profile`

對已註冊的資料表執行資料行剖析：基數、最小/最大值及空值率。

---

### 數據來源描述 {: #source-descriptions }

#### `POST /admin/source-meta/db-description`

為數據來源的資料表與欄位產生 LLM 輔助的描述。

---

### 物件儲存 {: #object-storage }

#### `GET /admin/org-storage`

回報作用中組織相對於其平台配額的儲存用量，以及該組織是否已註冊自己的儲存區。

當組織已註冊自己的 DSN 時，其具體化資料會存入該處，不再計入配額。DSN 本身從不傳回。

#### `PUT /admin/org-storage`

註冊（或清除）組織自有的具體化儲存區。

**請求主體：**

```json
{"storage_url": "s3://my-bucket/provisa?region=us-east-1&access_key=..."}
```

DSN 在被接受之前會先針對聯邦引擎驗證，所以不可用的 DSN 會在註冊時已經失敗，而不是幾小時後在重新整理時才失敗。該值靜態加密儲存，GET 永遠不會傳回它。

送出 `storage_url: null` 可清除組織自有的儲存區，並將其具體化資料歸還平台儲存區（與配額）。組織執行階段會在同一次呼叫中重建，因此新儲存區立即生效。

---

### 組織加密 {: #org-encryption }

#### `GET /admin/org-encryption`

傳回組織目前的密鑰狀態：指紋、id 與來源。從不傳回密鑰材料。

當組織未設定密鑰時，傳回 `{"configured": false}`。每個組織都以此狀態開始，並繼承部署的密鑰。

#### `PUT /admin/org-encryption`

設定或輪換組織的靜態加密密鑰。

**請求主體：**

```json
{"key_b64": "<32 raw bytes, base64-encoded>"}
```

省略 `key_b64`，由 Provisa 產生金鑰。這是最安全的做法，因為金鑰永遠不會出現在剪貼簿或請求日誌中。提供 `key_b64` 則表示使用您自己的金鑰。

輪換會在密鑰環中加入新的作用中項目並保留舊的，因此以先前密鑰寫入的數據仍可讀取。輪換不等於重新加密。沒有刪除端點：淘汰最後一把密鑰會使所有已包裝的酬載無法讀取。

即時密鑰環會在同一次呼叫中重新繫結，因此下一次加密寫入立即使用新密鑰。

---

### Hasura / DDN 匯入 {: #hasura-ddn-import }

#### `POST /admin/import/hasura/preview`

將 Hasura v2 或 DDN 專案封存檔轉換為建議的 Provisa 組態，不寫入任何內容。

**請求主體：**

```json
{
  "filename": "my-project.zip",
  "content_b64": "<base64-encoded archive>",
  "flavor": "auto",
  "domain_map": {"public": "sales"},
  "source_overrides": {}
}
```

`flavor` 為 `"auto"`（依封存檔結構偵測）、`"hasura_v2"` 或 `"ddn"`。

**回應：**

```json
{
  "config_yaml": "...",
  "warnings": ["..."],
  "summary": {
    "sources": 1, "domains": 2, "tables": 40,
    "columns": 180, "roles": 3, "relationships": 15, "rls_rules": 6
  }
}
```

不會保存任何內容。預覽不會在伺服器端快取；`apply` 採用你提供的 YAML，因此套用的正是已審閱（並可能已編輯）的內容。

#### `POST /admin/import/hasura/apply`

將先前預覽的組態載入作用中組織。

**請求主體：**

```json
{"config_yaml": "<yaml string>"}
```

匯入是對目前組織模型儲存的明確一次性填入，與 `PUT /admin/config` 的套用方式相同：它經由模型儲存（帶防護及版本）新增及更新匯入設定所宣告的內容，不會刪除其中沒有提及的任何內容。沒有 `replace` 欄位。所宣告數據來源的引擎目錄、組織的連線集區及其結構描述會在回應傳回之前重建。

**回應:** `{"summary": {...}}` — 匯入設定所宣告內容的計數。

---

### Apache Ossie 互通 {: #apache-ossie-interchange }

#### `GET /admin/ossie`

將組織受治理的模型匯出為 Apache Ossie（孵化中）YAML 文件。

該文件在每次請求時都根據即時狀態產生，從不快取，所以不可能過時。資料表變成 `dataset` 物件，資料行變成 `field` 物件，關係對應為 Ossie 的 `relationship` 物件。

傳回 `text/yaml`，並附帶 `Content-Disposition: attachment; filename=provisa.ossie.yaml`。

#### `GET /admin/dbt/sources`

以 dbt `sources.yml` 的形式下載受治理目錄，按提供給某一個角色的樣子。

**查詢參數:** `role` — 呼叫者持有的角色，或對 `access_config` 持有者而言的任何角色。

傳回 `text/yaml`，帶有 `Content-Disposition: attachment; filename=provisa.sources.yml`。未知的角色為 `404`，代碼為 `dbt_sources.unknown_role`。該檔案包含甚麼，請參閱[整合](integrations.md#dbt-sources-file)。

#### `POST /admin/ossie/import`

剖析 Ossie YAML 或 JSON 文件，並傳回建議的資料表與關聯註冊項。

**請求主體：** 原始 Ossie YAML 或 JSON。格式會自動偵測。

**回應：**

```json
{
  "proposals": {
    "tables": [...],
    "relationships": [...]
  }
}
```

不會註冊任何內容。請在管理員 UI 的審閱畫面中接受或刪減建議，任何變更操作才會觸發。

---

### 動作（函式與 Webhook） {: #actions-functions-and-webhooks }

所有端點都在 `/admin/actions` 前綴之下。

無論來自 GraphQL、SQL、Cypher、Bolt、Arrow Flight、MCP `run_sql` 還是 Provisa gRPC，每次呼叫都經過同一個受治理的執行器。它每次都以同樣方式強制執行命令指派（`visible_to`）、變更所需的 `write` 權限以及治理。各協定的呼叫語法見 [docs/integrations.md](integrations.md#invoking-commands-across-protocols)。

#### `GET /admin/actions`

傳回所有受追蹤的資料庫函式與 webhook。

**回應：**

```json
{
  "functions": [
    {
      "name": "random_python_set",
      "implKind": "python",
      "binding": {"callable": "demo.py_functions:random_dataset"},
      "returns": "",
      "returnSchema": {
        "type": "array",
        "items": {"type": "object", "properties": {"id": {"type": "integer"}, "region": {"type": "string"}}}
      },
      "arguments": [{"name": "rows", "type": "Int"}, {"name": "seed", "type": "Int"}],
      "visibleTo": ["admin"],
      "domainId": "pet-store",
      "description": "Demo Python command returning random rows",
      "kind": "query"
    }
  ],
  "webhooks": [
    {
      "name": "add-pet",
      "url": "https://petstore.example.com/pets",
      "method": "POST",
      "kind": "mutation",
      "approved": true
    }
  ]
}
```

每個 webhook 物件都帶有 `approved` 布林值。當數據管家執行其建立請求後，webhook 即獲核准；組態中宣告的 webhook 自動核准。未核准的 webhook 雖已註冊，但不會在任何介面上公開。

#### `POST /admin/actions/functions`

註冊受追蹤的函式（命令）。

**主要欄位：**

| 欄位 | 必要 | 描述 |
| --- | --- | --- |
| `name` | 是 | 唯一的命令名稱 |
| `kind` | 是 | `"query"` → GraphQL Query 欄位；`"mutation"` → Mutation 欄位 |
| `implKind` | 否 | 命令的執行方式 — 見下表（預設 `source_procedure`） |
| `binding` | 否 | `implKind` 專屬的連線細節（JSON 物件） |
| `returnSchema` | 否 | JSON Schema `{type:"array", items:{type:"object", properties:{...}}}` — 使命令在每個介面上都傳回集合 |
| `arguments` | 否 | `[{name, type}]` 引數定義；位置順序對 SQL 與 Bolt 呼叫者有意義 |
| `visibleTo` | 否 | 該指令分配給的角色 ID（為空：所有角色）。`mutation` 另外需要 `write` 權利 |
| `domainId` | 否 | 用於 GraphQL 放置與存取控制的網域 |

**`implKind` 值：**

| `implKind` | 執行內容 | `binding` 欄位 |
| --- | --- | --- |
| `source_procedure` | 已註冊數據來源上的預存程序（預設） | `sourceId`、`schemaName`、`functionName` |
| `script` | 伺服器端指令碼 | `script` |
| `http` | 對外 HTTP 呼叫 | `url`、`method` |
| `grpc` | 對外部伺服器的 gRPC 呼叫 | `target`、`method` |
| `python` | 由 Provisa 託管的 Python 可呼叫項 | `callable`（例如 `"demo.py_functions:random_dataset"`） |

示範命令 `random_python_set`（`implKind: python`）與 `random_grpc_set`（`implKind: grpc`）展示了帶 `returnSchema` 的集合傳回命令實例；兩者都在 `config/provisa-install.yaml` 中。

#### `PUT /admin/actions/functions/{name}`

依名稱更新受追蹤的函式。

#### `DELETE /admin/actions/functions/{name}`

依名稱刪除受追蹤的函式。

#### `POST /admin/actions/webhooks`

註冊一個受追蹤的 Webhook。註冊或更新 Webhook 會排入一個待數據管家審批的請求，只有數據管家批准後，該 Webhook 才會在所有入口生效。設定中宣告的 Webhook 會自動批准。**請求主體欄位：**`name`、`url`、`method`、`timeoutMs`、`returns`、`inlineReturnType`、`arguments`、`visibleTo`、`domainId`、`description`、`kind`。

#### `PUT /admin/actions/webhooks/{name}`

依名稱更新受追蹤的 webhook。任何編輯都會將核准狀態重設為待處理，直到再次獲得核准。

#### `DELETE /admin/actions/webhooks/{name}`

依名稱刪除受追蹤的 webhook。

#### `POST /admin/actions/test`

依名稱測試動作（函式或 webhook）。

---

### 角色 {: #roles }

所有端點都在 `/admin/roles` 前綴之下。

| 方法 | 路徑 | 描述 |
| --- | --- | --- |
| `GET` | `/admin/roles/` | 列出所有角色 |
| `POST` | `/admin/roles/` | 建立角色 |
| `PUT` | `/admin/roles/{role_id}` | 更新角色 |
| `DELETE` | `/admin/roles/{role_id}` | 刪除角色 |

---

### 使用者 {: #users }

所有端點都在 `/admin/users` 前綴之下。

| 方法 | 路徑 | 描述 |
| --- | --- | --- |
| `POST` | `/admin/users/` | 建立本機使用者 |
| `GET` | `/admin/users/` | 列出本機使用者 |
| `GET` | `/admin/users/{user_id}` | 取得使用者 |
| `PUT` | `/admin/users/{user_id}` | 更新使用者 |
| `PATCH` | `/admin/users/{user_id}/password` | 變更密碼 |
| `DELETE` | `/admin/users/{user_id}` | 刪除使用者 |
| `GET` | `/admin/users/{user_id}/assignments` | 列出角色指派 |
| `POST` | `/admin/users/{user_id}/assignments` | 新增角色指派 |
| `DELETE` | `/admin/users/{user_id}/assignments/{assignment_id}` | 移除角色指派 |

---

### 組織 {: #organizations }

所有端點都在 `/admin/orgs` 之下。

| 方法 | 路徑 | 描述 |
| --- | --- | --- |
| `GET` | `/admin/orgs/` | 列出組織 |
| `POST` | `/admin/orgs/` | 建立組織 |
| `PUT` | `/admin/orgs/{org_id}` | 更新組織 |
| `DELETE` | `/admin/orgs/{org_id}` | 刪除組織 |
| `GET` | `/admin/orgs/{org_id}/members` | 列出成員 |
| `POST` | `/admin/orgs/{org_id}/members` | 新增成員 |
| `DELETE` | `/admin/orgs/{org_id}/members/{user_id}` | 移除成員 |

---

### 邀請 {: #invites }

所有端點都在 `/admin/invites` 之下。

| 方法 | 路徑 | 描述 |
| --- | --- | --- |
| `POST` | `/admin/invites/` | 建立邀請 |
| `GET` | `/admin/invites/` | 列出待處理的邀請 |
| `DELETE` | `/admin/invites/{token}` | 撤銷邀請 |

---

### 管理員 GraphQL {: #admin-graphql }

#### `POST /admin/graphql`

所有管理員操作的 Strawberry GraphQL 端點：數據來源與資料表 CRUD、關聯管理、網域組態、RLS 規則、快取控制、命名慣例、排程工作管理，以及查詢編譯。

完整的結構描述參考（每個 Query 欄位、Mutation 欄位及輸入/輸出類型）請參閱[管理 GraphQL API 參考](admin-graphql.md)。

**主要變更操作：**

```graphql
# Cache
mutation { update_source_cache(source_id: "sales-pg", enabled: true, ttl: 600) { success } }
mutation { update_table_cache(table_id: 1, ttl: 60) { success } }

# Naming conventions
mutation { update_source_naming(source_id: "legacy-db", convention: "camelCase") { success } }
mutation { update_table_naming(table_id: 1, convention: "PascalCase") { success } }

# Scheduled tasks
mutation { toggle_scheduled_task(name: "daily-report", enabled: false) { success } }

# Compile a query (returns enforcement metadata and routed SQL)
mutation {
  compile_query(input: {role: "admin", query: "{ orders { id } }"}) {
    sql semantic_sql trino_sql direct_sql route route_reason sources root_field
    enforcement { rls_filters_applied columns_excluded masking_applied }
  }
}
```

---

### 設定精靈 {: #setup }

#### `GET /setup/status`

傳回首次執行的設定狀態。一律免驗證。

#### `POST /setup/`

完成首次執行設定。

---

## 健康檢查 {: #health-check }

#### `GET /health` 或 `HEAD /health` {: #get-health-or-head-health }

傳回 `{"status": "ok"}`。一律免驗證。

---

## 錯誤回應 {: #error-responses }

| 狀態 | 意義 |
| --- | --- |
| 400 | 無效查詢、驗證錯誤或 SQL 剖析錯誤 |
| 401 | 缺少或無效的驗證權杖 |
| 403 | 能力不足；治理違規 |
| 404 | 找不到角色、資源或組態檔 |
| 422 | 缺少必要標頭（例如 `X-Provisa-Role`） |
| 503 | 資料庫或數據來源未連線；相依項無法使用 |
| 504 | 請求逾時 |

`POST /data/sql` 上的治理違規會傳回 HTTP 403 與結構化主體：

```json
{
  "detail": {
    "violations": [
      {"code": "V000", "message": "Table 'orders' is not accessible for role 'analyst'"}
    ]
  }
}
```

其他所有錯誤皆使用：`{"detail": "<message>"}`。

---

## Arrow Flight 端點 {: #arrow-flight-endpoint }

連接埠 `8815`。透過 gRPC 的原生 Arrow 資料行式傳輸。

查詢與目錄探索都可在同一條連線上使用。完整的治理管線（RLS、遮罩、抽樣）會套用到每一個查詢。

目錄探索（`list_flights`、`get_flight_info`、`get_schema` 以及不帶 `query` 的 ticket）會列出授予呼叫者角色的內容。啟用身分驗證時，這三個不帶 ticket 的呼叫從呼叫標頭 `authorization: Bearer` 取得憑證、從 `x-provisa-role` 取得角色，沒有憑證時會被拒絕；請參閱[整合](integrations.md)。

**Ticket 格式**（JSON）：

```json
{"query": "{ customers { name email } }", "role": "analyst", "variables": {}}
```

**用法（Python）：**

```python
import pyarrow.flight as flight

client = flight.FlightClient("grpc://localhost:8815")
ticket = flight.Ticket(b'{"query": "{ orders { id amount } }", "role": "admin"}')
# Stream batch-by-batch
for batch in client.do_get(ticket):
    process(batch.data)
# Or read all at once
table = client.do_get(ticket).read_all()
```

當 Zaychik Flight SQL 代理可用時（連接埠 8480），記錄批次會端對端串流，無需完整具體化。 若 Zaychik 無法使用，則退回透過聯邦查詢層具體化。

**警告。** 回應關於自身所報告的內容，會在串流中作為不帶資料列的批次的 `app_metadata` 傳遞：`{"provisa_warnings": [{"code", "params", "message"}]}`。讀取資料列之前已知的警告，會在資料列之前的不帶資料列批次中到達。只有在讀取資料列之後才知道的警告，會在第二個不帶資料列的批次中到達，它是串流中的最後一批：`statement.rows_cut` 表示回應在資料列上限處被截斷，並且還有更多資料列相符。請讀取每一批的 `app_metadata`，包括最後一批。

---

## Protobuf gRPC 端點 {: #protobuf-grpc-endpoint }

連接埠 `50051`（以 `GRPC_PORT` 環境變數或 `server.grpc_port` 組態覆寫）。

在 `x-provisa-role` gRPC 中繼資料索引鍵中傳遞角色。若缺少，伺服器會以 `UNAUTHENTICATED` 中止。

從 `GET /data/proto/{role_id}` 下載角色專屬的 proto。只有該角色可見的資料表與欄位會出現。

```proto
service ProvisaService {
  rpc QueryOrders (QueryOrdersRequest) returns (stream Orders);
  rpc InsertOrders (InsertOrdersRequest) returns (InsertOrdersResponse);
}
```

角色結構描述根部的每張資料表會產生一個 `Query{TypeName}` 串流 RPC。這些就是該角色的 GraphQL `Query` 類型所列出的資料表。角色只能透過關聯到達的資料表沒有自己的 RPC，正如它在 GraphQL 中沒有根欄位。`Insert{TypeName}` RPC 是為結構描述對稱性而存在，會以 `UNIMPLEMENTED` 中止。

**參數。** 帶參數的資料表（基於 API 登記的資料表或查詢表）有一個 `{TypeName}Params` 訊息，每個參數一個可選欄位，名稱去掉 `_nf_` 前綴，`Query{TypeName}Request` 以 `params` 攜帶它。其中設定的每個值作為參數到達來源，綁定在語句中；不是該資料表參數的名稱以 `INVALID_ARGUMENT` 拒絕，缺少必填參數的讀取被拒絕並指出其名稱。HTTP gRPC 代理以請求主體的 `params` 物件接收相同的值（未知名稱或 JSON 類型錯誤的值返回 HTTP 400，代碼 `data.invalid_parameter`）。

不屬於呼叫方角色的服務的方法會回應 `UNIMPLEMENTED`。

已啟用 `grpc_reflection.v1alpha`，無需預先編譯的 proto 即可進行服務探索。 反射向每個呼叫方描述其所擔任角色的服務：與該角色從 `GET /data/proto/{role_id}` 下載的 `.proto` 相同。它按名稱列出 Provisa 服務和反射服務，並描述 Provisa 服務的檔案。

反射呼叫與其他 gRPC 呼叫一樣需要識別身分。啟用驗證時，角色來自呼叫方的憑證。未啟用驗證時，請在 `x-provisa-role` 中指明角色；在為多於一個組織提供服務的部署中，還需在 `x-provisa-org` 中指明組織。缺少這些中繼資料的呼叫會以 `UNAUTHENTICATED` 被拒絕。未啟用驗證時，除非設定了 `server.grpc_allow_unsecured_reflection`，否則反射保持停用。

```bash
grpcurl -plaintext -H 'x-provisa-role: analyst' localhost:50051 list
grpcurl -plaintext -H 'x-provisa-role: analyst' \
  -d '{}' localhost:50051 provisa.v1.ProvisaService/QueryOrders
```

當啟動時載入的模型定義了 gRPC 服務時，gRPC 伺服器才會啟動。 此後每次呼叫都按模型的現況提供服務：伺服器執行期間註冊或變更的資料表，會在下一次呼叫時即被提供和描述，無需重新啟動。

#### 彙總與分組 RPC {: #aggregate-and-group-by-rpcs }

當資料表設定了 `enable_aggregates` 時，產生的 proto 會在 `Query{TypeName}` 之外額外包含兩個 RPC：

- **`Query{TypeName}Aggregate`** — 傳回該資料表的彙總純量（`count`；每個數值欄位的 `sum`、`avg`、`stddev`、`variance`；每個可比較欄位的 `min`、`max`）
- **`Query{TypeName}GroupBy`** — 每個分組索引鍵傳回一列，帶有彙總子欄位，並可選擇性地在 `nodes` 欄位中帶有基礎資料表純量與聯結維度資料列

兩者都經過與 GraphQL 根欄位 `{field}_aggregate`、`{field}_group_by` 相同的編譯器彙總管道，所以沒有獨立的彙總實作。

**`funcs` 欄位。** 請求訊息接受一個 `funcs` 重複字串欄位。有效值為 `count`、`sum`、`avg`、`stddev`、`variance`、`min` 與 `max`。省略 `funcs` 時，會要求結構描述為該資料表公開的每一個函式。設定後，只有指名的函式會出現。若指名的函式都不適用於該資料表的欄位類型，查詢會退回 `count`。

**`include_nodes` 與 `include` 欄位。** `Query{TypeName}GroupBy` 請求可設定 `include_nodes: true`，在每一列的 `nodes` 欄位中納入基礎資料表的純量欄位。`include` 重複字串欄位指名多對一關聯欄位，其純量欄位同樣會巢狀放入 `nodes`。此行為與 JSON:API 的 `?includeNodes=` / `?include=` 一致。

---

## JDBC 驅動程式 {: #jdbc-driver }

Provisa JDBC 驅動程式（`provisa-jdbc-<release>.jar`）將語意目錄公開給 BI 工具（Tableau、PowerBI、DBeaver）。

**連線 URL：** `jdbc:provisa://host:port`

網域對映到 JDBC 結構描述。 資料表使用其註冊的別名。欄位使用別名，並以 `REMARKS` 呈現描述。 標準中繼資料方法（`getPrimaryKeys`、`getImportedKeys`、`getExportedKeys`）會將語意關聯以主索引鍵／外部索引鍵中繼資料公開。

**SQL 支援：** `SELECT * FROM <alias> [WHERE col = 'value']`。

驅動程式預設要求 Arrow IPC 重新導向。結果透過 `ArrowStreamReader` 逐批串流，記憶體中最多只保留一個記錄批次。

---

## `orderBy` 引數格式 {: #orderby-argument-format }

`order_by` 引數使用 `{column: direction}` 物件，方向列舉有 6 個值：

```json
{
  "query": "{ orders(order_by: [{created_at: desc_nulls_last}]) { id created_at } }",
  "role": "admin"
}
```

支援的方向：`asc`、`desc`、`asc_nulls_first`、`asc_nulls_last`、`desc_nulls_first`、`desc_nulls_last`。

---

## 訂閱 {: #subscriptions }

SSE 訂閱位於 `GET /data/subscribe/{table}`。 通知傳遞採用依數據來源類型選擇的可插拔提供者：PostgreSQL 來源使用 `LISTEN/NOTIFY`，MongoDB 來源使用 Change Streams，Kafka 來源使用消費者群組。無論採用哪個提供者，RLS 篩選與結構描述驗證都會套用。WebSocket 與 RSS 來源也透過同一端點支援。

---

## 業務詞彙表 {: #business-glossary }

業務詞彙表把來源資料庫中的實體欄位名稱對應到一套共用的人類詞彙上。模型中註冊的每個資料行都會自動獲得一個術語，所以毋須人手輸入就能填滿詞彙表。管理員在系統推導出的內容之上補充定義、關係及專家。

### 術語如何推導 {: #how-terms-are-derived }

當 Provisa 註冊或更新資料表的欄位時，`normalize_term`（`provisa/core/glossary.py`）會在每個欄位名稱上執行並產生一個標準片語。

正規化依序套用五條規則：

1. 在 camelCase 邊界與分隔字元（`_`、`-`、`.`、`/`、空白）處切分。
2. 將結果轉為小寫。
3. 展開一份固定的縮寫對照表（例如 `cust` → `customer`、`amt` → `amount`、`dt` → `date`、`id` → `identifier`、`key` → `identifier`、`guid` → `identifier`）。
4. 移除結尾的**代理權杖**（`identifier`、`code`、`index` 或 `reference`）。以索引鍵或代碼命名的欄位，是透過一個替代值指向底層概念，因此術語應該就是那個概念本身。最後剩下的權杖永遠不會被移除。
5. 以資料表的概念修飾**過於一般化的片語**。當完整的正規化片語只是一個裸屬性詞（`name`、`identifier`、`date`、`location`、`message`、`first name`、`last name` 之類）時，術語會變成 `<table concept> <phrase>`：`employees.first_name` → `employee first name`、`orders.id` → `order identifier`。若讓一個共用的 `name` 術語橫跨互不相關的資料表，會把不同的意義合併在一起；修飾則把每個欄位連結到其所屬概念。資料表概念即該資料表的業務名稱，並正規化為單數主名詞（`order_lines` → `order line`）。

原生篩選的虛擬欄位（以 `_nf_` 為前綴，或任何帶有 `native_filter_type` 的欄位）屬於查詢參數機制，而非業務欄位，不會推導出任何術語。

由於 `id`、`key`、`pk` 與 `sk` 在代理檢查之前都會展開為 `identifier`，三個實體上不同的欄位名稱會落在完全相同的術語上：

| 實體名稱 | 正規化之後 |
| --- | --- |
| `cust_id` | `customer` |
| `customerId` | `customer` |
| `CUSTOMER_KEY` | `customer` |
| `txn_amt` | `transaction amount` |

首三個會合併為同一個術語。`transaction amount` 保留兩個詞元，因為 `amount` 不是替代詞。單獨的 `id` 資料行前面沒有詞元，無法裁剪，所以正規化為 `identifier`，以免術語為空。

### 生命週期 {: #lifecycle }

術語**由語義層的成員關係推導而來**，不由使用者按需建立。資料表儲存庫是唯一的寫入路徑：`sync_table_refs` 在每次資料行集 upsert 內執行，`sweep_refless_terms` 在任何刪除路徑之後執行。

**新增欄位時：** Provisa 依名稱查找正規化後的術語。若已存在，該欄位會取得一個指向它的參照（若該術語先前已淘汰，則會復原，`deprecated` 會設回 `False`）。若尚無術語，則建立一個。

**當某資料行移除時**（結構描述變更或刪除資料表），其參照會被刪除，術語按「刪除或棄用」規則**處置**。沒有剩餘參照的根術語會連同其邊及專家指派被直接刪除，除非刪除它會令某個抽象術語在術語圖中再沒有通往任何根術語的路徑。在這種情況下，該術語會被**棄用**（標記 `deprecated=True`）而不是刪除，從而令抽象術語保留它在圖中的錨點。

抽象術語永不自動移除；它們存在於實體生命週期之外，只能透過管理員 API 明確刪除。

**復原：** 若已淘汰術語的正規化名稱再次出現（欄位重新註冊），該術語會取消標記，其參照亦重新累積。

### 策展端點 {: #curation-endpoints }

所有端點都在 `/admin/glossary` 之下。它們需要 `org_admin` 存取權與已設定的組織。每一次變更操作都會觸發中繼資料發佈。

| 方法 | 路徑 | 描述 |
| --- | --- | --- |
| `GET` | `/admin/glossary/terms` | 列出術語。查詢參數：`q`（名稱／定義搜尋）、`include_deprecated`（預設 `true`） |
| `GET` | `/admin/glossary/terms/{term_id}` | 取得術語明細：定義、實體參照、具型別的邊、專家 |
| `POST` | `/admin/glossary/terms` | 建立抽象術語 — 沒有實體參照的使用者詞彙 |
| `PATCH` | `/admin/glossary/terms/{term_id}` | 重新命名、設定定義，或切換匯出排除 |
| `DELETE` | `/admin/glossary/terms/{term_id}` | 刪除沒有實體參照的術語 |
| `POST` | `/admin/glossary/refs/move` | 將一個實體參照移到另一個術語（合併整併） |
| `POST` | `/admin/glossary/terms/{term_id}/edges` | 在兩個術語之間新增具型別的關聯邊 |
| `DELETE` | `/admin/glossary/terms/{term_id}/edges` | 移除一條邊（查詢參數：`to_term_id`、`rel_type`） |
| `POST` | `/admin/glossary/terms/{term_id}/experts` | 將使用者標記為某術語的專家或作者 |
| `DELETE` | `/admin/glossary/terms/{term_id}/experts/{user_id}` | 移除使用者的專家／作者身分 |
| `POST` | `/admin/glossary/terms/{term_id}/definition/generate` | 使用組織的 AI 模型為單一術語草擬定義 — 只傳回文字，未儲存前不會保存任何內容 |
| `POST` | `/admin/glossary/definitions/generate` | 為每個尚無定義的術語產生並保存定義 — 永不覆寫人工撰寫的文字 |
| `POST` | `/admin/glossary/relationships/generate` | 使用組織的 AI 模型，為整個詞彙表建議並保存具型別的邊 |

**`POST /admin/glossary/terms` 主體：**

```json
{"name": "revenue", "definition": "Recognized net revenue after returns and discounts."}
```

**`POST /admin/glossary/terms/{term_id}/edges` 主體：**

```json
{"to_term_id": 42, "rel_type": "KIND_OF"}
```

有效的 `rel_type` 值：`KIND_OF`、`RELATED_TO`、`PART_OF`、`SYNONYM_OF`。

**`POST /admin/glossary/terms/{term_id}/experts` 主體：**

```json
{"user_id": "alice@example.com", "kind": "author"}
```

有效的 `kind` 值：`expert`、`author`。

**`POST /admin/glossary/refs/move` 主體：**

```json
{"table_id": 7, "column_name": "cust_id", "to_term_id": 12}
```

移動參照後，失去它的那個術語會按「刪除或棄用」規則處置。可以用它合併正規化未能合併的兩個術語，例如某個數據來源使用了擴充表未涵蓋的非標準縮寫時。

刪除已扎根的術語（帶有實體參照者）會傳回 `400 glossary.invalid`。請先移除或移動所有參照。

**`PATCH /admin/glossary/terms/{term_id}` — `export_excluded` 欄位：**

```json
{"export_excluded": true}
```

將 `export_excluded` 設為 `true`，可讓該術語不出現在任何中繼資料匯出快照中，無論其實體參照或抽象狀態為何。設回 `false` 則在下次發佈時把術語恢復到快照中。策展數據（定義、邊、專家）不受影響。

### AI 輔助策展 {: #ai-assisted-curation }

組織所設定的 AI 模型可以在一次操作中，為整個詞彙表草擬定義並建議關聯邊。這兩項批次動作都需要 `org_admin` 存取權與已設定的組織。

**`POST /admin/glossary/definitions/generate`**

遍歷詞彙表中的每個術語，略過已有定義的術語，並呼叫機構的 AI 模型為其餘每個術語起草定義。每份草稿都會即時儲存，沒有編輯步驟，這一點不同於單一術語的草稿端點（`POST /admin/glossary/terms/{term_id}/definition/generate`）。人手撰寫的定義永遠不會被覆寫，因為已有定義的術語在呼叫模型之前已被略過。整批只發一條發佈通知。

回應：

```json
{"generated": 12}
```

`generated` 是取得新定義的術語數量。當每個術語都已有定義時為零。

**`POST /admin/glossary/relationships/generate`**

將完整術語清單連同一段提示詞送給組織的 AI 模型，提示詞指定十種允許的邊類型（`KIND_OF`、`PART_OF`、`SYNONYM_OF`、`RELATED_TO`、`VALID_VALUE_OF`、`DERIVED_FROM`、`REPLACES`、`PREFERRED_TERM_FOR`、`TRANSLATION_OF`、`ANTONYM_OF`），並要求只提出有把握的建議。模型以 JSON 陣列回應；每一項在任何寫入之前都會經過驗證：未知的術語名稱、自我指向的邊，以及封閉列舉之外的邊類型都會被靜默丟棄。有效的建議會以冪等方式 upsert。重新執行該動作不會產生重複的邊。整批只發出一次發佈通知。當詞彙表中未淘汰的術語少於兩個時，端點會立即傳回 `{"added": 0}`。

回應：

```json
{"added": 5}
```

`added` 是寫入的邊數。已經存在的邊亦計入：upsert 會成功，但邊的數據不會改變。

### MCP `search_terms` 工具 {: #mcp-search_terms-tool }

```
search_terms(query, role=None, limit=25)
```

以不區分大小寫的子字串比對搜尋術語名稱與定義，最多傳回 `limit` 筆結果。每筆結果都是完整的術語明細：`name`、`definition`、`is_abstract`、`deprecated`、實體參照（帶有 `source_id`、`schema_name`、`table_name`、`column_name`）、具型別的邊，以及專家指派。

在撰寫 SQL 之前使用 `search_terms`，可依名稱找出代表某個概念的每一個實體欄位。例如搜尋 `"order date"` 會傳回該術語，以及每個已註冊資料表中所有的 `order_dt`、`orderDate`、`ORDER_DATE` 欄位。

### MCP 統計工具 {: #mcp-statistics-tools }

```
describe(sql, role=None)
correlate(sql, columns, method="pearson", role=None)
regress(sql, target, features, kind="linear", role=None)
compare(sql, value, group, role=None)
forecast(sql, time, value, horizon, seasonal_periods=None, role=None)
```

每個工具接收由 `sql` 定義的數據集，並以呼叫方的角色，透過與 `run_sql` 相同的受治理管道執行它：行規則、遮罩和塗黑都會生效，管道拒絕的 SQL 會像在 `run_sql` 中一樣失敗，呼叫方不持有的角色會被拒絕。每個工具只傳回結果，從不傳回行，並附上所用的方法和統計的 `n`。

| 工具 | 傳回 |
| --- | --- |
| `describe` | 每欄：`count`、`missing`、`distinct`；數值欄還有 `mean`、`sd`、`min`、`max` 和 `quantiles`（p0、p5、p25、p50、p75、p95、p100）；`top_values`（最常見的五個值） |
| `correlate` | 數值 `columns` 的 `pearson` 或 `spearman` 相關 `matrix`，基於在所有這些欄中都完整的行，以及該 `n` |
| `regress` | `kind="linear"`：最小二乘 `coefficients`（`estimate`、`se`、`p_value`）、`r_squared`、`adj_r_squared`。`kind="logistic"`：取兩個值的 `target`，最大似然 `coefficients`、`pseudo_r_squared`、`auc` 和 `positive_value`。文字特徵按除第一個值外的每個值各變為一個指示變數 |
| `compare` | 每組的 `n`、`mean` 和 `sd`；兩組時為 Welch t 檢定（`t`、`df`、`p_value`）和 Cohen's d；更多組時為單因子變異數分析（`f`、`df_between`、`df_within`、`p_value`）和 eta 平方 |
| `forecast` | 最後一個 `time` 之後的 `horizon` 個點，每點附 `value`、`lower` 和 `upper`（95% 預測區間），採用加性誤差的指數平滑（ETS），10 個點起帶加性趨勢，給出 `seasonal_periods` 時帶加性季節性 |

引擎在角色讀取的每一行上計算它能計算的內容：`describe` 的計數、去重計數、平均值、標準差、最小值和最大值，以及 `compare` 所需的每組 `n`、平均值和變異數。擬合的模型以及 `describe` 的分位數和最常見值最多讀取 `mcp.stats_max_rows` 行（Admin → Settings → Limits；預設 100,000）。每個讀取行的答案都會給出 `rows_read`、`row_cap` 和 `truncated`，角色自身的行數上限會像 `run_sql` 一樣在 `warnings` 中報告。

統計工具只傳回結果，因此分析背後的行永遠不會進入模型的上下文：預設即數據最小化。限制智能體所能得知內容的是存取控制。要讓身份資訊不在智能體可及範圍內，只向其角色授予不識別任何人的欄；能讀取某欄的角色就能對其聚合，而對小分組的聚合仍可能識別出個人。參見[安全](security.md#statistics-over-mcp)。

### 中繼資料匯出 {: #metadata-export }

詞彙表術語圖包含在 `build_snapshot` 建置的每個 `MetadataSnapshot` 中。

匯出套用與快照其餘部分相同的篩選：

- 標記 `export_excluded` 的術語會被整個保留不匯出，無論其實體參照、抽象狀態，或組織的目錄是否已設定。
- 已扎根的術語只在其至少一個實體參照所屬的欄位同時通過**數據產品**篩選（資料表的 `data_product` 旗標必須為 `true`）與**技術性**欄位篩選（標記為 `technical` 的欄位不匯出）時才會發佈。
- 若某個已扎根術語的所有參照都被上述篩選攔下，該術語也一併不匯出。
- 抽象術語無條件發佈：它們是使用者詞彙，不繫結於實體欄位。
- 兩個術語之間的邊，只有在兩端術語都發佈時才會發佈。

每個供應商配接器都以原生方式發佈術語圖，寫入一個歸 Provisa 所有、以冪等方式建立的詞彙表容器，從不寫入已有的目錄詞彙表：

| 提供者 | 容器 | 術語 | 關聯 | 淘汰 |
| --- | --- | --- | --- | --- |
| Apache Atlas | 「Provisa Glossary」（glossary API） | 詞彙表術語，定義寫在 `longDescription` | KIND_OF → `isA`、SYNONYM_OF → `synonyms`、RELATED_TO/PART_OF → `seeAlso` | `[DEPRECATED]` shortDescription 標記 |
| Atlan | 依穩定 qualifiedName 的 Provisa 詞彙表 | `longDescription`（絕不使用人工編輯的 `userDescription`） | 與 Atlas 相同的對映 | `certificateStatus = DEPRECATED` |
| DataHub | `urn:li:glossaryNode:provisa.<org>` | 每個術語一個 `glossaryTermInfo` 面向 | KIND_OF → Inherits、PART_OF → Contains（反向）、RELATED_TO/SYNONYM_OF → 相關術語 | deprecation 面向；重新命名遵循 URN 承繼 |
| OpenMetadata | 透過 `/v1/glossaries` 的 Provisa 詞彙表 | 以 fqn 為索引鍵的 PUT，重新命名則以已儲存的 UUID 進行 PATCH 重新繫結 | KIND_OF → 原生父階層、SYNONYM_OF → `synonyms`、其餘 → `relatedTerms` | `entityStatus` |
| Collibra | 詞彙表類型的網域「Provisa Glossary」 | 透過 Import API 的 Business Term 資產 | 原生 Business Term 關聯類型 | 資產狀態 |

關鍵在於繫結，而不是名稱。每個已發佈術語的供應商 id 都會以該術語的 URN（`provisa://<org>/terms/<name>`）記錄在 `catalog_bindings` 中，只有當 Provisa 持有該繫結，或該條目位於 Provisa 建立的容器中時，Provisa 才會修改或刪除供應商一側的詞彙表條目。沒有 Provisa 繫結的詞彙表條目來自外部系統，永遠不會被觸碰。更新會先讀取再合併，所以數據管家在 Provisa 自有術語上加入的欄位會保留；術語離開快照時亦不會刪除任何東西。數據管家所作的術語到資產的指派仍歸外部系統所有：沒有任何配接器寫入它們（發佈在 Provisa 中編寫的指派是另一項後續工作）。具體到 Collibra，Import API 會取代收到的內容，安全性依靠範圍限定：負載只提及 Provisa 詞彙表領域內的資產，以及 Provisa 術語之間的關係，所以數據管家的詞彙表及其關係都不會被觸及。

---

## 數據產品 {: #data-products }

數據產品將一起發佈供消費的資料表分組，並由單一網域擁有。在 Provisa 已是真實來源的地方，欄位沿用 ODPS（Open Data Product Standard）詞彙。管理員 UI 在 **Admin → Data Products** 下公開數據產品。

### 能力 {: #capabilities }

| 能力 | 授予 |
| --- | --- |
| `data_product_read` | 讀取 `data_products` 查詢欄位與數據產品管理頁面的存取權。預設種子指派給 `org_admin`、`analyst`、`developer` 與 `modeler`。 |
| `data_product_rw` | 建立與刪除變更操作。啟用 UI 中的「新增／編輯／刪除」控制項。 |

### 管理員 GraphQL {: #admin-graphql_1 }

所有數據產品操作皆透過 `POST /admin/graphql` 進行。

**查詢：**

```graphql
query {
  data_products {
    id
    domain_id
    name
    owner_role
    team_role
    purpose
    limitations
    usage
    version
    status
    sla
    support
    custom_properties
  }
}
```

需要 `data_product_read`。

**建立或更新：**

```graphql
mutation {
  create_data_product(input: {
    id: "customer_360"
    domain_id: "sales"
    name: "Customer 360"
    owner_role: "data-product-owner"
    team_role: "sales-analytics"
    purpose: "Single view of a customer across all touchpoints."
    status: "active"
    version: "1.0.0"
  }) {
    success
    message
  }
}
```

`create_data_product` 為 upsert 操作：以既有 `id` 呼叫會更新該記錄。需要 `data_product_rw`。

**刪除：**

```graphql
mutation {
  delete_data_product(id: "customer_360") {
    success
    message
  }
}
```

刪除一個產品會清除每個成員資料表的 `product_id`，移除其成員資格。需要 `data_product_rw`。

### 欄位結構描述 {: #field-schema }

| 欄位 | 類型 | 必要 | 備註 |
| --- | --- | --- | --- |
| `id` | `String` | 是 | 機器可讀的穩定識別碼，例如 `customer_360` |
| `domain_id` | `String` | 是 | 擁有網域。成員資料表必須共用此 `domain_id` — 不符者在儲存時會被拒絕 |
| `name` | `String` | 是 | 顯示名稱 |
| `owner_role` | `String` | 否 | 對此產品負責的角色；有別於網域管家 |
| `team_role` | `String` | 否 | 持有者日常維護此產品的角色；解析至個人 |
| `purpose` | `String` | 否 | 此產品發佈的內容及原因 |
| `limitations` | `String` | 否 | 已知限制、注意事項或排除項 |
| `usage` | `String` | 否 | 如何消費此產品 |
| `version` | `String` | 否 | 例如 `1.2.0` |
| `status` | `String` | 否 | 例如 `proposed`、`active`、`deprecated`、`retired` |
| `sla` | `String` | 否 | 服務層級承諾；為文字敘述 — 產品跨越多個資料表，結構化 SLA 無法明確指出其描述的是哪個成員 |
| `support` | `String` | 否 | 自由文字支援指引 |
| `custom_properties` | `JSON` | 否 | 標準欄位未涵蓋的任意鍵值中繼資料 |

模型中還有兩個欄位，但沒有在 `DataProductType` / `DataProductInput` 中公開。它們專用於 Snowflake Horizon Catalog：

| 欄位 | 備註 |
| --- | --- |
| `support_contact` | 電郵或 URL；Horizon Catalog 組織清單資訊清單要求此項 |
| `publish` | 設為 `true` 可立即發佈 Horizon 清單；新清單預設為 DRAFT |

### 資料表成員資格 {: #table-membership }

在資料表編輯表單中設定資料表的 `product_id` 欄位後，該資料表就加入了某個數據產品。選擇器只列出 `domain_id` 與該資料表所屬領域相同的產品，所以 `marketing` 領域中的資料表永遠不會看到 `sales` 領域的產品。

同一網域中的命令亦可指派為成員。

### 中繼資料匯出篩選 {: #metadata-export-filter }

`build_snapshot` 在每次目錄發佈時套用 `data_products_only=True`。沒有 `product_id` 的資料表會連同其關係邊、血緣邊與治理標籤一併從快照中保留。數據來源與網域一律發佈。詞彙表術語只在其至少一個實體參照所屬的資料表屬於已匯出（產品成員）資料表時才會發佈。

沒有匯出成員的產品不會產生快照條目，因為沒有成員的清單會令目錄對該產品產生錯誤認識。

### 依目錄目標劃分的數據產品支援 {: #data-product-support-by-catalog-target }

`MetadataSnapshot.data_products` 會抵達每個配接器，但只有其平台具備原生數據產品概念的配接器，才會將其發佈為一級實體；其餘配接器只發佈成員資料表（已依上述篩選），不含產品分組。

| 目標 | 數據產品呈現方式 |
| --- | --- |
| Snowflake Horizon | 每個產品成為其成員資料表實體位址之上的一個 `SHARE`，包裝在內部的 `CREATE ORGANIZATION LISTING` 中 —— 屬原生 Horizon Catalog Data Product。`publish=true` 會使清單立即上線；否則會以 DRAFT 狀態落地。 |
| BigQuery Dataplex | 每個產品透過 `/v1/dataProducts` 成為一項 Analytics Hub 清單。 |
| OpenMetadata | 每個產品成為原生 `DataProduct` 實體（`/api/v1/dataProducts`），擁有權衍生自網域。 |
| DataHub | 每個產品成為原生 `dataProduct` 實體（`urn:li:dataProduct:...`），擁有自己的 `dataProductProperties`／擁有權面向。 |
| Collibra | 每個產品成為 `Data Product` 社群類型的資產，透過 `Data Product groups Table` 關係連結至其成員資料表。 |
| Atlan | 以自訂 `DataProduct` typedef 猜測方式發佈 —— Atlan 對此概念並無文件化的穩定類型名稱，因此此對映屬盡力而為。 |
| Apache Atlas | 以自訂 `provisa_data_product` typedef 及 `provisa_data_product_members` 關係發佈 —— Atlas 並無原生數據產品實體類型。 |
| OpenLineage | 並非一級實體 —— 成員資料表帶有一個指名擁有產品的 `provisa_data_product` 自訂面向。 |
