# API 參考

## 概覽

Provisa 在兩個前綴下公開 REST 端點：`/data` 用於查詢執行及結構描述內省，`/admin` 用於設定管理。（REQ-043）大多數數據端點需要一個角色識別碼。管理設定操作使用位於 `/admin/graphql` 的 Strawberry GraphQL API。（REQ-164）

---

## 身分驗證

當 `provisa.yaml` 中設定了 `auth.provider` 時，除 `/health` 及 `/setup/status` 外的所有端點，都需要一個 `Authorization: Bearer <token>` 標頭。（REQ-120） [tool-verified: `provisa/api/app.py`, `provisa/auth/wiring.py`]

未設定身分驗證時，伺服器以開發模式執行。任何請求都會被視為 `anonymous` 身分，該身分對映至所有已設定的角色，並具備萬用字元的網域存取權。（REQ-535）

當設定了 `provider: basic` 時，**登入（`POST /auth/login`）**由目前使用中的身分驗證供應商提供。（REQ-124）憑證格式及回應內容視供應商而定。

**身分內省：**

```http
GET /auth/me
```

傳回已驗證使用者的 id、電郵、顯示名稱、組織成員資格及角色指派。在開發模式下傳回 `dev_mode: true` 及所有角色 ID 的清單。 [tool-verified: `provisa/api/auth_router.py`]

```http
GET /auth/provider-type
```

傳回 `{"provider": "<name>"}`，或在未設定身分驗證時傳回 `{"provider": null}`。 [tool-verified: `provisa/api/auth_router.py`]

---

## 數據端點

### `POST /data/graphql`

執行一則 GraphQL 查詢或 mutation。（REQ-043） [tool-verified: `provisa/api/data/endpoint.py:151`]

**請求主體：**

```json
{
  "query": "{ orders(where: {region: {eq: \"us\"}}) { id amount } }",
  "variables": {},
  "role": "admin",
  "extensions": {}
}
```

`role` 欄位僅在開發模式（無身分驗證）下使用。當身分驗證已啟用時，會使用已驗證使用者的角色，主體中的 `role` 將被忽略。

`extensions` 欄位支援自動持久化查詢（APQ）協定：（REQ-288）

```json
{
  "extensions": {"persistedQuery": {"sha256Hash": "<sha256-of-query>"}}
}
```

**標頭：**

- `X-Provisa-Role` — 覆寫角色（開發模式）
- `Accept` — 回應格式（見內容協商）
- `Authorization` — 已啟用身分驗證時的 `Bearer <token>`
- `X-Provisa-Redirect-Format` — S3 重新導向輸出的 MIME 類型（REQ-137）
- `X-Provisa-Redirect-Threshold` — 觸發重新導向所需的資料列數（REQ-137）
- `X-Provisa-Redirect` — 設為 `true` 以無條件強制重新導向（REQ-029）

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

**回應（多根查詢，內嵌與重新導向混合）：**

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

多根查詢會獨立執行每一個根欄位。低於重新導向門檻的欄位會內嵌傳回；超過門檻的欄位則會重新導向。`redirects`（複數）鍵會將欄位名稱對映至重新導向資訊。（REQ-029） [tool-verified: `provisa/api/data/endpoint.py`]

**快取標頭：**

- `X-Provisa-Cache: HIT|MISS`（REQ-536）
- `X-Provisa-Cache-Age: <seconds>`（HIT 時）（REQ-536）

**所需能力：**所有請求（包括內省）皆需 `QUERY_DEVELOPMENT`。 [tool-verified: `provisa/api/data/endpoint.py:186-283`]

---

### 內容協商

| Accept 標頭 | 格式 |
| --- | --- |
| `application/json` | JSON（預設） |
| `application/x-ndjson` | 換行分隔 JSON |
| `text/csv` | CSV |
| `application/vnd.apache.parquet` | Parquet |
| `application/vnd.apache.arrow.stream` | Arrow IPC |

（REQ-047、REQ-048、REQ-049、REQ-050） [tool-verified: `provisa/api/data/endpoint.py:84-90`]

---

### 重新導向

超出所設定資料列門檻（或當 `X-Provisa-Redirect: true`）的結果，會被寫入 S3，並傳回一個預先簽署的網址。（REQ-029、REQ-044）

| 重新導向格式 | 由誰寫入 | 記憶體 |
| --- | --- | --- |
| `application/vnd.apache.parquet` | 聯邦式 CTAS | 無——數據永不經手 Provisa |
| `application/x-orc` | 聯邦式 CTAS | 無——數據永不經手 Provisa |
| `application/json` | Provisa | 受記憶體限制 |
| `application/x-ndjson` | Provisa | 受記憶體限制 |
| `text/csv` | Provisa | 受記憶體限制 |
| `application/vnd.apache.arrow.stream` | Provisa | 受記憶體限制 |

對於大型分析匯出，使用 Parquet 或 ORC 重新導向。聯邦引擎會平行直接寫入 S3——沒有任何數據經手 Provisa。（REQ-138）

```yaml
X-Provisa-Redirect-Format: application/vnd.apache.parquet
X-Provisa-Redirect-Threshold: 1000
```

---

### `POST /data/sql`

透過第二階段治理管線執行原始 SQL。（REQ-267） [tool-verified: `provisa/api/data/endpoint_dev.py:62`]

**請求主體：**

```json
{
  "sql": "SELECT id, amount FROM orders WHERE region = 'us'",
  "role": "admin"
}
```

**所需能力：**`QUERY_DEVELOPMENT`。

`POST /data/sql` 上的治理違規會傳回 HTTP 403。（REQ-002、REQ-266）

**回應：**與 `/data/graphql` 格式相同（預設為 JSON 資料列，經由 `Accept` 進行內容協商）。

---

### `POST /data/query`

統一查詢端點。接受 GraphQL、SQL 或 Cypher——語法會自動偵測。（REQ-267） [tool-verified: `provisa/api/data/endpoint_dev.py:509`]

Cypher 查詢也可以提交至僅限 Cypher 的 `POST /query/cypher` 端點。（REQ-345）

**請求主體：**

```json
{
  "query": "{ orders { id } }",
  "params": {},
  "variables": {},
  "role": "admin"
}
```

GraphQL 傳回 `{"data": ...}`；SQL 及 Cypher 傳回 `{"columns": [...], "rows": [...]}`。

---

### `GET /data/rest/{domain_id}/{table_name}`

每一個已登記的資料表都會有一個自動產生的純 REST 端點。查詢字串會對映至 GraphQL 引數，該請求會透過與 GraphQL 相同的管線（RLS、遮罩、路由）進行編譯及執行。（REQ-256） [tool-verified: `provisa/api/rest/generator.py:153`]

**查詢參數：**

- `limit` — 最大資料列數（≥ 1）
- `offset` — 跳過的資料列數（≥ 0）
- `fields` — 以逗號分隔的欄位名稱（預設為全部純量欄位）
- `filter` — `{"field", "comparator", "value"}` 篩選物件的 JSON 陣列
- `orderBy` — `{"field", "direction"}` 排序物件的 JSON 陣列

需要已驗證的角色；未經身分驗證的請求傳回 `401`。這些路由的 OpenAPI 規格由 `GET /data/rest/openapi.json` 提供，Swagger UI 則位於 `GET /data/rest/docs`。

#### OpenAPI / Swagger UI 探索器

OpenAPI 探索器頁面（`/app/openapi`）在一個沙盒化的 iframe 中內嵌了 Swagger UI。該規格是角色範圍界定的——僅顯示目前角色可見的資料表及欄位——並可選擇性地透過網域選擇器進行網域篩選。此 UI 會自動在淺色與深色主題之間切換。 [tool-verified: `provisa-ui/src/pages/OpenApiPage.tsx:20-34`]

該頁面是透過 `fetch()` 而非直接的 iframe `src` 來載入規格 HTML，因此請求會攜帶工作階段的 bearer token，且 Swagger UI 自身的相對請求能正確地解析至相同的來源 (origin)。 [tool-verified: `provisa-ui/src/pages/OpenApiPage.tsx:44-69`]

當從自然語言的「Open in OpenAPI」連結導向而來時，該頁面會自動展開目標端點、以自然語言產生的網址填入查詢參數（例如 `aggregate`、`groupBy`），並點擊 Execute——使用 DOM 輪詢確保每一步都在下一步觸發前完成。（REQ-1359） [tool-verified: `provisa-ui/src/pages/OpenApiPage.tsx:94-171`]

---

### `GET /data/jsonapi/{domain_id}/{table_name}`

每一個已登記的資料表都有一個自動產生、符合 [JSON:API](https://jsonapi.org) 規範的端點。與 GraphQL 相同的 RLS、遮罩及路由。（REQ-257） [tool-verified: `provisa/api/jsonapi/generator.py:284`]

**`Accept` 標頭：**必須包含 `application/vnd.api+json`（JSON:API 媒體類型），否則請求傳回 `406`。

**查詢參數：**

- `fields[<type>]` — 稀疏欄位集，例如 `?fields[orders]=amount`
- `filter[<col>]` / `filter[<col>][<op>]` — 例如 `?filter[region]=US`、`?filter[amount][gt]=100`
- `sort` — 以逗號分隔，`-` 前綴代表降冪，例如 `?sort=-created_at,amount`
- `page[number]` / `page[size]` — 分頁
- `aggregate` — 以逗號分隔的聚合函式，用以取代資料列擷取：`count`、`sum`、`avg`、`stddev`、`variance`、`min`、`max`。使用 `?aggregate=count,sum` 以請求其中一部分。聚合回應會傳回 `data: null`，結果放在 `meta.aggregate` 中。（REQ-1359） [tool-verified: `provisa-ui/src/pages/JsonApiPage.tsx:238`]
- `groupBy` — 以逗號分隔的欄位名稱；與 `?aggregate=` 併用以將結果分組。只有資料表 `DistinctOnColumn` 列舉型別中的欄位才有效；對於該角色無法看見的任何欄位，伺服器會傳回 `400`。（REQ-1361） [tool-verified: `provisa-ui/src/pages/JsonApiPage.tsx:447`]
- `includeNodes` — 設為 `true` 以在每一個分組列的 `nodes` 陣列中包含基礎資料表的純量欄位（以及 `include=` 中所指名、經 join 的維度純量欄位）。當自然語言的分組查詢同時要求維度細節時，此參數為必要項。（REQ-1405）

回應是帶有 `type`/`id`/`attributes` 的資源物件。錯誤遵循 JSON:API 錯誤物件的形狀。

#### JSON:API 探索器

JSON:API 探索器頁面（`/app/jsonapi`）是這些端點之上的一個瀏覽器 UI。從依網域分組的清單中選擇一個資料表，然後設定：

- **欄位**——選擇要包含的欄位（稀疏欄位集）；全部不勾選則請求每一個欄位
- **關聯**——選擇要透過 `?include=` 附帶載入的 FK 衍生關聯名稱
- **篩選**——欄位、運算子（`eq`、`neq`、`gt`、`gte`、`lt`、`lte`、`like`）及值
- **排序**——一個欄位，升冪或降冪
- **聚合**——從伺服器驗證過的清單中挑選分組欄位，再勾選一個或多個聚合函式；當已選擇分組欄位時，會出現一個「Include nodes」核取方塊，將基礎資料表純量欄位附加至每一列
- **每頁筆數**——每頁的資源數，具備第一頁/上一頁/下一頁/最後一頁的導覽

結果會以格式化摘要檢視（可點擊關聯錨點的資源卡片）或原始 JSON 分頁呈現。目前的請求網址會顯示出來，並可複製。資料表選擇及每頁筆數會在 `localStorage` 中跨工作階段保留。 [tool-verified: `provisa-ui/src/pages/JsonApiPage.tsx`]

當從自然語言的「Open in JSON:API」連結導向而來時，探索器會預先選取該資料表，並以自然語言產生的查詢參數填入聚合選擇器，然後自動執行請求。 [tool-verified: `provisa-ui/src/pages/JsonApiPage.tsx:460-479`]

---

### `POST /query/nl`

提交一則自然語言問題。此服務會啟動一項非同步工作，並立即傳回帶有 `job_id` 的 `202 Accepted`。需要在 `ai_models` 設定區段下設定一個 LLM 供應商。（REQ-354） [tool-verified: `provisa/api/rest/nl_router.py:50`]

**請求主體：**

```json
{"q": "How many orders were placed last month?", "role": "admin"}
```

傳回 `{"job_id": "<id>"}`。超出逐角色自然語言速率限制時，傳回帶有 `Retry-After` 標頭的 `429`。（REQ-370）

**取得結果：**

- `GET /query/nl/{job_id}` — 輪詢。傳回該項工作文件。
- `GET /query/nl/{job_id}/stream` — SSE。每一個產生目標完成時發出一個 `branch` 事件，最後發出一個 `done` 事件。（REQ-357、REQ-358）

三個產生迴圈（Cypher、GraphQL、SQL）平行執行，各自透過編譯器驗證，並在出錯時精修。（REQ-355）提示內容的範圍限定於該角色可見的結構描述。（REQ-356）結果文件依目標對每個分支建立鍵值：（REQ-357） [tool-verified: `provisa/nl/job.py:69`]

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

耗盡其迭代上限的分支會傳回 `query: null`、`result: null` 及一個 `error` 字串。每一則產生的查詢都在消費端的權限下執行，並套用第二階段治理——此服務永不繞過治理。（REQ-359）

#### 帶維度細節的自然語言分組查詢（REQ-1405）

當一則自然語言分組查詢同時投影了來自某個已 join 維度資料表的欄位——例如「依使用者統計查詢次數，並附上使用者姓名及電郵」——執行器會從 SELECT 投影的維度欄位衍生出逐欄位的點路徑（`dim_paths`）。這些路徑會填入 JSON:API 及 OpenAPI 面板所產生網址上的 `includeNodes=` 參數，使這些面板請求與 SQL 及 GraphQL 分支所解析出的相同已 join 維度欄位。若無此機制，`includeNodes=true` 將只會傳回基礎聚合資料表自身的純量欄位。（REQ-1405） [tool-verified: `docs/arch/requirements.md:REQ-1405`]

在 gRPC 面板上，所產生的 `{Type}GroupByRequest` 攜帶 `include_nodes`（布林值）及 `include`（關聯欄位名稱的重複字串）。傳回的 `{Type}GroupByRow` 包含一個帶有維度細節資料列的具型別 `nodes` 欄位。 [tool-verified: `provisa/grpc/query_ir.py:168-196`]

---

### `GET /data/sdl`

傳回某角色結構描述的 GraphQL SDL。（REQ-008） [tool-verified: `provisa/api/data/sdl.py:137`]

**標頭：**`X-Role: <role_id>`（必要）

**查詢參數：**

- `domain` — 以逗號分隔的網域 ID。設定時，回應會篩選為僅限指名的網域及可從中觸及的資料表。

**回應：**`text/plain` 格式的 GraphQL SDL。

---

### `GET /data/introspection`

傳回 GraphQL 內省 JSON，可選擇性地依網域篩選。 [tool-verified: `provisa/api/data/sdl.py:200`]

**標頭：**`X-Provisa-Role: <role_id>`（必要）

**查詢參數：**`domain` — 以逗號分隔的網域 ID。

**回應：**`application/json` 內省結果。

---

### `GET /data/graph-schema`

傳回該角色結構描述的圖形檢視：節點標籤及其關聯類型，供 Cypher/圖形用戶端使用。每個節點標籤皆包含 `pk_columns`，讓呼叫方可判斷主鍵欄位。（REQ-398） [tool-verified: `provisa/api/rest/cypher_router.py:689`]

**回應：**`application/json`，包含 `node_labels`（各自攜帶 `pk`/`pk_columns`）及 `relationship_types`。

---

### `GET /data/domains`

傳回請求角色可存取的網域 ID。 [tool-verified: `provisa/api/data/sdl.py:116`]

**標頭：**`X-Role: <role_id>`（必要）

**回應：**`["sales", "support", ...]`

---

### `GET /data/schema-version`

傳回目前的結構描述版本字串。結合了每次啟動的隨機值與重建計數器。用戶端以此在伺服器重新啟動後使結構描述快取失效。（REQ-537） [tool-verified: `provisa/api/data/sdl.py:102`]

**回應：**`{"version": "<boot-id>-<counter>"}`

---

### `GET /data/proto/{role_id}`

傳回某角色的自動產生 `.proto` 檔案。 [tool-verified: `provisa/api/data/endpoint_dev.py:49`]

**回應：**`text/plain` 格式的 protobuf 結構描述。

每一個已登記的資料表都會產生一個 proto `message`。關聯會產生巢狀訊息欄位。型別對映：`integer → int32`、`bigint → int64`、`varchar → string`、`decimal → double`、`boolean → bool`、`timestamp → google.protobuf.Timestamp`。（REQ-538）

---

### `GET /data/subscribe/{table}`

用於資料表即時變更通知的伺服器發送事件 (Server-Sent Events) 串流。（REQ-219、REQ-258） [tool-verified: `provisa/api/data/subscribe.py:239`]

通知傳送使用依來源類型挑選的可插拔供應商：PostgreSQL 來源使用 `LISTEN/NOTIFY`（經由 asyncpg）、MongoDB 來源使用 Change Streams（`collection.watch()`）、Kafka 來源使用消費者群組。每個供應商都實作一個共通的非同步監看介面。無論使用哪一個供應商，RLS 篩選及結構描述驗證皆會套用。（REQ-258）亦支援 WebSocket 及 RSS 來源。（REQ-338、REQ-342）

**標頭 — `X-Provisa-Sink`：**設為一個 Kafka 目標（例如 `kafka://broker:9092/topic`），以將變更事件重新導向至一個 Kafka sink，而非 SSE 回應。伺服器會啟動一個 sink 消費者，並傳回 `202 Accepted`，而非開啟一個串流。（REQ-812） [tool-verified: `provisa/api/data/subscription_sse.py:137`]

---

## 管理 REST 端點

### 設定

#### `GET /admin/config`

以 `application/x-yaml`、帶 `Content-Disposition: attachment` 標頭，下載目前的 `provisa.yaml`。（REQ-164） [tool-verified: `provisa/api/admin/settings_router.py:19`]

#### `PUT /admin/config`

上傳經修訂的設定 YAML。伺服器會寫入一份 `.bak` 備份、儲存新檔案，並重新載入所有結構描述、來源及具體化檢視。（REQ-164） [tool-verified: `provisa/api/admin/settings_router.py:32`]

**請求主體：**原始 YAML 內容。

**回應：**

```json
{"success": true, "message": "Config uploaded and reloaded"}
```

重新載入失敗時：`{"success": false, "message": "<error>"}`。

---

### 設定值 (Settings)

#### `GET /admin/settings`

以 JSON 傳回目前的平台設定值。（REQ-165） [tool-verified: `provisa/api/admin/settings_router.py:50`]

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

於執行時更新平台設定值。所有欄位皆為選填——僅有主體中出現的鍵值會被更新。（REQ-165） [tool-verified: `provisa/api/admin/settings_router.py:100`]

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
- `naming`：`domain_prefix`、`convention` ——寫入設定檔並觸發結構描述重新載入（REQ-253）
- `relationships`：`auto_track_fk`
- `otel`：`endpoint`、`service_name`、`sample_rate`、`support_endpoint`、`support_redact_sql_literals`、`support_redact_attributes`

**回應：**

```json
{"success": true, "updated": ["otel.support_endpoint", "cache.default_ttl"]}
```

---

### 可觀測性

#### `GET /admin/traces/recent`

從記憶體內的 span 緩衝區傳回最多 N 個近期已完成的 span。（REQ-302） [tool-verified: `provisa/api/admin/settings_router.py:317`]

**查詢參數：**`limit`（預設 50，上限 200）

**回應：**`{"traces": [...]}`

#### `POST /admin/query-engine/reload-catalog`

透過聯邦引擎協調器的 REST API，熱重新載入其中一個具名目錄。重新連接 Provisa 的內部連線，並重新執行 OTel DDL。 [tool-verified: `provisa/api/admin/settings_router.py:208`]

**查詢參數：**`catalog`（預設 `"otel"`）

**回應：**

```json
{"success": true, "errors": []}
```

#### `POST /admin/query-engine/restart`

重新啟動聯邦引擎容器（僅限單節點開發環境）。 [tool-verified: `provisa/api/admin/settings_router.py:287`]

**查詢參數：**`container`（預設為 `QUERY_ENGINE_CONTAINER` 環境變數，其後預設為 `"trino"`）

---

### 探索

#### `POST /admin/discover/relationships`

觸發關聯探索。一律會從聯邦引擎執行 FK 內省。（REQ-018）若已設定 `ANTHROPIC_API_KEY`，則執行 LLM 推論。（REQ-167） [tool-verified: `provisa/api/admin/discovery.py:55`]

**請求主體：**

```json
{
  "scope": "domain",
  "domain_id": "sales"
}
```

`scope` 必須為 `"table"`、`"domain"` 或 `"cross-domain"` 其中之一。對於 `"table"` 範圍，需要 `table_id`（整數）。對於 `"domain"` 範圍，需要 `domain_id`。

**回應：**`{"candidates_found": 12, "stored_ids": [1, 2, 3, ...]}`

#### `GET /admin/discover/candidates`

列出待處理的關聯候選項。 [tool-verified: `provisa/api/admin/discovery.py:96`]

#### `POST /admin/discover/candidates/{candidate_id}/accept`

接受一個候選項，並將其登記為一項關聯。 [tool-verified: `provisa/api/admin/discovery.py:103`]

**請求主體（選填）：**`{"name": "custom-relationship-name"}`

#### `POST /admin/discover/candidates/{candidate_id}/reject`

拒絕一個候選項。 [tool-verified: `provisa/api/admin/discovery.py:110`]

**請求主體：**`{"reason": "Not a real join"}`

#### `GET /admin/discover/candidates/rejected/count`

傳回已拒絕候選項的數量。 [tool-verified: `provisa/api/admin/discovery.py:118`]

#### `DELETE /admin/discover/candidates/rejected`

刪除所有已拒絕的候選項。 [tool-verified: `provisa/api/admin/discovery.py:128`]

---

### 來源爬取

#### `POST /admin/sources/crawl`

爬取一個數據來源以內省其結構描述並登記資料表。（REQ-012） [tool-verified: `provisa/api/admin/crawl_router.py:36`]

---

### 來源資料表搜尋

#### `GET /admin/sources/{source_id}/tables/search`

依名稱搜尋某來源中可用（尚未登記）的資料表。 [tool-verified: `provisa/api/admin/table_search_router.py:103`]

---

### 資料表輪廓分析

#### `POST /admin/tables/{table_id}/profile`

對某個已登記資料表執行欄位輪廓分析——基數、最小/最大值、null 比例。 [tool-verified: `provisa/api/admin/table_profile_router.py:28`]

---

### 來源描述

#### `POST /admin/source-meta/db-description`

為某個來源的資料表及欄位產生由 LLM 協助的描述。 [tool-verified: `provisa/api/admin/source_meta_router.py:48`]

---

### 動作（函式與 Webhook）

所有端點皆位於 `/admin/actions` 前綴下。（REQ-205） [tool-verified: `provisa/api/admin/actions_router.py:24`]

每一次呼叫——無論來自 GraphQL、SQL、Cypher、Bolt、Arrow Flight、MCP `run_sql`，還是 Provisa gRPC——都會經由單一個受治理的執行器路由，統一強制執行 `writable_by` 及治理規則。（REQ-1156） [tool-verified: `provisa/api/data/action_exec.py`]各協定的呼叫語法請參見 [docs/integrations.md](integrations.md#command)。

#### `GET /admin/actions`

傳回所有已追蹤的 DB 函式及 webhook。（REQ-242） [tool-verified: `provisa/api/admin/actions_router.py:104`]

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
      "writableBy": [],
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

每一個 webhook 物件皆攜帶一個 `approved` 布林值。當一位數據管家執行其建立請求後，該 webhook 即被核准（REQ-209）；設定中宣告的 webhook 則會自動核准。未經核准的 webhook 雖已登記，但不會公開於任何介面。 [tool-verified: `provisa/api/admin/actions_router.py:124-131`]

#### `POST /admin/actions/functions`

登記一個已追蹤的函式（指令）。（REQ-205） [tool-verified: `provisa/api/admin/actions_router.py:117`]

**主要欄位：**

| 欄位 | 是否必要 | 說明 |
| --- | --- | --- |
| `name` | 是 | 唯一的指令名稱 |
| `kind` | 是 | `"query"` → GraphQL Query 欄位；`"mutation"` → Mutation 欄位 |
| `implKind` | 否 | 指令的執行方式——見下表（預設 `source_procedure`） |
| `binding` | 否 | `implKind` 專屬的連線細節（JSON 物件） |
| `returnSchema` | 否 | JSON Schema `{type:"array", items:{type:"object", properties:{...}}}` ——使該指令在每一種介面上皆可傳回集合 |
| `arguments` | 否 | `[{name, type}]` 引數定義；位置順序對 SQL 及 Bolt 呼叫方而言至關重要 |
| `visibleTo` | 否 | 可呼叫此指令的角色 ID |
| `writableBy` | 否 | 被允許以 mutation 形式呼叫此指令的角色 ID |
| `domainId` | 否 | GraphQL 置放位置及存取控制所屬的網域 |

**`implKind` 值：**

| `implKind` | 執行內容 | `binding` 欄位 |
| --- | --- | --- |
| `source_procedure` | 已登記來源上的預儲程序（預設） | `sourceId`、`schemaName`、`functionName` |
| `script` | 伺服器端指令碼 | `script` |
| `http` | 出站 HTTP 呼叫 | `url`、`method` |
| `grpc` | 對外部伺服器的出站 gRPC 呼叫 | `target`、`method` |
| `python` | 由 Provisa 託管的 Python 可呼叫物件（REQ-885） | `callable`（例如 `"demo.py_functions:random_dataset"`） |

示範指令 `random_python_set`（`implKind: python`）及 `random_grpc_set`（`implKind: grpc`）實際展示了帶有 `returnSchema` 的可傳回集合指令；兩者都位於 `config/provisa-install.yaml` 中。 [tool-verified: `config/provisa-install.yaml:809-856`]

#### `PUT /admin/actions/functions/{name}`

依名稱更新一個已追蹤的函式。 [tool-verified: `provisa/api/admin/actions_router.py:182`]

#### `DELETE /admin/actions/functions/{name}`

依名稱刪除一個已追蹤的函式。 [tool-verified: `provisa/api/admin/actions_router.py:233`]

#### `POST /admin/actions/webhooks`

登記一個已追蹤的 webhook。（REQ-209）登記或更新一個 webhook 會將一項數據管家核准請求加入佇列——該 webhook 僅在數據管家核准後，才會在所有介面上啟用。設定中宣告的 webhook 會自動核准。**請求主體欄位：**`name`、`url`、`method`、`timeoutMs`、`returns`、`inlineReturnType`、`arguments`、`visibleTo`、`domainId`、`description`、`kind`。 [tool-verified: `provisa/api/admin/actions_router.py:132`, `provisa/api/admin/actions_router.py:325-331`]

#### `PUT /admin/actions/webhooks/{name}`

依名稱更新一個已追蹤的 webhook。任何編輯都會將核准狀態重設為待處理，直到重新核准為止。 [tool-verified: `provisa/api/admin/actions_router.py:306`]

#### `DELETE /admin/actions/webhooks/{name}`

依名稱刪除一個已追蹤的 webhook。 [tool-verified: `provisa/api/admin/actions_router.py:355`]

#### `POST /admin/actions/test`

依名稱測試一個動作（函式或 webhook）。（REQ-245） [tool-verified: `provisa/api/admin/actions_router.py:384`]

---

### 角色

所有端點皆位於 `/admin/roles` 前綴下。 [tool-verified: `provisa/api/admin/roles_router.py:18`]

| 方法 | 路徑 | 說明 |
| --- | --- | --- |
| `GET` | `/admin/roles/` | 列出所有角色 |
| `POST` | `/admin/roles/` | 建立一個角色 |
| `PUT` | `/admin/roles/{role_id}` | 更新一個角色 |
| `DELETE` | `/admin/roles/{role_id}` | 刪除一個角色 |

[tool-verified: `provisa/api/admin/roles_router.py`]

---

### 使用者

所有端點皆位於 `/admin/users` 前綴下。 [tool-verified: `provisa/api/admin/local_users_router.py:21`]

| 方法 | 路徑 | 說明 |
| --- | --- | --- |
| `POST` | `/admin/users/` | 建立一個本機使用者 |
| `GET` | `/admin/users/` | 列出本機使用者 |
| `GET` | `/admin/users/{user_id}` | 取得一位使用者 |
| `PUT` | `/admin/users/{user_id}` | 更新一位使用者 |
| `PATCH` | `/admin/users/{user_id}/password` | 變更密碼 |
| `DELETE` | `/admin/users/{user_id}` | 刪除一位使用者 |
| `GET` | `/admin/users/{user_id}/assignments` | 列出角色指派 |
| `POST` | `/admin/users/{user_id}/assignments` | 新增一項角色指派 |
| `DELETE` | `/admin/users/{user_id}/assignments/{assignment_id}` | 移除一項角色指派 |

---

### 組織

所有端點皆位於 `/admin/orgs` 之下。 [tool-verified: `provisa/api/admin/orgs_router.py:18`]

| 方法 | 路徑 | 說明 |
| --- | --- | --- |
| `GET` | `/admin/orgs/` | 列出組織 |
| `POST` | `/admin/orgs/` | 建立一個組織 |
| `PUT` | `/admin/orgs/{org_id}` | 更新一個組織 |
| `DELETE` | `/admin/orgs/{org_id}` | 刪除一個組織 |
| `GET` | `/admin/orgs/{org_id}/members` | 列出成員 |
| `POST` | `/admin/orgs/{org_id}/members` | 新增一位成員 |
| `DELETE` | `/admin/orgs/{org_id}/members/{user_id}` | 移除一位成員 |

---

### 邀請

所有端點皆位於 `/admin/invites` 之下。 [tool-verified: `provisa/api/admin/invites_router.py:18`]

| 方法 | 路徑 | 說明 |
| --- | --- | --- |
| `POST` | `/admin/invites/` | 建立一份邀請 |
| `GET` | `/admin/invites/` | 列出待處理的邀請 |
| `DELETE` | `/admin/invites/{token}` | 撤銷一份邀請 |

---

### 管理 GraphQL

#### `POST /admin/graphql`

所有管理操作的 Strawberry GraphQL 端點：來源及資料表的 CRUD、關聯管理、網域設定、RLS 規則、快取控制、命名慣例、排程工作管理，以及查詢編譯。（REQ-164） [tool-verified: `provisa/api/app.py:2171`]

**主要 mutation：**

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

[tool-verified: `provisa/api/admin/schema.py`, `provisa/api/admin/actions_router.py`]

---

### 設置 (Setup)

#### `GET /setup/status`

傳回首次執行的設置狀態。一律不需身分驗證。（REQ-539） [tool-verified: `provisa/api/setup_router.py:100`]

#### `POST /setup/`

完成首次執行的設置。 [tool-verified: `provisa/api/setup_router.py:142`]

---

## 健康檢查

#### `GET /health` 或 `HEAD /health`

傳回 `{"status": "ok"}`。一律不需身分驗證。（REQ-539） [tool-verified: `provisa/api/app.py:2258`]

---

## 錯誤回應

| 狀態碼 | 意義 |
| --- | --- |
| 400 | 無效的查詢、驗證錯誤，或 SQL 剖析錯誤 |
| 401 | 缺少或無效的身分驗證權杖 |
| 403 | 能力不足；治理違規 |
| 404 | 找不到角色、資源或設定檔 |
| 422 | 缺少必要標頭（例如 `X-Role`） |
| 503 | 資料庫或來源未連線；相依項無法使用 |
| 504 | 請求逾時 |

`POST /data/sql` 上的治理違規會傳回帶有結構化主體的 HTTP 403：（REQ-002） [tool-verified: `provisa/api/data/endpoint_dev.py:184-190`]

```json
{
  "detail": {
    "violations": [
      {"code": "V000", "message": "Table 'orders' is not accessible for role 'analyst'"}
    ]
  }
}
```

所有其他錯誤使用：`{"detail": "<message>"}`。

---

## Arrow Flight 端點

連接埠 `8815`。透過 gRPC 進行原生 Arrow 欄式傳輸。（REQ-143、REQ-045） [tool-verified: `provisa/api/flight/server.py`]

查詢及目錄探索兩者皆可在同一條連線上使用。完整的治理管線（RLS、遮罩、抽樣）會套用於每一則查詢。（REQ-130、REQ-143）

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

當 Zaychik Flight SQL 代理伺服器可用時（連接埠 8480），record batches 會端對端串流，不進行完整具體化。（REQ-144）若 Zaychik 不可用，則回退為透過聯邦查詢層進行具體化。（REQ-146）

---

## Protobuf gRPC 端點

連接埠 `50051`（可透過 `GRPC_PORT` 環境變數或 `server.grpc_port` 設定覆寫）。（REQ-529） [tool-verified: `provisa/grpc/server.py`, `provisa/api/app.py`]

透過 `x-provisa-role` gRPC metadata 鍵傳遞角色。若缺席，伺服器會以 `UNAUTHENTICATED` 中止。 [tool-verified: `provisa/grpc/server.py`]

從 `GET /data/proto/{role_id}` 下載該角色專屬的 proto。僅顯示該角色可見的資料表及欄位。（REQ-039）

```proto
service ProvisaService {
  rpc QueryOrders (QueryOrdersRequest) returns (stream Orders);
  rpc InsertOrders (InsertOrdersRequest) returns (InsertOrdersResponse);
}
```

每一個資料表都會產生一個 `Query{TypeName}` 串流 RPC。`Insert{TypeName}` RPC 為了結構描述對稱性而存在，但會以 `UNIMPLEMENTED` 中止。 [tool-verified: `provisa/grpc/server.py`]

已啟用 `grpc_reflection.v1alpha`，可在沒有預先編譯 proto 的情況下進行服務探索。（REQ-529） [tool-verified: `provisa/grpc/reflection.py`]

```bash
grpcurl -plaintext localhost:50051 list
grpcurl -plaintext -H 'x-provisa-role: analyst' \
  -d '{}' localhost:50051 ProvisaService/QueryOrders
```

gRPC 伺服器只有在啟動時能成功編譯出有效 proto 時才會啟動。若結構描述建置失敗，gRPC 伺服器將不會啟動。（REQ-529）

#### 聚合及分組 RPC（REQ-1359、REQ-1361、REQ-1405）

當某資料表已設定 `enable_aggregates` 時，所產生的 proto 會在 `Query{TypeName}` 之外，包含另外兩個 RPC：

- **`Query{TypeName}Aggregate`** ——傳回該資料表的聚合純量（`count`；每個數值欄位的 `sum`、`avg`、`stddev`、`variance`；每個可比較欄位的 `min`、`max`）
- **`Query{TypeName}GroupBy`** ——每一個分組鍵傳回一列，帶有聚合子欄位，並可選擇性地在一個 `nodes` 欄位中包含基礎資料表純量及已 join 的維度資料列

兩者都經由與 GraphQL 的 `{field}_aggregate` 及 `{field}_group_by` 根欄位相同的編譯器聚合管線路由——沒有另外獨立的聚合實作。（REQ-1359） [tool-verified: `provisa/grpc/query_ir.py:133-196`]

**`funcs` 欄位（REQ-1361）。**請求訊息接受一個 `funcs` 重複字串欄位。有效值為 `count`、`sum`、`avg`、`stddev`、`variance`、`min` 及 `max`。省略 `funcs` 時，會請求該結構描述為該資料表公開的每一個函式。設定時，僅出現已指名的函式。若已指名的函式沒有一個適用於該資料表的欄位型別，該查詢會回退為 `count`。 [tool-verified: `provisa/grpc/query_ir.py:66`, `provisa/grpc/query_ir.py:75-97`]

**`include_nodes` 及 `include` 欄位（REQ-1405）。**`Query{TypeName}GroupBy` 請求可設定 `include_nodes: true`，以在每一列的 `nodes` 欄位中包含基礎資料表純量欄位。`include` 重複字串欄位指名多對一關聯欄位，其純量欄位同樣會巢狀於 `nodes` 之內。這與 JSON:API 的 `?includeNodes=` / `?include=` 行為一致。 [tool-verified: `provisa/grpc/query_ir.py:168-195`]

---

## JDBC 驅動程式

Provisa JDBC 驅動程式（`provisa-jdbc-0.1.0.jar`）將語義目錄公開給 BI 工具（Tableau、PowerBI、DBeaver）。（REQ-126）

**連線網址：**`jdbc:provisa://host:port`（REQ-131）

網域對映至 JDBC 結構描述。（REQ-127）資料表使用其已登記的別名。欄位使用別名，並以 `REMARKS` 形式公開描述。（REQ-128）標準 metadata 方法（`getPrimaryKeys`、`getImportedKeys`、`getExportedKeys`）將語義關聯公開為 PK/FK metadata。

**SQL 支援：**`SELECT * FROM <alias> [WHERE col = 'value']`。（REQ-129）

驅動程式預設請求 Arrow IPC 重新導向。結果以批次方式透過 `ArrowStreamReader` 串流，記憶體中最多保留一個 record batch。（REQ-293）

---

## `orderBy` 引數格式

`order_by` 引數使用 `{column: direction}` 物件，具備一個六值的方向列舉：（REQ-200）

```json
{
  "query": "{ orders(order_by: [{created_at: desc_nulls_last}]) { id created_at } }",
  "role": "admin"
}
```

支援的方向：`asc`、`desc`、`asc_nulls_first`、`asc_nulls_last`、`desc_nulls_first`、`desc_nulls_last`。（REQ-201）

---

## 訂閱

SSE 訂閱可於 `GET /data/subscribe/{table}` 取得。（REQ-219、REQ-258）通知傳送使用依來源類型挑選的可插拔供應商：PostgreSQL 來源使用 `LISTEN/NOTIFY`、MongoDB 來源使用 Change Streams、Kafka 來源使用消費者群組。無論使用哪一個供應商，RLS 篩選及結構描述驗證皆會套用。亦可透過同一個端點支援 WebSocket 及 RSS 來源。（REQ-338、REQ-342） [tool-verified: `provisa/api/data/subscribe.py:239`, `provisa/subscriptions/registry.py`, `provisa/api/app.py` `_rebuild_schemas`]

---

## 業務詞彙表（REQ-1387）

業務詞彙表將物理欄位名稱——即其在來源資料庫中的實際形式——對映至一套共用的人類詞彙。每一個登記於語義層的欄位都會自動取得一個詞條。填入詞彙表不需要任何人手輸入；策展人是在系統衍生內容之上新增定義、關聯及專家標註。

### 詞條如何衍生

當 Provisa 登記或更新某資料表的欄位時，`normalize_term`（`provisa/core/glossary.py`）會對每一個欄位名稱執行，並產生一個規範化片語。 [tool-verified: `provisa/core/repositories/glossary.py:sync_table_refs`]

規範化依序套用五項規則：

1. 依 camelCase 邊界及分隔字元（`_`、`-`、`.`、`/`、空白）拆分。
2. 將結果轉為小寫。
3. 展開一份固定的縮寫對照表（例如 `cust` → `customer`、`amt` → `amount`、`dt` → `date`、`id` → `identifier`、`key` → `identifier`、`guid` → `identifier`）。
4. 去除結尾的**代理符記 (proxy token)**（`identifier`、`code`、`index` 或 `reference`）——以其鍵值或代碼命名的欄位，是透過一個替代值指向底層概念，因此該詞條應為概念本身。最後剩下的符記絕不會被去除。
5. 以資料表的概念，限定一個**過於通用的片語**。當完整的規範化片語是一個裸露的屬性詞（`name`、`identifier`、`date`、`location`、`message`、`first name`、`last name` 及類似詞彙）時，該詞條會變成 `<table concept> <phrase>`——`employees.first_name` → `employee first name`、`orders.id` → `order identifier`。若跨不相關資料表共用一個 `name` 詞條，會混淆不同的意義；限定條件會將每一個欄位連接至其所屬的概念，而非如此。資料表概念是該資料表的業務名稱，以一個單數形式的中心名詞規範化（`order_lines` → `order line`）。

原生篩選假欄位（`_nf_` 前綴，或任何攜帶 `native_filter_type` 的欄位）是查詢參數機制，並非業務欄位，因此不衍生任何詞條。

由於 `id`、`key`、`pk` 及 `sk` 在代理符記檢查之前都會展開為 `identifier`，三個實際上不同的物理欄位名稱會落在完全相同的詞條上：

| 物理名稱 | 規範化後 |
| --- | --- |
| `cust_id` | `customer` |
| `customerId` | `customer` |
| `CUSTOMER_KEY` | `customer` |
| `txn_amt` | `transaction amount` |

前三者收合為同一個詞條。`transaction amount` 保留兩個符記，因為 `amount` 並非代理符記。一個裸露的 `id` 欄位——前面沒有其他符記——無法被去除；它會規範化為 `identifier`，使該詞條不致為空。 [tool-verified: `provisa/core/glossary.py:normalize_term`]

### 生命週期

詞條是**由語義層的成員資格所衍生**，而非由使用者隨選建立。資料表儲存庫是唯一的寫入路徑：`sync_table_refs` 會在每一次欄位集合的 upsert 中執行，`sweep_refless_terms` 則會在任何刪除路徑之後執行。 [tool-verified: `provisa/core/repositories/glossary.py`]

**當新增一個欄位時：**Provisa 依名稱查找規範化詞條。若已存在，該欄位會取得一個指向它的參照（若該詞條先前已被淘汰，則會被恢復——`deprecated` 重設回 `False`）。若尚無此詞條，則建立一個新的。

**當一個欄位離開時**（結構描述變更或資料表移除）：其參照會被刪除，且該詞條會依「移除或淘汰」規則進行結算。一個已無任何剩餘參照的根詞條（rooted term）會被徹底移除——連同其邊及專家指派——除非移除它會使某個抽象詞條與所有根詞條斷開連接（詞條圖中無任何路徑）。在此情況下，該詞條會被**淘汰**（標記 `deprecated=True`）而非刪除，讓該抽象詞條的圖形錨點得以存續。

抽象詞條絕不會被自動移除；它們存在於物理生命週期之外，僅能透過管理 API 明確刪除。

**恢復：**若一個已淘汰詞條的規範化名稱再度出現（某欄位被重新登記），該詞條會被取消標記，其參照會恢復累積。

### 策展端點

所有端點皆位於 `/admin/glossary` 之下。它們需要 `org_admin` 存取權，及一個已設定的組織。每一次 mutation 都會觸發一次 metadata 發佈。 [tool-verified: `provisa/api/admin/glossary_router.py`]

| 方法 | 路徑 | 說明 |
| --- | --- | --- |
| `GET` | `/admin/glossary/terms` | 列出詞條。查詢參數：`q`（名稱/定義搜尋）、`include_deprecated`（預設 `true`） |
| `GET` | `/admin/glossary/terms/{term_id}` | 取得詞條細節：定義、物理參照、具型別的邊、專家 |
| `POST` | `/admin/glossary/terms` | 建立一個抽象詞條——沒有物理參照的使用者詞彙 |
| `PATCH` | `/admin/glossary/terms/{term_id}` | 重新命名、設定定義，或切換匯出排除 |
| `DELETE` | `/admin/glossary/terms/{term_id}` | 刪除一個沒有物理參照的詞條 |
| `POST` | `/admin/glossary/refs/move` | 將一個物理參照移至另一個詞條（合併） |
| `POST` | `/admin/glossary/terms/{term_id}/edges` | 於兩個詞條之間新增一個具型別的關聯邊 |
| `DELETE` | `/admin/glossary/terms/{term_id}/edges` | 移除一條邊（查詢參數：`to_term_id`、`rel_type`） |
| `POST` | `/admin/glossary/terms/{term_id}/experts` | 將某使用者標記為某詞條的專家或作者 |
| `DELETE` | `/admin/glossary/terms/{term_id}/experts/{user_id}` | 移除某使用者的專家/作者標記 |
| `POST` | `/admin/glossary/terms/{term_id}/definition/generate` | 使用組織的 AI 模型為單一詞條草擬一個定義——僅傳回文字，儲存前不會有任何內容被持久化 |
| `POST` | `/admin/glossary/definitions/generate` | 為每一個尚無定義的詞條產生並持久化定義——絕不覆寫人手撰寫的文字 |
| `POST` | `/admin/glossary/relationships/generate` | 使用組織的 AI 模型，對整個詞彙表提議並持久化具型別的邊 |

**`POST /admin/glossary/terms` 主體：**

```json
{"name": "revenue", "definition": "Recognized net revenue after returns and discounts."}
```

**`POST /admin/glossary/terms/{term_id}/edges` 主體：**

```json
{"to_term_id": 42, "rel_type": "KIND_OF"}
```

有效的 `rel_type` 值：`KIND_OF`、`RELATED_TO`、`PART_OF`、`SYNONYM_OF`。 [tool-verified: `provisa/core/glossary.py:TERM_EDGE_TYPES`]

**`POST /admin/glossary/terms/{term_id}/experts` 主體：**

```json
{"user_id": "alice@example.com", "kind": "author"}
```

有效的 `kind` 值：`expert`、`author`。 [tool-verified: `provisa/core/repositories/glossary.py:add_expert`]

**`POST /admin/glossary/refs/move` 主體：**

```json
{"table_id": 7, "column_name": "cust_id", "to_term_id": 12}
```

移動一個參照，會依「移除或淘汰」規則結算失去該參照的詞條。可用此方式合併兩個因規範化而被分隔的詞條——例如，當某來源使用了展開對照表以外的非標準縮寫時。

刪除一個根詞條（帶有物理參照的詞條）會傳回 `400 glossary.invalid`。請先移除或移動所有參照。

**`PATCH /admin/glossary/terms/{term_id}` — `export_excluded` 欄位：**

```json
{"export_excluded": true}
```

將 `export_excluded` 設為 `true`，會將該詞條從所有 metadata 匯出快照中排除，無論其物理參照或抽象狀態為何。將其設回 `false`，會在下一次發佈時將該詞條恢復至快照中。策展數據（定義、邊、專家）不受影響。 [tool-verified: `provisa/core/repositories/glossary.py:set_export_excluded`, `provisa/api/admin/glossary_router.py:update_term`]

### AI 輔助策展

組織已設定的 AI 模型可一次操作，草擬定義並對整個詞彙表提議關聯邊。兩項批次操作皆需要 `org_admin` 存取權及一個已設定的組織。

**`POST /admin/glossary/definitions/generate`**

遍歷詞彙表中的每一個詞條，跳過已有定義的詞條，並呼叫組織的 AI 模型為每一個剩餘詞條草擬一個定義。草稿會立即被持久化——不同於逐詞條的草稿端點（`POST /admin/glossary/terms/{term_id}/definition/generate`），這裡沒有編輯步驟。人手撰寫的定義絕不會被覆寫：在任何模型呼叫之前的守衛條件是 `if summary["definition"]: continue`。整批僅有一則發佈通知。 [tool-verified: `provisa/api/admin/glossary_router.py:generate_all_definitions`]

回應：

```json
{"generated": 12}
```

`generated` 是取得新定義的詞條數量。當每一個詞條都已有定義時，此值為零。

**`POST /admin/glossary/relationships/generate`**

將完整的詞條清單送至組織的 AI 模型，提示內容中指明十種允許的邊類型（`KIND_OF`、`PART_OF`、`SYNONYM_OF`、`RELATED_TO`、`VALID_VALUE_OF`、`DERIVED_FROM`、`REPLACES`、`PREFERRED_TERM_FOR`、`TRANSLATION_OF`、`ANTONYM_OF`），並要求只提出有把握的提議。模型會以一個 JSON 陣列回應；每一項條目在任何寫入之前都會被驗證：未知的詞條名稱、自我連結的邊，以及封閉列舉範圍以外的邊類型，皆會被靜默捨棄。有效的提議會以冪等方式 upsert——重複執行此動作不會產生重複的邊。整批僅有一則發佈通知。當詞彙表中未淘汰的詞條少於兩個時，此端點會立即傳回 `{"added": 0}`。 [tool-verified: `provisa/api/admin/glossary_router.py:generate_relationships`]

回應：

```json
{"added": 5}
```

`added` 是寫入的邊數量。已存在的邊仍計入其中——該次 upsert 成功，但邊的數據不會變更。

### MCP `search_terms` 工具

```
search_terms(query, role=None, limit=25)
```

以不分大小寫的子字串比對，搜尋詞條名稱及定義，最多傳回 `limit` 個結果。每一個結果都是完整的詞條細節：`name`、`definition`、`is_abstract`、`deprecated`、物理參照（含 `source_id`、`schema_name`、`table_name`、`column_name`）、具型別的邊，以及專家指派。 [tool-verified: `provisa/api/mcp/server.py:236-244`, `provisa/core/repositories/glossary.py:search_terms`]

在撰寫 SQL 之前，先使用 `search_terms` 依名稱找出所有代表某個概念的物理欄位。例如，搜尋 `"order date"` 會傳回該詞條，以及跨每一個已登記資料表的所有 `order_dt`、`orderDate`、`ORDER_DATE` 欄位。

### Metadata 匯出

詞彙表詞條圖會包含在 `build_snapshot` 建構的每一個 `MetadataSnapshot` 中。 [tool-verified: `provisa/api/metadata_export/builder.py:_glossary_assets`]

該匯出套用與快照其餘部分相同的篩選條件：

- 標記為 `export_excluded` 的詞條會被徹底排除——無論其物理參照、抽象狀態，或該組織的目錄是否已設定。 [tool-verified: `provisa/api/metadata_export/builder.py:_glossary_assets`]
- 一個根詞條只有在其至少一個物理參照，同時通過**數據產品**篩選（該資料表的 `data_product` 旗標必須為 `true`）及**技術性**欄位篩選（標記為 `technical` 的欄位會被排除）時，才會發佈。
- 一個根詞條若其所有參照都被上述篩選排除，該詞條也會隨之被排除。
- 抽象詞條無條件發佈——它們是使用者詞彙，不受限於物理欄位。
- 兩個詞條之間的邊，僅在兩端詞條皆有發佈時才會發佈。

每一個供應商轉接器都會將詞條圖原生發佈至一個由 Provisa 擁有、以冪等方式建立的詞彙表容器中——絕不會發佈至既有的目錄詞彙表：

| 供應商 | 容器 | 詞條 | 關係 | 淘汰標記 |
| --- | --- | --- | --- | --- |
| Apache Atlas | "Provisa Glossary"（詞彙表 API） | 詞彙表詞條，定義寫入 `longDescription` | KIND_OF → `isA`、SYNONYM_OF → `synonyms`、RELATED_TO/PART_OF → `seeAlso` | `[DEPRECATED]` shortDescription 標記 |
| Atlan | 以穩定 qualifiedName 表示的 Provisa 詞彙表 | `longDescription`（絕不使用人手編輯的 `userDescription`） | 與 Atlas 相同的對映 | `certificateStatus = DEPRECATED` |
| DataHub | `urn:li:glossaryNode:provisa.<org>` | 每個詞條一個 `glossaryTermInfo` 切面 | KIND_OF → Inherits、PART_OF → Contains（反轉）、RELATED_TO/SYNONYM_OF → related terms | 淘汰切面；重新命名遵循 URN 繼承 |
| OpenMetadata | 透過 `/v1/glossaries` 的 Provisa 詞彙表 | 以 fqn 為鍵的 PUT，重新命名以已儲存的 UUID 進行 PATCH-rebind | KIND_OF → 原生父層階層、SYNONYM_OF → `synonyms`、其他 → `relatedTerms` | `entityStatus` |
| Collibra | 詞彙表類型網域 "Provisa Glossary" | 透過 Import API 的 Business Term 資產 | 原生 Business Term 關係類型 | 資產狀態 |

所有權綁定於繫結本身，而非名稱：每一個已發佈詞條的供應商 id，皆會被擷取進 `catalog_bindings` 中，置於該詞條的 URN（`provisa://<org>/terms/<name>`）之下，Provisa 只有在持有該項繫結（或該項目位於其所建立、由 Provisa 擁有的容器中）時，才會修改或刪除供應商端的詞彙表項目。沒有 Provisa 繫結的詞彙表項目，是源自外部系統的，絕不會被觸碰；更新採讀取合併方式，因此數據管家在 Provisa 自身詞條上新增的欄位得以存續；當某個詞條離開快照時，不會有任何內容被刪除。數據管家的詞條對資產指派，仍由外部擁有——沒有任何轉接器會寫入詞條對資產的指派（由 Provisa 撰寫的指派發佈是明確的後續項目）。特別是在 Collibra 上，Import API 的 REPLACE 語義下的安全性，仰賴於範圍限定：承載內容僅提及 Provisa 詞彙表網域內的資產，以及僅限 Provisa 詞條之間的關係實例，因此數據管家的詞彙表及其關係絕不會被觸及。 [tool-verified: `provisa/api/metadata_export/atlan.py`, `provisa/api/metadata_export/datahub.py`, `provisa/api/metadata_export/atlas.py`, `provisa/api/metadata_export/openmetadata.py`]
