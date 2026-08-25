# API 參考

## 概觀

Provisa 在兩個前綴下公開 REST 端點：`/data` 負責查詢執行與結構描述自省，`/admin` 負責組態管理。(REQ-043) 大部分數據端點需要角色識別碼。管理員組態操作使用位於 `/admin/graphql` 的 Strawberry GraphQL API。(REQ-164)

---

## 驗證

當 `provisa.yaml` 中設定了 `auth.provider` 時，除 `/health` 與 `/setup/status` 以外的所有端點都需要 `Authorization: Bearer <token>` 標頭。(REQ-120) [tool-verified: `provisa/api/app.py`, `provisa/auth/wiring.py`]

未設定驗證時，伺服器以開發模式執行。任何請求都被視為 `anonymous` 身分，該身分對映到所有已設定的角色，並具備萬用字元網域存取權。(REQ-535)

**登入（`POST /auth/login`）** 由使用中的驗證提供者在設定 `provider: basic` 時提供。(REQ-124) 憑證格式與回應取決於提供者。

**身分自省：**

```http
GET /auth/me
```

傳回已驗證使用者的 id、電郵、顯示名稱、組織成員資格與角色指派。在開發模式下傳回 `dev_mode: true`，並列出所有角色 ID。[tool-verified: `provisa/api/auth_router.py`]

```http
GET /auth/provider-type
```

傳回 `{"provider": "<name>"}`，或在未設定驗證時傳回 `{"provider": null}`。[tool-verified: `provisa/api/auth_router.py`]

---

## 數據端點

### `POST /data/graphql`

執行 GraphQL 查詢或變更操作。(REQ-043) [tool-verified: `provisa/api/data/endpoint.py:151`]

**請求主體：**

```json
{
  "query": "{ orders(where: {region: {eq: \"us\"}}) { id amount } }",
  "variables": {},
  "role": "admin",
  "extensions": {}
}
```

`role` 欄位僅在開發模式（無驗證）下使用。驗證啟用時，會採用已驗證使用者的角色，主體中的 `role` 會被忽略。

`extensions` 欄位支援自動持續化查詢（APQ）通訊協定：(REQ-288)

```json
{
  "extensions": {"persistedQuery": {"sha256Hash": "<sha256-of-query>"}}
}
```

**標頭：**

- `X-Provisa-Role` — 覆寫角色（開發模式）
- `Accept` — 回應格式（見內容協商）
- `Authorization` — 啟用驗證時為 `Bearer <token>`
- `X-Provisa-Redirect-Format` — S3 重新導向輸出的 MIME 類型 (REQ-137)
- `X-Provisa-Redirect-Threshold` — 觸發重新導向的資料列數門檻 (REQ-137)
- `X-Provisa-Redirect` — 設為 `true` 可無條件強制重新導向 (REQ-029)

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

多根欄位查詢會各自獨立執行每個根欄位。低於重新導向門檻的欄位以內嵌方式傳回；高於門檻的欄位則重新導向。`redirects` 索引鍵（複數）將欄位名稱對映到重新導向資訊。(REQ-029) [tool-verified: `provisa/api/data/endpoint.py`]

**快取標頭：**

- `X-Provisa-Cache: HIT|MISS` (REQ-536)
- `X-Provisa-Cache-Age: <seconds>`（HIT 時）(REQ-536)

**必要能力：** 所有請求（包括自省）皆需 `QUERY_DEVELOPMENT`。[tool-verified: `provisa/api/data/endpoint.py:186-283`]

---

### 內容協商

| Accept 標頭 | 格式 |
| --- | --- |
| `application/json` | JSON（預設） |
| `application/x-ndjson` | 換行分隔的 JSON |
| `text/csv` | CSV |
| `application/vnd.apache.parquet` | Parquet |
| `application/vnd.apache.arrow.stream` | Arrow IPC |

(REQ-047, REQ-048, REQ-049, REQ-050) [tool-verified: `provisa/api/data/endpoint.py:84-90`]

---

### 重新導向

超出已設定資料列門檻的結果（或當 `X-Provisa-Redirect: true` 時）會寫入 S3，並傳回預先簽署的 URL。(REQ-029, REQ-044)

| 重新導向格式 | 寫入方 | 記憶體 |
| --- | --- | --- |
| `application/vnd.apache.parquet` | 聯邦 CTAS | 無 — 數據從不經過 Provisa |
| `application/x-orc` | 聯邦 CTAS | 無 — 數據從不經過 Provisa |
| `application/json` | Provisa | 受記憶體限制 |
| `application/x-ndjson` | Provisa | 受記憶體限制 |
| `text/csv` | Provisa | 受記憶體限制 |
| `application/vnd.apache.arrow.stream` | Provisa | 受記憶體限制 |

大型分析匯出請使用 Parquet 或 ORC 重新導向。聯邦引擎會並行直接寫入 S3 — 數據不經過 Provisa。(REQ-138)

```yaml
X-Provisa-Redirect-Format: application/vnd.apache.parquet
X-Provisa-Redirect-Threshold: 1000
```

---

### `POST /data/sql`

透過第 2 階段治理管線執行原始 SQL。(REQ-267) [tool-verified: `provisa/api/data/endpoint_dev.py:62`]

**請求主體：**

```json
{
  "sql": "SELECT id, amount FROM orders WHERE region = 'us'",
  "role": "admin"
}
```

**必要能力：** `QUERY_DEVELOPMENT`。

`POST /data/sql` 上的治理違規會傳回 HTTP 403。(REQ-002, REQ-266)

**回應：** 與 `/data/graphql` 格式相同（預設為 JSON 資料列，並透過 `Accept` 進行內容協商）。

---

### `POST /data/query`

統一查詢端點。接受 GraphQL、SQL 或 Cypher — 語法會自動偵測。(REQ-267) [tool-verified: `provisa/api/data/endpoint_dev.py:509`]

Cypher 查詢也可以提交到僅限 Cypher 的 `POST /query/cypher` 端點。(REQ-345)

**請求主體：**

```json
{
  "query": "{ orders { id } }",
  "params": {},
  "variables": {},
  "role": "admin"
}
```

GraphQL 傳回 `{"data": ...}`，SQL 與 Cypher 傳回 `{"columns": [...], "rows": [...]}`。

---

### `POST /data/sql/explain`

透過受治理的管線說明或分析 SQL 陳述式。(REQ-1519) [tool-verified: `provisa/api/data/endpoint_dev.py:328`]

該端點會將**受治理的** SQL — 即在呼叫者角色下、套用 RLS 與遮罩後實際執行的陳述式 — 包裝在該方言的 EXPLAIN 語法中。計劃顯示的是查詢的已授權版本，而非原始輸入。

**請求主體：**

```json
{
  "sql": "SELECT id, amount FROM orders",
  "role": "admin",
  "analyze": false
}
```

設定 `analyze: true` 可執行 EXPLAIN ANALYZE。查詢會實際執行，計劃會帶有真實的資料列數與時間。並非每種方言都支援 ANALYZE；請參閱 [查詢計劃與統計資料](engines.md#query-plans-and-statistics) 中的表格。

**回應：** `{"plan": "<plan text or JSON>", "dialect": "trino", "analyzed": false}`

當方言不支援 EXPLAIN，或在不支援的方言（例如 SQLite）上要求 `analyze: true` 時傳回 `400`。[tool-verified: `provisa/executor/explain.py:wrap_explain`, `analyze_sql`]

---

### `GET /data/engine/state`

在不喚醒引擎的情況下傳回引擎分區的目前狀態。(REQ-1516) [tool-verified: `provisa/api/data/endpoint_dev.py:892`]

UI 會輪詢此端點，以在引擎冷啟動期間顯示啟動橫幅。它從不觸發喚醒 — 輪詢是安全的，也不會被閒置回收器視為活動。

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

[tool-verified: `provisa/federation/engine_wake.py:engine_state`]

---

### `POST /data/engine/prewarm`

在不執行查詢的情況下觸發引擎喚醒。(REQ-1516) [tool-verified: `provisa/api/data/endpoint_dev.py:913`]

立即傳回 `202 Accepted`。喚醒在背景執行。如果你希望引擎在第一個查詢抵達之前就緒，可使用此端點 — 例如由數分鐘後才執行查詢的排程器呼叫。

**回應：** `202 Accepted`，主體為 `{"started": true}`

[tool-verified: `provisa/federation/engine_wake.py:prewarm_engine`]

---

### `GET /data/rest/{domain_id}/{table_name}`

為每個已註冊資料表自動產生的純 REST 端點。查詢字串會對映到 GraphQL 引數，請求會透過與 GraphQL 相同的管線（RLS、遮罩、路由）編譯並執行。(REQ-256) [tool-verified: `provisa/api/rest/generator.py:153`]

**查詢參數：**

- `limit` — 最大資料列數（≥ 1）
- `offset` — 略過的資料列數（≥ 0）
- `fields` — 以逗號分隔的欄位名稱（預設為所有純量欄位）
- `filter` — `{"field", "comparator", "value"}` 篩選物件的 JSON 陣列
- `orderBy` — `{"field", "direction"}` 排序物件的 JSON 陣列

需要已驗證的角色；未驗證的請求傳回 `401`。這些路由的 OpenAPI 規格由 `GET /data/rest/openapi.json` 提供，Swagger UI 位於 `GET /data/rest/docs`。

#### OpenAPI / Swagger UI 探索工具

OpenAPI 探索工具頁面（`/app/openapi`）在沙箱化的 iframe 中內嵌 Swagger UI。規格以角色為範圍 — 只會出現目前角色可見的資料表與欄位 — 並可透過網域選擇器選擇性地依網域篩選。UI 會自動在淺色與深色佈景主題之間切換。[tool-verified: `provisa-ui/src/pages/OpenApiPage.tsx:20-34`]

該頁面透過 `fetch()` 而非直接使用 iframe `src` 載入規格 HTML，因此請求會攜帶工作階段的 bearer 權杖，而 Swagger UI 本身的相對請求也能正確解析到同一來源。[tool-verified: `provisa-ui/src/pages/OpenApiPage.tsx:44-69`]

從自然語言的「Open in OpenAPI」連結導覽而來時，該頁面會自動展開目標端點、以自然語言產生的 URL 填入查詢參數（例如 `aggregate`、`groupBy`），並按下 Execute — 使用 DOM 輪詢確保每一步在下一步觸發前已完成。(REQ-1359) [tool-verified: `provisa-ui/src/pages/OpenApiPage.tsx:94-171`]

---

### `GET /data/jsonapi/{domain_id}/{table_name}`

為每個已註冊資料表自動產生、符合 [JSON:API](https://jsonapi.org) 規範的端點。與 GraphQL 採用相同的 RLS、遮罩與路由。(REQ-257) [tool-verified: `provisa/api/jsonapi/generator.py:284`]

**`Accept` 標頭：** 必須包含 `application/vnd.api+json`（JSON:API 媒體類型），否則請求傳回 `406`。

**查詢參數：**

- `fields[<type>]` — 稀疏欄位集，例如 `?fields[orders]=amount`
- `filter[<col>]` / `filter[<col>][<op>]` — 例如 `?filter[region]=US`、`?filter[amount][gt]=100`
- `sort` — 以逗號分隔，前綴 `-` 表示遞減，例如 `?sort=-created_at,amount`
- `page[number]` / `page[size]` — 分頁
- `aggregate` — 以逗號分隔的彙總函式，取代資料列擷取執行：`count`、`sum`、`avg`、`stddev`、`variance`、`min`、`max`。使用 `?aggregate=count,sum` 可要求其中一部分。彙總回應傳回 `data: null`，結果放在 `meta.aggregate` 中。(REQ-1359) [tool-verified: `provisa-ui/src/pages/JsonApiPage.tsx:238`]
- `groupBy` — 以逗號分隔的欄位名稱；與 `?aggregate=` 搭配使用以分組結果。只有資料表 `DistinctOnColumn` 列舉中的欄位有效；角色看不到的欄位，伺服器一律傳回 `400`。(REQ-1361) [tool-verified: `provisa-ui/src/pages/JsonApiPage.tsx:447`]
- `includeNodes` — 設為 `true` 可在每個群組資料列的 `nodes` 陣列中包含基礎資料表的純量欄位（以及 `include=` 中指名的聯結維度純量）。當自然語言分組查詢同時要求維度明細時為必要。(REQ-1405)

回應為帶有 `type`/`id`/`attributes` 的資源物件。錯誤遵循 JSON:API 錯誤物件的形狀。

#### JSON:API 探索工具

JSON:API 探索工具頁面（`/app/jsonapi`）是這些端點之上的瀏覽器 UI。從依網域分組的清單中選取一個資料表，然後設定：

- **Fields** — 選擇要包含哪些欄位（稀疏欄位集）；全部不勾選則要求所有欄位
- **Relationships** — 選取由外部索引鍵衍生的關聯名稱，以 `?include=` 一併載入
- **Filter** — 欄位、運算子（`eq`、`neq`、`gt`、`gte`、`lt`、`lte`、`like`）與值
- **Sort** — 單一欄位，遞增或遞減
- **Aggregate** — 從伺服器驗證過的清單中挑選分組欄位，然後勾選一個或多個彙總函式；選取分組欄位後，「Include nodes」核取方塊會將基礎資料表的純量欄位附加到每一列
- **Page size** — 每頁資源數，附首頁／上一頁／下一頁／末頁導覽

結果以格式化摘要檢視（帶可點擊關聯錨點的資源卡片）或原始 JSON 分頁呈現。即時請求 URL 會顯示出來並可複製。資料表選取與每頁大小會透過 `localStorage` 跨工作階段保留。[tool-verified: `provisa-ui/src/pages/JsonApiPage.tsx`]

從自然語言的「Open in JSON:API」連結導覽而來時，探索工具會預先選取資料表，並依自然語言產生的查詢參數預填彙總挑選器，然後自動執行請求。[tool-verified: `provisa-ui/src/pages/JsonApiPage.tsx:460-479`]

---

### `POST /query/nl`

提交自然語言問題。服務會啟動非同步作業，並立即傳回 `202 Accepted` 與一個 `job_id`。需要在 `ai_models` 組態區段下設定 LLM 提供者。(REQ-354) [tool-verified: `provisa/api/rest/nl_router.py:50`]

**請求主體：**

```json
{"q": "How many orders were placed last month?", "role": "admin"}
```

傳回 `{"job_id": "<id>"}`。超出每個角色的自然語言速率限制時傳回 `429`，並附帶 `Retry-After` 標頭。(REQ-370)

**擷取結果：**

- `GET /query/nl/{job_id}` — 輪詢。傳回作業文件。
- `GET /query/nl/{job_id}/stream` — SSE。每個產生目標完成時發出一個 `branch` 事件，最後發出 `done` 事件。(REQ-357, REQ-358)

三個產生迴圈（Cypher、GraphQL、SQL）並行執行，各自經編譯器驗證並在出錯時修正。(REQ-355) 提示詞的範圍限定在該角色可見的結構描述內。(REQ-356) 結果文件依目標為每個分支建立索引鍵：(REQ-357) [tool-verified: `provisa/nl/job.py:69`]

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

耗盡疊代上限的分支會傳回 `query: null`、`result: null` 與一個 `error` 字串。每個產生的查詢都在消費者的權限下執行，並套用第 2 階段治理 — 服務從不繞過治理。(REQ-359)

#### 帶維度明細的自然語言分組查詢 (REQ-1405)

當自然語言分組查詢同時投影聯結維度資料表的欄位時 — 例如「按使用者統計查詢數，並附使用者名稱與電郵」— 執行器會從 SELECT 投影的維度欄位推導出每個欄位的點路徑（`dim_paths`）。這些路徑會填入 JSON:API 與 OpenAPI 面板所產生 URL 的 `includeNodes=` 參數，讓這些面板要求與 SQL 及 GraphQL 分支所解析的相同聯結維度欄位。若無此機制，`includeNodes=true` 只會傳回基礎彙總資料表自身的純量欄位。(REQ-1405) [tool-verified: `docs/arch/requirements.md:REQ-1405`]

在 gRPC 面板上，產生的 `{Type}GroupByRequest` 帶有 `include_nodes`（布林值）與 `include`（關聯欄位名稱的重複字串）。傳回的 `{Type}GroupByRow` 包含帶有維度明細資料列的具型別 `nodes` 欄位。[tool-verified: `provisa/grpc/query_ir.py:168-196`]

---

### `GET /data/sdl`

傳回某個角色結構描述的 GraphQL SDL。(REQ-008) [tool-verified: `provisa/api/data/sdl.py:137`]

**標頭：** `X-Role: <role_id>`（必要）

**查詢參數：**

- `domain` — 以逗號分隔的網域 ID。設定後，回應會篩選為指名網域及可從其抵達的資料表。

**回應：** `text/plain` GraphQL SDL。

---

### `GET /data/introspection`

傳回 GraphQL 自省 JSON，可選擇依網域篩選。[tool-verified: `provisa/api/data/sdl.py:200`]

**標頭：** `X-Provisa-Role: <role_id>`（必要）

**查詢參數：** `domain` — 以逗號分隔的網域 ID。

**回應：** `application/json` 自省結果。

---

### `GET /data/graph-schema`

傳回該角色結構描述的圖形檢視：節點標籤及其關聯類型，供 Cypher／圖形用戶端使用。每個節點標籤包含 `pk_columns`，讓呼叫者可判斷主索引鍵欄位。(REQ-398) [tool-verified: `provisa/api/rest/cypher_router.py:689`]

**回應：** `application/json`，包含 `node_labels`（各自帶有 `pk`/`pk_columns`）與 `relationship_types`。

每種關係類型還帶有 `junction_table_name` 與 `properties`（REQ-1586）。在由聯結資料表支撐的邊上，前者給出它所穿過的關聯資料表名稱，後者列出該資料表中可作為 `r.attr` 讀取並可在 `WHERE` 中過濾的欄位；在由外部索引鍵支撐的邊上，該名稱為 `null`，屬性清單為空——用戶端正是據此區分兩者。聯結資料表本身永遠不是節點標籤——它就是邊，因此在圖用戶端中沒有對應的標籤膠囊，在 `node_labels` 中也沒有對應資料列。[tool-verified: `provisa/api/rest/cypher_router.py:797-805`, `provisa/cypher/label_map.py:378-397`]

---

### `GET /data/domains`

傳回提出請求的角色可存取的網域 ID。[tool-verified: `provisa/api/data/sdl.py:116`]

**標頭：** `X-Role: <role_id>`（必要）

**回應：** `["sales", "support", ...]`

---

### `GET /data/schema-version`

傳回目前的結構描述版本字串。它結合了每次開機的 nonce 與重建計數器。用戶端據此在伺服器重新啟動後使結構描述快取失效。(REQ-537) [tool-verified: `provisa/api/data/sdl.py:102`]

**回應：** `{"version": "<boot-id>-<counter>"}`

---

### `GET /data/proto/{role_id}`

傳回為某角色自動產生的 `.proto` 檔。[tool-verified: `provisa/api/data/endpoint_dev.py:49`]

**回應：** `text/plain` protobuf 結構描述。

每個已註冊的資料表會產生一個 proto `message`。關聯會產生巢狀 message 欄位。類型對映：`integer → int32`、`bigint → int64`、`varchar → string`、`decimal → double`、`boolean → bool`、`timestamp → google.protobuf.Timestamp`。(REQ-538)

---

### `GET /data/subscribe/{table}`

從資料表接收即時變更通知的 Server-Sent Events 串流。(REQ-219, REQ-258) [tool-verified: `provisa/api/data/subscribe.py:239`]

通知傳遞採用依數據來源類型選擇的可插拔提供者：PostgreSQL 來源使用 `LISTEN/NOTIFY`（透過 asyncpg），MongoDB 來源使用 Change Streams（`collection.watch()`），Kafka 來源使用消費者群組。每個提供者都實作共通的非同步監看介面。無論採用哪個提供者，RLS 篩選與結構描述驗證都會套用。(REQ-258) WebSocket 與 RSS 來源同樣受支援。(REQ-338, REQ-342)

**標頭 — `X-Provisa-Sink`：** 設為 Kafka 目標（例如 `kafka://broker:9092/topic`）可將變更事件重新導向到 Kafka 接收端，而非 SSE 回應。伺服器會啟動接收端消費者並傳回 `202 Accepted`，而非開啟串流。(REQ-812) [tool-verified: `provisa/api/data/subscription_sse.py:137`]

---

## 管理員 REST 端點

### 組態

#### `GET /admin/config`

以 `application/x-yaml` 下載目前的 `provisa.yaml`，並附帶 `Content-Disposition: attachment` 標頭。(REQ-164) [tool-verified: `provisa/api/admin/settings_router.py:19`]

#### `PUT /admin/config`

上載修訂後的組態 YAML。伺服器會寫入 `.bak` 備份、儲存新檔，並重新載入所有結構描述、數據來源與具體化檢視。(REQ-164) [tool-verified: `provisa/api/admin/settings_router.py:32`]

**請求主體：** 原始 YAML 內容。

**回應：**

```json
{"success": true, "message": "Config uploaded and reloaded"}
```

重新載入失敗時：`{"success": false, "message": "<error>"}`。

#### `GET /admin/config/live`

下載**目前的即時組態** — 即 Provisa 今天會寫出的組態，反映自啟動以來累積的每一項管理員建立的資料表、關聯、網域、角色與 RLS 規則。(REQ-164) [tool-verified: `provisa/api/admin/settings_router.py:67`]

如果變更是透過管理員 API 進行而未後續上載，磁碟上的檔案可能落後於即時狀態。此端點補上這個落差：其輸出正是 `PUT /admin/config` 需要收到、才能讓磁碟檔案與即時狀態一致的內容。

傳回 `application/x-yaml`，並附帶 `Content-Disposition: attachment; filename=provisa.live.yaml`。

#### `GET /admin/config/diff`

傳回組態差異的兩側 — `original`（啟動時的基準）與 `current`（即時狀態） — 兩者以相同方式正規化，因此比較結果只顯示真正的變更，而非重新排序或註解漂移。(REQ-164) [tool-verified: `provisa/api/admin/settings_router.py:82`]

**回應：**

```json
{"original": "<yaml>", "current": "<yaml>"}
```

#### `POST /admin/config/patch`

從基準到所張貼組態產生統一差異修補檔。(REQ-164) [tool-verified: `provisa/api/admin/settings_router.py:93`]

將修訂後的 YAML 作為請求主體送出。回應是一個 `text/x-patch` 檔案（`provisa.config.patch`），可直接由 `git apply` 或 `patch` 使用 — 適合透過 CI/CD 管線提交 UI 驅動的組態變更。

---

### 設定

#### `GET /admin/settings`

以 JSON 傳回目前的平台設定。(REQ-165) [tool-verified: `provisa/api/admin/settings_router.py:50`]

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

在執行階段更新平台設定。所有欄位皆為選填 — 只有主體中出現的索引鍵會被更新。(REQ-165) [tool-verified: `provisa/api/admin/settings_router.py:100`]

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
- `naming`：`domain_prefix`、`convention` — 會寫入組態檔並觸發結構描述重新載入 (REQ-253)
- `relationships`：`auto_track_fk` —— 僅管轄外部索引鍵追蹤。由聯結資料表支撐的關係是在資料表註冊時宣告的，從不被推斷，因此該設定對它不起作用。（REQ-1586）
- `otel`：`endpoint`、`service_name`、`sample_rate`、`support_endpoint`、`support_redact_sql_literals`、`support_redact_attributes`

**回應：**

```json
{"success": true, "updated": ["otel.support_endpoint", "cache.default_ttl"]}
```

---

### AI 模型

#### `GET /admin/ai-models`

傳回作用中組織的 AI 模型指派、向量模型登錄檔與自然語言速率限制。(REQ-464, REQ-1349) [tool-verified: `provisa/api/admin/ai_models_router.py:58`]

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

API 密鑰從不回傳 — `api_keys_set` 只回報每個供應商是否已設定密鑰。變更在下一個請求生效；無需重新啟動。(REQ-1349)

#### `PUT /admin/ai-models`

更新組織的 AI 模型指派、向量模型登錄檔或自然語言速率限制。在下一個請求生效。[tool-verified: `provisa/api/admin/ai_models_router.py:148`]

#### `GET /admin/ai-models/vendors/{vendor}/models`

傳回某供應商目前提供的模型名稱，供模型挑選器使用。(REQ-1395, REQ-1398, REQ-1409) [tool-verified: `provisa/api/admin/ai_models_router.py:89`]

清單使用組織已設定的密鑰 — 或在未設定組織密鑰時使用部署層級的憑證 — 從供應商自身的 list-models API 即時讀取。在此組建發佈之後才推出的模型，供應商提供的當天即可選用。

當供應商未發佈 list-models API（此時請直接輸入模型名稱），或沒有可用密鑰時，傳回 `400`。[tool-verified: `provisa/api/admin/ai_models_router.py:109-128`]

---

### 聯邦引擎

#### `GET /admin/federation-engine`

傳回目前的聯邦引擎選擇、其連線組態，以及完整的可選引擎登錄檔。(REQ-916) [tool-verified: `provisa/api/admin/settings_router.py:730`]

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

持續保存聯邦引擎的選擇。(REQ-916) [tool-verified: `provisa/api/admin/settings_router.py:774`]

**請求主體：**

```json
{"engine": "trino", "federation_engine_url": "http://trino-coordinator:8080"}
```

選擇會寫入平台組態。它在服務下次重新啟動後生效 — 引擎只在開機時選定一次。

---

### 網域原則

#### `POST /admin/domain-policy`

變更作用中組織的網域原則（`use_domains` / `default_domain`）。(REQ-165, REQ-1266, REQ-1349) [tool-verified: `provisa/api/admin/settings_router.py:632`]

這是限定於作用中組織的破壞性操作。每一個已註冊的數據來源、資料表、網域與關聯都會被清除，並依新原則重建。當要把組織從網域命名空間切換為扁平結構（或反之）時使用。

**請求主體：**

```json
{
  "use_domains": true,
  "default_domain": "default"
}
```

`use_domains: null` 會清除組織的覆寫值，改用部署層級的設定。`use_domains: false` 需要 `default_domain`（所有資料表落腳的單一網域名稱）。目錄重建是同步的；回應在結構描述就緒後才傳回。

---

### 可觀測性

#### `GET /admin/traces/recent`

從記憶體內的 span 緩衝區傳回最多 N 個最近完成的 span。(REQ-302) [tool-verified: `provisa/api/admin/settings_router.py:317`]

**查詢參數：** `limit`（預設 50，上限 200）

**回應：** `{"traces": [...]}`

#### `POST /admin/query-engine/reload-catalog`

透過聯邦引擎協調器的 REST API 熱重新載入指名的目錄。會重新連線 Provisa 的內部連線並重新執行 OTel DDL。[tool-verified: `provisa/api/admin/settings_router.py:208`]

**查詢參數：** `catalog`（預設 `"otel"`）

**回應：**

```json
{"success": true, "errors": []}
```

#### `POST /admin/query-engine/restart`

重新啟動聯邦引擎容器（僅限單節點開發環境）。[tool-verified: `provisa/api/admin/settings_router.py:287`]

**查詢參數：** `container`（預設取 `QUERY_ENGINE_CONTAINER` 環境變數，其次為 `"trino"`）

---

### 探索

#### `POST /admin/discover/relationships`

觸發關聯探索。一律從聯邦引擎執行外部索引鍵自省。(REQ-018) 若已設定 `ANTHROPIC_API_KEY`，則執行 LLM 推論。(REQ-167) [tool-verified: `provisa/api/admin/discovery.py:55`]

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

列出待處理的關聯候選項。[tool-verified: `provisa/api/admin/discovery.py:96`]

#### `POST /admin/discover/candidates/{candidate_id}/accept`

接受候選項並將其註冊為關聯。[tool-verified: `provisa/api/admin/discovery.py:103`]

**請求主體（選填）：** `{"name": "custom-relationship-name"}`

#### `POST /admin/discover/candidates/{candidate_id}/reject`

拒絕候選項。[tool-verified: `provisa/api/admin/discovery.py:110`]

**請求主體：** `{"reason": "Not a real join"}`

#### `GET /admin/discover/candidates/rejected/count`

傳回已拒絕候選項的數量。[tool-verified: `provisa/api/admin/discovery.py:118`]

#### `DELETE /admin/discover/candidates/rejected`

刪除所有已拒絕的候選項。[tool-verified: `provisa/api/admin/discovery.py:128`]

---

### 數據來源爬取

#### `POST /admin/sources/crawl`

爬取數據來源以自省其結構描述並註冊資料表。(REQ-012) [tool-verified: `provisa/api/admin/crawl_router.py:36`]

---

### 數據來源資料表搜尋

#### `GET /admin/sources/{source_id}/tables/search`

依名稱搜尋數據來源中可用（尚未註冊）的資料表。[tool-verified: `provisa/api/admin/table_search_router.py:103`]

---

### 資料表剖析

#### `POST /admin/tables/{table_id}/profile`

對已註冊的資料表執行欄位剖析 — 基數、最小／最大值、空值比率。[tool-verified: `provisa/api/admin/table_profile_router.py:28`]

---

### 數據來源描述

#### `POST /admin/source-meta/db-description`

為數據來源的資料表與欄位產生 LLM 輔助的描述。[tool-verified: `provisa/api/admin/source_meta_router.py:48`]

---

### 物件儲存 (REQ-1046, REQ-1048, REQ-1049)

#### `GET /admin/org-storage`

回報作用中組織相對於其平台配額的儲存用量，以及該組織是否已註冊自己的儲存區。[tool-verified: `provisa/api/admin/org_storage_router.py:69`]

當組織已註冊自己的 DSN 時，其具體化資料會存入該處，不再計入配額。DSN 本身從不傳回。

#### `PUT /admin/org-storage`

註冊（或清除）組織自有的具體化儲存區。[tool-verified: `provisa/api/admin/org_storage_router.py:81`]

**請求主體：**

```json
{"storage_url": "s3://my-bucket/provisa?region=us-east-1&access_key=..."}
```

DSN 在被接受之前會先對聯邦引擎驗證 — 無法使用的 DSN 在註冊時就失敗，而不是數小時後在重新整理時才發現。該值靜態加密儲存，GET 從不傳回。

送出 `storage_url: null` 可清除組織自有的儲存區，並將其具體化資料歸還平台儲存區（與配額）。組織執行階段會在同一次呼叫中重建，因此新儲存區立即生效。[tool-verified: `provisa/api/admin/org_storage_router.py:123-138`]

---

### 組織加密 (REQ-1574)

#### `GET /admin/org-encryption`

傳回組織目前的密鑰狀態：指紋、id 與來源。從不傳回密鑰材料。[tool-verified: `provisa/api/admin/org_encryption_router.py:53`]

當組織未設定密鑰時，傳回 `{"configured": false}`。每個組織都以此狀態開始，並繼承部署的密鑰。

#### `PUT /admin/org-encryption`

設定或輪換組織的靜態加密密鑰。[tool-verified: `provisa/api/admin/org_encryption_router.py:68`]

**請求主體：**

```json
{"key_b64": "<32 raw bytes, base64-encoded>"}
```

省略 `key_b64` 可讓 Provisa 產生密鑰 — 這是最安全的做法，因為密鑰不會出現在剪貼簿或請求記錄中。提供 `key_b64` 則是自備密鑰。

輪換會在密鑰環中加入新的作用中項目並保留舊的，因此以先前密鑰寫入的數據仍可讀取。輪換不等於重新加密。沒有刪除端點：淘汰最後一把密鑰會使所有已包裝的酬載無法讀取。[tool-verified: `provisa/api/admin/org_encryption_router.py:75`]

即時密鑰環會在同一次呼叫中重新繫結，因此下一次加密寫入立即使用新密鑰。

---

### Hasura / DDN 匯入 (REQ-1483)

#### `POST /admin/import/hasura/preview`

將 Hasura v2 或 DDN 專案封存檔轉換為建議的 Provisa 組態，不寫入任何內容。[tool-verified: `provisa/api/admin/import_router.py`]

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

將先前預覽的組態載入作用中組織。[tool-verified: `provisa/api/admin/import_router.py`]

**請求主體：**

```json
{"config_yaml": "<yaml string>"}
```

使用與 `PUT /admin/config` 相同的熱重新載入路徑。組織的目錄、結構描述與連線池會在回應傳回前重建。

---

### Apache Ossie 互通 (REQ-1316, REQ-1321)

#### `GET /admin/ossie`

將組織受治理的模型匯出為 Apache Ossie（孵化中）YAML 文件。(REQ-1321) [tool-verified: `provisa/api/admin/ossie_router.py`]

該文件在每次請求時由即時狀態衍生 — 從不快取 — 因此不可能過時。資料表成為 `dataset` 物件，欄位成為 `field` 物件，關聯對映為 Ossie 的 `relationship` 物件。

傳回 `text/yaml`，並附帶 `Content-Disposition: attachment; filename=provisa-ossie.yaml`。

#### `POST /admin/ossie/import`

剖析 Ossie YAML 或 JSON 文件，並傳回建議的資料表與關聯註冊項。(REQ-1316) [tool-verified: `provisa/api/admin/ossie_router.py`]

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

### 動作（函式與 Webhook）

所有端點都在 `/admin/actions` 前綴之下。(REQ-205) [tool-verified: `provisa/api/admin/actions_router.py:24`]

每一次呼叫 — 來自 GraphQL、SQL、Cypher、Bolt、Arrow Flight、MCP `run_sql` 以及 Provisa gRPC — 都會經由單一受治理的執行器路由，統一強制執行 `writable_by` 與治理。(REQ-1156) [tool-verified: `provisa/api/data/action_exec.py`] 各通訊協定的呼叫語法請見 [docs/integrations.md](integrations.md#invoking-commands-across-protocols)。

#### `GET /admin/actions`

傳回所有受追蹤的資料庫函式與 webhook。(REQ-242) [tool-verified: `provisa/api/admin/actions_router.py:104`]

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

每個 webhook 物件都帶有 `approved` 布林值。當數據管家執行其建立請求後，webhook 即獲核准 (REQ-209)；組態中宣告的 webhook 自動核准。未核准的 webhook 雖已註冊，但不會在任何介面上公開。[tool-verified: `provisa/api/admin/actions_router.py:124-131`]

#### `POST /admin/actions/functions`

註冊受追蹤的函式（命令）。(REQ-205) [tool-verified: `provisa/api/admin/actions_router.py:117`]

**主要欄位：**

| 欄位 | 必要 | 描述 |
| --- | --- | --- |
| `name` | 是 | 唯一的命令名稱 |
| `kind` | 是 | `"query"` → GraphQL Query 欄位；`"mutation"` → Mutation 欄位 |
| `implKind` | 否 | 命令的執行方式 — 見下表（預設 `source_procedure`） |
| `binding` | 否 | `implKind` 專屬的連線細節（JSON 物件） |
| `returnSchema` | 否 | JSON Schema `{type:"array", items:{type:"object", properties:{...}}}` — 使命令在每個介面上都傳回集合 |
| `arguments` | 否 | `[{name, type}]` 引數定義；位置順序對 SQL 與 Bolt 呼叫者有意義 |
| `visibleTo` | 否 | 可呼叫該命令的角色 ID |
| `writableBy` | 否 | 獲准以變更操作方式呼叫它的角色 ID |
| `domainId` | 否 | 用於 GraphQL 放置與存取控制的網域 |

**`implKind` 值：**

| `implKind` | 執行內容 | `binding` 欄位 |
| --- | --- | --- |
| `source_procedure` | 已註冊數據來源上的預存程序（預設） | `sourceId`、`schemaName`、`functionName` |
| `script` | 伺服器端指令碼 | `script` |
| `http` | 對外 HTTP 呼叫 | `url`、`method` |
| `grpc` | 對外部伺服器的 gRPC 呼叫 | `target`、`method` |
| `python` | 由 Provisa 託管的 Python 可呼叫項 (REQ-885) | `callable`（例如 `"demo.py_functions:random_dataset"`） |

示範命令 `random_python_set`（`implKind: python`）與 `random_grpc_set`（`implKind: grpc`）展示了帶 `returnSchema` 的集合傳回命令實例；兩者都在 `config/provisa-install.yaml` 中。[tool-verified: `config/provisa-install.yaml:809-856`]

#### `PUT /admin/actions/functions/{name}`

依名稱更新受追蹤的函式。[tool-verified: `provisa/api/admin/actions_router.py:182`]

#### `DELETE /admin/actions/functions/{name}`

依名稱刪除受追蹤的函式。[tool-verified: `provisa/api/admin/actions_router.py:233`]

#### `POST /admin/actions/webhooks`

註冊受追蹤的 webhook。(REQ-209) 註冊或更新 webhook 會排入一個數據管家核准請求 — webhook 只有在管家核准後才會在所有介面上生效。組態中宣告的 webhook 自動核准。**請求主體欄位：** `name`、`url`、`method`、`timeoutMs`、`returns`、`inlineReturnType`、`arguments`、`visibleTo`、`domainId`、`description`、`kind`。[tool-verified: `provisa/api/admin/actions_router.py:132`, `provisa/api/admin/actions_router.py:325-331`]

#### `PUT /admin/actions/webhooks/{name}`

依名稱更新受追蹤的 webhook。任何編輯都會將核准狀態重設為待處理，直到再次獲得核准。[tool-verified: `provisa/api/admin/actions_router.py:306`]

#### `DELETE /admin/actions/webhooks/{name}`

依名稱刪除受追蹤的 webhook。[tool-verified: `provisa/api/admin/actions_router.py:355`]

#### `POST /admin/actions/test`

依名稱測試動作（函式或 webhook）。(REQ-245) [tool-verified: `provisa/api/admin/actions_router.py:384`]

---

### 角色

所有端點都在 `/admin/roles` 前綴之下。[tool-verified: `provisa/api/admin/roles_router.py:18`]

| 方法 | 路徑 | 描述 |
| --- | --- | --- |
| `GET` | `/admin/roles/` | 列出所有角色 |
| `POST` | `/admin/roles/` | 建立角色 |
| `PUT` | `/admin/roles/{role_id}` | 更新角色 |
| `DELETE` | `/admin/roles/{role_id}` | 刪除角色 |

[tool-verified: `provisa/api/admin/roles_router.py`]

---

### 使用者

所有端點都在 `/admin/users` 前綴之下。[tool-verified: `provisa/api/admin/local_users_router.py:21`]

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

### 組織

所有端點都在 `/admin/orgs` 之下。[tool-verified: `provisa/api/admin/orgs_router.py:18`]

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

### 邀請

所有端點都在 `/admin/invites` 之下。[tool-verified: `provisa/api/admin/invites_router.py:18`]

| 方法 | 路徑 | 描述 |
| --- | --- | --- |
| `POST` | `/admin/invites/` | 建立邀請 |
| `GET` | `/admin/invites/` | 列出待處理的邀請 |
| `DELETE` | `/admin/invites/{token}` | 撤銷邀請 |

---

### 管理員 GraphQL

#### `POST /admin/graphql`

所有管理員操作的 Strawberry GraphQL 端點：數據來源與資料表 CRUD、關聯管理、網域組態、RLS 規則、快取控制、命名慣例、排程工作管理，以及查詢編譯。(REQ-164) [tool-verified: `provisa/api/app.py:2171`]

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

[tool-verified: `provisa/api/admin/schema.py`, `provisa/api/admin/actions_router.py`]

---

### 設定精靈

#### `GET /setup/status`

傳回首次執行的設定狀態。一律免驗證。(REQ-539) [tool-verified: `provisa/api/setup_router.py:100`]

#### `POST /setup/`

完成首次執行設定。[tool-verified: `provisa/api/setup_router.py:142`]

---

## 健康檢查

#### `GET /health` 或 `HEAD /health`

傳回 `{"status": "ok"}`。一律免驗證。(REQ-539) [tool-verified: `provisa/api/app.py:2258`]

---

## 錯誤回應

| 狀態 | 意義 |
| --- | --- |
| 400 | 無效查詢、驗證錯誤或 SQL 剖析錯誤 |
| 401 | 缺少或無效的驗證權杖 |
| 403 | 能力不足；治理違規 |
| 404 | 找不到角色、資源或組態檔 |
| 422 | 缺少必要標頭（例如 `X-Role`） |
| 503 | 資料庫或數據來源未連線；相依項無法使用 |
| 504 | 請求逾時 |

`POST /data/sql` 上的治理違規會傳回 HTTP 403 與結構化主體：(REQ-002) [tool-verified: `provisa/api/data/endpoint_dev.py:184-190`]

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

## Arrow Flight 端點

連接埠 `8815`。透過 gRPC 的原生 Arrow 資料行式傳輸。(REQ-143, REQ-045) [tool-verified: `provisa/api/flight/server.py`]

查詢與目錄探索都可在同一條連線上使用。完整的治理管線（RLS、遮罩、抽樣）會套用到每一個查詢。(REQ-130, REQ-143)

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

當 Zaychik Flight SQL 代理可用時（連接埠 8480），記錄批次會端對端串流，無需完整具體化。(REQ-144) 若 Zaychik 無法使用，則退回透過聯邦查詢層具體化。(REQ-146)

---

## Protobuf gRPC 端點

連接埠 `50051`（以 `GRPC_PORT` 環境變數或 `server.grpc_port` 組態覆寫）。(REQ-529) [tool-verified: `provisa/grpc/server.py`, `provisa/api/app.py`]

在 `x-provisa-role` gRPC 中繼資料索引鍵中傳遞角色。若缺少，伺服器會以 `UNAUTHENTICATED` 中止。[tool-verified: `provisa/grpc/server.py`]

從 `GET /data/proto/{role_id}` 下載角色專屬的 proto。只有該角色可見的資料表與欄位會出現。(REQ-039)

```proto
service ProvisaService {
  rpc QueryOrders (QueryOrdersRequest) returns (stream Orders);
  rpc InsertOrders (InsertOrdersRequest) returns (InsertOrdersResponse);
}
```

每個資料表會產生一個 `Query{TypeName}` 串流 RPC。`Insert{TypeName}` RPC 為結構描述對稱性而存在，但會以 `UNIMPLEMENTED` 中止。[tool-verified: `provisa/grpc/server.py`]

已啟用 `grpc_reflection.v1alpha`，可在沒有預先編譯 proto 的情況下探索服務。(REQ-529) [tool-verified: `provisa/grpc/reflection.py`]

```bash
grpcurl -plaintext localhost:50051 list
grpcurl -plaintext -H 'x-provisa-role: analyst' \
  -d '{}' localhost:50051 ProvisaService/QueryOrders
```

gRPC 伺服器只在啟動時能編譯出有效 proto 的情況下才會啟動。若結構描述建置失敗，gRPC 伺服器不會啟動。(REQ-529)

#### 彙總與分組 RPC (REQ-1359, REQ-1361, REQ-1405)

當資料表設定了 `enable_aggregates` 時，產生的 proto 會在 `Query{TypeName}` 之外額外包含兩個 RPC：

- **`Query{TypeName}Aggregate`** — 傳回該資料表的彙總純量（`count`；每個數值欄位的 `sum`、`avg`、`stddev`、`variance`；每個可比較欄位的 `min`、`max`）
- **`Query{TypeName}GroupBy`** — 每個分組索引鍵傳回一列，帶有彙總子欄位，並可選擇性地在 `nodes` 欄位中帶有基礎資料表純量與聯結維度資料列

兩者都經由與 GraphQL 的 `{field}_aggregate` 及 `{field}_group_by` 根欄位相同的編譯器彙總管線 — 沒有另一套獨立的彙總實作。(REQ-1359) [tool-verified: `provisa/grpc/query_ir.py:133-196`]

**`funcs` 欄位 (REQ-1361)。** 請求訊息接受一個 `funcs` 重複字串欄位。有效值為 `count`、`sum`、`avg`、`stddev`、`variance`、`min` 與 `max`。省略 `funcs` 時，會要求結構描述為該資料表公開的每一個函式。設定後，只有指名的函式會出現。若指名的函式都不適用於該資料表的欄位類型，查詢會退回 `count`。[tool-verified: `provisa/grpc/query_ir.py:66`, `provisa/grpc/query_ir.py:75-97`]

**`include_nodes` 與 `include` 欄位 (REQ-1405)。** `Query{TypeName}GroupBy` 請求可設定 `include_nodes: true`，在每一列的 `nodes` 欄位中納入基礎資料表的純量欄位。`include` 重複字串欄位指名多對一關聯欄位，其純量欄位同樣會巢狀放入 `nodes`。此行為與 JSON:API 的 `?includeNodes=` / `?include=` 一致。[tool-verified: `provisa/grpc/query_ir.py:168-195`]

---

## JDBC 驅動程式

Provisa JDBC 驅動程式（`provisa-jdbc-0.1.0.jar`）將語意目錄公開給 BI 工具（Tableau、PowerBI、DBeaver）。(REQ-126)

**連線 URL：** `jdbc:provisa://host:port` (REQ-131)

網域對映到 JDBC 結構描述。(REQ-127) 資料表使用其註冊的別名。欄位使用別名，並以 `REMARKS` 呈現描述。(REQ-128) 標準中繼資料方法（`getPrimaryKeys`、`getImportedKeys`、`getExportedKeys`）會將語意關聯以主索引鍵／外部索引鍵中繼資料公開。

**SQL 支援：** `SELECT * FROM <alias> [WHERE col = 'value']`。(REQ-129)

驅動程式預設要求 Arrow IPC 重新導向。結果透過 `ArrowStreamReader` 逐批串流，記憶體中最多只保留一個記錄批次。(REQ-293)

---

## `orderBy` 引數格式

`order_by` 引數使用 `{column: direction}` 物件，方向列舉有 6 個值：(REQ-200)

```json
{
  "query": "{ orders(order_by: [{created_at: desc_nulls_last}]) { id created_at } }",
  "role": "admin"
}
```

支援的方向：`asc`、`desc`、`asc_nulls_first`、`asc_nulls_last`、`desc_nulls_first`、`desc_nulls_last`。(REQ-201)

---

## 訂閱

SSE 訂閱位於 `GET /data/subscribe/{table}`。(REQ-219, REQ-258) 通知傳遞採用依數據來源類型選擇的可插拔提供者：PostgreSQL 來源使用 `LISTEN/NOTIFY`，MongoDB 來源使用 Change Streams，Kafka 來源使用消費者群組。無論採用哪個提供者，RLS 篩選與結構描述驗證都會套用。WebSocket 與 RSS 來源也透過同一端點支援。(REQ-338, REQ-342) [tool-verified: `provisa/api/data/subscribe.py:239`, `provisa/subscriptions/registry.py`, `provisa/api/app.py` `_rebuild_schemas`]

---

## 業務詞彙表 (REQ-1387)

業務詞彙表把實體欄位名稱 — 即它們在來源資料庫中的樣子 — 對映到共用的人類詞彙。語意層中註冊的每個欄位都會自動取得一個術語。填充詞彙表無需手動輸入；策展人只在系統推導出的內容之上補充定義、關聯與專家。

### 術語如何推導

當 Provisa 註冊或更新資料表的欄位時，`normalize_term`（`provisa/core/glossary.py`）會在每個欄位名稱上執行並產生一個標準片語。[tool-verified: `provisa/core/repositories/glossary.py:sync_table_refs`]

正規化依序套用五條規則：

1. 在 camelCase 邊界與分隔字元（`_`、`-`、`.`、`/`、空白）處切分。
2. 將結果轉為小寫。
3. 展開一份固定的縮寫對照表（例如 `cust` → `customer`、`amt` → `amount`、`dt` → `date`、`id` → `identifier`、`key` → `identifier`、`guid` → `identifier`）。
4. 移除結尾的**代理權杖**（`identifier`、`code`、`index` 或 `reference`）— 以索引鍵或代碼命名的欄位，是透過一個替代值指向底層概念，因此術語應該就是那個概念本身。最後剩下的權杖永遠不會被移除。
5. 以資料表的概念修飾**過於一般化的片語**。當完整的正規化片語只是一個裸屬性詞（`name`、`identifier`、`date`、`location`、`message`、`first name`、`last name` 之類）時，術語會變成 `<table concept> <phrase>` — `employees.first_name` → `employee first name`、`orders.id` → `order identifier`。若讓一個共用的 `name` 術語橫跨互不相關的資料表，會把不同的意義合併在一起；修飾則把每個欄位連結到其所屬概念。資料表概念即該資料表的業務名稱，並正規化為單數主名詞（`order_lines` → `order line`）。

原生篩選的虛擬欄位（以 `_nf_` 為前綴，或任何帶有 `native_filter_type` 的欄位）屬於查詢參數機制，而非業務欄位，不會推導出任何術語。

由於 `id`、`key`、`pk` 與 `sk` 在代理檢查之前都會展開為 `identifier`，三個實體上不同的欄位名稱會落在完全相同的術語上：

| 實體名稱 | 正規化之後 |
| --- | --- |
| `cust_id` | `customer` |
| `customerId` | `customer` |
| `CUSTOMER_KEY` | `customer` |
| `txn_amt` | `transaction amount` |

前三者收斂為一個術語。`transaction amount` 保留兩個權杖，因為 `amount` 不是代理。裸 `id` 欄位 — 前面沒有其他權杖 — 無法被移除；它正規化為 `identifier`，因此術語不會為空。[tool-verified: `provisa/core/glossary.py:normalize_term`]

### 生命週期

術語是**從語意層成員資格推導而來**，而非由使用者按需建立。資料表儲存庫是唯一的寫入路徑：`sync_table_refs` 在每一次欄位集 upsert 內執行，`sweep_refless_terms` 則在任何刪除路徑之後執行。[tool-verified: `provisa/core/repositories/glossary.py`]

**新增欄位時：** Provisa 依名稱查找正規化後的術語。若已存在，該欄位會取得一個指向它的參照（若該術語先前已淘汰，則會復原 — `deprecated` 會設回 `False`）。若尚無術語，則建立一個。

**欄位離開時**（結構描述變更或資料表移除）：其參照會被刪除，術語則依「移除或淘汰」規則結算。已扎根、且不再有任何參照的術語會被整個移除 — 連同其邊與專家指派 — 除非移除它會讓某個抽象術語與所有已扎根術語失去連結（在術語圖中無路徑可達）。在那種情況下，術語會被**淘汰**（標記 `deprecated=True`）而非刪除，讓抽象術語的圖形錨點得以保留。

抽象術語永不自動移除；它們存在於實體生命週期之外，只能透過管理員 API 明確刪除。

**復原：** 若已淘汰術語的正規化名稱再次出現（欄位重新註冊），該術語會取消標記，其參照亦重新累積。

### 策展端點

所有端點都在 `/admin/glossary` 之下。它們需要 `org_admin` 存取權與已設定的組織。每一次變更操作都會觸發中繼資料發佈。[tool-verified: `provisa/api/admin/glossary_router.py`]

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

有效的 `rel_type` 值：`KIND_OF`、`RELATED_TO`、`PART_OF`、`SYNONYM_OF`。[tool-verified: `provisa/core/glossary.py:TERM_EDGE_TYPES`]

**`POST /admin/glossary/terms/{term_id}/experts` 主體：**

```json
{"user_id": "alice@example.com", "kind": "author"}
```

有效的 `kind` 值：`expert`、`author`。[tool-verified: `provisa/core/repositories/glossary.py:add_expert`]

**`POST /admin/glossary/refs/move` 主體：**

```json
{"table_id": 7, "column_name": "cust_id", "to_term_id": 12}
```

移動參照會使失去參照的術語依「移除或淘汰」規則結算。用它來整併正規化未能合併的兩個術語 — 例如某個數據來源使用了落在展開對照表之外的非標準縮寫。

刪除已扎根的術語（帶有實體參照者）會傳回 `400 glossary.invalid`。請先移除或移動所有參照。

**`PATCH /admin/glossary/terms/{term_id}` — `export_excluded` 欄位：**

```json
{"export_excluded": true}
```

將 `export_excluded` 設為 `true`，可讓該術語不出現在任何中繼資料匯出快照中，無論其實體參照或抽象狀態為何。設回 `false` 則在下次發佈時把術語恢復到快照中。策展數據（定義、邊、專家）不受影響。[tool-verified: `provisa/core/repositories/glossary.py:set_export_excluded`, `provisa/api/admin/glossary_router.py:update_term`]

### AI 輔助策展

組織所設定的 AI 模型可以在一次操作中，為整個詞彙表草擬定義並建議關聯邊。這兩項批次動作都需要 `org_admin` 存取權與已設定的組織。

**`POST /admin/glossary/definitions/generate`**

逐一走訪詞彙表中的每個術語，略過已有定義者，並呼叫組織的 AI 模型為其餘術語各草擬一則定義。草稿會立即保存 — 與逐術語草擬端點（`POST /admin/glossary/terms/{term_id}/definition/generate`）不同，這裡沒有編輯步驟。人工撰寫的定義永不被覆寫：在任何模型呼叫之前的防護是 `if summary["definition"]: continue`。整批只發出一次發佈通知。[tool-verified: `provisa/api/admin/glossary_router.py:generate_all_definitions`]

回應：

```json
{"generated": 12}
```

`generated` 是取得新定義的術語數量。當每個術語都已有定義時為零。

**`POST /admin/glossary/relationships/generate`**

將完整術語清單連同一段提示詞送給組織的 AI 模型，提示詞指定十種允許的邊類型（`KIND_OF`、`PART_OF`、`SYNONYM_OF`、`RELATED_TO`、`VALID_VALUE_OF`、`DERIVED_FROM`、`REPLACES`、`PREFERRED_TERM_FOR`、`TRANSLATION_OF`、`ANTONYM_OF`），並要求只提出有把握的建議。模型以 JSON 陣列回應；每一項在任何寫入之前都會經過驗證：未知的術語名稱、自我指向的邊，以及封閉列舉之外的邊類型都會被靜默丟棄。有效的建議會以冪等方式 upsert — 重新執行該動作不會產生重複的邊。整批只發出一次發佈通知。當詞彙表中未淘汰的術語少於兩個時，端點會立即傳回 `{"added": 0}`。[tool-verified: `provisa/api/admin/glossary_router.py:generate_relationships`]

回應：

```json
{"added": 5}
```

`added` 是已寫入的邊數量。已存在的邊仍會計入 — upsert 成功，只是邊的數據沒有改變。

### MCP `search_terms` 工具

```
search_terms(query, role=None, limit=25)
```

以不區分大小寫的子字串比對搜尋術語名稱與定義，最多傳回 `limit` 筆結果。每筆結果都是完整的術語明細：`name`、`definition`、`is_abstract`、`deprecated`、實體參照（帶有 `source_id`、`schema_name`、`table_name`、`column_name`）、具型別的邊，以及專家指派。[tool-verified: `provisa/api/mcp/server.py:236-244`, `provisa/core/repositories/glossary.py:search_terms`]

在撰寫 SQL 之前使用 `search_terms`，可依名稱找出代表某個概念的每一個實體欄位。例如搜尋 `"order date"` 會傳回該術語，以及每個已註冊資料表中所有的 `order_dt`、`orderDate`、`ORDER_DATE` 欄位。

### 中繼資料匯出

詞彙表術語圖包含在 `build_snapshot` 建置的每個 `MetadataSnapshot` 中。[tool-verified: `provisa/api/metadata_export/builder.py:_glossary_assets`]

匯出套用與快照其餘部分相同的篩選：

- 標記 `export_excluded` 的術語會被整個保留不匯出 — 無論其實體參照、抽象狀態，或組織的目錄是否已設定。[tool-verified: `provisa/api/metadata_export/builder.py:_glossary_assets`]
- 已扎根的術語只在其至少一個實體參照所屬的欄位同時通過**數據產品**篩選（資料表的 `data_product` 旗標必須為 `true`）與**技術性**欄位篩選（標記為 `technical` 的欄位不匯出）時才會發佈。
- 若某個已扎根術語的所有參照都被上述篩選攔下，該術語也一併不匯出。
- 抽象術語無條件發佈 — 它們是使用者詞彙，不繫結於實體欄位。
- 兩個術語之間的邊，只有在兩端術語都發佈時才會發佈。

每個廠商配接器都原生發佈術語圖，寫入一個由它冪等建立、由 Provisa 擁有的詞彙表容器 — 絕不寫入既有的目錄詞彙表：

| 提供者 | 容器 | 術語 | 關聯 | 淘汰 |
| --- | --- | --- | --- | --- |
| Apache Atlas | 「Provisa Glossary」（glossary API） | 詞彙表術語，定義寫在 `longDescription` | KIND_OF → `isA`、SYNONYM_OF → `synonyms`、RELATED_TO/PART_OF → `seeAlso` | `[DEPRECATED]` shortDescription 標記 |
| Atlan | 依穩定 qualifiedName 的 Provisa 詞彙表 | `longDescription`（絕不使用人工編輯的 `userDescription`） | 與 Atlas 相同的對映 | `certificateStatus = DEPRECATED` |
| DataHub | `urn:li:glossaryNode:provisa.<org>` | 每個術語一個 `glossaryTermInfo` 面向 | KIND_OF → Inherits、PART_OF → Contains（反向）、RELATED_TO/SYNONYM_OF → 相關術語 | deprecation 面向；重新命名遵循 URN 承繼 |
| OpenMetadata | 透過 `/v1/glossaries` 的 Provisa 詞彙表 | 以 fqn 為索引鍵的 PUT，重新命名則以已儲存的 UUID 進行 PATCH 重新繫結 | KIND_OF → 原生父階層、SYNONYM_OF → `synonyms`、其餘 → `relatedTerms` | `entityStatus` |
| Collibra | 詞彙表類型的網域「Provisa Glossary」 | 透過 Import API 的 Business Term 資產 | 原生 Business Term 關聯類型 | 資產狀態 |

繫結的依據是所有權，而非名稱：每個已發佈術語的廠商 id 都會擷取到 `catalog_bindings` 中該術語的 URN（`provisa://<org>/terms/<name>`）之下，而 Provisa 只在持有該繫結時（或該項目位於它自己建立、由 Provisa 擁有的容器內）才會修改或刪除廠商端的詞彙表項目。沒有 Provisa 繫結的詞彙表項目源自外部系統，永不被觸碰；更新採讀取合併，因此數據管家在 Provisa 自有術語上新增的欄位得以保留；當術語離開快照時，不會刪除任何東西。管家的術語對資產指派仍由外部擁有 — 沒有任何配接器寫入術語對資產的指派（由 Provisa 撰寫的指派發佈是明確的後續工作）。在 Collibra 上尤其如此：在 Import API 的 REPLACE 語意下，安全性建立在包含關係上 — 酬載只提及 Provisa 詞彙表網域內的資產，關聯實例也只存在於 Provisa 術語之間，因此管家的詞彙表及其關聯永遠不會被觸及。[tool-verified: `provisa/api/metadata_export/atlan.py`, `provisa/api/metadata_export/datahub.py`, `provisa/api/metadata_export/atlas.py`, `provisa/api/metadata_export/openmetadata.py`]
