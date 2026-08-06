# API 參考

## 概觀

Provisa 於兩個前綴下公開 REST 端點：`/data` 供查詢執行及結構描述內省使用，以及 `/admin` 供設定管理使用。(REQ-043) 大多數數據端點均需要角色識別碼。管理設定作業使用位於 `/admin/graphql` 的 Strawberry GraphQL API。(REQ-164)

---

## 驗證

當 `provisa.yaml` 中設定了 `auth.provider` 時，除 `/health` 及 `/setup/status` 外的所有端點均需要 `Authorization: Bearer <token>` 標頭。(REQ-120) [tool-verified: `provisa/api/app.py`, `provisa/auth/wiring.py`]

若未設定驗證，伺服器會以開發模式執行。任何要求均被視為 `anonymous` 身分，該身分會對應至所有已設定的角色，並具備萬用字元領域存取權。(REQ-535)

當設定了 `provider: basic` 時，**登入（`POST /auth/login`）**由目前啟用的驗證提供者提供。(REQ-124) 憑證格式及回應內容視提供者而定。

**身分內省：**

```http
GET /auth/me
```

回傳已驗證使用者的 id、電郵、顯示名稱、組織成員資格及角色指派。於開發模式下會回傳 `dev_mode: true` 及所有角色 ID 的列表。[tool-verified: `provisa/api/auth_router.py`]

```http
GET /auth/provider-type
```

回傳 `{"provider": "<name>"}`，或於驗證未設定時回傳 `{"provider": null}`。[tool-verified: `provisa/api/auth_router.py`]

---

## 數據端點

### `POST /data/graphql`

執行一則 GraphQL 查詢或變異。(REQ-043) [tool-verified: `provisa/api/data/endpoint.py:151`]

**要求本文：**

```json
{
  "query": "{ orders(where: {region: {eq: \"us\"}}) { id amount } }",
  "variables": {},
  "role": "admin",
  "extensions": {}
}
```

`role` 欄位僅於開發模式（無驗證）下使用。當驗證已啟用時，會使用已驗證使用者的角色，本文中的 `role` 會被忽略。

`extensions` 欄位支援自動保存查詢（Automatic Persisted Query，APQ）通訊協定：(REQ-288)

```json
{
  "extensions": {"persistedQuery": {"sha256Hash": "<sha256-of-query>"}}
}
```

**標頭：**

- `X-Provisa-Role`——覆寫角色（開發模式）
- `Accept`——回應格式（見內容協商）
- `Authorization`——當驗證已啟用時為 `Bearer <token>`
- `X-Provisa-Redirect-Format`——S3 重新導向輸出的 MIME 型別 (REQ-137)
- `X-Provisa-Redirect-Threshold`——觸發重新導向的列數門檻 (REQ-137)
- `X-Provisa-Redirect`——設為 `true` 以無條件強制重新導向 (REQ-029)

**回應（JSON 內嵌）：**

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

**回應（多根欄位、內嵌與重新導向混合）：**

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

多根欄位查詢會獨立執行每個根欄位。低於重新導向門檻的欄位會以內嵌方式回傳；高於門檻的欄位則會重新導向。`redirects`（複數）鍵會將欄位名稱對應至重新導向資訊。(REQ-029) [tool-verified: `provisa/api/data/endpoint.py`]

**快取標頭：**

- `X-Provisa-Cache: HIT|MISS` (REQ-536)
- `X-Provisa-Cache-Age: <seconds>`（於 HIT 時） (REQ-536)

**必要功能：**所有要求（包括內省）均需要 `QUERY_DEVELOPMENT`。[tool-verified: `provisa/api/data/endpoint.py:186-283`]

---

### 內容協商

| Accept 標頭 | 格式 |
| --- | --- |
| `application/json` | JSON（預設） |
| `application/x-ndjson` | 以換行分隔的 JSON |
| `text/csv` | CSV |
| `application/vnd.apache.parquet` | Parquet |
| `application/vnd.apache.arrow.stream` | Arrow IPC |

(REQ-047、REQ-048、REQ-049、REQ-050) [tool-verified: `provisa/api/data/endpoint.py:84-90`]

---

### 重新導向

超過所設定列數門檻的結果（或當 `X-Provisa-Redirect: true` 時）會寫入 S3，並回傳一個預先簽署的 URL。(REQ-029、REQ-044)

| 重新導向格式 | 寫入者 | 記憶體 |
| --- | --- | --- |
| `application/vnd.apache.parquet` | 聯邦 CTAS | 無——數據永不經過 Provisa |
| `application/x-orc` | 聯邦 CTAS | 無——數據永不經過 Provisa |
| `application/json` | Provisa | 受記憶體限制 |
| `application/x-ndjson` | Provisa | 受記憶體限制 |
| `text/csv` | Provisa | 受記憶體限制 |
| `application/vnd.apache.arrow.stream` | Provisa | 受記憶體限制 |

對於大型分析匯出，請使用 Parquet 或 ORC 重新導向。聯邦引擎會平行直接寫入 S3——不會有任何數據經過 Provisa。(REQ-138)

```yaml
X-Provisa-Redirect-Format: application/vnd.apache.parquet
X-Provisa-Redirect-Threshold: 1000
```

---

### `POST /data/sql`

透過第二階段治理管線執行原始 SQL。(REQ-267) [tool-verified: `provisa/api/data/endpoint_dev.py:62`]

**要求本文：**

```json
{
  "sql": "SELECT id, amount FROM orders WHERE region = 'us'",
  "role": "admin"
}
```

**必要功能：**`QUERY_DEVELOPMENT`。

`POST /data/sql` 上的治理違規會回傳 HTTP 403。(REQ-002、REQ-266)

**回應：**格式與 `/data/graphql` 相同（預設為 JSON 列，並透過 `Accept` 進行內容協商）。

---

### `POST /data/query`

統一查詢端點。接受 GraphQL、SQL 或 Cypher——語法會自動偵測。(REQ-267) [tool-verified: `provisa/api/data/endpoint_dev.py:509`]

Cypher 查詢亦可提交至僅供 Cypher 使用的 `POST /query/cypher` 端點。(REQ-345)

**要求本文：**

```json
{
  "query": "{ orders { id } }",
  "params": {},
  "variables": {},
  "role": "admin"
}
```

GraphQL 回傳 `{"data": ...}`；SQL 及 Cypher 回傳 `{"columns": [...], "rows": [...]}`。

---

### `GET /data/rest/{domain_id}/{table_name}`

為每個已註冊資料表自動產生的純 REST 端點。查詢字串會對應至 GraphQL 引數，該要求會經由與 GraphQL 相同的管線（行級安全、遮罩、路由）編譯及執行。(REQ-256) [tool-verified: `provisa/api/rest/generator.py:153`]

**查詢參數：**

- `limit`——列數上限（≥ 1）
- `offset`——略過列數（≥ 0）
- `fields`——以逗號分隔的欄位名稱（預設為所有純量欄位）
- `filter`——`{"field", "comparator", "value"}` 篩選物件的 JSON 陣列
- `orderBy`——`{"field", "direction"}` 排序物件的 JSON 陣列

需要已驗證的角色；未經驗證的要求會回傳 `401`。這些路由的 OpenAPI 規格於 `GET /data/rest/openapi.json` 提供，並於 `GET /data/rest/docs` 提供 Swagger UI。

---

### `GET /data/jsonapi/{domain_id}/{table_name}`

為每個已註冊資料表自動產生、符合 [JSON:API](https://jsonapi.org) 規範的端點。與 GraphQL 相同的行級安全、遮罩及路由。(REQ-257) [tool-verified: `provisa/api/jsonapi/generator.py:284`]

**`Accept` 標頭：**必須包含 `application/vnd.api+json`（JSON:API 媒體型別），否則要求會回傳 `406`。

**查詢參數：**

- `fields[<type>]`——稀疏欄位集，例如 `?fields[orders]=amount`
- `filter[<col>]` / `filter[<col>][<op>]`——例如 `?filter[region]=US`、`?filter[amount][gt]=100`
- `sort`——以逗號分隔，`-` 前綴表示遞減，例如 `?sort=-created_at,amount`
- `page[number]` / `page[size]`——分頁

回應為帶有 `type`/`id`/`attributes` 的資源物件。錯誤依循 JSON:API 錯誤物件格式。

---

### `POST /query/nl`

提交一則自然語言問題。此服務會啟動一項非同步工作，並立即回傳附帶 `job_id` 的 `202 Accepted`。需要在 `ai_models` 設定區段下設定 LLM 提供者。(REQ-354) [tool-verified: `provisa/api/rest/nl_router.py:50`]

**要求本文：**

```json
{"q": "How many orders were placed last month?", "role": "admin"}
```

回傳 `{"job_id": "<id>"}`。超出每角色的自然語言速率限制時，會回傳附帶 `Retry-After` 標頭的 `429`。(REQ-370)

**擷取結果：**

- `GET /query/nl/{job_id}`——輪詢。回傳工作文件。
- `GET /query/nl/{job_id}/stream`——SSE。每個生成目標完成時各觸發一個 `branch` 事件，最後觸發一個 `done` 事件。(REQ-357、REQ-358)

三個生成迴圈（Cypher、GraphQL、SQL）並行執行，各自經編譯器驗證，並於發生錯誤時進行精修。(REQ-355) 提示語會限定於該角色的可見結構描述範圍內。(REQ-356) 結果文件會依目標為每個分支建立鍵值：(REQ-357) [tool-verified: `provisa/nl/job.py:69`]

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

耗盡疊代上限的分支會回傳 `query: null`、`result: null` 及一個 `error` 字串。每項生成的查詢均在該使用者的權限下執行，並套用第二階段治理——此服務永不繞過治理機制。(REQ-359)

---

### `GET /data/sdl`

回傳某角色結構描述的 GraphQL SDL。(REQ-008) [tool-verified: `provisa/api/data/sdl.py:137`]

**標頭：**`X-Role: <role_id>`（必填）

**查詢參數：**

- `domain`——以逗號分隔的領域 ID。設定後，回應會篩選為該等領域及由其可達的資料表。

**回應：**`text/plain` 格式的 GraphQL SDL。

---

### `GET /data/introspection`

回傳 GraphQL 內省 JSON，可選擇依領域篩選。[tool-verified: `provisa/api/data/sdl.py:200`]

**標頭：**`X-Provisa-Role: <role_id>`（必填）

**查詢參數：**`domain`——以逗號分隔的領域 ID。

**回應：**`application/json` 格式的內省結果。

---

### `GET /data/graph-schema`

回傳該角色結構描述的圖形檢視：節點標籤及其關係型別，供 Cypher/圖形用戶端使用。每個節點標籤均包含 `pk_columns`，讓呼叫端得以判斷主索引鍵欄位。(REQ-398) [tool-verified: `provisa/api/rest/cypher_router.py:689`]

**回應：**`application/json`，包含 `node_labels`（各自帶有 `pk`/`pk_columns`）及 `relationship_types`。

---

### `GET /data/domains`

回傳該要求角色可存取的領域 ID。[tool-verified: `provisa/api/data/sdl.py:116`]

**標頭：**`X-Role: <role_id>`（必填）

**回應：**`["sales", "support", ...]`

---

### `GET /data/schema-version`

回傳目前的結構描述版本字串。結合了每次啟動的隨機亂數與重建計數器。用戶端可用此值於伺服器重新啟動後使結構描述快取失效。(REQ-537) [tool-verified: `provisa/api/data/sdl.py:102`]

**回應：**`{"version": "<boot-id>-<counter>"}`

---

### `GET /data/proto/{role_id}`

回傳某角色的自動產生 `.proto` 檔案。[tool-verified: `provisa/api/data/endpoint_dev.py:49`]

**回應：**`text/plain` 格式的 protobuf 結構描述。

每個已註冊資料表均會產生一個 proto `message`。關係則會產生巢狀訊息欄位。型別對應：`integer → int32`、`bigint → int64`、`varchar → string`、`decimal → double`、`boolean → bool`、`timestamp → google.protobuf.Timestamp`。(REQ-538)

---

### `GET /data/subscribe/{table}`

供某資料表即時異動通知使用的伺服器發送事件（Server-Sent Events）串流。(REQ-219、REQ-258) [tool-verified: `provisa/api/data/subscribe.py:239`]

通知傳遞會依數據來源型別選用可插拔的提供者：PostgreSQL 數據來源使用 `LISTEN/NOTIFY`（經 asyncpg）；MongoDB 數據來源使用 Change Streams（`collection.watch()`）；Kafka 數據來源使用消費者群組。各提供者均實作一套共通的非同步監看介面。無論使用何種提供者，均會套用行級安全篩選及結構描述驗證。(REQ-258) 亦支援 WebSocket 及 RSS 數據來源。(REQ-338、REQ-342)

**標頭——`X-Provisa-Sink`：**設為某個 Kafka 目標（例如 `kafka://broker:9092/topic`），即可將異動事件重新導向至 Kafka 接收端，而非以 SSE 回應。伺服器會啟動一個接收端消費者，並回傳 `202 Accepted`，而非開啟一條串流。(REQ-812) [tool-verified: `provisa/api/data/subscription_sse.py:137`]

---

## 管理 REST 端點

### 設定

#### `GET /admin/config`

以 `application/x-yaml` 格式下載目前的 `provisa.yaml`，並附帶 `Content-Disposition: attachment` 標頭。(REQ-164) [tool-verified: `provisa/api/admin/settings_router.py:19`]

#### `PUT /admin/config`

上傳一份修訂後的設定 YAML。伺服器會寫入一份 `.bak` 備份，儲存新檔案，並重新載入所有結構描述、數據來源及具體化檢視。(REQ-164) [tool-verified: `provisa/api/admin/settings_router.py:32`]

**要求本文：**原始 YAML 內容。

**回應：**

```json
{"success": true, "message": "Config uploaded and reloaded"}
```

重新載入失敗時：`{"success": false, "message": "<error>"}`。

---

### 設定值

#### `GET /admin/settings`

以 JSON 格式回傳目前的平台設定值。(REQ-165) [tool-verified: `provisa/api/admin/settings_router.py:50`]

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

於執行階段更新平台設定值。所有欄位均為選填——僅更新本文中出現的鍵。(REQ-165) [tool-verified: `provisa/api/admin/settings_router.py:100`]

**要求本文（部分範例）：**

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

各區段的可更新欄位：

- `redirect`：`enabled`、`threshold`、`default_format`、`ttl`
- `sampling`：`default_sample_size`
- `cache`：`default_ttl`
- `naming`：`domain_prefix`、`convention`——寫入設定檔並觸發結構描述重新載入 (REQ-253)
- `relationships`：`auto_track_fk`
- `otel`：`endpoint`、`service_name`、`sample_rate`、`support_endpoint`、`support_redact_sql_literals`、`support_redact_attributes`

**回應：**

```json
{"success": true, "updated": ["otel.support_endpoint", "cache.default_ttl"]}
```

---

### 可觀測性

#### `GET /admin/traces/recent`

由記憶體內的 span 緩衝區回傳最多 N 個近期已完成的 span。(REQ-302) [tool-verified: `provisa/api/admin/settings_router.py:317`]

**查詢參數：**`limit`（預設 50，上限 200）

**回應：**`{"traces": [...]}`

#### `POST /admin/query-engine/reload-catalog`

透過聯邦引擎協調器的 REST API，熱重載其中一個具名 catalog。重新連接 Provisa 的內部連線，並重新執行 OTel DDL。[tool-verified: `provisa/api/admin/settings_router.py:208`]

**查詢參數：**`catalog`（預設為 `"otel"`）

**回應：**

```json
{"success": true, "errors": []}
```

#### `POST /admin/query-engine/restart`

重新啟動聯邦引擎容器（僅限單節點開發環境）。[tool-verified: `provisa/api/admin/settings_router.py:287`]

**查詢參數：**`container`（預設為 `QUERY_ENGINE_CONTAINER` 環境變數，其次為 `"trino"`）

---

### 探索

#### `POST /admin/discover/relationships`

觸發關係探索。恆會由聯邦引擎執行外部索引鍵內省。(REQ-018) 若已設定 `ANTHROPIC_API_KEY`，則會執行 LLM 推論。(REQ-167) [tool-verified: `provisa/api/admin/discovery.py:55`]

**要求本文：**

```json
{
  "scope": "domain",
  "domain_id": "sales"
}
```

`scope` 必須為 `"table"`、`"domain"` 或 `"cross-domain"` 之一。於 `"table"` 範圍下，需要提供 `table_id`（整數）。於 `"domain"` 範圍下，需要提供 `domain_id`。

**回應：**`{"candidates_found": 12, "stored_ids": [1, 2, 3, ...]}`

#### `GET /admin/discover/candidates`

列出待處理的關係候選項目。[tool-verified: `provisa/api/admin/discovery.py:96`]

#### `POST /admin/discover/candidates/{candidate_id}/accept`

接受一個候選項目，並將其註冊為關係。[tool-verified: `provisa/api/admin/discovery.py:103`]

**要求本文（選用）：**`{"name": "custom-relationship-name"}`

#### `POST /admin/discover/candidates/{candidate_id}/reject`

拒絕一個候選項目。[tool-verified: `provisa/api/admin/discovery.py:110`]

**要求本文：**`{"reason": "Not a real join"}`

#### `GET /admin/discover/candidates/rejected/count`

回傳已拒絕候選項目的數量。[tool-verified: `provisa/api/admin/discovery.py:118`]

#### `DELETE /admin/discover/candidates/rejected`

刪除所有已拒絕的候選項目。[tool-verified: `provisa/api/admin/discovery.py:128`]

---

### 數據來源爬取

#### `POST /admin/sources/crawl`

爬取一個數據來源以內省其結構描述並註冊資料表。(REQ-012) [tool-verified: `provisa/api/admin/crawl_router.py:36`]

---

### 數據來源資料表搜尋

#### `GET /admin/sources/{source_id}/tables/search`

依名稱搜尋某數據來源中可用（尚未註冊）的資料表。[tool-verified: `provisa/api/admin/table_search_router.py:103`]

---

### 資料表側寫

#### `POST /admin/tables/{table_id}/profile`

對某已註冊資料表執行欄位側寫——基數、最小/最大值、空值比率。[tool-verified: `provisa/api/admin/table_profile_router.py:28`]

---

### 數據來源描述

#### `POST /admin/source-meta/db-description`

為某數據來源的資料表及欄位產生 LLM 輔助描述。[tool-verified: `provisa/api/admin/source_meta_router.py:48`]

---

### 動作（函式與 Webhook）

所有端點均位於 `/admin/actions` 前綴下。(REQ-205) [tool-verified: `provisa/api/admin/actions_router.py:24`]

每一次呼叫——無論來自 GraphQL、SQL、Cypher、Bolt、Arrow Flight、MCP `run_sql`，或 Provisa gRPC——均經由單一受治理的執行器路由，並統一強制執行 `writable_by` 及治理規則。(REQ-1156) [tool-verified: `provisa/api/data/action_exec.py`] 各通訊協定的呼叫語法請見 [docs/integrations.md](integrations.md#_6)。

#### `GET /admin/actions`

回傳所有已追蹤的資料庫函式及 webhook。(REQ-242) [tool-verified: `provisa/api/admin/actions_router.py:104`]

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

每個 webhook 物件均帶有一個 `approved` 布林值。webhook 需經數據管家執行其建立要求後方獲核准 (REQ-209)；設定中宣告的 webhook 則會自動核准。未經核准的 webhook 雖已註冊，但不會於任何介面上公開。[tool-verified: `provisa/api/admin/actions_router.py:124-131`]

#### `POST /admin/actions/functions`

註冊一個已追蹤的函式（Command）。(REQ-205) [tool-verified: `provisa/api/admin/actions_router.py:117`]

**關鍵欄位：**

| 欄位 | 必填 | 描述 |
| --- | --- | --- |
| `name` | 是 | 唯一的 Command 名稱 |
| `kind` | 是 | `"query"` → GraphQL Query 欄位；`"mutation"` → Mutation 欄位 |
| `implKind` | 否 | Command 的執行方式——見下表（預設為 `source_procedure`） |
| `binding` | 否 | `implKind` 專屬的連線詳情（JSON 物件） |
| `returnSchema` | 否 | JSON Schema `{type:"array", items:{type:"object", properties:{...}}}`——令該 Command 於所有介面上均可回傳集合 |
| `arguments` | 否 | `[{name, type}]` 引數定義；對 SQL 及 Bolt 呼叫端而言，位置順序具意義 |
| `visibleTo` | 否 | 可呼叫該 Command 的角色 ID |
| `writableBy` | 否 | 允許以變異方式呼叫該 Command 的角色 ID |
| `domainId` | 否 | 供 GraphQL 放置及存取控制使用的領域 |

**`implKind` 值：**

| `implKind` | 執行內容 | `binding` 欄位 |
| --- | --- | --- |
| `source_procedure` | 已註冊數據來源上的預存程序（預設） | `sourceId`、`schemaName`、`functionName` |
| `script` | 伺服端指令碼 | `script` |
| `http` | 外送 HTTP 呼叫 | `url`、`method` |
| `grpc` | 外送至外部伺服器的 gRPC 呼叫 | `target`、`method` |
| `python` | 由 Provisa 代管的 Python 可呼叫物件（REQ-885） | `callable`（例如 `"demo.py_functions:random_dataset"`） |

範例 Command `random_python_set`（`implKind: python`）及 `random_grpc_set`（`implKind: grpc`）於實務中展示了帶有 `returnSchema` 的集合回傳 Command；兩者均位於 `config/provisa-install.yaml` 中。[tool-verified: `config/provisa-install.yaml:809-856`]

#### `PUT /admin/actions/functions/{name}`

依名稱更新一個已追蹤的函式。[tool-verified: `provisa/api/admin/actions_router.py:182`]

#### `DELETE /admin/actions/functions/{name}`

依名稱刪除一個已追蹤的函式。[tool-verified: `provisa/api/admin/actions_router.py:233`]

#### `POST /admin/actions/webhooks`

註冊一個已追蹤的 webhook。(REQ-209) 註冊或更新 webhook 會建立一項數據管家核准要求——該 webhook 僅於數據管家核准後方於所有介面上生效。設定中宣告的 webhook 則自動核准。**要求本文欄位：**`name`、`url`、`method`、`timeoutMs`、`returns`、`inlineReturnType`、`arguments`、`visibleTo`、`domainId`、`description`、`kind`。[tool-verified: `provisa/api/admin/actions_router.py:132`, `provisa/api/admin/actions_router.py:325-331`]

#### `PUT /admin/actions/webhooks/{name}`

依名稱更新一個已追蹤的 webhook。任何編輯均會將核准狀態重設為待核准，直至重新核准為止。[tool-verified: `provisa/api/admin/actions_router.py:306`]

#### `DELETE /admin/actions/webhooks/{name}`

依名稱刪除一個已追蹤的 webhook。[tool-verified: `provisa/api/admin/actions_router.py:355`]

#### `POST /admin/actions/test`

依名稱測試一個動作（函式或 webhook）。(REQ-245) [tool-verified: `provisa/api/admin/actions_router.py:384`]

---

### 角色

所有端點均位於 `/admin/roles` 前綴下。[tool-verified: `provisa/api/admin/roles_router.py:18`]

| 方法 | 路徑 | 描述 |
| --- | --- | --- |
| `GET` | `/admin/roles/` | 列出所有角色 |
| `POST` | `/admin/roles/` | 建立一個角色 |
| `PUT` | `/admin/roles/{role_id}` | 更新一個角色 |
| `DELETE` | `/admin/roles/{role_id}` | 刪除一個角色 |

[tool-verified: `provisa/api/admin/roles_router.py`]

---

### 使用者

所有端點均位於 `/admin/users` 前綴下。[tool-verified: `provisa/api/admin/local_users_router.py:21`]

| 方法 | 路徑 | 描述 |
| --- | --- | --- |
| `POST` | `/admin/users/` | 建立一個本機使用者 |
| `GET` | `/admin/users/` | 列出本機使用者 |
| `GET` | `/admin/users/{user_id}` | 取得一個使用者 |
| `PUT` | `/admin/users/{user_id}` | 更新一個使用者 |
| `PATCH` | `/admin/users/{user_id}/password` | 變更密碼 |
| `DELETE` | `/admin/users/{user_id}` | 刪除一個使用者 |
| `GET` | `/admin/users/{user_id}/assignments` | 列出角色指派 |
| `POST` | `/admin/users/{user_id}/assignments` | 新增一項角色指派 |
| `DELETE` | `/admin/users/{user_id}/assignments/{assignment_id}` | 移除一項角色指派 |

---

### 組織

所有端點均位於 `/admin/orgs` 之下。[tool-verified: `provisa/api/admin/orgs_router.py:18`]

| 方法 | 路徑 | 描述 |
| --- | --- | --- |
| `GET` | `/admin/orgs/` | 列出組織 |
| `POST` | `/admin/orgs/` | 建立一個組織 |
| `PUT` | `/admin/orgs/{org_id}` | 更新一個組織 |
| `DELETE` | `/admin/orgs/{org_id}` | 刪除一個組織 |
| `GET` | `/admin/orgs/{org_id}/members` | 列出成員 |
| `POST` | `/admin/orgs/{org_id}/members` | 新增一個成員 |
| `DELETE` | `/admin/orgs/{org_id}/members/{user_id}` | 移除一個成員 |

---

### 邀請

所有端點均位於 `/admin/invites` 之下。[tool-verified: `provisa/api/admin/invites_router.py:18`]

| 方法 | 路徑 | 描述 |
| --- | --- | --- |
| `POST` | `/admin/invites/` | 建立一則邀請 |
| `GET` | `/admin/invites/` | 列出待處理的邀請 |
| `DELETE` | `/admin/invites/{token}` | 撤銷一則邀請 |

---

### 管理 GraphQL

#### `POST /admin/graphql`

供所有管理作業使用的 Strawberry GraphQL 端點：數據來源及資料表的 CRUD、關係管理、領域設定、行級安全規則、快取控制、命名慣例、排程工作管理，以及查詢編譯。(REQ-164) [tool-verified: `provisa/api/app.py:2171`]

**關鍵變異：**

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

### 設置

#### `GET /setup/status`

回傳首次執行的設置狀態。恆為未經驗證。(REQ-539) [tool-verified: `provisa/api/setup_router.py:100`]

#### `POST /setup/`

完成首次執行設置。[tool-verified: `provisa/api/setup_router.py:142`]

---

## 健康檢查

#### `GET /health` 或 `HEAD /health`

回傳 `{"status": "ok"}`。恆為未經驗證。(REQ-539) [tool-verified: `provisa/api/app.py:2258`]

---

## 錯誤回應

| 狀態 | 意義 |
| --- | --- |
| 400 | 無效查詢、驗證錯誤，或 SQL 剖析錯誤 |
| 401 | 缺少或無效的驗證權杖 |
| 403 | 功能不足；治理違規 |
| 404 | 找不到角色、資源或設定檔 |
| 422 | 缺少必要標頭（例如 `X-Role`） |
| 503 | 資料庫或數據來源未連接；相依項目無法使用 |
| 504 | 要求逾時 |

`POST /data/sql` 上的治理違規會回傳附帶結構化本文的 HTTP 403：(REQ-002) [tool-verified: `provisa/api/data/endpoint_dev.py:184-190`]

```json
{
  "detail": {
    "violations": [
      {"code": "V000", "message": "Table 'orders' is not accessible for role 'analyst'"}
    ]
  }
}
```

所有其他錯誤均使用：`{"detail": "<message>"}`。

---

## Arrow Flight 端點

連接埠 `8815`。經 gRPC 傳輸的原生 Arrow 欄式格式。(REQ-143、REQ-045) [tool-verified: `provisa/api/flight/server.py`]

查詢與目錄探索均可於同一連線上使用。完整的治理管線（行級安全、遮罩、取樣）會套用於每一則查詢。(REQ-130、REQ-143)

**票證格式**（JSON）：

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

當 Zaychik Flight SQL 代理伺服器可用時（連接埠 8480），記錄批次會端對端串流，無須完整具體化。(REQ-144) 若 Zaychik 無法使用，則會回退為透過聯邦查詢層進行具體化。(REQ-146)

---

## Protobuf gRPC 端點

連接埠 `50051`（可以 `GRPC_PORT` 環境變數或 `server.grpc_port` 設定覆寫）。(REQ-529) [tool-verified: `provisa/grpc/server.py`, `provisa/api/app.py`]

請於 `x-provisa-role` gRPC 中介資料鍵中傳遞角色。若缺少此值，伺服器會以 `UNAUTHENTICATED` 中止。[tool-verified: `provisa/grpc/server.py`]

由 `GET /data/proto/{role_id}` 下載該角色專屬的 proto。僅該角色可見的資料表及欄位會出現於其中。(REQ-039)

```proto
service ProvisaService {
  rpc QueryOrders (QueryOrdersRequest) returns (stream Orders);
  rpc InsertOrders (InsertOrdersRequest) returns (InsertOrdersResponse);
}
```

每個資料表均會產生一個 `Query{TypeName}` 串流 RPC。`Insert{TypeName}` RPC 為求結構描述對稱性而存在，但會以 `UNIMPLEMENTED` 中止。[tool-verified: `provisa/grpc/server.py`]

已啟用 `grpc_reflection.v1alpha`，讓服務探索無須預先編譯的 proto 亦可進行。(REQ-529) [tool-verified: `provisa/grpc/reflection.py`]

```bash
grpcurl -plaintext localhost:50051 list
grpcurl -plaintext -H 'x-provisa-role: analyst' \
  -d '{}' localhost:50051 ProvisaService/QueryOrders
```

gRPC 伺服器僅於啟動時能成功編譯出有效 proto 的情況下才會啟動。若結構描述建置失敗，gRPC 伺服器即不會啟動。(REQ-529)

---

## JDBC 驅動程式

Provisa JDBC 驅動程式（`provisa-jdbc-0.1.0.jar`）將語意目錄公開給 BI 工具（Tableau、PowerBI、DBeaver）。(REQ-126)

**連線 URL：**`jdbc:provisa://host:port` (REQ-131)

領域對應至 JDBC 結構描述。(REQ-127) 資料表使用其已註冊的別名。欄位使用別名，並以 `REMARKS` 形式呈現描述。(REQ-128) 標準中介資料方法（`getPrimaryKeys`、`getImportedKeys`、`getExportedKeys`）會將語意關係以 PK/FK 中介資料形式公開。

**SQL 支援：**`SELECT * FROM <alias> [WHERE col = 'value']`。(REQ-129)

驅動程式預設會要求 Arrow IPC 重新導向。結果會透過 `ArrowStreamReader` 逐批次串流，記憶體中受限於單一記錄批次。(REQ-293)

---

## `orderBy` 引數格式

`order_by` 引數使用 `{column: direction}` 物件，具備 6 種方向列舉值：(REQ-200)

```json
{
  "query": "{ orders(order_by: [{created_at: desc_nulls_last}]) { id created_at } }",
  "role": "admin"
}
```

支援的方向：`asc`、`desc`、`asc_nulls_first`、`asc_nulls_last`、`desc_nulls_first`、`desc_nulls_last`。(REQ-201)

---

## 訂閱

SSE 訂閱可於 `GET /data/subscribe/{table}` 使用。(REQ-219、REQ-258) 通知傳遞會依數據來源型別選用可插拔的提供者：PostgreSQL 數據來源使用 `LISTEN/NOTIFY`，MongoDB 數據來源使用 Change Streams，Kafka 數據來源使用消費者群組。無論使用何種提供者，均會套用行級安全篩選及結構描述驗證。WebSocket 及 RSS 數據來源亦透過相同端點提供支援。(REQ-338、REQ-342) [tool-verified: `provisa/api/data/subscribe.py:239`, `provisa/subscriptions/registry.py`, `provisa/api/app.py` `_rebuild_schemas`]
</content>
