# 欄位層級血緣 {: #column-level-lineage }

Provisa 以靜態方式追蹤欄級資料血緣。它根據 SQL 定義及指令合約計算血緣，所以無須執行任何東西。你可以看到兩種視圖：每條陳述式的 DAG，以及涵蓋所有已登錄視圖及具體化視圖（MV）的聯邦級溯源圖。

## 血緣探索器 {: #the-lineage-explorer }

在 UI 中前往 **血緣**（`/lineage`）。貼上一段 SQL 陳述式並點擊**建立陳述式圖**，即可看到它的欄位層級 DAG。點擊**聯邦圖**則會載入涵蓋註冊表中每個 MV 的來源圖。

## 陳述式層級 DAG {: #statement-level-dag }

你 SQL 中每個具名的輸出欄位都會成為一個節點。建圖器會穿過每個 CTE、子查詢、聯結與內嵌命令呼叫，一路追溯回它的來源欄位，建出一張由來源輸入通往最終輸出的有向圖。

### 實例演練 {: #worked-example }

```sql
SELECT o.id, e.embedding, upper(e.geo) AS geo_u
FROM   orders o
JOIN   enrich_grpc_set('main.public.orders') e ON o.id = e.id
```

這段陳述式產出三個輸出欄位。`geo_u` 的圖長成這樣：

```text
orders.geo  ──[enrich_grpc_set(...)]──►  e.geo  ──[UPPER]──►  geo_u
orders.id   ─╮                                              (taint closure)
orders.region ─╯
```

- `orders.id`、`orders.region` 與 `orders.geo` 是**來源**節點（`enrich_grpc_set` 的窄輸入合約宣告了 `id` 與 `region`；完整的污染閉合會把所有已宣告的輸入連到所有輸出）。
- `e.embedding` 與 `e.geo` 是位於 `enrich_grpc_set` 邊界上的**命令**節點。
- `geo_u` 是由 `UPPER` 這個 SQL 函式產出的**衍生**節點。

命令邊界**並非不透明**。由於 `enrich_grpc_set` 宣告了自己的輸入欄位（`id`、`region`）與輸出欄位（`id`、`embedding`、`geo`），血緣引擎得以把污染閉合從來源關聯的已宣告欄位，連續地接合到每個輸出。

### 節點種類與視覺提示 {: #node-kinds-and-visual-cues }

| 節點種類 | 顏色 | 含義 |
| --- | --- | --- |
| `source` | 綠色 | 基底表的一個欄位 |
| `derived` | 藍色 | 由 SQL 運算式（函式、運算子、CTE）產出 |
| `command` | 紫色 | 來自某個已註冊命令的輸出欄位 |
| `native` | 橙色 | 資料源讀取的原始欄位，位於已註冊表的欄之下 |

節點上額外的環：

- **橙色環**——該陳述式的最終輸出欄位。
- **雙重邊框**——該欄位所屬的關聯是一個具體化檢視（MV／CTAS 快照）。
- **紅色環**——歸類為錯誤的循環中的成員。
- **黃色環**——歸類為回饋迴路的循環中的成員。

### 邊上的具名轉換 {: #named-transforms-on-edges }

每條邊都帶著產出目標欄位的原始 SQL 運算式，以及一份具名操作清單：SQL 函式（`sql_function`）、算術／邏輯運算子（`operator`）、已註冊的命令（`command`）、裸欄位引用（`identity`），以及常值（`constant`）。

由命令呼叫而來的邊，在 UI 中以紫色虛線呈現。

### 已註冊表之下

擁有資料源註冊權限的用戶會在每個源欄之下再看到一跳：資料源讀取時的欄位，放在一個代表其讀取位置的框中。該框帶有資料源的標誌和類型標籤（與 Sources 頁面顯示的一致），以及按資料源類型給出的地址：

| 資料源 | 地址 |
| --- | --- |
| OpenAPI | 操作（`GET /v2/customers`）、其固定參數和行的位置 |
| Remote GraphQL | 根欄位、其必需參數和行的位置 |
| MongoDB、Cosmos DB | 數據庫以及集合或容器 |
| Elasticsearch、Qdrant | 索引或集合 |
| CSV、Parquet、檔案、日誌 | 路徑，以及讀取多個檔案的表的檔案通配模式 |
| Kafka | 主題 |
| 程式碼資料源 | 函數 |
| 其他任何資料源 | 原生的模式和表 |

原始欄位在欄有欄位路徑時就是該路徑，否則就是欄自己的名稱。從它到已註冊欄的邊會標明重新命名方式：`path`、`alias` 或 `naming`（命名約定）。已註冊表的框說明它如何提供服務：`virtual`，在每條語句時讀取；或 `replica`，附帶其 TTL 和上次載入時間。憑據、密鑰引用以及 URL 中的用戶名、密碼和 query 從不顯示。沒有資料源註冊權限的用戶只能看到從已註冊表向上的血緣。

### 連結

當用戶的權限能打開相應頁面時，每個元素都會在其定義處打開。單擊欄位會打開它的表、視圖或資料源；雙擊框會打開它的關係；單擊命令的邊會打開該命令。內置 SQL 函數沒有頁面，也沒有連結。

## 聯邦級圖 {: #federation-wide-graph }

聯邦圖把每個已登錄 MV 的逐陳述式血緣合併成一張溯源圖。節點的識別是 `relation.column`，所以一個視圖的輸出欄與另一個視圖對該欄的輸入參照會合併為同一個節點。結果是一張單一的 DAG，從基礎來源欄一直延伸到平台中每個衍生資料集。

用 `focus`、`direction` 與 `depth` 可在聯邦規模下縮限檢視範圍，而毋須重新計算整張圖。

## 循環 {: #cycles }

血緣引擎會偵測每一個有向環並對其進行**分類**。環只會被描述，從不會被拒絕。

| 分類 | 邊框顏色 | 含義 |
| --- | --- | --- |
| `feedback` | 黃色 | 循環穿過一個已具體化的節點——這是合法、帶時間落差的回饋迴路。MV 快照就是那道使其定義明確的版本邊界。 |
| `error` | 紅色 | 迴路上沒有任何具體化邊界——這是一個沒有穩定求值次序的循環定義，很可能是設計錯誤。 |

`feedback` 環是一種有效的模式。把衍生欄回寫到自身來源關係的增強 MV，只要環上有一個節點是具體化的，就能正常運作，因為快照在時間上把兩半分隔開。`error` 環需要營運人員判斷，它通常意味著兩個視圖相互參照，中間沒有快照。

## API

兩個端點都是**靜態**的。它們讀取定義及合約，從不讀取資料。

### POST /admin/lineage/graph

傳回單一 SQL 陳述式的欄位層級 DAG。

```http
POST /admin/lineage/graph
Content-Type: application/json

{
  "sql": "SELECT o.id, e.embedding FROM orders o JOIN enrich_grpc_set('main.public.orders') e ON o.id = e.id",
  "dialect": "postgres"
}
```

回應形狀：

```json
{
  "nodes": [
    {"id": "orders.id", "column": "id", "relation": "orders", "kind": "source", "materialized": false}
  ],
  "edges": [
    {
      "source": "orders.id",
      "target": "e.id",
      "transform": "enrich_grpc_set(...)",
      "ops": [{"name": "enrich_grpc_set", "kind": "command"}]
    }
  ],
  "outputs": ["id", "embedding"]
}
```

對於擁有資料源註冊權限的調用方，兩個端點都會加上原生這一跳：類型為 `native` 的節點，以及一個以每個原生框的關係（`native:<domain>.<table>`）為鍵的 `natives` 物件：

```json
"natives": {
  "native:sales.customers": {
    "relation": "sales.customers",
    "source": "crm",
    "type": "openapi",
    "hints": {"brand": "stripe"},
    "address": [
      {"what": "operation", "value": "GET /v2/customers"},
      {"what": "rows", "value": "data.items"}
    ],
    "served": {"mode": "virtual"}
  }
}
```

副本的 `served` 為 `{"mode": "replica", "cache_ttl": 3600, "last_load": "...", "next_load": "..."}`。對於其他調用方，`natives` 為空。

SQL 無法剖析時傳回 HTTP 422。

### GET /admin/lineage/federation

傳回涵蓋註冊表中所有 MV 的合併來源圖。

```http
GET /admin/lineage/federation
GET /admin/lineage/federation?focus=orders.id&direction=downstream&depth=3
```

查詢參數：

| 參數 | 值 | 預設 | 作用 |
| --- | --- | --- | --- |
| `focus` | 一個節點 id | — | 把回應縮限到此節點周圍的子圖 |
| `direction` | `upstream` \| `downstream` \| `both` | `both` | 從 `focus` 出發往哪個方向走訪 |
| `depth` | 整數 | 無上限 | 距 `focus` 的最大跳躍距離 |

回應形狀與陳述式圖相同，另加一個 `cycles` 欄位
：

```json
{
  "nodes": [...],
  "edges": [...],
  "outputs": [...],
  "cycles": [
    {
      "nodes": ["orders.region", "enriched_orders.region"],
      "has_materialization_boundary": true,
      "classification": "feedback"
    }
  ]
}
```

## 每次重新整理由甚麼建構 {: #what-each-refresh-was-built-from }

實體化檢視的每次重新整理都會記錄它由甚麼建構：檢視定義的版本、其輸入的版本以及該版本是如何確定的，以及每個輸出欄位的來源欄位和運算式。維運檢視 `"ops"."mv_refresh_lineage"` 每次重新整理傳回一列：

| 欄位 | 內容 |
|---|---|
| `mv_id` | 被重新整理的實體化檢視 |
| `target_table` | 重新整理寫入的資料表 |
| `definition_version` | 檢視定義的版本 |
| `input_version`, `input_version_kind` | 其輸入的版本：Iceberg 快照、水位線、新鮮度權杖或重新整理時間 |
| `trace_id` | 該重新整理在 `"ops"."traces"` 中的追蹤 |
| `refreshed_at` | 執行時間 |
| `column_derivations` | 每個輸出欄位及其來源欄位和運算式，以 JSON 表示 |

```sql
SELECT refreshed_at, definition_version, input_version, input_version_kind
FROM "ops"."mv_refresh_lineage"
WHERE mv_id = 'view-view_exposure'
ORDER BY refreshed_at
```

該檢視讀取 `"ops"."traces"` 中的重新整理記錄，並在保留這些記錄的每個引擎上作答。在 Trino 上，它們是 `otel` 目錄的 Iceberg 資料表。在 DuckDB 上，伺服器把自身的追蹤記錄以 Parquet 檔案寫入其遙測資料夾（`PROVISA_TELEMETRY_DIR`，預設 `~/.provisa/telemetry`），每個匯出批次一個檔案，每天一個資料夾。排程的遙測壓縮（`otel.compact_cron`）會把每天的小檔案合併為最大 64 MB 的檔案。每個組織只讀取自己的重新整理，記錄無限期保留。只有在啟用 OpenTelemetry SDK 時（`OTEL_SDK_DISABLED` 未設定或為 false）才會記錄 span。

## 檢視當時的樣子，以及當時記錄的樣子 {: #a-view-as-it-stood-and-as-it-was-recorded }

將來源嘅欄映射為 `business_key`、`valid_time` 同 `recorded_time` 嘅檢視會保留佢擁有過嘅每個版本：每次刷新都係附加，任何儲存上都唔會改寫任何版本。普通讀取返回當前狀態。兩個請求標頭讀取佢嘅過去：

- `X-Provisa-As-Of` 按某個時間或某個時間範圍讀取檢視。
- `X-Provisa-As-Recorded` 按 Provisa 在某個時間點所記錄的樣子讀取檢視。該時間之後記錄的版本不計入，因此之後記錄的更正不會改變對過去的重播。

截至 9 月 30 日、按 10 月 1 日的記錄讀取得到原始數字；按 10 月 5 日的記錄讀取得到更正後的數字。每條陳述式的稽核記錄都保存這兩個時間（`as_of`、`as_recorded`），因此報表可以按其首次產生時的樣子重現。[帶歷史的檢視](configuration.md#views-with-history-time-travel)說明呢三項宣告（附示例），以及接受這兩個值的每個介面。

## 改名或刪除一個欄位會弄壞什麼 {: #what-a-column-rename-or-drop-would-break }

一個欄位帶著兩個名字，而各由不同的一組構件儲存。

**對外名稱**是 SQL 與 GraphQL 介面所顯示的那個：`table_columns.alias`，未設定別名時退回 snake_case 預設值。檢視、具體化檢視、指標運算式、RLS 謂詞、DQ 合約、指標檢視的粒度與 MV 行索引鍵，全都是照著那個名字撰寫的，因此**改一個別名，跟刪掉該欄位一樣，同樣會把它們弄壞**。

**實體名稱**是 `table_columns.column_name`，是能挺過表 upsert 整批替換欄位的那個身分。關係、[本體](ontology.md)繫結、標籤指派、浮水印欄位與欄位預設集儲存的是這一個，因此它們只在欄位被**移除**時才會壞。

`columnDependents` 兩者都會回報。下游檢視與 MV 來自按欄位對外名稱切分聯邦圖；圖涵蓋不到的構件，則來自對註冊表的直接掃描。

```graphql
query {
  columnDependents(tableId: "42", renamed: ["order_total"], removed: ["legacy_code"]) {
    columnName
    dependents { kind name detail breaksOn }
  }
}
```

`breaksOn` 對於引用對外名稱者為 `rename`，對於引用實體名稱者為 `remove`，因此呼叫方能分辨每個構件是在對這次編輯的哪一半起反應。

請**在**儲存**之前**問這個問題。要被改名的欄位，是靠它在註冊表中仍持有的對外名稱來定位的；別名一旦落地，舊名字就沒了，查詢也就什麼都找不到。

當待儲存的編輯變更了別名或縮減了欄集合時，「表」頁面會自動執行該查詢並列出結果。該警告僅供參考：它列出受影響的製品，由管理員決定。它從不阻止儲存，因為無法觸及所有使用方：按名稱查詢該欄的外部儀表板或用戶端應用程式，不在登錄表的掌握之內。出於同樣的原因，對自由 SQL 文字的掃描只把欄當作識別碼記號來比對，並不解析作用範圍，因此可能列出實際上並未使用該欄的製品。對於警告來說，多報才是安全的方向。

## 用血緣來治理命令合約 {: #using-lineage-to-govern-command-contracts }

由於污染閉合把每個已宣告的輸入欄位連到每個已宣告的輸出欄位，那次閉合有多寬，完全取決於你宣告了什麼。

設想一個指令，它接收完整的訂單表（`id`、`region`、`amount`、`customer_id`、`discount`、`notes`……）並傳回一個 `embedding`。如果輸入合約列出了所有這些欄，那麼下游每個使用該嵌入的欄都會顯示來自所有這些欄的血緣。這是準確的，卻沒有幫助，因為你看不出真正起作用的是哪些欄。

只宣告 `id` 與 `text`（embedding 模型真正讀取的欄位），血緣錐形就會收緊到那兩個來源欄位。這樣的推導既健全又精確。

宣告窄輸入合約的具體做法，見 [命令](commands.md)。
