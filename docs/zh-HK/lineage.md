# 欄級數據血緣 (Column-Level Lineage)

Provisa 以靜態方式追蹤欄級數據血緣——由 SQL 定義及 command 合約推算得出，不需要實際執行。系統提供兩種檢視方式：單一陳述式的 DAG，以及涵蓋所有已註冊檢視及具體化檢視 (MV) 的聯邦級溯源圖。

## 血緣探索工具

在使用者介面中前往 **Lineage**（`/lineage`）。貼上一段 SQL 陳述式並按一下 **Build statement graph**，即可查看其欄級 DAG。按一下 **Federation graph** 可載入登記冊中所有 MV 的溯源圖。[tool-verified: LineagePage.tsx:28-119]

## 陳述式層級 DAG（REQ-1160）

你的 SQL 中每一個具名輸出欄都會成為一個節點。建構工具會透過每一個 CTE、子查詢、join 及內嵌 command 呼叫，將其追溯回源頭欄，並建構出一個由來源輸入指向最終輸出的有向圖。

### 實例演示

```sql
SELECT o.id, e.embedding, upper(e.geo) AS geo_u
FROM   orders o
JOIN   enrich_grpc_set('main.public.orders') e ON o.id = e.id
```

此陳述式產生三個輸出欄。`geo_u` 的圖形如下：

```text
orders.geo  ──[enrich_grpc_set(...)]──►  e.geo  ──[UPPER]──►  geo_u
orders.id   ─╮                                              (taint closure)
orders.region ─╯
```

- `orders.id`、`orders.region` 及 `orders.geo` 是**來源 (source)** 節點（`enrich_grpc_set` 的窄輸入合約聲明了 `id` 及 `region`；完整的污染閉合 (taint closure) 會將所有已聲明的輸入連接至所有輸出）。[tool-verified: `_splice_commands` in graph.py:223-242]
- `e.embedding` 及 `e.geo` 是 **command** 節點——即 `enrich_grpc_set` 邊界。
- `geo_u` 是由 `UPPER` SQL 函式產生的**衍生 (derived)** 節點。

command 邊界**並非不透明**。由於 `enrich_grpc_set` 聲明了其輸入欄（`id`、`region`）及輸出欄（`id`、`embedding`、`geo`），血緣引擎會將污染閉合從來源關聯已聲明的欄持續延伸至每一個輸出。
[tool-verified: `_splice_commands` and `_input_relation` in graph.py:245-271]

### 節點種類與視覺提示

[tool-verified: LineageDag.tsx:25-29, KIND_COLOR constants; LineagePage.tsx:21-26 LEGEND]

| 節點種類 | 顏色 | 意義 |
| --- | --- | --- |
| `source` | 綠色 | 基礎資料表的欄 |
| `derived` | 藍色 | 由 SQL 運算式（函式、運算子、CTE）產生 |
| `command` | 紫色 | 由已註冊 command 產生的輸出欄 |

節點上的其他外圈標示：

- **橙色外圈**——該陳述式的最終輸出欄。
- **雙重邊框**——該欄所屬的關聯是一個具體化檢視（MV/CTAS 快照）。
- **紅色外圈**——被歸類為錯誤的循環中的成員。
- **黃色外圈**——被歸類為回饋迴圈的循環中的成員。

[tool-verified: LineageDag.tsx:88-103 Cytoscape style selectors]

### 邊上的具名轉換

每一條邊都帶有產生目標欄的原始 SQL 運算式，以及一份具名運算清單：SQL 函式（`sql_function`）、算術/邏輯運算子（`operator`）、已註冊 command（`command`）、單純欄參照（`identity`），以及常數 (`constant`)。
[tool-verified: TransformOp and name_transform in graph.py:36-145]

來自 command 呼叫的邊，在使用者介面中會以紫色虛線呈現。
[tool-verified: LineageDag.tsx:122-124]

## 聯邦級圖形（REQ-1161）

聯邦圖會將登記冊中每個已註冊 MV 的陳述式層級血緣合併為一個溯源圖。節點身分為 `relation.column`——同一欄的檢視輸出欄與另一檢視的輸入參照會收合為同一個節點。結果是一個由基礎來源欄指向平台中每個衍生數據集的單一 DAG。[tool-verified: `build_federation_graph` in merge.py:205-229 and `qualify_outputs` in graph.py:275-299]

使用 `focus`、`direction` 及 `depth`，可以在不重新計算整個圖的情況下，於聯邦層級縮小檢視範圍。[tool-verified: `slice_graph` in merge.py:160-189]

## 循環（REQ-1161）

循環會被描述，而非被拒絕。血緣引擎會偵測每一個有向循環並加以**分類**。[tool-verified: `Cycle.classification` property in merge.py:43-46]

| 分類 | 邊框顏色 | 意義 |
| --- | --- | --- |
| `feedback` | 黃色 | 該循環跨越一個具體化節點——屬於合法、帶有時間延遲的回饋迴圈。MV 快照是使其定義明確的版本邊界。 |
| `error` | 紅色 | 該迴圈上沒有具體化邊界——屬於沒有穩定求值順序的循環定義，很可能是設計錯誤。 |

[tool-verified: LineagePage.tsx:83-98 cycle alert rendering; merge.py:38-48]

`feedback` 循環並非失敗。只要迴圈中有一個節點被具體化，一個將衍生欄回饋至自身來源關聯的擴充 (enrichment) MV 就是有效模式——該快照在時間上將兩半隔開。`error` 循環則需要操作人員判斷：通常代表兩個檢視互相參照，而中間沒有任何快照。

## API

兩個端點都是**靜態**的——它們讀取的是定義及合約，而非數據本身。

### POST /admin/lineage/graph

傳回單一 SQL 陳述式的欄級 DAG。

```http
POST /admin/lineage/graph
Content-Type: application/json

{
  "sql": "SELECT o.id, e.embedding FROM orders o JOIN enrich_grpc_set('main.public.orders') e ON o.id = e.id",
  "dialect": "postgres"
}
```

[tool-verified: `lineage_graph` endpoint at lineage_router.py:45-54, LineageGraphRequest model at
lineage_router.py:29-31]

回應格式 [tool-verified: `LineageGraph.to_dict` in graph.py:82-105]：

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

當 SQL 無法解析時，會傳回 HTTP 422。
[tool-verified: lineage_router.py:51-54]

### GET /admin/lineage/federation

傳回登記冊中所有 MV 合併後的溯源圖。

```http
GET /admin/lineage/federation
GET /admin/lineage/federation?focus=orders.id&direction=downstream&depth=3
```

[tool-verified: `federation_graph` endpoint at lineage_router.py:73-98]

查詢參數 [tool-verified: function signature at lineage_router.py:73-76]：

| 參數 | 值 | 預設值 | 效果 |
| --- | --- | --- | --- |
| `focus` | 節點 id | — | 將回應範圍限定於此節點周邊的子圖 |
| `direction` | `upstream` \| `downstream` \| `both` | `both` | 從 `focus` 開始遍歷的方向 |
| `depth` | 整數 | 無限制 | 距離 `focus` 的最大跳數 |

回應格式與陳述式圖相同，並新增了 `cycles` 欄位
[tool-verified: `MergedGraph.to_dict` in merge.py:60-64]：

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

## 重新命名或刪除一個欄會影響什麼（REQ-1484）

一個欄帶有兩個名稱，並各自由不同的一組構件 (artifact) 儲存。

**顯示名稱 (exposed name)** 是 SQL 及 GraphQL 介面所顯示的名稱：`table_columns.alias`，若未設定別名 (alias)，則回退至 snake_case 預設值 [tool-verified: `computed_sql_alias` at
`schema_helpers.py:317`]。檢視、具體化檢視、指標運算式、RLS 判斷式、DQ 合約、指標檢視粒度 (grain) 及 MV 資料列索引鍵，全部都是依這個名稱撰寫的，因此**重新命名別名會像刪除該欄一樣，同樣確實地破壞它們**。

**實體名稱 (physical name)** 是 `table_columns.column_name`，是在資料表整批更新替換欄時仍然存續的身分標識。關聯、詞彙表綁定、標籤指派、浮水印欄 (watermark column) 及欄預設集都儲存這個名稱，因此只有在該欄被**移除**時才會受影響。

`columnDependents` 會同時回報兩者。下游的檢視及 MV 是透過在該欄的顯示名稱處切割聯邦圖得出；該圖未涵蓋的構件，則來自對登記冊的直接掃描 [tool-verified: `graph_dependents` in `provisa/lineage/dependents.py`, registry scans in
`provisa/api/admin/column_dependents.py`]。

```graphql
query {
  columnDependents(tableId: "42", renamed: ["order_total"], removed: ["legacy_code"]) {
    columnName
    dependents { kind name detail breaksOn }
  }
}
```

對於顯示名稱參照，`breaksOn` 為 `rename`；對於實體名稱參照，則為 `remove`，讓呼叫方可以判斷每個構件所反應的是編輯動作的哪一半。

請在儲存**之前**提出此查詢。重新命名後的欄，是以其在登記冊中仍然帶有的顯示名稱來定位；一旦別名已經套用落地，舊名稱便不復存在，查詢將一無所獲。

當待處理的編輯變更了別名或縮減了欄集時，Tables 頁面會自動執行此查詢，並列出查詢結果 [tool-verified: `diffEditedColumns` in
`provisa-ui/src/pages/tables/columnDiff.ts`, dialog in `TablesPage.tsx`]。此警告僅供參考：它會列出受影響的構件，並由管理員自行決定。它不會阻擋儲存動作，因為並非所有數據體系 (estate) 的使用方都能被觸及——例如外部儀表板或按欄名稱查詢的用戶端應用程式，均超出登記冊的認知範圍。基於同樣的理由，對自由 SQL 文字的掃描是以識別字詞方式比對該欄，而非解析作用範圍 (scope)，因此可能會指出某個構件，但該構件其實並未使用該欄。對於警告而言，寧可多報也不要漏報。

## 利用血緣治理 command 合約

由於污染閉合會將每個已聲明的輸入欄連接至每個已聲明的輸出欄，該閉合的涵蓋範圍完全取決於你所聲明的內容。

考慮一個接收完整 orders 資料表（`id`、`region`、`amount`、`customer_id`、`discount`、`notes` 等）並傳回一個 `embedding` 的 command。如果輸入合約列出了所有這些欄，那麼每個使用該 embedding 的下游欄都會顯示來自所有這些欄的血緣。這雖然準確，但用處不大——很難分辨出實際上重要的是什麼。

只聲明 `id` 及 `text`（即 embedding 模型實際讀取的欄），血緣錐形 (lineage cone) 便會收窄至這兩個來源欄。這樣得出的推導既穩妥又精確。

有關聲明窄輸入合約的機制，請參閱 [Commands](commands.md)。
