# 欄位層級血緣

Provisa 以靜態方式追蹤欄位層級的數據血緣——由 SQL 定義與命令合約推算而得，毋須實際執行。可用的檢視有兩種：逐陳述式的 DAG，以及橫跨所有已註冊檢視與具體化檢視 (MV) 的聯邦級來源圖。

## 血緣探索器

在 UI 中前往 **血緣**（`/lineage`）。貼上一段 SQL 陳述式並點擊**建立陳述式圖**，即可看到它的欄位層級 DAG。點擊**聯邦圖**則會載入涵蓋註冊表中每個 MV 的來源圖。[tool-verified: LineagePage.tsx:28-119]

## 陳述式層級 DAG（REQ-1160）

你 SQL 中每個具名的輸出欄位都會成為一個節點。建圖器會穿過每個 CTE、子查詢、聯結與內嵌命令呼叫，一路追溯回它的來源欄位，建出一張由來源輸入通往最終輸出的有向圖。

### 實例演練

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

- `orders.id`、`orders.region` 與 `orders.geo` 是**來源**節點（`enrich_grpc_set` 的窄輸入合約宣告了 `id` 與 `region`；完整的污染閉合會把所有已宣告的輸入連到所有輸出）。[tool-verified: `_splice_commands` in graph.py:223-242]
- `e.embedding` 與 `e.geo` 是**命令**節點——即 `enrich_grpc_set` 這道邊界。
- `geo_u` 是由 `UPPER` 這個 SQL 函式產出的**衍生**節點。

命令邊界**並非不透明**。由於 `enrich_grpc_set` 宣告了自己的輸入欄位（`id`、`region`）與輸出欄位（`id`、`embedding`、`geo`），血緣引擎得以把污染閉合從來源關聯的已宣告欄位，連續地接合到每個輸出。
[tool-verified: `_splice_commands` and `_input_relation` in graph.py:245-271]

### 節點種類與視覺提示

[tool-verified: LineageDag.tsx:25-29, KIND_COLOR constants; LineagePage.tsx:21-26 LEGEND]

| 節點種類 | 顏色 | 含義 |
| --- | --- | --- |
| `source` | 綠色 | 基底表的一個欄位 |
| `derived` | 藍色 | 由 SQL 運算式（函式、運算子、CTE）產出 |
| `command` | 紫色 | 來自某個已註冊命令的輸出欄位 |

節點上額外的環：

- **橙色環**——該陳述式的最終輸出欄位。
- **雙重邊框**——該欄位所屬的關聯是一個具體化檢視（MV／CTAS 快照）。
- **紅色環**——歸類為錯誤的循環中的成員。
- **黃色環**——歸類為回饋迴路的循環中的成員。

[tool-verified: LineageDag.tsx:88-103 Cytoscape style selectors]

### 邊上的具名轉換

每條邊都帶著產出目標欄位的原始 SQL 運算式，以及一份具名操作清單：SQL 函式（`sql_function`）、算術／邏輯運算子（`operator`）、已註冊的命令（`command`）、裸欄位引用（`identity`），以及常值（`constant`）。
[tool-verified: TransformOp and name_transform in graph.py:36-145]

由命令呼叫而來的邊，在 UI 中以紫色虛線呈現。
[tool-verified: LineageDag.tsx:122-124]

## 聯邦級圖（REQ-1161）

聯邦圖把每個已註冊 MV 的逐陳述式血緣合併成一張來源圖。節點身分為 `relation.column`——某個檢視的輸出欄位，與另一個檢視對同一欄位的輸入引用，會收攏成同一個節點。結果是一張由基底來源欄位通往平台上每個衍生數據集的單一 DAG。[tool-verified: `build_federation_graph` in merge.py:205-229
and `qualify_outputs` in graph.py:275-299]

用 `focus`、`direction` 與 `depth` 可在聯邦規模下縮限檢視範圍，而毋須重新計算整張圖。[tool-verified: `slice_graph` in merge.py:160-189]

## 循環（REQ-1161）

循環會被描述，而不是被拒絕。血緣引擎會偵測出每個有向循環並加以**分類**。[tool-verified: `Cycle.classification` property in merge.py:43-46]

| 分類 | 邊框顏色 | 含義 |
| --- | --- | --- |
| `feedback` | 黃色 | 循環穿過一個已具體化的節點——這是合法、帶時間落差的回饋迴路。MV 快照就是那道使其定義明確的版本邊界。 |
| `error` | 紅色 | 迴路上沒有任何具體化邊界——這是一個沒有穩定求值次序的循環定義，很可能是設計錯誤。 |

[tool-verified: LineagePage.tsx:83-98 cycle alert rendering; merge.py:38-48]

`feedback` 循環並非失敗。只要迴路上有一個節點是具體化的，一個把衍生欄位回饋進自身來源關聯的資料增益 MV 就是有效的做法——快照在時間上把兩半隔開了。`error` 循環則需要操作者判斷：它通常意味著兩個檢視互相引用，中間卻沒有快照。

## API

兩個端點都是**靜態的**——它們讀的是定義與合約，不是數據。

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

[tool-verified: `lineage_graph` endpoint at lineage_router.py:45-54, LineageGraphRequest model at
lineage_router.py:29-31]

回應形狀 [tool-verified: `LineageGraph.to_dict` in graph.py:82-105]：

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

SQL 無法剖析時傳回 HTTP 422。
[tool-verified: lineage_router.py:51-54]

### GET /admin/lineage/federation

傳回涵蓋註冊表中所有 MV 的合併來源圖。

```http
GET /admin/lineage/federation
GET /admin/lineage/federation?focus=orders.id&direction=downstream&depth=3
```

[tool-verified: `federation_graph` endpoint at lineage_router.py:73-98]

查詢參數 [tool-verified: function signature at lineage_router.py:73-76]：

| 參數 | 值 | 預設 | 作用 |
| --- | --- | --- | --- |
| `focus` | 一個節點 id | — | 把回應縮限到此節點周圍的子圖 |
| `direction` | `upstream` \| `downstream` \| `both` | `both` | 從 `focus` 出發往哪個方向走訪 |
| `depth` | 整數 | 無上限 | 距 `focus` 的最大跳躍距離 |

回應形狀與陳述式圖相同，另加一個 `cycles` 欄位
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

## 改名或刪除一個欄位會弄壞什麼（REQ-1484）

一個欄位帶著兩個名字，而各由不同的一組構件儲存。

**對外名稱**是 SQL 與 GraphQL 介面所顯示的那個：`table_columns.alias`，未設定別名時退回 snake_case 預設值 [tool-verified: `computed_sql_alias` at
`schema_helpers.py:317`]。檢視、具體化檢視、指標運算式、RLS 謂詞、DQ 合約、指標檢視的粒度與 MV 行索引鍵，全都是照著那個名字撰寫的，因此**改一個別名，跟刪掉該欄位一樣，同樣會把它們弄壞**。

**實體名稱**是 `table_columns.column_name`，是能挺過表 upsert 整批替換欄位的那個身分。關係、[詞彙表](glossary.md)繫結、標籤指派、浮水印欄位與欄位預設集儲存的是這一個，因此它們只在欄位被**移除**時才會壞。

`columnDependents` 兩者都會回報。下游檢視與 MV 來自按欄位對外名稱切分聯邦圖；圖涵蓋不到的構件，則來自對註冊表的直接掃描 [tool-verified: `graph_dependents` in `provisa/lineage/dependents.py`, registry scans in
`provisa/api/admin/column_dependents.py`]。

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

當待處理的編輯改動了別名或縮減了欄位集合時，表頁面會自動執行這個查詢，並把找到的東西列出來 [tool-verified: `diffEditedColumns` in
`provisa-ui/src/pages/tables/columnDiff.ts`, dialog in `TablesPage.tsx`]。這項警告只是建議性質：它把受影響的構件點名，由管理員定奪。它不會擋下儲存，因為整個數據體系的消費方不可能全都觸及得到——外部的一個儀表板、或一個按名稱查詢該欄位的用戶端應用程式，都超出註冊表的認知範圍。基於同樣的理由，對自由 SQL 文字的掃描是把欄位當成識別碼權杖來比對，而不解析範圍，因此可能點名到其實並未用到該欄位的構件。就警告而言，寧多勿漏才是安全的方向。

## 用血緣來治理命令合約

由於污染閉合把每個已宣告的輸入欄位連到每個已宣告的輸出欄位，那次閉合有多寬，完全取決於你宣告了什麼。

設想一個命令，吃下整張 orders 表（`id`、`region`、`amount`、`customer_id`、`discount`、`notes`……）並傳回一個 `embedding`。若輸入合約把那些欄位全列上，那麼每個用到該 embedding 的下游欄位，都會顯示出自全部欄位的血緣。那是準確的，但沒有用處——很難看出真正要緊的是哪些。

只宣告 `id` 與 `text`（embedding 模型真正讀取的欄位），血緣錐形就會收緊到那兩個來源欄位。這樣的推導既健全又精確。

宣告窄輸入合約的具體做法，見 [命令](commands.md)。
