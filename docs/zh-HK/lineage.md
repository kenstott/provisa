# 欄位級數據血緣

Provisa以靜態方式追蹤欄位級數據血緣——根據SQL定義及命令合約計算得出，無需執行。系統提供兩種檢視：單一陳述式的DAG，以及涵蓋所有已註冊檢視及具體化檢視（MV）的聯邦級溯源圖。

## 血緣探索工具

在使用者介面中導航至**Lineage**（`/lineage`）。貼上SQL陳述式並按一下**Build statement graph**，即可查看其欄位級DAG。按一下**Federation graph**，可載入涵蓋登記處中每個MV的溯源圖。[tool-verified: LineagePage.tsx:28-119]

## 陳述式級DAG（REQ-1160）

SQL中每個具名輸出欄位都會成為一個節點。產生器會沿著每個CTE、子查詢、join及行內命令呼叫向上追溯，直至其來源欄位，從而建立由來源輸入到最終輸出的有向圖。

### 實例演算

```sql
SELECT o.id, e.embedding, upper(e.geo) AS geo_u
FROM   orders o
JOIN   enrich_grpc_set('main.public.orders') e ON o.id = e.id
```

此陳述式產生三個輸出欄位。`geo_u`的圖如下：

```
orders.geo  ──[enrich_grpc_set(...)]──►  e.geo  ──[UPPER]──►  geo_u
orders.id   ─╮                                              (taint closure)
orders.region ─╯
```

- `orders.id`、`orders.region`及`orders.geo`為**source**節點（`enrich_grpc_set`的窄式輸入合約聲明了`id`及`region`；完整的taint closure會將所有已聲明的輸入連接至所有輸出）。[tool-verified: `_splice_commands` in graph.py:223-242]
- `e.embedding`及`e.geo`為**command**節點——即`enrich_grpc_set`的邊界。
- `geo_u`為由`UPPER`這個SQL函式所產生的**derived**節點。

命令邊界**並非不透明**。由於`enrich_grpc_set`聲明了其輸入欄位（`id`、`region`）及輸出欄位（`id`、`embedding`、`geo`），血緣引擎會將taint closure由來源關聯已聲明的欄位持續連接至每個輸出。[tool-verified: `_splice_commands` and `_input_relation` in graph.py:245-271]

### 節點種類及視覺提示

[tool-verified: LineageDag.tsx:25-29, KIND_COLOR constants; LineagePage.tsx:21-26 LEGEND]

| 節點種類 | 顏色 | 意義 |
|---|---|---|
| `source` | 綠色 | 基礎資料表的欄位 |
| `derived` | 藍色 | 由SQL運算式（函式、運算子、CTE）產生 |
| `command` | 紫色 | 已註冊命令的輸出欄位 |

節點上的附加圓環：

- **橙色圓環**——該陳述式的最終輸出欄位。
- **雙邊框**——該欄位所屬的關聯為具體化檢視（MV／CTAS快照）。
- **紅色圓環**——被分類為錯誤的循環成員。
- **黃色圓環**——被分類為回饋迴路的循環成員。

[tool-verified: LineageDag.tsx:88-103 Cytoscape style selectors]

### 邊上的具名轉換

每條邊都帶有產生目標欄位的原始SQL運算式，以及一份具名操作清單：SQL函式（`sql_function`）、算術／邏輯運算子（`operator`）、已註冊命令（`command`）、純欄位參照（`identity`）及字面值（`constant`）。[tool-verified: TransformOp and name_transform in graph.py:36-145]

來自命令呼叫的邊，會在使用者介面中以紫色虛線表示。[tool-verified: LineageDag.tsx:122-124]

## 聯邦級圖（REQ-1161）

聯邦圖將每個已註冊MV的陳述式級血緣合併為單一溯源圖。節點的身分為`relation.column`——某檢視的輸出欄位與另一檢視對同一欄位的輸入參照會合併為單一節點。結果是由基礎來源欄位延伸至平台上每個衍生數據集的單一DAG。[tool-verified: `build_federation_graph` in merge.py:205-229 and `qualify_outputs` in graph.py:275-299]

可使用`focus`、`direction`及`depth`，在不重新計算圖的情況下，於聯邦規模下限定檢視範圍。[tool-verified: `slice_graph` in merge.py:160-189]

## 循環（REQ-1161）

循環會被描述，而非被拒絕。血緣引擎會偵測每個有向循環並將其**分類**。[tool-verified: `Cycle.classification` property in merge.py:43-46]

| 分類 | 邊框顏色 | 意義 |
|---|---|---|
| `feedback` | 黃色 | 該循環經過一個具體化節點——屬合法、具時間延遲的回饋迴路。MV快照即為使其成為明確定義的版本邊界。 |
| `error` | 紅色 | 該迴路上並無具體化邊界——屬無穩定求值順序的循環定義，很可能是設計錯誤。 |

[tool-verified: LineagePage.tsx:83-98 cycle alert rendering; merge.py:38-48]

`feedback`循環並非失敗。若充實化MV將衍生欄位回饋至其自身的來源關聯，只要迴路上有一個節點已具體化，這便是有效模式——快照會在時間上將兩部分隔開。`error`循環則需要操作人員判斷：通常代表兩個檢視互相參照，中間並無快照。

## API

兩個端點均為**靜態**——只讀取定義及合約，不讀取數據。

### POST /admin/lineage/graph

傳回單一SQL陳述式的欄位級DAG。

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

回應結構[tool-verified: `LineageGraph.to_dict` in graph.py:82-105]：

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

當SQL無法解析時，會傳回HTTP 422。
[tool-verified: lineage_router.py:51-54]

### GET /admin/lineage/federation

傳回登記處中所有MV的合併溯源圖。

```
GET /admin/lineage/federation
GET /admin/lineage/federation?focus=orders.id&direction=downstream&depth=3
```

[tool-verified: `federation_graph` endpoint at lineage_router.py:73-98]

查詢參數[tool-verified: function signature at lineage_router.py:73-76]：

| 參數 | 值 | 預設值 | 效果 |
|---|---|---|---|
| `focus` | 節點ID | — | 將回應限定於此節點周圍的子圖 |
| `direction` | `upstream` \| `downstream` \| `both` | `both` | 由`focus`出發的走訪方向 |
| `depth` | 整數 | 不限 | 距`focus`的最大跳數 |

回應結構與陳述式圖相同，並附加一個`cycles`欄位
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

## 利用血緣治理命令合約

由於taint closure會將每個已聲明的輸入欄位連接至每個已聲明的輸出欄位，該closure的廣度完全取決於你聲明了甚麼。

試想一個命令，接收完整的`orders`資料表（`id`、`region`、`amount`、`customer_id`、`discount`、`notes`等），並傳回一個`embedding`。若輸入合約列出全部這些欄位，則每個使用該embedding的下游欄位都會顯示來自全部這些欄位的血緣。這雖然準確，卻並不實用——難以判斷實際上有甚麼真正發揮了作用。

只聲明`id`及`text`（即embedding模型實際讀取的欄位），血緣範圍便會收窄至這兩個來源欄位。這樣得出的推導既嚴謹又精確。

有關聲明窄式輸入合約的機制，請參閱[Commands](commands.md)。
