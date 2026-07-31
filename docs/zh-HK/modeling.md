<!-- markdownlint-disable MD046 -->
<!-- MD046 off: mkdocs-material `===` content-tab bodies are indented, which the linter
     misreads as indented code blocks; the fenced code blocks below are required for rendering. -->

# 數據建模（Entities 與 Facts）

Provisa 提供兩個宣告式基元 —— `entity` 同 `fact` —— 涵蓋咗每個星型結構（star schema）同
Data Vault 賴以組裝嘅所有構建塊。只需宣告規格；Provisa 會將佢準確轉換成具體化檢視、雙時態
（bitemporal）同關係定義，唔使你自己手寫（REQ-1164）。[tool-verified: modeling.py module
docstring lines 11-28]

## Entity 同 Fact 係咩

**entity** 係源關係嘅一個帶鍵、去重複、可選歷史化嘅投影。你為佢命名、指向一個數據來源、
宣告 entity key 同要保留嘅屬性，並選擇一個歷史模式。Provisa 會撰寫檢視嘅 SQL 並登記該具體化
檢視。當歷史啟用時，該具體化檢視為雙時態。[tool-verified: `Entity` dataclass, modeling.py
lines 53-69; `entity_registration` function, modeling.py lines 105-120]

**fact** 係一個對 entity key 嘅連接（join），縮減至一個宣告嘅粒度（grain），並帶有匯總的
度量（measures）。Provisa 會撰寫一個匯總具體化檢視查詢（按粒度 + FK 欄位嘅 `GROUP BY`），
並為每個宣告嘅維度連結登記一個關係。無度量嘅 fact 係一個純鍵集合 —— 即 Data Vault 嘅 link
模式。[tool-verified: `Fact` dataclass, modeling.py lines 91-102; `fact_registration`
function, modeling.py lines 123-141; comment at line 130 "a measureless fact is a pure key-set
(DV link)"]

呢兩個構造都係 IR（中間表示）。所生成嘅定義可以喺唔同引擎之間重新導向 —— 喺 Oracle、
Databricks 具體化，或者喺 MPP 引擎之上保持虛擬 —— 而唔使重新建模。[tool-verified:
modeling.py docstring lines 25-28]

## 歷史模式

一個 entity 有三種可用模式 [tool-verified: `_HISTORY` constant at modeling.py line 38,
`_HISTORY_MODE` dict at modeling.py line 40]：

| 模式 | 含義 | 雙時態模式 |
| --- | --- | --- |
| `none` | 僅當前狀態，冇歷史。 | — |
| `scd2` | 追蹤每個變更。只附加變更咗嘅行（delta），以 entity key 為鍵。 | `delta` |
| `snapshot` | 追蹤每次刷新。每次刷新都附加完整嘅結果集，並蓋上系統時間戳。 | `snapshot` |

`scd2` 需要一個 entity key 嚟計算 delta。`snapshot` 喺任何引擎都可以運行，但每次刷新儲存空間
就會增加一份完整副本。對大型、緩慢變化嘅數據來源選用 `scd2`；當你需要完整歷史而數據來源
又冇辦法提供鍵時，選用 `snapshot`。

fact 冇歷史模式 —— 時態涵蓋範圍由底層 entity 嘅歷史提供。

## 度量同匯總

度量以 `column:agg` 對嘅形式宣告。支援嘅匯總方式 [tool-verified: `_AGGS` at modeling.py
line 41]：

`sum` &nbsp;`avg` &nbsp;`min` &nbsp;`max` &nbsp;`count`

預設匯總方式係 `sum` [tool-verified: `Measure.agg` default at modeling.py line 75]。

## 實例演練：Customer entity + Sales fact

### 源表

- `raw.customers` —— id、name、region、tier
- `raw.orders` —— order_id、customer_id、amount、quantity

### 登記 Customer entity

=== "Admin UI"

    1. 打開 **Tables**，然後按 **+ Model**。
    2. 選擇 **Entity (dimension)**。
    3. 填寫表格：
       - **Name：** `Customer`
       - **Source relation：** `raw.customers`
       - **Domain：** *（你嘅 domain）*
       - **Entity key：** `id`
       - **Attributes：** `name, region, tier`
       - **History：** `SCD2 (track changes — delta bitemporal)`
    4. 按 **Create**。

=== "GraphQL API"

    ```graphql
    mutation {
      registerEntity(input: {
        name: "Customer"
        source: "raw.customers"
        domainId: "sales"
        key: ["id"]
        attributes: ["name", "region", "tier"]
        history: "scd2"
      }) {
        success
        message
      }
    }
    ```

Provisa 會生成並登記呢個雙時態具體化檢視 [tool-verified: `entity_registration` in
modeling.py lines 105-120]：

```sql
SELECT "id", "name", "region", "tier" FROM "raw"."customers"
-- registered as a bitemporal delta MV, entity key: ["id"]
```

### 登記 Sales fact

=== "Admin UI"

    1. 再次按 **+ Model**。
    2. 選擇 **Fact**。
    3. 填寫表格：
       - **Name：** `Sales`
       - **Source relation：** `raw.orders`
       - **Domain：** *（你嘅 domain）*
       - **Grain：** `order_id`
       - **Measures：** `amount:sum, quantity:sum`
       - **Dimensions：** `Customer:customer_id`
    4. 按 **Create**。

=== "GraphQL API"

    ```graphql
    mutation {
      registerFact(input: {
        name: "Sales"
        source: "raw.orders"
        domainId: "sales"
        grain: ["order_id"]
        measures: [
          { column: "amount", agg: "sum" }
          { column: "quantity", agg: "sum" }
        ]
        dimensions: [
          { entity: "Customer", via: "customer_id" }
        ]
      }) {
        success
        message
      }
    }
    ```

Provisa 會生成並登記 [tool-verified: `fact_registration` in modeling.py lines 123-141]：

```sql
SELECT "order_id", "customer_id",
       SUM("amount") AS "amount",
       SUM("quantity") AS "quantity"
FROM   "raw"."orders"
GROUP BY "order_id", "customer_id"
```

另外仲會登記一個關係：`Sales.customer_id → Customer`（基數：many-to-one）。
[tool-verified: `fact_table_input` in modeling_register.py lines 89-98, cardinality at
line 95]

## Model 表格（管理介面）

**+ Model** 按鈕出現喺 **Tables** 頁面（提示文字："Model an entity or fact (star schema /
Data Vault)"）。[tool-verified: tablesPage.json line 13; TablesPage.tsx lines 441-450]

彈出視窗頂部嘅分段控制項可以喺 **Entity (dimension)** 同 **Fact** 之間切換。
[tool-verified: ModelingForm.tsx lines 102-110]

### Entity 欄位

[tool-verified: ModelingForm.tsx lines 141-171; modelingForm.json]

| 欄位 | 必填 | 備註 |
| --- | --- | --- |
| Name | 是 | 目錄中嘅具體化檢視名稱 |
| Source relation | 是 | 以點分隔嘅關係，例如 `raw.customers` |
| Domain | 是 | 具體化檢視所屬嘅 domain |
| Entity key | 是 | 以逗號分隔嘅鍵欄位，例如 `id` |
| Attributes | 否 | 以逗號分隔嘅屬性欄位，例如 `name, region, tier` |
| History | 否 | `none` / `scd2` / `snapshot`；預設為 `none` |

### Fact 欄位

[tool-verified: ModelingForm.tsx lines 172-196; modelingForm.json]

| 欄位 | 必填 | 備註 |
| --- | --- | --- |
| Name | 是 | 目錄中嘅具體化檢視名稱 |
| Source relation | 是 | 以點分隔嘅關係，例如 `raw.orders` |
| Domain | 是 | 具體化檢視所屬嘅 domain |
| Grain | 是 | 以逗號分隔嘅粒度欄位，例如 `order_id` |
| Measures | 否 | 以逗號分隔嘅 `col:agg` 對，例如 `amount:sum, quantity:sum` |
| Dimensions | 否 | 以逗號分隔嘅 `Entity:fk_column` 對，例如 `Customer:customer_id` |

若度量中省略咗 `agg`（例如寫 `amount` 而唔係 `amount:sum`），預設為 `sum`。
[tool-verified: ModelingForm.tsx line 73 `agg: agg || "sum"`]

## GraphQL API

兩個 mutation 都喺管理 schema 入面。[tool-verified: schema_mutation.py lines 449-472]

### `registerEntity`

```graphql
mutation RegisterEntity($input: EntityInput!) {
  registerEntity(input: $input) {
    success
    message
  }
}
```

`EntityInput` 欄位 [tool-verified: types.py lines 449-456]：

| 欄位 | 類型 | 預設值 | 說明 |
| --- | --- | --- | --- |
| `name` | String | — | entity 具體化檢視喺目錄中嘅名稱 |
| `source` | String | — | 數據來源關係（`schema.table` 或加引號） |
| `domainId` | String | — | Domain id |
| `key` | [String] | — | Entity key 欄位 |
| `attributes` | [String] | `[]` | 要投影嘅屬性欄位 |
| `history` | String | `"none"` | `"none"` \| `"scd2"` \| `"snapshot"` |
| `visibleTo` | [String] | `["public"]` | 角色可見性列表 |

### `registerFact`

```graphql
mutation RegisterFact($input: FactInput!) {
  registerFact(input: $input) {
    success
    message
  }
}
```

`FactInput` 欄位 [tool-verified: types.py lines 472-479]：

| 欄位 | 類型 | 預設值 | 說明 |
| --- | --- | --- | --- |
| `name` | String | — | fact 具體化檢視喺目錄中嘅名稱 |
| `source` | String | — | 數據來源關係 |
| `domainId` | String | — | Domain id |
| `grain` | [String] | — | 用於 GROUP BY 嘅粒度欄位 |
| `measures` | [MeasureInput] | `[]` | `{ column, agg }` 對 |
| `dimensions` | [DimRefInput] | `[]` | `{ entity, via }` 對 |
| `visibleTo` | [String] | `["public"]` | 角色可見性列表 |

`MeasureInput`：`{ column: String, agg: String }` —— agg 預設為 `"sum"`。
[tool-verified: types.py lines 460-462]

`DimRefInput`：`{ entity: String, via: String }` —— `entity` 係被引用嘅 entity 名稱；
`via` 係 fact 數據來源上嘅 FK 欄位。
[tool-verified: types.py lines 465-468]

成功時，`registerFact` 會傳返以下形式嘅訊息：
`Fact 'Sales' registered with 1 dimension link(s)`。
[tool-verified: schema_mutation.py line 471]

## Kimball 星型結構同 Data Vault

呢兩種模式都唔需要獨立嘅工具。同一組基元可以組合成兩者。

### Kimball 星型結構

呢個示範會建立一個三維度嘅星型結構。有兩張新嘅源表：

- `raw.products` —— `product_id`、`name`、`category`、`list_price` [inferred: introduced
  for this example]
- `raw.date_spine` —— `date_key`、`year`、`quarter`、`month` [inferred: introduced for this
  example]

呢度 `raw.orders` 亦增加咗 `product_id` 同 `order_date` 欄位。[inferred]

#### 選擇 SCD 類型

歷史模式係 SCD Type 1 同 Type 2 之間唯一嘅開關：

| SCD 類型 | 歷史模式 | 效果 |
| --- | --- | --- |
| Type 1（僅當前） | `none` | 每次刷新時重建具體化檢視；冇行歷史 |
| Type 2（版本化） | `scd2` | 雙時態 delta 具體化檢視；每次變更都會附加一行新記錄，以 entity key 為鍵 |

[tool-verified: `_HISTORY_MODE` at modeling.py line 40; `entity_registration` history branch
at lines 115-119]

當下游查詢需要按事務發生時嘅狀態去連接一個維度時 —— 例如客戶喺購買嗰刻嘅 tier，而唔係
佢而家嘅 tier —— 就用 `scd2`。對於穩定嘅查閱表，就用 `none`。日期基準表（date spine）永遠
唔會變。若一個產品目錄只需要當前價格，就可以每次刷新時重建。

#### 粒度決策

粒度係 fact 所回答嘅最低層次細節。`order_id` 會令每張訂單有一行，保留咗計算唯一訂單數，
以及喺訂單粒度上同任何維度連接嘅能力。較粗嘅粒度 —— 例如 `["customer_id", "order_date"]`
—— 會喺訂單之間預先匯總，並永久捨棄呢啲細節。宣告業務所需嘅最窄粒度；較粗嘅彙總其後可以
用低成本推導出嚟。

#### 登記維度

**Customer**（SCD Type 2 —— tier 變更必須保留）：

```graphql
mutation {
  registerEntity(input: {
    name: "Customer"
    source: "raw.customers"
    domainId: "sales"
    key: ["id"]
    attributes: ["name", "region", "tier"]
    history: "scd2"
  }) { success message }
}
```

生成一個以 `id` 為鍵嘅雙時態 delta 具體化檢視 [tool-verified: entity_registration
modeling.py lines 105-120]：

```sql
SELECT "id", "name", "region", "tier" FROM "raw"."customers"
-- bitemporal delta MV, entity key: ["id"]
```

**Product**（SCD Type 1 —— 當前目錄，唔需要版本歷史）：

```graphql
mutation {
  registerEntity(input: {
    name: "Product"
    source: "raw.products"
    domainId: "sales"
    key: ["product_id"]
    attributes: ["name", "category", "list_price"]
    history: "none"
  }) { success message }
}
```

生成一個每次刷新都重建嘅普通具體化檢視 [tool-verified: entity_registration modeling.py
lines 105-114; `mv_bitemporal_mode` is only added when `history != "none"`, line 115]：

```sql
SELECT "product_id", "name", "category", "list_price" FROM "raw"."products"
```

**DateDim**（冇歷史 —— 日期係不可變嘅）：

```graphql
mutation {
  registerEntity(input: {
    name: "DateDim"
    source: "raw.date_spine"
    domainId: "sales"
    key: ["date_key"]
    attributes: ["year", "quarter", "month"]
    history: "none"
  }) { success message }
}
```

生成：

```sql
SELECT "date_key", "year", "quarter", "month" FROM "raw"."date_spine"
```

#### 喺三個維度上登記 Sales fact

粒度：`order_id`。三個維度引用 —— 各自一個 FK 欄位。兩個度量都係可加總嘅總和。

```graphql
mutation {
  registerFact(input: {
    name: "Sales"
    source: "raw.orders"
    domainId: "sales"
    grain: ["order_id"]
    measures: [
      { column: "amount",   agg: "sum" }
      { column: "quantity", agg: "sum" }
    ]
    dimensions: [
      { entity: "Customer", via: "customer_id" }
      { entity: "Product",  via: "product_id"  }
      { entity: "DateDim",  via: "order_date"  }
    ]
  }) { success message }
}
```

Provisa 會計算 `group_cols = dedup([grain] + [dim FKs])`
= `["order_id", "customer_id", "product_id", "order_date"]`，並生成
[tool-verified: fact_registration modeling.py lines 125-131]：

```sql
SELECT "order_id", "customer_id", "product_id", "order_date",
       SUM("amount")   AS "amount",
       SUM("quantity") AS "quantity"
FROM   "raw"."orders"
GROUP BY "order_id", "customer_id", "product_id", "order_date"
```

系統會自動登記三個關係 [tool-verified: modeling_register.py lines 89-98, cardinality
`"many_to_one"` at line 95]：

| 關係 | 基數 |
| --- | --- |
| `Sales.customer_id → Customer` | many-to-one |
| `Sales.product_id → Product` | many-to-one |
| `Sales.order_date → DateDim` | many-to-one |

#### 一致性維度（Conformed dimensions）

一致性維度只需登記一次，之後可以喺任意數量嘅 fact 中以名稱引用。假設 `raw.returns`
包含 `return_id`、`customer_id`、`product_id` 同 `amount`。Returns fact 可以直接重用
Customer 同 Product，唔使重新登記：

```graphql
mutation {
  registerFact(input: {
    name: "Returns"
    source: "raw.returns"
    domainId: "sales"
    grain: ["return_id"]
    measures: [{ column: "amount", agg: "sum" }]
    dimensions: [
      { entity: "Customer", via: "customer_id" }
      { entity: "Product",  via: "product_id"  }
    ]
  }) { success message }
}
```

`Sales` 同 `Returns` 都指向同一組 `Customer` 同 `Product` entity。Provisa 嘅連接路徑
保證透過任何一個 fact 嘅查詢都會經過同一個維度定義 [tool-verified: fact_registration uses
entity name as `target_table` at modeling.py lines 138-140; fact_table_input wires
`target_table_id` from that name at modeling_register.py lines 91-93]。

---

### Data Vault

同一組基元可以直接映射到 Data Vault 詞彙：

| DV 構件 | 基元 | 歷史 |
| --- | --- | --- |
| Hub | `entity` | `none` —— 只有 entity key |
| Satellite | `entity` | `scd2` 或 `snapshot` —— hub key 旁邊嘅屬性歷史 |
| Link | 無度量嘅 `fact` | — |
| Bridge / 匯總 link | 有度量嘅 `fact` | — |

呢個例子喺 `raw.customers` 同 `raw.orders` 之上建立一個最小化嘅 vault。

#### Hub

Hub 只包含 entity key，冇其他嘢。`attributes: []` 配合 `history: "none"` 會產生一個去重複
嘅當前鍵集合；屬性歷史完全存喺 satellite 入面。

```graphql
mutation {
  registerEntity(input: {
    name: "CustomerHub"
    source: "raw.customers"
    domainId: "vault"
    key: ["id"]
    attributes: []
    history: "none"
  }) { success message }
}
```

生成 [tool-verified: entity_registration modeling.py lines 107-108;
`cols = dedup([*key, *attributes])` = `["id"]` when `attributes=[]`]：

```sql
SELECT "id" FROM "raw"."customers"
```

```graphql
mutation {
  registerEntity(input: {
    name: "OrderHub"
    source: "raw.orders"
    domainId: "vault"
    key: ["order_id"]
    attributes: []
    history: "none"
  }) { success message }
}
```

生成：

```sql
SELECT "order_id" FROM "raw"."orders"
```

#### Satellite

Satellite 位於 hub key 旁邊，攜帶完整嘅屬性歷史。用 `scd2` 只附加變更咗嘅行；用
`snapshot` 為每次完整刷新蓋上時間戳。

```graphql
mutation {
  registerEntity(input: {
    name: "CustomerSat"
    source: "raw.customers"
    domainId: "vault"
    key: ["id"]
    attributes: ["name", "region", "tier"]
    history: "scd2"
  }) { success message }
}
```

生成 [tool-verified: entity_registration modeling.py lines 115-119;
`_HISTORY_MODE["scd2"]` = `"delta"` at modeling.py line 40]：

```sql
SELECT "id", "name", "region", "tier" FROM "raw"."customers"
-- bitemporal delta MV, entity key: ["id"]
```

`CustomerSat` 同 `CustomerHub` 都以 `id` 為鍵。Hub 係穩定嘅連接目標；satellite 透過雙時態層
提供某一時間點嘅屬性存取。

#### Link（無度量嘅 fact）

Link 記錄邊啲 hub key 曾經共同出現過 —— 只有鍵，冇度量。當 `measures` 為空時，Provisa
會省略 `GROUP BY` [tool-verified: modeling.py lines 130-131: `if f.measures: view_sql +=
" GROUP BY ..."`]。

```graphql
mutation {
  registerFact(input: {
    name: "OrderCustomerLink"
    source: "raw.orders"
    domainId: "vault"
    grain: ["order_id"]
    measures: []
    dimensions: [
      { entity: "CustomerHub", via: "customer_id" }
      { entity: "OrderHub",    via: "order_id"    }
    ]
  }) { success message }
}
```

`group_cols = dedup(["order_id"] + ["customer_id", "order_id"])` = `["order_id",
"customer_id"]`。冇度量，所以冇 `GROUP BY`。生成 [tool-verified: fact_registration
modeling.py lines 125-131]：

```sql
SELECT "order_id", "customer_id" FROM "raw"."orders"
```

系統會登記兩個關係：`OrderCustomerLink.customer_id → CustomerHub` 同
`OrderCustomerLink.order_id → OrderHub`，兩者都係 many-to-one
[tool-verified: modeling_register.py lines 89-98]。

#### Bridge / 匯總 link

為 link 加上度量，Provisa 就會發出 `GROUP BY`，產生一個預先匯總嘅 bridge。喺 `order_id`
粒度、每張訂單一個客戶嘅情況下，結果係每張訂單一行匯總記錄：

```graphql
mutation {
  registerFact(input: {
    name: "OrderSummary"
    source: "raw.orders"
    domainId: "vault"
    grain: ["order_id"]
    measures: [{ column: "amount", agg: "sum" }]
    dimensions: [
      { entity: "CustomerHub", via: "customer_id" }
      { entity: "OrderHub",    via: "order_id"    }
    ]
  }) { success message }
}
```

`group_cols = dedup(["order_id"] + ["customer_id", "order_id"])` = `["order_id",
"customer_id"]`（維度列表入面重複嘅 `order_id` 會被 `_dedup` 剔除）。生成
[tool-verified: fact_registration modeling.py lines 125-131]：

```sql
SELECT "order_id", "customer_id", SUM("amount") AS "amount"
FROM   "raw"."orders"
GROUP BY "order_id", "customer_id"
```

模型本身唔會決定方法論。粒度、一致性、SCD 選擇同 hub/satellite 拆分依然係建模者嘅決定。
Provisa 只負責執行。[tool-verified: modeling.py docstring lines 25-26]

## 指標（Metrics）（REQ-1317、REQ-1318、REQ-1320）

**指標**（metric）係一個具名、受治理嘅匯總定義，本身冇粒度。粒度 —— 即匯總所按嘅維度 ——
係喺查詢時由調用方綁定，而唔係喺定義時綁定。呢個正正就係指標同檢視嘅分別：檢視喺建立時
就鎖定粒度；指標喺被查詢之前都保持開放。[tool-verified: `Metric` class comment,
`provisa/core/models.py` lines 452–455: "A named, governed aggregate definition with no
grain of its own... grain is bound at query time by the requested dimension set"]

### Metric 物件

[tool-verified: `Metric` class, `provisa/core/models.py` lines 451–476]

| 欄位 | 必填 | 備註 |
| --- | --- | --- |
| `name` | 是 | snake_case，例如 `net_revenue`。驗證規則：`[a-z][a-z0-9_]*` |
| `expression` | 是 | 匯總型 ANSI-SQL；必須包含至少一個匯總函數 |
| `datatype` | 否 | 結果類型提示，例如 `number`、`integer` |
| `description` | 否 | 人類可讀嘅業務定義 |
| `ai_context` | 否 | 供 AI 消費方使用嘅文字 —— 投影到 MCP 工具、pg_description、GraphQL 文檔同 Ossie 匯出 |
| `visible_to` | 否 | 角色列表；預設為 `["*"]`（所有角色） |
| `from_fact` | — | 當指標係由 fact 度量自動生成時，會自動設置 |

表達式入面嘅欄位引用必須以資料表限定（例如 `orders.amount`，而唔係 `amount`）。未限定嘅
欄位喺展開時屬於硬性錯誤，而唔係警告。
[tool-verified: `_expression_tables`, `provisa/compiler/metric_expand.py` lines 83–96]

指標儲存庫喺每次寫入時都會驗證表達式。無法解析或唔包含任何匯總函數嘅表達式會被拒絕，
永遠唔會被儲存。
[tool-verified: `validate_expression`, `provisa/core/repositories/metric.py` lines 34–43]

配置條目示例：

```yaml
metrics:
  - name: net_revenue
    expression: "SUM(orders.amount) - SUM(orders.refunds)"
    datatype: number
    description: "Order revenue after refunds"
    ai_context: "Net revenue: total order amounts minus approved refunds. Use for P&L."
```

### 查詢一個指標

編譯器保留咗 `metrics` schema。[tool-verified: `METRICS_SCHEMA = "metrics"`,
`provisa/compiler/metric_expand.py` line 43] 每個指標都可以喺該 schema 入面，以虛擬關係嘅
形式被定址。你可以好似查詢一張表咁去查詢佢 —— 你所選擇嘅欄位會成為維度集合同 GROUP BY：

```sql
-- Scalar total (no dimension)
SELECT value FROM metrics.net_revenue;

-- Broken out by region and month
SELECT region, month, value FROM metrics.net_revenue GROUP BY region, month;
```

編譯器會喺治理（governance）執行之前，將呢個查詢重寫成一個針對底層語義表嘅真正分組匯總，
令 RLS 同遮罩得以套用喺真實嘅欄位之上。
[tool-verified: `expand_metric_query` docstring, `provisa/compiler/metric_expand.py` lines
263–276: "BEFORE governance, so RLS/masking apply to the real columns (REQ-1317)"]

對指標關係使用 `SELECT *` 會被拒絕 —— 需要明確指定維度欄位同 `value`。
[tool-verified: `expand_metric_query`, `provisa/compiler/metric_expand.py` lines 302–306]

當一個指標嘅表達式橫跨多張表時，編譯器會透過已登記嘅關係將佢哋連接埋一齊。若一個維度係
直接被引用嘅表嘅一個欄位，就會解析到嗰張表。若一個維度距離一步關係之遙，就會自動連接。
兩步以上或者含糊嘅維度，就係一個會指名責任方嘅硬性錯誤。
[tool-verified: `_JoinPlan.resolve_dimension`, `provisa/compiler/metric_expand.py` lines
190–228]

### 由 fact 規格生成嘅指標（REQ-1320）

當你登記一個 fact 時，每個宣告嘅度量都會自動登記一個對應嘅 Metric 物件。指標嘅
`from_fact` 欄位記錄咗來源 fact 表嘅名稱，而有效嘅分組維度就係透過該 fact 嘅 FK 關係
可以到達嘅 entity 屬性。
[tool-verified: `Metric.from_fact` comment, `provisa/core/models.py` line 466–467: "set
when this metric was auto-registered from a fact spec's measure"; `from_fact` stored in
`provisa/core/repositories/metric.py` line 57]

自動登記嘅指標會喺 Metrics 頁面上顯示一個 **fact** 標記。你可以好似編輯其他指標一樣去
編輯佢哋。[tool-verified: `MetricsPage.tsx` lines 405–408: `{m.fromFact && <Badge ...
data-testid={`metrics-from-fact-${m.name}`}>...</Badge>}`]

### 由指標組成嘅檢視（view_metrics，REQ-1318）

`view_metrics` 檢視會喺定義時鎖定一個指標嘅粒度。宣告指標名稱、維度欄位同可選嘅篩選條件；
編譯器就會生成對應嘅 SELECT。

[tool-verified: `ViewMetricsSpec`, `provisa/core/models.py` lines 479–492]

```yaml
tables:
  - source_id: pg1
    domain_id: sales
    schema: public
    table: monthly_revenue
    view_metrics:
      metrics: [net_revenue]
      dimensions: [region, month]
      filters: ["orders.status = 'completed'"]
```

編譯器（以呢個例子嚟講）會生成：

```sql
SELECT orders.region AS region, orders.month AS month,
       SUM(orders.amount) - SUM(orders.refunds) AS net_revenue
FROM orders
WHERE orders.status = 'completed'
GROUP BY orders.region, orders.month
```

`view_metrics` 同 `view_sql` 喺同一張表上互相排斥。
[tool-verified: `Table` model validator, `provisa/core/models.py` lines 614–617:
`if self.view_sql is not None and self.view_metrics is not None: raise ValueError(...)`]

**指標變更時嘅自動重新生成。** 當一個指標嘅表達式被更新時，每一個引用佢嘅 `view_metrics`
檢視都會重新編譯，並且新嘅 SQL 會立即被持久化。從結構上嚟講，檢視唔可能同指標定義出現
偏差。
[tool-verified: `regenerate_metric_views`, `provisa/api/admin/_metric_views.py` lines
79–117: "each dependent view_metrics spec recompiles against the UPDATED metric set and the
fresh SQL is persisted"]

**手寫檢視 SQL 入面嘅內嵌 `metric()` 調用。** 手寫嘅 `view_sql` 都可以透過 `metric('name')`
引用指標。編譯器會用指標嘅表達式取代每個調用，並記錄一條血緣邊（lineage edge）。呢樣令
手寫檢視喺引用指標而唔係複製其公式時，都享有同樣嘅變更即重新編譯特性。
[tool-verified: `expand_metric_calls_in_sql`, `provisa/compiler/metric_expand.py` lines
393–429]

備註：使用內嵌 `metric()` 調用嘅配置路徑檢視，會喺配置重新載入時重新生成，而唔係喺指標
upsert 時。[tool-verified: `regenerate_metric_views` docstring, `_metric_views.py` lines
84–86: "Free-hand view_sql born from inline metric() calls carries no stored provenance, so
it is not regenerated here (config-path views regenerate on config reload)"]

### Metrics 管理頁面（REQ-1323、REQ-1324）

打開 **Metrics** 導覽項目以管理受治理嘅指標。按一行以展開一個唯讀嘅詳情面板；喺面板入面
按 **Edit** 即可切換到內嵌編輯（冇彈出視窗）。**New Metric** 會喺表格上方打開一張內嵌建立
卡片。刪除確認係呢個頁面上唯一嘅彈出視窗。
[tool-verified: `MetricsPage.tsx` lines 214–216 comment: "REQ-1317: registered-metrics
management page (list / create / edit / delete). REQ-1323: detail-then-edit"]

建立／編輯表單為源自 fact 嘅指標提供咗一個三步選擇器建構工具：選擇來源 fact 表（已按
`modelingRole=fact` 篩選）、一個度量欄位，以及一個匯總函數（`SUM`、`AVG`、`COUNT`、`MIN`、
`MAX`）。datatype 會自動推導：`COUNT → bigint`、`AVG → numeric`、`SUM/MIN/MAX → 度量欄位
本身嘅類型`。表達式文字區依然係任意表達式嘅逃生出口。
[tool-verified: `deriveDatatype` function, `MetricsPage.tsx` lines 66–70;
`applyBuilder`, `MetricsPage.tsx` lines 273–285]

## IR 帶嚟嘅好處

每次登記調用都會經過同一條路徑，跟手寫具體化檢視一模一樣。entity/fact 規格係一種中間表示
—— 唔係範本，都唔係宏。佢所指向嘅倉庫係部署嘅一個屬性，而唔係模型嘅屬性。改變目標引擎，
同一組 `entity` / `fact` 宣告依然會喺該引擎具體化，因為所生成嘅 SQL 同雙時態模式喺結構上
就係引擎中立嘅。[tool-verified: modeling.py docstring lines 25-28; modeling_register.py
lines 56-66, 80-88]
