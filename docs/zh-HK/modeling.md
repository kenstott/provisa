<!-- markdownlint-disable MD046 -->
<!-- MD046 off: mkdocs-material `===` content-tab bodies are indented, which the linter
     misreads as indented code blocks; the fenced code blocks below are required for rendering. -->

# 數據建模（Entities 與 Facts） {: #data-modeling-entities-facts }

Provisa 提供兩個宣告式原語 `entity` 及 `fact`，涵蓋了構成每個星型模型及每個 Data Vault 的基本構件。宣告規格，Provisa 就會把它精確轉換為您原本需要人手編寫的具體化檢視、雙時態及關係定義。

## Entity 同 Fact 係咩 {: #what-entities-and-facts-are }

**entity** 係源關係嘅一個帶鍵、去重複、可選歷史化嘅投影。你為佢命名、指向一個數據來源、
宣告 entity key 同要保留嘅屬性，並選擇一個歷史模式。Provisa 會撰寫檢視嘅 SQL 並登記該具體化
檢視。當歷史啟用時，該具體化檢視為雙時態。
此時佢將 entity key 宣告為 `business_key`，並附上你指定嘅有效時間欄同記錄時間欄。

**fact** 是一個連到實體鍵的聯結，歸約到宣告的粒度，並帶有彙總量度。Provisa 會寫出一個彙總 MV 查詢（`GROUP BY` 粒度 + 外鍵資料行），並為每個宣告的維度連結註冊一個關係。沒有量度的 fact 是純鍵集合，亦即 Data Vault 的 link 模式。

這兩種構造都是 IR。產生的定義可以在引擎之間遷移而毋須重新建模：在 Oracle 或 Databricks 中具體化，或在 MPP 引擎上保持虛擬。

## 歷史模式 {: #history-modes }

entity 有三種可用模式：

| 模式 | 含義 |
| --- | --- |
| `none` | 僅當前狀態，無歷史。 |
| `scd2` | 追蹤每次變更，以 entity key 為鍵。 |
| `snapshot` | 追蹤每次刷新，以 entity key 為鍵。 |

`scd2` 同 `snapshot` 都保留每個版本，由 Provisa 決定點樣儲存。兩者都需要一個有效時間欄同一個記錄時間欄（`validTime`、`recordedTime`）：業務賦予該記錄嘅日期，以及該版本幾時輸入或修改。[帶歷史的檢視](configuration.md#views-with-history-time-travel)解釋呢兩種時間。

fact 沒有自己的歷史模式；它們在時間上的覆蓋來自下層實體的歷史。

## 度量同匯總 {: #measures-and-aggregations }

度量以 `column:agg` 對嘅形式宣告。支援嘅匯總方式：

`sum` &nbsp;`avg` &nbsp;`min` &nbsp;`max` &nbsp;`count`

預設匯總方式係 `sum`。

## 實例演練：Customer entity + Sales fact {: #worked-example-customer-entity-sales-fact }

### 源表 {: #the-source-tables }

- `raw.customers` —— id、name、region、tier、effective_date、updated_at
- `raw.orders` —— order_id、customer_id、amount、quantity

### 登記 Customer entity {: #register-the-customer-entity }

=== "Admin UI"

    1. 打開 **Tables**，然後按 **+ Model**。
    2. 選擇 **Entity (dimension)**。
    3. 填寫表格：
       - **Name：** `Customer`
       - **Source relation：** `raw.customers`
       - **Domain：** *（你嘅 domain）*
       - **Business key：** `id`
       - **Attributes：** `name, region, tier`
       - **Keep history：** `Bitemporal`
       - **Valid time：** `effective_date`
       - **Recorded time：** `updated_at`
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
        validTime: "effective_date"
        recordedTime: "updated_at"
      }) {
        success
        message
      }
    }
    ```

Provisa 會生成並登記呢個雙時態具體化檢視：

```sql
SELECT "id", "name", "region", "tier", "effective_date", "updated_at" FROM "raw"."customers"
-- registered as a bitemporal MV: business_key ["id"], valid_time effective_date,
-- recorded_time updated_at
```

### 登記 Sales fact {: #register-the-sales-fact }

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

Provisa 會生成並登記：

```sql
SELECT "order_id", "customer_id",
       SUM("amount") AS "amount",
       SUM("quantity") AS "quantity"
FROM   "raw"."orders"
GROUP BY "order_id", "customer_id"
```

另外仲會登記一個關係：`Sales.customer_id → Customer`（基數：many-to-one）。

## Model 表格（管理介面） {: #the-model-form-admin-ui }

**+ Model** 按鈕出現喺 **Tables** 頁面（提示文字："Model an entity or fact (star schema /
Data Vault)"）。

彈出視窗頂部嘅分段控制項可以喺 **Entity (dimension)** 同 **Fact** 之間切換。

### Entity 欄位 {: #entity-fields }

| 欄位 | 必填 | 備註 |
| --- | --- | --- |
| Name | 是 | 目錄中嘅具體化檢視名稱 |
| Source relation | 是 | 以點分隔嘅關係，例如 `raw.customers` |
| Domain | 是 | 具體化檢視所屬嘅 domain |
| Entity key | 是 | 以逗號分隔嘅鍵欄位，例如 `id` |
| Attributes | 否 | 以逗號分隔嘅屬性欄位，例如 `name, region, tier` |
| History | 否 | `none` / `scd2` / `snapshot`；預設為 `none` |
| Valid time | 啟用歷史時 | 儲存業務賦予該記錄嘅日期嘅欄 |
| Recorded time | 啟用歷史時 | 儲存該版本幾時輸入或修改嘅欄 |

### Fact 欄位 {: #fact-fields }

| 欄位 | 必填 | 備註 |
| --- | --- | --- |
| Name | 是 | 目錄中嘅具體化檢視名稱 |
| Source relation | 是 | 以點分隔嘅關係，例如 `raw.orders` |
| Domain | 是 | 具體化檢視所屬嘅 domain |
| Grain | 是 | 以逗號分隔嘅粒度欄位，例如 `order_id` |
| Measures | 否 | 以逗號分隔嘅 `col:agg` 對，例如 `amount:sum, quantity:sum` |
| Dimensions | 否 | 以逗號分隔嘅 `Entity:fk_column` 對，例如 `Customer:customer_id` |

若度量中省略咗 `agg`（例如寫 `amount` 而唔係 `amount:sum`），預設為 `sum`。

## GraphQL API {: #the-graphql-api }

兩個 mutation 都喺管理 schema 入面。

### `registerEntity`

```graphql
mutation RegisterEntity($input: EntityInput!) {
  registerEntity(input: $input) {
    success
    message
  }
}
```

`EntityInput` 欄位：

| 欄位 | 類型 | 預設值 | 說明 |
| --- | --- | --- | --- |
| `name` | String | — | entity 具體化檢視喺目錄中嘅名稱 |
| `source` | String | — | 數據來源關係（`schema.table` 或加引號） |
| `domainId` | String | — | Domain id |
| `key` | [String] | — | Entity key 欄位 |
| `attributes` | [String] | `[]` | 要投影嘅屬性欄位 |
| `history` | String | `"none"` | `"none"` \| `"scd2"` \| `"snapshot"` |
| `validTime` | String | `null` | 有效時間欄；`history` 唔係 `"none"` 時必填 |
| `recordedTime` | String | `null` | 記錄時間欄；`history` 唔係 `"none"` 時必填 |
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

`FactInput` 欄位：

| 欄位 | 類型 | 預設值 | 說明 |
| --- | --- | --- | --- |
| `name` | String | — | fact 具體化檢視喺目錄中嘅名稱 |
| `source` | String | — | 數據來源關係 |
| `domainId` | String | — | Domain id |
| `grain` | [String] | — | 用於 GROUP BY 嘅粒度欄位 |
| `measures` | [MeasureInput] | `[]` | `{ column, agg }` 對 |
| `dimensions` | [DimRefInput] | `[]` | `{ entity, via }` 對 |
| `visibleTo` | [String] | `["public"]` | 角色可見性列表 |

`MeasureInput`：`{ column: String, agg: String }`；agg 預設為 `"sum"`。

`DimRefInput`：`{ entity: String, via: String }`；`entity` 係被引用嘅 entity 名稱； `via` 係 fact 數據來源上嘅 FK 欄位。

成功時，`registerFact` 會傳返以下形式嘅訊息：
`Fact 'Sales' registered with 1 dimension link(s)`。

## Kimball 星型結構同 Data Vault {: #kimball-star-schema-and-data-vault }

呢兩種模式都唔需要獨立嘅工具。同一組基元可以組合成兩者。

### Kimball 星型結構 {: #kimball-star-schema }

呢個示範會建立一個三維度嘅星型結構。有兩張新嘅源表：

- `raw.products` —— `product_id`、`name`、`category`、`list_price`
- `raw.date_spine` —— `date_key`、`year`、`quarter`、`month`

呢度 `raw.orders` 亦增加咗 `product_id` 同 `order_date` 欄位。

#### 選擇 SCD 類型 {: #choosing-scd-type }

歷史模式係 SCD Type 1 同 Type 2 之間唯一嘅開關：

| SCD 類型 | 歷史模式 | 效果 |
| --- | --- | --- |
| Type 1（僅當前） | `none` | 每次刷新時重建具體化檢視；冇行歷史 |
| Type 2（版本化） | `scd2` | 雙時態具體化檢視；每次變更都新增一個版本，以 entity key 為鍵 |

當下游查詢需要按交易發生時的狀態聯結維度時，使用 `scd2`，例如購買時客戶所在的等級，而不是目前等級。穩定的查找表用 `none`。日期軸永遠不變；只需要目前價格的產品目錄可以在每次重新整理時重建。

#### 粒度決策 {: #grain-decision }

粒度是 fact 所能回答的最細明細層級。`order_id` 表示每張訂單一列，保留了統計不同訂單數、以及在訂單層級聯結任意維度的能力。更粗的粒度，例如 `["customer_id", "order_date"]`，會在訂單之間預先彙總，並永久丟掉這些明細。請宣告業務所需的最細粒度；更粗的彙總以後很容易推導。

#### 登記維度 {: #register-the-dimensions }

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
    validTime: "effective_date"
    recordedTime: "updated_at"
  }) { success message }
}
```

生成一個以 `id` 為鍵嘅雙時態具體化檢視：

```sql
SELECT "id", "name", "region", "tier", "effective_date", "updated_at" FROM "raw"."customers"
-- bitemporal MV: business_key ["id"], valid_time effective_date, recorded_time updated_at
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

生成一個每次刷新都重建嘅普通具體化檢視：

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

#### 喺三個維度上登記 Sales fact {: #register-the-sales-fact-across-three-dimensions }

粒度：`order_id`。三個維度參照，每個各有一個外鍵資料行。兩個量度都是可加的求和。

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
：

```sql
SELECT "order_id", "customer_id", "product_id", "order_date",
       SUM("amount")   AS "amount",
       SUM("quantity") AS "quantity"
FROM   "raw"."orders"
GROUP BY "order_id", "customer_id", "product_id", "order_date"
```

系統會自動登記三個關係：

| 關係 | 基數 |
| --- | --- |
| `Sales.customer_id → Customer` | many-to-one |
| `Sales.product_id → Product` | many-to-one |
| `Sales.order_date → DateDim` | many-to-one |

#### 一致性維度（Conformed dimensions） {: #conformed-dimensions }

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
保證透過任何一個 fact 嘅查詢都會經過同一個維度定義。

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

#### Hub {: #hubs }

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

生成：

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

Satellite 位於 hub key 旁邊，攜帶完整嘅屬性歷史。用 `scd2` 追蹤每次變更，或用 `snapshot` 追蹤每次刷新。

```graphql
mutation {
  registerEntity(input: {
    name: "CustomerSat"
    source: "raw.customers"
    domainId: "vault"
    key: ["id"]
    attributes: ["name", "region", "tier"]
    history: "scd2"
    validTime: "effective_date"
    recordedTime: "updated_at"
  }) { success message }
}
```

生成：

```sql
SELECT "id", "name", "region", "tier", "effective_date", "updated_at" FROM "raw"."customers"
-- bitemporal MV: business_key ["id"], valid_time effective_date, recorded_time updated_at
```

`CustomerSat` 同 `CustomerHub` 都以 `id` 為鍵。Hub 係穩定嘅連接目標；satellite 透過雙時態層
提供某一時間點嘅屬性存取。

#### Link（無度量嘅 fact） {: #link-measureless-fact }

link 記錄哪些 hub 鍵一起出現過。它只保存鍵，沒有量度；`measures` 為空時，Provisa 會省略 `GROUP BY`。

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

`group_cols = dedup(["order_id"] + ["customer_id", "order_id"])` = `["order_id", "customer_id"]`。
冇度量，所以冇 `GROUP BY`。生成：

```sql
SELECT "order_id", "customer_id" FROM "raw"."orders"
```

系統會登記兩個關係：`OrderCustomerLink.customer_id → CustomerHub` 同
`OrderCustomerLink.order_id → OrderHub`，兩者都係 many-to-one
。

#### Bridge / 匯總 link {: #bridge-aggregate-link }

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

`group_cols = dedup(["order_id"] + ["customer_id", "order_id"])` = `["order_id", "customer_id"]`
（維度列表入面重複嘅 `order_id` 會被 `_dedup` 剔除）。生成
：

```sql
SELECT "order_id", "customer_id", SUM("amount") AS "amount"
FROM   "raw"."orders"
GROUP BY "order_id", "customer_id"
```

模型本身唔會決定方法論。粒度、一致性、SCD 選擇同 hub/satellite 拆分依然係建模者嘅決定。
Provisa 只負責執行。

## 指標（Metrics） {: #metrics }

**指標**是一個具名、受治理的彙總定義，本身沒有粒度。粒度，即彙總按哪些維度拆分，由呼叫方在查詢時繫結，而不是在定義時確定。這正是指標與檢視的分別：檢視在建立時已固定了粒度，指標則在被查詢之前一直保持開放。

### Metric 物件 {: #the-metric-object }

| 欄位 | 必填 | 備註 |
| --- | --- | --- |
| `name` | 是 | snake_case，例如 `net_revenue`。驗證規則：`[a-z][a-z0-9_]*` |
| `expression` | 是 | 匯總型 ANSI-SQL；必須包含至少一個匯總函數 |
| `datatype` | 否 | 結果類型提示，例如 `number`、`integer` |
| `description` | 否 | 人類可讀嘅業務定義 |
| `ai_context` | 否 | 供 AI 消費方使用嘅文字 —— 投影到 MCP 工具、pg_description、GraphQL 文檔同 Ossie 匯出 |
| `visible_to` | 否 | 角色列表；預設為 `["*"]`（所有角色） |
| `from_fact` | — | 當指標係由 fact 度量自動生成時，會自動設置 |

運算式中的資料行參照必須帶上資料表名稱限定（`orders.amount`，而不只是 `amount`）。未限定的資料行在展開指標時會直接報錯，不會只給警告就放行。

指標儲存庫喺每次寫入時都會驗證表達式。無法解析或唔包含任何匯總函數嘅表達式會被拒絕，
永遠唔會被儲存。

配置條目示例：

```yaml
metrics:
  - name: net_revenue
    expression: "SUM(orders.amount) - SUM(orders.refunds)"
    datatype: number
    description: "Order revenue after refunds"
    ai_context: "Net revenue: total order amounts minus approved refunds. Use for P&L."
```

### 查詢一個指標 {: #querying-a-metric }

編譯器保留了 `metrics` 結構描述，每個指標都可以作為其中的虛擬關係來存取。像查詢資料表一樣查詢它，您選擇的資料行就成為維度集合及 GROUP BY：

```sql
-- Scalar total (no dimension)
SELECT value FROM metrics.net_revenue;

-- Broken out by region and month
SELECT region, month, value FROM metrics.net_revenue GROUP BY region, month;
```

編譯器會喺治理（governance）執行之前，將呢個查詢重寫成一個針對底層語義表嘅真正分組匯總，
令 RLS 同遮罩得以套用喺真實嘅欄位之上。

對指標關係執行 `SELECT *` 會被拒絕。請明確列出維度資料行及 `value`。

當一個指標嘅表達式橫跨多張表時，編譯器會透過已登記嘅關係將佢哋連接埋一齊。若一個維度係
直接被引用嘅表嘅一個欄位，就會解析到嗰張表。若一個維度距離一步關係之遙，就會自動連接。
兩步以上或者含糊嘅維度，就係一個會指名責任方嘅硬性錯誤。

### 由 fact 規格生成嘅指標 {: #metrics-from-fact-specs }

當你登記一個 fact 時，每個宣告嘅度量都會自動登記一個對應嘅 Metric 物件。指標嘅
`from_fact` 欄位記錄咗來源 fact 表嘅名稱，而有效嘅分組維度就係透過該 fact 嘅 FK 關係
可以到達嘅 entity 屬性。

自動登記嘅指標會喺 Metrics 頁面上顯示一個 **fact** 標記。你可以好似編輯其他指標一樣去
編輯佢哋。

### 由指標組成嘅檢視（view_metrics） {: #metric-composed-views-view_metrics }

`view_metrics` 檢視會喺定義時鎖定一個指標嘅粒度。宣告指標名稱、維度欄位同可選嘅篩選條件；
編譯器就會生成對應嘅 SELECT。

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

**指標變更時嘅自動重新生成。** 當一個指標嘅表達式被更新時，每一個引用佢嘅 `view_metrics`
檢視都會重新編譯，並且新嘅 SQL 會立即被持久化。從結構上嚟講，檢視唔可能同指標定義出現
偏差。

**手寫檢視 SQL 入面嘅內嵌 `metric()` 調用。** 手寫嘅 `view_sql` 都可以透過 `metric('name')`
引用指標。編譯器會用指標嘅表達式取代每個調用，並記錄一條血緣邊（lineage edge）。呢樣令
手寫檢視喺引用指標而唔係複製其公式時，都享有同樣嘅變更即重新編譯特性。

注意：設定檔中使用內嵌 `metric()` 呼叫的檢視會在設定重新載入時重新產生，指標 upsert 不會令它們重新產生。

### Metrics 管理頁面 {: #the-metrics-admin-page }

打開 **Metrics** 導覽項目以管理受治理嘅指標。按一行以展開一個唯讀嘅詳情面板；喺面板入面
按 **Edit** 即可切換到內嵌編輯（冇彈出視窗）。**New Metric** 會喺表格上方打開一張內嵌建立
卡片。刪除確認係呢個頁面上唯一嘅彈出視窗。

建立／編輯表單為源自 fact 嘅指標提供咗一個三步選擇器建構工具：選擇來源 fact 表（已按
`modelingRole=fact` 篩選）、一個度量欄位，以及一個匯總函數（`SUM`、`AVG`、`COUNT`、`MIN`、
`MAX`）。datatype 會自動推導：`COUNT → bigint`、`AVG → numeric`、`SUM/MIN/MAX → the measure column's type`（度量欄位本身嘅類型）。
表達式文字區依然係任意表達式嘅逃生出口。

## IR 帶嚟嘅好處 {: #the-ir-payoff }

每次註冊呼叫都與人手編寫的 MV 走同一條路徑。entity 及 fact 規格是一種中間表示，不是範本，亦不是巨集。它面向哪個數據倉庫是部署的屬性，模型本身不指定。更換目標引擎後，同樣的 `entity` / `fact` 宣告會在那裏具體化，因為產生的 SQL 及雙時態歷史在設計上就與引擎無關。
