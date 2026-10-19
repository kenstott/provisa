<!-- markdownlint-disable MD046 -->
<!-- MD046 off: mkdocs-material `===` content-tab bodies are indented, which the linter
     misreads as indented code blocks; the fenced code blocks below are required for rendering. -->

# 数据建模（Entities 与 Facts） {: #data-modeling-entities-facts }

Provisa 提供两个声明式原语 `entity` 和 `fact`，涵盖了构成每个星型模型和每个 Data Vault 的基本构件。声明规格，Provisa 就会把它精确转换为您原本需要手写的物化视图、双时态和关系定义。

## Entity 与 Fact 是什么 {: #what-entities-and-facts-are }

**entity** 是源关系的一个带键、去重、可选历史化的投影。你为其命名、指向一个数据源、
声明 entity key 和要保留的属性，并选择一种历史模式。Provisa 会编写视图的 SQL 并注册该
物化视图。启用历史时，该物化视图为双时态。

**fact** 是一个连到实体键的联接，归约到声明的粒度，并带有聚合度量。Provisa 会写出一个聚合 MV 查询（`GROUP BY` 粒度 + 外键列），并为每个声明的维度链接注册一个关系。没有度量的 fact 是纯键集合，也就是 Data Vault 的 link 模式。

这两种构造都是 IR。生成的定义可以在引擎之间迁移而无需重新建模：在 Oracle 或 Databricks 中物化，或在 MPP 引擎上保持虚拟。

## 历史模式 {: #history-modes }

entity 有三种可用模式：

| 模式 | 含义 | 双时态模式 |
| --- | --- | --- |
| `none` | 仅当前状态，无历史。 | — |
| `scd2` | 追踪每次变更。仅追加变更的行（delta），以 entity key 为键。 | `delta` |
| `snapshot` | 追踪每次刷新。每次刷新都追加完整结果集，并加盖系统时间戳。 | `snapshot` |

`scd2` 需要一个 entity key 来计算 delta。`snapshot` 可在任何引擎上运行，但每次刷新的
存储空间都会增加一份完整副本。对大型、缓慢变化的数据源选用 `scd2`；当需要完整历史而
数据源无法提供键时，选用 `snapshot`。

fact 没有自己的历史模式；它们在时间上的覆盖来自下层实体的历史。

## 度量与聚合 {: #measures-and-aggregations }

度量以 `column:agg` 对的形式声明。支持的聚合方式：

`sum` &nbsp;`avg` &nbsp;`min` &nbsp;`max` &nbsp;`count`

默认聚合方式为 `sum`。

## 实例演练：Customer entity + Sales fact {: #worked-example-customer-entity-sales-fact }

### 源表 {: #the-source-tables }

- `raw.customers`——id、name、region、tier
- `raw.orders`——order_id、customer_id、amount、quantity

### 注册 Customer entity {: #register-the-customer-entity }

=== "Admin UI"

    1. 打开 **Tables**，点击 **+ Model**。
    2. 选择 **Entity (dimension)**。
    3. 填写表单：
       - **Name：** `Customer`
       - **Source relation：** `raw.customers`
       - **Domain：** *（你的 domain）*
       - **Entity key：** `id`
       - **Attributes：** `name, region, tier`
       - **History：** `SCD2 (track changes — delta bitemporal)`
    4. 点击 **Create**。

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

Provisa 会生成并注册这个双时态物化视图：

```sql
SELECT "id", "name", "region", "tier" FROM "raw"."customers"
-- registered as a bitemporal delta MV, entity key: ["id"]
```

### 注册 Sales fact {: #register-the-sales-fact }

=== "Admin UI"

    1. 再次点击 **+ Model**。
    2. 选择 **Fact**。
    3. 填写表单：
       - **Name：** `Sales`
       - **Source relation：** `raw.orders`
       - **Domain：** *（你的 domain）*
       - **Grain：** `order_id`
       - **Measures：** `amount:sum, quantity:sum`
       - **Dimensions：** `Customer:customer_id`
    4. 点击 **Create**。

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

Provisa 会生成并注册：

```sql
SELECT "order_id", "customer_id",
       SUM("amount") AS "amount",
       SUM("quantity") AS "quantity"
FROM   "raw"."orders"
GROUP BY "order_id", "customer_id"
```

外加一个注册的关系：`Sales.customer_id → Customer`（基数：many-to-one）。

## Model 表单（管理界面） {: #the-model-form-admin-ui }

**+ Model** 按钮出现在 **Tables** 页面（提示："Model an entity or fact (star schema /
Data Vault)"）。

弹窗顶部的分段控件在 **Entity (dimension)** 和 **Fact** 之间切换。

### Entity 字段 {: #entity-fields }

| 字段 | 必填 | 说明 |
| --- | --- | --- |
| Name | 是 | 目录中的物化视图名称 |
| Source relation | 是 | 点分关系，例如 `raw.customers` |
| Domain | 是 | 物化视图所属的 domain |
| Entity key | 是 | 逗号分隔的键列，例如 `id` |
| Attributes | 否 | 逗号分隔的属性列，例如 `name, region, tier` |
| History | 否 | `none` / `scd2` / `snapshot`；默认为 `none` |

### Fact 字段 {: #fact-fields }

| 字段 | 必填 | 说明 |
| --- | --- | --- |
| Name | 是 | 目录中的物化视图名称 |
| Source relation | 是 | 点分关系，例如 `raw.orders` |
| Domain | 是 | 物化视图所属的 domain |
| Grain | 是 | 逗号分隔的粒度列，例如 `order_id` |
| Measures | 否 | 逗号分隔的 `col:agg` 对，例如 `amount:sum, quantity:sum` |
| Dimensions | 否 | 逗号分隔的 `Entity:fk_column` 对，例如 `Customer:customer_id` |

当度量中省略 `agg`（写 `amount` 而非 `amount:sum`）时，默认值为 `sum`。

## GraphQL API {: #the-graphql-api }

两个 mutation 都位于管理 schema 中。

### `registerEntity`

```graphql
mutation RegisterEntity($input: EntityInput!) {
  registerEntity(input: $input) {
    success
    message
  }
}
```

`EntityInput` 字段：

| 字段 | 类型 | 默认值 | 描述 |
| --- | --- | --- | --- |
| `name` | String | — | entity 物化视图在目录中的名称 |
| `source` | String | — | 数据源关系（`schema.table` 或加引号） |
| `domainId` | String | — | Domain id |
| `key` | [String] | — | Entity key 列 |
| `attributes` | [String] | `[]` | 要投影的属性列 |
| `history` | String | `"none"` | `"none"` \| `"scd2"` \| `"snapshot"` |
| `visibleTo` | [String] | `["public"]` | 角色可见性列表 |

### `registerFact`

```graphql
mutation RegisterFact($input: FactInput!) {
  registerFact(input: $input) {
    success
    message
  }
}
```

`FactInput` 字段：

| 字段 | 类型 | 默认值 | 描述 |
| --- | --- | --- | --- |
| `name` | String | — | fact 物化视图在目录中的名称 |
| `source` | String | — | 数据源关系 |
| `domainId` | String | — | Domain id |
| `grain` | [String] | — | 用于 GROUP BY 的粒度列 |
| `measures` | [MeasureInput] | `[]` | `{ column, agg }` 对 |
| `dimensions` | [DimRefInput] | `[]` | `{ entity, via }` 对 |
| `visibleTo` | [String] | `["public"]` | 角色可见性列表 |

`MeasureInput`：`{ column: String, agg: String }`；agg 默认为 `"sum"`。

`DimRefInput`：`{ entity: String, via: String }`；`entity` 是被引用的 entity 名称； `via` 是 fact 数据源上的 FK 列。

成功时，`registerFact` 返回如下形式的消息：
`Fact 'Sales' registered with 1 dimension link(s)`。

## Kimball 星型模式与 Data Vault {: #kimball-star-schema-and-data-vault }

这两种模式都不需要独立的工具。同一组基元可以组合成两者。

### Kimball 星型模式 {: #kimball-star-schema }

本演练构建一个三维度的星型模式。新增两张源表：

- `raw.products`——`product_id`、`name`、`category`、`list_price`
- `raw.date_spine`——`date_key`、`year`、`quarter`、`month`

此处 `raw.orders` 也新增了 `product_id` 和 `order_date` 列。

#### 选择 SCD 类型 {: #choosing-scd-type }

历史模式是 SCD Type 1 与 Type 2 之间唯一的开关：

| SCD 类型 | 历史模式 | 效果 |
| --- | --- | --- |
| Type 1（仅当前） | `none` | 每次刷新时重建物化视图；无行历史 |
| Type 2（版本化） | `scd2` | 双时态 delta 物化视图；每次变更都追加一行新记录，以 entity key 为键 |

当下游查询需要按交易发生时的状态联接维度时，使用 `scd2`，例如购买时客户所在的等级，而不是当前等级。稳定的查找表用 `none`。日期轴永远不变；只需要当前价格的产品目录可以在每次刷新时重建。

#### 粒度决策 {: #grain-decision }

粒度是 fact 所能回答的最细明细层级。`order_id` 表示每个订单一行，保留了统计不同订单数、以及在订单层级联接任意维度的能力。更粗的粒度，例如 `["customer_id", "order_date"]`，会在订单之间预先聚合，并永久丢掉这些明细。请声明业务所需的最细粒度；更粗的汇总以后很容易推导。

#### 注册维度 {: #register-the-dimensions }

**Customer**（SCD Type 2——tier 变更必须保留）：

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

生成一个以 `id` 为键的双时态 delta 物化视图：

```sql
SELECT "id", "name", "region", "tier" FROM "raw"."customers"
-- bitemporal delta MV, entity key: ["id"]
```

**Product**（SCD Type 1——当前目录，不需要版本历史）：

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

生成一个每次刷新都重建的普通物化视图：

```sql
SELECT "product_id", "name", "category", "list_price" FROM "raw"."products"
```

**DateDim**（无历史——日期是不可变的）：

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

#### 在三个维度上注册 Sales fact {: #register-the-sales-fact-across-three-dimensions }

粒度：`order_id`。三个维度引用，每个各有一个外键列。两个度量都是可加的求和。

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

Provisa 计算 `group_cols = dedup([grain] + [dim FKs])`
= `["order_id", "customer_id", "product_id", "order_date"]`，并生成
：

```sql
SELECT "order_id", "customer_id", "product_id", "order_date",
       SUM("amount")   AS "amount",
       SUM("quantity") AS "quantity"
FROM   "raw"."orders"
GROUP BY "order_id", "customer_id", "product_id", "order_date"
```

系统会自动注册三个关系：

| 关系 | 基数 |
| --- | --- |
| `Sales.customer_id → Customer` | many-to-one |
| `Sales.product_id → Product` | many-to-one |
| `Sales.order_date → DateDim` | many-to-one |

#### 一致性维度（Conformed dimensions） {: #conformed-dimensions }

一致性维度只需注册一次，之后可以在任意数量的 fact 中按名称引用。假设 `raw.returns`
包含 `return_id`、`customer_id`、`product_id` 和 `amount`。Returns fact 直接复用
Customer 和 Product，无需重新注册：

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

`Sales` 和 `Returns` 都指向同一组 `Customer` 和 `Product` entity。Provisa 的连接路径
保证通过任一 fact 的查询都会经过同一个维度定义。

---

### Data Vault

同一组基元可以直接映射到 Data Vault 词汇：

| DV 构件 | 基元 | 历史 |
| --- | --- | --- |
| Hub | `entity` | `none`——仅 entity key |
| Satellite | `entity` | `scd2` 或 `snapshot`——hub key 旁的属性历史 |
| Link | 无度量的 `fact` | — |
| Bridge / 聚合 link | 有度量的 `fact` | — |

本例在 `raw.customers` 和 `raw.orders` 之上构建一个最小化的 vault。

#### Hub {: #hubs }

Hub 只包含 entity key，不含其他内容。`attributes: []` 配合 `history: "none"` 会产生
一个去重的当前键集合；属性历史完全保存在 satellite 中。

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

Satellite 位于 hub key 旁边，携带完整的属性历史。使用 `scd2` 仅追加变更的行；使用
`snapshot` 为每次完整刷新加盖时间戳。

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

生成：

```sql
SELECT "id", "name", "region", "tier" FROM "raw"."customers"
-- bitemporal delta MV, entity key: ["id"]
```

`CustomerSat` 和 `CustomerHub` 都以 `id` 为键。Hub 是稳定的连接目标；satellite 通过
双时态层提供某一时间点的属性访问。

#### Link（无度量的 fact） {: #link-measureless-fact }

link 记录哪些 hub 键一起出现过。它只保存键，没有度量；`measures` 为空时，Provisa 会省略 `GROUP BY`。

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
无度量，因此无 `GROUP BY`。生成：

```sql
SELECT "order_id", "customer_id" FROM "raw"."orders"
```

系统会注册两个关系：`OrderCustomerLink.customer_id → CustomerHub` 和
`OrderCustomerLink.order_id → OrderHub`，两者均为 many-to-one
。

#### Bridge / 聚合 link {: #bridge-aggregate-link }

为 link 添加度量后，Provisa 会发出 `GROUP BY`，生成一个预聚合的 bridge。在 `order_id`
粒度下、每个订单一个客户的情况下，结果是每个订单一行聚合记录：

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
（维度列表中重复的 `order_id` 被 `_dedup` 剔除）。生成
：

```sql
SELECT "order_id", "customer_id", SUM("amount") AS "amount"
FROM   "raw"."orders"
GROUP BY "order_id", "customer_id"
```

模型本身不决定方法论。粒度、一致性、SCD 选择以及 hub/satellite 拆分仍然是建模者的
决定。Provisa 只负责执行。

## 指标（Metrics） {: #metrics }

**指标**是一个具名、受治理的聚合定义，本身没有粒度。粒度，即聚合按哪些维度拆分，由调用方在查询时绑定，而不是在定义时确定。这正是指标与视图的区别：视图在创建时就固定了粒度，指标则在被查询之前一直保持开放。

### Metric 对象 {: #the-metric-object }

| 字段 | 必填 | 说明 |
| --- | --- | --- |
| `name` | 是 | snake_case，例如 `net_revenue`。校验规则：`[a-z][a-z0-9_]*` |
| `expression` | 是 | 聚合型 ANSI-SQL；必须包含至少一个聚合函数 |
| `datatype` | 否 | 结果类型提示，例如 `number`、`integer` |
| `description` | 否 | 人类可读的业务定义 |
| `ai_context` | 否 | 供 AI 消费方使用的文本——投影到 MCP 工具、pg_description、GraphQL 文档以及 Ossie 导出 |
| `visible_to` | 否 | 角色列表；默认为 `["*"]`（所有角色） |
| `from_fact` | — | 当指标由 fact 度量自动生成时自动设置 |

表达式中的列引用必须带上表名限定（`orders.amount`，而不只是 `amount`）。未限定的列在展开指标时会直接报错，不会只给警告就放行。

指标仓库在每次写入时都会校验表达式。无法解析或不含任何聚合函数的表达式会被拒绝，
永远不会被存储。

配置条目示例：

```yaml
metrics:
  - name: net_revenue
    expression: "SUM(orders.amount) - SUM(orders.refunds)"
    datatype: number
    description: "Order revenue after refunds"
    ai_context: "Net revenue: total order amounts minus approved refunds. Use for P&L."
```

### 查询一个指标 {: #querying-a-metric }

编译器保留了 `metrics` 模式，每个指标都可以作为其中的虚拟关系来访问。像查询表一样查询它，您选择的列就成为维度集合和 GROUP BY：

```sql
-- Scalar total (no dimension)
SELECT value FROM metrics.net_revenue;

-- Broken out by region and month
SELECT region, month, value FROM metrics.net_revenue GROUP BY region, month;
```

编译器会在治理（governance）执行之前，将该查询重写为针对底层语义表的真正分组聚合，
使 RLS 和脱敏得以作用于真实列。

对指标关系执行 `SELECT *` 会被拒绝。请明确列出维度列和 `value`。

当一个指标的表达式跨越多张表时，编译器会通过已注册的关系将它们连接起来。若一个维度是
被直接引用表的一列，则解析到该表。若一个维度距离一步关系之遥，则自动连接。两跳以上或
存在歧义的维度是一个会指明责任方的硬性错误。

### 由 fact 规格生成的指标 {: #metrics-from-fact-specs }

注册一个 fact 时，每个声明的度量都会自动注册一个对应的 Metric 对象。指标的
`from_fact` 字段记录来源 fact 表的名称，有效的分组维度是通过该 fact 的 FK 关系可达的
entity 属性。

自动注册的指标会在 Metrics 页面上显示一个 **fact** 徽章。可以像编辑其他指标一样编辑
它们。

### 由指标组成的视图（view_metrics） {: #metric-composed-views-view_metrics }

`view_metrics` 视图在定义时锁定一个指标的粒度。声明指标名称、维度列和可选的过滤条件；
编译器生成对应的 SELECT。

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

编译器（以本例而言）会生成：

```sql
SELECT orders.region AS region, orders.month AS month,
       SUM(orders.amount) - SUM(orders.refunds) AS net_revenue
FROM orders
WHERE orders.status = 'completed'
GROUP BY orders.region, orders.month
```

`view_metrics` 与 `view_sql` 在同一张表上互斥。

**指标变更时的自动重新生成。** 当一个指标的表达式被更新时，每一个引用它的
`view_metrics` 视图都会重新编译，新 SQL 会立即被持久化。视图在结构上不可能与指标定义
出现偏差。

**手写视图 SQL 中的内联 `metric()` 调用。** 手写的 `view_sql` 也可以通过
`metric('name')` 引用指标。编译器会用指标的表达式替换每个调用，并记录一条血缘边
（lineage edge）。这使手写视图在引用指标而非复制其公式时，同样具有变更即重新编译的
特性。

注意：配置文件中使用内联 `metric()` 调用的视图会在配置重新加载时重新生成，指标 upsert 不会让它们重新生成。

### Metrics 管理页面 {: #the-metrics-admin-page }

打开 **Metrics** 导航项以管理受治理的指标。点击一行以展开一个只读详情面板；在面板中
点击 **Edit** 即可切换到内联编辑（无弹窗）。**New Metric** 会在表格上方打开一张内联
创建卡片。删除确认是本页面上唯一的弹窗。

创建/编辑表单为来源于 fact 的指标提供了一个三步选择器构建器：选择来源 fact 表（已按
`modelingRole=fact` 过滤）、一个度量列，以及一个聚合函数（`SUM`、`AVG`、`COUNT`、
`MIN`、`MAX`）。数据类型自动推导：`COUNT → bigint`、`AVG → numeric`、`SUM/MIN/MAX → the measure column's type`（度量列本身的类型）。
表达式文本区仍是任意表达式的逃生出口。

## IR 带来的收益 {: #the-ir-payoff }

每次注册调用都与手写的 MV 走同一条路径。entity 和 fact 规格是一种中间表示，不是模板，也不是宏。它面向哪个数仓是部署的属性，模型本身不指定。更换目标引擎后，同样的 `entity` / `fact` 声明会在那里物化，因为生成的 SQL 和双时态模式在设计上就与引擎无关。
