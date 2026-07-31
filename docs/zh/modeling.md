<!-- markdownlint-disable MD046 -->
<!-- MD046 off: mkdocs-material `===` content-tab bodies are indented, which the linter
     misreads as indented code blocks; the fenced code blocks below are required for rendering. -->

# 数据建模（Entities 与 Facts）

Provisa 提供两个声明式基元——`entity` 和 `fact`——涵盖了每个星型模式（star schema）和
Data Vault 赖以构建的所有构建块。声明规格，Provisa 会将其精确转换为具体化视图、双时态
（bitemporal）和关系定义，无需你手工编写（REQ-1164）。[tool-verified: modeling.py module
docstring lines 11-28]

## Entity 与 Fact 是什么

**entity** 是源关系的一个带键、去重、可选历史化的投影。你为其命名、指向一个数据源、
声明 entity key 和要保留的属性，并选择一种历史模式。Provisa 会编写视图的 SQL 并注册该
物化视图。启用历史时，该物化视图为双时态。[tool-verified: `Entity` dataclass, modeling.py
lines 53-69; `entity_registration` function, modeling.py lines 105-120]

**fact** 是对 entity key 的连接（join），归约到一个声明的粒度（grain），并带有聚合的
度量（measures）。Provisa 会编写一个聚合物化视图查询（按粒度 + FK 列的 `GROUP BY`），
并为每个声明的维度链接注册一个关系。没有度量的 fact 是一个纯键集合——即 Data Vault 的
link 模式。[tool-verified: `Fact` dataclass, modeling.py lines 91-102; `fact_registration`
function, modeling.py lines 123-141; comment at line 130 "a measureless fact is a pure
key-set (DV link)"]

这两种构造都是 IR（中间表示）。所生成的定义可以在不同引擎之间重新定向——在 Oracle、
Databricks 中物化，或者在 MPP 引擎之上保持虚拟——而无需重新建模。[tool-verified:
modeling.py docstring lines 25-28]

## 历史模式

entity 有三种可用模式 [tool-verified: `_HISTORY` constant at modeling.py line 38,
`_HISTORY_MODE` dict at modeling.py line 40]：

| 模式 | 含义 | 双时态模式 |
| --- | --- | --- |
| `none` | 仅当前状态，无历史。 | — |
| `scd2` | 追踪每次变更。仅追加变更的行（delta），以 entity key 为键。 | `delta` |
| `snapshot` | 追踪每次刷新。每次刷新都追加完整结果集，并加盖系统时间戳。 | `snapshot` |

`scd2` 需要一个 entity key 来计算 delta。`snapshot` 可在任何引擎上运行，但每次刷新的
存储空间都会增加一份完整副本。对大型、缓慢变化的数据源选用 `scd2`；当需要完整历史而
数据源无法提供键时，选用 `snapshot`。

fact 没有历史模式——时态覆盖范围由底层 entity 的历史提供。

## 度量与聚合

度量以 `column:agg` 对的形式声明。支持的聚合方式 [tool-verified: `_AGGS` at modeling.py
line 41]：

`sum` &nbsp;`avg` &nbsp;`min` &nbsp;`max` &nbsp;`count`

默认聚合方式为 `sum` [tool-verified: `Measure.agg` default at modeling.py line 75]。

## 实例演练：Customer entity + Sales fact

### 源表

- `raw.customers`——id、name、region、tier
- `raw.orders`——order_id、customer_id、amount、quantity

### 注册 Customer entity

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

Provisa 会生成并注册这个双时态物化视图 [tool-verified: `entity_registration` in
modeling.py lines 105-120]：

```sql
SELECT "id", "name", "region", "tier" FROM "raw"."customers"
-- registered as a bitemporal delta MV, entity key: ["id"]
```

### 注册 Sales fact

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

Provisa 会生成并注册 [tool-verified: `fact_registration` in modeling.py lines 123-141]：

```sql
SELECT "order_id", "customer_id",
       SUM("amount") AS "amount",
       SUM("quantity") AS "quantity"
FROM   "raw"."orders"
GROUP BY "order_id", "customer_id"
```

外加一个注册的关系：`Sales.customer_id → Customer`（基数：many-to-one）。
[tool-verified: `fact_table_input` in modeling_register.py lines 89-98, cardinality at
line 95]

## Model 表单（管理界面）

**+ Model** 按钮出现在 **Tables** 页面（提示："Model an entity or fact (star schema /
Data Vault)"）。[tool-verified: tablesPage.json line 13; TablesPage.tsx lines 441-450]

弹窗顶部的分段控件在 **Entity (dimension)** 和 **Fact** 之间切换。
[tool-verified: ModelingForm.tsx lines 102-110]

### Entity 字段

[tool-verified: ModelingForm.tsx lines 141-171; modelingForm.json]

| 字段 | 必填 | 说明 |
| --- | --- | --- |
| Name | 是 | 目录中的物化视图名称 |
| Source relation | 是 | 点分关系，例如 `raw.customers` |
| Domain | 是 | 物化视图所属的 domain |
| Entity key | 是 | 逗号分隔的键列，例如 `id` |
| Attributes | 否 | 逗号分隔的属性列，例如 `name, region, tier` |
| History | 否 | `none` / `scd2` / `snapshot`；默认为 `none` |

### Fact 字段

[tool-verified: ModelingForm.tsx lines 172-196; modelingForm.json]

| 字段 | 必填 | 说明 |
| --- | --- | --- |
| Name | 是 | 目录中的物化视图名称 |
| Source relation | 是 | 点分关系，例如 `raw.orders` |
| Domain | 是 | 物化视图所属的 domain |
| Grain | 是 | 逗号分隔的粒度列，例如 `order_id` |
| Measures | 否 | 逗号分隔的 `col:agg` 对，例如 `amount:sum, quantity:sum` |
| Dimensions | 否 | 逗号分隔的 `Entity:fk_column` 对，例如 `Customer:customer_id` |

当度量中省略 `agg`（写 `amount` 而非 `amount:sum`）时，默认值为 `sum`。
[tool-verified: ModelingForm.tsx line 73 `agg: agg || "sum"`]

## GraphQL API

两个 mutation 都位于管理 schema 中。[tool-verified: schema_mutation.py lines 449-472]

### `registerEntity`

```graphql
mutation RegisterEntity($input: EntityInput!) {
  registerEntity(input: $input) {
    success
    message
  }
}
```

`EntityInput` 字段 [tool-verified: types.py lines 449-456]：

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

`FactInput` 字段 [tool-verified: types.py lines 472-479]：

| 字段 | 类型 | 默认值 | 描述 |
| --- | --- | --- | --- |
| `name` | String | — | fact 物化视图在目录中的名称 |
| `source` | String | — | 数据源关系 |
| `domainId` | String | — | Domain id |
| `grain` | [String] | — | 用于 GROUP BY 的粒度列 |
| `measures` | [MeasureInput] | `[]` | `{ column, agg }` 对 |
| `dimensions` | [DimRefInput] | `[]` | `{ entity, via }` 对 |
| `visibleTo` | [String] | `["public"]` | 角色可见性列表 |

`MeasureInput`：`{ column: String, agg: String }`——agg 默认为 `"sum"`。
[tool-verified: types.py lines 460-462]

`DimRefInput`：`{ entity: String, via: String }`——`entity` 是被引用的 entity 名称；
`via` 是 fact 数据源上的 FK 列。
[tool-verified: types.py lines 465-468]

成功时，`registerFact` 返回如下形式的消息：
`Fact 'Sales' registered with 1 dimension link(s)`。
[tool-verified: schema_mutation.py line 471]

## Kimball 星型模式与 Data Vault

这两种模式都不需要独立的工具。同一组基元可以组合成两者。

### Kimball 星型模式

本演练构建一个三维度的星型模式。新增两张源表：

- `raw.products`——`product_id`、`name`、`category`、`list_price` [inferred: introduced
  for this example]
- `raw.date_spine`——`date_key`、`year`、`quarter`、`month` [inferred: introduced for this
  example]

此处 `raw.orders` 也新增了 `product_id` 和 `order_date` 列。[inferred]

#### 选择 SCD 类型

历史模式是 SCD Type 1 与 Type 2 之间唯一的开关：

| SCD 类型 | 历史模式 | 效果 |
| --- | --- | --- |
| Type 1（仅当前） | `none` | 每次刷新时重建物化视图；无行历史 |
| Type 2（版本化） | `scd2` | 双时态 delta 物化视图；每次变更都追加一行新记录，以 entity key 为键 |

[tool-verified: `_HISTORY_MODE` at modeling.py line 40; `entity_registration` history branch
at lines 115-119]

当下游查询需要按事务发生时的状态连接一个维度时——例如客户在购买那一刻的 tier，而非其
当前 tier——使用 `scd2`。对于稳定的查找表，使用 `none`。日期基准表（date spine）永远
不会变化。若产品目录只需要当前价格，则可以每次刷新时重建。

#### 粒度决策

粒度是 fact 所回答的最低细节层次。`order_id` 使每个订单对应一行，保留了统计唯一订单数、
以及在订单粒度上与任意维度连接的能力。更粗的粒度——例如 `["customer_id", "order_date"]`
——会在订单之间预先聚合，并永久丢弃该细节。声明业务所需的最窄粒度；更粗的汇总之后可以
低成本地派生出来。

#### 注册维度

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

生成一个以 `id` 为键的双时态 delta 物化视图 [tool-verified: entity_registration
modeling.py lines 105-120]：

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

生成一个每次刷新都重建的普通物化视图 [tool-verified: entity_registration modeling.py
lines 105-114; `mv_bitemporal_mode` is only added when `history != "none"`, line 115]：

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

#### 在三个维度上注册 Sales fact

粒度：`order_id`。三个维度引用——每个一个 FK 列。两个度量都是可加总的总和。

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
[tool-verified: fact_registration modeling.py lines 125-131]：

```sql
SELECT "order_id", "customer_id", "product_id", "order_date",
       SUM("amount")   AS "amount",
       SUM("quantity") AS "quantity"
FROM   "raw"."orders"
GROUP BY "order_id", "customer_id", "product_id", "order_date"
```

系统会自动注册三个关系 [tool-verified: modeling_register.py lines 89-98, cardinality
`"many_to_one"` at line 95]：

| 关系 | 基数 |
| --- | --- |
| `Sales.customer_id → Customer` | many-to-one |
| `Sales.product_id → Product` | many-to-one |
| `Sales.order_date → DateDim` | many-to-one |

#### 一致性维度（Conformed dimensions）

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
保证通过任一 fact 的查询都会经过同一个维度定义 [tool-verified: fact_registration uses
entity name as `target_table` at modeling.py lines 138-140; fact_table_input wires
`target_table_id` from that name at modeling_register.py lines 91-93]。

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

#### Hub

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

生成 [tool-verified: entity_registration modeling.py lines 115-119;
`_HISTORY_MODE["scd2"]` = `"delta"` at modeling.py line 40]：

```sql
SELECT "id", "name", "region", "tier" FROM "raw"."customers"
-- bitemporal delta MV, entity key: ["id"]
```

`CustomerSat` 和 `CustomerHub` 都以 `id` 为键。Hub 是稳定的连接目标；satellite 通过
双时态层提供某一时间点的属性访问。

#### Link（无度量的 fact）

Link 记录哪些 hub key 曾经共同出现——仅键，无度量。当 `measures` 为空时，Provisa 会
省略 `GROUP BY` [tool-verified: modeling.py lines 130-131: `if f.measures: view_sql +=
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
"customer_id"]`。无度量，因此无 `GROUP BY`。生成 [tool-verified: fact_registration
modeling.py lines 125-131]：

```sql
SELECT "order_id", "customer_id" FROM "raw"."orders"
```

系统会注册两个关系：`OrderCustomerLink.customer_id → CustomerHub` 和
`OrderCustomerLink.order_id → OrderHub`，两者均为 many-to-one
[tool-verified: modeling_register.py lines 89-98]。

#### Bridge / 聚合 link

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

`group_cols = dedup(["order_id"] + ["customer_id", "order_id"])` = `["order_id",
"customer_id"]`（维度列表中重复的 `order_id` 被 `_dedup` 剔除）。生成
[tool-verified: fact_registration modeling.py lines 125-131]：

```sql
SELECT "order_id", "customer_id", SUM("amount") AS "amount"
FROM   "raw"."orders"
GROUP BY "order_id", "customer_id"
```

模型本身不决定方法论。粒度、一致性、SCD 选择以及 hub/satellite 拆分仍然是建模者的
决定。Provisa 只负责执行。[tool-verified: modeling.py docstring lines 25-26]

## 指标（Metrics）（REQ-1317、REQ-1318、REQ-1320）

**指标**（metric）是一个具名、受治理的聚合定义，本身没有粒度。粒度——即聚合所按的
维度——在查询时由调用方绑定，而非在定义时绑定。这正是指标与视图的区别：视图在创建时
锁定粒度；指标在被查询之前始终保持开放。[tool-verified: `Metric` class comment,
`provisa/core/models.py` lines 452–455: "A named, governed aggregate definition with no
grain of its own... grain is bound at query time by the requested dimension set"]

### Metric 对象

[tool-verified: `Metric` class, `provisa/core/models.py` lines 451–476]

| 字段 | 必填 | 说明 |
| --- | --- | --- |
| `name` | 是 | snake_case，例如 `net_revenue`。校验规则：`[a-z][a-z0-9_]*` |
| `expression` | 是 | 聚合型 ANSI-SQL；必须包含至少一个聚合函数 |
| `datatype` | 否 | 结果类型提示，例如 `number`、`integer` |
| `description` | 否 | 人类可读的业务定义 |
| `ai_context` | 否 | 供 AI 消费方使用的文本——投影到 MCP 工具、pg_description、GraphQL 文档以及 Ossie 导出 |
| `visible_to` | 否 | 角色列表；默认为 `["*"]`（所有角色） |
| `from_fact` | — | 当指标由 fact 度量自动生成时自动设置 |

表达式中的列引用必须以表名限定（例如 `orders.amount`，而非 `amount`）。未限定的列在
展开时属于硬性错误，而非警告。
[tool-verified: `_expression_tables`, `provisa/compiler/metric_expand.py` lines 83–96]

指标仓库在每次写入时都会校验表达式。无法解析或不含任何聚合函数的表达式会被拒绝，
永远不会被存储。
[tool-verified: `validate_expression`, `provisa/core/repositories/metric.py` lines 34–43]

配置条目示例：

```yaml
metrics:
  - name: net_revenue
    expression: "SUM(orders.amount) - SUM(orders.refunds)"
    datatype: number
    description: "Order revenue after refunds"
    ai_context: "Net revenue: total order amounts minus approved refunds. Use for P&L."
```

### 查询一个指标

编译器保留了 `metrics` schema。[tool-verified: `METRICS_SCHEMA = "metrics"`,
`provisa/compiler/metric_expand.py` line 43] 每个指标都可以在该 schema 中以虚拟关系的
形式被寻址。可以像查询表一样查询它——所选的列成为维度集合和 GROUP BY：

```sql
-- Scalar total (no dimension)
SELECT value FROM metrics.net_revenue;

-- Broken out by region and month
SELECT region, month, value FROM metrics.net_revenue GROUP BY region, month;
```

编译器会在治理（governance）执行之前，将该查询重写为针对底层语义表的真正分组聚合，
使 RLS 和脱敏得以作用于真实列。
[tool-verified: `expand_metric_query` docstring, `provisa/compiler/metric_expand.py` lines
263–276: "BEFORE governance, so RLS/masking apply to the real columns (REQ-1317)"]

对指标关系使用 `SELECT *` 会被拒绝——需要显式指定维度列和 `value`。
[tool-verified: `expand_metric_query`, `provisa/compiler/metric_expand.py` lines 302–306]

当一个指标的表达式跨越多张表时，编译器会通过已注册的关系将它们连接起来。若一个维度是
被直接引用表的一列，则解析到该表。若一个维度距离一步关系之遥，则自动连接。两跳以上或
存在歧义的维度是一个会指明责任方的硬性错误。
[tool-verified: `_JoinPlan.resolve_dimension`, `provisa/compiler/metric_expand.py` lines
190–228]

### 由 fact 规格生成的指标（REQ-1320）

注册一个 fact 时，每个声明的度量都会自动注册一个对应的 Metric 对象。指标的
`from_fact` 字段记录来源 fact 表的名称，有效的分组维度是通过该 fact 的 FK 关系可达的
entity 属性。
[tool-verified: `Metric.from_fact` comment, `provisa/core/models.py` line 466–467: "set
when this metric was auto-registered from a fact spec's measure"; `from_fact` stored in
`provisa/core/repositories/metric.py` line 57]

自动注册的指标会在 Metrics 页面上显示一个 **fact** 徽章。可以像编辑其他指标一样编辑
它们。[tool-verified: `MetricsPage.tsx` lines 405–408: `{m.fromFact && <Badge ...
data-testid={`metrics-from-fact-${m.name}`}>...</Badge>}`]

### 由指标组成的视图（view_metrics，REQ-1318）

`view_metrics` 视图在定义时锁定一个指标的粒度。声明指标名称、维度列和可选的过滤条件；
编译器生成对应的 SELECT。

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

编译器（以本例而言）会生成：

```sql
SELECT orders.region AS region, orders.month AS month,
       SUM(orders.amount) - SUM(orders.refunds) AS net_revenue
FROM orders
WHERE orders.status = 'completed'
GROUP BY orders.region, orders.month
```

`view_metrics` 与 `view_sql` 在同一张表上互斥。
[tool-verified: `Table` model validator, `provisa/core/models.py` lines 614–617:
`if self.view_sql is not None and self.view_metrics is not None: raise ValueError(...)`]

**指标变更时的自动重新生成。** 当一个指标的表达式被更新时，每一个引用它的
`view_metrics` 视图都会重新编译，新 SQL 会立即被持久化。视图在结构上不可能与指标定义
出现偏差。
[tool-verified: `regenerate_metric_views`, `provisa/api/admin/_metric_views.py` lines
79–117: "each dependent view_metrics spec recompiles against the UPDATED metric set and the
fresh SQL is persisted"]

**手写视图 SQL 中的内联 `metric()` 调用。** 手写的 `view_sql` 也可以通过
`metric('name')` 引用指标。编译器会用指标的表达式替换每个调用，并记录一条血缘边
（lineage edge）。这使手写视图在引用指标而非复制其公式时，同样具有变更即重新编译的
特性。
[tool-verified: `expand_metric_calls_in_sql`, `provisa/compiler/metric_expand.py` lines
393–429]

注意：使用内联 `metric()` 调用的配置路径视图，会在配置重新加载时重新生成，而非在指标
upsert 时。[tool-verified: `regenerate_metric_views` docstring, `_metric_views.py` lines
84–86: "Free-hand view_sql born from inline metric() calls carries no stored provenance, so
it is not regenerated here (config-path views regenerate on config reload)"]

### Metrics 管理页面（REQ-1323、REQ-1324）

打开 **Metrics** 导航项以管理受治理的指标。点击一行以展开一个只读详情面板；在面板中
点击 **Edit** 即可切换到内联编辑（无弹窗）。**New Metric** 会在表格上方打开一张内联
创建卡片。删除确认是本页面上唯一的弹窗。
[tool-verified: `MetricsPage.tsx` lines 214–216 comment: "REQ-1317: registered-metrics
management page (list / create / edit / delete). REQ-1323: detail-then-edit"]

创建/编辑表单为来源于 fact 的指标提供了一个三步选择器构建器：选择来源 fact 表（已按
`modelingRole=fact` 过滤）、一个度量列，以及一个聚合函数（`SUM`、`AVG`、`COUNT`、
`MIN`、`MAX`）。数据类型自动推导：`COUNT → bigint`、`AVG → numeric`、`SUM/MIN/MAX →
度量列本身的类型`。表达式文本区仍是任意表达式的逃生出口。
[tool-verified: `deriveDatatype` function, `MetricsPage.tsx` lines 66–70;
`applyBuilder`, `MetricsPage.tsx` lines 273–285]

## IR 带来的收益

每次注册调用都经过与手写物化视图相同的路径。entity/fact 规格是一种中间表示——不是
模板，也不是宏。它所指向的数据仓库是部署的一个属性，而非模型的属性。更换目标引擎，
同一组 `entity` / `fact` 声明依然会在该引擎中物化，因为所生成的 SQL 和双时态模式在
结构上就是引擎中立的。[tool-verified: modeling.py docstring lines 25-28;
modeling_register.py lines 56-66, 80-88]
