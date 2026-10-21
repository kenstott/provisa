# 列级血缘 {: #column-level-lineage }

Provisa 以静态方式追踪列级数据血缘。它根据 SQL 定义和命令契约计算血缘，所以无需执行任何东西。你可以看到两种视图：每条语句的 DAG，以及覆盖所有已注册视图和物化视图（MV）的联邦级溯源图。

## 血缘浏览器 {: #the-lineage-explorer }

在 UI 中导航到**血缘**（`/lineage`）。粘贴一条 SQL 语句并点击**构建语句图**，即可查看它的列级 DAG。点击**联邦图**可加载覆盖注册表中每个 MV 的溯源图。

## 语句级 DAG {: #statement-level-dag }

你的 SQL 中每个具名的输出列都会成为一个节点。构建器会穿过每一个 CTE、子查询、联接和内联命令调用，把它一路回溯到源列，构建出一张从源输入到最终输出的有向图。

### 实例演练 {: #worked-example }

```sql
SELECT o.id, e.embedding, upper(e.geo) AS geo_u
FROM   orders o
JOIN   enrich_grpc_set('main.public.orders') e ON o.id = e.id
```

这条语句产出三个输出列。`geo_u` 的图形如下：

```text
orders.geo  ──[enrich_grpc_set(...)]──►  e.geo  ──[UPPER]──►  geo_u
orders.id   ─╮                                              (taint closure)
orders.region ─╯
```

- `orders.id`、`orders.region` 和 `orders.geo` 是**源**节点（`enrich_grpc_set` 的窄输入契约声明了 `id` 和 `region`；完整的污点闭包把所有已声明的输入连到所有输出）。
- `e.embedding` 和 `e.geo` 是位于 `enrich_grpc_set` 边界上的**命令**节点。
- `geo_u` 是由 `UPPER` 这个 SQL 函数产出的**派生**节点。

命令边界**并不是不透明的**。由于 `enrich_grpc_set` 声明了它的输入列（`id`、`region`）和输出列（`id`、`embedding`、`geo`），血缘引擎会把污点闭包从源关系已声明的列一直连续拼接到每一个输出。

### 节点种类与视觉提示 {: #node-kinds-and-visual-cues }

| 节点种类 | 颜色 | 含义 |
| --- | --- | --- |
| `source` | 绿色 | 基表的一个列 |
| `derived` | 蓝色 | 由某个 SQL 表达式（函数、运算符、CTE）产出 |
| `command` | 紫色 | 来自某个已注册命令的输出列 |
| `native` | 橙色 | 数据源读取的原始字段，位于已注册表的列之下 |

节点上另有的圆环：

- **橙色圆环**——该语句的最终输出列。
- **双重边框**——该列所属的关系是一个物化视图（MV/CTAS 快照）。
- **红色圆环**——被归类为错误的环路的成员。
- **黄色圆环**——被归类为反馈回路的环路的成员。

### 边上的具名变换 {: #named-transforms-on-edges }

每条边都携带产出目标列的原始 SQL 表达式，外加一份具名操作清单：SQL 函数（`sql_function`）、算术/逻辑运算符（`operator`）、已注册的命令（`command`）、裸列引用（`identity`）以及字面量（`constant`）。

来自命令调用的边在 UI 中渲染为紫色虚线。

### 已注册表之下

拥有数据源注册权限的用户会在每个源列之下再看到一跳：数据源读取时的字段，放在一个代表其读取位置的框中。该框带有数据源的徽标和类型标签（与 Sources 页面显示的一致），以及按数据源类型给出的地址：

| 数据源 | 地址 |
| --- | --- |
| OpenAPI | 操作（`GET /v2/customers`）、其固定参数和行的位置 |
| Remote GraphQL | 根字段、其必需参数和行的位置 |
| MongoDB、Cosmos DB | 数据库以及集合或容器 |
| Elasticsearch、Qdrant | 索引或集合 |
| CSV、Parquet、文件、日志 | 路径，以及读取多个文件的表的文件通配模式 |
| Kafka | 主题 |
| 代码数据源 | 函数 |
| 其他任何数据源 | 原生的模式和表 |

原始字段在列有字段路径时就是该路径，否则就是列自己的名称。从它到已注册列的边会标明重命名方式：`path`、`alias` 或 `naming`（命名约定）。已注册表的框说明它如何提供服务：`virtual`，在每条语句时读取；或 `replica`，附带其 TTL 和上次加载时间。凭据、密钥引用以及 URL 中的用户名、密码和 query 从不显示。没有数据源注册权限的用户只能看到从已注册表向上的血缘。

### 链接

当用户的权限能打开相应页面时，每个元素都会在其定义处打开。单击字段会打开它的表、视图或数据源；双击框会打开它的关系；单击命令的边会打开该命令。内置 SQL 函数没有页面，也没有链接。

## 联邦级图 {: #federation-wide-graph }

联邦图把每个已注册 MV 的逐语句血缘合并成一张溯源图。节点的标识是 `relation.column`，所以一个视图的输出列与另一个视图对该列的输入引用会合并为同一个节点。结果是一张单一的 DAG，从基础源列一直延伸到平台中每个派生数据集。

使用 `focus`、`direction` 和 `depth` 可以在联邦规模下限定视图范围，而无需重新计算整张图。

## 环路 {: #cycles }

血缘引擎会检测每一个有向环并对其进行**分类**。环只会被描述，从不会被拒绝。

| 分类 | 边框颜色 | 含义 |
| --- | --- | --- |
| `feedback` | 黄色 | 该环路穿过一个物化节点——是合法的、带时间滞后的反馈回路。MV 快照就是使它定义明确的版本边界。 |
| `error` | 红色 | 回路上没有物化边界——是一个没有稳定求值顺序的循环定义。很可能是设计错误。 |

`feedback` 环是一种有效的模式。把派生列回写到自身源关系的增强 MV，只要环上有一个节点是物化的，就能正常工作，因为快照在时间上把两半分隔开。`error` 环需要运维人员判断，它通常意味着两个视图相互引用，中间没有快照。

## API

两个端点都是**静态**的。它们读取定义和契约，从不读取数据。

### POST /admin/lineage/graph

返回单条 SQL 语句的列级 DAG。

```http
POST /admin/lineage/graph
Content-Type: application/json

{
  "sql": "SELECT o.id, e.embedding FROM orders o JOIN enrich_grpc_set('main.public.orders') e ON o.id = e.id",
  "dialect": "postgres"
}
```

响应结构：

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

对于拥有数据源注册权限的调用方，两个端点都会加上原生这一跳：类型为 `native` 的节点，以及一个以每个原生框的关系（`native:<domain>.<table>`）为键的 `natives` 对象：

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

副本的 `served` 为 `{"mode": "replica", "cache_ttl": 3600, "last_load": "...", "next_load": "..."}`。对于其他调用方，`natives` 为空。

当 SQL 无法解析时返回 HTTP 422。

### GET /admin/lineage/federation

返回注册表中所有 MV 合并后的溯源图。

```http
GET /admin/lineage/federation
GET /admin/lineage/federation?focus=orders.id&direction=downstream&depth=3
```

查询参数：

| 参数 | 取值 | 默认值 | 作用 |
| --- | --- | --- | --- |
| `focus` | 一个节点 id | — | 把响应限定为该节点周围的子图 |
| `direction` | `upstream` \| `downstream` \| `both` | `both` | 从 `focus` 出发的遍历方向 |
| `depth` | 整数 | 无上限 | 距 `focus` 的最大跳数 |

响应结构与语句图相同，另加一个 `cycles` 字段
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

## 每次刷新由什么构建 {: #what-each-refresh-was-built-from }

物化视图的每次刷新都会记录它由什么构建：视图定义的版本、其输入的版本以及该版本是如何确定的，以及每个输出列的源列和表达式。运维视图 `"ops"."mv_refresh_lineage"` 每次刷新返回一行：

| 列 | 内容 |
|---|---|
| `mv_id` | 被刷新的物化视图 |
| `target_table` | 刷新写入的表 |
| `definition_version` | 视图定义的版本 |
| `input_version`, `input_version_kind` | 其输入的版本：Iceberg 快照、水位线、新鲜度令牌或刷新时间 |
| `trace_id` | 该刷新在 `"ops"."traces"` 中的追踪 |
| `refreshed_at` | 运行时间 |
| `column_derivations` | 每个输出列及其源列和表达式，以 JSON 表示 |

```sql
SELECT refreshed_at, definition_version, input_version, input_version_kind
FROM "ops"."mv_refresh_lineage"
WHERE mv_id = 'view-view_exposure'
ORDER BY refreshed_at
```

该视图读取 `"ops"."traces"` 中的刷新记录，并在保留这些记录的每个引擎上作答。在 Trino 上，它们是 `otel` 目录的 Iceberg 表。在 DuckDB 上，服务器把自身的追踪记录以 Parquet 文件写入其遥测文件夹（`PROVISA_TELEMETRY_DIR`，默认 `~/.provisa/telemetry`），每个导出批次一个文件，每天一个文件夹。定时的遥测压缩（`otel.compact_cron`）会把每天的小文件合并为最大 64 MB 的文件。每个组织只读取自己的刷新，记录无限期保留。只有在启用 OpenTelemetry SDK 时（`OTEL_SDK_DISABLED` 未设置或为 false）才会记录 span。

## 视图当时的样子，以及当时记录的样子 {: #a-view-as-it-stood-and-as-it-was-recorded }

声明为双时态的视图会保留它有过的每个版本：每次刷新都是追加，任何版本都不会被改写，在任何存储上都是如此。普通读取返回当前状态。两个请求头用于读取它的过去：

- `X-Provisa-As-Of` 按视图的时钟读取某个时间点或某个区间的视图：视图声明了 `bitemporal_source_time` 时用数据源自身的变更时间，未声明时用 Provisa 的加载时间。
- `X-Provisa-As-Recorded` 按 Provisa 在某个时间点所记录的样子读取视图。该时间之后记录的版本不计入，因此之后记录的更正不会改变对过去的重放。

截至 9 月 30 日、按 10 月 1 日的记录读取得到原始数字；按 10 月 5 日的记录读取得到更正后的数字。每条语句的审计记录都保存这两个时间（`as_of`、`as_recorded`），因此报表可以按其首次生成时的样子重现。[带历史的视图](configuration.md#views-with-history-time-travel)介绍如何声明双时态视图，以及接受这两个值的每个接口。

## 重命名或删除一个列会打断什么 {: #what-a-column-rename-or-drop-would-break }

一个列带着两个名字，每一个都由不同的一组制品保存。

**对外名称**是 SQL 与 GraphQL 界面所展示的名称：`table_columns.alias`，未设置别名时回退到 snake_case 默认值。视图、物化视图、指标表达式、RLS 谓词、DQ 契约、指标视图的粒度以及 MV 行键，全都是针对那个名称编写的，因此**重命名别名对它们的破坏程度，与删除该列一模一样**。

**物理名称**是 `table_columns.column_name`，它是那个能在表 upsert 的整体列替换中留存下来的身份。关系、[术语表](glossary.md)绑定、标签分配、水位列和列预设保存的是它，因此只有当该列被**移除**时它们才会被打断。

`columnDependents` 会同时报告这两者。下游视图和 MV 来自按该列的对外名称对联邦图做切片；该图未覆盖的制品则来自对注册表的直接扫描。

```graphql
query {
  columnDependents(tableId: "42", renamed: ["order_total"], removed: ["legacy_code"]) {
    columnName
    dependents { kind name detail breaksOn }
  }
}
```

对于对外名称的引用，`breaksOn` 是 `rename`；对于物理名称的引用则是 `remove`，因此调用方能分辨出每个制品是在对这次编辑的哪一半作出反应。

要在保存**之前**问这个问题。被重命名的列是靠它在注册表中仍然携带的对外名称定位的；一旦别名落地，旧名字就没了，查询也就什么都找不到。

当待保存的编辑更改了别名或缩减了列集合时，“表”页面会自动运行该查询并列出结果。该警告仅供参考：它列出受影响的制品，由管理员决定。它从不阻止保存，因为无法触及所有使用方：按名称查询该列的外部仪表板或客户端应用，不在注册表的掌握之内。出于同样的原因，对自由 SQL 文本的扫描只把列当作标识符记号来匹配，并不解析作用域，因此可能列出实际上并未使用该列的制品。对于警告来说，多报才是安全的方向。

## 用血缘来治理命令契约 {: #using-lineage-to-govern-command-contracts }

由于污点闭包把每一个已声明的输入列连到每一个已声明的输出列，闭包的宽度完全取决于你声明了什么。

设想一个命令，它接收完整的订单表（`id`、`region`、`amount`、`customer_id`、`discount`、`notes`……）并返回一个 `embedding`。如果输入契约列出了所有这些列，那么下游每个使用该嵌入的列都会显示来自所有这些列的血缘。这是准确的，却没有帮助，因为你看不出真正起作用的是哪些列。

只声明 `id` 和 `text`（embedding 模型实际读取的列），血缘锥就会收紧到那两个源列上。这样的推导既可靠又精确。

声明窄输入契约的具体做法，参见[命令](commands.md)。
