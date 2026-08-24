# 列级血缘

Provisa 静态地跟踪列级数据血缘——由 SQL 定义和命令契约计算得出，无需执行。有两种视图可用：按语句的 DAG，以及横跨所有已注册视图和物化视图（MV）的联邦级溯源图。

## 血缘浏览器

在 UI 中导航到**血缘**（`/lineage`）。粘贴一条 SQL 语句并点击**构建语句图**，即可查看它的列级 DAG。点击**联邦图**可加载覆盖注册表中每个 MV 的溯源图。[tool-verified: LineagePage.tsx:28-119]

## 语句级 DAG（REQ-1160）

你的 SQL 中每个具名的输出列都会成为一个节点。构建器会穿过每一个 CTE、子查询、联接和内联命令调用，把它一路回溯到源列，构建出一张从源输入到最终输出的有向图。

### 实例演练

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

- `orders.id`、`orders.region` 和 `orders.geo` 是**源**节点（`enrich_grpc_set` 的窄输入契约声明了 `id` 和 `region`；完整的污点闭包把所有已声明的输入连到所有输出）。[tool-verified: `_splice_commands` in graph.py:223-242]
- `e.embedding` 和 `e.geo` 是**命令**节点——即 `enrich_grpc_set` 边界。
- `geo_u` 是由 `UPPER` 这个 SQL 函数产出的**派生**节点。

命令边界**并不是不透明的**。由于 `enrich_grpc_set` 声明了它的输入列（`id`、`region`）和输出列（`id`、`embedding`、`geo`），血缘引擎会把污点闭包从源关系已声明的列一直连续拼接到每一个输出。
[tool-verified: `_splice_commands` and `_input_relation` in graph.py:245-271]

### 节点种类与视觉提示

[tool-verified: LineageDag.tsx:25-29, KIND_COLOR constants; LineagePage.tsx:21-26 LEGEND]

| 节点种类 | 颜色 | 含义 |
| --- | --- | --- |
| `source` | 绿色 | 基表的一个列 |
| `derived` | 蓝色 | 由某个 SQL 表达式（函数、运算符、CTE）产出 |
| `command` | 紫色 | 来自某个已注册命令的输出列 |

节点上另有的圆环：

- **橙色圆环**——该语句的最终输出列。
- **双重边框**——该列所属的关系是一个物化视图（MV/CTAS 快照）。
- **红色圆环**——被归类为错误的环路的成员。
- **黄色圆环**——被归类为反馈回路的环路的成员。

[tool-verified: LineageDag.tsx:88-103 Cytoscape style selectors]

### 边上的具名变换

每条边都携带产出目标列的原始 SQL 表达式，外加一份具名操作清单：SQL 函数（`sql_function`）、算术/逻辑运算符（`operator`）、已注册的命令（`command`）、裸列引用（`identity`）以及字面量（`constant`）。
[tool-verified: TransformOp and name_transform in graph.py:36-145]

来自命令调用的边在 UI 中渲染为紫色虚线。
[tool-verified: LineageDag.tsx:122-124]

## 联邦级图（REQ-1161）

联邦图把每个已注册 MV 的按语句血缘合并成一张溯源图。节点身份是 `relation.column`——某个视图的输出列与另一个视图对同一个列的输入引用会坍缩成同一个节点。结果是一张从基础源列通往平台中每一个派生数据集的单一 DAG。[tool-verified: `build_federation_graph` in merge.py:205-229
and `qualify_outputs` in graph.py:275-299]

使用 `focus`、`direction` 和 `depth` 可以在联邦规模下限定视图范围，而无需重新计算整张图。[tool-verified: `slice_graph` in merge.py:160-189]

## 环路（REQ-1161）

环路会被描述，而不是被拒绝。血缘引擎检测出每一个有向环，并对它**分类**。[tool-verified: `Cycle.classification` property in merge.py:43-46]

| 分类 | 边框颜色 | 含义 |
| --- | --- | --- |
| `feedback` | 黄色 | 该环路穿过一个物化节点——是合法的、带时间滞后的反馈回路。MV 快照就是使它定义明确的版本边界。 |
| `error` | 红色 | 回路上没有物化边界——是一个没有稳定求值顺序的循环定义。很可能是设计错误。 |

[tool-verified: LineagePage.tsx:83-98 cycle alert rendering; merge.py:38-48]

`feedback` 环路不是故障。只要回路上有一个节点是物化的，一个把派生列反馈回自身源关系的数据增强 MV 就是有效的模式——快照在时间上把两半隔开。`error` 环路需要操作者判断：它通常意味着两个视图彼此引用，中间没有任何快照。

## API

两个终结点都是**静态**的——它们读取定义和契约，而不是数据。

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

[tool-verified: `lineage_graph` endpoint at lineage_router.py:45-54, LineageGraphRequest model at
lineage_router.py:29-31]

响应结构 [tool-verified: `LineageGraph.to_dict` in graph.py:82-105]：

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

当 SQL 无法解析时返回 HTTP 422。
[tool-verified: lineage_router.py:51-54]

### GET /admin/lineage/federation

返回注册表中所有 MV 合并后的溯源图。

```http
GET /admin/lineage/federation
GET /admin/lineage/federation?focus=orders.id&direction=downstream&depth=3
```

[tool-verified: `federation_graph` endpoint at lineage_router.py:73-98]

查询参数 [tool-verified: function signature at lineage_router.py:73-76]：

| 参数 | 取值 | 默认值 | 作用 |
| --- | --- | --- | --- |
| `focus` | 一个节点 id | — | 把响应限定为该节点周围的子图 |
| `direction` | `upstream` \| `downstream` \| `both` | `both` | 从 `focus` 出发的遍历方向 |
| `depth` | 整数 | 无上限 | 距 `focus` 的最大跳数 |

响应结构与语句图相同，另加一个 `cycles` 字段
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

## 重命名或删除一个列会打断什么（REQ-1484）

一个列带着两个名字，每一个都由不同的一组制品保存。

**对外名称**是 SQL 与 GraphQL 界面所展示的名称：`table_columns.alias`，未设置别名时回退到 snake_case 默认值 [tool-verified: `computed_sql_alias` at
`schema_helpers.py:317`]。视图、物化视图、指标表达式、RLS 谓词、DQ 契约、指标视图的粒度以及 MV 行键，全都是针对那个名称编写的，因此**重命名别名对它们的破坏程度，与删除该列一模一样**。

**物理名称**是 `table_columns.column_name`，它是那个能在表 upsert 的整体列替换中留存下来的身份。关系、[术语表](glossary.md)绑定、标签分配、水位列和列预设保存的是它，因此只有当该列被**移除**时它们才会被打断。

`columnDependents` 会同时报告这两者。下游视图和 MV 来自按该列的对外名称对联邦图做切片；该图未覆盖的制品则来自对注册表的直接扫描 [tool-verified: `graph_dependents` in `provisa/lineage/dependents.py`, registry scans in
`provisa/api/admin/column_dependents.py`]。

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

当一次待保存的编辑改变了别名或缩小了列集合时，表页面会自动运行该查询，并列出它的发现 [tool-verified: `diffEditedColumns` in
`provisa-ui/src/pages/tables/columnDiff.ts`, dialog in `TablesPage.tsx`]。该警告只是提示性的：它点名受影响的制品，由管理员作决定。它不会阻止保存，因为并非所有资产消费方都能被触达——一个从外部按名称查询该列的仪表板或客户端应用，超出了注册表的认知范围。出于同样的原因，对自由 SQL 文本的扫描是把该列当作标识符令牌来匹配，而不是解析作用域，因而可能点名一个其实并未使用该列的制品。对于警告而言，多报是安全的方向。

## 用血缘来治理命令契约

由于污点闭包把每一个已声明的输入列连到每一个已声明的输出列，闭包的宽度完全取决于你声明了什么。

设想有个命令，接收一张完整的 orders 表（`id`、`region`、`amount`、`customer_id`、`discount`、`notes`……）并返回一个 `embedding`。如果输入契约把这些列全都列上，那么每一个用到该 embedding 的下游列都会显示出来自所有这些列的血缘。那是准确的，但没什么用——很难看出真正起作用的是什么。

只声明 `id` 和 `text`（embedding 模型实际读取的列），血缘锥就会收紧到那两个源列上。这样的推导既可靠又精确。

声明窄输入契约的具体做法，参见[命令](commands.md)。
