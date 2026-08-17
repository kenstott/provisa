# 列级数据血缘

Provisa以静态方式追踪列级数据血缘——根据SQL定义和命令合约计算得出，无需执行。系统提供两种视图：单条语句的DAG，以及涵盖所有已注册视图和物化视图（MV）的联邦级溯源图。

## 血缘浏览器

在界面中导航至**Lineage**（`/lineage`）。粘贴一条SQL语句并点击**Build statement graph**，即可查看其列级DAG。点击**Federation graph**，可加载涵盖注册表中每个MV的溯源图。[tool-verified: LineagePage.tsx:28-119]

## 语句级DAG（REQ-1160）

SQL中每个具名输出列都会成为一个节点。构建器会沿着每个CTE、子查询、join及内联命令调用向上回溯，直至其来源列，从而构建出由来源输入到最终输出的有向图。

### 示例演算

```sql
SELECT o.id, e.embedding, upper(e.geo) AS geo_u
FROM   orders o
JOIN   enrich_grpc_set('main.public.orders') e ON o.id = e.id
```

该语句产生三个输出列。`geo_u`的图如下：

```text
orders.geo  ──[enrich_grpc_set(...)]──►  e.geo  ──[UPPER]──►  geo_u
orders.id   ─╮                                              (taint closure)
orders.region ─╯
```

- `orders.id`、`orders.region`和`orders.geo`是**source**节点（`enrich_grpc_set`的窄输入合约声明了`id`和`region`；完整的taint closure会将所有已声明的输入连接到所有输出）。[tool-verified: `_splice_commands` in graph.py:223-242]
- `e.embedding`和`e.geo`是**command**节点——即`enrich_grpc_set`的边界。
- `geo_u`是由`UPPER`这个SQL函数产生的**derived**节点。

命令边界**并非不透明**。由于`enrich_grpc_set`声明了其输入列（`id`、`region`）和输出列（`id`、`embedding`、`geo`），血缘引擎会将taint closure从来源关系已声明的列持续连接至每个输出。[tool-verified: `_splice_commands` and `_input_relation` in graph.py:245-271]

### 节点种类与视觉提示

[tool-verified: LineageDag.tsx:25-29, KIND_COLOR constants; LineagePage.tsx:21-26 LEGEND]

| 节点种类 | 颜色 | 含义 |
| --- | --- | --- |
| `source` | 绿色 | 基础表的一列 |
| `derived` | 蓝色 | 由SQL表达式（函数、运算符、CTE）产生 |
| `command` | 紫色 | 已注册命令的输出列 |

节点上的附加圆环：

- **橙色圆环**——该语句的最终输出列。
- **双边框**——该列所属的关系是物化视图（MV／CTAS快照）。
- **红色圆环**——被归类为错误的循环成员。
- **黄色圆环**——被归类为反馈回路的循环成员。

[tool-verified: LineageDag.tsx:88-103 Cytoscape style selectors]

### 边上的具名转换

每条边都携带产生目标列的原始SQL表达式，以及一份具名操作列表：SQL函数（`sql_function`）、算术／逻辑运算符（`operator`）、已注册命令（`command`）、纯列引用（`identity`）和字面量（`constant`）。[tool-verified: TransformOp and name_transform in graph.py:36-145]

来自命令调用的边，会在界面中以紫色虚线表示。[tool-verified: LineageDag.tsx:122-124]

## 联邦级图（REQ-1161）

联邦图将每个已注册MV的语句级血缘合并为单一溯源图。节点的身份为`relation.column`——某视图的输出列与另一视图对同一列的输入引用会合并为单一节点。结果是从基础来源列延伸至平台上每个衍生数据集的单一DAG。[tool-verified: `build_federation_graph` in merge.py:205-229 and `qualify_outputs` in graph.py:275-299]

可使用`focus`、`direction`和`depth`，在不重新计算图的情况下，于联邦规模上限定视图范围。[tool-verified: `slice_graph` in merge.py:160-189]

## 循环（REQ-1161）

循环会被描述，而非被拒绝。血缘引擎会检测每个有向循环并将其**分类**。[tool-verified: `Cycle.classification` property in merge.py:43-46]

| 分类 | 边框颜色 | 含义 |
| --- | --- | --- |
| `feedback` | 黄色 | 该循环经过一个物化节点——属合法、具时间延迟的反馈回路。MV快照即是使其成为良定义的版本边界。 |
| `error` | 红色 | 该回路上没有物化边界——属无稳定求值顺序的循环定义，很可能是设计错误。 |

[tool-verified: LineagePage.tsx:83-98 cycle alert rendering; merge.py:38-48]

`feedback`循环并非失败。若增强型MV将衍生列反馈至其自身的来源关系，只要回路上有一个节点已物化，这便是有效模式——快照会在时间上将两部分隔开。`error`循环则需要运维人员判断：通常意味着两个视图相互引用，中间没有快照。

## API

两个端点均为**静态**——只读取定义和合约，不读取数据。

### POST /admin/lineage/graph

返回单条SQL语句的列级DAG。

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

响应结构[tool-verified: `LineageGraph.to_dict` in graph.py:82-105]：

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

当SQL无法解析时，返回HTTP 422。
[tool-verified: lineage_router.py:51-54]

### GET /admin/lineage/federation

返回注册表中所有MV的合并溯源图。

```http
GET /admin/lineage/federation
GET /admin/lineage/federation?focus=orders.id&direction=downstream&depth=3
```

[tool-verified: `federation_graph` endpoint at lineage_router.py:73-98]

查询参数[tool-verified: function signature at lineage_router.py:73-76]：

| 参数 | 取值 | 默认值 | 效果 |
| --- | --- | --- | --- |
| `focus` | 节点ID | — | 将响应限定于该节点周围的子图 |
| `direction` | `upstream` \| `downstream` \| `both` | `both` | 从`focus`出发的遍历方向 |
| `depth` | 整数 | 无限制 | 距`focus`的最大跳数 |

响应结构与语句图相同，并附加一个`cycles`字段
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

## 列重命名或删除会破坏什么（REQ-1484）

一列拥有两个名称，且每个名称由不同的一组构件存储。

**暴露名称**是SQL和GraphQL界面所展示的名称：`table_columns.alias`，若未设置别名则回退到snake_case默认名称[tool-verified: `computed_sql_alias` at
`schema_helpers.py:317`]。视图、物化视图、指标表达式、RLS谓词、DQ合约、指标视图粒度和MV行键都是针对该名称编写的，因此**重命名别名与删除该列一样，同样会破坏它们**。

**物理名称**是`table_columns.column_name`，即在表upsert整体列替换后仍然保留的身份标识。关系、术语表绑定、标签分配、水位列和列预设都存储这一个名称，因此只有在列被**移除**时它们才会被破坏。

`columnDependents`会同时报告两者。下游视图和MV的信息来自在该列的暴露名称处切分联邦图；该图未覆盖的构件则来自对注册表的直接扫描[tool-verified: `graph_dependents` in `provisa/lineage/dependents.py`, registry scans in
`provisa/api/admin/column_dependents.py`]。

```graphql
query {
  columnDependents(tableId: "42", renamed: ["order_total"], removed: ["legacy_code"]) {
    columnName
    dependents { kind name detail breaksOn }
  }
}
```

对于引用暴露名称的构件，`breaksOn`为`rename`；对于引用物理名称的构件，则为`remove`，因此调用方可以判断每个构件是对这次编辑的哪一半作出反应。

请在保存**之前**提出此查询。已重命名的列是通过它在注册表中仍然携带的暴露名称来定位的；一旦别名已经落地，旧名称便不复存在，查询将一无所获。

当一次待处理的编辑更改了别名或缩减了列集时，Tables页面会自动运行该查询，并列出其发现的结果[tool-verified: `diffEditedColumns` in
`provisa-ui/src/pages/tables/columnDiff.ts`, dialog in `TablesPage.tsx`]。该警告仅供参考：它列出受影响的构件，由管理员自行决定。它不会阻止保存，因为该资产的全部消费方无法尽数触达——注册表之外的仪表板或按名称查询该列的客户端应用，均超出了注册表的知晓范围。出于同样的原因，对自由SQL文本的扫描是将该列作为标识符token进行匹配，而非解析作用域，因此可能会指出某个实际上并未使用该列的构件。对于警告而言，宁可多报也不要漏报。

## 利用血缘治理命令合约

由于taint closure会将每个已声明的输入列连接到每个已声明的输出列，该closure的广度完全取决于你声明了什么。

试想一个命令，接收完整的`orders`表（`id`、`region`、`amount`、`customer_id`、`discount`、`notes`等），并返回一个`embedding`。若输入合约列出全部这些列，则每个使用该embedding的下游列都会显示来自全部这些列的血缘。这虽然准确，却并不实用——难以判断实际上是什么真正起了作用。

只声明`id`和`text`（即embedding模型实际读取的列），血缘范围便会收窄至这两个来源列。这样得出的推导既严谨又精确。

有关声明窄输入合约的机制，请参阅[Commands](commands.md)。
