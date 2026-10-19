# Cypher 查询支持 {: #cypher-query-support }

Provisa 通过 `provisa/cypher/` 模块将 openCypher 的一个子集转换为 SQL。查询由一个自定义的递归下降解析器解析（不依赖外部 Cypher 库），针对模型进行架构解析，生成 SQL，然后路由到目标执行引擎。

## 已实现的特性 {: #implemented-features }

### 子句 {: #clauses }

| 子句 | 状态 | 说明 |
| -------- | -------- | ------- |
| `MATCH (n:Label)` | ✓ | 带标签、变量、内联属性的节点模式 |
| `OPTIONAL MATCH` | ✓ | 生成 LEFT JOIN |
| `WHERE` | ✓ | 完整的表达式支持；在 MATCH 之后应用 |
| `RETURN` | ✓ | 星号、属性访问、表达式、别名 |
| `RETURN DISTINCT` | ✓ | 生成 SELECT DISTINCT |
| `WITH` | ✓ | 生成一个命名 CTE（`_w0`、`_w1`……）；支持 `WITH … WHERE` |
| `ORDER BY` | ✓ | ASC / DESC |
| `SKIP` / `LIMIT` | ✓ | 映射到 SQL OFFSET / LIMIT |
| `UNION` / `UNION ALL` | ✓ | 跨子 AST 的递归联合 |
| `CALL { … }` | ✓ | 通过 `cypher_calls_to_sql_list` 进行顶层 call 子查询分解 |
| `CALL { WITH x … }` | ✓ | 相关子查询 → `CROSS JOIN LATERAL`；参见 §相关 CALL |
| `CALL db.labels()` | ✓ | 从模型返回节点标签；无 SQL 转换 |
| `CALL db.relationshipTypes()` | ✓ | 从模型返回关系类型 |
| `CALL db.propertyKeys()` | ✓ | 返回所有节点类型下的全部属性键名称 |
| `UNWIND` | ✓ | 数组转行展开；第一项成为 FROM，后续项成为 CROSS JOIN UNNEST |

### 匹配模式 {: #match-patterns }

| 模式 | 状态 | 说明 |
| --------- | -------- | ------- |
| `(n)` — 无标签节点 | ✓ | 对所有已知类型的 UNION ALL |
| `(n:Label)` | ✓ | 映射到该 GraphQL 类型对应的已注册表 |
| `(n:Label {prop: val})` | ✓ | 内联属性过滤转为 WHERE |
| `(a)-[:TYPE]->(b)` | ✓ | 有向，单跳 |
| `(a)<-[:TYPE]-(b)` | ✓ | 反向遍历；联结列反转 |
| `(a)-[]->(b)` | ✓ | 任意 a→b 方向的关系；若匹配多个类型则为 UNION ALL |
| `(a)-[]-(b)` | ✓ | 双向；展开为所有正向和反向关系的 UNION ALL |
| `(a)-[:TYPE*..N]->(b)` | ✓ | 带上界的变长路径；自引用情形用递归 CTE，否则用扁平 JOIN |
| `(a)-[]->(b)-[]->(c)` | ✓ | 多跳链式 JOIN |
| `(n:DomainLabel)` | ✓ | 域标签 → 针对该域下所有类型的 UNION ALL 子查询 |
| `(n:A\|B)` | ✓ | 标签交替 → 临时域注入标签映射；对匹配类型做 UNION ALL |
| `shortestPath(…)` | ✓ | 异构端点用扁平 JOIN；同类型/自引用用 WITH RECURSIVE CTE |
| `allShortestPaths(…)` | ✓ | 与 shortestPath 相同，但不带 LIMIT 1 |

### 表达式与谓词 {: #expressions-and-predicates }

| 特性 | 状态 | SQL 映射 |
| --------- | -------- | ------------ |
| 属性访问 `n.prop` | ✓ | `n."prop"` |
| 参数 `$name` | ✓ | 位置化的 `$N` |
| 旧式参数 `{name}` | ✓ | 解析时规范化为 `$name` |
| 比较 `=`、`<>`、`<`、`>`、`<=`、`>=` | ✓ | 直接映射 |
| `AND`、`OR`、`NOT` | ✓ | 直接映射 |
| `IS NULL` / `IS NOT NULL` | ✓ | 直接映射 |
| `IN [list]` | ✓ | SQL IN；Cypher 的 `[...]` 方括号语法重写为 `(...)` |
| `STARTS WITH` | ✓ | `starts_with(col, val)` |
| `ENDS WITH` | ✓ | `col LIKE CONCAT('%', val)` |
| `CONTAINS` | ✓ | `strpos(col, val) > 0` |
| `=~` 正则 | ✓ | `regexp_like(col, pattern)` |
| `exists(n.prop)` | ✓ | `(n.prop) IS NOT NULL` |
| `EXISTS { MATCH … }` | ✓ | 相关的 `EXISTS (SELECT 1 FROM …)` 子查询 |
| `COUNT { MATCH … }` | ✓ | 相关的 `(SELECT count(*) FROM …)` 子查询 |
| `COLLECT { MATCH … RETURN x }` | ✓ | 相关的 `ARRAY(SELECT x FROM …)` 子查询 |
| `id(n)` | ✓ | 解析为该节点配置的 ID 列 |
| `labels(n)` | ✓ | `ARRAY['Label']` |
| `keys(n)` | ✓ | `ARRAY['prop1', 'prop2', …]` |
| `type(r)` | ✓ | 编译期解析为 `'REL_TYPE'` 字符串字面量；无运行时列 |
| `length(p)` | ✓ | 递归 CTE 路径为 `_t.hops`；扁平 JOIN 路径为 `1` |
| `CASE WHEN … THEN … ELSE … END` | ✓ | 直接映射（搜索式和简单式两种形式） |
| 隐式 GROUP BY | ✓ | 当任一项包含聚合函数时，未聚合的 RETURN 项成为 GROUP BY 键 |

### 映射投影 {: #map-projections }

| 语法 | SQL 映射 |
| -------- | ------------ |
| `n { .prop1, .prop2 }` | `MAP(ARRAY['prop1','prop2'], ARRAY[n."prop1",n."prop2"])` |
| `n { .* }` | `MAP(ARRAY[all props...], ARRAY[n."col",...])` —— 从架构展开 |
| `n { .*, extra: expr }` | 全部架构属性加上命名键；合并后的 MAP |
| `n { key: expr }` | `MAP(ARRAY['key'], ARRAY[expr])` |

**对象属性。** 持有已声明对象的列（远程 GraphQL 类型的嵌入对象，或任何带对象字段注册的列）是映射属性：`RETURN n.address` 以映射返回它，无论引擎以何种形式返回，节点 `n` 也在其属性中携带它。指向已注册类型的远程 GraphQL 字段（一个对象或一个列表）不是属性；通过其关系到达。

### 聚合函数 {: #aggregation-functions }

| Cypher | SQL |
| -------- | ----- |
| `count(*)`、`count(x)` | 直接映射 |
| `count(DISTINCT x)` | `count(DISTINCT x)` |
| `collect(x)` | `array_agg(x)` |
| `avg`、`sum`、`min`、`max` | 直接映射 |
| `stDev(x)` | `stddev_samp(x)` |
| `stDevP(x)` | `stddev_pop(x)` |
| `percentileCont(x, p)` | `approx_percentile(x, p)` |
| `percentileDisc(x, p)` | `approx_percentile(x, p)` |

### 字符串函数 {: #string-functions }

| Cypher | SQL |
| -------- | ----- |
| `toLower(x)` | `lower(x)` |
| `toUpper(x)` | `upper(x)` |
| `ltrim(x)`、`rtrim(x)`、`trim(x)` | 直接映射 |
| `replace(x, a, b)` | 直接映射 |
| `reverse(x)` | 直接映射 |
| `split(x, d)` | 直接映射 |
| `left(x, n)` | `left(x, n)` |
| `right(x, n)` | `right(x, n)` |
| `substring(x, start, len)` | `substr(x, start+1, len)`（0→1 索引） |
| `size(string)` | `char_length(string)` |
| `size(list)` | `cardinality(list)` |

### 类型转换函数 {: #type-conversion-functions }

| Cypher | SQL |
| -------- | ----- |
| `toString(x)` | `CAST(x AS VARCHAR)` |
| `toInteger(x)` | `TRY_CAST(x AS BIGINT)` |
| `toFloat(x)` | `TRY_CAST(x AS DOUBLE)` |
| `toBoolean(x)` | `TRY_CAST(x AS BOOLEAN)` |
| `toStringOrNull`、`toIntegerOrNull`、`toFloatOrNull`、`toBooleanOrNull` | `TRY_CAST` 变体 |

### 数学函数 {: #math-functions }

| Cypher | SQL |
| -------- | ----- |
| `log(x)` | `ln(x)`（自然对数） |
| `log2(x)` | `log2(x)` |
| `range(start, end)` | `sequence(start, end)` |
| `abs`、`sqrt`、`ceil`、`floor`、`round`、`sign` | 直接透传 |

### 列表函数 {: #list-functions }

| Cypher | SQL |
| -------- | ----- |
| `head(list)` | `element_at(list, 1)` |
| `last(list)` | `element_at(list, -1)` |
| `tail(list)` | `slice(list, 2, cardinality(list))` |
| `isEmpty(list)` | `cardinality(list) = 0` |

### 列表推导式 {: #list-comprehensions }

| 语法 | SQL 映射 |
| -------- | ------------ |
| `[x IN list \| f(x)]` | `transform(list, x -> f(x))` |
| `[x IN list WHERE p(x)]` | `filter(list, x -> p(x))` |
| `[x IN list WHERE p(x) \| f(x)]` | `transform(filter(list, x -> p(x)), x -> f(x))` |
| `any(x IN list WHERE p(x))` | `any_match(list, x -> p(x))` |
| `all(x IN list WHERE p(x))` | `all_match(list, x -> p(x))` |
| `none(x IN list WHERE p(x))` | `none_match(list, x -> p(x))` |
| `single(x IN list WHERE p(x))` | `cardinality(filter(list, x -> p(x))) = 1` |
| `reduce(acc = init, x IN list \| expr)` | `reduce(list, init, (acc, x) -> expr, acc -> acc)` |

### 模式推导式 {: #pattern-comprehensions }

| 语法 | SQL 映射 |
| -------- | ------------ |
| `[(a)-[:R]->(b) \| b.prop]` | `ARRAY(SELECT b."prop" FROM ... WHERE a.fk = b.pk)` |
| `[(a)-[]->(b:Label) \| b.prop]` | 从模型推断类型；相同的 ARRAY 子查询形式 |

### 相关 CALL 子查询 {: #correlated-call-subqueries }

`CALL { WITH x MATCH (x)-[:R]->(n) RETURN n.prop AS alias }` 转换为 `CROSS JOIN LATERAL (SELECT n."prop" AS alias FROM ... WHERE x."pk" = n."fk")`。规则：

- 外层作用域变量（`x`）必须出现在 `WITH` 中
- 支持多个导入变量（`WITH a, b`）
- 内层 MATCH 中第一个源为横向绑定变量的关系，决定了内层的 `FROM` 和联结条件
- 不带 `WITH` 的非相关顶层 `CALL { ... }` 块由 `cypher_calls_to_sql_list` 处理

---

## 写入 {: #writes }

Cypher 通过 `/data/cypher` 端点支持三种写入模式，由 `provisa/cypher/write_translator.py` 执行。

| Cypher | SQL |
|---|---|
| `CREATE (n:Label {props})` | `INSERT INTO catalog.schema.table (cols) VALUES (vals)` |
| `MATCH (n:Label) WHERE … DELETE n` | `DELETE FROM catalog.schema.table WHERE …` |
| `MATCH (n:Label) WHERE … SET n.prop = val, …` | `UPDATE catalog.schema.table SET col = val, … WHERE …` |

属性名称通过域前缀剥离和别名解析映射到列；Cypher 标量值会被强制转换为目标列类型。响应体携带 `affected_rows` 计数。

节点的属性是其列的发布名称：列有别名时用别名，否则用命名约定下的名称——即 GraphQL 和 SQL 读取它所用的名称——在 `MATCH`、`RETURN`、`CREATE`、`SET` 和 `MERGE` 中一致。

规则：

- 标签必须精确解析到一张已注册的表。歧义或未知标签是硬性错误；不进行模糊匹配。无法通过 Cypher 创建新标签或新类型。
- 每次写入都受目标表写入授权的门控：角色需要 `write` 权利和该操作的授权，且写入的列必须在授权中。不满足的角色会在编译期被拒绝。
- 后端数据源连接器必须支持 DML。只读数据源（Trino 联邦、没有 Delta 连接器的 Iceberg）会在转换时拒绝写入。
- 关系无法被写入——它们派生自模型中声明的联结，而非存储的边。以关系为目标是硬性错误。由联结表支撑的边也不例外：其背后的关联表本身就是一张已注册的表，写入的行进入那张表，而不是进入边。
- 写入会经过完整的写入流水线：RLS 注入和变更后钩子（响应缓存失效、物化视图过期标记、Kafka 变更事件、热表重新加载）。
- `MERGE`、`DETACH DELETE` 和 `REMOVE` 不受支持，会在解析时被拒绝。

---

## 协议访问 {: #protocol-access }

Cypher 通过两种传输方式访问同一个受治理的流水线：

- **HTTP** —— `POST /data/cypher`，携带一个 JSON 请求体（`{"query": "...", "params": {...}}`）。返回类型化的行数据，写入操作则返回 `affected_rows`。`RETURN` 子句中的图变量序列化为 JSON：节点携带 `id`、`label`、`tableLabel` 和 `properties`；边携带 `identity`、`start`、`end`、`type`、`properties`、`startNode` 和 `endNode`；路径携带 `nodes`、`edges` 和 `length`/`hops`。已注册的命令也可以在这里通过 `CALL fn(args) YIELD col1, col2` 调用——位置参数按顺序映射到该命令声明的参数名称。
- **Bolt** —— 一个与 Neo4j 兼容的二进制协议服务器（PackStream 编解码、分块成帧），使 Neo4j Browser、Bloom 以及各类 Bolt 驱动能够对联邦图运行 Cypher。当 `PROVISA_BOLT_PORT` 被设置为非零值时启动，默认禁用；设置 `PROVISA_BOLT_CERT` / `PROVISA_BOLT_KEY` 以启用 TLS。 Bolt 认证将主体映射到用户，将数据库映射到角色：`SHOW DATABASES` 为每个（视图 × 角色）组合列出一个条目，命名为 `provisa_<role>`（业务域）或 `provisa_ops_<role>`（带 system/meta/ops 域）；`:use` 选择活动的角色和视图。关系通过 `rel_ids` 表获得持久的整数 ID，与 `node_ids` 的设计相仿。已注册的命令可以通过 `CALL command(args)` 调用——位置参数按顺序映射到声明的参数名称；`CALL dbms.*` / `CALL db.*` 过程优先。

### 图分析 {: #graph-analytics }

`POST /data/graph-analytics` 运行一条 Cypher 查询，从结果节点和边构建一个内存中的 NetworkX 图，执行一个命名算法，并在返回为 JSON（带 `elapsed_ms` 字段）之前，将一个 `_analytics` 字典合并进每个节点和边。`_analytics` 的键因算法而异：中心性得出 `score`；社区检测得出 `cluster`；k-核得出 `core_number`；度中心性还会添加 `in_degree` 和 `out_degree`。该端点会以 HTTP 413 拒绝超过可配置规模（默认 10,000 个节点 / 50,000 条边）的图；Girvan-Newman 算法上限为 500 个节点，除非调用方传入 `force=true`。

---

## 限制 {: #limitations }

### 设计约束 {: #design-constraints }

1. **写入仅限于 `CREATE`、`SET` 和 `DELETE`。** 它们通过与 GraphQL 和 SQL 变更操作相同的流水线执行为直接的表写入。参见下文 §写入。`MERGE`、`DETACH DELETE` 和 `REMOVE` 会在解析时被拒绝。APOC 过程同样会被拒绝。

2. **关系属性只存在于由联结表支撑的边上。** 基于一对外键列声明的边，在模型中只作为连接元数据存在，不存储任何属性，所以 `WHERE r.since > 2020` 或 `RETURN r.weight` 对它没有意义。基于联结表声明的边则带有属性。关联表的其余列就是该关系的属性，`RETURN r` 会返回它们。对其中某个属性的 `WHERE` 会编译为联结表别名上的谓词，因此它约束的是遍历本身，而不是在组装后过滤行。联结表会从图模式的节点一侧去掉：它在这里是边，在其他地方都是表。

3. **双向遍历** `(a)-[]-(b)` 会改写为模型中所有匹配的有向关系的正向与反向 UNION ALL。模型中的每个关系都有方向，双向语法只是展开为两个方向的语法糖。额外的分支在查询的最外层生成。同一查询中后续的 MATCH 模式不会在各分支中复制，这限制了含多个 MATCH 子句的查询中的双向模式。

4. **递归路径需要一个上界。** 变长模式（`[*]`）必须包含上界（例如 `[*..10]`）。无界遍历会在解析时被拒绝，以防止失控的递归 CTE。

### 行为说明 {: #behaviour-notes }

5. **非自引用路径上的 `shortestPath` 使用扁平 JOIN，不按跳数排序。** 当起点与终点类型不同、且模式中不存在自引用关系时，转换器会沿最短的模式路径生成一条扁平 JOIN 链。该代码路径不跟踪跳数，所以不会生成 `ORDER BY hops`。结果是结构上最短的模式路径，它可能不同于跨多行数据的最短路径。

6. **多条架构路径会产生 `UNION ALL`。** 当两条跳数相同的架构路径连接相同的起点和终点类型时（例如 `Person -[WORKS_AT]-> Company` 和 `Person -[MANAGES]-> Company`），两者都会作为 `UNION ALL` 分支生成。不会对同时出现在两个分支中的重复行进行去重。

7. **每个 源→目标 类型对与 rel_type 组合只对应一个 `RelationshipMapping`。** 如果同一源类型上的两个 GraphQL 字段生成了相同的 `rel_type` 字符串（大写化后）指向相同的目标类型，第二次注册会覆盖 `CypherLabelMap.relationships` 中的第一次。关系键包含源和目标类型名称，因此类型名称相同但源/目标不同的关系对各自拥有独立条目，不受影响。

8. **`WITH` 子句 CTE 命名为 `_w0`、`_w1`……**名称在单次转换调用内按位置分配。若朴素地拼接多个已转换的查询（例如批处理场景），可能产生冲突的 CTE 名称。

### 表达式与模式覆盖范围 {: #expression-and-pattern-coverage }

Cypher 表达式被解析为 AST，并逐节点下推为 SQL（`provisa/cypher/expr_parser.py`、`provisa/cypher/expr_visitor.py`）。该语法遵循 openCypher 的 `oC_Expression` 优先级体系。已支持：字面量、参数、属性访问、`n.prop`、索引与切片、算术运算（`+ - * / % ^`）、比较、`IN`、`STARTS WITH` / `ENDS WITH` / `CONTAINS` / `=~`、`IS [NOT] NULL`、布尔 `AND` / `OR` / `XOR` / `NOT`、`CASE`、列表与映射字面量、列表与模式推导式（包括 `p = (…)` 路径绑定）、映射投影、`reduce`、`all` / `any` / `none` / `single` 量词、存在性子查询，以及函数调用。

9. **标签是固定的；您无法通过 Cypher 创建对象类型。** 标签会解析为已知领域、已知对象类型或限定形式的 `domain:object_type`，也就是已注册模式定义的封闭集合。Cypher 从不引入新的标签或类型。您只能为可写数据源中已定义的类型创建实例：`CREATE` 会向这类表写入行（见 §Writes），但不能定义新的标签或类型。两种标签形式都可接受，表示同一个判断：后缀形式 `n:Label` 和完整形式 `n IS :Label`（及其否定 `n IS NOT :Label`）。限定标签写作 `n:domain:object_type`。

10. **`shortestPath` 和 `allShortestPaths` 只能在 `MATCH` 内部使用。** 在模式中（`MATCH p = shortestPath((a:Person)-[:KNOWS*..5]->(b:Person))`），它们会转换为 `WITH RECURSIVE` CTE，并要求起点和终点节点带标签。它们不支持出现在表达式位置，例如 `RETURN shortestPath((a)-[*]->(b))` 或 `WHERE length(shortestPath((a)-[*]->(b))) < 5`。递归改写由 `MATCH` 子句驱动，没有相关子查询可以承载它。

11. **列表推导式、`REDUCE` 和量词作用于列表值；模式推导式则进行遍历。** `reduce(...)`、`all/any/none/single(...)` 以及列表推导式 `[x IN list | …]` 作用于列表表达式，并下降为引擎的高阶列表函数。它们本身不遍历图。**模式**推导式 `[(a)-[:R]->(b) WHERE p | e]` 则会遍历：它的图模式按相关子查询处理，所以它的来源就是一次遍历。要把遍历结果送入列表形式，请使用 `nodes(p)` / `relationships(p)` / `collect(...)`，或直接使用模式推导式。
