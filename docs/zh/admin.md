# Admin API

Admin API 是一个 Strawberry GraphQL 终结点，位于 `POST /admin/graphql`（REQ-533）。它需要超级用户或管理员角色（REQ-125、REQ-060），并且与数据 GraphQL 终结点是分开的（REQ-533）。

## 身份验证

使用 Provisa 的标准身份验证提供程序（REQ-120），在 `Authorization` 标头中传递您的凭据：

```yaml
Authorization: Bearer <token>
```

管理员访问权限由分配给角色的 `admin` 功能所控制（REQ-060、REQ-042）。

### 个人访问令牌

凡是接受 bearer 令牌的地方都接受个人访问令牌，本端点也不例外。签发与吊销均为自助操作——它是持有者的私有凭据，因此位于管理界面的用户资料页而非管理员页面，与退出组织、删除账户并列。管理员不代他人签发令牌。（REQ-1263）

| 路由 | 作用 |
| ------- | -------- |
| `POST /auth/tokens` | 为调用者签发一个令牌。请求体：`name`，以及可选的 `role_id`、`scopes`、`expires_in_days`（1–366）。响应是密钥唯一一次出现的地方 |
| `GET /auth/tokens` | 调用者在本组织中的有效令牌——显示前缀、名称、生命周期时间戳，以及用于吊销的令牌标识哈希。绝不返回可用凭据 |
| `DELETE /auth/tokens/{token_hash}` | 吊销调用者的某个令牌。若不属于调用者或已被吊销则返回 404 |

省略 `role_id` 时，令牌解析为其所有者持有的角色；指定角色则把令牌收窄到低于其所有者。吊销也会隐式发生：移除用户在某组织中的成员资格会吊销其针对该组织的所有令牌。凭据本身参见[安全模型](security.md#_17)。

## 功能

### 配置管理

下载当前正在运行的配置（REQ-164）：

```http
GET /admin/config
```

以 YAML 文件形式返回完整的 `config.yaml`。上传新配置（REQ-164）：

```http
PUT /admin/config
```

Provisa 会验证 YAML、重新加载目录，并重新生成架构 (Schema)（REQ-012、REQ-253）。无需重启。

### 运行时设置

在不编辑配置文件的情况下读取和写入运行时平台设置（REQ-165）：

```http
GET  /admin/settings
PUT  /admin/settings
```

设置范围涵盖大结果重定向、默认采样和行数限制、响应缓存的 TTL、命名约定、关联外键的自动追踪、物化存储的 DSN、联邦引擎内存（`jvm_heap_gb`、`query_max_memory`、`query_max_memory_per_node`、`query_max_total_memory`、`fault_tolerant_execution`、`fault_tolerant_task_memory`、`exchange_spool_dir`），以及整个 OpenTelemetry 追踪管道的调优范围（REQ-1082）。远程 GraphQL 遍历限制以及暖层/读取缓存设置也已提供（REQ-1081、REQ-1083）。

安全态势 — `security.mode`（`standard` | `high`）— 在重启时生效（REQ-1079）：

```http
GET  /admin/security
PUT  /admin/security
```

AI 模型分配、嵌入/向量模型注册表，以及自然语言速率限制 — 在重启时生效（REQ-1080）：

```http
GET  /admin/ai-models
PUT  /admin/ai-models
```

管理员加密选项卡会实时从加密注册表获取其提供程序列表；不可用的提供程序会显示，但不可选择（REQ-1091）。

`GET`/`HEAD /health` 和 `GET /setup/status` 始终无需身份验证 — 即使已配置身份验证提供程序，它们也会绕过 `Authorization: Bearer` 的要求（REQ-539）。

### 关联编辑器

列出关联（REQ-166）：

```graphql
query {
  relationships {
    id
    sourceTableId
    targetTableId
    sourceColumn
    targetColumn
    cardinality
    materialize
  }
}
```

创建关联（REQ-019）：

```graphql
mutation {
  upsertRelationship(input: {
    id: "orders-to-customers"
    sourceTableId: "orders"
    targetTableId: "customers"
    sourceColumn: "customer_id"
    targetColumn: "id"
    cardinality: "many_to_one"
  }) {
    success
  }
}
```

### AI 关联发现

通过 REST 触发由 Claude 提供支持的外键分析（REQ-167、REQ-018）：

```bash
curl -X POST http://localhost:8001/admin/discover/relationships \
  -H "Content-Type: application/json" \
  -d '{"scope": "domain", "domain_id": "sales"}'
```

返回按置信度排序的外键候选项。接受候选项：

```bash
curl -X POST http://localhost:8001/admin/discover/candidates/{id}/accept \
  -H "Content-Type: application/json" \
  -d '{"name": "orders_to_customers"}'
```

### 架构内省

浏览所有数据源中已发布的表（REQ-008）：

```graphql
query {
  tables {
    id
    sourceId
    columns {
      columnName
      unmaskedTo
      writableBy
    }
  }
}
```

### 列依赖检查（REQ-1484）

在保存会重命名某列的 SQL 别名或删除某列的表编辑之前，先查询还有哪些内容引用了它：

```graphql
query {
  columnDependents(tableId: "42", renamed: ["order_total"], removed: ["legacy_code"]) {
    columnName
    dependents { kind name detail breaksOn }
  }
}
```

重命名别名会破坏所有针对暴露名称编写的构件——视图、MV、指标表达式、RLS 谓词、DQ 合约。删除列除了破坏这些之外，还会破坏存储物理 `column_name` 的构件：关系、术语表绑定、标签分配。`breaksOn` 会说明是哪一种。Tables 页面会在保存时运行该查询，并以提示对话框展示结果。查询覆盖的范围及其局限性参见[血缘](lineage.md)。

### 视图管理

注册物化视图（REQ-133、REQ-135）：

```graphql
mutation {
  registerTable(input: {
    viewSql: "SELECT o.id, o.amount, c.name FROM orders o JOIN customers c ON o.customer_id = c.id"
    mvRefreshInterval: 300
    materialize: true
  }) {
    success
  }
}
```

触发手动刷新（REQ-135）：

```graphql
mutation {
  refreshMv(mvId: "orders-with-customers") {
    success
  }
}
```

### 图数据源注册

Neo4j 和 SPARQL 数据源通过 REST 终结点注册（而非 GraphQL 管理员 API）（REQ-295、REQ-297）：

**Neo4j：**

```bash
# 1. Register the Neo4j source
curl -X POST http://localhost:8001/admin/sources/neo4j \
  -H "Content-Type: application/json" \
  -d '{"source_id": "graph", "host": "neo4j", "port": 7474, "database": "neo4j"}'

# 2. Preview a Cypher query (validates scalar projections)
curl -X POST http://localhost:8001/admin/sources/neo4j/graph/preview \
  -H "Content-Type: application/json" \
  -d '{"cypher": "MATCH (p:Person) RETURN p.name AS name, p.age AS age"}'

# 3. Register a table (runs preview+validate automatically)
curl -X POST http://localhost:8001/admin/sources/neo4j/graph/tables \
  -H "Content-Type: application/json" \
  -d '{"table_name": "people", "cypher": "MATCH (p:Person) RETURN p.name AS name, p.age AS age", "ttl": 300}'
```

**SPARQL：**

```bash
# 1. Register the SPARQL source
curl -X POST http://localhost:8001/admin/sources/sparql \
  -H "Content-Type: application/json" \
  -d '{"source_id": "kg", "endpoint_url": "http://fuseki:3030/ds/sparql"}'

# 2. Register a table (probes endpoint and infers columns)
curl -X POST http://localhost:8001/admin/sources/sparql/kg/tables \
  -H "Content-Type: application/json" \
  -d '{"table_name": "products", "sparql_query": "SELECT ?name ?category WHERE { ?p a :Product ; :name ?name ; :category ?category . }", "ttl": 600}'
```

注册完成后，表会出现在 GraphQL 架构中，并可像任何其他数据源一样进行查询（REQ-016）。

## GraphiQL

Admin API 在浏览器中的 `GET /admin/graphql` 附带 GraphiQL（REQ-622）。可用它以交互方式探索完整的管理员架构。

## ops 域管理视图（REQ-1386）

每次安装都会向内置的 `ops` 域中植入八个 SQL 视图。[tool-verified: `provisa/api/startup_seed.py:225-331` `_seed_ops_domain`] 它们把查询审计日志暴露为受治理的表——可经由 SQL（pgwire）、GraphQL 和 Cypher 查询，并适用与任何业务表相同的域访问规则、行级安全和脱敏。

植入时会把 `org_admin` 设为 ops 域的管家，因此该域绝不会在 `stale_metadata` 中显示为治理缺口。[tool-verified: `startup_seed.py:326-331`]

| 视图 | 回答什么问题 |
| --- | --- |
| `usage_ranking` | 每个已注册表的查询次数与去重用户数；无人访问的表会浮现为退役候选 |
| `deprecated_usage` | 对带 `deprecated` 标签的表或列的每一次访问——阻碍安全移除的活跃消费方 |
| `pii_access` | 对带 `pii` 标签的表或列的每一次访问：谁查询的、以何角色、经由哪个接口 |
| `policy_denials` | 治理拒绝的所有访问尝试（HTTP 401/403） |
| `surface_mix` | 按协议接口（SQL、GraphQL、Cypher、gRPC 等）统计的每日查询次数与去重用户数 |
| `query_health` | 按接口统计的每日错误数与平均/最大延迟 |
| `stale_metadata` | 缺少描述的表与列；缺少管家的域 |
| `join_hotspots` | 最常被一起查询的表对——物化或缓存的候选 |

当前有两项限制。粒度为表级——审计日志记录 `table_ids`，而非具体访问了哪些列。查询文本已加密（REQ-689）并从此处所有视图中排除；只能经由授权的管理解密路径访问。[tool-verified: `_meta_views.py:148-162` — comment notes `query_text_enc` exclusion]

角色需要 `ops` 域的访问权限，这些视图才可见。授予方式与授予任何其他域的访问权限相同。

```sql
-- Which tables have never been queried?
SELECT table_name, domain_id
FROM ops.usage_ranking
WHERE query_count = 0;

-- Who accessed PII-tagged data in the last 7 days?
SELECT user_id, role_id, source, pii_column, logged_at
FROM ops.pii_access
WHERE logged_at >= CURRENT_DATE - INTERVAL '7 days'
ORDER BY logged_at DESC;

-- Where does traffic originate by protocol?
SELECT source, day, query_count, distinct_users
FROM ops.surface_mix
ORDER BY day DESC, query_count DESC;
```

同样的查询可以作为 GraphQL 或 Cypher 运行在任何受治理的传输之上——pgwire、Arrow Flight 或 Bolt。[inferred from governed-surface design]

## 报表查看器（REQ-1390）

报表查看器位于 `/admin/reports`。没有 `observability` 功能的角色无法访问它。

左侧面板列出 `ops` 域中每个已注册的表，按别名排序。[tool-verified: `ReportsTab.tsx:46-52` — filters `tables` to `domainId === "ops"`] 八个植入的管理视图会自动出现在那里。点击任一报表即可在右侧的受治理数据查看器中加载它。

**添加自定义报表。**「添加报表」按钮会打开一个对话框。填写名称、可选描述以及一条 SELECT 语句。保存会把该视图注册为 `ops` 域中受治理的派生表——已编目、受访问控制，并可与植入视图一同从所有接口查询。[tool-verified: `ReportsTab.tsx:70-96` — `registerTable` called with `sourceId: DERIVED_SOURCE_ID, domainId: "ops"`]

**删除。**垃圾桶图标仅对自定义报表显示。植入的管理视图无法从此接口删除。[tool-verified: `ReportsTab.tsx:151` — `const custom = report.sourceId === DERIVED_SOURCE_ID` gates the delete button]

## 表预览（REQ-1392）

在「表」页面展开任意一行。**预览**按钮会打开一个宽度为 90% 的模态框，显示该表的实时受治理数据。[tool-verified: `TablePreviewModal.tsx:24` — `size="90%"`; `GovernedTableViewer.tsx` is the underlying viewer]

以 API 为后端且带有必填路径参数的表会阻止预览，直到提供这些值。内嵌表单会在首次查询运行前收集每个必填参数；可选查询参数出现在同一表单中。[tool-verified: `GovernedTableViewer.tsx:51-55, 153-155` — `requiredParamColumns` check; "paramsRequired" message shown when `activeParams == null`]

## 受治理数据查看器（REQ-1391）

同一个查看器组件同时驱动预览模态框和报表查看器。两种场景下行为一致。

**服务端分页。**每一页都是独立的受治理 `SELECT *`，带 `LIMIT 101 OFFSET n`。每页显示 100 行；第 101 行用于判断是否还有更多。完整数据集绝不会加载到浏览器中。[tool-verified: `nativeParams.ts:72` — `LIMIT ${pageSize + 1} OFFSET ${page * pageSize}`; `types.ts:74` — `PAGE_SIZE = 100`]

**筛选与排序下推到数据源。**每个列标题都有一个筛选框。筛选词会变成 `WHERE LOWER(CAST(col AS VARCHAR)) LIKE LOWER('%term%')` 谓词；点击排序会生成 `ORDER BY` 子句。二者都会发往数据库——对十亿行的表做筛选扫描的是数据源，而非你眼前的 100 行。[tool-verified: `nativeParams.ts:53-70`]

**多级分组。**每个列标题上的图层图标会把该列加入分组。分组列排在 `ORDER BY` 最前，因此即使跨页，组成员也会与其组标题落在同一页。主键列会追加在末尾，作为稳定的决胜条件。[tool-verified: `nativeParams.ts:61-70` — group columns first, then explicit sorts, then PKs] 组标题行可折叠；折叠会隐藏成员而不发出新查询。[tool-verified: `useResultsGrid.ts:150-171` — `collapsedGroups` set gates the `build()` recursion]

**选择会保留。**筛选、排序和分组设置保存在 `localStorage` 的 `provisa.grid.table:<domain>.<table>` 下，下次访问时恢复。[tool-verified: `useResultsGrid.ts:95-98`, `GovernedTableViewer.tsx:66`]

**导出。**把当前页下载为 CSV，或以制表符分隔的文本复制到剪贴板。导出仅覆盖可见页。[tool-verified: `useResultsGrid.ts:247-274` — both handlers iterate `displayRows`, which in server-paged mode is the current page]
