# Admin API

Admin API 是一个 Strawberry GraphQL 终结点，位于 `POST /admin/graphql`。它需要超级用户或管理员角色，并且与数据 GraphQL 终结点是分开的。

## 身份验证 {: #authentication }

使用 Provisa 的标准身份验证提供程序，在 `Authorization` 标头中传递您的凭据：

```yaml
Authorization: Bearer <token>
```

管理员访问权限由分配给角色的 `admin` 功能所控制。

### 个人访问令牌 {: #personal-access-tokens }

凡是接受 bearer 令牌的地方都接受个人访问令牌，本端点也不例外。你自己签发和吊销自己的令牌，因为令牌是持有者本人的凭据。所以它位于管理界面中你的用户资料页，与退出组织、删除账户并列，而不在管理员页面下。管理员从不代他人签发令牌。

| 路由 | 作用 |
| ------- | -------- |
| `POST /auth/tokens` | 为调用者签发一个令牌。请求体：`name`，以及可选的 `role_id`、`scopes`、`expires_in_days`（1–366）。响应是密钥唯一一次出现的地方 |
| `GET /auth/tokens` | 调用者在本组织中的有效令牌——显示前缀、名称、生命周期时间戳，以及用于吊销的令牌标识哈希。绝不返回可用凭据 |
| `DELETE /auth/tokens/{token_hash}` | 吊销调用者的某个令牌。若不属于调用者或已被吊销则返回 404 |

省略 `role_id` 时，令牌解析为其所有者持有的角色；指定角色则把令牌收窄到低于其所有者。吊销也会隐式发生：移除用户在某组织中的成员资格会吊销其针对该组织的所有令牌。凭据本身参见[安全模型](security.md#personal-access-tokens)。

## 功能 {: #capabilities }

### 配置管理 {: #config-management }

下载当前正在运行的配置：

```http
GET /admin/config
```

以 YAML 文件形式返回完整的 `config.yaml`。上传新配置：

```http
PUT /admin/config
```

Provisa 会验证 YAML、重新加载目录，并重新生成架构 (Schema)。无需重启。

### 运行时设置 {: #runtime-settings }

在不编辑配置文件的情况下读取和写入运行时平台设置：

```http
GET  /admin/settings
PUT  /admin/settings
```

设置范围涵盖大结果重定向、默认采样和行数限制、响应缓存的 TTL、命名约定、关联外键的自动追踪、物化存储的 DSN、联邦引擎内存（`jvm_heap_gb`、`query_max_memory`、`query_max_memory_per_node`、`query_max_total_memory`、`fault_tolerant_execution`、`fault_tolerant_task_memory`、`exchange_spool_dir`），以及整个 OpenTelemetry 追踪管道的调优范围。远程 GraphQL 遍历限制以及暖层/读取缓存设置也已提供。

**连接器服务器**卡片为每个有随附服务器的连接器（File Crawler、SharePoint、Salesforce、ServiceNow、Cloud Inventory、Splunk、AskAmerica）命名一个文件夹，其中存放为该主机构建的服务器程序包（`connectors.<connector>.bundle_dir`，环境变量 `PROVISA_PGWIRE_BUNDLE_<CONNECTOR>`）。为空时，连接器服务器从固定的 `kenstott/calcite` release 获取。已设置时，文件夹按原样使用：其启动程序必须位于 `bin/pgwire-<connector>`；没有该启动程序的文件夹会阻止连接器服务器启动，并报错指出该设置 — release 不会在其位置被获取。该设置在每次连接器服务器启动时读取。

安全姿态 `security.mode`（`standard` | `high`）在重启时生效：

```http
GET  /admin/security
PUT  /admin/security
```

AI 模型分配、嵌入与向量模型注册表以及 NL 速率限制从下一个请求起生效，无需重启：

```http
GET  /admin/ai-models
PUT  /admin/ai-models
```

管理员加密选项卡会实时从加密注册表获取其提供程序列表；不可用的提供程序会显示，但不可选择。

`GET`/`HEAD /health` 和 `GET /setup/status` 从不要求认证。即使配置了认证提供方，它们也跳过 `Authorization: Bearer` 要求。

### 联邦引擎 {: #federation-engine }

读取或更改本部署所使用的引擎：

```http
GET  /admin/federation-engine
PUT  /admin/federation-engine
```

`GET` 返回当前生效的引擎键，以及它所需的配置字段。`PUT` 接受包含 `engine`（引擎键）及任意引擎专属字段的请求体；该选择会持久化到平台配置，并在下次服务重启时绑定。

### 关联编辑器 {: #relationship-editor }

列出关联：

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

创建关系。已存在的 id 会被拒绝；关系通过 `updateRelationship` 修改，它接受相同的输入：

```graphql
mutation {
  createRelationship(input: {
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

声明一个由联结表支撑的关系：

```graphql
mutation {
  createRelationship(input: {
    id: "pets-bonded-pair"
    sourceTableId: "pets"
    targetTableId: "pets"
    sourceColumn: "id"
    targetColumn: "id"
    cardinality: "one-to-many"
    viaTable: "pet_companions"
    viaSourceColumn: "pet_id"
    viaTargetColumn: "companion_pet_id"
    viaTypeColumn: "companion_type"
    viaTypeValue: "bonded pair"
    viaLabelSource: "column"
  }) {
    success
  }
}
```

关联表由你声明为边，Provisa 从不自动发现。`viaTable` 指定一张已注册的表。它的两个键列承载这条边，其余每一列都成为该关系的属性，可以像任何字段一样过滤。`viaTypeColumn` / `viaTypeValue` 把一张连接表拆分成多种边类型。例如，`pet_companions` 中 `companion_type` 为 `bonded pair`、`littermate` 或 `shares enclosure` 的行，会在同一对表之间形成三个不同的关系。

`viaLabelSource` 指定对外名称从哪里来，Cypher 拿到的每种形式都是大写加下划线。`column` 使用 `viaTypeValue`（`BONDED_PAIR`），`table` 使用连接表自己的名称（`PET_COMPANIONS`），`fixed` 使用声明的 `alias`。这样声明的连接表是一条边。Provisa 会把它从节点标签中去掉，所以它永远不会以节点标签块出现在图界面中。

### AI 关联发现 {: #ai-relationship-discovery }

通过 REST 触发由 Claude 提供支持的外键分析：

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

### 架构内省 {: #schema-introspection }

浏览所有数据源中已发布的表：

```graphql
query {
  tables {
    id
    sourceId
    columns {
      columnName
      unmaskedTo
    }
    writeGrants {
      role
      operation
      columns
      rows
      check
      presets { column source name value }
    }
  }
}
```

### 列依赖检查 {: #column-dependency-check }

在保存会重命名某列的 SQL 别名或删除某列的表编辑之前，先查询还有哪些内容引用了它：

```graphql
query {
  columnDependents(tableId: "42", renamed: ["order_total"], removed: ["legacy_code"]) {
    columnName
    dependents { kind name detail breaksOn }
  }
}
```

重命名别名会破坏所有基于对外名称编写的制品：视图、MV、指标表达式、RLS 谓词和 DQ 契约。删除列除了破坏这些之外，还会破坏存储物理 `column_name` 的制品，即关系、术语表绑定和标签分配。`breaksOn` 会说明是哪一种。“表”页面在保存时运行这项检查，并以提示对话框显示结果。[血缘](lineage.md) 说明了查询覆盖什么、不能覆盖什么。

### 视图管理 {: #view-management }

注册物化视图：

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

触发手动刷新：

```graphql
mutation {
  refreshMv(mvId: "orders-with-customers") {
    success
  }
}
```

### 图数据源注册 {: #graph-source-registration }

Neo4j 和 SPARQL 数据源通过 REST 终结点注册（而非 GraphQL 管理员 API）：

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

注册完成后，表会出现在 GraphQL 架构中，并可像任何其他数据源一样进行查询。

### Hasura / DDN 导入 {: #hasura-ddn-import }

通过管理界面或 API 把现有的 Hasura v2 或 Hasura DDN 项目转换为 Provisa 配置，在你批准之前不会有任何内容落库。

```http
POST /admin/import/hasura/preview
POST /admin/import/hasura/apply
```

**预览**会转换上传的归档文件，并返回建议的 `config_yaml`、一份警告列表，以及所发现内容的摘要（数据源、域、表、列、角色、关联和 RLS 的数量）。不会向租户数据库写入任何内容。请求体：

```json
{
  "filename": "my-hasura-project.zip",
  "content_b64": "<base64-encoded archive>",
  "flavor": "auto",
  "domain_map": {"public": "sales"},
  "source_overrides": {}
}
```

`flavor` 可为 `"auto"`（从归档结构中检测）、`"hasura_v2"` 或 `"ddn"`。

**应用** 会把你审阅过（如果愿意也可编辑过）的 YAML 加载到当前操作的组织中。它走的是与 `PUT /admin/config` 相同的热重载路径。请求体：`{"config_yaml": "<yaml string>"}`。

预览绝不在服务端缓存转换后的 YAML；应用取用的是你提交的 YAML，因此所应用的内容与所审阅的完全一致。

### Apache Ossie 互通 {: #apache-ossie-interchange }

Provisa 以导入/导出边界的形式与 Apache Ossie（孵化中）互通。

```http
GET  /admin/ossie
POST /admin/ossie/import
```

**导出**（`GET /admin/ossie`）在每次请求时都从实时的受治理模型构建 Ossie YAML 文档。不做任何缓存，所以文档永远不会过时。响应为 `text/yaml`，带有 `Content-Disposition: attachment` 请求头。文档是一个模型，根部包含 `version`、`name`、`datasets`、`relationships` 和 `metrics`。表变为 `dataset` 对象，列变为 `field` 对象，关系映射为 Ossie 的 `relationship` 对象。没有任何表的模型会以 `422` 被拒绝。

**导入**（`POST /admin/ossie/import`）接受 YAML 或 JSON 格式的 Ossie 文档，并自行识别格式。它解析文档，以 JSON 对象返回拟议的表、关系和指标注册。不会注册任何东西。它只读取扁平的文档形态，把模型包在 `semantic_model` 中的文档会被点名拒绝。在管理界面中，审阅页面让你在发生任何更改之前接受或删减这些提议。

### 对象存储 {: #object-storage }

读取或配置组织的物化存储：

```http
GET  /admin/org-storage
PUT  /admin/org-storage
```

`GET` 报告该组织用掉了多少平台存储配额。`PUT` 注册该组织自有的存储 DSN（静态加密；GET 绝不返回它）。一经设置，该组织的物化便落入它自己的存储桶，不再计入平台配额。发送 `storage_url: null` 会清除该设置，并把该组织移回平台存储。

### 组织加密 {: #org-encryption }

设置或轮换组织的静态加密密钥：

```http
GET  /admin/org-encryption
PUT  /admin/org-encryption
```

`GET` 返回密钥的指纹、id 和来源，从不返回密钥材料。`PUT` 设置或轮换密钥。提供 `key_b64`（32 个原始字节，base64 编码）即可使用你自己的密钥；省略它则由 Provisa 生成一个。没有删除操作，因为停用最后一把密钥会让它包裹过的每个负载都无法读取。

## GraphiQL

Admin API 在浏览器中的 `GET /admin/graphql` 附带 GraphiQL。可用它以交互方式探索完整的管理员架构。

## ops 域管理视图 {: #ops-domain-management-views }

每次安装都会在内置的 `ops` 领域中植入八个 SQL 视图。它们把查询审计日志以受治理表的形式公开。你可以通过 SQL（pgwire）、GraphQL 和 Cypher 查询它们，适用的领域访问、RLS 和脱敏规则与任何业务表相同。

植入时会把 `org_admin` 设为 ops 域的管家，因此该域绝不会在 `stale_metadata` 中显示为治理缺口。

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

粒度止于表级。审计日志记录 `table_ids`，不记录查询读取的具体列。查询文本经过加密，不出现在这里的任何视图中；只有经授权的管理员解密路径才能读取它。

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

由于这些视图是受治理表，你也可以用 GraphQL 或 Cypher 编写同样的查询。

## 报表查看器 {: #reports-viewer }

报表查看器位于 `/admin/reports`。没有 `observability` 功能的角色无法访问它。

左侧面板列出 `ops` 域中每个已注册的表，按别名排序。 八个植入的管理视图会自动出现在那里。点击任一报表即可在右侧的受治理数据查看器中加载它。

**添加自定义报表。** “Add report”按钮会打开一个对话框，你在其中填写名称、可选的描述和一条 SELECT 语句。保存后，该视图会作为受治理的派生表注册到 `ops` 领域中。它会进入目录、受访问控制，并可通过所有接口查询，与预置视图并列。

**删除。**垃圾桶图标仅对自定义报表显示。植入的管理视图无法从此接口删除。

## 表预览 {: #table-preview }

在「表」页面展开任意一行。**预览**按钮会打开一个宽度为 90% 的模态框，显示该表的实时受治理数据。

以 API 为后端且带有必填路径参数的表会阻止预览，直到提供这些值。内嵌表单会在首次查询运行前收集每个必填参数；可选查询参数出现在同一表单中。

## 受治理数据查看器 {: #governed-data-viewer }

同一个查看器组件同时驱动预览模态框和报表查看器。两种场景下行为一致。

**服务端分页。**每一页都是独立的受治理 `SELECT *`，带 `LIMIT 101 OFFSET n`。每页显示 100 行；第 101 行用于判断是否还有更多。完整数据集绝不会加载到浏览器中。

**筛选与排序下推到数据源。** 每个列标题都有一个筛选输入框。筛选词会变成 `WHERE LOWER(CAST(col AS VARCHAR)) LIKE LOWER('%term%')` 谓词，点击排序会生成 `ORDER BY` 子句。两者都在数据库中执行。对十亿行的表筛选会扫描数据源，范围从不局限于你眼前那一页的 100 行。

**多级分组。**每个列标题上的图层图标会把该列加入分组。分组列排在 `ORDER BY` 最前，因此即使跨页，组成员也会与其组标题落在同一页。主键列会追加在末尾，作为稳定的决胜条件。 组标题行可折叠；折叠会隐藏成员而不发出新查询。

**选择会保留。**筛选、排序和分组设置保存在 `localStorage` 的 `provisa.grid.table:<domain>.<table>` 下，下次访问时恢复。

**导出。**把当前页下载为 CSV，或以制表符分隔的文本复制到剪贴板。导出仅覆盖可见页。
