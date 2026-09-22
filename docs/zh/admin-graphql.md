# 管理 GraphQL API 参考

管理 GraphQL API 是 Provisa 的配置平面。它是管理 Web 应用为每一项管理操作所调用的 API——创建数据源、注册表、定义关系、配置 RLS 规则，以及塑造该模型的其他一切。

**挂载点：** `POST /admin/graphql`

这与 `/data/graphql` 处的数据平面 API 不同。数据平面为注册域上的最终用户查询提供服务，并由 `/data/sdl` 处的 SDL 描述。管理 API 则配置该架构的样子以及谁可以看到什么。

---

## UI 如何与该 API 通信

管理 Web 应用使用 Apollo Client，指向 `${API_BASE}/admin/graphql`。[tool-verified: `provisa-ui/src/apolloClient.ts:19`]

每个请求都携带一个 Bearer 令牌（每次调用时从身份验证提供程序重新获取）、在多租户情况下携带一个 `X-Org-Id` 头，以及在为分支环境提供服务时携带一个 `X-Env` 头。[tool-verified: `provisa-ui/src/apolloClient.ts:24-42`]

该架构由两个 `@strawberry.type` 类组装而成——来自 `schema_query.py` 的 `Query` 和来自 `schema_mutation.py` 的 `Mutation`——并包裹在一个 `ModelCommitExtension` 中，该扩展会针对当前环境分支记录每一次变更（REQ-1524）。[tool-verified: `provisa/api/admin/schema.py:44`]

---

## 授权

**开发模式：** 当未配置身份验证且每个请求都以匿名主体身份到达时，所有能力检查都会被跳过。这使本地安装无需身份验证设置即可正常工作。[tool-verified: `provisa/api/admin/capabilities.py:98-99`]

**能力关卡：** 生产部署会强制执行命名能力。每个字段所需的具体权限都在行内注明。在没有所需能力的情况下调用变更会引发 `PermissionError`。平台管理员角色可绕过所有能力检查（REQ-1297）。[tool-verified: `provisa/api/admin/capabilities.py:80-110`]

**域关卡：** 若干变更还会检查对象所属的域。一个作用域限定为 `sales` 的调用者不能将表注册到 `finance` 中、为其排队一条 RLS 规则，或创建一个源表位于其不持有的域中的关系（REQ-1530、REQ-1531）。视图受到更进一步的约束：视图 SQL 读取的每一张表都必须在调用者的域范围内，因为自由手写的 SQL 否则会让成员访问其作用域之外的数据。[tool-verified: `provisa/api/admin/domain_guard.py:1-133`]

**角色继承：** 父角色的能力会被子角色继承（REQ-1677）。`createRole` 和 `deleteRole` 会拒绝形成循环，并阻止删除有继承者的角色。

---

## 通用返回类型

大多数变更返回 `MutationResult`。[tool-verified: `provisa/api/admin/types.py:1181-1188`]

```graphql
type MutationResult {
  success: Boolean!
  message: String!
  code: String          # stable i18n key, e.g. "schema.source_created"
  params: JSON          # key/value pairs for client-side localization (REQ-1350)
}
```

当变更失败时，`success` 为 `false`，`message` 携带英文原因。`code` 是一个稳定标识符，UI 使用它来渲染本地化消息。

---

## 查询

### 数据源

#### `sources → [SourceType!]!`

所有已注册的数据源。[tool-verified: `provisa/api/admin/schema_query.py:327-331`]

```graphql
query {
  sources {
    id type host port database username dialect
    cacheEnabled cacheTtl preferMaterialized
    loadProtected offPeakWindow offPeakTz
    gqlNamingConvention path allowedDomains
    description mappingJson federationHintsJson
    changeSignal passwordRef
    cdc { bootstrapServers topicPrefix schemaRegistryUrl consumerGroupId }
  }
}
```

`passwordRef` 是指向组织保管库的 `${secret:NAME}` 引用——绝不是字面凭据。[tool-verified: `provisa/api/admin/types.py:105`]

#### `source(id: String!) → SourceType`

按 ID 查找单个数据源。未找到时返回 `null`。[tool-verified: `provisa/api/admin/schema_query.py:334-339`]

#### `availableSchemas(sourceId: String!) → [String!]!`

数据源中可见的架构，已过滤掉 Provisa 内部的架构。优先使用原生内省；当数据源类型没有直接连接池时，回退到引擎目录。[tool-verified: `provisa/api/admin/schema_query.py:628-667`]

#### `availableTables(sourceId: String!, schemaName: String = "public") → [AvailableTableType!]!`

数据源某个架构中的表及其注释。对于 OpenAPI 数据源，返回响应为数组或分页包装器的 GET 操作。对于 GraphQL 数据源，返回返回列表的查询字段。对于 gRPC，返回服务器流式 RPC。[tool-verified: `provisa/api/admin/schema_query.py:669-731`]

#### `availableColumns(sourceId: String!, schemaName: String!, tableName: String!) → [String!]!`

引擎目录中某张表的列名。对于 govdata 数据源，使用单独的解析器。[tool-verified: `provisa/api/admin/schema_query.py:845-866`]

#### `availableColumnsMetadata(sourceId: String!, schemaName: String!, tableName: String!) → [AvailableColumnType!]!`

带有数据类型、注释、原生过滤器类型和主键标志的列名。对于 OpenAPI 数据源，会根据操作的响应架构和参数推导出结构。[tool-verified: `provisa/api/admin/schema_query.py:869-876`]

#### `availableFunctions(sourceId: String!, schemaName: String = "openapi") → [AvailableTableType!]!`

OpenAPI 数据源的非 GET 操作（POST、PUT、PATCH、DELETE）。对于非 OpenAPI 数据源返回空列表。[tool-verified: `provisa/api/admin/schema_query.py:822-843`]

#### `crawlSource(path, depth, pattern, recursive, simpleLinks, sameDomain, excludePattern) → CrawlResultType`

在创建数据源之前，预览文件连接器爬取将会发现的内容——文件、表和列。仅限 HTTP 的设置（`simpleLinks`、`sameDomain`、`excludePattern`）在本地、S3、FTP 和 SFTP 根目录下会被忽略。（REQ-1785）[tool-verified: `provisa/api/admin/schema_query.py:733-790`]

#### `suggestTableAlias(tableName: String!, domainId: String!, sourceId: String!) → String!`

返回在 `domainId` 中从 `sourceId` 注册 `tableName` 时应使用的别名。当不存在冲突时返回一个普通的 snake-case 别名，当有效名称已被同一域中的另一数据源占用时返回一个带数据源前缀的别名（`sqlite_b_orders`）。[tool-verified: `provisa/api/admin/schema_query.py:879-922`]

---

### 表

#### `tables → [RegisteredTableType!]!`

所有已注册的表，各自带有完整的列列表。响应中的列可见性遵从调用者的 `table_registration` 能力——`canDeployToDb` 取决于调用者是否持有该权限。（REQ-016、REQ-021、REQ-042）[tool-verified: `provisa/api/admin/schema_query.py:502-536`]

每个 `RegisteredTableType` 都暴露计算得出的子字段：

- **`refreshPolicySummary → RefreshPolicySummaryType`** —— 以纯文本形式呈现的有效刷新/服务策略，由服务器端从引擎所使用的同一个规划器解析派生。启动期间返回 `null`。（REQ-1143）[tool-verified: `provisa/api/admin/types.py:319-327`]
- **`graphqlFieldName → String`** —— 该表在已编译的数据平面架构中的字段名称，使数据产品面板无需重新实现命名算法即可构建可运行的示例。（REQ-1634）[tool-verified: `provisa/api/admin/types.py:330-339`]
- **`dqDataset → String`** —— 该表作为一个数据质量契约数据集，采用检查器所扫描的形式。（REQ-1443）[tool-verified: `provisa/api/admin/types.py:374-387`]
- **`productId → String`** —— 该表所属的数据产品。DQ 检查器表继承其契约所扫描表的产品。（REQ-1634）[tool-verified: `provisa/api/admin/types.py:342-372`]

#### `refreshPolicyPreview(...) → RefreshPolicySummaryType`

为*草稿*（尚未保存）的表配置项预览有效的刷新/服务摘要，使表单顶部的摘要随字段变化而更新，而不持久化任何内容。派生方式与上面的 `refreshPolicySummary` 相同。（REQ-1143）[tool-verified: `provisa/api/admin/schema_query.py:947-979`]

参数：`sourceId`、`domainId`、`schemaName`、`tableName`、`cacheTtl`、`preferMaterialized`、`loadProtected`、`offPeakWindow`、`offPeakTz`、`changeSignal`。

#### `columnDependents(tableId: String!, renamed: [String!], removed: [String!]) → [ColumnDependentsType!]!`

待处理的别名重命名或列删除将破坏的构件。仅供参考——管理 UI 会在保存前显示此信息，由管理员决定。必须在*保存之前*调用，因为依赖项是针对该列当前所持有的暴露名称编写的。（REQ-1484）[tool-verified: `provisa/api/admin/schema_query.py:1301-1331`]

---

### 关系

#### `relationships → [RelationshipType!]!`

所有用户定义的关系（不包括自动生成的 `gql_auto__` 条目以及 ERD 使用的合成 `meta:%` 条目）。[tool-verified: `provisa/api/admin/schema_query.py:539-569`]

#### `allRelationships → [RelationshipType!]!`

与 `relationships` 相同，但包含 `meta:%` 合成条目。供图形 ERD 使用，它需要显示每一条边，包括数据表与元数据注册表之间的隐式 `HAS_TABLE` 链接。[tool-verified: `provisa/api/admin/schema_query.py:572-601`]

每个 `RelationshipType` 暴露：

- **`autoSuggested → Boolean`** —— 该关系是否由外键分析建议（`id` 以 `fk__` 开头）。[tool-verified: `provisa/api/admin/types.py:523-525`]
- **`physicalName → String`** —— 该关系在 SQL 和 gRPC 平面上的名称（`?include=` 参数）。由服务器端派生；客户端不得音译 GraphQL 别名。（REQ-471、REQ-1417）[tool-verified: `provisa/api/admin/types.py:527-536`]

---

### 域、角色与用户

#### `domains → [DomainType!]!`

活动组织租户数据库中的所有域。租户数据库在架构层面是隔离的，因此某个组织管理员的域列表只包含其组织的行。（REQ-021、REQ-042、REQ-1293）[tool-verified: `provisa/api/admin/schema_query.py:342-357`]

#### `roles → [RoleType!]!`

调用者可见的角色。管理员可以看到每个角色；非管理员只能看到没有 `org_id` 的角色，或属于其组织的角色。（REQ-042、REQ-059、REQ-060、REQ-215）[tool-verified: `provisa/api/admin/schema_query.py:603-618`]

#### `resolveOwners(refs: [String!]!) → [UserSummaryType!]!`

将角色 ID 或用户 ID 解析为具体用户。用于将 `DataProduct.ownerRole`、`Domain.steward` 和 `Column.visibleTo` 展开为人类可读的列表。未知引用会原样回显，以便 UI 显示原始 ID 而不是空白。[tool-verified: `provisa/api/admin/schema_query.py:388-444`]

---

### RLS 规则

#### `rlsRules → [RLSRuleType!]!`

所有行级安全规则。底层仓储会在边界处解密 `filterExpr`。（REQ-041、REQ-402、REQ-686）[tool-verified: `provisa/api/admin/schema_query.py:621-625`]

---

### 数据产品

#### `dataProducts → [DataProductType!]!`

所有数据产品。需要 `data_product_read` 能力。（REQ-1634）[tool-verified: `provisa/api/admin/schema_query.py:360-386`]

---

### 标签

#### `tags → [TagType!]!`

所有标签定义，包括每个标签允许的参数值。（REQ-1373、REQ-1467）[tool-verified: `provisa/api/admin/schema_query.py:447-475`]

#### `tagAssignments → [TagAssignmentType!]!`

跨数据源、表、列和关系的所有标签分配。（REQ-1377）[tool-verified: `provisa/api/admin/schema_query.py:478-499`]

---

### 物化视图

#### `mvList → [MVType!]!`

所有物化视图及其运行时状态：启用/禁用、上次刷新时间戳、行数和上次错误。[tool-verified: `provisa/api/admin/schema_query.py:927-944`]

---

### 缓存

#### `cacheStats → CacheStatsType`

缓存统计信息。配置了 Redis 时返回带有完整运行指标的 `storeType: "redis"`；使用内嵌 fakeredis 存储时返回 `storeType: "memory"`；未配置缓存时返回 `storeType: "noop"`。[tool-verified: `provisa/api/admin/schema_query.py:1106-1140`]

#### `cacheTableStats → [CacheTableStatType!]!`

每张表已缓存条目的计数。未配置缓存存储时为空。[tool-verified: `provisa/api/admin/schema_query.py:1143-1148`]

#### `hotTables → [HotTableStatType!]!`

Provisa 正在保留副本的表，分为两个层级：`hot`（镜像到响应存储中以供 JOIN 内联）和 `warm`（作为 Iceberg 副本落地）。一张表最多处于一个层级中（REQ-241）。[tool-verified: `provisa/api/admin/schema_query.py:1151-1175`]

#### `materializeStoreInfo → MaterializeStoreInfoType`

持久化物化存储的身份信息：引擎名称、存储 DSN 引用、物化视图数量，以及该存储是否为实例本地存储（如 DuckDB 或 SQLite 这类本地文件，意味着负载均衡器背后的每个实例都保留自己的副本）。[tool-verified: `provisa/api/admin/schema_query.py:1178-1189`]

---

### 系统健康状态

#### `systemHealth → SystemHealthType`

引擎连接状态、工作进程池计数、元数据数据库连接池状态、缓存模式，以及每个协议监听器（pgwire、gRPC、Arrow Flight、Bolt）的存活状态。[tool-verified: `provisa/api/admin/schema_query.py:1194-1198`]

```graphql
query {
  systemHealth {
    engineConnected engineWorkerCount engineActiveWorkers
    metadataPoolSize metadataPoolFree metadataDialect
    cacheMode cacheConnected mvRefreshLoopRunning
    protocols { name status port }
  }
}
```

---

### 计划任务

#### `scheduledTasks → [ScheduledTaskType!]!`

来自配置的计划触发器及其运行时状态。每个条目携带其 cron 表达式、`kind`（`webhook` 或 `sql`）、当前是否启用、上次运行时间戳（本版本中始终为 `null`——由调度器跟踪），以及来自 APScheduler 的下次计划运行时间。[tool-verified: `provisa/api/admin/schema_query.py:1203-1245`]

---

### 数据质量

#### `dqContractParse(checker: String!, contractText: String!) → DqContractType`

将原始契约文本解析为构建面板中可编辑的行。每次编辑都会调用；解析失败会以 `error` 形式返回，而不是作为 GraphQL 错误返回，因为操作员输入过程中出现半成品文本是正常情况。（REQ-1443 第 7 条）[tool-verified: `provisa/api/admin/schema_query.py:1007-1022`]

#### `dqCheckCatalog(checker: String!, dataset: String!) → DqCheckCatalogType`

`checker` 提供的检查项，限定在 `dataset` 的列范围内。该数据集是契约的观测目标，其解析方式与扫描器的解析方式相同——因此所提供的检查项与检查器实际会看到的列相匹配。（REQ-1443 第 7 条）[tool-verified: `provisa/api/admin/schema_query.py:1025-1050`]

#### `dqCheckDefinition(checker: String!, check: DqCheckBuildInput!) → DqCheckDefinitionType`

来自面板编辑器的某一项检查的文本。放在服务器端是因为该方言只有一种实现；通过构建器创建的检查和手工输入的检查必须无法区分。（REQ-1443 第 7 条）[tool-verified: `provisa/api/admin/schema_query.py:1053-1075`]

#### `dqContractBuild(checker: String!, dataset: String!, checks: [DqCheckInput!]!) → DqContractTextType`

将编辑后的检查行序列化回契约文本。是 `dqContractParse` 的逆操作。放在服务器端出于同样的原因：面板不能生成检查器会拒绝的文本。（REQ-1443 第 7 条）[tool-verified: `provisa/api/admin/schema_query.py:1078-1101`]

---

### 数据源预览

#### `neo4jPreview(sourceId: String!, cypher: String!) → QueryPreviewType`

预览 Neo4j 数据源上的 Cypher 投影：最多五行以及注册将携带的列类型。失败会以 `error` 形式返回。（REQ-1670）[tool-verified: `provisa/api/admin/schema_query.py:984-992`]

#### `sparqlPreview(sourceId: String!, query: String!) → QueryPreviewType`

预览 SPARQL 数据源上的 SPARQL SELECT：最多五行，所有列均为文本形式。（REQ-1683）[tool-verified: `provisa/api/admin/schema_query.py:995-1002`]

---

### Kaggle

#### `kaggleTokenValid(token: String!) → Boolean!`

针对 Kaggle API 的实时检查。只有当令牌通过身份验证时才返回 `true`。为 Kaggle 数据源表单中的令牌关卡步骤提供支持。（REQ-1783）[tool-verified: `provisa/api/admin/schema_query.py:793-799`]

#### `kaggleDatasets(token: String!, query: String = "", page: Int = 1) → [KaggleDatasetType!]!`

搜索 Kaggle 完整的公共数据集目录。受令牌限制。（REQ-1783）[tool-verified: `provisa/api/admin/schema_query.py:802-819`]

---

### 日历

#### `calendars → [CalendarType!]!`

所有已注册的快照边界日历版本。为快照计划配置选择器提供数据，并确认周期性物化视图可以引用哪些日历。（REQ-962）[tool-verified: `provisa/api/admin/schema_query.py:218-242`]

---

### 指标

#### `metrics → [MetricType!]!`

所有受治理的指标定义。源自事实表的指标携带 `fromFact`。（REQ-1317、REQ-1320）[tool-verified: `provisa/api/admin/schema_query.py:245-264`]

---

### 架构版本

#### `schemaVersion → String!`

当前架构状态（域、表 ID、关系 ID）的 SHA-256 哈希值。Apollo 客户端从 `X-Schema-Version` 响应头中读取此值，并在其推进时重新获取所有活动查询。[tool-verified: `provisa/api/admin/schema_query.py:291-324`]

---

### AI 助手

#### `generateTableDescription(tableId: String!) → String!`

使用配置的 LLM 为一张已注册的表生成一到两句话的描述。请先保存该表；对未保存的表调用此接口会返回一条说明性消息。[tool-verified: `provisa/api/admin/schema_query.py:1250-1298`]

#### `generateColumnDescription(tableId: String!, columnName: String!) → String!`

使用配置的 LLM 为单个列生成一句话的描述。[tool-verified: `provisa/api/admin/schema_query.py:1334-1383`]

---

### 创建请求

#### `creationRequests → [CreationRequestType!]!`

待处理的创建请求，对持有相关创建能力的调用者可见。当没有 `create_relationship` 或 `create_view` 能力的成员提交一项需要权限持有者批准的请求时使用。（REQ-434、REQ-063）[tool-verified: `provisa/api/admin/schema_query.py:267-289`]

---

## 变更

### 数据源

#### `createSource(input: SourceInput!) → MutationResult`

注册一个新的数据源。在持久化之前验证连接——被拒绝的数据源不会留下保管库条目。将凭据存储在组织保管库中并记录引用；明文永远不会落入数据库。（REQ-012、REQ-013）需要 `source_registration` 能力。[tool-verified: `provisa/api/admin/schema_mutation.py:616-781`]

#### `updateSource(input: SourceInput!) → MutationResult`

更新现有数据源的连接详情、描述和配置。对于文件/SharePoint 数据源，会拆除并重新连接 pgwire 终结点，使路径更改立即生效。需要 `source_registration`。[tool-verified: `provisa/api/admin/schema_mutation.py:922-1073`]

#### `deleteSource(id: String!) → MutationResult`

移除一个数据源及其保管库条目。删除引擎目录并重建架构。[tool-verified: `provisa/api/admin/schema_mutation.py:1101-1132`]

#### `renameSource(oldId: String!, newId: String!) → MutationResult`

重命名数据源 ID。[tool-verified: `provisa/api/admin/schema_mutation.py:1076-1098`]

#### `updateSourceCache(sourceId: String!, cacheEnabled: Boolean!, cacheTtl: Int) → MutationResult`

为一个数据源启用或禁用查询结果缓存，并以秒为单位设置 TTL。[tool-verified: `provisa/api/admin/schema_mutation.py:2327-2350`]

#### `updateSourcePreferMaterialized(sourceId: String!, preferMaterialized: Boolean!) → MutationResult`

对某数据源上的所有表强制（或释放）物化联邦。（REQ-826）[tool-verified: `provisa/api/admin/schema_mutation.py:2379-2402`]

#### `updateSourceLoadProtection(sourceId, loadProtected, offPeakWindow, offPeakTz) → MutationResult`

将一个数据源标记为受负载保护（仅按计划刷新）。至少需要一个关卡——非高峰时段窗口、缓存 TTL 节奏，或探测式变更信号——否则调用失败。（REQ-1141）[tool-verified: `provisa/api/admin/schema_mutation.py:2431-2480`]

#### `updateSourceNaming(sourceId: String!, gqlNamingConvention: String) → MutationResult`

设置按数据源的 GraphQL 命名约定。[tool-verified: `provisa/api/admin/schema_mutation.py:2595-2619`]

#### `updateSourceAllowedDomains(sourceId: String!, allowedDomains: [String!]!) → MutationResult`

设置哪些域可以使用某数据源（空列表 = 不受限制）。需要 `source_registration`。（REQ-1531）[tool-verified: `provisa/api/admin/schema_mutation.py:2622-2658`]

#### `stageKaggleDataset(token, owner, ref, idPrefix) → KaggleStageResultType`

下载并解压一个 Kaggle 数据集到本地磁盘。返回暂存目录路径；调用者随后创建一个指向该目录的 `files` 类型数据源。携带 SQLite 的压缩包会被整体拒绝。需要 `source_registration`。（REQ-1780–1782）[tool-verified: `provisa/api/admin/schema_mutation.py:784-824`]

#### `refreshKaggleSource(sourceId: String!, token: String!) → MutationResult`

原地重新获取一个源自 Kaggle 的数据源的数据集。如果 Kaggle 上没有比磁盘上更新的内容，则跳过下载。需要 `source_registration`。（REQ-1787）[tool-verified: `provisa/api/admin/schema_mutation.py:827-920`]

#### `refreshSourceStatistics(sourceId: String!) → MutationResult`

对某数据源的所有已注册表运行 `ANALYZE`。改善联邦查询的连接顺序和广播决策。（REQ-276）[tool-verified: `provisa/api/admin/schema_mutation.py:2944-3008`]

---

### 表

#### `registerTable(input: TableInput!) → MutationResult`

将一张新表（或视图）注册到某个域中。需要 `table_registration` 能力以及目标域的成员身份。缺少 `create_relationship` 能力而提交视图的调用者，会被加入待权限持有者批准的创建请求队列。（REQ-013、REQ-016、REQ-252、REQ-434）[tool-verified: `provisa/api/admin/schema_mutation.py:1714-1718`]

#### `updateTable(input: TableInput!) → MutationResult`

更新一张现有表的别名、描述、列元数据、物化视图设置和实时投递配置。（REQ-016、REQ-020）需要 `table_registration`。[tool-verified: `provisa/api/admin/schema_mutation.py:1858-1995`]

#### `deleteTable(id: Int!) → MutationResult`

删除一张已注册的表。在删除前查找该表所属的域以进行域关卡检查。（REQ-1531）[tool-verified: `provisa/api/admin/schema_mutation.py:1998-2029`]

#### `updateTableCache(tableId: Int!, cacheTtl: Int) → MutationResult`

覆盖单张表的缓存 TTL。[tool-verified: `provisa/api/admin/schema_mutation.py:2353-2376`]

#### `updateTablePreferMaterialized(tableId: Int!, preferMaterialized: Boolean) → MutationResult`

覆盖单张表的物化联邦设置。`null` 表示继承数据源默认值。（REQ-826）[tool-verified: `provisa/api/admin/schema_mutation.py:2405-2428`]

#### `updateTableLoadProtection(tableId, loadProtected, offPeakWindow, offPeakTz) → MutationResult`

覆盖单张表的负载保护设置。`loadProtected` 为 `null` 时继承数据源默认值。验证有效的（表 → 数据源）关卡组合。（REQ-1141）[tool-verified: `provisa/api/admin/schema_mutation.py:2483-2566`]

#### `updateTableNaming(tableId: Int!, gqlNamingConvention: String) → MutationResult`

设置按表的 GraphQL 命名约定。[tool-verified: `provisa/api/admin/schema_mutation.py:2661-2685`]

#### `deployViewToDb(tableId: Int!) → MutationResult`

将一个虚拟 Provisa 视图提升为其底层原生数据源上的真实数据库视图。[tool-verified: `provisa/api/admin/schema_mutation.py:3058-3060`]

#### `forceRegen(tableId: Int!, reason: String!) → MutationResult`

按需重新计算一张表的落地行，绕过正常的变更关卡。`reason` 是必需的审计注释。对于实时联邦表（没有落地行）会被拒绝。（REQ-968）[tool-verified: `provisa/api/admin/schema_mutation.py:2689-2782`]

#### `invalidateFileSource(tableId: Int!) → MutationResult`

强制一张 SQLite 文件连接器表在下一次访问时从磁盘重新同步。[tool-verified: `provisa/api/admin/schema_mutation.py:2877-2880`]

#### `registerEntity(input: EntityInput!) → MutationResult`

注册维度/中心实体的语法糖。降级为一个（在历史化时为双时态的）物化视图，并调用 `registerTable`。（REQ-1164）[tool-verified: `provisa/api/admin/schema_mutation.py:1721-1725`]

#### `registerFact(input: FactInput!) → MutationResult`

注册星型架构事实表的语法糖。降级为一个聚合物化视图，创建维度关系，并将事实度量自动注册为受治理的指标。（REQ-1164、REQ-1320）[tool-verified: `provisa/api/admin/schema_mutation.py:1728-1776`]

---

### 关系

#### `upsertRelationship(input: RelationshipInput!) → MutationResult`

创建或更新一个关系。该关卡检查的是源表的域（而不是目标表的域）。跨域的边会以 `needsReview: true` 存储。缺少 `create_relationship` 能力的调用者会被加入创建请求队列。联结（多对多）边要求键列表长度匹配。（REQ-019、REQ-020、REQ-366、REQ-434）[tool-verified: `provisa/api/admin/schema_mutation.py:2297-2300`]

#### `deleteRelationship(id: String!) → MutationResult`

按 ID 删除一个关系并重建架构。[tool-verified: `provisa/api/admin/schema_mutation.py:2303-2322`]

---

### 域

#### `createDomain(input: DomainInput!) → MutationResult`

创建一个域。保留的字段词（`tables`、`relationships` 以及其他 URI 路径段）会被拒绝，通配符字面量 `*` 也是如此。需要 `org_settings` 能力。（REQ-021、REQ-1531）[tool-verified: `provisa/api/admin/schema_mutation.py:1135-1189`]

#### `deleteDomain(id: String!) → MutationResult`

删除一个域。需要 `org_settings`。（REQ-1531）[tool-verified: `provisa/api/admin/schema_mutation.py:1192-1213`]

#### `updateGqlNamingConvention(convention: String!) → MutationResult`

设置全局 GraphQL 命名约定，并为所有角色重建架构。只接受已识别的约定名称。（REQ-253、REQ-416）[tool-verified: `provisa/api/admin/schema_mutation.py:2571-2592`]

---

### 角色

#### `createRole(input: RoleInput!) → MutationResult`

创建或替换一个角色，包含能力、域访问权限、可选的速率限制以及可选的父角色。验证父角色是否存在，以及父级链是否不含循环。需要 `user_management`。（REQ-042、REQ-1531、REQ-1677）[tool-verified: `provisa/api/admin/schema_mutation.py:1657-1712`]

#### `deleteRole(id: String!) → MutationResult`

删除一个角色。如果其他角色继承自它则失败——请先重新指定它们的父级。需要 `user_management`。（REQ-1531、REQ-1677）[tool-verified: `provisa/api/admin/schema_mutation.py:2032-2063`]

---

### RLS 规则

#### `upsertRlsRule(input: RLSRuleInput!) → MutationResult`

创建或更新一条行级安全规则。过滤表达式会在保存时针对目标表或域的列进行验证，因此管理员无法查询的规则会连同原因一起被拒绝，而不是在查询时悄然失败。需要 `masking_config`。（REQ-041、REQ-402、REQ-1531、REQ-1676）[tool-verified: `provisa/api/admin/schema_mutation.py:2066-2136`]

目标是互斥的：为表级规则设置 `tableId`，为域级规则设置 `domainId`，或为跟踪的函数/webhook 设置 `actionName`。（REQ-1679）

#### `deleteRlsRule(roleId, tableId, domainId, actionName) → MutationResult`

删除一条 RLS 规则。需要 `masking_config`。（REQ-1531）[tool-verified: `provisa/api/admin/schema_mutation.py:2139-2178`]

---

### 数据产品

#### `createDataProduct(input: DataProductInput!) → MutationResult`

创建或替换一个数据产品。需要 `data_product_rw` 能力。（REQ-1634）[tool-verified: `provisa/api/admin/schema_mutation.py:1216-1259`]

#### `deleteDataProduct(id: String!) → MutationResult`

删除一个数据产品。需要 `data_product_rw`。（REQ-1634）[tool-verified: `provisa/api/admin/schema_mutation.py:1262-1285`]

---

### 标签

#### `upsertTag(input: TagInput!) → MutationResult`

创建或更新一个标签定义。系统标签和派生标签不能被重新定义。`appliesTo` 必须是 `["source", "table", "column", "relationship", "command"]` 的非空子集。（REQ-1373、REQ-1375）[tool-verified: `provisa/api/admin/schema_mutation.py:1288-1361`]

#### `deleteTag(id: String!) → MutationResult`

删除一个标签。拒绝删除系统标签和派生标签。（REQ-1373）[tool-verified: `provisa/api/admin/schema_mutation.py:1364-1393`]

#### `assignTag(input: TagAssignmentInput!) → MutationResult`

将一个标签分配给数据源、表、列、关系或命令。强制执行该标签的字段策略（`reason_policy`、`expires_policy`），并且——对于参数化标签——根据标签允许的列表验证参数值。（REQ-1376、REQ-1377）[tool-verified: `provisa/api/admin/schema_mutation.py:1396-1527`]

#### `unassignTag(input: TagAssignmentInput!) → MutationResult`

移除一个标签分配。（REQ-1377）[tool-verified: `provisa/api/admin/schema_mutation.py:1530-1564`]

#### `upsertTagParamValue(input: TagParamValueInput!) → MutationResult`

为一个参数化标签添加或重新描述一个允许的参数值。允许值列表是封闭的：每次分配都必须命名其中的一个值。（REQ-1467）[tool-verified: `provisa/api/admin/schema_mutation.py:1567-1616`]

#### `deleteTagParamValue(tagId: String!, value: String!) → MutationResult`

移除一个允许的值。只要仍有分配携带该值，就会被拒绝，因为那些分配将命名一个该列表已不再接受的类型。（REQ-1467）[tool-verified: `provisa/api/admin/schema_mutation.py:1619-1655`]

---

### 指标

#### `upsertMetric(input: MetricInput!) → MutationResult`

创建或替换一个受治理的指标定义。该表达式必须能在 sqlglot 下解析，且至少包含一个聚合函数。重新生成所有引用该指标的、由指标组成的视图。需要 `table_registration`。（REQ-1317、REQ-1318）[tool-verified: `provisa/api/admin/schema_mutation.py:1779-1831`]

#### `deleteMetric(name: String!) → MutationResult`

删除一个受治理的指标。重建架构。需要 `table_registration`。（REQ-1317）[tool-verified: `provisa/api/admin/schema_mutation.py:1834-1856`]

---

### 日历

#### `createCalendar(input: CalendarInput!) → MutationResult`

创建或替换一个带版本的快照边界日历。通过在持久化之前构造内存中的 `Calendar` 来验证——在未知的基础系统、错误的时区或错误的财年锚点上会失败。（REQ-962）[tool-verified: `provisa/api/admin/schema_mutation.py:525-579`]

#### `deleteCalendar(name: String!) → MutationResult`

删除一个日历（所有版本）。当任何物化视图引用它时会被拒绝。（REQ-962）[tool-verified: `provisa/api/admin/schema_mutation.py:582-613`]

---

### 物化视图

#### `refreshMv(mvId: String!) → MutationResult`

触发一次物化视图的手动刷新。当物化视图一致性模式为 `shared` 时会在整个集群范围内协调。（REQ-133、REQ-158、REQ-879）[tool-verified: `provisa/api/admin/schema_mutation.py:2787-2813`]

#### `toggleMv(mvId: String!, enabled: Boolean!) → MutationResult`

启用或禁用一个物化视图。[tool-verified: `provisa/api/admin/schema_mutation.py:2816-2839`]

---

### 缓存

#### `purgeCache → MutationResult`

清除所有缓存的查询结果。[tool-verified: `provisa/api/admin/schema_mutation.py:2844-2858`]

#### `purgeCacheByTable(tableId: Int!) → MutationResult`

清除单张表的缓存结果。[tool-verified: `provisa/api/admin/schema_mutation.py:2861-2875`]

---

### 计划任务

#### `createScheduledTask(id, name, cron, kind, webhookName, argsJson, sql) → MutationResult`

创建一个计划触发器——webhook 调用或 SQL 语句——并在 APScheduler 中实时注册它。`kind` 为 `"webhook"` 或 `"sql"`。（REQ-1003、REQ-1004）[tool-verified: `provisa/api/admin/schema_mutation.py:2923-2936`]

#### `deleteScheduledTask(taskId: String!) → MutationResult`

从配置和实时调度器中移除一个计划触发器。（REQ-1003）[tool-verified: `provisa/api/admin/schema_mutation.py:2939-2941`]

#### `toggleScheduledTask(taskId: String!, enabled: Boolean!) → MutationResult`

在配置文件中启用或禁用一个计划任务。[tool-verified: `provisa/api/admin/schema_mutation.py:2885-2920`]

---

### 数据质量

#### `dryRunDqContract(sourceId: String!, contractText: String!) → DqDryRunType`

针对实时表运行一个契约，并返回结果而不落地任何内容。之所以是一个变更而不是查询，是因为它会产生一次真实的扫描成本。它所验证的是数据集标识符是否能解析为操作员所指的受治理的表。（REQ-1443 第 7 条）[tool-verified: `provisa/api/admin/schema_mutation.py:463-487`]

#### `runDqCheckNow(schemaName: String!, tableName: String!) → MutationResult`

立即触发某个检查器表的轮询任务。以正常方式落地行，因此结果会持久化，并且 DQ 历史会显示新的扫描。（REQ-1443）[tool-verified: `provisa/api/admin/schema_mutation.py:490-522`]

---

### 架构维护

#### `rebuildSchemas → MutationResult`

从数据库状态重建内存中的架构。在外部数据库发生变更后很有用。[tool-verified: `provisa/api/admin/schema_mutation.py:456-461`]

---

### 创建请求

#### `executeCreationRequest(requestId: Int!) → MutationResult`

权限持有者执行一个排队的创建请求——关系、视图或 webhook。需要该请求所等待的能力。（REQ-434、REQ-063）[tool-verified: `provisa/api/admin/schema_mutation.py:2181-2255`]

#### `rejectCreationRequest(requestId: Int!, reason: String!) → MutationResult`

以一个可操作的原因拒绝一个排队的请求。`reason` 为必填。（REQ-434、REQ-063）[tool-verified: `provisa/api/admin/schema_mutation.py:2258-2294`]

---

### 查询编译

#### `compileQuery(input: CompileQueryInput!) → [CompileQueryResult!]!`

针对某个角色的架构编译一个数据平面 GraphQL 查询，并返回完整的路由决策：语义 SQL、引擎 SQL、直接 SQL、路由、强制执行元数据（已应用的 RLS 过滤器、被排除的列、已应用的脱敏），以及编译后的 Cypher。查询中每个根字段返回一个结果。（REQ-161）[tool-verified: `provisa/api/admin/schema_mutation.py:3011-3055`]

```graphql
mutation {
  compileQuery(input: {
    role: "analyst"
    query: "{ orders { id customer_id total } }"
  }) {
    sql
    semanticSql
    engineSql
    route
    routeReason
    enforcement {
      rlsFiltersApplied
      columnsExcluded
      maskingApplied
    }
  }
}
```

`CompileQueryInput` 字段：

| 字段 | 类型 | 说明 |
|-------|------|-------------|
| `query` | `String!` | 要编译的数据平面 GraphQL 查询 |
| `role` | `String!` | 要针对哪个角色的架构编译 |
| `variables` | `JSON` | 变量绑定 |
| `flatSql` | `Boolean` | 返回单个扁平化的 SQL 字符串，而不是语义/引擎一对 |
| `flatCypher` | `Boolean` | 扁平化 Cypher 输出 |
| `nodeOnlyCypher` | `Boolean` | 生成仅含节点的 Cypher（不含边模式） |

---

## 关键输入类型

### `SourceInput`

[tool-verified: `provisa/api/admin/types.py:590-611`]

| 字段 | 类型 | 说明 |
|-------|------|-------|
| `id` | `String!` | 数据源标识符 |
| `type` | `String!` | 连接器类型（例如 `postgres`、`files`、`openapi`） |
| `host` | `String` | |
| `port` | `Int` | |
| `database` | `String` | |
| `username` | `String` | |
| `password` | `String` | 明文或 `${secret:NAME}` 引用 |
| `path` | `String` | 文件/CSV 数据源的文件系统路径 |
| `federationHintsJson` | `String` | 用于数据仓库附加项的 JSON 对象（Snowflake 仓库/角色、Databricks http_path） |
| `changeSignal` | `String` | `ttl` \| `probe` \| `ttl_probe`（REQ-929） |
| `loadProtected` | `Boolean` | 仅按计划刷新（REQ-1141） |
| `offPeakWindow` | `String` | `HH:MM-HH:MM` 维护窗口 |
| `offPeakTz` | `String` | IANA 时区 |
| `cdc` | `SourceCdcConfigInput` | Kafka CDC 传输配置（REQ-824） |

### `TableInput`

[tool-verified: `provisa/api/admin/types.py:745-800`]

核心的表注册输入。基础字段之外的关键字段：

| 字段 | 说明 |
|-------|-------|
| `materialize` | 在物化存储中落地一份副本 |
| `mvRefreshInterval` | 刷新间隔（秒） |
| `mvPersist` | `replace` \| `append` \| `upsert`（REQ-965） |
| `mvIncremental` | 增量维护（REQ-969） |
| `mvBitemporalMode` | 双时态表的 `snapshot` \| `delta`（REQ-1162） |
| `mvCalendar` | 快照日历名称（REQ-962） |
| `mvGrain` | 快照粒度：`daily`、`weekly`、`monthly`、`annual`，或自定义的 `3WE` / `LFR`（REQ-962） |
| `viewSql` | 派生视图的 SQL |
| `viewMetrics` | 声明式的、由指标组成的视图规范——与 `viewSql` 互斥（REQ-1318） |
| `dqContract` | YAML/JSON 数据质量契约文本（REQ-1443） |
| `queryTemplate` | Neo4j 表的 Cypher（REQ-1670） |
| `live` | SSE/Kafka 推送的实时投递配置（REQ-565、REQ-813） |
| `discover` | 在注册时从实时数据源推断列（REQ-252） |

### `RelationshipInput`

[tool-verified: `provisa/api/admin/types.py:804-826`]

| 字段 | 说明 |
|-------|-------|
| `id` | 关系标识符 |
| `sourceTableId` | 虚拟表名称（若已设置则为别名，否则为表名） |
| `targetTableId` | 虚拟表名称；对于计算得出的关系为空 |
| `sourceColumn` | 源端的连接列 |
| `targetColumn` | 目标端的连接列 |
| `cardinality` | `one-to-one` \| `one-to-many` \| `many-to-one` \| `many-to-many` |
| `alias` | Cypher 边标签（例如 `WORKS_FOR`） |
| `graphqlAlias` | 源类型上的 GraphQL 字段名称 |
| `viaTable` | 多对多边的联结表名称（REQ-1586） |
| `recordCandidate` | 同时写入一条 `accepted` 状态的 relationship_candidates 行 |

---

## 示例：注册一张表

```graphql
mutation {
  registerTable(input: {
    sourceId: "sales-pg"
    domainId: "sales"
    schemaName: "public"
    tableName: "orders"
    alias: "orders"
    columns: [
      { name: "id", visibleTo: ["public"], isPrimaryKey: true }
      { name: "customer_id", visibleTo: ["public"] }
      { name: "total", visibleTo: ["public"] }
    ]
  }) {
    success
    message
    code
  }
}
```

## 示例：创建一条 RLS 规则

```graphql
mutation {
  upsertRlsRule(input: {
    tableId: "orders"
    roleId: "regional-analyst"
    filterExpr: "region = '{{user.region}}'"
  }) {
    success
    message
  }
}
```
