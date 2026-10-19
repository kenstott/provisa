# API 参考 {: #api-reference }

## 概述 {: #overview }

Provisa 在两个前缀下暴露 REST 端点：`/data` 用于查询执行和架构自省，`/admin` 用于配置管理。 大多数数据端点需要一个角色标识符。管理配置操作使用 `/admin/graphql` 处的 Strawberry GraphQL API。

---

## 身份验证 {: #authentication }

当 `provisa.yaml` 中配置了 `auth.provider` 时，除 `/health` 和 `/setup/status` 外的所有端点都需要 `Authorization: Bearer <token>` 请求头。

未配置身份验证时，服务器以开发模式运行。任何请求都被视为 `anonymous` 身份，该身份映射到所有已配置角色并具备通配符域访问权限。

配置 `provider: basic` 时，**登录（`POST /auth/login`）** 由活动的身份验证提供程序提供。 凭据格式和响应因提供程序而异。

**身份自省：**

```http
GET /auth/me
```

返回已验证用户的 id、电子邮件、显示名称、组织成员身份和角色分配。在开发模式下返回 `dev_mode: true` 并列出所有角色 ID。

```http
GET /auth/provider-type
```

返回 `{"provider": "<name>"}`，未配置身份验证时返回 `{"provider": null}`。

---

## 数据端点 {: #data-endpoints }

### `POST /data/graphql`

执行 GraphQL 查询或变更。

**请求体：**

```json
{
  "query": "{ orders(where: {region: {eq: \"us\"}}) { id amount } }",
  "variables": {},
  "role": "org_admin",
  "extensions": {}
}
```

请求以其执行角色运行：已验证用户的角色，或在开发模式（无身份验证）下取 `X-Provisa-Role` 标头，缺省时为 `org_admin`。正文中的 `role` 字段不选择角色。若存在，它必须与执行角色一致；不同的值返回 `400`，代码为 `data.role_mismatch`，并指明两个角色。同一规则适用于 `POST /data/sql`、`POST /data/query` 和 `POST /query/nl`。

`extensions` 字段支持自动持久化查询（APQ）协议：

```json
{
  "extensions": {"persistedQuery": {"sha256Hash": "<sha256-of-query>"}}
}
```

**请求头：**

- `X-Provisa-Role` — 要代为执行的角色（开发模式）；启用身份验证时为角色请求，仅当该角色已分配给用户时才生效
- `Accept` — 响应格式（参见内容协商）
- `Authorization` — 启用身份验证时为 `Bearer <token>`
- `X-Provisa-Redirect-Format` — S3 重定向输出的 MIME 类型
- `X-Provisa-Redirect-Threshold` — 触发重定向的行数阈值
- `X-Provisa-Redirect` — `true` 表示无条件强制重定向

**响应缓存：** 通过操作级指令 `@cached` 按请求选择启用（`query @cached(ttl: 60) { ... }`，`ttl` 可选）；同一指令也适用于经 Arrow Flight 发送的 GraphQL。Cypher 以 `// @provisa cache=true` 或 `// @provisa cache_ttl=N` 行选择启用，gRPC 以调用元数据 `x-provisa-cache: true` 或 `x-provisa-cache-ttl: N` 选择启用。没有该指令时，请求既不读取也不写入响应缓存。响应头 `X-Provisa-Cache` 报告 `HIT` 或 `MISS`。请参阅[缓存](configuration.md#per-request-opt-in)。

<a id="response-cache-request-headers"></a>

#### 响应缓存请求头 {: #response-cache-request-headers }

`/data/graphql`、`/data/cypher`、Neo4j Query API（`/db/{database}/query/v2`）、`/data/rest` 和 `/data/jsonapi` 也接受以请求头形式选择启用。下列每种形式的行为都与 `@cached(ttl: N)` 相同：

| 请求头 | 等同于 |
| --- | --- |
| `Cache-Control: max-age=N` / `Cache-Control: max-stale=N` | `@cached(ttl: N)` |
| `Cache-Control: max-stale`（无值） | `@cached`（运营方的 TTL） |
| `Cache-Control: no-cache` / `no-store` | 不缓存，与不发送任何内容相同 |
| `x-provisa-cache: true` | `@cached` |
| `x-provisa-cache-ttl: N` | `@cached(ttl: N)` |

没有上述请求头、文本中也没有提示的请求不会被缓存，尽管普通 HTTP 缓存默认会缓存。运营方的缓存设置仍与 `@cached` 一样适用：请求头不会使读取比这些设置允许的更新，也不会因为要求更长的 `ttl` 而被拒绝。其他 `Cache-Control` 指令（`no-transform`、`min-fresh` 等）会被忽略，这是 RFC 9111 对缓存的要求。

一个请求可以多次声明选择启用：在其文本中和请求头中，或在两个请求头中。若一致，则按要求缓存一次。若不一致（例如 `@cached(ttl: 60)` 搭配 `Cache-Control: max-age=5`，或 `x-provisa-cache-ttl: 10` 搭配 `Cache-Control: max-age=20`），请求将以 `400` 和错误码 `data.invalid_cache_control` 失败，并指明所发送的每种形式。`N` 不是整数秒的值同样失败。不会偏向任何一种形式。

```bash
curl -H 'Cache-Control: max-age=120' -H 'X-Provisa-Role: analyst' \
  'http://localhost:8001/data/rest/sales/orders?limit=10'
```

**响应（内联 JSON）：**

```json
{
  "data": {
    "orders": [
      {"id": 1, "amount": 99.99}
    ]
  }
}
```

**响应（重定向）：**

```json
{
  "data": {"orders": null},
  "redirect": {
    "redirect_url": "https://...",
    "row_count": 50000,
    "expires_in": 3600,
    "content_type": "application/vnd.apache.parquet"
  }
}
```

**响应（内联/重定向混合的多根查询）：**

```json
{
  "data": {
    "orders": [{"id": 1}],
    "customers": null
  },
  "redirects": {
    "customers": {
      "redirect_url": "https://...",
      "row_count": 10000,
      "expires_in": 3600,
      "content_type": "application/vnd.apache.parquet"
    }
  }
}
```

多根查询独立运行每个根字段。低于重定向阈值的字段以内联方式返回；超过阈值的字段则重定向。`redirects` 键（复数）将字段名映射到重定向信息。

**缓存请求头：**

- `X-Provisa-Cache: HIT|MISS`
- `X-Provisa-Cache-Age: <seconds>`（命中时）

**所需能力：** 所有请求（包括自省）均需要 `QUERY_DEVELOPMENT`。

---

### 内容协商 {: #content-negotiation }

| Accept 请求头 | 格式 |
| --- | --- |
| `application/json` | JSON（默认） |
| `application/x-ndjson` | 换行分隔 JSON |
| `text/csv` | CSV |
| `application/vnd.apache.parquet` | Parquet |
| `application/vnd.apache.arrow.stream` | Arrow IPC |

---

### 重定向 {: #redirect }

超过配置行数阈值的结果（或当 `X-Provisa-Redirect: true` 时）会写入 S3，并返回一个预签名 URL。

| 重定向格式 | 写入方 | 内存占用 |
| --- | --- | --- |
| `application/vnd.apache.parquet` | 联邦 CTAS | 无——数据从不经过 Provisa |
| `application/x-orc` | 联邦 CTAS | 无——数据从不经过 Provisa |
| `application/json` | Provisa | 受内存限制 |
| `application/x-ndjson` | Provisa | 受内存限制 |
| `text/csv` | Provisa | 受内存限制 |
| `application/vnd.apache.arrow.stream` | Provisa | 受内存限制 |

对于大型分析导出，请使用 Parquet 或 ORC 重定向。联邦引擎并行直接写入 S3——数据不经过 Provisa。

```yaml
X-Provisa-Redirect-Format: application/vnd.apache.parquet
X-Provisa-Redirect-Threshold: 1000
```

---

### `POST /data/sql`

通过第二阶段治理管道执行原始 SQL。

**请求体：**

```json
{
  "sql": "SELECT id, amount FROM orders WHERE region = 'us'",
  "role": "org_admin"
}
```

`params`（可选）按顺序携带语句 `$1…$n` 占位符的值，与其文本分开：

```json
{
  "sql": "SELECT id, amount FROM orders WHERE region = $1 AND amount > $2",
  "params": ["us", 100]
}
```

这些值在语句运行时由引擎或数据源的驱动绑定，从不写入语句，因此值无法改变语句本身。它们只属于一条语句，其占位符精确地为它们编号：语句批次、没有值的占位符，或没有占位符接收的值，都会在语句被治理或运行之前以 `400` 和代码 `data.sql_parameters_do_not_fit` 被拒绝。没有占位符的语句不带 `params` 发送，与之前一样。

**所需能力：** `QUERY_DEVELOPMENT`。

`POST /data/sql` 上的治理违规返回 HTTP 403。

**响应：** 与 `/data/graphql` 格式相同（默认按 JSON 行返回，通过 `Accept` 进行内容协商）。

---

### `POST /data/query`

统一查询端点。接受 GraphQL、SQL 或 Cypher——语法自动检测。

Cypher 查询也可以提交到仅支持 Cypher 的 `POST /query/cypher` 端点。

**请求体：**

```json
{
  "query": "{ orders { id } }",
  "params": {},
  "variables": {},
  "role": "org_admin"
}
```

GraphQL 返回 `{"data": ...}`，SQL 和 Cypher 返回 `{"columns": [...], "rows": [...]}`。

---

### `POST /data/sql/explain`

通过治理管道解释或分析一条 SQL 语句。

该端点将**治理后**的 SQL——即在调用者角色下、经过行级安全与脱敏处理后实际运行的语句——包装在对应方言的 EXPLAIN 语法中。计划展示的是查询的授权版本，而不是原始输入。

**请求体：**

```json
{
  "sql": "SELECT id, amount FROM orders",
  "role": "org_admin",
  "analyze": false
}
```

设置 `analyze: true` 以运行 EXPLAIN ANALYZE。查询会实际执行，计划中包含真实的行数和耗时。并非所有方言都支持 ANALYZE；参见 [查询计划与统计信息](engines.md#query-plans-and-statistics) 中的表格。

**响应：** `{"plan": "<plan text or JSON>", "dialect": "trino", "analyzed": false}`

当方言不支持 EXPLAIN，或在不支持 `analyze: true` 的方言（如 SQLite）上请求时，返回 `400`。

---

### `GET /data/engine/state`

返回引擎分片的当前状态，而不唤醒它。

UI 会轮询此端点，在引擎冷启动期间显示启动横幅。它从不触发唤醒——轮询是安全的，也不会被空闲回收器计为活动。

**响应：**

```json
{"state": "ready"}
```

可能的取值：

| 状态 | 含义 |
| --- | --- |
| `always-on` | 桌面版、自托管或自带协调器——无生命周期管理 |
| `ready` | 分片已启动并接受查询 |
| `starting` | 冷启动进行中 |
| `stopped` | 分片已缩容至零 |

---

### `POST /data/engine/prewarm`

触发引擎唤醒而不运行查询。

立即返回 `202 Accepted`。唤醒过程在后台运行。如果希望在第一个查询到达前引擎已就绪——例如调度程序会在几分钟后运行查询——可使用此端点。

**响应：** `202 Accepted`，响应体 `{"started": true}`

---

### `GET /data/rest/{domain_id}/{table_name}`

为每个已注册表自动生成的纯 REST 端点。查询字符串映射到 GraphQL 参数，请求通过与 GraphQL 相同的管道（行级安全、脱敏、路由）编译和执行。

**查询参数：**

- `limit` — 最大行数（≥ 1）
- `offset` — 跳过的行数（≥ 0）
- `fields` — 以逗号分隔的字段名，采用 GraphQL schema 的拼写，在默认命名约定下为 camelCase，例如 `?fields=orderId,amount`（默认：所有标量字段）。`filter` 和 `orderBy` 中的字段名遵循相同拼写。JSON:API 则接受物理列名（`fields[orders]=order_id`）。
- `filter` — `{"field", "comparator", "value"}` 过滤对象的 JSON 数组。值按其列的类型读取，因此数值列上的 `"100"` 按 100 比较；列类型无法容纳的值返回 `400`。
- `orderBy` — `{"field", "direction"}` 排序对象的 JSON 数组
- `params` — 表的参数，作为一个按参数名索引的 JSON 对象，例如 `?params={"region":"EU"}`。基于 API 注册的表或查询表，其源接受的每个输入都有一个参数。缺少必填参数的读取返回 `400` 并指出其名称，表中没有的参数名同样如此。每个值都按其参数的类型检查。

过滤值和参数值作为变量随端点构建的 GraphQL 文本一起传递，因此含引号的值按原样比较。

需要已验证的角色；未验证的请求返回 `401`。这些路由的 OpenAPI 规范在 `GET /data/rest/openapi.json` 提供，Swagger UI 在 `GET /data/rest/docs` 提供。

**响应缓存：** 通过请求头选择启用，`Cache-Control: max-age=N` 或 `x-provisa-cache: true`（参见[响应缓存请求头](#response-cache-request-headers)）。每个响应都带有 `X-Provisa-Cache: HIT|MISS`，命中时还带有 `X-Provisa-Cache-Age`。

#### OpenAPI / Swagger UI 浏览器 {: #openapi-swagger-ui-explorer }

OpenAPI 浏览器页面（`/app/openapi`）在一个沙盒 iframe 中嵌入 Swagger UI。该规范按角色范围限定——只有当前角色可见的表和列会出现——并可通过域选择器进一步按域过滤。UI 会自动在浅色和深色主题之间切换。

该页面通过 `fetch()` 而不是直接的 iframe `src` 加载规范 HTML，因此请求会携带会话的持有者令牌，且 Swagger UI 自身的相对请求能正确解析到同一源。

当从 NL「在 OpenAPI 中打开」链接导航而来时，页面会自动展开目标端点，从 NL 生成的 URL 中填充查询参数（例如 `aggregate`、`groupBy`），并点击 Execute——使用 DOM 轮询确保每一步在下一步触发前完成。

---

### `GET /data/jsonapi/{domain_id}/{table_name}`

为每个已注册表自动生成的符合 [JSON:API](https://jsonapi.org) 规范的端点。与 GraphQL 相同的行级安全、脱敏和路由。

**`Accept` 请求头：** 必须包含 `application/vnd.api+json`（JSON:API 媒体类型），否则请求返回 `406`。

**查询参数：**

- `fields[<type>]` — 稀疏字段集，例如 `?fields[orders]=amount`。名称是物理列名，例如 `fields[orders]=order_id`，与 `filter[...]`、`sort` 以及返回属性的键相同。REST 则接受 GraphQL 拼写（`fields=orderId`）。
- `filter[<col>]` / `filter[<col>][<op>]` — 例如 `?filter[region]=US`、`?filter[amount][gt]=100`。值按其列的类型读取；类型无法容纳的值返回 `400`。
- `params` — 表的参数，作为一个按参数名索引的 JSON 对象，例如 `?params={"region":"EU"}`，与 REST 端点相同。缺少必填参数返回 `400` 并指出其名称；未知名称或类型错误的值返回 `400`，`source.parameter` 为 `params`。分页链接保留该参数。
- `sort` — 逗号分隔，`-` 前缀表示降序，例如 `?sort=-created_at,amount`
- `page[number]` / `page[size]` — 分页
- `page[total]` — `true` 统计与过滤条件匹配的行数，并返回 `meta.total` 和 `links.last`。不带它时，响应只携带 `links.self`、`links.first`、`links.prev` 和 `links.next`，数据库不执行计数。`true` 或 `false` 以外的任何值返回 `400`。响应中的链接保留该参数。
- `aggregate` — 逗号分隔的聚合函数，用于代替行检索：`count`、`sum`、`avg`、`stddev`、`variance`、`min`、`max`。使用 `?aggregate=count,sum` 请求子集。聚合响应返回 `data: null`，结果放在 `meta.aggregate` 中。
- `groupBy` — 逗号分隔的列名；与 `?aggregate=` 一起使用以对结果分组。只有表的 `DistinctOnColumn` 枚举中的列有效；对角色不可见的列，服务器返回 `400`。
- `includeNodes` — `true` 表示在每个分组行的 `nodes` 数组中包含基表标量列（以及 `include=` 中命名的已联接维度标量）。当 NL 分组查询同时请求维度详情时需要此参数。

- `include` — 以逗号分隔的关系名称，使用物理拼写，例如 `?include=order_items,shipment`：每个相关资源在 `included` 中只返回一次，每个主资源在 `relationships.<name>.data` 下链接它包含的内容（一对多关系为资源标识符列表，一对一关系为一个标识符或 `null`）。

响应是带有 `type`/`id`/`attributes` 的资源对象。资源的 `id` 是其表的主键：键列的值；对于复合键，则是按键顺序以 `:` 连接的各列值（主键为 `(order_id, line)` 的表中，订单 7 的第 2 行是 `7:2`）。值中的 `:` 或 `\` 用 `\` 转义。没有声明主键的表使用其 `id` 列；两者都没有的表返回 `400`，到达这种表的 `include` 也是如此。 错误遵循 JSON:API 错误对象的格式。

**响应缓存：** 通过请求头选择启用，`Cache-Control: max-age=N` 或 `x-provisa-cache: true`（参见[响应缓存请求头](#response-cache-request-headers)）。每个响应都带有 `X-Provisa-Cache: HIT|MISS`，命中时还带有 `X-Provisa-Cache-Age`。两种形式不一致时，以 JSON:API 错误对象返回 `400`。

#### JSON:API 浏览器 {: #jsonapi-explorer }

JSON:API 浏览器页面（`/app/jsonapi`）是这些端点之上的浏览器 UI。从按域分组的列表中选择一张表，然后配置：

- **字段** — 选择要包含的列（稀疏字段集）；全部取消勾选则请求所有列
- **关系** — 选择通过 `?include=` 侧载的、由外键派生的关系名
- **过滤** — 字段、运算符（`eq`、`neq`、`gt`、`gte`、`lt`、`lte`、`like`）和取值
- **排序** — 一个字段，升序或降序
- **聚合** — 从服务器校验后的列表中选择分组列，再勾选一个或多个聚合函数；选择分组列后会出现「包含节点」复选框，用于将基表标量列附加到每一行
- **每页数量** — 每页资源数，带首页/上一页/下一页/末页导航

结果以格式化摘要视图（带可点击关系锚点的资源卡片）或原始 JSON 标签页呈现。实时请求 URL 会显示并可复制。表选择和每页数量在会话间通过 `localStorage` 持久化。

当从 NL「在 JSON:API 中打开」链接导航而来时，浏览器会预选该表，并从 NL 生成的查询参数中为聚合选择器填充种子值，然后自动运行请求。

---

### `POST /query/nl`

提交一个自然语言问题。服务会启动一个异步任务，并立即返回带有 `job_id` 的 `202 Accepted`。需要在 `ai_models` 配置节下配置 LLM 提供程序。

**请求体：**

```json
{"q": "How many orders were placed last month?", "role": "org_admin"}
```

返回 `{"job_id": "<id>"}`。超出每角色 NL 速率限制时返回带 `Retry-After` 请求头的 `429`。

**获取结果：**

- `GET /query/nl/{job_id}` — 轮询。返回任务文档。
- `GET /query/nl/{job_id}/stream` — SSE。每个生成目标完成时触发一次 `branch` 事件，最后触发一次 `done` 事件。

三个生成循环（Cypher、GraphQL、SQL）并行运行，每个都经过编译器验证并在出错时进行修正。 提示词的范围限定在该角色可见的架构内。 结果文档按目标为每个分支设置键：

```json
{
  "job_id": "<id>",
  "state": "complete",
  "branches": {
    "cypher":  {"query": "MATCH ...", "result": [...], "error": null},
    "graphql": {"query": "{ ... }",   "result": {...}, "error": null},
    "sql":     {"query": "SELECT ...", "result": [...], "error": null}
  }
}
```

耗尽迭代次数上限的分支返回 `query: null`、`result: null`，并附带一个 `error` 字符串。每个生成的查询都在使用者的权限下执行，并应用第二阶段治理——该服务从不绕过治理。

#### 带维度详情的 NL 分组查询 {: #nl-group-by-with-dimension-details }

当 NL 分组查询还从已联接的维度表中投影列时——例如「按用户统计的询价数量，附带用户名和邮箱」——运行器会从 SELECT 投影的维度列中派生每字段的点路径（`dim_paths`）。这些路径填充到 JSON:API 和 OpenAPI 面板生成的 URL 上的 `includeNodes=` 参数中，使这些面板请求与 SQL 和 GraphQL 分支所解析出的相同已联接维度字段。若没有此机制，`includeNodes=true` 将只返回基础聚合表自身的标量字段。

在 gRPC 面板上，生成的 `{Type}GroupByRequest` 携带 `include_nodes`（布尔值）和 `include`（关系字段名的重复字符串）。返回的 `{Type}GroupByRow` 包含一个带有维度详情行的类型化 `nodes` 字段。

---

### `GET /data/sdl`

返回某个角色架构的 GraphQL SDL。

**请求头：** `X-Provisa-Role: <role_id>`（必需）

**查询参数：**

- `domain` — 逗号分隔的域 ID。设置后，响应会过滤为所指定域及其可达表。

**响应：** `text/plain` GraphQL SDL。

---

### `GET /data/introspection`

返回 GraphQL 自省 JSON，可选按域过滤。

**请求头：** `X-Provisa-Role: <role_id>`（必需）

**查询参数：** `domain` — 逗号分隔的域 ID。

**响应：** `application/json` 自省结果。

---

### `GET /data/graph-schema`

返回角色架构的图视图：节点标签及其关系类型，供 Cypher/图客户端使用。包含每个节点标签的 `pk_columns`，以便调用方确定主键列。

**响应：** `application/json`，含 `node_labels`（每个携带 `pk`/`pk_columns`）和 `relationship_types`。

每种关系类型还携带 `junction_table_name` 和 `properties`。在联结表支撑的边上，前者命名它所遍历的关联表，后者列出该表中可作为 `r.attr` 读取并可在 `WHERE` 中过滤的列；在外键支撑的边上，名称为 `null` 且属性列表为空，这就是客户端区分两者的方式。联结表本身从不是节点标签——它是边，因此在图客户端中没有对应的图钉，在 `node_labels` 中也没有对应的行。

---

### `GET /data/domains`

返回请求角色可访问的域 ID。

**请求头：** `X-Provisa-Role: <role_id>`（必需）

**响应：** `["sales", "support", ...]`

---

### `GET /data/schema-version`

返回当前的架构版本字符串。结合了每次启动的随机数与重建计数器。客户端用此值在服务器重启后使架构缓存失效。

**响应：** `{"version": "<boot-id>-<counter>"}`

---

### `GET /data/catalog`

返回请求所执行角色的目录：提供给该角色的表，与 Arrow Flight 列表给出的形式相同。

**响应：** `{"tables": [{"path": ["<domain>", "<table>"], "schema": "<base64>"}]}`。`schema` 是该表以 Arrow IPC 编码的 Arrow schema：每一列以 SQL 读取它所用的名称出现，字段元数据中带有其注册类型、描述和键元数据（`primary_key`、`references`）。命令和指标不列出。

角色是身份验证层确定的角色（`X-Provisa-Role` 可以请求一个已持有的角色或逗号分隔的集合）。没有身份验证提供程序的部署会列出整个目录，与 Flight 列表相同。当 Flight 端口不可达时，JDBC 驱动读取此路由。

---

### `GET /data/proto/{role_id}`

返回某个角色自动生成的 `.proto` 文件。

**响应：** `text/plain` protobuf 架构。

`{role_id}` 是调用方持有的一个角色，或以逗号分隔的一组已持有角色（`analyst,org_admin`），该组以其元角色的身份提供服务：即其成员所获授权的并集。调用方未持有的角色以 `403 auth.role_not_assigned` 拒绝，直接指定的元角色 id 以 `403 auth.meta_role_named` 拒绝。`?domains=a,b` 将 proto 限定到这些域；角色无法访问的域以 `403 data.domain_not_accessible` 拒绝。没有数据面的角色返回 `404 data.no_proto_for_role`，未知角色返回 `404 data.no_role`，模型尚未构建时返回 `503 data.schema_cache_not_ready`。

角色 schema 中的每张表都生成一个 proto `message`。关系生成嵌套的消息字段。 服务只为角色架构根部的表列出 RPC，即其 GraphQL `Query` 类型所列的那些表；关联表有自己的消息，并通过其关系访问。

添加 `?domains=a,b` 可将文件限制在这些域内。此时服务为所选域自身的表列出 RPC；与之关联的表以消息形式出现。

该文件随模型而变。每当模型或角色发生变化时，它会随角色的架构一起重新生成，元角色也是如此。类型映射：`integer → int32`、`bigint → int64`、`varchar → string`、`decimal → double`、`boolean → bool`、`timestamp → google.protobuf.Timestamp`。

---

### `GET /data/subscribe/{table}`

用于表实时变更通知的服务器发送事件（SSE）流。

通知投递使用按源类型选择的可插拔提供程序：PostgreSQL 源使用 `LISTEN/NOTIFY`（通过 asyncpg）、MongoDB 源使用变更流（`collection.watch()`）、Kafka 源使用消费者组。每个提供程序实现一个通用的异步监听接口。无论使用哪种提供程序，都会应用行级安全过滤和架构验证。 同时也支持 WebSocket 和 RSS 源。

**请求头 —— `X-Provisa-Sink`：** 设置为一个 Kafka 目标（例如 `kafka://broker:9092/topic`）以将变更事件重定向到 Kafka 接收端，而不是 SSE 响应。服务器会启动一个接收端消费者并返回 `202 Accepted`，而不是打开一个流。

### `POST /data/sources/{source_id}/signal`

在不带行的情况下告诉 Provisa 某个数据源已变化：`signal` 变更信号的接收端。数据源中变更信号为 `signal` 的每张表（自身的，或在没有自身信号时为数据源的）都被视为已写入，与通过 Provisa 写入相同：其缓存的应答被丢弃，基于它的物化视图变为过期，并请求构建其副本。受负载保护的表的构建会等待其非高峰窗口。`?table=schema.table` 只通知一张表。

调用者的角色需要写入权，并且必须覆盖它所通知的每张表的域。响应为 `202 Accepted`，附带被通知的表：

```json
{"signalled": ["public.orders"]}
```

不存在的数据源，或它没有的 `table`，应答 `404`；`signal` 变更信号上没有表的数据源应答 `409`，因为没有可刷新的内容。

---

## 管理 REST 端点 {: #admin-rest-endpoints }

### 配置 {: #config }

#### `GET /admin/config`

将当前的 `provisa.yaml` 下载为 `application/x-yaml`，并带有 `Content-Disposition: attachment` 请求头。

#### `PUT /admin/config`

上传修订后的配置 YAML。服务器写入 `.bak` 备份，保存新文件，从中重新加载部署设置，并把它作为显式的一次性填充应用到模型存储：文件声明的内容通过模型存储添加或更新，不移除任何内容。之后的重启会读取文件的设置，但从不再次应用其模型；首次启动之后，只有模型存储拥有模型。

**请求体：** 原始 YAML 内容。

**响应：**

```json
{"success": true, "message": "Config uploaded and applied"}
```

重新加载失败时：`{"success": false, "message": "<error>"}`。

#### `GET /admin/config/live`

以配置文件的形式下载当前的模型：部署文件的设置，以及从模型存储读取的每个模型部分——存储、区域、数据源、域、角色、数据产品、术语表术语、表及其列、关系、指标、标签及其分配、行过滤、命令、webhook、定时触发器和命名规则。应用到空的模型存储时，该文件会构建出相同的模型。部署为每个存储添加的内容（内置数据源及其表、系统域、`org_admin` 和 `platform_admin`）不包括在内。

**查询参数：** `sections`（可选）——以逗号分隔的模型部分列表，例如 `sources,tables`。只写出这些部分；未知名称应答 400 `settings.export_unknown_section`。

该端点在任何部署中都对拥有部署设置权利的调用者开放；管理页面的配置文件区域以**导出模型**提供它。

返回 `application/x-yaml`，带 `Content-Disposition: attachment; filename=provisa.live.yaml`。

#### `GET /admin/config/diff`

返回配置差异的两侧——`original`（启动时的基线）和 `current`（实时状态）——经过相同的规范化处理，因此比较结果只显示真实的变更，而不是重新排序或注释漂移。

**响应：**

```json
{"original": "<yaml>", "current": "<yaml>"}
```

#### `POST /admin/config/patch`

从基线到所提交配置生成统一差异补丁。

在请求体中发送修订后的 YAML。响应是一个 `text/x-patch` 文件（`provisa.config.patch`），`git apply` 或 `patch` 可以直接使用——便于通过 CI/CD 流水线提交 UI 驱动的配置变更。

---

### 设置 {: #settings }

#### `GET /admin/settings`

以 JSON 返回当前的平台设置。

**响应：**

```json
{
  "redirect": {
    "enabled": true,
    "threshold": 10000,
    "default_format": "application/vnd.apache.parquet",
    "ttl": 3600
  },
  "sampling": {
    "default_sample_size": 1000
  },
  "cache": {
    "default_ttl": 300
  },
  "naming": {
    "domain_prefix": false,
    "convention": "apollo_graphql"
  },
  "relationships": {
    "auto_track_fk": true
  },
  "otel": {
    "endpoint": "http://otel-collector:4318",
    "service_name": "provisa",
    "sample_rate": 1.0,
    "support_endpoint": "",
    "support_redact_sql_literals": true,
    "support_redact_attributes": []
  }
}
```

#### `PUT /admin/settings`

在运行时更新平台设置。所有字段都是可选的——只有请求体中出现的键才会被更新。

**请求体（部分示例）：**

```json
{
  "otel": {
    "support_endpoint": "https://telemetry.vendor.com/v1/traces",
    "support_redact_sql_literals": true,
    "support_redact_attributes": ["db.statement", "user.email"]
  },
  "cache": {"default_ttl": 600}
}
```

各节的可更新字段：

- `redirect`：`enabled`、`threshold`、`default_format`、`ttl`
- `sampling`：`default_sample_size`
- `cache`：`default_ttl`
- `naming`：`domain_prefix`、`convention` —— 写入配置文件并触发架构重新加载
- `relationships`：`auto_track_fk` —— 仅控制外键跟踪。联结表支撑的关系是在表注册时声明的，从不进行推断，因此此设置不影响它。
- `otel`：`endpoint`、`service_name`、`sample_rate`、`support_endpoint`、`support_redact_sql_literals`、`support_redact_attributes`

**响应：**

```json
{"success": true, "updated": ["otel.support_endpoint", "cache.default_ttl"]}
```

---

### AI 模型 {: #ai-models }

#### `GET /admin/ai-models`

返回当前操作组织的 AI 模型分配、向量模型注册表和 NL 速率限制。

**响应：**

```json
{
  "ai_models": {
    "nl": "claude-3-5-sonnet-20241022",
    "embedding": "text-embedding-3-small"
  },
  "vector_models": [...],
  "nl": {"rate_limit": 20},
  "api_keys_set": {"anthropic": true, "openai": false}
}
```

API 密钥从不回显——`api_keys_set` 仅报告每个厂商是否配置了密钥。更改在下一次请求时生效；无需重启。

#### `PUT /admin/ai-models`

更新组织的 AI 模型分配、向量模型注册表或 NL 速率限制。在下一次请求时生效。

#### `GET /admin/ai-models/vendors/{vendor}/models`

返回某个厂商当前提供的模型名称，用于模型选择器。

该列表使用组织配置的密钥（若未设置组织密钥则使用部署凭据）从厂商自身的模型列表 API 实时读取。此构建发布之后新发布的模型，在厂商开始提供的当天即可选用。

当厂商未发布模型列表 API 时（此时请直接输入模型名称）或没有可用密钥时，返回 `400`。

---

### 联邦引擎 {: #federation-engine }

#### `GET /admin/federation-engine`

返回当前的联邦引擎选择、其连接配置以及完整的可选引擎注册表。

**响应：**

```json
{
  "current": "trino",
  "persisted": "trino",
  "registry": [
    {"key": "trino", "label": "Trino (embedded)", "fields": [...]},
    {"key": "duckdb", "label": "DuckDB", "fields": []}
  ],
  "note": "Changing the federation engine takes effect after the service is restarted."
}
```

`current` 键是当前正在运行的引擎；`persisted` 是写入配置文件、将在下次重启时加载的引擎。当配置已更改但服务尚未重启时，两者会出现分歧。

#### `PUT /admin/federation-engine`

持久化一个联邦引擎选择。

**请求体：**

```json
{"engine": "trino", "federation_engine_url": "http://trino-coordinator:8080"}
```

该选择被写入平台配置。它在下一次服务重启后生效——引擎在启动时确定一次。

---

### 域策略 {: #domain-policy }

#### `POST /admin/domain-policy`

更改当前操作组织的域策略（`use_domains` / `default_domain`）。

切换不会删除任何内容。只要当前组织拥有目录——部署未自行添加的已注册表、数据源或域——它就以 `409` 和代码 `settings.domain_policy_catalog_exists` 被拒绝。拒绝的 `params` 统计每一类（`tables`、`sources`、`domains`）。先通过各自的操作移除这些对象，再进行切换。当配置文件声明了该目录的一部分时，代码为 `settings.domain_policy_catalog_in_config`：下一次加载会把它恢复，因此该策略在配置文件中设置。

**请求体：**

```json
{
  "use_domains": true,
  "default_domain": "default"
}
```

`use_domains: null` 清除组织的覆盖设置，回退到部署级设置。`use_domains: false` 要求提供 `default_domain`（所有表落入的单一域名称）。目录重建是同步的；响应会在架构就绪后返回。

---

### 可观测性 {: #observability }

#### `GET /admin/traces/recent`

从内存中的跨度缓冲区返回最多 N 条最近完成的跨度。

**查询参数：** `limit`（默认 50，最大 200）

**响应：** `{"traces": [...]}`

#### `POST /admin/query-engine/reload-catalog`

通过联邦引擎协调器的 REST API 热重载一个指定目录。重新连接 Provisa 的内部连接并重新运行 OTel DDL。

**查询参数：** `catalog`（默认为 `"otel"`）

**响应：**

```json
{"success": true, "errors": []}
```

#### `POST /admin/query-engine/restart`

重启联邦引擎容器（仅限单节点开发环境）。

**查询参数：** `container`（默认为 `QUERY_ENGINE_CONTAINER` 环境变量，其次为 `"trino"`）

---

### 发现 {: #discovery }

#### `POST /admin/discover/relationships`

触发关系发现。始终从联邦引擎运行外键自省。 若设置了 `ANTHROPIC_API_KEY`，则运行 LLM 推断。

**请求体：**

```json
{
  "scope": "domain",
  "domain_id": "sales"
}
```

`scope` 必须是 `"table"`、`"domain"`、`"cross-domain"` 之一。对于 `"table"` 范围，需要提供 `table_id`（整数）。对于 `"domain"` 范围，需要提供 `domain_id`。

**响应：** `{"candidates_found": 12, "stored_ids": [1, 2, 3, ...]}`

#### `GET /admin/discover/candidates`

列出待处理的关系候选项。

#### `POST /admin/discover/candidates/{candidate_id}/accept`

接受一个候选项并将其注册为关系。

**请求体（可选）：** `{"name": "custom-relationship-name"}`

#### `POST /admin/discover/candidates/{candidate_id}/reject`

拒绝一个候选项。

**请求体：** `{"reason": "Not a real join"}`

#### `GET /admin/discover/candidates/rejected/count`

返回已拒绝候选项的数量。

#### `DELETE /admin/discover/candidates/rejected`

删除所有已拒绝的候选项。

---

### 数据源爬取 {: #source-crawl }

#### `POST /admin/sources/crawl`

爬取一个数据源以自省其架构并注册表。

---

### 数据源表搜索 {: #source-table-search }

#### `GET /admin/sources/{source_id}/tables/search`

按名称搜索某数据源中可用（尚未注册）的表。

---

### 表画像分析 {: #table-profiling }

#### `POST /admin/tables/{table_id}/profile`

对已注册表运行列画像分析——基数、最小值/最大值、空值率。

---

### 数据源描述 {: #source-descriptions }

#### `POST /admin/source-meta/db-description`

为某数据源的表和列生成 LLM 辅助描述。

---

### 对象存储 {: #object-storage }

#### `GET /admin/org-storage`

报告当前操作组织相对于其平台配额的存储占用，以及该组织是否已注册自己的存储。

当组织注册了自己的 DSN 时，其物化结果会写入那里，不再计入配额。DSN 本身从不返回。

#### `PUT /admin/org-storage`

注册（或清除）组织自己的物化存储。

**请求体：**

```json
{"storage_url": "s3://my-bucket/provisa?region=us-east-1&access_key=..."}
```

DSN 在被接受前会针对联邦引擎进行校验——不可用的 DSN 在注册时就会失败，而不是等到几小时后的刷新时才发现。该值在静态存储时被加密，且 GET 从不返回它。

发送 `storage_url: null` 以清除组织自己的存储，并将其物化结果归还给平台存储（及配额）。组织运行时在同一调用中被重建，因此新存储会立即生效。

---

### 组织加密 {: #org-encryption }

#### `GET /admin/org-encryption`

返回组织当前的密钥状态：指纹、id 和来源。从不返回密钥材料。

当组织未设置密钥时，返回 `{"configured": false}`。每个组织最初都处于此状态，并继承部署级密钥。

#### `PUT /admin/org-encryption`

设置或轮换组织的静态加密密钥。

**请求体：**

```json
{"key_b64": "<32 raw bytes, base64-encoded>"}
```

省略 `key_b64` 可让 Provisa 生成一个密钥——这是最安全的方式，因为密钥不会出现在剪贴板或请求日志中。提供 `key_b64` 则表示自带密钥。

轮换会向密钥环添加一个新的活动条目，并保留旧条目，因此在旧密钥下写入的数据仍然可读。轮换不是重新加密。没有删除端点：撤销最后一个密钥会使所有已封装的负载都变得不可读。

实时密钥环会在同一调用中重新绑定，因此下一次加密写入会立即使用新密钥。

---

### Hasura / DDN 导入 {: #hasura-ddn-import }

#### `POST /admin/import/hasura/preview`

将一个 Hasura v2 或 DDN 项目归档文件转换为拟议的 Provisa 配置，而不写入任何内容。

**请求体：**

```json
{
  "filename": "my-project.zip",
  "content_b64": "<base64-encoded archive>",
  "flavor": "auto",
  "domain_map": {"public": "sales"},
  "source_overrides": {}
}
```

`flavor` 为 `"auto"`（从归档结构中检测）、`"hasura_v2"` 或 `"ddn"`。

**响应：**

```json
{
  "config_yaml": "...",
  "warnings": ["..."],
  "summary": {
    "sources": 1, "domains": 2, "tables": 40,
    "columns": 180, "roles": 3, "relationships": 15, "rls_rules": 6
  }
}
```

不持久化任何内容。预览不在服务端缓存；`apply` 使用你提交的 YAML，因此应用的内容正是被审查（并可能已编辑）过的内容。

#### `POST /admin/import/hasura/apply`

将先前预览过的配置加载到当前操作组织中。

**请求体：**

```json
{"config_yaml": "<yaml string>"}
```

导入是对当前组织模型存储的显式一次性填充，与 `PUT /admin/config` 的应用方式相同：它通过模型存储（带防护和版本）添加并更新导入配置所声明的内容，不删除其中未提及的任何内容。没有 `replace` 字段。所声明数据源的引擎目录、组织的连接池及其 schema 会在响应返回之前重建。

**响应:** `{"summary": {...}}` — 导入配置所声明内容的计数。

---

### Apache Ossie 互操作 {: #apache-ossie-interchange }

#### `GET /admin/ossie`

将组织的治理模型导出为 Apache Ossie（孵化中）YAML 文档。

该文档在每次请求时都从实时状态派生——从不缓存——因此不会过时。表变为 `dataset` 对象，列变为 `field` 对象，关系映射到 Ossie 的 `relationship` 对象。

返回 `text/yaml`，带 `Content-Disposition: attachment; filename=provisa.ossie.yaml`。

#### `GET /admin/dbt/sources`

以 dbt `sources.yml` 的形式下载受治理目录，按提供给某一个角色的样子。

**查询参数:** `role` — 调用者持有的角色，或对 `access_config` 持有者而言的任何角色。

返回 `text/yaml`，带有 `Content-Disposition: attachment; filename=provisa.sources.yml`。未知角色为 `404`，代码为 `dbt_sources.unknown_role`。该文件包含什么，参见[集成](integrations.md#dbt-sources-file)。

#### `POST /admin/ossie/import`

解析一个 Ossie YAML 或 JSON 文档，并返回拟议的表和关系注册项。

**请求体：** 原始 Ossie YAML 或 JSON。格式自动检测。

**响应：**

```json
{
  "proposals": {
    "tables": [...],
    "relationships": [...]
  }
}
```

不注册任何内容。使用管理 UI 的审查界面在任何变更生效前接受或裁剪提案。

---

### 操作（函数与 Webhook） {: #actions-functions-and-webhooks }

所有端点都在 `/admin/actions` 前缀下。

来自 GraphQL、SQL、Cypher、Bolt、Arrow Flight、MCP `run_sql` 和 Provisa gRPC 的每一次调用，都会经过一个统一的治理执行器，一致地强制执行 命令的分配（`visible_to`）、变更所需的 `write` 权利 和治理规则。 各协议的具体调用语法参见 [docs/integrations.md](integrations.md#invoking-commands-across-protocols)。

#### `GET /admin/actions`

返回所有已跟踪的数据库函数和 Webhook。

**响应：**

```json
{
  "functions": [
    {
      "name": "random_python_set",
      "implKind": "python",
      "binding": {"callable": "demo.py_functions:random_dataset"},
      "returns": "",
      "returnSchema": {
        "type": "array",
        "items": {"type": "object", "properties": {"id": {"type": "integer"}, "region": {"type": "string"}}}
      },
      "arguments": [{"name": "rows", "type": "Int"}, {"name": "seed", "type": "Int"}],
      "visibleTo": ["admin"],
      "domainId": "pet-store",
      "description": "Demo Python command returning random rows",
      "kind": "query"
    }
  ],
  "webhooks": [
    {
      "name": "add-pet",
      "url": "https://petstore.example.com/pets",
      "method": "POST",
      "kind": "mutation",
      "approved": true
    }
  ]
}
```

每个 Webhook 对象都携带一个 `approved` 布尔值。Webhook 在数据管家批准其创建请求后即获批准；配置声明的 Webhook 自动获批准。未获批准的 Webhook 会被注册，但不会暴露在任何界面上。

#### `POST /admin/actions/functions`

注册一个已跟踪函数（命令）。

**关键字段：**

| 字段 | 是否必需 | 说明 |
| --- | --- | --- |
| `name` | 是 | 唯一的命令名称 |
| `kind` | 是 | `"query"` → GraphQL Query 字段；`"mutation"` → Mutation 字段 |
| `implKind` | 否 | 命令的运行方式——见下表（默认 `source_procedure`） |
| `binding` | 否 | `implKind` 特定的连接详情（JSON 对象） |
| `returnSchema` | 否 | JSON Schema `{type:"array", items:{type:"object", properties:{...}}}` —— 使该命令在每个界面上都可返回集合 |
| `arguments` | 否 | `[{name, type}]` 参数定义；对 SQL 和 Bolt 调用方而言，位置顺序很重要 |
| `visibleTo` | 否 | 该命令分配给的角色 ID（为空：所有角色）。`mutation` 另外需要 `write` 权利 |
| `domainId` | 否 | 用于 GraphQL 放置和访问控制的域 |

**`implKind` 取值：**

| `implKind` | 运行内容 | `binding` 字段 |
| --- | --- | --- |
| `source_procedure` | 已注册数据源上的存储过程（默认） | `sourceId`、`schemaName`、`functionName` |
| `script` | 服务端脚本 | `script` |
| `http` | 出站 HTTP 调用 | `url`、`method` |
| `grpc` | 到外部服务器的出站 gRPC 调用 | `target`、`method` |
| `python` | Provisa 托管的 Python 可调用对象 | `callable`（例如 `"demo.py_functions:random_dataset"`） |

演示命令 `random_python_set`（`implKind: python`）和 `random_grpc_set`（`implKind: grpc`）实际展示了带 `returnSchema` 的可返回集合的命令；两者都在 `config/provisa-install.yaml` 中。

#### `PUT /admin/actions/functions/{name}`

按名称更新一个已跟踪函数。

#### `DELETE /admin/actions/functions/{name}`

按名称删除一个已跟踪函数。

#### `POST /admin/actions/webhooks`

注册一个已跟踪 Webhook。 注册或更新一个 Webhook 会加入一个数据管家审批请求队列——该 Webhook 只有在数据管家批准后才会在所有界面上生效。配置声明的 Webhook 自动获批准。**请求体字段：** `name`、`url`、`method`、`timeoutMs`、`returns`、`inlineReturnType`、`arguments`、`visibleTo`、`domainId`、`description`、`kind`。

#### `PUT /admin/actions/webhooks/{name}`

按名称更新一个已跟踪 Webhook。任何编辑都会将审批状态重置为待处理，直至重新获批。

#### `DELETE /admin/actions/webhooks/{name}`

按名称删除一个已跟踪 Webhook。

#### `POST /admin/actions/test`

按名称测试一个操作（函数或 Webhook）。

---

### 角色 {: #roles }

所有端点都在 `/admin/roles` 前缀下。

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| `GET` | `/admin/roles/` | 列出所有角色 |
| `POST` | `/admin/roles/` | 创建一个角色 |
| `PUT` | `/admin/roles/{role_id}` | 更新一个角色 |
| `DELETE` | `/admin/roles/{role_id}` | 删除一个角色 |

---

### 用户 {: #users }

所有端点都在 `/admin/users` 前缀下。

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| `POST` | `/admin/users/` | 创建一个本地用户 |
| `GET` | `/admin/users/` | 列出本地用户 |
| `GET` | `/admin/users/{user_id}` | 获取一个用户 |
| `PUT` | `/admin/users/{user_id}` | 更新一个用户 |
| `PATCH` | `/admin/users/{user_id}/password` | 修改密码 |
| `DELETE` | `/admin/users/{user_id}` | 删除一个用户 |
| `GET` | `/admin/users/{user_id}/assignments` | 列出角色分配 |
| `POST` | `/admin/users/{user_id}/assignments` | 添加一个角色分配 |
| `DELETE` | `/admin/users/{user_id}/assignments/{assignment_id}` | 移除一个角色分配 |

---

### 组织 {: #organizations }

所有端点都在 `/admin/orgs` 下。

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| `GET` | `/admin/orgs/` | 列出组织 |
| `POST` | `/admin/orgs/` | 创建一个组织 |
| `PUT` | `/admin/orgs/{org_id}` | 更新一个组织 |
| `DELETE` | `/admin/orgs/{org_id}` | 删除一个组织 |
| `GET` | `/admin/orgs/{org_id}/members` | 列出成员 |
| `POST` | `/admin/orgs/{org_id}/members` | 添加一个成员 |
| `DELETE` | `/admin/orgs/{org_id}/members/{user_id}` | 移除一个成员 |

---

### 邀请 {: #invites }

所有端点都在 `/admin/invites` 下。

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| `POST` | `/admin/invites/` | 创建一个邀请 |
| `GET` | `/admin/invites/` | 列出待处理邀请 |
| `DELETE` | `/admin/invites/{token}` | 撤销一个邀请 |

---

### 管理 GraphQL {: #admin-graphql }

#### `POST /admin/graphql`

用于所有管理操作的 Strawberry GraphQL 端点：数据源和表的增删改查、关系管理、域配置、行级安全规则、缓存控制、命名约定、计划任务管理和查询编译。

完整的架构参考——每一个 Query 字段、Mutation 字段以及输入/输出类型——请参见[管理 GraphQL API 参考](admin-graphql.md)。

**关键变更：**

```graphql
# Cache
mutation { update_source_cache(source_id: "sales-pg", enabled: true, ttl: 600) { success } }
mutation { update_table_cache(table_id: 1, ttl: 60) { success } }

# Naming conventions
mutation { update_source_naming(source_id: "legacy-db", convention: "camelCase") { success } }
mutation { update_table_naming(table_id: 1, convention: "PascalCase") { success } }

# Scheduled tasks
mutation { toggle_scheduled_task(name: "daily-report", enabled: false) { success } }

# Compile a query (returns enforcement metadata and routed SQL)
mutation {
  compile_query(input: {role: "admin", query: "{ orders { id } }"}) {
    sql semantic_sql trino_sql direct_sql route route_reason sources root_field
    enforcement { rls_filters_applied columns_excluded masking_applied }
  }
}
```

---

### 初始设置 {: #setup }

#### `GET /setup/status`

返回首次运行的设置状态。始终无需身份验证。

#### `POST /setup/`

完成首次运行设置。

---

## 健康检查 {: #health-check }

#### `GET /health` 或 `HEAD /health` {: #get-health-or-head-health }

返回 `{"status": "ok"}`。始终无需身份验证。

---

## 错误响应 {: #error-responses }

| 状态码 | 含义 |
| --- | --- |
| 400 | 无效查询、校验错误或 SQL 解析错误 |
| 401 | 缺少或无效的身份验证令牌 |
| 403 | 能力不足；治理违规 |
| 404 | 未找到角色、资源或配置文件 |
| 422 | 缺少必需的请求头（例如 `X-Provisa-Role`） |
| 503 | 数据库或数据源未连接；依赖项不可用 |
| 504 | 请求超时 |

`POST /data/sql` 上的治理违规返回带结构化响应体的 HTTP 403：

```json
{
  "detail": {
    "violations": [
      {"code": "V000", "message": "Table 'orders' is not accessible for role 'analyst'"}
    ]
  }
}
```

所有其他错误使用：`{"detail": "<message>"}`。

---

## Arrow Flight 端点 {: #arrow-flight-endpoint }

端口 `8815`。基于 gRPC 的原生 Arrow 列式传输。

查询和目录发现都在同一连接上可用。完整的治理管道（行级安全、脱敏、抽样）应用于每一个查询。

目录发现（`list_flights`、`get_flight_info`、`get_schema` 以及不带 `query` 的 ticket）列出授予调用者角色的内容。启用身份验证时，这三个不带 ticket 的调用从调用标头 `authorization: Bearer` 获取凭据、从 `x-provisa-role` 获取角色，没有凭据时被拒绝；参见[集成](integrations.md)。

**票据格式**（JSON）：

```json
{"query": "{ customers { name email } }", "role": "analyst", "variables": {}}
```

**用法（Python）：**

```python
import pyarrow.flight as flight

client = flight.FlightClient("grpc://localhost:8815")
ticket = flight.Ticket(b'{"query": "{ orders { id amount } }", "role": "admin"}')
# Stream batch-by-batch
for batch in client.do_get(ticket):
    process(batch.data)
# Or read all at once
table = client.do_get(ticket).read_all()
```

当 Zaychik Flight SQL 代理可用时（端口 8480），记录批次会端到端流式传输，无需完全物化。 若 Zaychik 不可用，则回退到通过联邦查询层进行物化。

**警告。** 响应关于自身报告的内容，在流中作为不带行的批次的 `app_metadata` 传递：`{"provisa_warnings": [{"code", "params", "message"}]}`。读取行之前已知的警告，在行之前的不带行批次中到达。只有读取行之后才知道的警告，在第二个不带行的批次中到达，它是流中的最后一批：`statement.rows_cut` 表示响应在行数上限处被截断，且还有更多行匹配。请读取每一批的 `app_metadata`，包括最后一批。

---

## Protobuf gRPC 端点 {: #protobuf-grpc-endpoint }

端口 `50051`（可通过 `GRPC_PORT` 环境变量或 `server.grpc_port` 配置覆盖）。

在 `x-provisa-role` gRPC 元数据键中传递角色。如果缺失，服务器会以 `UNAUTHENTICATED` 中止。

从 `GET /data/proto/{role_id}` 下载特定角色的 proto。只有该角色可见的表和列会出现。

```proto
service ProvisaService {
  rpc QueryOrders (QueryOrdersRequest) returns (stream Orders);
  rpc InsertOrders (InsertOrdersRequest) returns (InsertOrdersResponse);
}
```

角色架构根部的每张表生成一个 `Query{TypeName}` 流式 RPC。这些就是该角色的 GraphQL `Query` 类型所列出的表。角色只能通过关系到达的表没有自己的 RPC，正如它在 GraphQL 中没有根字段。`Insert{TypeName}` RPC 是为架构对称性而存在，会以 `UNIMPLEMENTED` 中止。

**参数。** 带参数的表（基于 API 注册的表或查询表）有一个 `{TypeName}Params` 消息，每个参数一个可选字段，名称去掉 `_nf_` 前缀，`Query{TypeName}Request` 以 `params` 携带它。其中设置的每个值作为参数到达源，绑定在语句中；不是该表参数的名称以 `INVALID_ARGUMENT` 拒绝，缺少必填参数的读取被拒绝并指出其名称。HTTP gRPC 代理以请求体的 `params` 对象接收相同的值（未知名称或 JSON 类型错误的值返回 HTTP 400，代码 `data.invalid_parameter`）。

不属于调用方角色的服务的方法返回 `UNIMPLEMENTED`。

启用了 `grpc_reflection.v1alpha`，无需预编译的 proto 即可进行服务发现。 反射向每个调用方描述其所充当角色的服务：与该角色从 `GET /data/proto/{role_id}` 下载的 `.proto` 相同。它按名称列出 Provisa 服务和反射服务，并描述 Provisa 服务的文件。

反射调用与其他 gRPC 调用一样需要标识身份。启用认证时，角色来自调用方的凭据。未启用认证时，请在 `x-provisa-role` 中指明角色；在为多个组织提供服务的部署中，还需在 `x-provisa-org` 中指明组织。缺少这些元数据的调用会以 `UNAUTHENTICATED` 被拒绝。未启用认证时，除非设置了 `server.grpc_allow_unsecured_reflection`，否则反射保持禁用。

```bash
grpcurl -plaintext -H 'x-provisa-role: analyst' localhost:50051 list
grpcurl -plaintext -H 'x-provisa-role: analyst' \
  -d '{}' localhost:50051 provisa.v1.ProvisaService/QueryOrders
```

当启动时加载的模型定义了 gRPC 服务时，gRPC 服务器才会启动。 此后每次调用都按模型的当前状态提供服务：服务器运行期间注册或更改的表，会在下一次调用时即被提供和描述，无需重启。

#### 聚合与分组 RPC {: #aggregate-and-group-by-rpcs }

当某张表设置了 `enable_aggregates` 时，生成的 proto 会在 `Query{TypeName}` 之外额外包含两个 RPC：

- **`Query{TypeName}Aggregate`** —— 返回该表的聚合标量（`count`；每个数值列的 `sum`、`avg`、`stddev`、`variance`；每个可比较列的 `min`、`max`）
- **`Query{TypeName}GroupBy`** —— 每个分组键返回一行，包含聚合子字段，以及可选的基表标量和已联接维度行

两者都经过与 GraphQL 的 `{field}_aggregate` 和 `{field}_group_by` 根字段相同的编译器聚合管道——没有单独的聚合实现。

**`funcs` 字段。** 请求消息接受一个 `funcs` 重复字符串字段。有效取值为 `count`、`sum`、`avg`、`stddev`、`variance`、`min` 和 `max`。省略 `funcs` 时，会请求架构为该表暴露的每个函数。设置后，只出现所命名的函数。如果所命名的函数都不适用于该表的列类型，查询会回退到 `count`。

**`include_nodes` 和 `include` 字段。** `Query{TypeName}GroupBy` 请求可以设置 `include_nodes: true`，以在每行的 `nodes` 字段中包含基表标量列。`include` 重复字符串字段命名多对一关系字段，其标量列也会嵌套在 `nodes` 内。这与 JSON:API 的 `?includeNodes=` / `?include=` 行为一致。

---

## JDBC 驱动 {: #jdbc-driver }

Provisa JDBC 驱动（`provisa-jdbc-<release>.jar`）向 BI 工具（Tableau、PowerBI、DBeaver）暴露语义目录。

**连接 URL：** `jdbc:provisa://host:port`

域映射到 JDBC 架构。 表使用其已注册的别名。列使用别名，并将描述作为 `REMARKS` 暴露。 标准元数据方法（`getPrimaryKeys`、`getImportedKeys`、`getExportedKeys`）将语义关系作为主键/外键元数据暴露。

**SQL 支持：** `SELECT * FROM <alias> [WHERE col = 'value']`。

该驱动默认请求 Arrow IPC 重定向。结果通过 `ArrowStreamReader` 逐批流式传输，内存中最多保留一个记录批次。

---

## `orderBy` 参数格式 {: #orderby-argument-format }

`order_by` 参数使用 `{column: direction}` 对象，direction 是一个包含 6 个取值的枚举：

```json
{
  "query": "{ orders(order_by: [{created_at: desc_nulls_last}]) { id created_at } }",
  "role": "admin"
}
```

支持的方向：`asc`、`desc`、`asc_nulls_first`、`asc_nulls_last`、`desc_nulls_first`、`desc_nulls_last`。

---

## 订阅 {: #subscriptions }

SSE 订阅在 `GET /data/subscribe/{table}` 处提供。 通知投递使用按源类型选择的可插拔提供程序：PostgreSQL 源使用 `LISTEN/NOTIFY`，MongoDB 源使用变更流，Kafka 源使用消费者组。无论使用哪种提供程序，都会应用行级安全过滤和架构验证。同一端点也支持 WebSocket 和 RSS 源。

---

## 业务术语表 {: #business-glossary }

业务术语表将物理字段名——即它们在源数据库中存在的样子——映射到一套共享的人类可读词汇。模型中注册的每一列都会自动获得一个术语。填充术语表无需任何手动录入；策展人在系统派生的基础上添加定义、关系和专家。

### 术语如何派生 {: #how-terms-are-derived }

当 Provisa 注册或更新一张表的列时，`normalize_term`（`provisa/core/glossary.py`）会针对每个列名运行，产生一个规范化短语。

规范化按顺序应用五条规则：

1. 按 camelCase 边界和分隔符（`_`、`-`、`.`、`/`、空白）拆分。
2. 将结果转为小写。
3. 展开一个固定的缩写表（例如 `cust` → `customer`、`amt` → `amount`、`dt` → `date`、`id` → `identifier`、`key` → `identifier`、`guid` → `identifier`）。
4. 剥除末尾的**代理标记**（`identifier`、`code`、`index` 或 `reference`）——以键或代码命名的列是通过一个代替值指向底层概念的，因此术语应该是概念本身。最后剩余的标记从不会被剥除。
5. 用表的概念限定**过于泛化的短语**。当完整的规范化短语是一个裸属性词（`name`、`identifier`、`date`、`location`、`message`、`first name`、`last name` 及类似词）时，术语会变为 `<table concept> <phrase>`——`employees.first_name` → `employee first name`，`orders.id` → `order identifier`。若在不相关的表之间共用一个 `name` 术语，会把不同的含义混为一谈；限定操作则将每一列与其所属概念关联起来。表概念是该表的业务名称，并规范化为单数中心名词（`order_lines` → `order line`）。

原生过滤器伪列（以 `_nf_` 为前缀，或任何携带 `native_filter_type` 的列）是查询参数机制，不是业务字段，不会派生任何术语。

由于 `id`、`key`、`pk`、`sk` 在代理检查之前都会展开为 `identifier`，三个物理上不同的列名会落在完全相同的术语上：

| 物理名称 | 规范化后 |
| --- | --- |
| `cust_id` | `customer` |
| `customerId` | `customer` |
| `CUSTOMER_KEY` | `customer` |
| `txn_amt` | `transaction amount` |

前三者合并为一个术语。`transaction amount` 保留两个标记，因为 `amount` 不是代理标记。裸 `id` 列——前面没有其他标记——无法被剥除；它规范化为 `identifier`，使术语非空。

### 生命周期 {: #lifecycle }

术语是**从模型成员关系中派生**的，而不是由用户按需创建的。表存储库是唯一的写入路径：`sync_table_refs` 会在每次列集更新（upsert）中运行，`sweep_refless_terms` 会在任何删除路径之后运行。

**添加一列时：** Provisa 按名称查找规范化术语。如果已存在，该列会获得对它的引用（如果该术语曾被弃用，则会被恢复——`deprecated` 会被设回 `False`）。如果尚不存在术语，则创建一个。

**一列被移除时**（架构变更或表删除）：其引用被删除，术语在一条移除或弃用规则下被**处理**。没有剩余引用的有根术语会被彻底移除——连同其边和专家指派——除非移除它会导致某个抽象术语与所有有根术语失去连接（在术语图中没有可达路径）。在这种情况下，该术语会被**弃用**（标记为 `deprecated=True`）而不是删除，从而使该抽象术语在图中的锚点得以保留。

抽象术语从不会被自动移除；它们存在于物理生命周期之外，只能通过管理 API 显式删除。

**恢复：** 如果一个已弃用术语的规范化名称再次出现（某列被重新注册），该术语会被取消弃用标记，其引用重新开始累积。

### 策展端点 {: #curation-endpoints }

所有端点都在 `/admin/glossary` 下。它们需要 `org_admin` 访问权限和一个已配置的组织。每次变更都会触发一次元数据发布。

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| `GET` | `/admin/glossary/terms` | 列出术语。查询参数：`q`（名称/定义搜索）、`include_deprecated`（默认 `true`） |
| `GET` | `/admin/glossary/terms/{term_id}` | 获取术语详情：定义、物理引用、类型化边、专家 |
| `POST` | `/admin/glossary/terms` | 创建一个抽象术语——没有物理引用的用户词汇 |
| `PATCH` | `/admin/glossary/terms/{term_id}` | 重命名、设置定义，或切换导出排除状态 |
| `DELETE` | `/admin/glossary/terms/{term_id}` | 删除一个没有物理引用的术语 |
| `POST` | `/admin/glossary/refs/move` | 将一个物理引用移动到另一个术语（合并） |
| `POST` | `/admin/glossary/terms/{term_id}/edges` | 在两个术语之间添加一条类型化关系边 |
| `DELETE` | `/admin/glossary/terms/{term_id}/edges` | 移除一条边（查询参数：`to_term_id`、`rel_type`） |
| `POST` | `/admin/glossary/terms/{term_id}/experts` | 将某用户标记为某术语的专家或作者 |
| `DELETE` | `/admin/glossary/terms/{term_id}/experts/{user_id}` | 移除某用户的专家/作者标记 |
| `POST` | `/admin/glossary/terms/{term_id}/definition/generate` | 使用组织的 AI 模型为单个术语起草定义——仅返回文本，保存前不会持久化 |
| `POST` | `/admin/glossary/definitions/generate` | 为每个尚无定义的术语生成并持久化定义——从不覆盖人工撰写的文本 |
| `POST` | `/admin/glossary/relationships/generate` | 使用组织的 AI 模型为整个术语表提出并持久化类型化边 |

**`POST /admin/glossary/terms` 请求体：**

```json
{"name": "revenue", "definition": "Recognized net revenue after returns and discounts."}
```

**`POST /admin/glossary/terms/{term_id}/edges` 请求体：**

```json
{"to_term_id": 42, "rel_type": "KIND_OF"}
```

有效的 `rel_type` 取值：`KIND_OF`、`RELATED_TO`、`PART_OF`、`SYNONYM_OF`。

**`POST /admin/glossary/terms/{term_id}/experts` 请求体：**

```json
{"user_id": "alice@example.com", "kind": "author"}
```

有效的 `kind` 取值：`expert`、`author`。

**`POST /admin/glossary/refs/move` 请求体：**

```json
{"table_id": 7, "column_name": "cust_id", "to_term_id": 12}
```

移动一个引用会在移除或弃用规则下处理被移出的那个术语。用它来合并被规范化算法分开的两个术语——例如，当某个数据源使用了缩写表之外的非标准缩写时。

删除一个有根术语（拥有物理引用的术语）会返回 `400 glossary.invalid`。请先移除或移动所有引用。

**`PATCH /admin/glossary/terms/{term_id}` —— `export_excluded` 字段：**

```json
{"export_excluded": true}
```

将 `export_excluded` 设置为 `true` 会将该术语从所有元数据导出快照中排除，无论其物理引用或抽象状态如何。将其设回 `false` 会在下一次发布时将该术语恢复到快照中。策展数据（定义、边、专家）不受影响。

### AI 辅助策展 {: #ai-assisted-curation }

组织配置的 AI 模型可以在一次操作中为整个术语表起草定义并提出关系边。这两个批量操作都需要 `org_admin` 访问权限和一个已配置的组织。

**`POST /admin/glossary/definitions/generate`**

遍历术语表中的每个术语，跳过已有定义的术语，并调用组织的 AI 模型为每个剩余术语起草一条定义。草稿会立即持久化——与逐个术语的起草端点（`POST /admin/glossary/terms/{term_id}/definition/generate`）不同，这里没有编辑步骤。人工撰写的定义从不会被覆盖：在任何模型调用之前都有 `if summary["definition"]: continue` 这一保护。一次发布通知覆盖整个批次。

响应：

```json
{"generated": 12}
```

`generated` 是获得新定义的术语数量。当每个术语都已有定义时为零。

**`POST /admin/glossary/relationships/generate`**

将完整的术语列表连同一段说明十种允许边类型（`KIND_OF`、`PART_OF`、`SYNONYM_OF`、`RELATED_TO`、`VALID_VALUE_OF`、`DERIVED_FROM`、`REPLACES`、`PREFERRED_TERM_FOR`、`TRANSLATION_OF`、`ANTONYM_OF`）的提示词发送给组织的 AI 模型，并要求只给出有把握的提案。模型返回一个 JSON 数组；每一条在写入前都会被校验：未知的术语名、自环边以及封闭枚举之外的边类型会被静默丢弃。有效提案会被幂等地更新插入（upsert）——重复运行该操作不会产生重复的边。一次发布通知覆盖整个批次。当术语表中未弃用的术语少于两个时，端点会立即返回 `{"added": 0}`。

响应：

```json
{"added": 5}
```

`added` 是写入的边数量。已存在的边仍会计入——更新插入操作成功执行，但边数据不会改变。

### MCP `search_terms` 工具 {: #mcp-search_terms-tool }

```
search_terms(query, role=None, limit=25)
```

以不区分大小写的子串匹配搜索术语名称和定义，最多返回 `limit` 条结果。每条结果都是完整的术语详情：`name`、`definition`、`is_abstract`、`deprecated`、物理引用（含 `source_id`、`schema_name`、`table_name`、`column_name`）、类型化边和专家指派。

在编写 SQL 之前使用 `search_terms`，按名称查找代表某个概念的每一个物理字段。例如，搜索 `"order date"` 会返回该术语以及所有已注册表中的 `order_dt`、`orderDate`、`ORDER_DATE` 等列。

### MCP 统计工具 {: #mcp-statistics-tools }

```
describe(sql, role=None)
correlate(sql, columns, method="pearson", role=None)
regress(sql, target, features, kind="linear", role=None)
compare(sql, value, group, role=None)
forecast(sql, time, value, horizon, seasonal_periods=None, role=None)
```

每个工具接收由 `sql` 定义的数据集，并以调用方的角色，通过与 `run_sql` 相同的受治理流水线运行它：行规则、脱敏和涂黑都会生效，流水线拒绝的 SQL 会像在 `run_sql` 中一样失败，调用方不持有的角色会被拒绝。每个工具只返回结果，从不返回行，并附上所用的方法和统计的 `n`。

| 工具 | 返回 |
| --- | --- |
| `describe` | 每列：`count`、`missing`、`distinct`；数值列还有 `mean`、`sd`、`min`、`max` 和 `quantiles`（p0、p5、p25、p50、p75、p95、p100）；`top_values`（最常见的五个值） |
| `correlate` | 数值 `columns` 的 `pearson` 或 `spearman` 相关 `matrix`，基于在所有这些列中都完整的行，以及该 `n` |
| `regress` | `kind="linear"`：最小二乘 `coefficients`（`estimate`、`se`、`p_value`）、`r_squared`、`adj_r_squared`。`kind="logistic"`：取两个值的 `target`，最大似然 `coefficients`、`pseudo_r_squared`、`auc` 和 `positive_value`。文本特征按除第一个值外的每个值各变为一个指示变量 |
| `compare` | 每组的 `n`、`mean` 和 `sd`；两组时为 Welch t 检验（`t`、`df`、`p_value`）和 Cohen's d；更多组时为单因素方差分析（`f`、`df_between`、`df_within`、`p_value`）和 eta 平方 |
| `forecast` | 最后一个 `time` 之后的 `horizon` 个点，每个点带 `value`、`lower` 和 `upper`（95% 预测区间），采用加性误差的指数平滑（ETS），10 个点起带加性趋势，给出 `seasonal_periods` 时带加性季节性 |

引擎在角色读取的每一行上计算它能计算的内容：`describe` 的计数、去重计数、均值、标准差、最小值和最大值，以及 `compare` 所需的每组 `n`、均值和方差。拟合的模型以及 `describe` 的分位数和最常见值最多读取 `mcp.stats_max_rows` 行（Admin → Settings → Limits；默认 100,000）。每个读取行的答案都会给出 `rows_read`、`row_cap` 和 `truncated`，角色自身的行数上限会像 `run_sql` 一样在 `warnings` 中报告。

统计工具只返回结果，因此分析背后的行永远不会进入模型的上下文：默认即数据最小化。限制智能体所能获知内容的是访问控制。若只公开聚合，请发布聚合视图，并把这些视图而不是底层表授予角色；对小分组的聚合可能识别出个人，而聚合视图决定了角色读取的分组方式。参见[安全](security.md#statistics-over-mcp)。

### 元数据导出 {: #metadata-export }

术语图被包含在 `build_snapshot` 构建的每个 `MetadataSnapshot` 中。

导出应用与快照其余部分相同的过滤规则：

- 标记为 `export_excluded` 的术语会被彻底排除——无论其物理引用、抽象状态，或组织的目录是否已配置。
- 只有当一个有根术语的至少一个物理引用属于同时通过**数据产品**过滤（表的 `data_product` 标志必须为 `true`）和**技术**列过滤（标记为 `technical` 的列被排除）的列时，该术语才会发布。
- 一个所有引用都被这些过滤规则排除的有根术语，也随之被排除。
- 抽象术语无条件发布——它们是用户词汇，不绑定到物理列。
- 两个术语之间的边只有在两端术语都发布时才会发布。

每个厂商适配器都会原生发布术语图，写入一个由 Provisa 幂等创建的、Provisa 拥有的术语表容器——从不写入已有的目录术语表：

| 提供程序 | 容器 | 术语 | 关系 | 弃用 |
| --- | --- | --- | --- | --- |
| Apache Atlas | "Provisa Glossary"（术语表 API） | 术语表术语，定义写入 `longDescription` | KIND_OF → `isA`，SYNONYM_OF → `synonyms`，RELATED_TO/PART_OF → `seeAlso` | `[DEPRECATED]` shortDescription 标记 |
| Atlan | 按稳定 qualifiedName 划分的 Provisa 术语表 | `longDescription`（从不使用人工编辑的 `userDescription`） | 与 Atlas 相同的映射 | `certificateStatus = DEPRECATED` |
| DataHub | `urn:li:glossaryNode:provisa.<org>` | 每个术语一个 `glossaryTermInfo` 方面 | KIND_OF → Inherits，PART_OF → Contains（反向），RELATED_TO/SYNONYM_OF → related terms | 弃用方面；重命名遵循 URN 迁移 |
| OpenMetadata | 通过 `/v1/glossaries` 的 Provisa 术语表 | 以 fqn 为键的 PUT，重命名通过存储的 UUID 进行 PATCH 重绑定 | KIND_OF → 原生父级层级，SYNONYM_OF → `synonyms`，其他 → `relatedTerms` | `entityStatus` |
| Collibra | 术语表类型域 "Provisa Glossary" | 通过 Import API 创建的 Business Term 资产 | 原生 Business Term 关系类型 | 资产状态 |

绑定关系依据的是所有权，而不是名称：每个已发布术语的厂商 id 会被捕获进该术语 URN（`provisa://<org>/terms/<name>`）下的 `catalog_bindings`，Provisa 只有在持有该绑定（或该条目位于其自己创建的 Provisa 拥有容器中）时才会修改或删除厂商侧的术语表条目。没有 Provisa 绑定的术语表条目源自外部系统，从不会被触碰；更新采用读取合并方式，因此策展人在 Provisa 自身术语上添加的字段得以保留；当某个术语离开快照时，不会删除任何内容。数据管家的术语到资产指派仍归外部所有——没有适配器会写入术语到资产的指派（Provisa 撰写的指派发布是明确的后续工作）。特别是在 Collibra 上，Import API 的 REPLACE 语义下的安全性依赖于遏制范围：负载只提及 Provisa 术语表域内的资产，以及仅在 Provisa 术语之间的关系实例，因此数据管家的术语表及其关系永远不会被触及。

---

## 数据产品 {: #data-products }

数据产品将一组共同发布以供消费的表分组，归属于恰好一个域。字段遵循 ODPS（开放数据产品标准）词汇——在 Provisa 已经拥有真实来源的地方。管理 UI 在**管理 → 数据产品**下暴露数据产品。

### 能力 {: #capabilities }

| 能力 | 授予内容 |
| --- | --- |
| `data_product_read` | 对 `data_products` 查询字段和数据产品管理页面的读取权限。默认为 `org_admin`、`analyst`、`developer` 和 `modeler` 预置。 |
| `data_product_rw` | 创建和删除变更。在 UI 中启用新建/编辑/删除控件。 |

### 管理 GraphQL {: #admin-graphql_1 }

所有数据产品操作都通过 `POST /admin/graphql` 进行。

**查询：**

```graphql
query {
  data_products {
    id
    domain_id
    name
    owner_role
    team_role
    purpose
    limitations
    usage
    version
    status
    sla
    support
    custom_properties
  }
}
```

需要 `data_product_read`。

**创建或更新：**

```graphql
mutation {
  create_data_product(input: {
    id: "customer_360"
    domain_id: "sales"
    name: "Customer 360"
    owner_role: "data-product-owner"
    team_role: "sales-analytics"
    purpose: "Single view of a customer across all touchpoints."
    status: "active"
    version: "1.0.0"
  }) {
    success
    message
  }
}
```

`create_data_product` 执行更新插入（upsert）——以已存在的 `id` 调用会更新该记录。需要 `data_product_rw`。

**删除：**

```graphql
mutation {
  delete_data_product(id: "customer_360") {
    success
    message
  }
}
```

删除一个产品会清除每张成员表上的 `product_id`，移除其成员资格。需要 `data_product_rw`。

### 字段架构 {: #field-schema }

| 字段 | 类型 | 是否必需 | 说明 |
| --- | --- | --- | --- |
| `id` | `String` | 是 | 机器可读的稳定标识符，例如 `customer_360` |
| `domain_id` | `String` | 是 | 所属域。成员表必须共享此 `domain_id`——不匹配会在保存时被拒绝 |
| `name` | `String` | 是 | 显示名称 |
| `owner_role` | `String` | 否 | 对该产品负责的角色；与域数据管家不同 |
| `team_role` | `String` | 否 | 日常维护该产品的角色，解析为具体个人 |
| `purpose` | `String` | 否 | 该产品发布什么内容以及原因 |
| `limitations` | `String` | 否 | 已知的限制、注意事项或排除项 |
| `usage` | `String` | 否 | 如何消费该产品 |
| `version` | `String` | 否 | 例如 `1.2.0` |
| `status` | `String` | 否 | 例如 `proposed`、`active`、`deprecated`、`retired` |
| `sla` | `String` | 否 | 服务级别承诺；纯文本——一个产品跨越多张表，结构化 SLA 无法明确指出它描述的是哪个成员 |
| `support` | `String` | 否 | 自由文本支持指南 |
| `custom_properties` | `JSON` | 否 | 标准字段未覆盖的任意键值元数据 |

模型上还存在两个额外字段，但未在 Strawberry 的 `DataProductType` / `DataProductInput` 中暴露——它们是 Snowflake Horizon Catalog 专属的：

| 字段 | 说明 |
| --- | --- |
| `support_contact` | 电子邮件或 URL；Horizon Catalog 组织列表清单要求提供 |
| `publish` | `true` 表示立即发布 Horizon 列表；新列表默认是 DRAFT |

### 表成员关系 {: #table-membership }

一张表通过在表编辑表单中设置其 `product_id` 字段来加入某个数据产品。选择器的范围限定为 `domain_id` 与该表自身域相匹配的产品——`marketing` 域中的表永远不会被提供 `sales` 域中的产品。

同一域中的命令也可以被指派为成员。

### 元数据导出过滤 {: #metadata-export-filter }

`build_snapshot` 在每次目录发布时应用 `data_products_only=True`。没有 `product_id` 的表会被从快照中排除，连同其关系边、血缘边和治理标签一起。数据源和域始终发布。术语表术语只有在其至少一个物理引用属于某个已导出（产品成员）表时才会发布。

一个没有已导出成员的产品不会构建快照条目——一个没有成员的列表会向目录错误地呈现该产品。

### 按目录目标的数据产品支持 {: #data-product-support-by-catalog-target }

`MetadataSnapshot.data_products` 会传递给每个适配器，但只有平台具备原生数据产品概念的适配器会将其作为一等实体发布；其余适配器只发布成员表（已如上过滤），不带产品分组。

| 目标 | 数据产品表示方式 |
| --- | --- |
| Snowflake Horizon | 每个产品变为一个覆盖其成员表物理地址的 `SHARE`，包裹在一个内部的 `CREATE ORGANIZATION LISTING` 中——一个原生的 Horizon Catalog 数据产品。`publish=true` 会使该列表立即上线；否则以 DRAFT 状态落地。 |
| BigQuery Dataplex | 每个产品通过 `/v1/dataProducts` 变为一个 Analytics Hub 列表。 |
| OpenMetadata | 每个产品变为一个原生的 `DataProduct` 实体（`/api/v1/dataProducts`），归属关系由域派生。 |
| DataHub | 每个产品变为一个原生的 `dataProduct` 实体（`urn:li:dataProduct:...`），拥有自己的 `dataProductProperties`/所有权方面。 |
| Collibra | 每个产品变为一个 `Data Product` 社区类型的资产，通过 `Data Product groups Table` 关系与其成员表关联。 |
| Atlan | 以自定义的 `DataProduct` 类型定义猜测方式发布——Atlan 对此概念没有文档化的稳定类型名，因此该映射是尽力而为的。 |
| Apache Atlas | 以自定义的 `provisa_data_product` 类型定义发布，并带有 `provisa_data_product_members` 关系——Atlas 没有原生的数据产品实体类型。 |
| OpenLineage | 不是一等实体——成员表携带一个命名所属产品的 `provisa_data_product` 自定义面。 |
