# API 参考

## 概述

Provisa 在两个前缀下暴露 REST 端点：`/data` 用于查询执行和架构自省，`/admin` 用于配置管理。(REQ-043) 大多数数据端点需要角色标识。管理配置操作使用 `/admin/graphql` 上的 Strawberry GraphQL API。(REQ-164)

---

## 身份验证

当 `provisa.yaml` 中配置了 `auth.provider` 时，除 `/health` 和 `/setup/status` 外的所有端点都需要 `Authorization: Bearer <token>` 请求头。(REQ-120) [tool-verified: `provisa/api/app.py`, `provisa/auth/wiring.py`]

未配置身份验证时，服务器以开发模式运行。任何请求都被视为 `anonymous` 身份，该身份映射到所有已配置角色，并具有通配符域访问权限。(REQ-535)

当配置了 `provider: basic` 时，**登录（`POST /auth/login`）** 由当前激活的身份验证提供方提供。(REQ-124) 凭据格式和响应取决于该提供方。

**身份自省：**

```http
GET /auth/me
```

返回已认证用户的 id、邮箱、显示名称、组织成员关系和角色分配。在开发模式下返回 `dev_mode: true` 以及所有角色 ID 的列表。[tool-verified: `provisa/api/auth_router.py`]

```http
GET /auth/provider-type
```

返回 `{"provider": "<name>"}`；若未配置身份验证，则返回 `{"provider": null}`。[tool-verified: `provisa/api/auth_router.py`]

---

## 数据端点

### `POST /data/graphql`

执行 GraphQL 查询或变更。(REQ-043) [tool-verified: `provisa/api/data/endpoint.py:151`]

**请求体：**

```json
{
  "query": "{ orders(where: {region: {eq: \"us\"}}) { id amount } }",
  "variables": {},
  "role": "admin",
  "extensions": {}
}
```

`role` 字段仅在开发模式（无身份验证）下使用。启用身份验证时，使用已认证用户的角色，请求体中的 `role` 会被忽略。

`extensions` 字段支持自动持久化查询（APQ）协议：(REQ-288)

```json
{
  "extensions": {"persistedQuery": {"sha256Hash": "<sha256-of-query>"}}
}
```

**请求头：**

- `X-Provisa-Role` — 覆盖角色（开发模式）
- `Accept` — 响应格式（见"内容协商"）
- `Authorization` — 启用身份验证时使用 `Bearer <token>`
- `X-Provisa-Redirect-Format` — S3 重定向输出的 MIME 类型（REQ-137）
- `X-Provisa-Redirect-Threshold` — 触发重定向的行数阈值（REQ-137）
- `X-Provisa-Redirect` — 设为 `true` 以无条件强制重定向（REQ-029）

**响应（JSON 内联）：**

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

**响应（多根字段，内联与重定向混合）：**

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

多根字段查询会独立执行每个根字段。低于重定向阈值的字段以内联方式返回；超过阈值的字段则重定向。`redirects` 键（复数）将字段名映射到重定向信息。(REQ-029) [tool-verified: `provisa/api/data/endpoint.py`]

**缓存响应头：**

- `X-Provisa-Cache: HIT|MISS`（REQ-536）
- `X-Provisa-Cache-Age: <seconds>`（命中时）（REQ-536）

**所需能力：** 所有请求（包括自省）都需要 `QUERY_DEVELOPMENT`。[tool-verified: `provisa/api/data/endpoint.py:186-283`]

---

### 内容协商

| Accept 请求头 | 格式 |
| --- | --- |
| `application/json` | JSON（默认） |
| `application/x-ndjson` | 换行分隔 JSON |
| `text/csv` | CSV |
| `application/vnd.apache.parquet` | Parquet |
| `application/vnd.apache.arrow.stream` | Arrow IPC |

(REQ-047, REQ-048, REQ-049, REQ-050) [tool-verified: `provisa/api/data/endpoint.py:84-90`]

---

### 重定向

超过配置行数阈值的结果（或当 `X-Provisa-Redirect: true` 时）会写入 S3，并返回一个预签名 URL。(REQ-029, REQ-044)

| 重定向格式 | 写入者 | 内存 |
| --- | --- | --- |
| `application/vnd.apache.parquet` | 联邦 CTAS | 无 — 数据从不经过 Provisa |
| `application/x-orc` | 联邦 CTAS | 无 — 数据从不经过 Provisa |
| `application/json` | Provisa | 受内存限制 |
| `application/x-ndjson` | Provisa | 受内存限制 |
| `text/csv` | Provisa | 受内存限制 |
| `application/vnd.apache.arrow.stream` | Provisa | 受内存限制 |

对于大型分析导出，请使用 Parquet 或 ORC 重定向。联邦引擎会并行直接写入 S3 — 数据不经过 Provisa。(REQ-138)

```yaml
X-Provisa-Redirect-Format: application/vnd.apache.parquet
X-Provisa-Redirect-Threshold: 1000
```

---

### `POST /data/sql`

通过第二阶段治理管道执行原始 SQL。(REQ-267) [tool-verified: `provisa/api/data/endpoint_dev.py:62`]

**请求体：**

```json
{
  "sql": "SELECT id, amount FROM orders WHERE region = 'us'",
  "role": "admin"
}
```

**所需能力：** `QUERY_DEVELOPMENT`。

`POST /data/sql` 上的治理违规返回 HTTP 403。(REQ-002, REQ-266)

**响应：** 与 `/data/graphql` 格式相同（默认返回 JSON 行，可通过 `Accept` 进行内容协商）。

---

### `POST /data/query`

统一查询端点。接受 GraphQL、SQL 或 Cypher — 自动检测语法。(REQ-267) [tool-verified: `provisa/api/data/endpoint_dev.py:509`]

Cypher 查询也可以提交给仅支持 Cypher 的 `POST /query/cypher` 端点。(REQ-345)

**请求体：**

```json
{
  "query": "{ orders { id } }",
  "params": {},
  "variables": {},
  "role": "admin"
}
```

对于 GraphQL 返回 `{"data": ...}`，对于 SQL 和 Cypher 返回 `{"columns": [...], "rows": [...]}`。

---

### `POST /data/sql/explain`

通过受治理管道解释或分析一条 SQL 语句。(REQ-1519) [tool-verified: `provisa/api/data/endpoint_dev.py:328`]

该端点会把**受治理的** SQL——即在 RLS 与脱敏之后、真正以调用者角色运行的那条语句——包裹进该方言的 EXPLAIN 语法中。计划展示的是查询的授权版本，而非原始输入。

**请求体：**

```json
{
  "sql": "SELECT id, amount FROM orders",
  "role": "admin",
  "analyze": false
}
```

设置 `analyze: true` 以运行 EXPLAIN ANALYZE。查询会实际执行，计划中带有真实的行数与耗时。并非每种方言都支持 ANALYZE；参见[查询计划与统计信息](engines.md#query-plans-and-statistics)中的表格。

**响应：** `{"plan": "<plan text or JSON>", "dialect": "trino", "analyzed": false}`

当方言不支持 EXPLAIN，或在不支持 ANALYZE 的方言（例如 SQLite）上请求 `analyze: true` 时，返回 `400`。[tool-verified: `provisa/executor/explain.py:wrap_explain`, `analyze_sql`]

---

### `GET /data/engine/state`

返回引擎分片的当前状态，且不会唤醒它。(REQ-1516) [tool-verified: `provisa/api/data/endpoint_dev.py:892`]

界面会轮询该端点，以便在引擎冷启动期间显示启动横幅。它绝不会触发唤醒——轮询是安全的，也不会被空闲回收器计为活动。

**响应：**

```json
{"state": "ready"}
```

可能的取值：

| 状态 | 含义 |
| --- | --- |
| `always-on` | 桌面版、自托管或自带协调器——不做生命周期管理 |
| `ready` | 分片已启动并接受查询 |
| `starting` | 冷启动进行中 |
| `stopped` | 分片已缩容至零 |

[tool-verified: `provisa/federation/engine_wake.py:engine_state`]

---

### `POST /data/engine/prewarm`

在不运行查询的情况下触发引擎唤醒。(REQ-1516) [tool-verified: `provisa/api/data/endpoint_dev.py:913`]

立即返回 `202 Accepted`。唤醒在后台进行。若希望在第一条查询到达之前引擎已就绪，可使用它——例如由几分钟后才运行查询的调度器发起。

**响应：** `202 Accepted`，响应体 `{"started": true}`

[tool-verified: `provisa/federation/engine_wake.py:prewarm_engine`]

---

### `GET /data/rest/{domain_id}/{table_name}`

为每张已注册表自动生成的普通 REST 端点。查询字符串映射为 GraphQL 参数，请求通过与 GraphQL 相同的管道（RLS、脱敏、路由）编译并执行。(REQ-256) [tool-verified: `provisa/api/rest/generator.py:153`]

**查询参数：**

- `limit` — 最大行数（≥ 1）
- `offset` — 跳过的行数（≥ 0）
- `fields` — 逗号分隔的列名（默认为所有标量字段）
- `filter` — `{"field", "comparator", "value"}` 过滤对象组成的 JSON 数组
- `orderBy` — `{"field", "direction"}` 排序对象组成的 JSON 数组

需要已认证的角色；未认证请求返回 `401`。这些路由的 OpenAPI 规范在 `GET /data/rest/openapi.json` 提供，Swagger UI 在 `GET /data/rest/docs` 提供。

#### OpenAPI / Swagger UI 浏览器

OpenAPI 浏览器页面（`/app/openapi`）在沙盒化的 iframe 中嵌入 Swagger UI。该规范是按角色限定的 — 只显示当前角色可见的表和列 — 并可选地通过域选择器按域过滤。UI 会自动在浅色和深色主题之间切换。[tool-verified: `provisa-ui/src/pages/OpenApiPage.tsx:20-34`]

该页面通过 `fetch()` 而非直接的 iframe `src` 来加载规范 HTML，因此请求会携带会话的持有者令牌，且 Swagger UI 自身的相对请求能正确解析到同一源。[tool-verified: `provisa-ui/src/pages/OpenApiPage.tsx:44-69`]

当从 NL 的“在 OpenAPI 中打开”链接导航过来时，该页面会自动展开目标端点，从 NL 生成的 URL 中填充查询参数（例如 `aggregate`、`groupBy`），并点击“执行” — 使用 DOM 轮询确保每一步完成后再触发下一步。(REQ-1359) [tool-verified: `provisa-ui/src/pages/OpenApiPage.tsx:94-171`]

---

### `GET /data/jsonapi/{domain_id}/{table_name}`

为每张已注册表自动生成的符合 [JSON:API](https://jsonapi.org) 规范的端点。与 GraphQL 相同的 RLS、脱敏和路由。(REQ-257) [tool-verified: `provisa/api/jsonapi/generator.py:284`]

**`Accept` 请求头：** 必须包含 `application/vnd.api+json`（JSON:API 媒体类型），否则请求返回 `406`。

**查询参数：**

- `fields[<type>]` — 稀疏字段集，例如 `?fields[orders]=amount`
- `filter[<col>]` / `filter[<col>][<op>]` — 例如 `?filter[region]=US`、`?filter[amount][gt]=100`
- `sort` — 逗号分隔，`-` 前缀表示降序，例如 `?sort=-created_at,amount`
- `page[number]` / `page[size]` — 分页
- `aggregate` — 逗号分隔的聚合函数，替代行检索：`count`、`sum`、`avg`、`stddev`、`variance`、`min`、`max`。使用 `?aggregate=count,sum` 请求子集。聚合响应返回 `data: null`，结果在 `meta.aggregate` 中。(REQ-1359) [tool-verified: `provisa-ui/src/pages/JsonApiPage.tsx:238`]
- `groupBy` — 逗号分隔的列名；与 `?aggregate=` 一起使用以对结果分组。只有表的 `DistinctOnColumn` 枚举中的列才有效；对于角色不可见的任何列，服务器返回 `400`。(REQ-1361) [tool-verified: `provisa-ui/src/pages/JsonApiPage.tsx:447`]
- `includeNodes` — 设为 `true` 以在每个分组行的 `nodes` 数组中包含基表标量列（以及 `include=` 中指定的关联维度标量）。当 NL 分组查询同时请求维度详情时为必需项。(REQ-1405)

响应是带有 `type`/`id`/`attributes` 的资源对象。错误遵循 JSON:API 错误对象格式。

#### JSON:API 浏览器

JSON:API 浏览器页面（`/app/jsonapi`）是这些端点上的浏览器 UI。从按域分组的列表中选择一张表，然后配置：

- **字段** — 选择要包含的列（稀疏字段集）；全部不勾选则请求所有列
- **关联** — 选择要通过 `?include=` 侧载的、由外键派生的关联名称
- **过滤** — 字段、运算符（`eq`、`neq`、`gt`、`gte`、`lt`、`lte`、`like`）和值
- **排序** — 一个字段，升序或降序
- **聚合** — 从服务器校验过的列表中选择分组列，然后勾选一个或多个聚合函数；选择了分组列后，会出现一个“包含节点”复选框，用于将基表标量列附加到每一行
- **分页大小** — 每页资源数，支持首页/上一页/下一页/末页导航

结果以格式化摘要视图（带可点击关联锚点的资源卡片）或原始 JSON 标签页呈现。会显示当前请求 URL，可复制。表选择和分页大小会在 `localStorage` 中跨会话保留。[tool-verified: `provisa-ui/src/pages/JsonApiPage.tsx`]

当从 NL 的“在 JSON:API 中打开”链接导航过来时，浏览器会预选该表，并从 NL 生成的查询参数中填充聚合选择器，然后自动运行请求。[tool-verified: `provisa-ui/src/pages/JsonApiPage.tsx:460-479`]

---

### `POST /query/nl`

提交一个自然语言问题。该服务启动一个异步作业，并立即返回 `202 Accepted` 及一个 `job_id`。需要在 `ai_models` 配置节下配置一个 LLM 提供方。(REQ-354) [tool-verified: `provisa/api/rest/nl_router.py:50`]

**请求体：**

```json
{"q": "How many orders were placed last month?", "role": "admin"}
```

返回 `{"job_id": "<id>"}`。超过每角色 NL 速率限制会返回带 `Retry-After` 响应头的 `429`。(REQ-370)

**获取结果：**

- `GET /query/nl/{job_id}` — 轮询。返回作业文档。
- `GET /query/nl/{job_id}/stream` — SSE。每个生成目标完成时触发一个 `branch` 事件，最后触发一个 `done` 事件。(REQ-357, REQ-358)

三个生成循环（Cypher、GraphQL、SQL）并行运行，每个都通过编译器校验并在出错时进行修正。(REQ-355) 提示词的作用域限定在该角色可见的架构内。(REQ-356) 结果文档按目标为每个分支建键：(REQ-357) [tool-verified: `provisa/nl/job.py:69`]

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

耗尽迭代次数限制的分支会返回 `query: null`、`result: null` 以及一个 `error` 字符串。每个生成的查询都在消费者的权限下执行，并应用第二阶段治理 — 该服务从不绕过治理。(REQ-359)

#### 带维度详情的 NL 分组查询（REQ-1405）

当一个 NL 分组查询同时投影来自关联维度表的列时 — 例如“按用户分组统计咨询数量，并显示用户名和邮箱” — 运行器会从 SELECT 投影出的维度列中派生出按字段的点路径（`dim_paths`）。这些路径会填充 JSON:API 和 OpenAPI 面板生成 URL 上的 `includeNodes=` 参数，使这些面板请求与 SQL 和 GraphQL 分支所解析的相同关联维度字段。若没有这一机制，`includeNodes=true` 将只返回基础聚合表自身的标量字段。(REQ-1405) [tool-verified: `docs/arch/requirements.md:REQ-1405`]

在 gRPC 面板上，生成的 `{Type}GroupByRequest` 携带 `include_nodes`（布尔值）和 `include`（关联字段名的重复字符串）。返回的 `{Type}GroupByRow` 包含一个带维度详情行的类型化 `nodes` 字段。[tool-verified: `provisa/grpc/query_ir.py:168-196`]

---

### `GET /data/sdl`

返回某角色架构的 GraphQL SDL。(REQ-008) [tool-verified: `provisa/api/data/sdl.py:137`]

**请求头：** `X-Role: <role_id>`（必需）

**查询参数：**

- `domain` — 逗号分隔的域 ID。设置后，响应会过滤为所指定的域及其可达的表。

**响应：** `text/plain` 格式的 GraphQL SDL。

---

### `GET /data/introspection`

返回 GraphQL 自省 JSON，可选按域过滤。[tool-verified: `provisa/api/data/sdl.py:200`]

**请求头：** `X-Provisa-Role: <role_id>`（必需）

**查询参数：** `domain` — 逗号分隔的域 ID。

**响应：** `application/json` 格式的自省结果。

---

### `GET /data/graph-schema`

返回该角色架构的图视图：节点标签及其关联类型，供 Cypher/图客户端使用。每个节点标签都包含 `pk_columns`，以便调用方确定主键列。(REQ-398) [tool-verified: `provisa/api/rest/cypher_router.py:689`]

**响应：** `application/json`，包含 `node_labels`（每个都带有 `pk`/`pk_columns`）和 `relationship_types`。

每种关系类型还带有 `junction_table_name` 和 `properties`（REQ-1586）。在由联结表支撑的边上，前者给出它所穿过的关联表名，后者列出该表中可作为 `r.attr` 读取并可在 `WHERE` 中过滤的列；在由外键支撑的边上，该名称为 `null`，属性列表为空——客户端正是据此区分两者。联结表本身永远不是节点标签——它就是边，因此在图客户端中没有对应的标签胶囊，在 `node_labels` 中也没有对应行。[tool-verified: `provisa/api/rest/cypher_router.py:797-805`, `provisa/cypher/label_map.py:378-397`]

---

### `GET /data/domains`

返回请求角色可访问的域 ID。[tool-verified: `provisa/api/data/sdl.py:116`]

**请求头：** `X-Role: <role_id>`（必需）

**响应：** `["sales", "support", ...]`

---

### `GET /data/schema-version`

返回当前架构版本字符串。将每次启动的随机数与重建计数器组合而成。客户端使用它在服务器重启后使架构缓存失效。(REQ-537) [tool-verified: `provisa/api/data/sdl.py:102`]

**响应：** `{"version": "<boot-id>-<counter>"}`

---

### `GET /data/proto/{role_id}`

返回某角色自动生成的 `.proto` 文件。[tool-verified: `provisa/api/data/endpoint_dev.py:49`]

**响应：** `text/plain` 格式的 protobuf 架构。

每张已注册表都会生成一个 proto `message`。关联会生成嵌套的消息字段。类型映射：`integer → int32`、`bigint → int64`、`varchar → string`、`decimal → double`、`boolean → bool`、`timestamp → google.protobuf.Timestamp`。(REQ-538)

---

### `GET /data/subscribe/{table}`

用于从表获取实时变更通知的服务器发送事件（SSE）流。(REQ-219, REQ-258) [tool-verified: `provisa/api/data/subscribe.py:239`]

通知投递使用按数据源类型选择的可插拔提供方：PostgreSQL 数据源使用 `LISTEN/NOTIFY`（通过 asyncpg），MongoDB 数据源使用 Change Streams（`collection.watch()`），Kafka 数据源使用消费者组。每个提供方都实现一个通用的异步监听接口。无论使用哪种提供方，RLS 过滤和架构校验都会照常应用。(REQ-258) 同一端点也支持 WebSocket 和 RSS 数据源。(REQ-338, REQ-342)

**请求头 —— `X-Provisa-Sink`：** 设为 Kafka 目标（例如 `kafka://broker:9092/topic`）以将变更事件重定向到 Kafka 接收端，而非 SSE 响应。服务器会启动一个接收消费者并返回 `202 Accepted`，而不是打开一个流。(REQ-812) [tool-verified: `provisa/api/data/subscription_sse.py:137`]

---

## Admin REST 端点

### 配置

#### `GET /admin/config`

以 `application/x-yaml` 格式下载当前 `provisa.yaml`，并附带 `Content-Disposition: attachment` 响应头。(REQ-164) [tool-verified: `provisa/api/admin/settings_router.py:19`]

#### `PUT /admin/config`

上传修订后的配置 YAML。服务器会写入 `.bak` 备份，保存新文件，并重新加载所有架构、数据源和物化视图。(REQ-164) [tool-verified: `provisa/api/admin/settings_router.py:32`]

**请求体：** 原始 YAML 内容。

**响应：**

```json
{"success": true, "message": "Config uploaded and reloaded"}
```

重新加载失败时：`{"success": false, "message": "<error>"}`。

#### `GET /admin/config/live`

下载**当前实时配置**——即 Provisa 此刻会写出的配置，反映自启动以来累积的每一处管理端创建的表、关联、域、角色和 RLS 规则。(REQ-164) [tool-verified: `provisa/api/admin/settings_router.py:67`]

若改动是通过 Admin API 做出且此后未再上传，磁盘上的文件可能落后于实时状态。该端点弥合这一差距：它的输出正是 `PUT /admin/config` 需要收到、才能让磁盘文件与实时状态一致的内容。

返回 `application/x-yaml`，并带 `Content-Disposition: attachment; filename=provisa.live.yaml`。

#### `GET /admin/config/diff`

返回配置差异的两侧——`original`（启动时的基线）与 `current`（实时状态）——二者以相同方式规范化，因此比较只呈现真正的改动，而非重新排序或注释漂移。(REQ-164) [tool-verified: `provisa/api/admin/settings_router.py:82`]

**响应：**

```json
{"original": "<yaml>", "current": "<yaml>"}
```

#### `POST /admin/config/patch`

生成一个从基线到所提交配置的 unified diff 补丁。(REQ-164) [tool-verified: `provisa/api/admin/settings_router.py:93`]

把修订后的 YAML 作为请求体发送。响应是一个 `text/x-patch` 文件（`provisa.config.patch`），`git apply` 或 `patch` 可直接使用——便于把界面驱动的配置改动经由 CI/CD 管道提交。

---

### 设置

#### `GET /admin/settings`

以 JSON 格式返回当前平台设置。(REQ-165) [tool-verified: `provisa/api/admin/settings_router.py:50`]

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

在运行时更新平台设置。所有字段都是可选的 — 仅更新请求体中出现的键。(REQ-165) [tool-verified: `provisa/api/admin/settings_router.py:100`]

**请求体（局部示例）：**

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

各节可更新字段：

- `redirect`：`enabled`、`threshold`、`default_format`、`ttl`
- `sampling`：`default_sample_size`
- `cache`：`default_ttl`
- `naming`：`domain_prefix`、`convention` — 写入配置文件并触发架构重新加载（REQ-253）
- `relationships`：`auto_track_fk` —— 仅管辖外键追踪。由联结表支撑的关系是在表注册时声明的，从不被推断，因此该设置对它不起作用。(REQ-1586)
- `otel`：`endpoint`、`service_name`、`sample_rate`、`support_endpoint`、`support_redact_sql_literals`、`support_redact_attributes`

**响应：**

```json
{"success": true, "updated": ["otel.support_endpoint", "cache.default_ttl"]}
```

---

### AI 模型

#### `GET /admin/ai-models`

返回当前操作所属组织的 AI 模型分配、向量模型注册表以及自然语言速率限制。(REQ-464、REQ-1349) [tool-verified: `provisa/api/admin/ai_models_router.py:58`]

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

API 密钥绝不会被回显——`api_keys_set` 只报告每个供应商是否已配置密钥。改动自下一次请求起生效，无需重启。(REQ-1349)

#### `PUT /admin/ai-models`

更新该组织的 AI 模型分配、向量模型注册表或自然语言速率限制。自下一次请求起生效。[tool-verified: `provisa/api/admin/ai_models_router.py:148`]

#### `GET /admin/ai-models/vendors/{vendor}/models`

返回某供应商当前提供的模型名称，供模型选择器使用。(REQ-1395、REQ-1398、REQ-1409) [tool-verified: `provisa/api/admin/ai_models_router.py:89`]

该列表使用组织已配置的密钥（未设置组织密钥时则用部署凭据），从供应商自身的 list-models API 实时读取。在本版本发布之后才推出的模型，供应商上线当天即可选择。

当供应商未发布 list-models API（此时直接输入模型名称）或没有可用密钥时，返回 `400`。[tool-verified: `provisa/api/admin/ai_models_router.py:109-128`]

---

### 联邦引擎

#### `GET /admin/federation-engine`

返回当前的联邦引擎选择、其连接配置，以及完整的可选引擎注册表。(REQ-916) [tool-verified: `provisa/api/admin/settings_router.py:730`]

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

`current` 键是此刻正在运行的引擎；`persisted` 是写入配置文件、并将在下次重启时加载的那个。当配置已更改但服务尚未重启时，二者会不一致。

#### `PUT /admin/federation-engine`

持久化一次联邦引擎选择。(REQ-916) [tool-verified: `provisa/api/admin/settings_router.py:774`]

**请求体：**

```json
{"engine": "trino", "federation_engine_url": "http://trino-coordinator:8080"}
```

该选择会写入平台配置。它在下次服务重启后生效——引擎在启动时选定一次。

---

### 域策略

#### `POST /admin/domain-policy`

更改当前操作所属组织的域策略（`use_domains` / `default_domain`）。(REQ-165、REQ-1266、REQ-1349) [tool-verified: `provisa/api/admin/settings_router.py:632`]

这是限定在该组织范围内的破坏性操作。每个已注册的数据源、表、域和关联都会被清除并按新策略重建。在把某组织从域命名空间切换为扁平（或反向切换）时使用它。

**请求体：**

```json
{
  "use_domains": true,
  "default_domain": "default"
}
```

`use_domains: null` 会清除该组织的覆盖设置，回落到部署级设置。`use_domains: false` 需要提供 `default_domain`（所有表落入的那个唯一域名）。目录重建是同步的；响应在架构就绪后才返回。

---

### 可观测性

#### `GET /admin/traces/recent`

从内存中的跨度缓冲区返回最多 N 条最近完成的跨度。(REQ-302) [tool-verified: `provisa/api/admin/settings_router.py:317`]

**查询参数：** `limit`（默认 50，最大 200）

**响应：** `{"traces": [...]}`

#### `POST /admin/query-engine/reload-catalog`

通过联邦引擎协调器的 REST API 对其中命名的目录进行热重载。重新连接 Provisa 的内部连接并重新运行 OTel DDL。[tool-verified: `provisa/api/admin/settings_router.py:208`]

**查询参数：** `catalog`（默认 `"otel"`）

**响应：**

```json
{"success": true, "errors": []}
```

#### `POST /admin/query-engine/restart`

重启联邦引擎容器（仅限单节点开发环境）。[tool-verified: `provisa/api/admin/settings_router.py:287`]

**查询参数：** `container`（默认使用 `QUERY_ENGINE_CONTAINER` 环境变量，其次为 `"trino"`）

---

### 发现

#### `POST /admin/discover/relationships`

触发关联发现。始终从联邦引擎运行外键自省。(REQ-018) 若设置了 `ANTHROPIC_API_KEY`，则运行 LLM 推理。(REQ-167) [tool-verified: `provisa/api/admin/discovery.py:55`]

**请求体：**

```json
{
  "scope": "domain",
  "domain_id": "sales"
}
```

`scope` 必须是 `"table"`、`"domain"`、`"cross-domain"` 之一。对于 `"table"` 作用域，需要 `table_id`（整数）。对于 `"domain"` 作用域，需要 `domain_id`。

**响应：** `{"candidates_found": 12, "stored_ids": [1, 2, 3, ...]}`

#### `GET /admin/discover/candidates`

列出待处理的关联候选项。[tool-verified: `provisa/api/admin/discovery.py:96`]

#### `POST /admin/discover/candidates/{candidate_id}/accept`

接受一个候选项并将其注册为一个关联。[tool-verified: `provisa/api/admin/discovery.py:103`]

**请求体（可选）：** `{"name": "custom-relationship-name"}`

#### `POST /admin/discover/candidates/{candidate_id}/reject`

拒绝一个候选项。[tool-verified: `provisa/api/admin/discovery.py:110`]

**请求体：** `{"reason": "Not a real join"}`

#### `GET /admin/discover/candidates/rejected/count`

返回已拒绝候选项的数量。[tool-verified: `provisa/api/admin/discovery.py:118`]

#### `DELETE /admin/discover/candidates/rejected`

删除所有已拒绝的候选项。[tool-verified: `provisa/api/admin/discovery.py:128`]

---

### 数据源爬取

#### `POST /admin/sources/crawl`

爬取一个数据源以自省其架构并注册表。(REQ-012) [tool-verified: `provisa/api/admin/crawl_router.py:36`]

---

### 数据源表搜索

#### `GET /admin/sources/{source_id}/tables/search`

按名称搜索某数据源中可用（尚未注册）的表。[tool-verified: `provisa/api/admin/table_search_router.py:103`]

---

### 表画像分析

#### `POST /admin/tables/{table_id}/profile`

对已注册表运行列画像分析 — 基数、最小/最大值、空值率。[tool-verified: `provisa/api/admin/table_profile_router.py:28`]

---

### 数据源描述

#### `POST /admin/source-meta/db-description`

为某数据源的表和列生成 LLM 辅助描述。[tool-verified: `provisa/api/admin/source_meta_router.py:48`]

---

### 对象存储（REQ-1046、REQ-1048、REQ-1049）

#### `GET /admin/org-storage`

报告当前操作所属组织相对其平台配额的存储占用，以及该组织是否已注册自有存储。[tool-verified: `provisa/api/admin/org_storage_router.py:69`]

当组织已注册自有 DSN 时，其物化会落到那里，且不再计入配额。DSN 本身绝不返回。

#### `PUT /admin/org-storage`

注册（或清除）该组织自有的物化存储。[tool-verified: `provisa/api/admin/org_storage_router.py:81`]

**请求体：**

```json
{"storage_url": "s3://my-bucket/provisa?region=us-east-1&access_key=..."}
```

DSN 在被接受之前会先针对联邦引擎进行验证——不可用的 DSN 在注册时即失败，而不是数小时后在刷新过程中才暴露。该值静态加密存储，GET 绝不返回。

发送 `storage_url: null` 可清除组织自有存储，并把其物化归还到平台存储（及配额）。组织运行时会在同一次调用中重建，因此新存储立即生效。[tool-verified: `provisa/api/admin/org_storage_router.py:123-138`]

---

### 组织加密（REQ-1574）

#### `GET /admin/org-encryption`

返回该组织当前的密钥状态：指纹、id 和来源。绝不返回密钥材料。[tool-verified: `provisa/api/admin/org_encryption_router.py:53`]

当组织未设置密钥时，返回 `{"configured": false}`。每个组织都以此状态起步，并继承部署的密钥。

#### `PUT /admin/org-encryption`

设置或轮换该组织的静态加密密钥。[tool-verified: `provisa/api/admin/org_encryption_router.py:68`]

**请求体：**

```json
{"key_b64": "<32 raw bytes, base64-encoded>"}
```

省略 `key_b64` 可让 Provisa 生成密钥——这是最稳妥的路径，因为密钥不会出现在剪贴板或请求日志中。提供 `key_b64` 则表示自带密钥。

轮换会向密钥环中新增一个活动条目并保留旧条目，因此以先前密钥写入的数据仍可读取。轮换不是重新加密。没有删除端点：停用最后一个密钥会让每一份被包裹的载荷都无法读取。[tool-verified: `provisa/api/admin/org_encryption_router.py:75`]

实时密钥环会在同一次调用中重新绑定，因此下一次加密写入立即使用新密钥。

---

### Hasura / DDN 导入（REQ-1483）

#### `POST /admin/import/hasura/preview`

把 Hasura v2 或 DDN 项目归档转换为建议的 Provisa 配置，且不写入任何内容。[tool-verified: `provisa/api/admin/import_router.py`]

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

`flavor` 可为 `"auto"`（从归档结构中检测）、`"hasura_v2"` 或 `"ddn"`。

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

不会持久化任何内容。预览不在服务端缓存；`apply` 取用的是你提交的 YAML，因此所应用的内容与所审阅（并可选择性编辑过）的完全一致。

#### `POST /admin/import/hasura/apply`

把先前预览过的配置加载到当前操作所属的组织。[tool-verified: `provisa/api/admin/import_router.py`]

**请求体：**

```json
{"config_yaml": "<yaml string>"}
```

使用与 `PUT /admin/config` 相同的热重载路径。该组织的目录、架构与连接池会在响应返回之前完成重建。

---

### Apache Ossie 互通（REQ-1316、REQ-1321）

#### `GET /admin/ossie`

把该组织的受治理模型导出为 Apache Ossie（孵化中）YAML 文档。(REQ-1321) [tool-verified: `provisa/api/admin/ossie_router.py`]

该文档在每次请求时都从实时状态推导——绝不缓存——因此不可能过期。表成为 `dataset` 对象，列成为 `field` 对象，关联映射为 Ossie 的 `relationship` 对象。

返回 `text/yaml`，并带 `Content-Disposition: attachment; filename=provisa-ossie.yaml`。

#### `POST /admin/ossie/import`

解析一份 Ossie 的 YAML 或 JSON 文档，并返回建议注册的表与关联。(REQ-1316) [tool-verified: `provisa/api/admin/ossie_router.py`]

**请求体：** 原始的 Ossie YAML 或 JSON。格式自动检测。

**响应：**

```json
{
  "proposals": {
    "tables": [...],
    "relationships": [...]
  }
}
```

不会注册任何内容。请使用管理界面的审阅页面，在任何变更触发之前接受或删减这些建议。

---

### 命令（函数与 Webhook）

所有端点都在 `/admin/actions` 前缀下。(REQ-205) [tool-verified: `provisa/api/admin/actions_router.py:24`]

每次调用 — 无论来自 GraphQL、SQL、Cypher、Bolt、Arrow Flight、MCP `run_sql` 还是 Provisa gRPC — 都会通过一个统一受治理的执行器路由，该执行器统一强制执行 `writable_by` 和治理规则。(REQ-1156) [tool-verified: `provisa/api/data/action_exec.py`] 各协议的调用语法参见 [docs/integrations.md](integrations.md#_6)。

#### `GET /admin/actions`

返回所有已跟踪的数据库函数和 Webhook。(REQ-242) [tool-verified: `provisa/api/admin/actions_router.py:104`]

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
      "writableBy": [],
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

每个 Webhook 对象都带有一个 `approved` 布尔值。当某位治理员执行其创建请求后，该 Webhook 才被批准（REQ-209）；配置声明的 Webhook 会自动批准。未批准的 Webhook 会被注册，但不会在任何界面上暴露。[tool-verified: `provisa/api/admin/actions_router.py:124-131`]

#### `POST /admin/actions/functions`

注册一个已跟踪的函数（命令）。(REQ-205) [tool-verified: `provisa/api/admin/actions_router.py:117`]

**关键字段：**

| 字段 | 必需 | 说明 |
| --- | --- | --- |
| `name` | 是 | 唯一的命令名称 |
| `kind` | 是 | `"query"` → GraphQL Query 字段；`"mutation"` → Mutation 字段 |
| `implKind` | 否 | 命令的运行方式 — 见下表（默认 `source_procedure`） |
| `binding` | 否 | `implKind` 特定的连接详情（JSON 对象） |
| `returnSchema` | 否 | JSON Schema `{type:"array", items:{type:"object", properties:{...}}}` — 使该命令在所有界面上都是集合返回型 |
| `arguments` | 否 | `[{name, type}]` 参数定义；对于 SQL 和 Bolt 调用方，位置顺序很重要 |
| `visibleTo` | 否 | 可调用该命令的角色 ID |
| `writableBy` | 否 | 允许将其作为变更调用的角色 ID |
| `domainId` | 否 | 用于 GraphQL 放置和访问控制的域 |

**`implKind` 取值：**

| `implKind` | 运行内容 | `binding` 字段 |
| --- | --- | --- |
| `source_procedure` | 已注册数据源上的存储过程（默认） | `sourceId`、`schemaName`、`functionName` |
| `script` | 服务端脚本 | `script` |
| `http` | 出站 HTTP 调用 | `url`、`method` |
| `grpc` | 到外部服务器的出站 gRPC 调用 | `target`、`method` |
| `python` | 由 Provisa 托管的 Python 可调用对象（REQ-885） | `callable`（例如 `"demo.py_functions:random_dataset"`） |

演示命令 `random_python_set`（`implKind: python`）和 `random_grpc_set`（`implKind: grpc`）展示了带 `returnSchema` 的集合返回型命令的实际用法；二者均在 `config/provisa-install.yaml` 中。[tool-verified: `config/provisa-install.yaml:809-856`]

#### `PUT /admin/actions/functions/{name}`

按名称更新一个已跟踪的函数。[tool-verified: `provisa/api/admin/actions_router.py:182`]

#### `DELETE /admin/actions/functions/{name}`

按名称删除一个已跟踪的函数。[tool-verified: `provisa/api/admin/actions_router.py:233`]

#### `POST /admin/actions/webhooks`

注册一个已跟踪的 Webhook。(REQ-209) 注册或更新一个 Webhook 会排入一个治理员批准请求 — 只有在治理员批准后，该 Webhook 才会在所有界面上生效。配置声明的 Webhook 会自动批准。**请求体字段：** `name`、`url`、`method`、`timeoutMs`、`returns`、`inlineReturnType`、`arguments`、`visibleTo`、`domainId`、`description`、`kind`。[tool-verified: `provisa/api/admin/actions_router.py:132`, `provisa/api/admin/actions_router.py:325-331`]

#### `PUT /admin/actions/webhooks/{name}`

按名称更新一个已跟踪的 Webhook。任何编辑都会将批准状态重置为待处理，直到重新批准。[tool-verified: `provisa/api/admin/actions_router.py:306`]

#### `DELETE /admin/actions/webhooks/{name}`

按名称删除一个已跟踪的 Webhook。[tool-verified: `provisa/api/admin/actions_router.py:355`]

#### `POST /admin/actions/test`

按名称测试一个命令（函数或 Webhook）。(REQ-245) [tool-verified: `provisa/api/admin/actions_router.py:384`]

---

### 角色

所有端点都在 `/admin/roles` 前缀下。[tool-verified: `provisa/api/admin/roles_router.py:18`]

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| `GET` | `/admin/roles/` | 列出所有角色 |
| `POST` | `/admin/roles/` | 创建一个角色 |
| `PUT` | `/admin/roles/{role_id}` | 更新一个角色 |
| `DELETE` | `/admin/roles/{role_id}` | 删除一个角色 |

[tool-verified: `provisa/api/admin/roles_router.py`]

---

### 用户

所有端点都在 `/admin/users` 前缀下。[tool-verified: `provisa/api/admin/local_users_router.py:21`]

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

### 组织

所有端点都在 `/admin/orgs` 下。[tool-verified: `provisa/api/admin/orgs_router.py:18`]

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

### 邀请

所有端点都在 `/admin/invites` 下。[tool-verified: `provisa/api/admin/invites_router.py:18`]

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| `POST` | `/admin/invites/` | 创建一个邀请 |
| `GET` | `/admin/invites/` | 列出待处理的邀请 |
| `DELETE` | `/admin/invites/{token}` | 撤销一个邀请 |

---

### Admin GraphQL

#### `POST /admin/graphql`

用于所有管理操作的 Strawberry GraphQL 端点：数据源和表的增删改查、关联管理、域配置、RLS 规则、缓存控制、命名约定、调度任务管理，以及查询编译。(REQ-164) [tool-verified: `provisa/api/app.py:2171`]

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

[tool-verified: `provisa/api/admin/schema.py`, `provisa/api/admin/actions_router.py`]

---

### 初始化设置

#### `GET /setup/status`

返回首次运行的设置状态。始终无需身份验证。(REQ-539) [tool-verified: `provisa/api/setup_router.py:100`]

#### `POST /setup/`

完成首次运行设置。[tool-verified: `provisa/api/setup_router.py:142`]

---

## 健康检查

#### `GET /health` 或 `HEAD /health`

返回 `{"status": "ok"}`。始终无需身份验证。(REQ-539) [tool-verified: `provisa/api/app.py:2258`]

---

## 错误响应

| 状态码 | 含义 |
| --- | --- |
| 400 | 无效查询、校验错误或 SQL 解析错误 |
| 401 | 缺失或无效的身份验证令牌 |
| 403 | 能力不足；治理违规 |
| 404 | 未找到角色、资源或配置文件 |
| 422 | 缺失必需的请求头（例如 `X-Role`） |
| 503 | 数据库或数据源未连接；依赖不可用 |
| 504 | 请求超时 |

`POST /data/sql` 上的治理违规返回带结构化响应体的 HTTP 403：(REQ-002) [tool-verified: `provisa/api/data/endpoint_dev.py:184-190`]

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

## Arrow Flight 端点

端口 `8815`。基于 gRPC 的原生 Arrow 列式传输。(REQ-143, REQ-045) [tool-verified: `provisa/api/flight/server.py`]

查询和目录发现都在同一连接上可用。完整的治理管道（RLS、脱敏、抽样）应用于每个查询。(REQ-130, REQ-143)

**Ticket 格式**（JSON）：

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

当 Zaychik Flight SQL 代理可用时（端口 8480），记录批次会端到端流式传输，无需完全物化。(REQ-144) 若 Zaychik 不可用，则回退到通过联邦查询层进行物化。(REQ-146)

---

## Protobuf gRPC 端点

端口 `50051`（可通过 `GRPC_PORT` 环境变量或 `server.grpc_port` 配置覆盖）。(REQ-529) [tool-verified: `provisa/grpc/server.py`, `provisa/api/app.py`]

在 `x-provisa-role` gRPC 元数据键中传递角色。若缺失，服务器会以 `UNAUTHENTICATED` 中止。[tool-verified: `provisa/grpc/server.py`]

从 `GET /data/proto/{role_id}` 下载特定角色的 proto。仅显示该角色可见的表和列。(REQ-039)

```proto
service ProvisaService {
  rpc QueryOrders (QueryOrdersRequest) returns (stream Orders);
  rpc InsertOrders (InsertOrdersRequest) returns (InsertOrdersResponse);
}
```

每张表都会生成一个 `Query{TypeName}` 流式 RPC。`Insert{TypeName}` RPC 出于架构对称性而存在，但会以 `UNIMPLEMENTED` 中止。[tool-verified: `provisa/grpc/server.py`]

启用了 `grpc_reflection.v1alpha`，无需预编译的 proto 即可进行服务发现。(REQ-529) [tool-verified: `provisa/grpc/reflection.py`]

```bash
grpcurl -plaintext localhost:50051 list
grpcurl -plaintext -H 'x-provisa-role: analyst' \
  -d '{}' localhost:50051 ProvisaService/QueryOrders
```

只有在启动时能够成功编译出有效 proto 时，gRPC 服务器才会启动。若架构构建失败，gRPC 服务器不会启动。(REQ-529)

#### 聚合与分组 RPC（REQ-1359、REQ-1361、REQ-1405）

当某张表设置了 `enable_aggregates` 时，生成的 proto 除 `Query{TypeName}` 外还会包含两个额外的 RPC：

- **`Query{TypeName}Aggregate`** — 返回该表的聚合标量（`count`；每个数值列的 `sum`、`avg`、`stddev`、`variance`；每个可比较列的 `min`、`max`）
- **`Query{TypeName}GroupBy`** — 每个分组键返回一行，带聚合子字段，并可选地在 `nodes` 字段中包含基表标量和关联维度行

两者都通过与 GraphQL 的 `{field}_aggregate` 和 `{field}_group_by` 根字段相同的编译器聚合管道路由 — 没有单独的聚合实现。(REQ-1359) [tool-verified: `provisa/grpc/query_ir.py:133-196`]

**`funcs` 字段（REQ-1361）。** 请求消息接受一个 `funcs` 重复字符串字段。有效值为 `count`、`sum`、`avg`、`stddev`、`variance`、`min` 和 `max`。省略 `funcs` 时，会请求架构为该表暴露的每一个函数。设置后，只有指定的函数才会出现。如果指定的函数都不适用于该表的列类型，查询会回退到 `count`。[tool-verified: `provisa/grpc/query_ir.py:66`, `provisa/grpc/query_ir.py:75-97`]

**`include_nodes` 和 `include` 字段（REQ-1405）。** `Query{TypeName}GroupBy` 请求可以设置 `include_nodes: true`，以在每一行的 `nodes` 字段中包含基表标量列。`include` 重复字符串字段指定多对一关联字段的名称，其标量列同样嵌套在 `nodes` 内。这与 JSON:API 的 `?includeNodes=` / `?include=` 行为一致。[tool-verified: `provisa/grpc/query_ir.py:168-195`]

---

## JDBC 驱动

Provisa JDBC 驱动（`provisa-jdbc-0.1.0.jar`）将语义目录暴露给 BI 工具（Tableau、PowerBI、DBeaver）。(REQ-126)

**连接 URL：** `jdbc:provisa://host:port`（REQ-131）

域映射为 JDBC 架构。(REQ-127) 表使用其已注册的别名。列使用别名，并将描述暴露为 `REMARKS`。(REQ-128) 标准元数据方法（`getPrimaryKeys`、`getImportedKeys`、`getExportedKeys`）将语义关系暴露为主键/外键元数据。

**SQL 支持：** `SELECT * FROM <alias> [WHERE col = 'value']`。(REQ-129)

驱动默认请求 Arrow IPC 重定向。结果通过 `ArrowStreamReader` 逐批流式传输，内存占用不超过一个记录批次。(REQ-293)

---

## `orderBy` 参数格式

`order_by` 参数使用 `{column: direction}` 对象，direction 是一个 6 值枚举：(REQ-200)

```json
{
  "query": "{ orders(order_by: [{created_at: desc_nulls_last}]) { id created_at } }",
  "role": "admin"
}
```

支持的方向：`asc`、`desc`、`asc_nulls_first`、`asc_nulls_last`、`desc_nulls_first`、`desc_nulls_last`。(REQ-201)

---

## 订阅

SSE 订阅可在 `GET /data/subscribe/{table}` 获取。(REQ-219, REQ-258) 通知投递使用按数据源类型选择的可插拔提供方：PostgreSQL 数据源使用 `LISTEN/NOTIFY`，MongoDB 数据源使用 Change Streams，Kafka 数据源使用消费者组。无论使用哪种提供方，RLS 过滤和架构校验都会照常应用。同一端点也支持 WebSocket 和 RSS 数据源。(REQ-338, REQ-342) [tool-verified: `provisa/api/data/subscribe.py:239`, `provisa/subscriptions/registry.py`, `provisa/api/app.py` `_rebuild_schemas`]

---

## 业务术语表（REQ-1387）

业务术语表将物理字段名 — 如源数据库中实际存在的那样 — 映射到一套共享的人类词汇。语义层中注册的每一列都会自动获得一个术语。填充术语表无需手动录入；治理者是在系统派生结果之上添加定义、关系和专家。

### 术语如何派生

当 Provisa 注册或更新某张表的列时，`normalize_term`（`provisa/core/glossary.py`）会对每个列名运行，并生成一个规范化短语。[tool-verified: `provisa/core/repositories/glossary.py:sync_table_refs`]

规范化按顺序应用五条规则：

1. 按 camelCase 边界和分隔字符（`_`、`-`、`.`、`/`、空白）拆分。
2. 将结果统一转为小写。
3. 展开一个固定的缩写表（例如 `cust` → `customer`、`amt` → `amount`、`dt` → `date`、`id` → `identifier`、`key` → `identifier`、`guid` → `identifier`）。
4. 去掉末尾的**代理令牌**（`identifier`、`code`、`index` 或 `reference`）— 以键或代码命名的列是通过一个替代值指向其底层概念的，因此术语应该是该概念本身。最后一个剩余的令牌永远不会被去除。
5. 用表的概念限定**过于泛化的短语**。当完整的规范化短语是一个裸属性词（`name`、`identifier`、`date`、`location`、`message`、`first name`、`last name` 及类似词）时，该术语变为 `<表概念> <短语>` —— `employees.first_name` → `employee first name`，`orders.id` → `order identifier`。若不同的、不相关的表共用同一个 `name` 术语，会把不同的含义混为一谈；限定操作则将每个列与其所属概念关联起来。表概念是该表的业务名称，经过单数化的中心名词规范化处理（`order_lines` → `order line`）。

原生过滤器伪列（以 `_nf_` 为前缀，或任何携带 `native_filter_type` 的列）是查询参数机制，而非业务字段，不会派生出术语。

由于 `id`、`key`、`pk` 和 `sk` 在代理检查之前都会展开为 `identifier`，三个物理上不同的列名会归到完全相同的术语上：

| 物理名称 | 规范化后 |
| --- | --- |
| `cust_id` | `customer` |
| `customerId` | `customer` |
| `CUSTOMER_KEY` | `customer` |
| `txn_amt` | `transaction amount` |

前三个归并为一个术语。`transaction amount` 保留了两个词元，因为 `amount` 不是代理词。裸 `id` 列 — 前面没有其他词元 — 无法被去除；它会规范化为 `identifier`，以确保术语非空。[tool-verified: `provisa/core/glossary.py:normalize_term`]

### 生命周期

术语是**从语义层成员关系中派生的**，而不是由用户按需创建的。表仓储是唯一的写入路径：`sync_table_refs` 在每次列集合更新插入操作中运行，`sweep_refless_terms` 在任何删除路径之后运行。[tool-verified: `provisa/core/repositories/glossary.py`]

**添加一列时：** Provisa 按名称查找规范化术语。如果它已存在，该列会获得指向它的引用（如果该术语已被弃用，则会被恢复 — `deprecated` 会被重新设为 `False`）。如果尚不存在该术语，则会创建一个。

**某列离开时**（架构变更或表移除）：其引用会被删除，该术语会根据“移除或弃用”规则进行**结算**。一个没有剩余引用的有根术语会被彻底移除 — 连同其边和专家分配一起 — 除非移除它会使某个抽象术语与所有有根术语失去连接（术语图中没有路径）。在这种情况下，该术语会被**弃用**（标记为 `deprecated=True`）而非删除，以便该抽象术语的图锚点得以保留。

抽象术语从不会被自动移除；它们存在于物理生命周期之外，只能通过管理 API 显式删除。

**恢复：** 如果一个已弃用术语的规范化名称重新出现（某列被重新注册），该术语会被取消标记，其引用会重新开始累积。

### 治理端点

所有端点都在 `/admin/glossary` 下。它们需要 `org_admin` 访问权限和一个已配置的组织。每次变更都会触发一次元数据发布。[tool-verified: `provisa/api/admin/glossary_router.py`]

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| `GET` | `/admin/glossary/terms` | 列出术语。查询参数：`q`（名称/定义搜索）、`include_deprecated`（默认 `true`） |
| `GET` | `/admin/glossary/terms/{term_id}` | 获取术语详情：定义、物理引用、类型化边、专家 |
| `POST` | `/admin/glossary/terms` | 创建一个抽象术语 — 没有物理引用的用户词汇 |
| `PATCH` | `/admin/glossary/terms/{term_id}` | 重命名、设置定义或切换导出排除 |
| `DELETE` | `/admin/glossary/terms/{term_id}` | 删除一个没有物理引用的术语 |
| `POST` | `/admin/glossary/refs/move` | 将一个物理引用移动到另一个术语（合并） |
| `POST` | `/admin/glossary/terms/{term_id}/edges` | 在两个术语之间添加一条类型化关系边 |
| `DELETE` | `/admin/glossary/terms/{term_id}/edges` | 移除一条边（查询参数：`to_term_id`、`rel_type`） |
| `POST` | `/admin/glossary/terms/{term_id}/experts` | 将某用户标记为该术语的专家或作者 |
| `DELETE` | `/admin/glossary/terms/{term_id}/experts/{user_id}` | 移除某用户的专家/作者指定 |
| `POST` | `/admin/glossary/terms/{term_id}/definition/generate` | 使用组织的 AI 模型为一个术语起草定义 — 仅返回文本，在保存前不持久化 |
| `POST` | `/admin/glossary/definitions/generate` | 为每一个尚无定义的术语生成并持久化定义 — 从不覆盖人工撰写的文本 |
| `POST` | `/admin/glossary/relationships/generate` | 使用组织的 AI 模型在整个术语表中提议并持久化类型化边 |

**`POST /admin/glossary/terms` 请求体：**

```json
{"name": "revenue", "definition": "Recognized net revenue after returns and discounts."}
```

**`POST /admin/glossary/terms/{term_id}/edges` 请求体：**

```json
{"to_term_id": 42, "rel_type": "KIND_OF"}
```

有效的 `rel_type` 取值：`KIND_OF`、`RELATED_TO`、`PART_OF`、`SYNONYM_OF`。[tool-verified: `provisa/core/glossary.py:TERM_EDGE_TYPES`]

**`POST /admin/glossary/terms/{term_id}/experts` 请求体：**

```json
{"user_id": "alice@example.com", "kind": "author"}
```

有效的 `kind` 取值：`expert`、`author`。[tool-verified: `provisa/core/repositories/glossary.py:add_expert`]

**`POST /admin/glossary/refs/move` 请求体：**

```json
{"table_id": 7, "column_name": "cust_id", "to_term_id": 12}
```

移动一个引用会根据“移除或弃用”规则结算失去引用的术语。用它来合并两个因规范化而被分开的术语 — 例如，某个数据源使用了展开表之外的非标准缩写时。

删除一个有根术语（拥有物理引用的术语）会返回 `400 glossary.invalid`。请先移除或移动所有引用。

**`PATCH /admin/glossary/terms/{term_id}` — `export_excluded` 字段：**

```json
{"export_excluded": true}
```

将 `export_excluded` 设为 `true` 会将该术语从所有元数据导出快照中扣留，无论其物理引用或抽象状态如何。将其重新设为 `false` 会在下一次发布时将该术语恢复到快照中。治理数据（定义、边、专家）不受影响。[tool-verified: `provisa/core/repositories/glossary.py:set_export_excluded`, `provisa/api/admin/glossary_router.py:update_term`]

### AI 辅助治理

组织配置的 AI 模型可以在一次操作中为整个术语表起草定义并提议关系边。这两项批量操作都需要 `org_admin` 访问权限和一个已配置的组织。

**`POST /admin/glossary/definitions/generate`**

遍历术语表中的每一个术语，跳过已有定义的术语，并调用组织的 AI 模型为每个剩余术语起草一份定义。草稿会立即持久化 — 与逐术语的起草端点（`POST /admin/glossary/terms/{term_id}/definition/generate`）不同，这里没有编辑步骤。人工撰写的定义永远不会被覆盖：在调用模型之前有 `if summary["definition"]: continue` 这一保护。一次发布通知覆盖整批操作。[tool-verified: `provisa/api/admin/glossary_router.py:generate_all_definitions`]

响应：

```json
{"generated": 12}
```

`generated` 是获得新定义的术语数量。当每个术语都已有定义时为零。

**`POST /admin/glossary/relationships/generate`**

将完整的术语列表发送给组织的 AI 模型，提示词中指定了十种允许的边类型（`KIND_OF`、`PART_OF`、`SYNONYM_OF`、`RELATED_TO`、`VALID_VALUE_OF`、`DERIVED_FROM`、`REPLACES`、`PREFERRED_TERM_FOR`、`TRANSLATION_OF`、`ANTONYM_OF`），并要求只提供有把握的提议。模型返回一个 JSON 数组；每一项在写入前都会被校验：未知的术语名称、自环边以及封闭枚举之外的边类型都会被静默丢弃。有效的提议会被幂等地更新插入 — 重新运行该操作不会产生重复的边。一次发布通知覆盖整批操作。当术语表中未弃用的术语少于两个时，该端点会立即返回 `{"added": 0}`。[tool-verified: `provisa/api/admin/glossary_router.py:generate_relationships`]

响应：

```json
{"added": 5}
```

`added` 是写入的边数。即使一条边已经存在，也仍会计数 — 更新插入操作会成功，但边数据不会改变。

### MCP `search_terms` 工具

```
search_terms(query, role=None, limit=25)
```

以不区分大小写的子串匹配搜索术语名称和定义，最多返回 `limit` 条结果。每个结果都是完整的术语详情：`name`、`definition`、`is_abstract`、`deprecated`、物理引用（含 `source_id`、`schema_name`、`table_name`、`column_name`）、类型化边和专家分配。[tool-verified: `provisa/api/mcp/server.py:236-244`, `provisa/core/repositories/glossary.py:search_terms`]

在编写 SQL 之前使用 `search_terms`，按名称查找代表某个概念的每一个物理字段。例如，搜索 `"order date"` 会返回该术语以及每张已注册表中所有的 `order_dt`、`orderDate`、`ORDER_DATE` 列。

### 元数据导出

术语表关系图包含在由 `build_snapshot` 构建的每一个 `MetadataSnapshot` 中。[tool-verified: `provisa/api/metadata_export/builder.py:_glossary_assets`]

该导出应用与快照其余部分相同的过滤器：

- 标记为 `export_excluded` 的术语会被彻底扣留 — 无论其物理引用、抽象状态，或组织的目录是否已配置。[tool-verified: `provisa/api/metadata_export/builder.py:_glossary_assets`]
- 一个有根术语只有在其至少一个物理引用属于同时通过**数据产品**过滤器（该表的 `data_product` 标志必须为 `true`）和**技术**列过滤器（标记为 `technical` 的列会被扣留）的列时才会发布。
- 一个所有引用都被这些过滤器扣留的有根术语，会随之一起被扣留。
- 抽象术语无条件发布 — 它们是用户词汇，不绑定到物理列。
- 两个术语之间的边只有在两端术语都发布时才会发布。

每个供应商适配器都会将术语关系图原生发布到一个由 Provisa 拥有、幂等创建的术语表容器中 — 从不发布到已有的目录术语表中：

| 供应商 | 容器 | 术语 | 关系 | 弃用 |
| --- | --- | --- | --- | --- |
| Apache Atlas | “Provisa Glossary”（术语表 API） | 术语表术语，定义写入 `longDescription` | KIND_OF → `isA`，SYNONYM_OF → `synonyms`，RELATED_TO/PART_OF → `seeAlso` | `[DEPRECATED]` shortDescription 标记 |
| Atlan | 以稳定 qualifiedName 标识的 Provisa 术语表 | `longDescription`（从不使用人工编辑的 `userDescription`） | 与 Atlas 相同的映射 | `certificateStatus = DEPRECATED` |
| DataHub | `urn:li:glossaryNode:provisa.<org>` | 每个术语一个 `glossaryTermInfo` 切面 | KIND_OF → Inherits，PART_OF → Contains（反转），RELATED_TO/SYNONYM_OF → 相关术语 | 弃用切面；重命名沿 URN 演进 |
| OpenMetadata | 通过 `/v1/glossaries` 的 Provisa 术语表 | 以 fqn 为键的 PUT，重命名通过存储的 UUID 进行 PATCH 重绑定 | KIND_OF → 原生父级层级，SYNONYM_OF → `synonyms`，其他 → `relatedTerms` | `entityStatus` |
| Collibra | 术语表类型域“Provisa Glossary” | 通过导入 API 创建的业务术语资产 | 原生业务术语关系类型 | 资产状态 |

所有权以绑定关系为准，而非名称：每个已发布术语的供应商 ID 会被捕获进 `catalog_bindings`，位于该术语的 URN（`provisa://<org>/terms/<name>`）之下，Provisa 只有在持有该绑定时（或该条目位于其创建的 Provisa 拥有的容器中）才会修改或删除供应商侧的术语表条目。没有 Provisa 绑定的术语表条目源自外部系统，永远不会被触碰；更新采用读取-合并方式，因此治理者在 Provisa 自有术语上添加的字段得以保留；当某个术语退出快照时不会删除任何内容。治理者的术语到资产分配仍由外部拥有 — 没有适配器会写入术语到资产的分配（Provisa 撰写的分配发布是一个明确的后续项）。在 Collibra 上，特别是在导入 API 的 REPLACE 语义下，安全性依赖于内容边界：负载只提及 Provisa 术语表域内的资产，关系实例也只在 Provisa 术语之间，因此治理者的术语表及其关系永远不会被触及。[tool-verified: `provisa/api/metadata_export/atlan.py`, `provisa/api/metadata_export/datahub.py`, `provisa/api/metadata_export/atlas.py`, `provisa/api/metadata_export/openmetadata.py`]
