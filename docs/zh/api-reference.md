# API 参考

## 概述

Provisa 在两个前缀下暴露 REST 端点：`/data` 用于查询执行和架构自省，`/admin` 用于配置管理。(REQ-043) 大多数数据端点需要一个角色标识符。管理配置操作使用位于 `/admin/graphql` 的 Strawberry GraphQL API。(REQ-164)

---

## 身份验证

当在 `provisa.yaml` 中配置了 `auth.provider` 时，除 `/health` 和 `/setup/status` 之外的所有端点都需要 `Authorization: Bearer <token>` 请求头。(REQ-120) [tool-verified: `provisa/api/app.py`, `provisa/auth/wiring.py`]

未配置身份验证时，服务器以开发模式运行。任何请求都被视为 `anonymous` 身份，该身份映射到所有已配置角色并具有通配符域访问权限。(REQ-535)

当配置了 `provider: basic` 时，由当前生效的身份验证提供方提供**登录（`POST /auth/login`）**。(REQ-124) 凭据格式和响应内容取决于所用提供方。

**身份自省：**

```http
GET /auth/me
```

返回已认证用户的 id、邮箱、显示名称、组织成员关系和角色分配。在开发模式下返回 `dev_mode: true` 并列出所有角色 ID。[tool-verified: `provisa/api/auth_router.py`]

```http
GET /auth/provider-type
```

返回 `{"provider": "<name>"}`；当身份验证未配置时返回 `{"provider": null}`。[tool-verified: `provisa/api/auth_router.py`]

---

## 数据端点

### `POST /data/graphql`

执行一条 GraphQL 查询或变更操作。(REQ-043) [tool-verified: `provisa/api/data/endpoint.py:151`]

**请求体：**

```json
{
  "query": "{ orders(where: {region: {eq: \"us\"}}) { id amount } }",
  "variables": {},
  "role": "admin",
  "extensions": {}
}
```

`role` 字段仅在开发模式（无身份验证）下使用。启用身份验证后，使用已认证用户的角色，请求体中的 `role` 会被忽略。

`extensions` 字段支持自动持久化查询（APQ）协议：(REQ-288)

```json
{
  "extensions": {"persistedQuery": {"sha256Hash": "<sha256-of-query>"}}
}
```

**请求头：**

- `X-Provisa-Role` — 覆盖角色（开发模式）
- `Accept` — 响应格式（参见内容协商）
- `Authorization` — 启用身份验证时使用 `Bearer <token>`
- `X-Provisa-Redirect-Format` — S3 重定向输出的 MIME 类型（REQ-137）
- `X-Provisa-Redirect-Threshold` — 触发重定向的行数阈值（REQ-137）
- `X-Provisa-Redirect` — `true` 表示无条件强制重定向（REQ-029）

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

**响应（多根字段，内联/重定向混合）：**

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

多根字段查询会独立执行每个根字段。低于重定向阈值的字段以内联方式返回；高于阈值的字段则重定向。`redirects` 键（复数）将字段名映射到重定向信息。(REQ-029) [tool-verified: `provisa/api/data/endpoint.py`]

**缓存响应头：**

- `X-Provisa-Cache: HIT|MISS`（REQ-536）
- `X-Provisa-Cache-Age: <seconds>`（命中时）（REQ-536）

**所需能力：** 所有请求（包括自省）均需要 `QUERY_DEVELOPMENT`。[tool-verified: `provisa/api/data/endpoint.py:186-283`]

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

| 重定向格式 | 写入方 | 内存占用 |
| --- | --- | --- |
| `application/vnd.apache.parquet` | 联邦 CTAS | 无——数据从不经过 Provisa |
| `application/x-orc` | 联邦 CTAS | 无——数据从不经过 Provisa |
| `application/json` | Provisa | 受内存限制 |
| `application/x-ndjson` | Provisa | 受内存限制 |
| `text/csv` | Provisa | 受内存限制 |
| `application/vnd.apache.arrow.stream` | Provisa | 受内存限制 |

对于大型分析导出，请使用 Parquet 或 ORC 重定向。联邦查询引擎会并行直接写入 S3——数据不经过 Provisa。(REQ-138)

```yaml
X-Provisa-Redirect-Format: application/vnd.apache.parquet
X-Provisa-Redirect-Threshold: 1000
```

---

### `POST /data/sql`

通过 Stage 2 治理流水线执行原始 SQL。(REQ-267) [tool-verified: `provisa/api/data/endpoint_dev.py:62`]

**请求体：**

```json
{
  "sql": "SELECT id, amount FROM orders WHERE region = 'us'",
  "role": "admin"
}
```

**所需能力：** `QUERY_DEVELOPMENT`。

`POST /data/sql` 上的治理违规会返回 HTTP 403。(REQ-002, REQ-266)

**响应：** 与 `/data/graphql` 格式相同（默认 JSON 行，通过 `Accept` 进行内容协商）。

---

### `POST /data/query`

统一查询端点。接受 GraphQL、SQL 或 Cypher——语法会被自动检测。(REQ-267) [tool-verified: `provisa/api/data/endpoint_dev.py:509`]

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

### `GET /data/rest/{domain_id}/{table_name}`

为每个已注册表自动生成的纯 REST 端点。查询字符串会映射为 GraphQL 参数，请求通过与 GraphQL 相同的流水线（RLS、脱敏、路由）编译和执行。(REQ-256) [tool-verified: `provisa/api/rest/generator.py:153`]

**查询参数：**

- `limit` — 最大行数（≥ 1）
- `offset` — 跳过的行数（≥ 0）
- `fields` — 逗号分隔的列名（默认为所有标量字段）
- `filter` — `{"field", "comparator", "value"}` 过滤对象组成的 JSON 数组
- `orderBy` — `{"field", "direction"}` 排序对象组成的 JSON 数组

需要已认证的角色；未认证请求返回 `401`。这些路由的 OpenAPI 规范服务于 `GET /data/rest/openapi.json`，Swagger UI 位于 `GET /data/rest/docs`。

---

### `GET /data/jsonapi/{domain_id}/{table_name}`

为每个已注册表自动生成的符合 [JSON:API](https://jsonapi.org) 规范的端点。与 GraphQL 使用相同的 RLS、脱敏和路由。(REQ-257) [tool-verified: `provisa/api/jsonapi/generator.py:284`]

**`Accept` 请求头：** 必须包含 `application/vnd.api+json`（JSON:API 媒体类型），否则请求返回 `406`。

**查询参数：**

- `fields[<type>]` — 稀疏字段集，例如 `?fields[orders]=amount`
- `filter[<col>]` / `filter[<col>][<op>]` — 例如 `?filter[region]=US`、`?filter[amount][gt]=100`
- `sort` — 逗号分隔，`-` 前缀表示降序，例如 `?sort=-created_at,amount`
- `page[number]` / `page[size]` — 分页

响应是带有 `type`/`id`/`attributes` 的资源对象。错误遵循 JSON:API 的错误对象格式。

---

### `POST /query/nl`

提交一个自然语言问题。服务会启动一个异步作业，并立即返回带有 `job_id` 的 `202 Accepted`。需要在 `ai_models` 配置节下配置一个 LLM 提供方。(REQ-354) [tool-verified: `provisa/api/rest/nl_router.py:50`]

**请求体：**

```json
{"q": "How many orders were placed last month?", "role": "admin"}
```

返回 `{"job_id": "<id>"}`。超过按角色计算的自然语言查询速率限制时，返回带 `Retry-After` 响应头的 `429`。(REQ-370)

**获取结果：**

- `GET /query/nl/{job_id}` — 轮询。返回作业文档。
- `GET /query/nl/{job_id}/stream` — SSE。每个生成目标完成时发出一个 `branch` 事件，最后发出一个 `done` 事件。(REQ-357, REQ-358)

三个生成回路（Cypher、GraphQL、SQL）并行运行，每个都通过编译器验证并在出错时进行修正。(REQ-355) 提示词的范围被限定在该角色可见的架构内。(REQ-356) 结果文档按目标为每个分支建立键：(REQ-357) [tool-verified: `provisa/nl/job.py:69`]

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

耗尽迭代限制的分支会返回 `query: null`、`result: null` 以及一个 `error` 字符串。每个生成的查询都在消费者的权限下执行，并应用 Stage 2 治理——该服务从不绕过治理。(REQ-359)

---

### `GET /data/sdl`

返回某个角色架构的 GraphQL SDL。(REQ-008) [tool-verified: `provisa/api/data/sdl.py:137`]

**请求头：** `X-Role: <role_id>`（必填）

**查询参数：**

- `domain` — 逗号分隔的域 ID。设置后，响应会被过滤为仅包含指定域及从中可达的表。

**响应：** `text/plain` 格式的 GraphQL SDL。

---

### `GET /data/introspection`

返回 GraphQL 自省 JSON，可选按域过滤。[tool-verified: `provisa/api/data/sdl.py:200`]

**请求头：** `X-Provisa-Role: <role_id>`（必填）

**查询参数：** `domain` — 逗号分隔的域 ID。

**响应：** `application/json` 自省结果。

---

### `GET /data/graph-schema`

返回该角色架构的图视图：节点标签及其关系类型，供 Cypher/图客户端使用。每个节点标签包含 `pk_columns`，以便调用方确定主键列。(REQ-398) [tool-verified: `provisa/api/rest/cypher_router.py:689`]

**响应：** `application/json`，包含 `node_labels`（每项携带 `pk`/`pk_columns`）和 `relationship_types`。

---

### `GET /data/domains`

返回请求角色可访问的域 ID。[tool-verified: `provisa/api/data/sdl.py:116`]

**请求头：** `X-Role: <role_id>`（必填）

**响应：** `["sales", "support", ...]`

---

### `GET /data/schema-version`

返回当前架构版本字符串。将按启动生成的随机数与重建计数器组合而成。客户端可用此值在服务器重启后使架构缓存失效。(REQ-537) [tool-verified: `provisa/api/data/sdl.py:102`]

**响应：** `{"version": "<boot-id>-<counter>"}`

---

### `GET /data/proto/{role_id}`

返回某个角色自动生成的 `.proto` 文件。[tool-verified: `provisa/api/data/endpoint_dev.py:49`]

**响应：** `text/plain` protobuf 架构。

每个已注册表都会生成一个 proto `message`。关系会生成嵌套消息字段。类型映射：`integer → int32`、`bigint → int64`、`varchar → string`、`decimal → double`、`boolean → bool`、`timestamp → google.protobuf.Timestamp`。(REQ-538)

---

### `GET /data/subscribe/{table}`

用于表的实时变更通知的服务器发送事件（SSE）流。(REQ-219, REQ-258) [tool-verified: `provisa/api/data/subscribe.py:239`]

通知投递使用按数据源类型选择的可插拔提供方：PostgreSQL 数据源使用 `LISTEN/NOTIFY`（通过 asyncpg），MongoDB 数据源使用 Change Streams（`collection.watch()`），Kafka 数据源使用消费者组。每个提供方都实现一个通用的异步监听接口。无论使用哪种提供方，RLS 过滤和架构校验都会照常应用。(REQ-258) 同时也支持 WebSocket 和 RSS 数据源。(REQ-338, REQ-342)

**请求头 — `X-Provisa-Sink`：** 设置为一个 Kafka 目标（例如 `kafka://broker:9092/topic`），将变更事件重定向到 Kafka sink，而不是走 SSE 响应。服务器会启动一个 sink 消费者，并返回 `202 Accepted` 而不是打开一条流。(REQ-812) [tool-verified: `provisa/api/data/subscription_sse.py:137`]

---

## Admin REST 端点

### 配置

#### `GET /admin/config`

以 `application/x-yaml` 格式下载当前的 `provisa.yaml`，并带有 `Content-Disposition: attachment` 响应头。(REQ-164) [tool-verified: `provisa/api/admin/settings_router.py:19`]

#### `PUT /admin/config`

上传修订后的配置 YAML。服务器会写入 `.bak` 备份，保存新文件，并重新加载所有架构、数据源和物化视图。(REQ-164) [tool-verified: `provisa/api/admin/settings_router.py:32`]

**请求体：** 原始 YAML 内容。

**响应：**

```json
{"success": true, "message": "Config uploaded and reloaded"}
```

重新加载失败时：`{"success": false, "message": "<error>"}`。

---

### 设置

#### `GET /admin/settings`

以 JSON 形式返回当前平台设置。(REQ-165) [tool-verified: `provisa/api/admin/settings_router.py:50`]

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

在运行时更新平台设置。所有字段均为可选——仅更新请求体中出现的键。(REQ-165) [tool-verified: `provisa/api/admin/settings_router.py:100`]

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

各配置节可更新的字段：

- `redirect`：`enabled`、`threshold`、`default_format`、`ttl`
- `sampling`：`default_sample_size`
- `cache`：`default_ttl`
- `naming`：`domain_prefix`、`convention` — 写入配置文件并触发架构重新加载（REQ-253）
- `relationships`：`auto_track_fk`
- `otel`：`endpoint`、`service_name`、`sample_rate`、`support_endpoint`、`support_redact_sql_literals`、`support_redact_attributes`

**响应：**

```json
{"success": true, "updated": ["otel.support_endpoint", "cache.default_ttl"]}
```

---

### 可观测性

#### `GET /admin/traces/recent`

从内存中的 span 缓冲区返回最多 N 条最近完成的 span。(REQ-302) [tool-verified: `provisa/api/admin/settings_router.py:317`]

**查询参数：** `limit`（默认 50，最大 200）

**响应：** `{"traces": [...]}`

#### `POST /admin/query-engine/reload-catalog`

通过其 REST API 热重载联邦查询引擎协调节点中的一个指定目录。重新连接 Provisa 的内部连接，并重新运行 OTel DDL。[tool-verified: `provisa/api/admin/settings_router.py:208`]

**查询参数：** `catalog`（默认 `"otel"`）

**响应：**

```json
{"success": true, "errors": []}
```

#### `POST /admin/query-engine/restart`

重启联邦查询引擎容器（仅限单节点开发环境）。[tool-verified: `provisa/api/admin/settings_router.py:287`]

**查询参数：** `container`（默认取 `QUERY_ENGINE_CONTAINER` 环境变量，其次为 `"trino"`）

---

### 发现

#### `POST /admin/discover/relationships`

触发关系发现。始终从联邦查询引擎运行外键自省。(REQ-018) 若设置了 `ANTHROPIC_API_KEY`，则运行 LLM 推断。(REQ-167) [tool-verified: `provisa/api/admin/discovery.py:55`]

**请求体：**

```json
{
  "scope": "domain",
  "domain_id": "sales"
}
```

`scope` 必须是 `"table"`、`"domain"`、`"cross-domain"` 之一。对于 `"table"` 范围，`table_id`（整数）为必填。对于 `"domain"` 范围，`domain_id` 为必填。

**响应：** `{"candidates_found": 12, "stored_ids": [1, 2, 3, ...]}`

#### `GET /admin/discover/candidates`

列出待处理的关系候选项。[tool-verified: `provisa/api/admin/discovery.py:96`]

#### `POST /admin/discover/candidates/{candidate_id}/accept`

接受某个候选项并将其注册为关系。[tool-verified: `provisa/api/admin/discovery.py:103`]

**请求体（可选）：** `{"name": "custom-relationship-name"}`

#### `POST /admin/discover/candidates/{candidate_id}/reject`

拒绝某个候选项。[tool-verified: `provisa/api/admin/discovery.py:110`]

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

按名称搜索数据源中可用（尚未注册）的表。[tool-verified: `provisa/api/admin/table_search_router.py:103`]

---

### 表画像分析

#### `POST /admin/tables/{table_id}/profile`

对已注册表运行列画像分析——基数、最小/最大值、空值率。[tool-verified: `provisa/api/admin/table_profile_router.py:28`]

---

### 数据源描述

#### `POST /admin/source-meta/db-description`

为某个数据源的表和列生成 LLM 辅助描述。[tool-verified: `provisa/api/admin/source_meta_router.py:48`]

---

### 命令（函数与 Webhook）

所有端点都在 `/admin/actions` 前缀下。(REQ-205) [tool-verified: `provisa/api/admin/actions_router.py:24`]

每一次调用——无论来自 GraphQL、SQL、Cypher、Bolt、Arrow Flight、MCP 的 `run_sql`，还是 Provisa gRPC——都会通过单一的受治理执行器路由，该执行器统一强制执行 `writable_by` 和治理规则。(REQ-1156) [tool-verified: `provisa/api/data/action_exec.py`] 各协议的具体调用语法参见 [docs/integrations.md](integrations.md#_6)。

#### `GET /admin/actions`

返回所有被跟踪的数据库函数和 Webhook。(REQ-242) [tool-verified: `provisa/api/admin/actions_router.py:104`]

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

每个 Webhook 对象都携带一个 `approved` 布尔值。当数据管家执行其创建请求后，该 Webhook 即被批准（REQ-209）；配置中声明的 Webhook 会被自动批准。未批准的 Webhook 会被注册，但不会在任何界面上暴露。[tool-verified: `provisa/api/admin/actions_router.py:124-131`]

#### `POST /admin/actions/functions`

注册一个被跟踪的函数（命令）。(REQ-205) [tool-verified: `provisa/api/admin/actions_router.py:117`]

**关键字段：**

| 字段 | 是否必填 | 说明 |
| --- | --- | --- |
| `name` | 是 | 唯一的命令名称 |
| `kind` | 是 | `"query"` → GraphQL Query 字段；`"mutation"` → Mutation 字段 |
| `implKind` | 否 | 命令的运行方式——见下表（默认为 `source_procedure`） |
| `binding` | 否 | 特定于 `implKind` 的连接详情（JSON 对象） |
| `returnSchema` | 否 | JSON Schema `{type:"array", items:{type:"object", properties:{...}}}`——使该命令在所有界面上都返回集合 |
| `arguments` | 否 | `[{name, type}]` 参数定义；对 SQL 和 Bolt 调用方而言，位置顺序很重要 |
| `visibleTo` | 否 | 可调用该命令的角色 ID |
| `writableBy` | 否 | 允许将其作为变更操作调用的角色 ID |
| `domainId` | 否 | 用于 GraphQL 放置和访问控制的域 |

**`implKind` 取值：**

| `implKind` | 运行内容 | `binding` 字段 |
| --- | --- | --- |
| `source_procedure` | 已注册数据源上的存储过程（默认） | `sourceId`、`schemaName`、`functionName` |
| `script` | 服务端脚本 | `script` |
| `http` | 出站 HTTP 调用 | `url`、`method` |
| `grpc` | 到外部服务器的出站 gRPC 调用 | `target`、`method` |
| `python` | Provisa 托管的 Python 可调用对象（REQ-885） | `callable`（例如 `"demo.py_functions:random_dataset"`） |

演示命令 `random_python_set`（`implKind: python`）和 `random_grpc_set`（`implKind: grpc`）在实践中展示了带有 `returnSchema` 的集合返回型命令；二者都在 `config/provisa-install.yaml` 中。[tool-verified: `config/provisa-install.yaml:809-856`]

#### `PUT /admin/actions/functions/{name}`

按名称更新一个被跟踪的函数。[tool-verified: `provisa/api/admin/actions_router.py:182`]

#### `DELETE /admin/actions/functions/{name}`

按名称删除一个被跟踪的函数。[tool-verified: `provisa/api/admin/actions_router.py:233`]

#### `POST /admin/actions/webhooks`

注册一个被跟踪的 Webhook。(REQ-209) 注册或更新一个 Webhook 会加入一个数据管家审批请求队列——该 Webhook 只有在数据管家批准后才会在所有界面上生效。配置中声明的 Webhook 会被自动批准。**请求体字段：** `name`、`url`、`method`、`timeoutMs`、`returns`、`inlineReturnType`、`arguments`、`visibleTo`、`domainId`、`description`、`kind`。[tool-verified: `provisa/api/admin/actions_router.py:132`, `provisa/api/admin/actions_router.py:325-331`]

#### `PUT /admin/actions/webhooks/{name}`

按名称更新一个被跟踪的 Webhook。任何编辑都会将审批状态重置为待处理，直到重新获批。[tool-verified: `provisa/api/admin/actions_router.py:306`]

#### `DELETE /admin/actions/webhooks/{name}`

按名称删除一个被跟踪的 Webhook。[tool-verified: `provisa/api/admin/actions_router.py:355`]

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

用于所有管理操作的 Strawberry GraphQL 端点：数据源和表的 CRUD、关系管理、域配置、RLS 规则、缓存控制、命名约定、计划任务管理和查询编译。(REQ-164) [tool-verified: `provisa/api/app.py:2171`]

**关键变更操作：**

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

`POST /data/sql` 上的治理违规会返回带有结构化正文的 HTTP 403：(REQ-002) [tool-verified: `provisa/api/data/endpoint_dev.py:184-190`]

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

查询和目录发现在同一个连接上都可用。完整的治理流水线（RLS、脱敏、采样）会应用到每一次查询。(REQ-130, REQ-143)

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

当 Zaychik Flight SQL 代理可用时（端口 8480），记录批次会端到端流式传输，无需完整物化。(REQ-144) 若 Zaychik 不可用，则回退为通过联邦查询层进行物化。(REQ-146)

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
</content>
