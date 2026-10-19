# SSE 订阅 {: #sse-subscriptions }

Provisa 通过 Server-Sent Events（SSE）支持实时推送。客户端可接收变更事件流，无需轮询。（REQ-258）

## 数据源 {: #sources }

订阅以**已注册的数据表**为目标：

| 数据源 | 可用的 `strategy` 值 |
| -------- | ------------------------- |
| 数据表（PostgreSQL） | `native`（LISTEN/NOTIFY）、`poll` |
| 数据表（非 PG RDBMS，带有数据源级别的 `cdc` 区块） | `debezium`、`kafka`、`poll` |
| 数据表（联邦视图 / 任何其他数据源） | 仅 `poll` |

### PostgreSQL 触发器自动安装 {: #postgresql-trigger-auto-installation }

Provisa 在启动时，会自动在所有**已预先批准**的 PostgreSQL 数据表上安装 `AFTER INSERT OR UPDATE OR DELETE` 触发器。（REQ-565）这些触发器会调用 `pg_notify('provisa_{table}', ...)`，使原始 DML（不仅限于 Provisa 的变更操作）也能被订阅捕获。（REQ-565）

如触发器安装失败（例如权限不足——数据库角色必须拥有该数据表的所有权），Provisa 会回退到该数据表的水位线轮询，前提是已配置 `watermark_column`。（REQ-566）系统会记录一条警告。（REQ-566）

### 跨数据源视图订阅 {: #cross-datasource-view-subscriptions }

对于通过联邦引擎合并多个数据源的视图，请在数据表注册时添加 `watermark_column`。（REQ-260、REQ-283）该列必须存在于视图的 SQL 中（无需出现在 GraphQL 架构中）：

```sql
-- Example: federated view with derived watermark
CREATE OR REPLACE VIEW orders_with_segments AS
SELECT o.*, s.name AS segment_name,
       GREATEST(o.updated_at, s.updated_at) AS _watermark
FROM postgresql.public.orders o
JOIN mysql.crm.customer_segments s ON o.customer_id = s.customer_id;
```

使用 `watermark_column: _watermark` 进行注册。Provisa 使用 `WHERE _watermark > <last_seen>` 进行轮询。（REQ-260）

### 嵌套关系订阅 {: #nested-relationship-subscriptions }

当订阅字段选取了已合并数据表（通过已注册的关系）的字段时，Provisa 会同时监视**所有**相关的物理数据表。（REQ-567）任何已合并数据表的变更，都会重新触发该订阅查询。（REQ-567）

## 端点 {: #endpoint }

订阅某数据表：

```http
GET /data/subscribe/{table}
Accept: text/event-stream
```

连接会保持开启，并针对每次变更发送一个 JSON 事件：（REQ-258、REQ-568）

```text
data: {"event":"insert","table":"orders","row":{"id":43,"amount":55.00,"region":"east"}}

data: {"event":"update","table":"orders","row":{"id":42,"amount":199.00,"region":"west"}}
```

## 传送模式 {: #delivery-modes }

传送方式由数据表配置中的 `live.strategy` 选定：（REQ-813、REQ-814）

| `strategy` | 机制 | 适用于 | 需要 |
| ------------ | ----------- | --------------- | --------- |
| `native` | PostgreSQL `LISTEN`/`NOTIFY`、MongoDB Change Streams | PG、MongoDB | 无需额外项 |
| `debezium` | 来自 Debezium 连接器的 Kafka 主题 | 非 PG RDBMS 数据表 | 数据源级别的 `cdc` 区块（Debezium + Kafka） |
| `kafka` | 任意的 Kafka delta 主题 | 任何以 Kafka 供数的数据表 | 数据源级别的 `cdc` 区块 |
| `poll` | 基于水位线的轮询 | 任何具有水位线的数据表 | `watermark_column` |

### LISTEN/NOTIFY

Provisa 会在一条持久的 PG 连接上发出 `LISTEN <channel>`。（REQ-258）Provisa 的变更操作会自动触发 `NOTIFY`。（REQ-565）外部写入方必须在写入后调用 `NOTIFY <channel>, '<payload>'`。无需额外的基础设施。

### 轮询 {: #polling }

Provisa 会定期重新执行数据源查询，只选取 `watermark_column > last_watermark` 的数据行。（REQ-260）差异会以 SSE 事件形式发出。轮询无法检测到硬删除——被移除的数据行不会推进水位线。若要使删除操作可见，请使用软删除（例如设置 `deleted_at` 标志）以推进水位线列；该删除操作随后会以更新事件形式到达，并携带软删除标记。（REQ-260）

数据表轮询配置（在 `provisa.yaml` 中）：

```yaml
tables:
  - id: federated_orders
    source_id: federated-source
    live:
      strategy: poll
      watermark_column: updated_at
      poll_interval: 30
      outputs:
        - type: sse
```

### Debezium CDC

需要一个正在运行、并将数据写入 Kafka 的 Debezium 连接器。（REQ-261）Provisa 会消费该 Kafka 主题，并将变更事件转发给已连接的 SSE 客户端。（REQ-261）

CDC 传输每个数据源只需在 `cdc` 区块中配置一次；主题按 `{topic_prefix}.{schema}.{table}` 的格式派生，不会按数据表重复配置。（REQ-824）之后每个数据表只需选取 `strategy: debezium`：

```yaml
sources:
  - id: sales-mysql
    cdc:
      bootstrap_servers: kafka:9092
      topic_prefix: debezium
      # schema_registry_url: http://schema-registry:8081   # set for Avro; omit for JSON
    tables:
      - id: orders
        live:
          strategy: debezium
```

#### 消息编码 {: #message-encodings }

Debezium 主题以两种编码之一读取，每条消息都会说明是哪一种。（REQ-1951）

| 编码 | 主题携带的内容 | 数据源需要的内容 |
|---|---|---|
| JSON | Debezium JSON 转换器的信封，带或不带 `schema`/`payload` 包装 | `cdc` 块之外无需任何内容 |
| Avro | Confluent schema registry 的线上格式：一个零字节、四字节的 schema id、一个 Avro 数据项 | `schema_registry_url` 以及向它进行身份验证的方式 |

Avro 消息用其 id 所指的 schema 解码，该 schema 从 registry 获取一次并保存。schema 已经演进的主题也能正确读取，因为每条消息都指明了写入它所用的 schema。Protobuf 和 JSON Schema 主题不会被读取。

Avro 消息绝不会被当作 JSON 读取。无法读取它的每一种原因都会被点名拒绝：

| 情况 | 拒绝 |
|---|---|
| Avro 消息，数据源未指定 registry | `subscribe.avro_topic_without_registry`，附主题 |
| registry 无法访问或未及时应答 | `subscribe.schema_registry_unreachable` |
| registry 拒绝数据源的凭据 | `subscribe.schema_registry_refused_credentials` |
| registry 中不存在的 schema id | `subscribe.schema_id_unknown`，附 id |
| 该 id 指向 Protobuf 或 JSON schema | `subscribe.schema_not_avro` |

无法访问或拒绝凭据的 registry 会在订阅开始时、流打开之前被拒绝。对 registry 的请求最多等待 5 秒建立连接、10 秒等待应答；若请求剩余时间更少，则等待更短。

#### Schema registry 身份验证 {: #schema-registry-authentication }

`schema_registry_auth` 指定方法；该方法所需的字段在保存数据源时检查。

| `schema_registry_auth` | 必需字段 | 发送给 registry 的内容 |
|---|---|---|
| `none`（默认） | — | 无凭据 |
| `basic` | `schema_registry_username`、`schema_registry_password` | HTTP 基本身份验证 |
| `bearer` | `schema_registry_token` | `Authorization: Bearer <token>`（静态令牌） |
| `mtls` | `schema_registry_client_cert`、`schema_registry_client_key` | 客户端证书，在 TLS 握手中 |

`schema_registry_ca` 在任何方法下都是可选的：当 registry 的证书不是由公共机构签发时所信任的 CA 包。

密码和令牌遵循密钥契约。在数据源表单中输入的值保存在组织的密钥库中，数据源保存一个 `${secret:NAME}` 引用；在配置文件中写入的引用（`${env:...}`、`${secret:...}`）按原样保存。证书、密钥和 CA 包是 Provisa 服务器进程打开的文件，以绝对路径给出。

```yaml
sources:
  - id: sales-mysql
    cdc:
      bootstrap_servers: kafka:9092
      topic_prefix: debezium
      schema_registry_url: https://schema-registry:8081
      schema_registry_auth: basic
      schema_registry_username: provisa
      schema_registry_password: ${secret:registry_password}
      schema_registry_ca: /etc/provisa/registry-ca.pem
```
## 流失败时 {: #when-a-stream-ends-on-a-failure }

已经打开的流已经应答了 `200`，因此之后的失败不能再是 HTTP 状态。订阅者会收到说明原因的最后一帧，然后流关闭。流绝不会在没有这一帧的情况下因失败而关闭。

该帧携带的内容与普通请求对同一失败的应答相同：

| 失败 | 正文 |
|---|---|
| 带代码的拒绝（schema registry、区域、治理拒绝） | `detail`、`code`、`params` |
| 请求的截止时间 | `detail: "Request timed out"` |
| 任何意外情况 | `detail: "Internal server error"`、`type`（异常类型名称） |

每个正文还携带 `subscription_id`。发生意外失败时，消息和 traceback 位于服务器日志中该 id 之下，而不在帧中。

表订阅（`/data/subscribe/{table}`）将正文作为名为 `stream_error` 的事件数据发送：

```text
event: stream_error
data: {"detail": "Topic 'debezium.app.orders' carries Avro messages, and its source names no schema registry to read them with: set the source's schema registry URL", "code": "subscribe.avro_topic_without_registry", "params": {"topic": "debezium.app.orders"}, "subscription_id": "5f0c2a9d41b7e386"}
```
该事件不叫 `error`，因为 `EventSource` 在连接中断时会自行产生一个 `error` 事件，监听器将无法区分两者。请按名称监听它：

```javascript
source.addEventListener("stream_error", (e) => {
  const { detail, code, params } = JSON.parse(e.data);
  source.close();
});
```
GraphQL 订阅保持 GraphQL 应答的形式，代码和 params 位于 `extensions` 中：

```json
{"errors": [{"message": "…", "extensions": {"code": "subscribe.avro_topic_without_registry", "params": {"topic": "debezium.app.orders"}, "subscription_id": "5f0c2a9d41b7e386"}}]}
```
有一种情况不会结束 GraphQL 订阅：当某次变更之后的重新读取失败时，会以同样的形式发送该帧，然后流继续处理下一次变更。初始读取、变更流本身或其启动的失败是最后一帧。

## Kafka 接收器重定向 {: #kafka-sink-redirect }

任何 GraphQL 订阅都可以重定向到 Kafka 主题，而不是以流的形式返回给客户端。（REQ-812）请在订阅请求中添加 `X-Provisa-Sink` 请求头：

```yaml
POST /data/graphql
Authorization: Bearer <token>
Content-Type: application/json
X-Provisa-Sink: kafka://broker:9092/my-topic
```

服务器会立即响应 `202 Accepted`，并启动一个后台任务，该任务会：（REQ-812）

1. 使用与 SSE 相同的提供者解析方式（LISTEN/NOTIFY → asyncpg 轮询 → 联邦轮询）监视数据表变更
2. 在每次变更时重新执行对应的查询
3. 将结果以 JSON 消息形式发布到指定的 Kafka 主题

该接收器会在服务器进程的整个生命周期内运行。（REQ-812）如需停止，请重启服务器。

**URI 格式：** `kafka://[broker:port]/topic`

- 如省略 `broker:port`，则会使用 `KAFKA_BOOTSTRAP_SERVERS` 环境变量（默认值：`localhost:9092`）（REQ-812）
- `topic` 为必填项

**示例（curl）：**

```bash
curl -X POST http://localhost:8000/data/graphql \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -H "X-Provisa-Sink: kafka://kafka:9092/orders-live" \
  -d '{"query": "subscription { orders { id status amount } }"}'
# → 202 {"status":"streaming","sink":"kafka://kafka:9092/orders-live","table":"orders"}
```

### 作为配置级第二输出的 Kafka 接收器 {: #kafka-sink-as-a-config-level-second-output }

基于轮询的数据表订阅，可通过 `provisa.yaml` 同时发布到 Kafka 主题。（REQ-282、REQ-286）SSE 订阅与 Kafka 接收器都是同一个 Live Query Engine 的输出。（REQ-282）每个输出都会独立追踪其水位线。（REQ-286）

```yaml
tables:
  - id: active-orders
    live:
      strategy: poll
      watermark_column: updated_at
      poll_interval: 30
      outputs:
        - type: sse
        - type: kafka
          topic: provisa.active-orders
          bootstrap_servers: kafka:9092
          key_column: id
          role: analyst           # required: the rows are published governed as this role
```

Kafka 输出以它所指定的角色治理后发布表的行：该角色的行规则、列可见性和脱敏，与该角色的读取一样适用。没有 `role` 的输出会在加载配置时被拒绝。（REQ-565） [tool-verified: `provisa/core/models.py` `LiveOutputConfig`]

## 安全性 {: #security }

所有订阅模式都采用与常规查询相同的安全管道：（REQ-258、REQ-038）

- 行级安全过滤器会应用于每一行已发出的数据（REQ-040）
- 已脱敏的列在事件中会显示为已脱敏（REQ-040）
- 角色授权会在连接时进行校验（REQ-258）

## 客户端示例 {: #client-example }

```javascript
// Table subscription (LISTEN/NOTIFY)
const source = new EventSource('/data/subscribe/orders', {
  headers: { 'Authorization': 'Bearer <token>' }
});

source.onmessage = (e) => {
  const event = JSON.parse(e.data);
  console.log(event.event, event.row);
};
```

</content>
