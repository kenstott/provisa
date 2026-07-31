# SSE 订阅

Provisa 通过 Server-Sent Events（SSE）支持实时推送。客户端可接收变更事件流，无需轮询。（REQ-258）

## 数据源

订阅以**已注册的数据表**为目标：

| 数据源 | 可用的 `strategy` 值 |
|--------|-------------------------|
| 数据表（PostgreSQL） | `native`（LISTEN/NOTIFY）、`poll` |
| 数据表（非 PG RDBMS，带有数据源级别的 `cdc` 区块） | `debezium`、`kafka`、`poll` |
| 数据表（联邦视图 / 任何其他数据源） | 仅 `poll` |

### PostgreSQL 触发器自动安装

Provisa 在启动时，会自动在所有**已预先批准**的 PostgreSQL 数据表上安装 `AFTER INSERT OR UPDATE OR DELETE` 触发器。（REQ-565）这些触发器会调用 `pg_notify('provisa_{table}', ...)`，使原始 DML（不仅限于 Provisa 的变更操作）也能被订阅捕获。（REQ-565）

如触发器安装失败（例如权限不足——数据库角色必须拥有该数据表的所有权），Provisa 会回退到该数据表的水位线轮询，前提是已配置 `watermark_column`。（REQ-566）系统会记录一条警告。（REQ-566）

### 跨数据源视图订阅

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

### 嵌套关系订阅

当订阅字段选取了已合并数据表（通过已注册的关系）的字段时，Provisa 会同时监视**所有**相关的物理数据表。（REQ-567）任何已合并数据表的变更，都会重新触发该订阅查询。（REQ-567）

## 端点

订阅某数据表：
```
GET /data/subscribe/{table}
Accept: text/event-stream
```

连接会保持开启，并针对每次变更发送一个 JSON 事件：（REQ-258、REQ-568）
```
data: {"event":"insert","table":"orders","row":{"id":43,"amount":55.00,"region":"east"}}

data: {"event":"update","table":"orders","row":{"id":42,"amount":199.00,"region":"west"}}
```

## 传送模式

传送方式由数据表配置中的 `live.strategy` 选定：（REQ-813、REQ-814）

| `strategy` | 机制 | 适用于 | 需要 |
|------------|-----------|---------------|---------|
| `native` | PostgreSQL `LISTEN`/`NOTIFY`、MongoDB Change Streams | PG、MongoDB | 无需额外项 |
| `debezium` | 来自 Debezium 连接器的 Kafka 主题 | 非 PG RDBMS 数据表 | 数据源级别的 `cdc` 区块（Debezium + Kafka） |
| `kafka` | 任意的 Kafka delta 主题 | 任何以 Kafka 供数的数据表 | 数据源级别的 `cdc` 区块 |
| `poll` | 基于水位线的轮询 | 任何具有水位线的数据表 | `watermark_column` |

### LISTEN/NOTIFY

Provisa 会在一条持久的 PG 连接上发出 `LISTEN <channel>`。（REQ-258）Provisa 的变更操作会自动触发 `NOTIFY`。（REQ-565）外部写入方必须在写入后调用 `NOTIFY <channel>, '<payload>'`。无需额外的基础设施。

### 轮询

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

## Kafka 接收器重定向

任何 GraphQL 订阅都可以重定向到 Kafka 主题，而不是以流的形式返回给客户端。（REQ-812）请在订阅请求中添加 `X-Provisa-Sink` 请求头：

```
POST /data/graphql
Authorization: Bearer <token>
Content-Type: application/json
X-Provisa-Sink: kafka://broker:9092/my-topic
```

服务器会立即响应 `202 Accepted`，并启动一个后台任务，该任务会：（REQ-812）
1. 使用与 SSE 相同的提供者解析方式（LISTEN/NOTIFY → asyncpg 轮询 → 联邦轮询）监视数据表变更
2. 在每次变更时重新执行对应的查询
3. 将结果以 JSON 消息形式发布到指定的 Kafka 主题

该接收器会在服务器进程的整个生命周期内运行。（REQ-812）如需停止，请重启服务器（通过管理 API 进行持久化接收器注册的功能正在规划中）。

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

### 作为配置级第二输出的 Kafka 接收器

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
```

完整的接收器配置参考，请参阅 [Kafka Sinks](sources.md)。

## 安全性

所有订阅模式都采用与常规查询相同的安全管道：（REQ-258、REQ-038）

- 行级安全过滤器会应用于每一行已发出的数据（REQ-040）
- 已脱敏的列在事件中会显示为已脱敏（REQ-040）
- 角色授权会在连接时进行校验（REQ-258）

## 客户端示例

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
