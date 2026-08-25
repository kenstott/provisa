# Provisa pgwire 服务器

Provisa 提供一个 PostgreSQL 线协议（pgwire）端点。任何支持 PostgreSQL 客户端协议的工具——psycopg2、asyncpg、DBeaver、Tableau、JDBC——都可以连接并通过与 HTTP API 相同的治理管道查询 Provisa 数据。（REQ-266）

查询会经过完整的治理堆栈：行级安全执行、脱敏规则、关系防护、域访问检查。（REQ-001、REQ-002、REQ-263）pgwire 接口不是绕过机制。（REQ-002、REQ-266）

---

## 连接详情

当 `PROVISA_PGWIRE_PORT` 设置为非零整数时，服务器即启动。默认处于禁用状态。（REQ-527）[tool-verified: `app.py:1739`]

```yaml
Host: 0.0.0.0  (all interfaces)
Port: $PROVISA_PGWIRE_PORT
```

**TLS。** 将 `PROVISA_PGWIRE_CERT` 和 `PROVISA_PGWIRE_KEY` 设置为 PEM 证书和密钥的路径。两者都存在时，服务器会将传入连接封装在 TLS 中。两者都缺失时，TLS 关闭，服务器会对 SSL 协商请求回复 `N`。（REQ-530）[tool-verified: `server.py:1746-1750`]

**报告的服务器版本。** 客户端看到的版本为 `14.0.provisa`。根据版本号启用功能的工具，其行为可能如同连接到 PostgreSQL 14。（REQ-579）[tool-verified: `server.py:208`]

---

## 身份验证

启动包携带一个用户名和一个密文字段，但没有任何方案说明该密文是什么。Provisa 根据密文本身来判断，因此客户端除 `user` 和 `password` 之外无需任何额外配置：

| 密文是 | 识别依据 | 解析为 |
| --------------- | --------------- | ------------- |
| 个人访问令牌 | 其 `provisa_pat_` 前缀 | 该令牌的所有者及其角色（REQ-1263） |
| OIDC / 提供程序颁发的 bearer 令牌 | 所配置的提供程序是令牌提供程序 | 该令牌所声明的身份（REQ-890） |
| 密码 | 其他任何情况 | 所配置提供程序（`basic` 或 `simple`）中的账户 |

判断只做一次。被选定验证器拒绝的凭据不会再交给另一个验证器重试，因此一次拒绝不会变成第二次猜测。

trust 模式（`provider: none`，或身份验证中间件未启用）是例外：用户名会直接用作 `role_id`，密文会被忽略。请勿在未加密的连接上使用它。

**SCRAM-SHA-256。** 在 `provider: basic` 且 `auth.scram: true` 时，服务器会公布带 `SCRAM-SHA-256` 的 SASL（身份验证代码 10），密码通过证明而非发送来验证。（REQ-1394）不提供 `SCRAM-SHA-256-PLUS`。对于尚未写入验证器的用户——验证器无法从 bcrypt 哈希推导——服务器会以模拟交换作答，使链路上无法看出谁已迁移；该用户会通过 TLS 上的明文密码进行身份验证，直到下次输入密码时写入验证器为止。关闭 `auth.scram` 时，服务器使用 PG 身份验证类型 3（明文密码）。两种情况下都不支持 MD5。

**客户端证书。** 设置 `PROVISA_MTLS_CLIENT_CA` 后，服务器会在握手期间、检查任何凭据之前验证客户端证书。（REQ-1228）启用 `PROVISA_MTLS_BIND_PRINCIPAL` 时，证书的 common name 必须与该连接随后用于身份验证的 `user` 相同。参见[配置](configuration.md#tls)。

**失败尝试会被计数。** 五分钟内五次失败会将账户锁定十五分钟，且该计数器与 HTTP 和 Bolt 共享——在任一接口上招致的锁定，在所有接口上都生效。（REQ-1393）

**选择组织。** 在多组织部署中，连接到 `<org>.<您的域名>`，pgwire 会从 TLS ClientHello 的主机名中读取组织，方式与 HTTP 从 `Host` 头中读取相同。（REQ-1234）主机名是在请求某个组织，而不是授予它；在该组织中没有成员资格的主体会被拒绝。通过 IP 地址连接则不请求任何组织。

---

## 支持的功能

### SELECT

所有 SELECT 语句都会经过治理管道（`_pipeline.py`）。（REQ-001、REQ-262、REQ-266）该管道会：

1. 将语义 SQL 重写为物理 SQL（`rewrite_semantic_to_physical`）
2. 应用治理（行级安全、脱敏、域访问）（REQ-263）
3. 对照已注册的架构 (Schema) 进行验证（REQ-011）
4. 路由至 Trino 或直接路由至数据源池（REQ-027、REQ-028）

支持多语句的简单查询。以分号分隔的语句会被拆分并按顺序执行。（REQ-580）[tool-verified: `server.py:318-381`]

在简单查询模式和扩展查询（Bind/Execute）模式下均支持参数化查询（`$1`、`$2`……）。参数会在执行前以字面量替换。（REQ-581）[tool-verified: `server.py:78-85`]

`SELECT * FROM fn(args)` 和 `SELECT fn(args)`——其中 `fn` 指定一个已注册且受跟踪的函数——会在治理管道之前被拦截，并通过唯一受治理的执行器（`invoke_tracked_function`）路由。结果是一个类型化的行集，与该命令在其他任何界面所返回的结果一致。`writable_by` 及治理规则会在执行器内部强制执行。（REQ-1156）[tool-verified: `provisa/pgwire/function_call.py:74-88`]

### DDL

DDL 语句由 `server.py` 中的正则表达式检测，并派发至 `DdlHandler`。角色必须具备 `"ddl"` 权限。（REQ-042）若无此权限，该语句会以 SQLSTATE 42501 被拒绝。[tool-verified: `ddl_handler.py:82-83`]

可识别的 DDL 形式为：

```sql
CREATE TABLE / VIEW / INDEX / UNIQUE INDEX / SEQUENCE / SCHEMA
ALTER TABLE / INDEX / SEQUENCE / VIEW
DROP TABLE / VIEW / INDEX / SEQUENCE / SCHEMA
```

[tool-verified: `server.py:56-61`]

根据 `ddl_catalog` 存在两条执行路径：（REQ-582）

**Trino 路径**——当 `ddl_catalog` 为 Iceberg、Hive 或其他未注册的 Trino 目录（例如 `iceberg`、`hive`、`otel`、`results`）时使用。此路径仅支持 `CREATE TABLE` 和 `CREATE VIEW`。尝试执行 `ALTER`、`DROP` 或 `CREATE INDEX` 会引发错误。表名会完全限定为 `catalog.schema.table`。[tool-verified: `ddl_handler.py:92-100`]

**直接路径**——当 `ddl_catalog` 对应一个已注册的数据源 ID 时使用。支持完整 DDL：CREATE、ALTER、DROP、索引、序列。`CREATE TABLE` 和 `CREATE VIEW` 会以架构限定为 `schema.table`。其余 DDL（ALTER、DROP、CREATE INDEX）在设置架构上下文后会原样传递。对于 PostgreSQL 和 SQLite 数据源，上下文以 `SET search_path TO schema` 设置。对于 MySQL 和 MariaDB，则以 `USE schema` 设置。[tool-verified: `ddl_handler.py:139-170`, `ddl_handler.py:207-213`]

在任一路径执行 DDL 后，新表会注册到该角色的编译上下文中，以便立即可供查询。（REQ-583）[tool-verified: `ddl_handler.py:216-250`]

**写入目标的解析。** DDL 目录及架构来自该域的 `ddl_catalog` 和 `ddl_schema` 字段。若未设置 `ddl_catalog`，系统默认使用 Iceberg 目录。若未设置 `ddl_schema`，则默认使用域 ID。域会通过角色的 `domain_access` 列表解析。（REQ-584）[tool-verified: `app.py:804-811`, `ddl_handler.py:104-115`]

### COPY

`COPY ... TO STDOUT` 和 `COPY ... FROM STDIN` 均受支持。（REQ-585）[tool-verified: `copy_handler.py:231-257`]

**COPY TO STDOUT**——以 PG COPY 线格式导出查询结果。有两种形式可用：

```sql
-- Table reference
COPY my_table TO STDOUT WITH (FORMAT csv)

-- Arbitrary query
COPY (SELECT col1, col2 FROM my_table WHERE ...) TO STDOUT WITH (FORMAT text)
```

支持的格式：`text`（以制表符分隔，默认）和 `csv`。COPY 输出不支持二进制格式。[tool-verified: `copy_handler.py:36-52`]

**COPY FROM STDIN**——将行插入目标表。仅限类型为 `postgresql`、`mysql`、`sqlite` 或 `mariadb` 的数据源使用。（REQ-586）尝试对仅限 Trino 的数据源（例如 Iceberg）执行 COPY FROM 会引发权限错误。[tool-verified: `copy_handler.py:65`, `copy_handler.py:351-356`]

```sql
COPY my_table (col1, col2) FROM STDIN WITH (FORMAT text)
```

若未提供列列表，列会从已注册的架构推断得出。[tool-verified: `copy_handler.py:357`]

### 事务与会话命令

SET、BEGIN、COMMIT、ROLLBACK、SAVEPOINT、RELEASE、DISCARD、RESET 和 DEALLOCATE 会被拦截并返回空的成功响应。（REQ-587）服务器就事务而言是无状态的——不存在事务隔离或回滚支持。（REQ-587）[tool-verified: `catalog.py:27-31`, `catalog.py:1129-1132`]

---

## 目录拦截

对 `information_schema` 和 `pg_catalog` 的查询会在本地解答，无需往返 Trino。（REQ-532）拦截层会按每个请求构建一个内存中的 DuckDB 数据库，并以该角色的编译上下文填充数据。（REQ-532）[tool-verified: `catalog.py:210-213`]

被拦截的表：

**information_schema：** `schemata`、`tables`、`columns`、`views`、`table_constraints`、`key_column_usage`、`referential_constraints`

**pg_catalog：** `pg_namespace`、`pg_class`、`pg_attribute`、`pg_type`、`pg_attrdef`、`pg_description`、`pg_index`、`pg_constraint`、`pg_proc`、`pg_roles`、`pg_auth_members`、`pg_database`、`pg_settings`、`pg_tables`、`pg_stat_user_tables`、`pg_statio_user_tables`、`pg_am`、`pg_extension`、`pg_enum`、`pg_stat_activity`

[tool-verified: `catalog.py:39-67`]

`pg_constraint` 会以从域模型的 `pk_columns` 和 `joins` 字段派生的真实主键及外键数据填充。（REQ-392、REQ-399）检查外键关系的 BI 工具（Tableau、DBeaver 等）将看到 Provisa 所了解的连接图。[tool-verified: `catalog.py:551-632`] 同一数据源/目标对之间的单列连接，若其目标列共同构成该目标的复合主键，会被合并为单行 FK 记录，并以多元素的 `conkey`/`confkey` 数组表示。（REQ-1094）[tool-verified: `catalog_constraints.py`]

由联结表支撑的关系（REQ-1586）不会产生 FK 行。它是穿过关联表的一条边，而不是一对列，而 `pg_constraint` 没有可容纳两跳的形态——因此域模型不会把它放进 `joins`，联结表则表现为一张普通表，各自持有指向两端的外键。SQL 客户端通过联接该表来访问它；Cypher 客户端则把它当作单条关系遍历。[tool-verified: `provisa/compiler/schema_gen.py:302-306`]

`pg_index` 会就每个主键及 UNIQUE 约束填充一行（`indrelid` = 表 oid，`indkey` = 已排序的键值 attnum，`indisprimary`/`indisunique` 已设置）。通过 `pg_index.indkey` 而非 `pg_constraint` 解析键列的客户端——例如 DataGrip——会通过标准的 `pg_index` → `pg_attribute` 连接找出正确的列。（REQ-1095）[tool-verified: `catalog_constraints.py:340-384`]

以下标量表达式也会被拦截：（REQ-588）

- `current_user`、`session_user` → 已验证的 `role_id`
- `current_database()` → `"provisa"`
- `current_schema()` → `"public"`
- `version()` → `"PostgreSQL 14.0 on Provisa"`
- `pg_backend_pid()` → `0`
- `current_setting(...)` → 从固定的设置表返回值
- `SHOW <setting>` → 从同一设置表返回值

[tool-verified: `catalog.py:168-207`, `catalog.py:1076-1120`]

---

## 二进制参数编码

扩展查询协议（Bind/Execute）支持以二进制编码的参数。（REQ-589）以下类型 OID 会从二进制解码：[tool-verified: `postgres.py:69-97`]

| OID | PG 类型 | Python 类型 |
| ----- | --------- | ------------- |
| 16 | bool | bool |
| 17 | bytea | bytes |
| 20 | int8 | int |
| 21 | int2 | int |
| 23 | int4 | int |
| 25 | text | str |
| 700 | float4 | float |
| 701 | float8 | float |
| 1043 | varchar | str |
| 1082 | date | datetime.date |
| 1114 | timestamp | datetime.datetime |
| 1184 | timestamptz | datetime.datetime (UTC) |
| 1700 | numeric | decimal.Decimal |
| 2950 | uuid | str |

不在此表中的任何 OID 都会引发 `"Unsupported binary parameter type: <oid>"`。（REQ-589）[tool-verified: `postgres.py:579`]

当客户端请求时，结果列也会以二进制方式发送，适用于相同类型集加上 ARRAY、JSON、INTERVAL 和 BIGINT。（REQ-589）[tool-verified: `postgres.py:191-244`]

---

## 驱动程序建议

**原生 Python 驱动程序（psycopg2、asyncpg）。** 这些驱动程序默认会协商扩展查询协议，并对大多数类型使用二进制编码。类型保真度在此最高——`NUMERIC` 列以 `Decimal` 形式到达，`TIMESTAMP` 则以 `datetime` 形式到达，依此类推。适用于基于 Python 的 ETL、脚本或直接集成。

**JDBC（PostgreSQL JDBC 驱动程序）。** 适用于 Java 生态系统工具：DBeaver、Tableau、Power BI、Metabase、Airflow 的 JDBC 算子。JDBC 默认使用简单查询协议，可避免二进制编码带来的复杂情况。连接字符串：

```yaml
jdbc:postgresql://<host>:<PROVISA_PGWIRE_PORT>/provisa?user=<role_id>&password=<password>
```

部分基于 JDBC 的 BI 工具在连接时会发送一连串针对 `information_schema` 和 `pg_catalog` 的查询，以填充其架构浏览器。这些查询全部由目录拦截层解答——在架构检查期间不会产生任何 Trino 流量。（REQ-532）

**何时优先选用哪一种。** 若客户端为 Python，请使用 psycopg2 或 asyncpg 以获得更好的类型处理。若客户端为 BI 工具或任何 JVM 应用程序，请使用 JDBC。若观察到类型转换方面的异常情况，应避免在同一连接中混用二进制和文本协议的预期行为——JDBC 的文本模式行为更易于推断。

---

## 注意事项与限制

**仅限 SQL；不支持 DML 变更操作。** pgwire 监听器仅解析并执行 SQL——不接受 GraphQL 和 Cypher 字符串。（REQ-614）纯粹的 `INSERT`、`UPDATE` 和 `DELETE` 不会路由至写入路径。（REQ-615）请通过 `COPY FROM STDIN`（可写入的数据源）或 `CREATE TABLE AS` 写入数据；行级变更应改为通过 GraphQL、Cypher 或 Trino 的写入路径处理。

**COPY 和 DDL 需要 `ddl` 权限。** `COPY`（无论方向）和 DDL 均受角色的 `ddl` 权限限制；不具备此权限的角色会收到 SQLSTATE 42501。（REQ-616）

**不支持真正的事务功能。** BEGIN/COMMIT/ROLLBACK 会被接受并静默忽略。每条语句均独立执行。（REQ-587）[tool-verified: `server.py:146-158`——`in_transaction()` 始终返回 `False`]

**DDL 超时 60 秒，查询超时 120 秒。** 这些值在处理线程中硬编码。（REQ-590）针对远程数据源的长时间运行 DDL（大表上的架构变更）可能会超时。[tool-verified: `ddl_handler.py:136`, `server.py:186`]

**COPY FROM 仅适用于可写入的数据源。** Iceberg、Hive、仅限 Trino 的数据源，以及只读的数据源类型均不接受 COPY FROM。错误代码为 SQLSTATE 42501。（REQ-586）[tool-verified: `copy_handler.py:65`]

**COPY 输出格式为 text 或 csv。** 尚未实现 PG 二进制 COPY 格式（`FORMAT binary`）。[inferred：`_rows_to_copy_text` / `_rows_to_copy_csv` 中仅存在 `text` 和 `csv` 分支]

**Trino 路径上的 DDL 仅限 CREATE。** 不支持针对 Iceberg 或 Hive 目录执行 ALTER、DROP 和 CREATE INDEX。如需完整 DDL，请以已注册的 SQL 数据源作为 `ddl_catalog`。（REQ-582）[tool-verified: `ddl_handler.py:92-100`]

**参数替换为字面量形式。** `$1`、`$2`……等参数会在执行前以 SQL 字面量替换，而非以绑定参数形式发送至底层引擎。这意味着底层引擎永远不会看到已准备的语句。对 Trino 而言这没有实际影响；对于直接连接池的数据源，则会绕过预准备语句的缓存机制。（REQ-581）[tool-verified: `server.py:78-85`]

**`pg_stat_activity`、`pg_stat_user_tables`、`pg_extension`、`pg_enum`、`pg_attrdef`、`pg_proc`。** 这些表存在于目录层中，但属于空的桩 (stub)。查询它们的监控工具将收到零行结果，而非错误。（REQ-532）[tool-verified: `catalog.py:519-535`, `catalog.py:639-934`]（`pg_index` 已有数据填充——参见"目录拦截"一节。）
