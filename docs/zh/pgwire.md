# Provisa pgwire 服务器 {: #provisa-pgwire-server }

Provisa 提供一个 PostgreSQL 线协议（pgwire）端点。任何支持 PostgreSQL 客户端协议的工具——psycopg2、asyncpg、DBeaver、Tableau、JDBC——都可以连接并通过与 HTTP API 相同的治理管道查询 Provisa 数据。（REQ-266）

查询会经过完整的治理堆栈：行级安全执行、脱敏规则、关系防护、域访问检查。（REQ-001、REQ-002、REQ-263）pgwire 接口不是绕过机制。（REQ-002、REQ-266）

---

## 连接详情 {: #connection-details }

当 `PROVISA_PGWIRE_PORT` 设置为非零整数时，服务器即启动。默认处于禁用状态。（REQ-527）[tool-verified: `app.py:1739`]

```yaml
Host: 0.0.0.0  (all interfaces)
Port: $PROVISA_PGWIRE_PORT
```

**TLS。** 将 `PROVISA_PGWIRE_CERT` 和 `PROVISA_PGWIRE_KEY` 设置为 PEM 证书和密钥的路径。两者都存在时，服务器会将传入连接封装在 TLS 中。两者都缺失时，TLS 关闭，服务器会对 SSL 协商请求回复 `N`。（REQ-530）[tool-verified: `server.py:1746-1750`]

**报告的服务器版本。** 客户端看到的版本为 `14.0.provisa`。根据版本号启用功能的工具，其行为可能如同连接到 PostgreSQL 14。（REQ-579）[tool-verified: `server.py:208`]

---

## 身份验证 {: #authentication }

启动包携带一个用户名和一个密文字段，但没有任何方案说明该密文是什么。Provisa 根据密文本身来判断，因此客户端除 `user` 和 `password` 之外无需任何额外配置：

| 密文是 | 识别依据 | 解析为 |
| --------------- | --------------- | ------------- |
| 个人访问令牌 | 其 `provisa_pat_` 前缀 | 该令牌的所有者及其角色（REQ-1263） |
| OIDC / 提供程序颁发的 bearer 令牌 | 所配置的提供程序是令牌提供程序 | 该令牌所声明的身份（REQ-890） |
| 密码 | 其他任何情况 | 所配置提供程序（`basic` 或 `simple`）中的账户 |

判断只做一次。被选定验证器拒绝的凭据不会再交给另一个验证器重试，因此一次拒绝不会变成第二次猜测。

trust 模式（`provider: none`，或身份验证中间件未启用）是例外：用户名会直接用作 `role_id`，密文会被忽略。请勿在未加密的连接上使用它。

**SCRAM-SHA-256。** 在 `provider: basic` 且 `auth.scram: true` 时，服务器会公布带 `SCRAM-SHA-256` 的 SASL（身份验证代码 10），密码通过证明而非发送来验证。（REQ-1394）不提供 `SCRAM-SHA-256-PLUS`。对于尚未写入验证器的用户——验证器无法从 bcrypt 哈希推导——服务器会以模拟交换作答，使链路上无法看出谁已迁移；该用户会通过 TLS 上的明文密码进行身份验证，直到下次输入密码时写入验证器为止。关闭 `auth.scram` 时，服务器使用 PG 身份验证类型 3（明文密码）。两种情况下都不支持 MD5。

**客户端证书。** 设置 `PROVISA_MTLS_CLIENT_CA` 后，服务器会在握手期间、检查任何凭据之前验证客户端证书。（REQ-1228）启用 `PROVISA_MTLS_BIND_PRINCIPAL` 时，证书的 common name 必须与该连接随后用于身份验证的 `user` 相同。参见[配置](configuration.md#mutual-tls)。

**失败尝试会被计数。** 五分钟内五次失败会将账户锁定十五分钟，且该计数器与 HTTP 和 Bolt 共享——在任一接口上招致的锁定，在所有接口上都生效。（REQ-1393）

**选择组织。** 在多组织部署中，连接到 `<org>.<your-domain>`，pgwire 会从 TLS ClientHello 的主机名中读取组织，方式与 HTTP 从 `Host` 头中读取相同。（REQ-1234）主机名是在请求某个组织，而不是授予它；在该组织中没有成员资格的主体会被拒绝。通过 IP 地址连接则不请求任何组织。

---

## 支持的功能 {: #what-works }

### SELECT

所有 SELECT 语句都会经过治理管道（`_pipeline.py`）。（REQ-001、REQ-262、REQ-266）该管道会：

1. 将语义 SQL 重写为物理 SQL（`rewrite_semantic_to_physical`）
2. 应用治理（行级安全、脱敏、域访问）（REQ-263）
3. 对照已注册的架构 (Schema) 进行验证（REQ-011）
4. 路由至 Trino 或直接路由至数据源池（REQ-027、REQ-028）

支持多语句的简单查询。以分号分隔的语句会被拆分并按顺序执行。（REQ-580）[tool-verified: `server.py:318-381`]

在简单查询模式和扩展查询（Bind/Execute）模式下均支持参数化查询（`$1`、`$2`……）。参数会在执行前以字面量替换。（REQ-581）[tool-verified: `server.py:78-85`]

`SELECT * FROM fn(args)` 和 `SELECT fn(args)`——其中 `fn` 指定一个已注册且受跟踪的函数——会在治理管道之前被拦截，并通过唯一受治理的执行器（`invoke_tracked_function`）路由。结果是一个类型化的行集，与该命令在其他任何界面所返回的结果一致。`writable_by` 及治理规则会在执行器内部强制执行。（REQ-1156）[tool-verified: `provisa/pgwire/function_call.py:74-88`]

### 游标（DECLARE / FETCH / MOVE / CLOSE） {: #cursors-declare-fetch-move-close }

Provisa 在简单查询协议上处理 SQL 游标的文本语法。（REQ-1862） 将 `DECLARE`/`FETCH`/`MOVE`/`CLOSE` 作为普通 SQL 语句发送的客户端，会得到真正的服务端流式传输，而不是协议错误或在客户端内存中缓冲整个结果。

**这与基于 portal 的流式传输不同。** 扩展查询协议——带行数上限字段的 Bind/Execute——是另一种线协议机制。asyncpg 和大多数现代 PostgreSQL 驱动会自动使用它，不需要游标 SQL。如果你的驱动已经使用 portal，你就不需要这种语法。`DECLARE CURSOR` 面向按字面发送游标 SQL 的客户端：psycopg2 的命名/服务端游标（`conn.cursor(name="...")`）、部分 BI 工具以及一些较旧的 JDBC 驱动。

**DECLARE**

```sql
DECLARE c1 CURSOR FOR SELECT id, amount FROM orders WHERE region = 'us-west';
DECLARE c2 SCROLL CURSOR FOR SELECT * FROM events ORDER BY ts;
DECLARE "CaseSensitive" NO SCROLL CURSOR FOR SELECT * FROM large_table;
```
内部的 SELECT 会经过 Provisa 常规的受治理管道——与任何独立 SELECT 相同的行级安全执行、相同的脱敏和相同的域访问检查。[tool-verified: `server.py:1367` — `ctx.execute_sql(inner_sql)`, the same call path as a plain SELECT] 声明游标不会绕过治理。

`SCROLL` 关键字同时控制允许的导航和游标的内存占用：

| 声明 | 向后导航 | 内存开销 |
|---|---|---|
| `DECLARE c CURSOR FOR ...`（默认） | 不允许 | 限于一批 FETCH |
| `DECLARE c NO SCROLL CURSOR FOR ...` | 不允许 | 限于一批 FETCH |
| `DECLARE c SCROLL CURSOR FOR ...` | 允许 | 整个结果缓冲在 Python 内存中 |

既未指定 `SCROLL` 也未指定 `NO SCROLL` 时，默认为 `NO SCROLL`——与 PostgreSQL 本身一致。[tool-verified: `server.py:1361-1362`] `NO SCROLL` 游标在每次向前推进后都会从其内部缓冲区丢弃已消费的行，因此对大结果的 `FETCH FORWARD` 遍历在任一时刻都只在 Python 内存中保留大约一批数据。[tool-verified: `server.py:443-447`] `SCROLL` 游标会保留它拉取过的每一行——这是支持 `FETCH BACKWARD`、`ABSOLUTE`、`FIRST` 和 `LAST` 的代价。在对大结果声明 `SCROLL` 游标之前，请为整个结果的大小预留 Python 堆空间。[tool-verified: `server.py:383-389`]

重新声明已打开的游标名称会先关闭之前的游标。[tool-verified: `server.py:1369-1371`] `WITH HOLD` 和 `WITHOUT HOLD` 被语法接受；参见下文的游标生命周期。

**FETCH 和 MOVE**

```sql
FETCH 100 FROM c1            -- next 100 rows
FETCH FORWARD 100 FROM c1    -- same
FETCH ALL FROM c1            -- all remaining rows
FETCH BACKWARD 10 FROM c1    -- previous 10 rows (SCROLL cursors only)
FETCH ABSOLUTE 50 FROM c1    -- the row at absolute position 50 (SCROLL cursors only)
FETCH RELATIVE -5 FROM c1    -- 5 positions back (SCROLL cursors only)
FETCH FIRST FROM c1          -- row 1 (SCROLL cursors only)
FETCH LAST FROM c1           -- the final row (SCROLL cursors only)
MOVE 100 FROM c1             -- advance position without returning rows
```
`FETCH` 返回行并发送命令标签 `FETCH <n>`，其中 `n` 是实际返回的数量，当游标到达末尾时可能少于请求的数量。`MOVE` 移动位置而不返回行，并发送 `MOVE <n>`。[tool-verified: `server.py:1417`, `server.py:1419`]

`FROM` 和 `IN` 在游标名之前可以互换。未加引号的游标名会转为小写；加引号的名称（`"CaseSensitive"`）保留大小写。[tool-verified: `test_cursor.py:74-79`]

对 `NO SCROLL` 游标进行向后导航（`BACKWARD`、`ABSOLUTE`、`FIRST`、`LAST`、负的 `RELATIVE`）会返回 SQLSTATE 55000（`object_not_in_prerequisite_state`），与 PostgreSQL 自身的拒绝一致。[tool-verified: `server.py:1402-1406`] 对从未声明过的游标名执行 FETCH 或 MOVE 会返回 SQLSTATE 34000（`invalid_cursor_name`）。[tool-verified: `server.py:1398-1400`]

**CLOSE**

```sql
CLOSE c1;    -- release one cursor and free its result stream
CLOSE ALL;   -- release every open cursor on this connection
```
`CLOSE` 释放底层的结果流，并从会话中移除游标。[tool-verified: `server.py:1421-1436`] 不执行 `CLOSE` 就断开连接是安全的：`ProvisaSession.close()` 会在断开时自动释放所有仍处于打开状态的游标。[tool-verified: `test_cursor.py:335-346`]

**游标生命周期。** Provisa 的 pgwire 服务器没有事务状态机（`ProvisaSession.in_transaction()` 固定返回 `False`）。因此每个游标的行为都如同声明了 `WITH HOLD`：它在整个连接期间存续，而不是到 `COMMIT` 为止。[tool-verified: `server.py:200` — `in_transaction()` always returns `False`] 关键字 `WITH HOLD` 和 `WITHOUT HOLD` 会被接受并静默忽略——没有需要执行的按事务划分的游标生命周期。

### 定义（CREATE、ALTER、DROP、TRUNCATE） {: #definitions-create-alter-drop-truncate }

不通过 pgwire 定义任何内容。创建、修改或删除对象的语句（`CREATE TABLE`、`CREATE TABLE … AS SELECT`、`CREATE VIEW`、`ALTER`、`DROP`、索引、序列或 schema）无论角色持有什么，都会以 SQLSTATE 0A000 被拒绝；例外是会话自己的临时表（参见[临时表](#temporary-tables)）。表或视图是模型对象：在模型中创建它们（管理页面、管理 API 或配置），或在数据源中创建后纳入模型。`TRUNCATE` 以同样方式被拒绝；`DELETE` 是删除行的受治理方式。[tool-verified: `provisa/compiler/definitions.py`]

拒绝只发生一次，位于每个 SQL 界面都会经过的管道中，因此 HTTP 上的 SQL、Flight 和 MCP 会以相同的消息拒绝相同的语句。

### 临时表 {: #temporary-tables }

会话可以创建、写入、读取和删除自己的临时表：

```sql
CREATE TEMP TABLE busy AS SELECT customer_id, COUNT(*) AS n FROM sales.orders GROUP BY customer_id;
INSERT INTO busy VALUES (0, 0);
UPDATE busy SET n = n + 1 WHERE customer_id = 0;
DELETE FROM busy WHERE n < 5;
SELECT c.name, b.n FROM sales.customers c JOIN busy b ON b.customer_id = c.id;
DROP TABLE busy;
```
- **创建** —— `CREATE TEMP TABLE name (column type, ...)` 或 `CREATE TEMP TABLE name AS SELECT ...`。名称不接受 schema。它所基于的 `SELECT` 与其他任何读取一样：角色的行规则、脱敏、伪造数据和行数上限都适用于它取得的行。
- **列类型**：`smallint`、`integer`、`bigint`、`real`、`double precision`、`boolean`、`date`、`timestamp`、`text`、`varchar` 以及 `DECIMAL(precision, scale)`。小数保留其精度和标度。其他任何类型都会被拒绝，并指明列和类型：值绝不会以非其本身的类型存储。由 `SELECT` 创建的列采用其值的类型；没有可推断类型的值的列会被拒绝，因此请在 `SELECT` 中使用 `CAST`。
- **行数上限。** `CREATE ... AS SELECT` 或 `INSERT ... SELECT` 取得的行，按角色读取任何内容的方式读取，因此角色的行数上限适用。由被上限截断的读取填充的临时表，会带有与任何被截断读取相同的语句警告（`statement.rows_cut`），并指明上限。
- **写入** —— `INSERT ... VALUES`、`INSERT ... SELECT`、`UPDATE` 和 `DELETE`，不带 `RETURNING`，也不带 `FROM`。写入临时表不是变更：它既不触达数据源，也不触达环境的数据，因此在环境的任何变更处理方式下都可用，也不需要写入权。
- **读取** —— 按其名称，单独读取或与模型表联接。读取由引擎计算。
- **生命周期。** 临时表不是模型对象，其他会话看不到它，它随会话结束。在 pgwire 中，会话就是连接。在没有自己连接的界面上（HTTP 上的 SQL、Flight、MCP），会话就是请求，因此临时表只在该请求中一起发送的语句之间存续。
- **存储。** 行保存在引擎存储中会话自己的 schema 里，并在会话结束时移除。`INSERT`、`UPDATE` 或 `DELETE` 会通过引擎重写整张表，因此临时表适合工作数据，而不适合逐行写入的大表。未结束其会话就停止的进程会在存储中留下这些会话的 schema；它们会在环境停用时被移除。

同样的语句在每个 SQL 界面上都可用：它们在所有界面共享的管道中处理。[inferred: verified by `tests/integration/test_session_temp_tables.py` over HTTP; the pgwire connection-long session is not yet covered by a test]

### 数据写入（INSERT、UPDATE、DELETE、MERGE、COPY FROM STDIN） {: #data-writes-insert-update-delete-merge-copy-from-stdin }

已有注册表的行通过 pgwire 写入，方式与其他任何 SQL 界面相同，经由同一个写入准入（`provisa/compiler/write_admission.py`）：

1. **权利。** 角色持有 `write`。（REQ-868）
2. **列。** 语句写入的每一列都在其 `writable_by` 中指定了该角色；未声明任何内容的列任何人都不能写入。（REQ-663）
3. **可以触及的行。** 角色的行过滤（与其读取所携带的谓词相同）被添加到 `UPDATE` 或 `DELETE` 的 `WHERE` 中。
4. **保留下来的行。** 受过滤的角色只能写入之后它能读取的行：`INSERT` 的行，以及设置了过滤所读取的列的 `UPDATE` 的新值，会在向数据源发送任何内容之前对照过滤进行检查。

无法判定的内容会被点名拒绝，而不是在存疑时放行（`INSERT … SELECT`、既非字面量也非绑定参数的值、读取语句未提供的列的过滤）。[tool-verified: `provisa/compiler/write_admission.py`, `provisa/pgwire/copy_handler.py`]

### COPY

`COPY ... TO STDOUT` 和 `COPY ... FROM STDIN` 都受支持。（REQ-585）

**COPY TO STDOUT** —— 以 PG COPY 线格式导出查询结果。两种形式都可用：

```sql
-- Table reference
COPY my_table TO STDOUT WITH (FORMAT csv)

-- Arbitrary query
COPY (SELECT col1, col2 FROM my_table WHERE ...) TO STDOUT WITH (FORMAT text)
```
支持的格式：`text`（制表符分隔，默认）和 `csv`。COPY 输出不支持二进制格式。[tool-verified: `copy_handler.py:36-52`]

**COPY FROM STDIN** 是向已有注册表写入数据：它由与相同行的 `INSERT` 相同的写入准入放行（参见[数据写入](#data-writes-insert-update-delete-merge-copy-from-stdin)）。

### 事务与会话命令 {: #transactions-and-session-commands }

SET、BEGIN、COMMIT、ROLLBACK、SAVEPOINT、RELEASE、DISCARD、RESET 和 DEALLOCATE 会被拦截并返回空的成功响应。（REQ-587）服务器就事务而言是无状态的——不存在事务隔离或回滚支持。（REQ-587）[tool-verified: `catalog.py:27-31`, `catalog.py:1129-1132`]

---

## 目录拦截 {: #catalog-intercept }

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

## 二进制参数编码 {: #binary-parameter-encoding }

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

## 驱动程序建议 {: #driver-recommendations }

**原生 Python 驱动程序（psycopg2、asyncpg）。** 这些驱动程序默认会协商扩展查询协议，并对大多数类型使用二进制编码。类型保真度在此最高——`NUMERIC` 列以 `Decimal` 形式到达，`TIMESTAMP` 则以 `datetime` 形式到达，依此类推。适用于基于 Python 的 ETL、脚本或直接集成。

**JDBC（PostgreSQL JDBC 驱动程序）。** 适用于 Java 生态系统工具：DBeaver、Tableau、Power BI、Metabase、Airflow 的 JDBC 算子。JDBC 默认使用简单查询协议，可避免二进制编码带来的复杂情况。连接字符串：

```yaml
jdbc:postgresql://<host>:<PROVISA_PGWIRE_PORT>/provisa?user=<role_id>&password=<password>
```

部分基于 JDBC 的 BI 工具在连接时会发送一连串针对 `information_schema` 和 `pg_catalog` 的查询，以填充其架构浏览器。这些查询全部由目录拦截层解答——在架构检查期间不会产生任何 Trino 流量。（REQ-532）

**何时优先选用哪一种。** 若客户端为 Python，请使用 psycopg2 或 asyncpg 以获得更好的类型处理。若客户端为 BI 工具或任何 JVM 应用程序，请使用 JDBC。若观察到类型转换方面的异常情况，应避免在同一连接中混用二进制和文本协议的预期行为——JDBC 的文本模式行为更易于推断。

---

## 注意事项与限制 {: #caveats-and-constraints }

**仅限 SQL。** pgwire 监听器仅解析并执行 SQL——不接受 GraphQL 和 Cypher 字符串。（REQ-614） 对已有表的数据写入由角色的权利、其列授权和其行过滤放行；参见[数据写入](#data-writes-insert-update-delete-merge-copy-from-stdin)。

**不支持定义。** 无论角色持有什么，`CREATE`、`ALTER`、`DROP` 和 `TRUNCATE` 都会以 SQLSTATE 0A000 被拒绝，会话自己的临时表除外；参见[临时表](#temporary-tables)。（REQ-615、REQ-616）

**不支持真正的事务功能。** BEGIN/COMMIT/ROLLBACK 会被接受并静默忽略。每条语句均独立执行。（REQ-587）[tool-verified: `server.py:146-158`——`in_transaction()` 始终返回 `False`]

**查询超时。** 语句可以使用 pgwire 自己的请求超时 `limits.request_timeouts.pgwire`，未设置时使用共享的 `limits.request_timeout`；两者都是运维设置。（REQ-590、REQ-1905） [tool-verified: `provisa/core/limits.py`, `server.py`]

**COPY 输出格式为 text 或 csv。** 尚未实现 PG 二进制 COPY 格式（`FORMAT binary`）。[inferred：`_rows_to_copy_text` / `_rows_to_copy_csv` 中仅存在 `text` 和 `csv` 分支]

**参数替换为字面量形式。** `$1`、`$2`……等参数会在执行前以 SQL 字面量替换，而非以绑定参数形式发送至底层引擎。这意味着底层引擎永远不会看到已准备的语句。对 Trino 而言这没有实际影响；对于直接连接池的数据源，则会绕过预准备语句的缓存机制。（REQ-581）[tool-verified: `server.py:78-85`]

**DECLARE SCROLL 会缓冲整个结果。** `SCROLL` 游标会把它从引擎拉取的每一行累积到 Python 堆中，以便按需提供 `FETCH BACKWARD`、`ABSOLUTE`、`FIRST` 和 `LAST`。面对大结果，这意味着只要游标处于打开状态，整个结果就驻留在内存中。仅向前的流式传输请使用默认的 `NO SCROLL`。（REQ-1862） [tool-verified: `server.py:383-389`]

**`pg_stat_activity`、`pg_stat_user_tables`、`pg_extension`、`pg_enum`、`pg_attrdef`、`pg_proc`。** 这些表存在于目录层中，但属于空的桩 (stub)。查询它们的监控工具将收到零行结果，而非错误。（REQ-532）[tool-verified: `catalog.py:519-535`, `catalog.py:639-934`]（`pg_index` 已有数据填充——参见"目录拦截"一节。）
