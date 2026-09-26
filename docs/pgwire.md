# Provisa pgwire Server

Provisa exposes a PostgreSQL wire protocol (pgwire) endpoint. Any tool that speaks the PostgreSQL client protocol — psycopg2, asyncpg, DBeaver, Tableau, JDBC — can connect and query Provisa data through the same governance pipeline that governs the HTTP API. (REQ-266)

Queries go through the full governance stack: RLS enforcement, masking rules, relationship guards, domain-access checks. (REQ-001, REQ-002, REQ-263) The pgwire interface is not a bypass. (REQ-002, REQ-266)

---

## Connection Details

The server starts when `PROVISA_PGWIRE_PORT` is set to a non-zero integer. It is disabled by default. (REQ-527) [tool-verified: `app.py:1739`]

```yaml
Host: 0.0.0.0  (all interfaces)
Port: $PROVISA_PGWIRE_PORT
```

**TLS.** Set `PROVISA_PGWIRE_CERT` and `PROVISA_PGWIRE_KEY` to the paths of a PEM certificate and key. When both are present, the server wraps incoming connections in TLS. When absent, TLS is off and the server replies `N` to SSL negotiation requests. (REQ-530) [tool-verified: `server.py:1746-1750`]

**Reported server version.** Clients see `14.0.provisa`. Tools that gate features on the version number may behave as though connected to PostgreSQL 14. (REQ-579) [tool-verified: `server.py:208`]

---

## Authentication

The startup packet carries a username and one secret field, and no scheme to say what the secret is. Provisa decides from the secret itself, so a client needs no configuration beyond `user` and `password`:

| The secret is | Recognized by | Resolves to |
| --------------- | --------------- | ------------- |
| A personal access token | its `provisa_pat_` prefix | the token's owner and role (REQ-1263) |
| An OIDC / provider bearer token | the configured provider being a token provider | the identity the token asserts (REQ-890) |
| A password | anything else | the account in the configured provider (`basic` or `simple`) |

The decision is made once. A credential the chosen validator refuses is not retried against another, so one rejection does not become a second guess.

Trust mode (`provider: none`, or auth middleware inactive) is the exception: the username is used directly as the `role_id` and the secret is ignored. Do not use it over an unencrypted connection.

**SCRAM-SHA-256.** Under `provider: basic` with `auth.scram: true`, the server advertises SASL (authentication code 10) with `SCRAM-SHA-256` and the password is proved rather than sent. (REQ-1394) `SCRAM-SHA-256-PLUS` is not offered. A user whose verifier has not been written yet — verifiers cannot be derived from bcrypt hashes — is answered with a mock exchange, so the wire does not reveal who has migrated; that user authenticates by cleartext password over TLS until their next password entry writes one. With `auth.scram` off, the server uses PG auth type 3 (cleartext password). MD5 is not supported in either case.

**Client certificates.** Set `PROVISA_MTLS_CLIENT_CA` and the server verifies a client certificate during the handshake, before any credential is examined. (REQ-1228) With `PROVISA_MTLS_BIND_PRINCIPAL` the certificate's common name must equal the `user` the connection then authenticates as. See [Configuration](configuration.md#mutual-tls).

**Failed attempts are counted.** Five failures in five minutes locks the account out for fifteen minutes, and the counter is shared with HTTP and Bolt — a lockout earned on any surface holds on all of them. (REQ-1393)

**Choosing an org.** On a multi-org deployment, connect to `<org>.<your-domain>` and pgwire reads the org from the hostname in the TLS ClientHello, the same way HTTP reads it from the `Host` header. (REQ-1234) The hostname requests an org; it does not grant one, and a principal with no membership there is refused. Connecting by IP address requests no org.

---

## What Works

### SELECT

All SELECT statements go through the governance pipeline (`_pipeline.py`). (REQ-001, REQ-262, REQ-266) The pipeline:

1. Rewrites semantic SQL to physical SQL (`rewrite_semantic_to_physical`)
2. Applies governance (RLS, masking, domain access) (REQ-263)
3. Validates against registered schema (REQ-011)
4. Routes to Trino or direct source pool (REQ-027, REQ-028)

Multi-statement simple queries are supported. Semicolon-separated statements are split and executed in order. (REQ-580) [tool-verified: `server.py:318-381`]

Parameterized queries (`$1`, `$2`, ...) are supported in both simple-query and extended-query (Bind/Execute) modes. Parameters are substituted as literals before execution. (REQ-581) [tool-verified: `server.py:78-85`]

`SELECT * FROM fn(args)` and `SELECT fn(args)` — where `fn` names a registered tracked function — are intercepted before the governance pipeline and routed through the single governed executor (`invoke_tracked_function`). The result is a typed row set identical to what every other surface returns for that command. `writable_by` and governance rules are enforced inside the executor. (REQ-1156) [tool-verified: `provisa/pgwire/function_call.py:74-88`]

### Cursors (DECLARE / FETCH / MOVE / CLOSE)

Provisa handles textual SQL cursor syntax over the simple-query protocol. (REQ-1862) Clients that send `DECLARE`/`FETCH`/`MOVE`/`CLOSE` as plain SQL statements get real server-side streaming rather than a protocol error or a full-result buffer loaded into client memory.

**This is separate from portal-based streaming.** The extended-query protocol — Bind/Execute with a row-limit field — is a different wire mechanism. asyncpg and most modern PostgreSQL drivers use it automatically without any cursor SQL. If your driver already uses portals, you don't need this syntax. `DECLARE CURSOR` is for clients that send cursor SQL literally: psycopg2's named/server-side cursor feature (`conn.cursor(name="...")`), certain BI tools, and some legacy JDBC drivers.

**DECLARE**

```sql
DECLARE c1 CURSOR FOR SELECT id, amount FROM orders WHERE region = 'us-west';
DECLARE c2 SCROLL CURSOR FOR SELECT * FROM events ORDER BY ts;
DECLARE "CaseSensitive" NO SCROLL CURSOR FOR SELECT * FROM large_table;
```

The inner SELECT runs through Provisa's normal governed pipeline — the same RLS enforcement, masking, and domain-access checks that apply to any standalone SELECT. [tool-verified: `server.py:1367` — `ctx.execute_sql(inner_sql)`, the same call path as a plain SELECT] Declaring a cursor is not a governance bypass.

The `SCROLL` keyword controls both what navigation is allowed and how much memory the cursor uses:

| Declaration | Backward navigation | Memory cost |
|---|---|---|
| `DECLARE c CURSOR FOR ...` (default) | Not allowed | Bounded to one FETCH batch |
| `DECLARE c NO SCROLL CURSOR FOR ...` | Not allowed | Bounded to one FETCH batch |
| `DECLARE c SCROLL CURSOR FOR ...` | Allowed | Full result buffered in Python memory |

When neither `SCROLL` nor `NO SCROLL` is written, the default is `NO SCROLL` — matching PostgreSQL's own default. [tool-verified: `server.py:1361-1362`] A `NO SCROLL` cursor trims consumed rows from its internal buffer after each forward step, so a `FETCH FORWARD` scan through a large result never holds more than roughly one batch in Python memory at a time. [tool-verified: `server.py:443-447`] A `SCROLL` cursor retains every row it has ever pulled — that's the cost of supporting `FETCH BACKWARD`, `ABSOLUTE`, `FIRST`, and `LAST`. Before declaring a `SCROLL` cursor against a large result, budget the full result size as Python heap. [tool-verified: `server.py:383-389`]

Re-declaring a cursor name that is already open closes the previous cursor first. [tool-verified: `server.py:1369-1371`] `WITH HOLD` and `WITHOUT HOLD` are accepted in the syntax; see Cursor Lifetime below.

**FETCH and MOVE**

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

`FETCH` returns rows and sends command tag `FETCH <n>`, where `n` is the actual count returned — which may be less than requested when the cursor reaches the end. `MOVE` advances the position without returning rows and sends `MOVE <n>`. [tool-verified: `server.py:1417`, `server.py:1419`]

`FROM` and `IN` are interchangeable as the cursor name introducer. Unquoted cursor names are case-folded to lowercase; quoted names (`"CaseSensitive"`) preserve case. [tool-verified: `test_cursor.py:74-79`]

Backward navigation (`BACKWARD`, `ABSOLUTE`, `FIRST`, `LAST`, negative `RELATIVE`) against a `NO SCROLL` cursor returns SQLSTATE 55000 (`object_not_in_prerequisite_state`), matching real PostgreSQL's own rejection. [tool-verified: `server.py:1402-1406`] A FETCH or MOVE naming a cursor that was never declared returns SQLSTATE 34000 (`invalid_cursor_name`). [tool-verified: `server.py:1398-1400`]

**CLOSE**

```sql
CLOSE c1;    -- release one cursor and free its result stream
CLOSE ALL;   -- release every open cursor on this connection
```

`CLOSE` releases the underlying result stream and removes the cursor from the session. [tool-verified: `server.py:1421-1436`] Disconnecting without issuing `CLOSE` is safe: `ProvisaSession.close()` releases all still-open cursors automatically on disconnect. [tool-verified: `test_cursor.py:335-346`]

**Cursor lifetime.** Provisa's pgwire server has no transaction state machine (`ProvisaSession.in_transaction()` is hardcoded `False`). Every cursor therefore behaves as if declared `WITH HOLD`: it lives for the duration of the connection, not until `COMMIT`. [tool-verified: `server.py:200` — `in_transaction()` always returns `False`] The `WITH HOLD` and `WITHOUT HOLD` keywords are accepted and silently ignored — there is no per-transaction cursor lifecycle to enforce.

### DDL

DDL statements are detected by the regex in `server.py` and dispatched to `DdlHandler`. The role must have the `"ddl"` capability. (REQ-042) Without it, the statement is rejected with SQLSTATE 42501. [tool-verified: `ddl_handler.py:82-83`]

The recognized DDL forms are:

```sql
CREATE TABLE / VIEW / INDEX / UNIQUE INDEX / SEQUENCE / SCHEMA
ALTER TABLE / INDEX / SEQUENCE / VIEW
DROP TABLE / VIEW / INDEX / SEQUENCE / SCHEMA
```

[tool-verified: `server.py:56-61`]

Two execution paths exist depending on `ddl_catalog`: (REQ-582)

**Trino path** — used when `ddl_catalog` is an Iceberg, Hive, or other non-registered Trino catalog (e.g. `iceberg`, `hive`, `otel`, `results`). Only `CREATE TABLE` and `CREATE VIEW` are supported on this path. Attempting `ALTER`, `DROP`, or `CREATE INDEX` raises an error. The table name is fully qualified as `catalog.schema.table`. [tool-verified: `ddl_handler.py:92-100`]

**Direct path** — used when `ddl_catalog` matches a registered source ID. Full DDL is supported: CREATE, ALTER, DROP, indexes, sequences. `CREATE TABLE` and `CREATE VIEW` are schema-qualified as `schema.table`. All other DDL (ALTER, DROP, CREATE INDEX) passes through as-is after setting the schema context. For PostgreSQL and SQLite sources, context is set with `SET search_path TO schema`. For MySQL and MariaDB, context is set with `USE schema`. [tool-verified: `ddl_handler.py:139-170`, `ddl_handler.py:207-213`]

After DDL on either path, the new table is registered into the role's compilation context so it is immediately queryable. (REQ-583) [tool-verified: `ddl_handler.py:216-250`]

**Write target resolution.** The DDL catalog and schema come from the domain's `ddl_catalog` and `ddl_schema` fields. If `ddl_catalog` is not set, the system defaults to the Iceberg catalog. If `ddl_schema` is not set, it defaults to the domain ID. The domain is resolved through the role's `domain_access` list. (REQ-584) [tool-verified: `app.py:804-811`, `ddl_handler.py:104-115`]

### COPY

`COPY ... TO STDOUT` and `COPY ... FROM STDIN` are both supported. (REQ-585) [tool-verified: `copy_handler.py:231-257`]

**COPY TO STDOUT** — exports query results in PG COPY wire format. Two forms work:

```sql
-- Table reference
COPY my_table TO STDOUT WITH (FORMAT csv)

-- Arbitrary query
COPY (SELECT col1, col2 FROM my_table WHERE ...) TO STDOUT WITH (FORMAT text)
```

Supported formats: `text` (tab-delimited, default) and `csv`. Binary format is not supported on COPY output. [tool-verified: `copy_handler.py:36-52`]

**COPY FROM STDIN** — inserts rows into a target table. Restricted to sources with types `postgresql`, `mysql`, `sqlite`, or `mariadb`. (REQ-586) Attempting COPY FROM against a Trino-only source (e.g. Iceberg) raises a permission error. [tool-verified: `copy_handler.py:65`, `copy_handler.py:351-356`]

```sql
COPY my_table (col1, col2) FROM STDIN WITH (FORMAT text)
```

If no column list is provided, columns are inferred from the registered schema. [tool-verified: `copy_handler.py:357`]

### Transactions and Session Commands

SET, BEGIN, COMMIT, ROLLBACK, SAVEPOINT, RELEASE, DISCARD, RESET, and DEALLOCATE are intercepted and return an empty success response. (REQ-587) The server is stateless with respect to transactions — there is no transaction isolation or rollback support. (REQ-587) [tool-verified: `catalog.py:27-31`, `catalog.py:1129-1132`]

---

## Catalog Intercept

Queries against `information_schema` and `pg_catalog` are answered locally without a Trino round-trip. (REQ-532) The intercept layer builds an in-memory DuckDB database per request, populated from the role's compilation context. (REQ-532) [tool-verified: `catalog.py:210-213`]

Intercepted tables:

**information_schema:** `schemata`, `tables`, `columns`, `views`, `table_constraints`, `key_column_usage`, `referential_constraints`

**pg_catalog:** `pg_namespace`, `pg_class`, `pg_attribute`, `pg_type`, `pg_attrdef`, `pg_description`, `pg_index`, `pg_constraint`, `pg_proc`, `pg_roles`, `pg_auth_members`, `pg_database`, `pg_settings`, `pg_tables`, `pg_stat_user_tables`, `pg_statio_user_tables`, `pg_am`, `pg_extension`, `pg_enum`, `pg_stat_activity`

[tool-verified: `catalog.py:39-67`]

`pg_constraint` is populated with real PK and FK data derived from the domain model's `pk_columns` and `joins`. (REQ-392, REQ-399) BI tools that inspect foreign-key relationships (Tableau, DBeaver, etc.) will see the join graph Provisa knows about. [tool-verified: `catalog.py:551-632`] Single-column joins between the same source/target pair whose target columns together form the target's composite primary key are collapsed into one FK row with multi-element `conkey`/`confkey` arrays. (REQ-1094) [tool-verified: `catalog_constraints.py`]

A junction-backed relationship (REQ-1586) produces no FK row. It is an edge through an associative table, not a column pair, and `pg_constraint` has no shape for two hops — so the domain model leaves it out of `joins` and the junction table appears as an ordinary table with its own foreign keys to each endpoint. SQL clients reach it by joining that table; Cypher clients traverse it as a single relationship. [tool-verified: `provisa/compiler/schema_gen.py:302-306`]

`pg_index` is populated with one row per primary-key and UNIQUE constraint (`indrelid` = table oid, `indkey` = ordered key attnums, `indisprimary`/`indisunique` set). Clients that resolve key columns via `pg_index.indkey` rather than `pg_constraint` — DataGrip, for example — discover the correct columns through the standard `pg_index` → `pg_attribute` join. (REQ-1095) [tool-verified: `catalog_constraints.py:340-384`]

The following scalar expressions are also intercepted: (REQ-588)

- `current_user`, `session_user` → the authenticated `role_id`
- `current_database()` → `"provisa"`
- `current_schema()` → `"public"`
- `version()` → `"PostgreSQL 14.0 on Provisa"`
- `pg_backend_pid()` → `0`
- `current_setting(...)` → returns from a fixed settings table
- `SHOW <setting>` → returns from the same settings table

[tool-verified: `catalog.py:168-207`, `catalog.py:1076-1120`]

---

## Binary Parameter Encoding

The extended-query protocol (Bind/Execute) supports binary-encoded parameters. (REQ-589) The following type OIDs are decoded from binary: [tool-verified: `postgres.py:69-97`]

| OID | PG type | Python type |
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

Any OID not in this table raises `"Unsupported binary parameter type: <oid>"`. (REQ-589) [tool-verified: `postgres.py:579`]

Result columns are also sent in binary when the client requests it, for the same type set plus ARRAY, JSON, INTERVAL, and BIGINT. (REQ-589) [tool-verified: `postgres.py:191-244`]

---

## Driver Recommendations

**Native Python drivers (psycopg2, asyncpg).** These negotiate the extended-query protocol by default and use binary encoding for most types. Type fidelity is highest here — `NUMERIC` columns arrive as `Decimal`, `TIMESTAMP` as `datetime`, and so on. Use these for Python-based ETL, scripts, or direct integration.

**JDBC (PostgreSQL JDBC driver).** Use this for Java-ecosystem tools: DBeaver, Tableau, Power BI, Metabase, Airflow JDBC operators. JDBC defaults to the simple-query protocol, which avoids binary encoding complications. Connection string:

```yaml
jdbc:postgresql://<host>:<PROVISA_PGWIRE_PORT>/provisa?user=<role_id>&password=<password>
```

Some JDBC-based BI tools send a burst of `information_schema` and `pg_catalog` queries on connect to populate their schema browser. These are all answered by the catalog intercept layer — no Trino traffic is generated during schema inspection. (REQ-532)

**When to prefer one over the other.** If the client is Python, use psycopg2 or asyncpg for better type handling. If the client is a BI tool or any JVM application, use JDBC. Avoid mixing binary and text protocol expectations in the same connection if you observe type conversion surprises — JDBC's text-mode behavior is simpler to reason about.

---

## Caveats and Constraints

**SQL only; no DML mutations.** The pgwire listener parses and executes SQL only — GraphQL and Cypher strings are not accepted. (REQ-614) Plain `INSERT`, `UPDATE`, and `DELETE` are not routed to a write path. (REQ-615) Write data through `COPY FROM STDIN` (writable sources) or `CREATE TABLE AS`; row-level mutations go through the GraphQL, Cypher, or Trino write paths instead.

**COPY and DDL require the `ddl` capability.** Both `COPY` (in either direction) and DDL are gated on the role's `ddl` capability; roles without it receive SQLSTATE 42501. (REQ-616)

**No real transaction support.** BEGIN/COMMIT/ROLLBACK are accepted and silently ignored. Each statement runs independently. (REQ-587) [tool-verified: `server.py:146-158` — `in_transaction()` always returns `False`]

**60-second DDL timeout, 120-second query timeout.** These are hard-coded in the handler threads. (REQ-590) Long-running DDL against remote sources (schema changes on large tables) may time out. [tool-verified: `ddl_handler.py:136`, `server.py:186`]

**COPY FROM is writable-source-only.** Iceberg, Hive, Trino-only sources, and read-only source types do not accept COPY FROM. The error is SQLSTATE 42501. (REQ-586) [tool-verified: `copy_handler.py:65`]

**COPY output format is text or csv.** PG binary COPY format (`FORMAT binary`) is not implemented. [inferred: only `text` and `csv` branches exist in `_rows_to_copy_text` / `_rows_to_copy_csv`]

**DDL on Trino path is CREATE only.** ALTER, DROP, and CREATE INDEX against Iceberg or Hive catalogs are not supported. Use a registered SQL source as `ddl_catalog` if you need full DDL. (REQ-582) [tool-verified: `ddl_handler.py:92-100`]

**Parameter substitution is literal.** `$1`, `$2`, ... parameters are substituted as SQL literals before execution, not sent as bind parameters to the upstream engine. This means the upstream engine never sees a prepared statement. For Trino this has no practical impact; for direct-pool sources it bypasses prepared-statement caching. (REQ-581) [tool-verified: `server.py:78-85`]

**DECLARE SCROLL buffers the full result.** A `SCROLL` cursor accumulates every row it pulls from the engine into Python heap so `FETCH BACKWARD`, `ABSOLUTE`, `FIRST`, and `LAST` can be served on demand. Against a large result this means the whole result lives in memory for as long as the cursor is open. Use the default `NO SCROLL` for forward-only streaming. (REQ-1862) [tool-verified: `server.py:383-389`]

**`pg_stat_activity`, `pg_stat_user_tables`, `pg_extension`, `pg_enum`, `pg_attrdef`, `pg_proc`.** These tables exist in the catalog layer but are empty stubs. Monitoring tools that query them will receive zero rows rather than errors. (REQ-532) [tool-verified: `catalog.py:519-535`, `catalog.py:639-934`] (`pg_index` is populated — see Catalog Intercept.)
