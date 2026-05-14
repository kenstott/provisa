# PostgreSQL Wire Protocol Server

## Overview

The pgwire server exposes Provisa's governance pipeline as a standard PostgreSQL endpoint [tool-verified: `provisa/pgwire/server.py`].
Clients (psql, DBeaver, asyncpg, SQLAlchemy with psycopg2/asyncpg) connect on a configurable port (default 5439) using the
PG3 protocol. Queries are either answered from an in-memory DuckDB catalog (for `pg_catalog`, `information_schema`, and scalar
session functions) or forwarded through the Provisa governance pipeline to Trino [tool-verified: `provisa/pgwire/_pipeline.py`].

## Protocol layers

- **TLS** — optional; server wraps the TCP socket with an `ssl.SSLContext` when `PROVISA_PGWIRE_CERT` and `PROVISA_PGWIRE_KEY`
  are set; SSL-request handshake (code 80877103) is handled in `ProvisaHandler.handle_startup` [tool-verified]
- **Auth** — PG cleartext password (auth type 3); two providers are supported at the pgwire layer:
  `none` (trust mode) — the PG username is accepted as the Provisa role_id and the password is ignored,
  allowing role selection by username with no credential setup;
  `simple` — password validated via bcrypt (`SimpleAuthProvider.login`), failed auth sends FATAL `28P01`.
  All other providers (`firebase`, `keycloak`, `oauth`, `basic`) are rejected with FATAL `28P01` since
  they require token-based flows incompatible with the PG cleartext password protocol [tool-verified: `server.py:handle_md5_password`]
- **Catalog intercept** — `classify()` routes `SET`, `SHOW`, `BEGIN`/`COMMIT`/`ROLLBACK`/`SAVEPOINT`/`RELEASE`,
  `information_schema.*`, `pg_catalog.*`, and scalar session functions to `catalog.answer()` backed by DuckDB [tool-verified: `catalog.py:classify`]
- **Governance pipeline** — all other queries are forwarded to `execute_pgwire_sql()` which runs the full Provisa
  compile → Trino execute path with role-based visibility [tool-verified: `_pipeline.py`]
- **Parameter binding** — `$N` placeholders are substituted with safe SQL literals before routing [tool-verified: `server.py:_substitute_params`]

## Query routing

`classify(sql)` returns `"INTERCEPT"` or `"PASS_THROUGH"`:

| Pattern | Decision |
|---|---|
| `SET …` | INTERCEPT |
| `SHOW …` | INTERCEPT |
| `BEGIN`, `COMMIT`, `ROLLBACK`, `SAVEPOINT`, `RELEASE`, `START TRANSACTION` | INTERCEPT |
| `DISCARD`, `RESET`, `DEALLOCATE` | INTERCEPT |
| `SELECT current_user` / `session_user` / `current_database()` / `version()` / `current_schema()` / `pg_backend_pid()` | INTERCEPT |
| `SELECT … FROM information_schema.*` | INTERCEPT |
| `SELECT … FROM pg_catalog.*` | INTERCEPT |
| `SELECT current_setting(…)` | INTERCEPT |
| Anything else | PASS_THROUGH |

## Catalog tables

Intercepted tables are answered from a per-request DuckDB in-memory database built by `_build_catalog_db(role_id, state)` [tool-verified]:

**`information_schema`**: `schemata`, `tables`, `columns`, `views`

**`pg_catalog`**: `pg_namespace`, `pg_class`, `pg_attribute`, `pg_type`, `pg_attrdef`, `pg_description`,
`pg_index`, `pg_constraint`, `pg_proc`, `pg_roles`, `pg_auth_members`, `pg_database`, `pg_settings`,
`pg_tables`, `pg_stat_user_tables`, `pg_statio_user_tables`, `pg_am`

Table and column metadata comes from `state.contexts[role_id].tables` and `state.schema_build_cache["column_types"]` [tool-verified].

Scalar session functions (`current_user`, `version()`, etc.) are answered directly without DuckDB via `_handle_scalar()` [tool-verified].

## Parameter binding

`$N` parameters follow PostgreSQL positional syntax. Before routing, `_substitute_params(sql, params)` iterates
from the highest index down to `$1` to prevent `$1` from partially matching `$10`, `$11`, etc. Values are rendered
as SQL literals by `_pg_literal(v)`: `None` → `NULL`, `bool` → `TRUE`/`FALSE`, numeric → bare number,
bytes → `E'\\xHH...'`, strings → single-quoted with `'` escaped as `''` [tool-verified: `server.py:_pg_literal`].
This is a client-side rewrite — the substituted SQL is what reaches DuckDB or the pipeline [inferred: no server-side bind protocol].

## Configuration

| Env var | Default | Purpose |
|---|---|---|
| `PROVISA_PGWIRE_PORT` | 5439 | TCP port the server binds to [inferred] |
| `PROVISA_PGWIRE_CERT` | — | Path to TLS certificate file [inferred] |
| `PROVISA_PGWIRE_KEY` | — | Path to TLS private key file [inferred] |

The server is started by `start_pgwire_server(host, port, ssl_ctx, loop)` called from the FastAPI app lifespan [tool-verified: `server.py:start_pgwire_server`].

## Testing

Unit and integration tests live in `tests/unit/pgwire/`:

| File | Coverage |
|---|---|
| `test_server.py` | `ProvisaQueryResult`, `_infer_bvtype`, `_tag_from_sql`, session catalog routing |
| `test_catalog.py` | `classify()`, `answer()` for all intercepted query classes |
| `test_wire_protocol.py` | Phase 1 wire-level integration tests |
| `test_phase2.py` | Param binding (`_substitute_params`), scalar intercepts, txn regex fixes, `pg_am`, wire param integration |

Run with `python -m pytest tests/unit/pgwire/ -v`.
