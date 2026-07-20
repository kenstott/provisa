# PostgreSQL FDW / Extension Catalog for Embedded `pgserver` (REQ-898)

Reference for the curated extension catalog: what foreign-data wrappers exist for each
Provisa source type, how to acquire them, and how to compile them against a pinned
PostgreSQL version. Feeds the Postgres federation engine (REQ-893) and complements the
DuckDB engine (REQ-894/895).

Data gathered from upstream repositories and PGXS documentation, July 2026. Verify version
numbers and manylinux tags per wheel before pinning — several fields below carry stated
uncertainty.

## The embedded base: what `pgserver` gives you

`pgserver` and its forks ship precompiled PostgreSQL binaries plus `pgvector` only. Every FDW
below except `postgres_fdw`, `file_fdw`, and `dblink` (compiled into the standard source tree)
must be built from source and dropped into the bundled tree.

| Package | Repo | Bundled PG major | Extra bundled | Status |
|---|---|---|---|---|
| `pgserver` | orm011/pgserver | 16 (16.2) | pgvector | Unmaintained since mid-2024 |
| `pixeltable-pgserver` | pixeltable/pixeltable-pgserver | 16 (16.10) | pgvector 0.8.1, pgvectorscale | Active 2026 |
| `pgembed` | Ladybug-Memory/pgembed | 17 | via separate wheels | Most current; separate-wheel extension model |

Confirm the exact minor with `<pkg>.POSTGRES_BIN_PATH / "pg_config" --version` against the
installed wheel — do not hardcode.

### Locating the bundled tree

```python
import pgserver, pathlib
pg_config = pathlib.Path(pgserver.POSTGRES_BIN_PATH) / "pg_config"
```

Derive everything else from `pg_config`, never a hardcoded path:

| Query | Purpose |
|---|---|
| `pg_config --pkglibdir` | where the compiled `.so`/`.dylib` module lands (`$libdir`) |
| `pg_config --sharedir` | `.control` + `--version.sql` go under `sharedir/extension/` |
| `pg_config --includedir-server` | server headers for PGXS (shipped in the wheel — confirmed) |
| `pg_config --pgxs` | the PGXS makefile |

## The pin is a triple, not a version

An externally compiled `.so` must match three axes, or the server refuses to load it:

1. **PG major ABI** — the module magic block. Cross-major never loads; same-major
   cross-minor generally does.
2. **Platform / arch** — `.so` vs `.dylib` vs `.dll`; x86_64 vs arm64.
3. **libc + C++ ABI (Linux)** — the wheel's glibc floor, and for C++ extensions
   (Arrow / DuckDB) the libstdc++ / GCC ABI and `_GLIBCXX_USE_CXX11_ABI` setting.

The safe build is inside the same manylinux image the wheel was built with, using the
wheel's own `pg_config`. The linux wheels' exact manylinux tag is undocumented; the
`postgresql-wheel` lineage targets manylinux2014 (glibc >= 2.17), and `pgembed` advertises
arm64 + alpine, implying newer/multiple tags. Confirm per wheel via `pip download` + unzip.

Catalog resolution must fail closed: no artifact for a `(pg_major, platform, libc)` triple
means the source is unreachable on the Postgres engine — never a silent fallback (project rule).

## Generic PGXS build against the pinned server

```sh
PG_CONFIG=$(python -c "import pgserver,os;print(os.path.join(pgserver.POSTGRES_BIN_PATH,'pg_config'))")
make USE_PGXS=1 PG_CONFIG="$PG_CONFIG"
make USE_PGXS=1 PG_CONFIG="$PG_CONFIG" install
```

`PG_CONFIG=...` must come **after** `make`, not before — a leading assignment is overridden by
the makefile internals. External native libraries in a non-standard prefix are passed via
`PG_CPPFLAGS="-I..."` and `SHLIB_LINK`/`CCFLAGS="-L..."`.

The install step writes `<ext>.so` to `pkglibdir` and `<ext>.control` + `<ext>--*.sql` to
`sharedir/extension/`. Activate per database with `CREATE EXTENSION <ext>;`. For files outside
the main install dir, point the server at them with `dynamic_library_path` and (PG18+)
`extension_control_path`.

## Distribution model (the catalog itself)

Follow the `pgembed` template: one wheel per `(extension × pg_major × platform tag)`, built in
CI against the pinned base wheel's `pg_config`. The wheel's install step copies the `.so` into
`pkglibdir` and the `.control`/`.sql` into `sharedir/extension/`. Version-lock every extension
wheel to the base's PG major. Bundled native deps (libsqlite3, Arrow, DuckDB) must ride inside
the artifact — the base wheel does not provide them.

---

## Catalog: file and embedded sources

### file_fdw — CSV (`csv` source type)

- Repo: in-tree `contrib/file_fdw` (PostgreSQL License). In core contrib.
- **No compile needed** — ships with any PG built with contrib; activate with
  `CREATE EXTENSION file_fdw;`. If absent:
  `cd contrib/file_fdw && make PG_CONFIG=<bundled> && make install PG_CONFIG=<bundled>`.
- Read-only, no pushdown, **no IMPORT FOREIGN SCHEMA** — every column must be declared
  (flat files carry no schema). This is why `FileFdwConnector.details()` completes the
  `CREATE FOREIGN TABLE` column list from registry metadata.
- `filename`/`program` are superuser-restricted; paths are server-side, relative to the data
  dir. The embedded server can only read files its own process can see.

### sqlite_fdw — SQLite (`sqlite` source type)

- Repo: **pgspider/sqlite_fdw** (canonical, active). `gleu/sqlite_fdw` is deprecated and its
  README redirects to pgspider. BSD/PostgreSQL license.
- Latest v2.5.0 (2024-12-10), PG 13–17.
- Native dep: `libsqlite3-dev` (any modern SQLite 3.x; confirmed 3.49.0). Optional
  `libspatialite-dev` for SpatiaLite/GIS.
- Build: `make USE_PGXS=1 PG_CONFIG=<bundled>`; non-standard SQLite prefix via
  `SQLITE_INCLUDE=/prefix/include SQLITE_LIB=/prefix/lib`.
- Read/write (INSERT/UPDATE/DELETE, TRUNCATE→DELETE); pushdown of WHERE, ORDER BY, aggregates,
  GROUP BY/HAVING, JOIN, LIMIT/OFFSET; IMPORT FOREIGN SCHEMA supported; `force_readonly` (v2.5.0).
- **Gotcha:** SQLite type affinity — mismatched column types silently return wrong results.
  Use the `column_type` option (e.g. epoch timestamps as `INT`, UUID as TEXT/BLOB). Numeric
  limited to SQLite ~15-digit float precision; NaN read as NULL.

## Catalog: Parquet sources (`parquet` source type)

### parquet_fdw — local Parquet

- Repo: adjust/parquet_fdw (PostgreSQL License). **Effectively stale** — v0.2.1 (2021),
  **does not compile on PG16+** without the community `parquet_fdw_16.diff`. Given pgserver
  bundles PG16/17, this one needs patching to build at all.
- Native dep: libarrow + libparquet >= 0.15, practically pinned to **Arrow 6.x** (Arrow 7+
  breaks the build). The heaviest dependency in the catalog.
- Read-only. IMPORT FOREIGN SCHEMA directory discovery + `import_parquet()`; row-group
  (min/max) predicate pushdown + projection pushdown; parallel scan; four reader strategies
  including multifile-merge for presorted files. **Local filesystem only.**

### parquet_s3_fdw — local + S3 Parquet

- Repo: **pgspider/parquet_s3_fdw** (BSD-3, derived from adjust/parquet_fdw, actively
  maintained). v1.1.1 (2024-10-10), PG **13–17**. Prefer this over parquet_fdw for the
  bundled PG majors.
- Native deps: libarrow + libparquet **16.1.0** and **AWS SDK for C++ 1.11.335**. The project
  recommends building both from source — precompiled Arrow causes libstdc++ ABI mismatch. Arrow
  must be built `-DARROW_WITH_SNAPPY=ON -DARROW_WITH_ZSTD=ON`.
- Read + limited write (no transactions; concurrent same-file writes unsafe). IMPORT FOREIGN
  SCHEMA, predicate + projection pushdown, parallel scan, schemaless JSONB mode.
- **S3 + MinIO**: server/table OPTIONS `endpoint`, `region`, `use_minio 'true'`; credentials
  via `CREATE USER MAPPING ... OPTIONS (user '<accesskey>', password '<secret>')` or the AWS
  credentials file. `endpoint` override targets MinIO / non-AWS S3.
- **Gotcha:** the Arrow/AWS-SDK ABI lock is the dominant risk. Pin Arrow 16.1.0 / AWS SDK
  1.11.335 exactly and build inside the wheel's manylinux image.

## Catalog: remote RDBMS sources

| FDW | Source types | Repo | Latest | PG | Native dep | Read/Write | Pushdown |
|---|---|---|---|---|---|---|---|
| mysql_fdw | mysql, mariadb, singlestore | EnterpriseDB/mysql_fdw | 2.9.3 (2025-09) | 14–18 | libmysqlclient / mariadb-connector-c | R/W | WHERE, JOIN, agg, ORDER BY, LIMIT |
| tds_fdw | sqlserver | tds-fdw/tds_fdw | 2.0.5 (2025-09) | 13–18 | freetds-dev | Read-only | WHERE + column only |
| oracle_fdw | oracle | laurenz/oracle_fdw | 2.9.0 (2026-06) | 9.3+ (15–18) | Oracle Instant Client + SDK | R/W | WHERE, ORDER BY, same-server JOIN |
| jdbc_fdw | any JDBC (fallback) | pgspider/jdbc_fdw | 0.5.0 (2025-03) | 13–17 | JVM/JNI + driver JAR | R/W | WHERE, column, agg |
| clickhousedb_fdw | clickhouse | Percona-Lab/clickhousedb_fdw | untagged (PG≤13) | 11–13 | libclickhouseodbc / libcurl | R/W | agg, JOIN |

Notes:

- **mysql_fdw** covers SingleStore (MySQL wire-compatible). Put the MySQL/MariaDB client on
  PATH; test env `MYSQL_HOST`, etc.
- **tds_fdw** is read-only, no JOIN pushdown. SQL auth via username/password on the
  server/user-mapping; AD/Kerberos needs FreeTDS+GSSAPI. Set the TDS protocol version for
  Unicode correctness.
- **oracle_fdw**: Instant Client is Oracle-proprietary and **not redistributable** — the
  catalog cannot bundle it; the deployment supplies it and sets `ORACLE_HOME` +
  `LD_LIBRARY_PATH`. Build with those set.
- **jdbc_fdw** is the universal fallback (mssql-jdbc, ojdbc, clickhouse-jdbc), but embeds a
  **full JVM inside each PG backend process** — memory/startup cost, and connections originate
  from the server host IP. Set `JAVA_HOME` (needs `jni.h` + `libjvm.so`) to build.
- **clickhousedb_fdw** caps at **PG 13** — it will not build against pgserver's PG16/17 without
  patching. Prefer jdbc_fdw with clickhouse-jdbc for a modern PG, or route ClickHouse through
  Trino/DuckDB instead.

## Catalog: Iceberg / lakehouse (`iceberg`, `delta_lake` source types)

None of these is a classic single-`.so` FDW. Two architectures: embedded DuckDB in-process, or
a DuckDB sidecar speaking the PG wire protocol. All require a heavy C++/DuckDB (and sometimes
Rust) build.

| Extension | Repo | License | Arch | Iceberg | Feasible on embedded pgserver? |
|---|---|---|---|---|---|
| **pg_duckdb** | duckdb/pg_duckdb | MIT | Embedded DuckDB in-process | read + time travel | **Best drop-in** — single `.so`, no sidecar; still a large C++/DuckDB build |
| pg_lake | Snowflake-Labs/pg_lake | Apache-2.0 | `pgduck_server` sidecar (PG wire) | read + write + ACID | Needs a running sidecar daemon + very heavy vcpkg/DuckDB/Avro/GDAL build; Docker-only in practice |
| pg_mooncake | Mooncake-Labs/pg_mooncake | MIT | pg_duckdb read + Rust bgworker write | read + write | Needs a background worker; heavier than pg_duckdb; early (0.1.x) |
| pg_analytics | paradedb/pg_analytics | PostgreSQL | pgrx + embedded DuckDB | read | **Archived Mar 2025 — avoid** |
| DuckLake | duckdb/ducklake | MIT | Not a PG extension — PG is catalog only | via DuckDB | N/A: PG is a passive metadata store; the engine is DuckDB |

- **pg_duckdb** v1.1.1 (2025-12), PG 14–18. Embedded DuckDB, no sidecar. Iceberg/Delta read,
  Parquet/CSV/JSON read+write, S3/GCS/Azure/R2 via `duckdb.create_simple_secret(...)`. Build:
  `make PG_CONFIG=<bundled> install` (compiles vendored DuckDB — slow). No Iceberg write/ACID.
  This is the only realistic in-process `.so` for a bundled pgserver.
- **pg_lake** and **pg_mooncake** deliver Iceberg *write*, but each requires an accompanying
  running process, which breaks the single-process embedded model. Treat as out of scope for
  the v1 catalog.

## Alternative: multicorn2 — pure-Python FDWs

- Repo: **pgsql-io/multicorn2** (maintained fork; original Multicorn is dead). PostgreSQL
  License. v3.2, PG 14–18, Python 3.9–3.13.
- Compile `multicorn.so` **once** against the bundled `pg_config` (needs `python3-dev` + server
  headers); wrappers are then pure Python subclasses — **no per-source C build**.
- Lets you write **sqlite / parquet / iceberg wrappers in pure Python** (`sqlite3`, `pyarrow`,
  `pyiceberg`) with no Arrow/DuckDB C compilation.
- **Tradeoff:** data crosses the C↔Python boundary row-by-row — no columnar transfer, only basic
  OFFSET/LIMIT pushdown (v3.2), no join/aggregate pushdown. Fine for low-volume/ad-hoc
  federation; poor for large scans. Also incompatible with PL/Python under Python 3.12+.

This is the escape hatch when a native FDW is too painful to build for a triple: one compiled
shim, then Python wrappers, at the cost of scan performance.

---

## Recommendation for the Provisa engines

- **Prefer the DuckDB engine** for `parquet`, `sqlite`, `csv`, `iceberg`, `delta_lake`. It is a
  `pip install duckdb` with zero compilation and already implemented (REQ-894/895). The Postgres
  FDW path for these formats is compile-only and heavy.
- **Postgres engine ships working out of the box** with `postgres_fdw` (remote PG) and
  `file_fdw` (CSV) — no build step. This is what REQ-893 currently implements.
- **sqlite_fdw / parquet_s3_fdw / mysql_fdw / tds_fdw** are the realistic curated additions when
  a deployment wants everything inside one PG process. Each is a PGXS build against the pinned
  `pg_config` with a bundled native dep.
- **oracle_fdw** (Instant Client) and **jdbc_fdw** (embedded JVM) carry licensing / footprint
  costs — catalog them, but flag the constraints.
- **Iceberg write** and **ClickHouse on modern PG** do not fit the embedded single-process model
  cleanly; route them through the DuckDB or Trino engines instead.

### Chosen v1 catalog set

Five extensions, built per `(pg_major, platform, libc)` as separate wheels layered onto the
pinned base:

| Source | Extension | Build cost | Bundleable? |
|---|---|---|---|
| csv | file_fdw (core) | none — ships with PG | n/a (already present) |
| sqlite | pgspider/sqlite_fdw | libsqlite3 | yes |
| parquet / iceberg / delta | duckdb/pg_duckdb | vendored DuckDB (C++) | yes (heavy) |
| sqlserver | tds-fdw | freetds | yes |
| oracle | laurenz/oracle_fdw | Oracle Instant Client | **no — deployment supplies it** (not redistributable) |

**Parquet routes through pg_duckdb**, not a dedicated Parquet FDW. `parquet_s3_fdw` is dropped
from v1: pg_duckdb already reads Parquet (plus iceberg and delta) in one extension with S3/GCS/
Azure support, avoiding a second heavy Arrow+AWS-SDK build and its exact-pin ABI fragility. The
tradeoff is losing parquet_s3_fdw's write path and its `endpoint`/`use_minio` server options — if
a deployment needs Parquet *write* or fine MinIO endpoint control, revisit parquet_s3_fdw then.

Not in v1: `parquet_s3_fdw` (pg_duckdb covers parquet read), `mysql_fdw` (deferred),
`clickhousedb_fdw` (caps at PG13 — route via DuckDB/Trino), `jdbc_fdw` (embedded JVM),
`pg_lake`/`pg_mooncake` (require a sidecar/bgworker — break the single-process model).

Build-order notes for this set:

- **file_fdw** — no artifact; the wheel just documents `CREATE EXTENSION file_fdw`.
- **sqlite_fdw, tds_fdw** — lightest builds; a single system dev lib each. Start here to validate
  the CI triple harness.
- **pg_duckdb** — vendored DuckDB is a long compile; one `.so`, no sidecar. Covers parquet,
  iceberg, and delta read in a single extension — the one heavy C++ build in v1.
- **oracle_fdw** — build only; Instant Client + `ORACLE_HOME`/`LD_LIBRARY_PATH` are supplied at
  the deployment, never shipped in the wheel.
