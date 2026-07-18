# Airgap PyPI Mirror Preparation Checklist (REQ-1130)

Regulated/airgapped enterprises run a private PyPI mirror (Artifactory, Nexus,
`devpi`) that only serves whitelisted, CVE-scanned, hash-pinned wheels. Before
`pip install provisa[embedded]` can succeed offline, the mirror admin must
pre-seed every transitive dependency — with special attention to the
**native-wheel** packages below, which ship compiled C/C++/Rust extensions and
therefore have **platform-specific** wheels (not pure-Python `-any`).

## Prerequisites

- **Python interpreter pin:** `>=3.12,<3.13`. Provisa builds and runs on
  **CPython 3.12** only. Seed the **`cp312`** ABI wheels (e.g.
  `...-cp312-cp312-manylinux_2_28_x86_64.whl`, plus `macosx`/`win_amd64` for
  the platforms your operators run).
- **Lockfile:** [`requirements-embedded.lock`](requirements-embedded.lock) is
  the hash-pinned resolution of `provisa[embedded]` (base deps + the embedded
  extra) for Python 3.12. Regenerate with:

  ```bash
  uv pip compile pyproject.toml --extra embedded --python-version 3.12 \
      --generate-hashes -o packaging/pypi/requirements-embedded.lock
  ```

## One-pass mirror seed

Download every wheel named in the lock, for each target platform, then upload
to the private mirror:

```bash
# Run once per (platform, cp312) target you support.
pip download --require-hashes -r packaging/pypi/requirements-embedded.lock \
    --python-version 3.12 --only-binary=:all: \
    --platform manylinux_2_28_x86_64 -d ./mirror-seed
# → upload ./mirror-seed/*.whl to Artifactory/Nexus, then verify install:
pip install --require-hashes --no-index --find-links ./mirror-seed \
    'provisa[embedded]'
```

## Native-wheel packages requiring cp312 platform wheels

These carry compiled extensions — confirm a `cp312` wheel exists for **every**
operator platform before go-live (a missing platform wheel forces an sdist build
that airgapped hosts cannot complete):

| Package | Version | Notes |
|---|---|---|
| duckdb | 1.5.4 | Embedded federation engine (REQ-1129) |
| chdb | 4.2.1 | Embedded ClickHouse; **no Windows wheel** — gated off `win32` in pyproject.toml |
| pyarrow | 25.0.0 | Arrow-native transport |
| numpy | 2.5.1 | pandas/pyarrow/scipy transitive |
| scipy | 1.18.0 | Graph/analytics |
| pandas | 2.3.3 | Client dataframes / Arrow bridges |
| grpcio | 1.82.1 | gRPC transport |
| grpcio-status | 1.81.1 | Pinned `<1.82` for protobuf<7 compat |
| grpcio-tools | 1.81.1 | Proto codegen |
| grpcio-reflection | 1.81.1 | Server reflection |
| protobuf | 6.33.6 | Pinned `>=6.33.5,<7` |
| cryptography | 49.0.0 | Rust extension; TLS/encryption |
| bcrypt | 5.0.0 | Rust extension; password hashing |
| psycopg2-binary | 2.9.12 | Postgres driver |
| pyodbc | 5.3.0 | Synapse/Fabric ODBC |
| snowflake-connector-python | 4.7.1 | Snowflake engine (Arrow) |
| databricks-sql-connector | 4.3.0 | Databricks engine |
| clickhouse-driver | 0.2.11 | Native ClickHouse protocol |
| pymongo | 4.17.0 | Mongo (C extensions) |
| motor | 3.7.1 | Async Mongo over pymongo |
| jpype1 | 1.7.1 | JVM bridge (Iceberg/JDBC paths) |
| greenlet | 3.5.3 | SQLAlchemy async runtime |
| aiosqlite | 0.22.1 | SQLite control plane (embedded tier) |
| pydantic-core | 2.46.4 | Rust extension |
| rpds-py | 2026.6.3 | Rust extension (jsonschema) |
| orjson | 3.11.9 | Rust extension |
| cffi | 2.1.0 | FFI (cryptography/argon2/etc.) |
| charset-normalizer | 3.4.9 | Optional C speedups |
| hiredis | 3.4.0 | Redis C parser |
| lz4 | 4.4.5 | Compression |
| zstandard | 0.25.0 | Compression |
| websockets | 16.1.1 | Optional C speedups |
| wrapt | 2.2.2 | OTel instrumentation |
| thrift | 0.22.0 | Databricks/Hive transport |
| google-crc32c | 1.8.0 | GCS/BigQuery checksums |

## Bundled in the Provisa wheel (no separate seed)

- **buenavista** — the Provisa pgwire fork (`vendor/buenavista`, v0.5.0.post1)
  is **not** on PyPI. It ships as a top-level package **inside** the Provisa
  distribution (`[tool.setuptools.packages.find]` discovers it from a second
  root), so the mirror needs nothing extra for it. Its own runtime deps
  (`python-dateutil`, `sqlglot`) are folded into Provisa's dependency set and
  appear in the lock like any other wheel.

## Excluded by design (embedded tier)

- **No JVM / Trino server** — the embedded tier omits the bundled Trino JVM
  (PyPI carries no JVM server; avoids GPL/size/upload limits, REQ-1129). Full
  multi-engine federation stays available by pointing at a customer-provided
  external engine via `TRINO_HOST`/`TRINO_PORT`.
- **No npm / Node** — the React UI is precompiled into the wheel at
  `provisa/_ui/` (REQ-1127); nothing from the JS ecosystem is fetched at
  install or run time.
