# provisa-pg-ext

Pinned, per-platform PostgreSQL extension/FDW binaries for the
[Provisa](https://github.com/kenstott/provisa) embedded tier, delivered via PyPI.

## Why this package exists

The prebuilt PG extension bundles the embedded tier layers onto `pgserver` (`sqlite_fdw`,
`parquet_fdw`, `parquet_s3_fdw`, `pg_lake`, `pg_duckdb`, `pg_analytics`) are published as GitHub
release assets. Behind an enterprise firewall where only PyPI/Maven/npm/NuGet are proxied
(Artifactory), that host is unreachable and the out-of-the-box extension layer breaks.

This package embeds the pinned, per-platform binaries as ordinary wheel data so acquisition flows
through PyPI instead. One universal wheel carries every platform. Provisa's
`provisa.pg_extensions.catalog` stages the running platform's blobs from `provisa_pg_ext.ext_root()`
before pgserver's `CREATE EXTENSION`. Mirrors `provisa-duckdb-ext`.

This package is a dependency of Provisa's embedded tier — install `provisa` directly rather than this
package on its own.
