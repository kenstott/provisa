# provisa-duckdb-ext

Pinned, per-platform DuckDB extension binaries for the [Provisa](https://github.com/kenstott/provisa)
embedded tier, delivered via PyPI.

## Why this package exists

The extensions the embedded DuckDB engine loads (`sqlite_scanner`, `iceberg`, `delta`,
`postgres_scanner`, …) are not statically linked into the `duckdb` PyPI wheel — DuckDB fetches them
from `extensions.duckdb.org` at runtime. Behind an enterprise firewall where only PyPI/Maven/npm/NuGet
are proxied (Artifactory), that host is unreachable and the embedded tier breaks.

This package embeds the pinned, per-platform `.duckdb_extension` binaries as ordinary wheel data so
acquisition flows through PyPI instead. Each platform has its own wheel (macOS arm64, Linux x86_64
and aarch64, Windows x64), holding only that platform's extensions; pip picks the one for the machine.
There is no source distribution. Provisa's
`provisa.federation.duckdb_extensions.stage_bundled_extensions` copies the running platform's blobs
into a writable directory on first run and points `PROVISA_DUCKDB_EXT_DIR` at it.

On DuckDB 2.0 the `crypto` extension is published by Query.Farm in its own extension repository
(`query_farm`, prefix `https://duckdb-extensions.query.farm`), not in DuckDB's. It is mirrored here
like the others, in DuckDB's layout for a named repository
(`<version>/<platform>/repositories/query_farm/crypto.duckdb_extension` and its `.info`), and Provisa
loads it from the staged copy with `LOAD crypto FROM query_farm` — never from Query.Farm at runtime.

Extensions are locked to the exact DuckDB build they were mirrored from, so an incompatible `duckdb`
version can never silently load a mismatched extension.

This package is a dependency of Provisa's embedded tier — install `provisa` directly rather than this
package on its own.
