# provisa-duckdb-ext

Pinned, per-platform DuckDB extension binaries for the [Provisa](https://github.com/kenstott/provisa)
embedded tier, delivered via PyPI.

## Why this package exists

The extensions the embedded DuckDB engine loads (`sqlite_scanner`, `iceberg`, `delta`,
`postgres_scanner`, …) are not statically linked into the `duckdb` PyPI wheel — DuckDB fetches them
from `extensions.duckdb.org` at runtime. Behind an enterprise firewall where only PyPI/Maven/npm/NuGet
are proxied (Artifactory), that host is unreachable and the embedded tier breaks.

This package embeds the pinned, per-platform `.duckdb_extension` binaries as ordinary wheel data so
acquisition flows through PyPI instead. Each platform has its own wheel. One platform's full set is
136-208 MB compressed and PyPI's per-file limit is 100 MB, so a wheel carries what fits, in the order
of `collect.py`'s `WHEEL_PRIORITY`: first what every tier needs (`postgres_scanner`, `sqlite_scanner`,
`httpfs`, `azure`, `iceberg` with `avro`, `vss`, `inet`, `crypto`), then the warehouse and SaaS
connectors. `collect.py` prints what each platform's wheel leaves out.

The rest are staged once, on a connected machine, into the directory `provisa run` uses:

```bash
python -m provisa.federation.stage_duckdb_extensions
```

The Provisa installers and container image hold every extension, so only a `pip install` needs that
step. Provisa's `provisa.federation.duckdb_extensions.stage_bundled_extensions` copies the running
platform's blobs into a writable directory on first run and points `PROVISA_DUCKDB_EXT_DIR` at it.

Extensions are locked to the exact DuckDB build they were mirrored from, so an incompatible `duckdb`
version can never silently load a mismatched extension.

This package is a dependency of Provisa's embedded tier — install `provisa` directly rather than this
package on its own.
