# provisa-duckdb-ext

Pinned, per-platform DuckDB extension binaries for the [Provisa](https://github.com/kenstott/provisa)
embedded tier, delivered via PyPI.

## Why this package exists

The extensions the embedded DuckDB engine loads are not statically linked into the `duckdb` PyPI
wheel — DuckDB fetches them from `extensions.duckdb.org` at runtime. Behind an enterprise firewall
where only PyPI/Maven/npm/NuGet are proxied (Artifactory), that host is unreachable and the embedded
tier breaks.

This package embeds the pinned, per-platform `.duckdb_extension` binaries as ordinary wheel data so
acquisition flows through PyPI instead. Each platform has its own wheel (macOS arm64, Linux x86_64
and aarch64, Windows x64), holding only that platform's extensions; pip picks the one for the machine.
There is no source distribution. The project is split across version parts (`<release>.<part>`), each
under PyPI's 100 MB per-file limit. Provisa's stage command (`python -m provisa.runtime_deps.stage_assets`)
fetches every part with `pip download` and lays the running platform's blobs out in the directory
`PROVISA_DUCKDB_EXT_DIR` names; do not install this package.

The core extensions (`postgres_scanner`, `httpfs`, `sqlite_scanner`,
`iceberg`, `azure`, `delta`, and `avro`, which `iceberg` loads) come from third-party PyPI wheels, one per extension and platform
(`duckdb-extension-*`). Each holds the official signed binary, and Provisa stages it beside this
package's.

This package carries what those wheels lack: `crypto` and the community extensions, assigned to the
parts by `PARTS` in `collect.py`. A part past the limit, or a mirrored extension no part names, stops
the build by name.

On DuckDB 2.0 the `crypto` extension is published by Query.Farm in its own extension repository
(`query_farm`, prefix `https://duckdb-extensions.query.farm`), not in DuckDB's. It is mirrored here
like the others, in DuckDB's layout for a named repository
(`<version>/<platform>/repositories/query_farm/crypto.duckdb_extension` and its `.info`), and Provisa
loads it from the staged copy with `LOAD crypto FROM query_farm` — never from Query.Farm at runtime.

Every extension keeps the `.info` DuckDB wrote when it was installed. DuckDB 2.0 trusts an
extension's signature by the repository that file names, so a community extension shipped without it
does not load (0.2.0 shipped them so; 0.2.1 carries them), and staging refuses such a package by name.

Extensions are locked to the exact DuckDB build they were mirrored from, so an incompatible `duckdb`
version can never silently load a mismatched extension.

Install `provisa` rather than this package on its own; the stage command fetches its parts.
