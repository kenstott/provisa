# Fetch sources of a pip-installed Provisa

Rule (maintainer 2026-11-22): a pip-installed Provisa gets everything it needs, at install time and at
run time, from PyPI, Maven Central and npm only. Enterprise Artifactory proxies allow those three and
usually refuse GitHub release assets and GitHub Pages. The installers (DMG, AppImage, Windows,
install-host under `packaging/`) may use GitHub; they assume no firewall.

GitHub hosting of our own packages is a stopgap while we are locked out of PyPI. The `provisa` server
wheel also hit PyPI's 10 GB project size limit at v0.1.0-alpha.510 (`docs/releasing.md`).

`tests/unit/test_pip_path_no_github_fetch.py` fails when code on the pip path names a github.com,
github.io or githubusercontent.com host outside its explicit list. Each listed entry names the
registry move it waits for. Outside Python only URLs count: a bare `github.com/...` there is a Go
module path or protobuf `go_package`, resolved through the GOPROXY.

## Install time: `pip install 'provisa[embedded]'`

| What | From | Pip path | Registry home |
|---|---|---|---|
| `provisa` wheel | Pages index `kenstott.github.io/provisa/simple/` linking GitHub release assets | yes | PyPI (needs the size-limit increase or a smaller wheel) |
| `provisa-duckdb-ext` (non-core DuckDB extensions, Query.Farm crypto and airport) | Pages index, GitHub release assets | yes | PyPI |
| `provisa-notebook` (Pyodide), `provisa-client` | Pages index (`scripts/release_wheel_index.py` `PROJECTS`); `docs/cli.md` still says PyPI | yes | PyPI |
| `duckdb-extension-*` (postgres_scanner, httpfs, sqlite_scanner, iceberg, azure, delta, avro) | PyPI, third-party wheels | yes | PyPI already; later Provisa's own per-extension projects |
| `torch` (vector extra) | PyPI; Linux docs add `download.pytorch.org/whl/cpu` | yes | PyPI; the CPU index is a PyPI-protocol remote an Artifactory can proxy |
| every other declared dependency | PyPI | yes | PyPI already |
| `provisa-pg-ext` (FDW bundle, optional) | Pages index | optional | PyPI |

No declared dependency is a direct GitHub URL (the test checks `pyproject.toml`). The
`[tool.uv.index]` and `[tool.uv.sources]` tables are uv-only and do not reach the wheel's metadata.

## First run: `provisa run`

| What | From | When | Pip path | Registry home |
|---|---|---|---|---|
| DuckDB extensions | copied from the installed wheels into the data dir | every start (idempotent) | yes, offline | none needed |
| DuckDB extensions without the `embedded` extra | `extensions.duckdb.org`, community repository, `duckdb-extensions.query.farm` | first use of a connector | only without the extra | install the extra |
| UI | `provisa/_ui` in the wheel | served locally | yes, offline | none needed |
| Pyodide and its packages | `provisa-notebook` wheel | served locally | yes, offline | none needed |
| Default embedding model all-MiniLM-L6-v2 | `huggingface.co`, or `embedding.hub_endpoint` / `HF_ENDPOINT` | first embedding | yes | a Hugging Face mirror through the setting; not one of the three registries |
| Swagger UI on the REST docs page | browser loads `unpkg.com/swagger-ui-dist@5` | page view | yes, in the browser | bundle `swagger-ui-dist` from npm into the build |

## On demand: normal operation

| What | From | When | Registry home |
|---|---|---|---|
| pgwire connector bundles (file, SharePoint, Splunk and the rest), tarball and `.sha256` | `github.com/kenstott/calcite/releases/download/engine-v0.128.9/` | a source of that type is first used; pre-stageable per connector through its bundle-dir setting | Maven Central, published by the calcite engine release as per-platform classifier artifacts |
| Node | `nodejs.org/dist` | first JavaScript code source on Node | npm per-platform Node packages |
| Bun | GitHub release | first code source on Bun | npm `@oven/bun-<platform>` |
| Deno | GitHub release | first code source on Deno | npm `@deno/<platform>` |
| CPython for a Python code source | none: the interpreter Provisa runs on (the one pip installed it into, or the installer's bundled one) | never fetched | none needed |
| uv | the `uv` wheel, a declared dependency, from PyPI at install time | never fetched | PyPI already |
| CPython (python-build-standalone) for a remote host's image | GitHub release | first image built for a Python code source | open question |
| Go toolchain | `go.dev/dl` | first Go code source | open question |
| Rust toolchain | `static.rust-lang.org` | first Rust code source | open question |
| pyarrow wheel for the Python host | `files.pythonhosted.org` | first Python code source | PyPI already, but the URL is fixed |
| apache-arrow for the Node host | `registry.npmjs.org` | first JavaScript code source | npm already, but the URL is fixed |
| Java runtime for the sparql reasoner | the `jdk4py` wheel (`sparql` extra) from PyPI at install time | never fetched | PyPI already |
| sparql reasoner jar `io.simpleishard:provisa-sparql-reasoner` | `repo1.maven.org/maven2` | first sparql source with reasoning | Maven Central once promoted; until then it cannot be fetched |
| HermiT and OWL API jars | `repo1.maven.org/maven2` | first sparql source with reasoning | Maven Central already, but the URL is fixed |
| code-source dependencies (npm, PyPI, Go modules, crates, git) | the organisation's package registries | each code-source install | configurable mirror already |

A Python code source runs on the interpreter Provisa runs on, in a virtual environment uv makes for it;
only an image built for a remote host's platform takes the pinned python-build-standalone.

The reasoner jar is built from `provisa/sparql/reasoner/Reasoner.java` by
`.github/workflows/reasoner-maven-bundle.yml` with the pinned Temurin JDK's javac, which is fetched
from a GitHub release by that build and by the reasoner's tests, never by an install. The Temurin pin
stays in `provisa/runtime_deps/code_runtimes.py`, on the test's list, until
`io.simpleishard:provisa-sparql-reasoner` is promoted.

The code runtimes and reasoner jars use fixed public URLs. They do not go through the organisation's
configured package registries, so a site that reaches only its Artifactory fails even for the ones
already on PyPI, npm or Maven Central.

## Named but not fetched

| Reference | Why it stays |
|---|---|
| `github.com/kenstott/provisa/releases/download/pg-ext/...` in `provisa/pg_extensions/catalog.py` | quoted in an error message only; goes when `provisa-pg-ext` is on PyPI |
| Stripe spec on `raw.githubusercontent.com` | read by the build-time bake script; the wheel ships the baked spec |
| OpenLineage producer `github.com/provisa-dev/provisa` | an identifier in emitted events |
| `api.github.com` in `provisa/core/env_remote.py` and the GitHub GraphQL brand | services you choose to connect |
| UI links and vendored-file provenance comments | never fetched by the browser |
