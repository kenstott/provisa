# Code sources: one worker protocol, Node and Python (REQ-1984, REQ-1993)

Status: design approved by the lead on 2026-10-12, decisions in section 9. Node parts 1 to 3 are built; the rest follows section 7.

A code source is a source whose tables, commands and live tables are functions in code the organisation wrote. Node (REQ-1984) is the first; Python (REQ-1993) is the second. This note is the part both share, and the Python half. The Node note (docs/arch/node-connector.md) stays the Node half, and its sections 4, 5 and 7 move here where they are shared.

Every statement below that goes beyond REQ-1984 or REQ-1993 is marked *(proposed)*; the lead approved them with this design.

## 1. What is shared and what is per language

| Concern | Shared | Per language |
|---|---|---|
| The model of what code offers: exports, parameters, row types, named types, problems by name | `provisa/codesource/manifest.py` (today `provisa/node/manifest.py`, moved) | — |
| How a declaration says what it offers | — | Node: JSDoc tags and TypeScript declarations. Python: docstring tags and type hints |
| The type reader | its output format and rules (section 2) | the program that reads: `reader.mjs` (TypeScript compiler) / `reader.py` (section 6.2) |
| Content hash, uploads, Git fetch, the cache layout | `provisa/codesource/bundle.py`, `uploads.py`, `git.py` (today in `provisa/node/`, moved) | — |
| Package fetch and dependency install | the cache-by-hash and isolated-configuration rules | npm + lockfile (built) / PyPI + lockfile (section 6.3) |
| Runtimes, fetched pinned and hashed | the resolver (`runtime_deps/js_runtimes.py`, renamed `code_runtimes.py`) | the pins: Node, Bun, Deno / CPython (python-build-standalone) and uv |
| Organisation package settings | the admin page and its resolution (built for npm and Git) | a PyPI index kind (section 6.4) |
| The worker: process, socket, frames, messages, cancel, crash, pool | `provisa/codesource/worker.py` (section 3) | the host program: `host.mjs` / `host.py` |
| The sandbox | its rules and the OS mechanism (section 4) | Deno's own flags; everything else uses the OS sandbox |
| Tables, commands, live tables, parameters, landing, governance | parts 5 to 7 (REQ-1986, REQ-1980), unchanged | — |
| Source type | — | `SourceType.node`, `SourceType.python` |

Nothing in the shared layer names a language. A language is a `CodeLanguage` *(proposed)*: its source type, its runtimes, its reader command, its host command, its package fetch and install, and its lockfiles.

## 2. The reader's output (the manifest's input)

Each language's reader prints one JSON document, the same for both:

- `exports`: name, tags, description, signatures (parameters: name, optional, rest, type; returns: type);
- `types`: named object types with fields;
- `diagnostics`: the language's own errors in the declarations.

Types are the trees Node's reader already writes: `string`, `number`, `bigint`, `boolean`, `date`, `literal`, `union`, `array`, `object`, `ref`, `record`, `any`, `unknown`, `promise`, `iterable` (async, generator), `undefined`, `null`, `unsupported`.

Python's reader writes the same trees. Its mapping (section 6.2) adds three kinds: `int` is written as `number` with `integer: true`, so it maps to `bigint` rather than `double` (a Python int is an integer; Node's number stays double); `date` as `day`, which maps to date; and `Decimal` as `decimal`, which maps to numeric and travels as a string on the wire (decision P5). Node has neither, so its reader never writes them. The rules differ in two places only (`manifest.Rules`): Python has no standard `@` docstring tag, so every tag but Provisa's is unknown; and a Python `@stream` may be a plain generator as well as an async one (REQ-1993).

The rules in `manifest.read_manifest` apply unchanged to both languages: which exports are tables, commands and streams; a table returns a collection; a stream is an async generator (for Python, a generator or an async generator, per REQ-1993); columns from the row type or a preview for a loose row; every referenced type mapped; every problem by name, all at once.

## 3. The worker protocol

One long-running process per code source per Provisa process, in its runtime, started from the installed cache directory.

- **Transport:** a Unix domain socket in a private directory (mode 0700). Provisa listens; the host connects. A socket, not the process's stdout, so anything the code prints never reaches the protocol.
- **Frames:** a 4-byte big-endian length, then UTF-8 JSON.
- **Values:** JSON, with a timestamp as `{"$date": iso}`, a big integer as `{"$bigint": digits}`, a decimal as `{"$decimal": digits}`, a calendar date as `{"$day": iso}`, a missing value as null.
- **Messages:**

  | From | Message | Meaning |
  |---|---|---|
  | host | `hello {exports}` | the host loaded the code; these are its exports |
  | Provisa | `call {id, export, kind, args}` | `kind` is table, command, stream or columns; `args` in declared order (for columns, each input column a list of the batch's values, REQ-2131) |
  | host | `rows {id, rows}` | a batch (tables: up to 1000; streams: each row as it is yielded) |
  | host | `value {id, value}` | a command's single result |
  | host | `end {id}` / `error {id, message, stack}` | the call finished / failed |
  | Provisa | `cancel {id}` | stop the call; the host closes the iterator or generator |
  | either | `ping` / `pong` | health |

- **Lifecycle:** as Node's note section 7 has it. The process is started lazily, health is a ping, one that has exited is dropped so the next call starts another, it is stopped when its source is edited or deleted and when the app shuts down, and it exits when one of its sockets closes.
- **Calls:** each call leases one of the worker's 8 connections (`CONNECTIONS`) for its whole run, and a call that finds every connection leased waits for one. A call's host sends at most 4 rows frames (the call's `window`) ahead of the call's reader, and each frame the reader takes gives one back (`credit {id, n}`). A stalled reader therefore leaves its function at most 4 frames, plus the one taken and the batch the host holds, ahead of it, whatever the sockets between them buffer, and it holds back no other call. A built image's front end does the same on its own 8 connections to the host: it relays Provisa's credits to the host, and when it answers a statement itself it grants the host's credits as DuckDB takes rows and sends its answer as Provisa grants credits. A request's deadline sends `cancel`; a crash fails every call in flight, naming the source and the export.
- **Signature and read context:** a `@table` or `@stream` function takes `(inputs, context)`, either left out (`Export.takes`). Its parameters are the inputs type's fields, each an `_nf_` column. A call of it carries `takes` and `context`: the host makes the inputs object from `args` and `names` (Python, an instance of the annotated class, a field left out taking its default; Node, a plain object), then a `ReadContext` (`provisa_services.ReadContext`; `@provisa/services` in TypeScript, whose declarations the reader maps to `services.d.ts`). `residual.read_context` computes the context from the statement, for a direct read in Provisa and for an attached read in the image's pgfront; a landed read sends an empty one naming every column. A `@loader`'s batches each carry their own context, its `keys`. Neither reader reads inside Provisa's own types, so their names never meet the code's.
- **DuckDB bounds:** every statement over a code source's rows runs on one DuckDB instance per process (`residual.configure`), bounded by `codesource.duckdb_memory_mb`, `codesource.duckdb_threads` and `codesource.duckdb_spill_cap_mb`, with `preserve_insertion_order` off. A built image's front end gets them with the pairing message and with every call, and keeps them in `<state>/duckdb.json`, which it reads again when it starts. It spills into `/var/lib/provisa-spill`, a volume of its own. DuckDB 1.5.5 does not stop a sort's spill at `max_temp_directory_size`, so the cap holds where the operator sizes that volume to it. The resulting `No space left on device` is refused as `SpillCapped`, naming the setting. In dev mode Provisa's DuckDB spills into `<data dir>/code-spill`, unbounded.
- **Pool:** keyed by `(source, content hash)`.

`codesource/worker.py` is Provisa's side (`Worker`, `WorkerPool`, `HostCommand`); `host.mjs` (Node part 4) is Node's host. Deno is granted the socket as `--allow-net=unix:<socket>`. `provisa/python/host.py` is the same program in Python, built on asyncio (standard library only): it imports the code's module, says hello on each connection, and serves calls; a coroutine is awaited, a generator or async generator iterated (a sync one up to a batch of 1000 rows per thread hop), any other function run on a thread. A row is a dict, a dataclass, a NamedTuple or an annotated object; Arrow the code returns or yields (a Table, a RecordBatch or a RecordBatchReader) is sent as it is, cast to the call's schema.

## 4. The sandbox, for every runtime

The maintainer's ruling for Node applies to Python (REQ-1993): no file access beyond the code, and network only to the egress allow-list.

- **Deno:** its own permissions, `--allow-read` (the bundle and the socket directory), `--allow-write` (the socket only) and `--allow-net=<allow-list hosts>`.
- **Everything else** (Node, Bun, Python):
  - **Files:** the OS sandbox gives the process read access to its installed code directory, its runtime directory and the socket directory, and nothing else.
  - **Network:** the egress allow-list names hosts, and no OS sandbox filters by host name. So the process may connect only to the socket and to a local proxy Provisa runs, and the proxy admits a connection only to a host on the allow-list (HTTP CONNECT for TLS, and plain HTTP by host). The runtime is pointed at it: `HTTP_PROXY` and `HTTPS_PROXY` for every runtime, plus `NODE_USE_ENV_PROXY=1` for Node 24, whose built-in fetch otherwise ignores them. A program that opens a raw socket to any other address is refused by the OS sandbox, not by the proxy.
  - **Mechanism** (S1, lead 2026-10-12; `provisa/codesource/sandbox.py`):
    - macOS: `sandbox-exec` with a generated profile (`macos_sandbox.py`): `deny default`; read the code, the runtime, the host program and the system's libraries (and the root directory itself, which Node lists as it starts); write only `/dev/null`; exec only the runtime; connect only to the worker's socket path and to `localhost:<proxy port>`.
    - Linux: Landlock and seccomp, applied by a launcher (`python -m provisa.codesource.linux_sandbox`) that confines itself and then `exec`s the host. Neither needs a privilege, a user namespace or a capability, so they work under Docker's default seccomp profile and Kubernetes' `RuntimeDefault`; bubblewrap does not (it needs user namespaces, which both refuse).
      - Landlock (Linux 6.7 or later, ABI 4): read only the code, the runtime, the host program, the system's libraries and certificates and `/proc/<own pid>`; write only `/dev/null`; TCP connect only to the proxy's port; bind nothing. From ABI 6, abstract Unix sockets and signals outside the worker are refused too.
      - seccomp: `socket()` only for TCP over IPv4 or IPv6; `socketpair()` allowed (an unnamed connected pair reaches nothing; asyncio's event loop wakes itself through one, so a Python host cannot start without it); no io_uring (it opens sockets without `socket()`), no x32 calls on x86_64. So there is no UDP (no DNS around the proxy) and no Unix socket: Landlock does not mediate a Unix socket's connect, and a host's Docker or Postgres socket would otherwise be a path away.
      - The worker's socket is therefore handed to the host already connected, as an inherited descriptor (`fd:N`); the host reads and writes it with `node:fs`, because Bun's `net.Socket` does not take a descriptor.
    - Windows: no sandbox; a code source is refused there by name (S2).
  - **Proven by:** `.github/workflows/sandbox-probe.yml` runs the launcher's `--probe` on the runner, under Docker's default seccomp and under containerd's default profile (what `RuntimeDefault` applies); `tests/integration/test_node_worker.py` runs Node, Bun and Deno sandboxed (reads outside the code refused, the allow-list reached through the proxy and nothing else).
    - Run 38307119574 on main (2026-10-12): kernel 6.17.0-1022-azure, Landlock ABI 7. On the runner, under `docker run` (Docker's default seccomp) and under `ctr run --seccomp` (containerd's default, what Kubernetes' `RuntimeDefault` applies), every check held: reading `/etc/passwd` refused and the sandbox's own code read; writing `/tmp` refused; a TCP bind refused; TCP to another port refused and to the proxy's port allowed; UDP and Unix sockets refused (that run also refused `socketpair`; it is allowed since alpha.481's suite showed the Python host's event loop needs it).
    - The cloud's nodes boot `ubuntu-2204-lts`, whose GCP kernel is 6.8 (Landlock ABI 4 or later); a node's running kernel is the one its own upgrades installed.
  - **Missing sandbox:** a host where the mechanism is not available (no `sandbox-exec`; a kernel without Landlock ABI 4; a container profile that refuses `landlock_*` or `seccomp`) refuses to start a code source's worker, naming what is missing. It never starts one unsandboxed.

The egress allow-list is the deployment's `udf_egress_allowlist`, the one commands already use (REQ-885). Loopback is never reached by a code source, whatever the list says or a name resolves to (S3): Provisa's own ports are not a code source's to call. The proxy is `provisa/codesource/egress.py`, one per allow-list per process.

## 5. What changes in the Node work already built

All of it was a move, on the unmerged design/node-connector branch (done with Node part 4):

- `provisa/node/manifest.py` → `provisa/codesource/manifest.py`.
- `bundle.py` → `codesource/bundle.py`, and `uploads.py` → `codesource/uploads.py`. The `org_node_bundles` table becomes `org_code_bundles`, keyed `(org_id, content_hash)`.
- The Git fetch, the upload fetch, the HTTP and command runners and the cache by content hash in `install.py` → `codesource/install.py`; `node/install.py` keeps the npm fetch and install and calls it. The cache is `<cache>/code-bundles/<runtime>/<hash>`.
- `runtime_deps/js_runtimes.py` → `runtime_deps/code_runtimes.py` (`CodeRuntimeResolver`, cached under `<cache>/code/<name>/<version>`), with Python's pins beside Node's.
- `provisa/node/` keeps the Node-only parts: the npm fetch and install, `reader.mjs`, `reader.py`, `host.mjs`, and `worker.py` (how each runtime starts the host).
- The `BundleRefused` codes `node.*` become `codesource.*`.

The Node note keeps its decisions; it points here for the shared parts.

## 6. Python (REQ-1993)

### 6.1 What the code says

```python
from dataclasses import dataclass
from datetime import datetime

@dataclass
class RiskRow:
    account_id: str
    score: float
    computed_at: datetime
    reasons: list[str]

def churn_risk(account_id: str, as_of: datetime | None = None) -> list[RiskRow]:
    """Churn risk of one account.

    @table
    """
```

- **Tags:** a line of the docstring that is exactly `@table`, `@command` or `@stream` *(proposed: a tag is its own line, so prose that mentions "@table" is not a tag)*.
- **Description:** the docstring with its tag lines removed.
- **Unknown tags:** a line starting with `@` that is not one of the three is an unknown tag, refused by name *(proposed: Python has no standard docstring tag syntax beginning with `@`, unlike JSDoc)*.
- **Functions:** a module-level function only (REQ-1993); a method or a lambda is ignored.
- **Parameters:** a parameter without a default is required by the source; one with a default is not. `*args` and `**kwargs` are refused by name.
- **Rows:** a `@table` returns a collection: `list[T]`, `Iterable[T]`, `Iterator[T]`, `Generator[T, ...]`, `AsyncIterable[T]`, `AsyncIterator[T]`, `AsyncGenerator[T, ...]`, or an `async def` returning one of those. A `@stream` is a generator or an async generator (REQ-1993).
- **Named types:** a `dataclass`, `TypedDict`, `NamedTuple` or annotated class *(REQ-1993 names dataclass, TypedDict and annotated class; NamedTuple is proposed)*. Fields come from the annotations; `X | None` and `Optional[X]` are nullable; a TypedDict field that is `NotRequired` is optional.
- **Scalars:** `str` → text; `int` → bigint; `float` → double; `bool` → boolean; `Decimal` → numeric; `datetime` → timestamp; `date` → date *(proposed: the IR has date)*; `Literal["a", "b"]` → enum; `enum.Enum` with string values → enum; `dict[str, X]` → json; `Any` → json; `list[X]` → a list.

### 6.2 The reader

Python's annotations are not readable without importing the module: a type hint can name a type from another module or a dependency. So the Python reader imports the code, and **it runs in the same sandbox as the worker** (section 4). Importing the code is running it.

`provisa/python/reader.py` (standard library only) is shipped with Provisa and runs under the source's pinned Python, with the installed code on its path. It imports the entry module and:
- for each module-level function, reads its docstring with `inspect.getdoc` and its hints with `typing.get_type_hints(include_extras=True)`;
- writes the trees of section 2;
- writes an import error as a diagnostic: the code is refused, with the error.

**Entry module** (decision P1). The organisation's code is never changed for Provisa:
- the entry module is the code's single top-level import: for a wheel, from its `top_level.txt` or `RECORD`; for an upload or a Git subdirectory, its single top-level module or package;
- when there is more than one, the source registration form names the module, a field Provisa keeps with the source;
- when there is more than one and the form names none, the source is refused, naming the candidates.

Built in part P1 for a code directory (`provisa/python/source.entry_module`: setup.py, conftest.py and the like, test modules and `tests/` are not the code); the wheel's `top_level.txt` comes with the PyPI fetch in part P2. The reader and the host run sandboxed (`read_code`, `host_command`), the reader once per read with no worker socket.

### 6.3 Fetching the code and installing its dependencies

**The code.** One of three references; each is pinned, and the content hash is taken before any install:
- a PyPI package at an exact version, from the organisation's index for it, else the public PyPI;
- a Git repository at a commit, with a subdirectory: the shared Git fetch;
- an uploaded archive: the shared uploads store.

A package is fetched as its **wheel** for the pinned Python (`py3`/`cp312` tags, `none`/`abi3`/`cp312` ABI, `any` or this platform's `macosx_*_arm64` / `manylinux*_x86_64`), from the organisation's default index else the public PyPI, and checked against the index's published SHA-256. A release with no such wheel is refused: building an sdist runs its build backend, the Python counterpart of an install script (P2). The entry module is the wheel's `top_level.txt`, else the first part of each path its `RECORD` lists (P1). Built: `provisa/python/install.py`.

**The lockfile** (P4).
- A code with dependencies (a wheel's `Requires-Dist` without an extra, or a project's `[project].dependencies`) carries `uv.lock` (with its `pyproject.toml`), `pylock.toml` (PEP 751) or a `requirements.txt` with `--hash` on every requirement; one that does not is refused by name;
- one without dependencies needs none, and runs on the pinned Python itself.
- **A lockfile beside the code** (lead, 2026-10-12; widened from packages to Git and uploads 2026-10-20): a wheel carries no lockfile, most npm packages publish none, and Git or uploaded code may carry none, so the source's form may give one beside any code that carries none; it is kept with the source, put into the code before the content hash is taken, and hashed like the code (`codesource.install.Lockfile`). Without one, code that declares dependencies is refused, and the refusal says to add one. One beside code that carries its own (of the same name, or one the installer reads first) is refused by name. Every install stays frozen.

**The install.**
- `uv venv --python <pinned python> <install>/.venv`, then the lockfile into it: `uv sync --frozen --no-build --no-install-project` for `uv.lock`; `uv pip sync --no-build [--require-hashes] <file>` for `pylock.toml` and `requirements.txt`;
- the indexes reach uv in the install's environment only (`UV_DEFAULT_INDEX` for the default index, `UV_INDEX` for the others, each with its credential in its URL), never in a file; `UV_NO_CONFIG=1`, `UV_PYTHON_DOWNLOADS=never`, a HOME and a cache of the install's own, and Git dependencies through the same Git configuration as Node's;
- the result is cached by content hash (`<cache>/code-bundles/python/<hash>`), and a worker runs the venv's interpreter (`environment_python`); a project laid out with `src/` is entered there (`code_path`).

**The credential** of a PyPI index is a token, sent as PyPI's `__token__` user, or `user:password` as entered.

**The runtime** (P3): CPython 3.12.14 from python-build-standalone release 20260924 (`install_only`), and uv 0.12.18, pinned and hashed for the three platforms in `provisa/runtime_deps/code_runtimes.py`.

### 6.4 Package indexes

REQ-1993 asks for the organisation's package indexes, "like REQ-1984's registries". Built as a third kind, `pypi`, of the same table and admin page:
- **PyPI index:** a simple-index URL and a credential; one may be the default index every Python install goes through in place of the public PyPI. The mirror is per kind: one npm mirror and one default index per organisation.
- **No scope:** Python packages have none; a scope on a PyPI index is refused.
- **Same rules:** every field is kept as entered and resolved at use.
- **The page** "Package registries" has three lists: npm registries, PyPI indexes, Git hosts (13 locales; rendered at 1280 and 1900).

### 6.5 Tables, commands and live tables

Identical to Node's parts 5 to 7:
- the parameters are REQ-1986's, with the shared kind `query_arg`;
- landing per argument set through REQ-1980's branch and converter;
- commands use a new `impl_kind`, `code` *(proposed: one kind for every language, the binding naming the source and the export)*;
- each live-table subscription starts its own generator.

## 7. Sequence

1. **Shared layer** (this note's sections 1 to 5): move the Node parts, generalise the runtime resolver, and write `codesource/worker.py` with Node's `host.mjs`. This is Node part 4, built language-neutral.
2. **The sandbox** (section 4): the proxy, the macOS and Linux mechanisms, and Deno's flags. It is Node part 4b and serves every language.
3. **Python, part P1:** the reader and its mapping into the shared manifest, with `host.py`.
4. **Python, part P2:** runtime and uv pins, the PyPI fetch, the lockfile install, and the PyPI kind of package registries.
5. **Node parts 5 to 7, then Python's:** tables, commands and live tables. They are the same code paths and wait for REQ-1986 and REQ-1980 as before.

## 8. Tests

The same shape as Node's.

**Unit:**
- the reader's trees written by hand for the mapping rules;
- the worker protocol against a stub host written in Python;
- the sandbox profile and arguments as data.

**Integration (CI):**
- the real reader and the real worker under each runtime (Node, Bun, Deno, CPython), against a fixture package that is the same API in both languages, with the same manifest expected from each;
- the sandbox refusing a file read outside the code and a connection to a host off the allow-list, on Linux (CI) and on macOS (where CI has it).

## 9. Decisions (lead, 2026-10-12)

- **P1. Entry module.** The code's single top-level import; with several, the source registration form names it. No key in the organisation's code.
- **P2. sdists.** Wheels only.
- **P3. Python's runtime.** python-build-standalone, pinned and hashed.
- **P4. Python's lockfiles.** `uv.lock`, `pylock.toml`, or a hashed `requirements.txt`.
- **P5. `Decimal`.** Maps to numeric, sent as a string on the wire. Parity is a rule of query surfaces, not of languages: Node simply has no decimal.
- **`int`.** Maps to bigint.
- **S1. The sandbox for Node, Bun and Python.** An OS mechanism with a host-filtering local proxy for the allow-list: macOS `sandbox-exec`; Linux Landlock plus a seccomp filter for UDP, raw and Unix sockets, applied in the child before exec (bubblewrap was dropped: it cannot run under Docker's default seccomp or Kubernetes' `RuntimeDefault`). A host without it refuses by name.
- **S2. Windows.** A code source is refused on Windows, by name.
- **S3. Loopback.** Not reachable from a code source.
