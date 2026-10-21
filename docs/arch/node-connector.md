# Node connector (REQ-1984)

Status: reviewed. The maintainer ruled on every open decision on 2026-10-12 (section 12); REQ-1984 and REQ-1987 carry the rulings. Parts 1 to 4 are being built.

REQ-1984 is the spec. A Node source holds a bundle: JavaScript plus TypeScript declarations. Its exported functions are read by their JSDoc tags:

- `@table`: a table. Its parameters are the table's parameters; its return value is the rows.
- `@command`: a command.
- `@stream`: a live table. Its rows are delivered as they arrive.

The source names its runtime (Node, Bun or Deno) and its code (an npm package, a Git commit or tag, or an uploaded archive). Package registries are an organization setting.

The work has eight parts. Parts 1 to 4 do not depend on grpc-fix's work and come first. Parts 5 to 7 reuse REQ-1986 (parameters) and REQ-1980 (pipeline-table landing) and come after them. Part 8 is REQ-1987, named types for every source, a part of its own.

| Part | What | Depends on |
|---|---|---|
| 1 | Package registries org setting | nothing |
| 2 | Install pipeline | 1 |
| 3 | Type reader | 2 |
| 4 | Worker | 2 |
| 5 | `@table` tables | 3, 4, REQ-1986, REQ-1980's landing branch |
| 6 | `@command` commands | 3, 4 |
| 7 | `@stream` live tables | 5 |
| 8 | Named types for every source (REQ-1987) | nothing; Node types use it once it lands |

Section 12 records the maintainer's rulings. Statements still marked *(proposed)* are implementation choices within them.

## 1. What exists today

| Piece | Where | Used for |
|---|---|---|
| Org-wide list setting in the platform plane, credentials kept as entered and resolved at use | `org_mail_platforms` (`core/schema_admin.py:800`), `core/mail_platforms.py` (`put` :232, `resolved` :134), `api/admin/mail_platforms_router.py` | Part 1, copied in shape |
| The standard resolution of a field (`${secret:…}`, `${env:…}`, `${user:…}`), failing closed | `core/secrets.py:34,137` (`resolve_secrets`); vault bound per read with `secrets_store.bound` | Part 1, every registry field |
| Delete protection for a secret a stored value names | `core/secret_references.py:17-21,230` searches every text column of org-scoped platform tables | Part 1, nothing to register |
| Org settings admin tab, gated by `org_settings` held in that org | `App.tsx:550`, `AdminPage.tsx:84,488`, `navGroups.ts:211`, `MailPlatformsSection.tsx`, `secrets_router._org_guard` | Part 1 UI |
| Pinned, hash-verified download cache | `runtime_deps/pgwire_bundles.py` (`BundleResolver`, cache under `$PROVISA_RUNTIME_DEPS_CACHE`) | Part 2 cache layout; the runtimes themselves (section 8) |
| Archive upload with zip-bomb and path-traversal guards | `import_shared/upload.py:38-81,126` | Part 2 uploads |
| Git fetch with a URL resolved at use | `core/env_ci.py:108-180` (dulwich) | Part 2 Git |
| A per-source long-running process, started lazily, dying with its parent | `federation/pgwire_replica.py` (spawn :621, health :655, `endpoint()` :1079, `--owner-pid` :730, `stop_all_servers` from `api/app.py:3166`) | Part 4 lifecycle |
| Parameter columns, one required rule for every source kind | REQ-1986, in progress: `core/parameters.py` (`PARAMETER_KINDS`, `missing_parameters`, `is_required`), `native_filter_required`, `required_by_steward` | Part 5 |
| Rows landed in the engine per argument set before the statement runs | `api/data/materialization.py:850` (`_materialize_api_to_engine_cache`), `api_source/engine_cache.cache_table_name`; REQ-1980 adds the pipeline branch | Part 5 |
| Strict argument conversion and output-column validation | REQ-1980, to live in one module (grpc-fix: `mongodb/arguments.py` or a general one) | Parts 5 to 7 |
| Preview-then-register for tables whose columns come from a run | `api/admin/_neo4j_registration.py:28`, `neo4j/source.py:85` (`infer_columns`), `_query_api_registration.py:73` | Part 5 |
| Command model and dispatch | `core/models.py:1593` (`Function`), `executor/function_dispatch.py:500` (`_EXECUTORS`), `api/data/action_exec.py:234` (`invoke_command`), `security/mutation_authz.py:165` (`admit_command`) | Part 6 |
| Push sources: a listener per table landing rows by key; per-row SSE with governed read-back | `events/push_wiring.py:126,634`, `api/data/subscribe.py:370,451`, `live/governed.py:119`, `subscriptions/base.py:15` | Part 7 |
| Nested object types on a json column | `Column.object_fields` (`core/models.py:815,856`), `compiler/schema_inputs.py:54` (GraphQL), `compiler/context.py:186` (SQL paths) | Part 3, as a first step only (section 3.4) |

Facts that bear on the design:

- **No JavaScript runtime ships with Provisa.** The Docker image is `python:3.12-slim`, and the PyPI tier's policy says "No npm / Node" (`packaging/pypi/MIRROR-CHECKLIST.md:97`). Section 8 covers this.
- **No kept, hashed upload store exists.** The only kept upload is the org logo, stored as bytes without a hash. Part 2 needs one.
- **The IR has no array, struct or enum type** (`core/ir_types.py:36-57`). Nested values reach surfaces only as a `json` column with `object_fields`. That shape is one level deep for gRPC sources, has no named or shared types, and has no list-of-object type on GraphQL. Section 3.4 covers what "become types on every surface" needs.
- **No command kind keeps a worker.** `script` (`function_dispatch.py:58`) starts one process per call.

## 2. Model

### 2.1 The source

`SourceType.node`. The source row keeps one per-type configuration object *(proposed: in the per-type JSON the source store already keeps; to confirm against `provisa_sources` during part 2)*:

| Field | Meaning |
|---|---|
| `runtime` | `node`, `bun` or `deno` |
| `code.kind` | `npm`, `git` or `upload` |
| `code.package`, `code.version` | npm: the package name and an exact version. A range or tag is refused at save. |
| `code.repository`, `code.ref`, `code.subdirectory` | Git: the repository URL, a commit or tag, and the bundle's directory within it |
| `code.upload` | upload: the stored archive's content hash |
| `code.commit` | Git: the commit the ref named when it was pinned *(proposed: a tag is resolved to its commit at save, and the commit is what is fetched)* |
| `content_hash` | The SHA-256 of the bundle's content, recorded at save (section 4.2) |
| `manifest` | What the type reader found (section 3.3), recorded at save |

The source has no credential field. Credentials come from the org's package registries (part 1).

### 2.2 Tables, commands and live tables

These are the existing models, with nothing new except the values listed here.

- **`@table`**: a registered table. Its `source_id` is the Node source and its `table_name` is the export's name.
  - Each function parameter is a REQ-1986 parameter column named `_nf_<name>`, with `native_filter_type = "query_arg"`, REQ-1986's shared parameter kind.
  - `native_filter_required` is true for a non-optional parameter, and false for an optional one, which the steward can make required with `required_by_steward`.
  - The data columns come from the return type, or from a preview (section 5.2).
  - The table description is the JSDoc text.
- **`@command`**: a `Function` with a new `impl_kind = "node"` and `binding = {"source_id", "export"}`. Its `arguments` come from the parameters and its `output_columns` from the return type. The description is the JSDoc text.
- **`@stream`**: a registered table with the same parameter and column rules as `@table`, marked as a stream *(proposed: by its source type and the manifest entry's tag, as push tables are marked by source type today; no new column)*.

## 3. Type reader (part 3)

### 3.1 What it reads

The reader reads the bundle's TypeScript declarations only. It never reads the bundle's JavaScript and never runs the bundle.

- The entry declaration file is the package's `types` (or `typings`) field, else `index.d.ts`. A bundle with neither is refused when it is loaded.
- A JavaScript bundle typed with JSDoc ships declarations generated from it (`tsc --allowJs --declaration`). This is the form REQ-1984 states: "A bundle is JavaScript plus TypeScript declarations carrying the tags".

### 3.2 How it runs

Provisa ships the reader as one pre-bundled JavaScript file containing the TypeScript compiler API at a pinned version (*proposed: `provisa/node/reader.mjs`, built when Provisa is built*). It runs under the source's own runtime, so a source needs no runtime other than the one it names. The reader prints one JSON manifest. Python reads the manifest; it does not parse TypeScript.

### 3.3 Manifest

```json
{
  "exports": [
    {"name": "orders_by_customer", "tag": "table", "description": "…",
     "parameters": [{"name": "customerId", "optional": false, "type": {"ref": "CustomerId"}}],
     "returns": {"collection": "async_iterable", "element": {"ref": "Order"}}}
  ],
  "types": {"Order": {"kind": "object", "fields": [{"name": "id", "type": {"scalar": "text"}, "optional": false}]}},
  "problems": [{"export": "x", "type": "Foo", "reason": "…"}]
}
```

The rules REQ-1984 states, applied by the reader:

- **Untagged exports** are ignored.
- **Unknown tags.** A tag other than `@table`, `@command` or `@stream` on an export is a problem naming the export and the tag.
- **`@table` returns a collection.** The return type must be an array or an iterable (`T[]`, `Array`, `ReadonlyArray`, `Iterable`, `AsyncIterable`, `Generator`, `AsyncGenerator`), alone or inside a `Promise`. A function that returns a single object is a problem naming it.
- **`@stream` is an async generator.** The return type must be `AsyncGenerator<T>` or `AsyncIterable<T>`. A declaration file shows only the return type, so the reader cannot tell an `async function*` from a function that returns an async iterable. Both are accepted.
- **Referenced types.** Every type a tagged function refers to (its parameters, its return type and every type they reference) is entered in `types`. A type that cannot be mapped is a problem naming it.

The bundle is refused when it is loaded if `problems` is not empty. The refusal lists every problem by name.

### 3.4 Type mapping

| TypeScript | Maps to |
|---|---|
| `string` | `text` |
| `number` | `double` |
| `bigint` | `bigint` |
| `boolean` | `boolean` |
| `Date` | `timestamp` |
| `T \| null`, `T \| undefined`, an optional property | `T`, nullable |
| `T[]`, `Array<T>`, `ReadonlyArray<T>` | a list of `T` |
| interface, object type alias, class instance type | a named object type with its fields |
| a union of string literals | an enum |
| `Record<string, T>`, `{[k: string]: T}` | `json` |
| `any`, `unknown` | `json` |
| other unions, functions, symbols, tuples, recursive types | cannot be mapped |

"Become types on every surface" is more than today's surfaces can show. Today a nested value is a `json` column with per-column `object_fields`. That gives one object type per column, no list of objects, no enum, no shared type names, and a JSON string on gRPC. The maintainer ruled for full named types for every source: REQ-1987, part 8. The reader emits the named types in `types` from the start; until part 8 lands, a Node table's nested values use today's `object_fields`.

### 3.5 Columns versus types

The maintainer ruled (REQ-1984 now says so): the columns come from the return type, or, when the element type is loose (`any`, `unknown` or a `Record`), from a preview. Every other type that cannot be mapped is refused by name when the bundle is loaded.

## 4. Install pipeline (part 2)

### 4.1 Getting the code

| Kind | How | Pinned by |
|---|---|---|
| npm | `npm pack <package>@<version>` in an isolated directory, through the org's npm registries (section 6) | the exact version, and the tarball's registry integrity (`sha512`) checked against the one recorded at save |
| Git | dulwich fetch of the recorded commit through the org's Git hosts; the subdirectory is the bundle | the commit |
| upload | the archive from the upload store, checked with the `import_shared/upload.py` guards | its content hash |

### 4.2 Content hash

The content hash is the SHA-256 over the bundle's files, in sorted path order, with each path and its bytes, *before* any install. It is recorded at save. A later load that finds different content refuses to run the source and names the source and both hashes.

### 4.3 Dependencies

The bundle must carry a lockfile the runtime reads (`package-lock.json`, `npm-shrinkwrap.json`, `bun.lock`, `bun.lockb` or `deno.lock`), unless it is a single pre-bundled file, which needs no install. Dependencies are installed in the runtime's frozen-lockfile mode:

| Runtime | Command |
|---|---|
| Node | `npm ci` |
| Bun | `bun install --frozen-lockfile` |
| Deno | `deno install --frozen` |

A lockfile out of step with `package.json` fails the install, and the failure names the source.

Most npm packages carry no lockfile (`npm publish` never includes `package-lock.json`; an author may publish `npm-shrinkwrap.json`), and Git or uploaded code may carry none either. So the source's form may give one beside any code that carries none (lead, 2026-10-12; widened from packages to Git and uploads 2026-10-20): it is kept with the source, put into the code before the content hash is taken, and so hashed like the code (`codesource.install.Lockfile`). Without one, code that declares dependencies is refused by name, and the refusal says to add one. One the runtime does not read is refused, and so is one beside code that carries its own: a lockfile of the same name, or one the installer reads first (npm reads `npm-shrinkwrap.json` before `package-lock.json`). Python's PyPI packages follow the same rule (docs/arch/code-sources.md, section 6.3).

Install scripts are disabled for every install (`--ignore-scripts` for npm and Bun; Deno runs none without `--allow-scripts`). A dependency that needs its script to build is refused, and the failure names it.

### 4.4 When, and where it is kept

- **When.** Installation happens when the bundle is loaded: at save, at boot, and on the first use in a process that has not loaded it. It never happens per call.
- **Where.** The cache directory is `<cache>/node/<runtime>/<content_hash>/`, written to a temporary directory and renamed into place when complete, so a half-finished install is never used. *(proposed: the cache root is the `runtime_deps` cache root.)*
- **Fleet.** Each host installs for itself from the pinned reference, and the content hash makes every host's copy the same.

### 4.5 Uploads

There is no kept, hashed upload store today. An uploaded archive is kept in the platform plane by its content hash: an `org_code_bundles` table keyed by `(org_id, content_hash)`, shared by every code source (docs/arch/code-sources.md) holding the archive's bytes. Deleting a source does not delete its archive while another source names it.

## 5. `@table` tables (part 5, after REQ-1986)

### 5.1 Registration

Discovery offers each `@table` export as a table to register, as other API-style sources do. Nothing is registered until a steward registers it.

Registration writes:
- one parameter column per function parameter (section 2.2);
- the data columns;
- the JSDoc description.

The REQ-1986 rules then apply unchanged:
- A source-required parameter is locked.
- A steward may make an optional parameter required.
- The table page's Parameters section shows each parameter.
- The one check, `parameters.missing_parameters`, refuses a read missing a required parameter before anything is sent. It runs ahead of every per-kind branch in `_materialize_api_to_engine_cache`, so the Node branch is covered without code of its own.

### 5.2 Columns

- **From the type.** Each top-level field of the element type is a column, with nested types as in section 3.4.
- **From a preview.** For a loose element type (`any`, `unknown`, `Record`), the function is called once in the worker with sample arguments. Up to five rows are returned, and columns are inferred as `neo4j/source.py:infer_columns` does. The form refuses to submit until the columns are typed. The sample arguments are not stored.

### 5.3 Reading

- **Routing.** A Node table is landed, never read in place. `node` joins `_MATERIALIZE_ONLY` (`federation/strategy.py:84`) and `_ADAPTER_FETCH_ONLY` (`events/source_loader.py:41`), and routes as a pipeline table does.
- **The branch.** One new branch in `_materialize_api_to_engine_cache`, plus the same lookup in `would_materialize_optimize`. It:
  1. converts each argument strictly with REQ-1980's converter, refusing a value that does not convert by naming the table, the parameter and the type;
  2. calls the export in the worker (section 7);
  3. validates each row against the columns with REQ-1980's output validation;
  4. lands the rows with `land_api_cache` under `cache_table_name(source_id, "node:" + table_name, args)` and the table's `cache_ttl`.
- **Argument order.** Parameters are passed to the function in declared order. An absent optional parameter is passed as `undefined`, so the function's own default applies.
- **No-parameter tables.** A table with no parameters has a constant key. It behaves as a view with a TTL and is replicable, as a no-argument pipeline table is.

### 5.4 Governance and surfaces

A Node table is a registered table: visibility, masks, row rules and domains apply to the landed rows, and parameter columns are not data. It is served on every surface a pipeline table is (GraphQL field arguments, SQL `_nf_` conditions, and the rest). REQ-1980's test plan covers each surface, and this one copies it.

## 6. Package registries (part 1)

### 6.1 Model

*(proposed)* An `org_package_registries` table in the platform plane, keyed by `org_id`, in the shape of `org_mail_platforms`. It is one per organization and outside every environment, as REQ-1984 says: "an organization setting".

| Column | npm registry | Git host |
|---|---|---|
| `id` | yes | yes |
| `kind` | `npm` | `git` |
| `url` / `host` | the registry URL | the host |
| `scope` | optional, e.g. `@acme` | — |
| `credential` | the token | an HTTPS token |
| `mirror` | at most one npm row is the mirror | — |

Every field holds what was entered, so a value can be a secret reference (`${secret:npm_token}`). Nothing is written to a vault and no secret name is made up. Delete protection for a referenced secret already covers platform tables with nothing to register.

### 6.2 API and page

- **API.** `GET`, `PUT` and `DELETE` on `/admin/orgs/{org_id}/package-registries`, guarded as Mail Platforms is: `org_settings` held in that organization. `cross_org` and `platform_settings` do not open it.
- **Page.** A Package registries tab on the organization settings page, built from the Mail Platforms section, with `FormLayout`. It has two lists, npm registries and Git hosts, and the mirror is a checkbox on an npm row.

### 6.3 Use

Every install resolves the rows with the standard resolution, with the org vault bound for that install only (`mail_platforms.resolved` is the model). It then writes the runtime's configuration into the install's isolated directory:

| Runtime | Configuration |
|---|---|
| Node | `.npmrc` with `registry=`, `@scope:registry=` and `//host/:_authToken=` |
| Bun | `bunfig.toml` with the same entries |
| Deno | `DENO_AUTH_TOKENS` in the install's environment, and an `.npmrc` for npm specifiers |

- **The mirror.** When an npm row is the mirror, it is the default registry, and every unscoped package goes through it.
- **Git.** Git hosts supply the credential for a Git source's fetch, and for Git dependencies in a lockfile.
- **Cleanup.** The configuration is removed when the install ends, whether it succeeded or failed. The cache keeps no configuration, and the worker's environment has no credential.
- **The public npm registry** needs no row.

## 7. Worker (part 4)

- **One per bundle per Provisa process.** A long-running process runs per `(source, content_hash)` in each Provisa process, in the source's runtime, started from the installed cache directory.
- **Host script.** Provisa ships a small host script, the same one for every runtime (`provisa/node/host.mjs`; its protocol is every code source's, docs/arch/code-sources.md section 3). It imports the bundle's entry and serves calls. The bundle imports nothing of Provisa's.
- **Lifecycle.** The lifecycle copies `pgwire_replica`:
  - The worker starts lazily on first use.
  - It is spawned in its own session with `--owner-pid`, so it dies with its parent.
  - It is health-checked by a ping on the socket.
  - A worker that has exited is dropped, so the next call starts a new one. There is no restart loop.
  - It stops when its source is edited or deleted, and when the app shuts down.
- **Protocol.** A Unix domain socket in a private directory (mode 0700). Messages are length-prefixed JSON, multiplexed by call id:

  | Message | From | Purpose |
  |---|---|---|
  | `call {id, export, kind, args}` | Provisa | `kind` is `table`, `command` or `stream` |
  | `rows {id, rows}` | worker | a batch of rows |
  | `end {id}` | worker | the call is complete |
  | `error {id, message, stack}` | worker | the call failed |
  | `cancel {id}` | Provisa | stop the call |
  | `ping` / `pong` | both | health check |

- **Iteration.** The host iterates arrays and sync or async iterables in batches.
- **Deadlines.** A call registers with the request deadline (`request_deadline.cancel_on_deadline`), so a cancelled request sends `cancel`. The host then calls `return()` on the iterator.
- **Failures.** A worker crash fails every call in flight with an error naming the source and the export.
- **Sandbox.** The worker reads no files beyond its bundle (its installed cache directory) and reaches only the hosts on the egress allow-list.
  - Deno: `deno run --allow-read=<bundle dir> --allow-net=<allow-list hosts>` and no other permission; the socket directory is added to `--allow-read` and `--allow-write` for the protocol socket only.
  - Node and Bun: an OS-level sandbox around the process. *(proposed: on Linux, a mount namespace with the bundle directory read-only and nothing else of the host's filesystem, and a network namespace whose only route out is a local proxy that admits the allow-list hosts; on macOS, `sandbox-exec` with a profile allowing reads under the bundle directory and outbound connections to the allow-list hosts only. A host where neither is available refuses to start a Node or Bun worker, naming the source and the missing sandbox.)*
  - The egress allow-list is the one commands already use (`function_dispatch.py:317`).

## 8. Shipping the runtimes

Runtimes are fetched on demand, pinned and hashed, and cached as the pgwire bundles are: a `runtime_deps` resolver beside `pgwire_bundles.py` names each runtime's version and per-platform archive hash, downloads on first use, verifies the hash, and caches under the `runtime_deps` cache root. A hash mismatch refuses the runtime. No package (Docker, AppImage, macOS, Windows, NixOS, PyPI) carries a runtime; the PyPI policy line changes from "no Node" to "no Node in the wheel".

## 9. `@command` and `@stream`

### 9.1 `@command` (part 6)

- **Execution.** A new executor in `_EXECUTORS`, `impl_kind = "node"`, calls the export in the worker.
- **Arguments** are converted as in section 5.3.
- **Output.** A collection return gives rows; a single object gives one row; `void` gives none. Each is checked against `output_columns`.
- **Everything else is the existing command path:** `invoke_command`, `admit_command`, approval, audit, and every command surface.
- **Query or mutation.** A command is a `mutation` unless a steward sets it as a `query` when registering it.

### 9.2 `@stream` live tables (part 7)

The maintainer ruled: each subscription starts its own generator with that subscriber's arguments, its rows go only to that subscriber, governed by the subscriber's role, and no key is required.

- **Surfaces.** The per-row SSE subscription (`/data/subscribe/{table}`) and the Kafka sink deliver each row as it is yielded. A GraphQL subscription over SSE sends each row as an event rather than a re-run snapshot for a stream table.
- **Governance without a key.** Today's push path reads each row back by primary key through governance (`live/governed.py`). A stream row is governed in flight instead: the subscriber's role context (column visibility, masks, row rules) is applied to each row before it is sent *(proposed: by evaluating the governed statement the subscription compiles over a one-batch relation of the yielded rows, so the same compiled rules apply as for a read; to be designed in part 7)*.
- **Lifetime.** The generator starts when the subscription opens and is closed (`return()` through the protocol's `cancel`) when it ends. A worker crash ends every subscription on that worker with an error naming the source and the export.
- **Parameters.** The same REQ-1986 rules: a required parameter missing from the subscription is refused before the generator starts.

## 10. Sequence

1. **Part 1** (registries setting) and the upload store.
2. **Part 2** (installer, with scripts disabled) and the runtime resolver.
3. **Part 3** (reader) and **part 4** (worker, with its sandbox), in parallel.
4. **Part 5** (`@table`), after REQ-1986 has landed and REQ-1980's landing branch and converter exist. If REQ-1980 lands later, the Node branch waits for its converter rather than building one.
5. **Part 6** (`@command`).
6. **Part 7** (`@stream`).
7. **Part 8** (named types, REQ-1987), as its own part; the Node type reader's `types` feed it when it lands.

Each part ships when it passes its own gate. The docs and the source-type counts (`docs/sources.md`, the tour's count, the source picker) are updated with part 5, when the first Node table can be registered.

## 11. Tests

**Unit** (`tests/unit`, no container, no network):
- Reader: fixture declaration files for every row of the mapping table, every refusal (unknown tag, single-object `@table`, unmappable type, missing declarations), and JSDoc descriptions. These need the pinned reader and one runtime on the unit lane, fetched by the runtime resolver (section 8).
- Content hash: stable across file order, and changed by any byte.
- Registries: as-entered storage, resolution at use, one mirror at most, the guard, and the configuration files written and removed (including on a failed install).
- Worker protocol: framing, multiplexing, cancel on deadline, and a crash failing the calls in flight. These use a stub worker that speaks the protocol.
- Parameters and landing: the `query_arg` kind, `native_filter_required` from optionality, the cache key per argument set, and `would_materialize_optimize` agreeing with the branch.

**Integration and e2e** (CI only, never run locally). An isolated compose project on unique, unexposed ports, with a private npm registry (Verdaccio) and a Git server. For each runtime:
- an npm source through a scoped private registry, and through the mirror;
- a Git source at a tag;
- an uploaded archive;
- a `@table` with a required and an optional parameter, joined in SQL to a Postgres table;
- the same table over GraphQL;
- a missing required parameter, refused;
- a mask and a row rule on Node rows;
- a `@command`;
- a `@stream` subscribed over per-row SSE;
- a changed lockfile refused by the frozen install;
- a changed archive refused by the content hash.

UI: the Package registries tab, and the Register Table form on a Node source.

## 12. Decisions (maintainer, 2026-10-12)

1. **Types on every surface.** Full, shared, named types for every source (nested objects, lists of objects, enums) as GraphQL types and gRPC messages, not only for Node. Its own requirement (REQ-1987) and its own part (part 8).
2. **Preview versus refusal.** A preview only for loose element types (`any`, `unknown`, `Record`); every other unmappable type is refused by name. REQ-1984 amended.
3. **Git credentials.** HTTPS tokens (recommendation accepted).
4. **Install scripts.** Disabled for every install.
5. **Upload store.** The platform plane, keyed by content hash (recommendation accepted).
6. **`number`, `any`, `unknown`.** `double`; `json` (recommendation accepted).
7. **Worker scope.** One per bundle per Provisa process (recommendation accepted).
8. **Runtimes.** Fetched on demand, pinned and hashed, cached as the pgwire bundles are.
9. **A command's kind.** `mutation` unless a steward sets `query` (recommendation accepted).
10. **Streams.** One generator per subscription, with that subscriber's arguments; rows to that subscriber only, governed per role; no key required.
11. **Isolation.** The worker is sandboxed: no file access beyond its bundle; network only to the egress allow-list (Deno's permissions; an OS-level sandbox for Node and Bun).
