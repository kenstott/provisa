# Work in progress and new requirements — October 2026

Status as of the `0911799a` build (2026-10-01). Each section names the requirement that
holds the full text in [requirements.yaml](requirements.yaml); this page is the map, not
the specification.

Two kinds of entry appear below. **New requirements** were decided during the performance
work and have little or no code yet. **Work in progress** has code in the build that
covers part of its requirement.

## Summary

| Area | Requirement | In the build | Still to build |
|---|---|---|---|
| Config sync across processes | REQ-1914 | Operator settings and org overrides reach every worker | The config stamp, reload on change, object-level concurrency check |
| Data replicator | REQ-1915 | Engine-side copy for the pg engine with a Postgres source; write-target guard | One component for every whole-table copy; central state; eager builds |
| Row-level tables | REQ-1915, REQ-1865 | A read must bind the key | A read may use any filter |
| Process roles | REQ-1916 | — | Query node and coordinator roles |
| `replicate` setting | REQ-826, REQ-238 | — | Integer setting, hot threshold, UI drop-down |
| Replica address layout | REQ-1912 | Write-target check | Replicas and materialized views in their own schemas |
| Operator settings | REQ-1913 | Registry, catalog API, 124 settings, admin tab | Remaining cards, live telemetry, launch entry |
| Request timeouts | REQ-1905 | Per-transport values, API and UI card | — |
| Benchmark and tuning tool (one tool) | REQ-1911 | Contract, knobs and report against fakes | Proof against a running stack; measured guide |
| Terminology | REQ-826 | Requirements use the new terms | Code and UI strings |

## Config sync (REQ-1914)

A change to the model or its governance must take effect in every worker of every
instance that shares a control plane.

**Config stamp.** The control plane holds one stamp for the model and its governance.
Every change writes a new stamp in the same transaction as the change. The control plane
assigns the value, so it is one stored value that every process reads; no process compares
it with its own clock.

**Reload.** Each process that holds the model remembers the stamp it loaded, reads the
stored stamp on a short interval, and reloads when the two differ. A request reads nothing
from the control plane for this. The interval is an operator setting.

**Other reloadable things.** Operator settings applied live in each worker, the telemetry
exporters, and the naming and remote-GraphQL settings follow the same pattern, each with
its own stamp, so a change of one kind does not reload another.

**Concurrency check.** The stamp is used strictly for reloading. Concurrency is checked at
the level of the object: each editable object carries its own version, the editor sends
the version it read, and the control plane applies the update only when the stored version
still matches. A mismatch is refused with a conflict error and nothing is written. An
update that sends no version is refused.

## Data replicator (REQ-1915)

`data_replicator(source, target, engine)` produces and runs every whole-table copy of a
source table into an engine store.

**Version 1** formalizes and centralizes the replication code that exists: the API-source
path, the general source path, and the engine-side copy added for the pg engine. It has one
default method — a read loop that yields bounded batches and a write loop that fills a
fresh table and swaps it in — and the engine-side copy where the engine can reach the
source.

**Rules the component must hold to:**

- Memory does not grow with the size of the table.
- At most one copy of a table runs across all workers and instances.
- A reader never sees an empty or half-filled replica; a copy that dies leaves the previous
  replica intact.
- Replication always streams: Arrow record batches where the driver has them, a cursor
  fetched in bounded batches otherwise.
- The engine performs the copy and the coordinator orchestrates it. Where the engine cannot
  reach the source, the coordinator extracts a stream into the engine's own loader.
- The choice of method is a pure decision from the declared capabilities of source, target
  and engine, testable as a table without running any of them.

**Eager builds.** A replica starts building when the decision to replicate takes effect —
an operator saving the setting, a boot that finds no replica, a table crossing its hot
threshold — and not on the first query. A read that finds no replica and no build still
starts one, as a backstop.

**Central state.** The control plane holds one record per replica: its state, rows copied,
last completion, next refresh, last error, and the process holding the build. The
coordinator is the only thing that starts a build or a refresh.

**Later versions** add methods one at a time as declared capabilities: asynchronous jobs
on Trino, Databricks and Snowflake, native bulk loads, stages.

**Large tables.** A whole-table replica is the wrong tool for a large table. The
recommendation is row-level replication, scheduled materialized views of the queried
subset, or both.

## Row-level tables (REQ-1915, REQ-1865)

Row-level replication is outside the data replicator. A request fetches the rows it needs
from the source and writes them into the row-level replica itself.

- **In the build:** a read of a row-level table, on an engine that cannot attach its
  source, must bind the table's key or join it on its key. Anything else is refused at
  planning.
- **Target:** the read must filter the table; any predicate counts. The request pushes the
  filter to the source, replicates the matching rows, and records the filter so the same
  one is served from the replica until its TTL passes. A read with no filter is refused,
  because the table is replicated row by row for a reason: it is large.

## Process roles (REQ-1916)

The same code runs in two roles chosen at start. A **query node** serves requests on every
transport and runs no background work. A **coordinator** serves no data requests; it runs
every replica build and refresh and the scheduled work that query workers elect one of
themselves to run today. One coordinator starts on the same host by default and may run on
its own server.

## The `replicate` setting (REQ-826, REQ-238)

`prefer_materialized` becomes `replicate`, an integer per table with the source's value as
the inherited default.

| Drop-down entry | Value | Meaning |
|---|---|---|
| Default | not set | The global threshold applies |
| Never | -1 | Live wherever a live path exists (best effort) |
| Hot-50 … Hot-10000 | 50 … 10000 | Replicated once the table passes that many governed statements per interval (best effort) |
| Always | 0 | Reads come from the replica (the only guarantee) |

A warm table is this same mechanism: a table treated as Always once it crosses its
threshold. Automatic promotion needs a Redis shared by all workers.

## Replica address layout (REQ-1912)

Replicas and materialized views are each written to a schema that holds nothing else. A
read is served from a replica by addressing it where it lives, on every engine alike; a
source that is always replicated has no live attach registered. The build checks that a
write target is an ordinary table; checking that it sits in the replica schema comes with
the layout.

## Operator settings (REQ-1913)

Every operator setting is readable and editable in the admin UI. The build has the
registry, the catalog API with its platform-administrator gate, 124 declared settings, and
the Deployment settings tab. Remaining: telemetry exporter settings applied live on every
worker, the provisioning and multitenancy cards, the first-user claim for external
identity providers, and a single launch entry that resolves worker count, port, TLS and
role from stored settings.

## Benchmark and tuning tool (REQ-1911)

The benchmark and the tuning tool are one tool. With every knob at zero it is the
optimistic benchmark — one row, one column, no filter. With knobs set it sizes and tunes a
deployment. Each knob is a distribution and a probability, settable per transport: fields,
filters, rows, joins, cache opt-in and repetition, source selection, route, and the
replication setting a run is measured under. Its setup is one contract file naming the
deployment and the tables; transport-specific names are resolved from the deployment.

A run produces a sizing and tuning guide for a solution engineer or a customer: workload
in, physical cores and workers out, with the setting behind each recommendation. Published
figures are the zero-knob run on the expected language and transport pairs; knob runs are
for conversations about a specific workload.

The contract, the knobs and the report exist and pass against recorded responses. They have
not yet run against a live stack.

## Performance validation: the end state

Performance validation ends with one proper benchmark, suitable for publication. The steps
before it exist to make that run correct the first time: small local jobs that prove each
code path, then a cheap VM to prove it runs on a VM, then the published run, briefly, at
scale.

What makes the final run publishable:

- **One methodology, written down before the run.** The zero-knob request on the expected
  pairs: SQL over pgwire (cached and uncached, side by side), SQL over Arrow Flight,
  GraphQL over HTTP, Cypher over Bolt, gRPC, REST and JSON:API.
- **Separate machines.** Provisa alone on the reference server; sources and the load
  generator each on their own. A single shared machine measures the neighbours as well.
- **Reference hardware stated in physical cores:** 16 cores on a current-generation
  machine, with the chip, clock speed and threads per core read off the machine.
- **Repeatable.** The contract file, the commit, the worker count and the client
  concurrency are published with the figures; the run is repeated and the spread reported,
  not a single best number.
- **Warm-up and steady state** separated; throughput reported with median and p99 latency
  at the same point, and errors counted.
- **Conditions stated beside every figure:** query shape, cache state, engine and source,
  tracing on with the collector off the box, and that real workloads with filters, joins
  and larger results cost more.
- **Raw results kept,** so every published number can be traced to its run.

Anything measured before that run — the sizing sweep, local timings, projections — is
working data and is not published.

## Performance items still open

- No transport has been measured on a server since the per-request fixes.
- CPU per request rose 30–60% between one client and the peak on the earlier build; the
  cause is not established.
- REST, JSON:API, Flight SQL and MCP have no measurement after the fixes.
- One shared deadline watchdog thread would replace a timer thread per request.

## Terminology

A copy of a source table is a **replica**; making or refreshing it is **replication**.
These replace "materialized", "landed" and "landing" in that sense. A **materialized view**
keeps its name. The requirements use the new terms; code identifiers and UI strings follow
in a separate pass.

## Close-out work that every item above carries

Building a feature does not finish it. Each entry in the summary table also needs the three
things below before its requirement can be marked complete.

### Documentation

No user or operator documentation was written for anything on this page. To write:

- **Operator guide:** the Deployment settings tab and what "restart required" and "pending
  restart" mean; per-transport request timeouts and the shipped Flight and pgwire values;
  the request-thread bound; multi-worker launch and the per-worker listener.
- **Replication guide:** the `replicate` setting and its drop-down, what Never, Hot and
  Always guarantee, the large-table recommendation, row-level tables and their filter
  rule, what a reader sees while a replica is being built.
- **Deployment guide:** control plane and data plane, query node and coordinator roles,
  horizontal scaling, what must be highly available.
- **API reference:** the body-role rule (header only), the settings catalog routes and
  their error shapes, the row-level and replica-building errors, timeout errors that name
  the transport.
- **Sizing and tuning guide:** generated from a benchmark run; the hardware it was
  measured on stated in physical cores.
- **Terminology:** replica and replication in every document that says materialized or
  landed for a copy of a source table.

### Tests

The `0911799a` build has 33 known failing tests. They are to be fixed, not left:

| Count | Tests | What is wrong |
|---|---|---|
| 11 | pgwire integration | Test doubles take the old number of arguments |
| 3 | Arrow Flight integration | Fixture connects no audit database |
| 10 | Debug-trace span counts | Pass alone; an earlier test file in the same process leaves something emitting spans |
| 2 | Debug-trace detail | Statement text missing on GraphQL over Flight; a window opened on one instance not seen on another |
| 1 | Flight cached read | Arrow schema differs between the first read and the cached one |
| 1 | Per-transport timeout, multi-worker | Needs its own Postgres; fails under the shared stack |
| 2 | Source created through the admin API | The defect it documents is open |
| 3 | Replica enforcement on Trino | The defect they document is open (REQ-1912) |

Test work still owed by the items on this page:

- Real-service runs for the pooled Databricks, Fabric and Synapse, Hive and Exasol drivers,
  which have only run against stand-ins.
- The settings catalog's multi-worker integration test, which has one case that has never
  run.
- End-to-end tests for the Deployment settings tab and for the first administrator of a
  single-tenant install.
- The benchmark tool against a running four-source stack.
- A Trino run for every replication change; none of this work has run against Trino.
- For REQ-1914: every change type observed on every worker of two instances.

### Requirements: validation and consolidation

The requirements were amended as decisions were made, so several now read as a decision
log more than a specification.

| Requirement | Amendments dated 2026-10-01 | Recorded status |
|---|---|---|
| REQ-1915 | 8 | proposed |
| REQ-1900 | 6 | complete |
| REQ-1911 | 5 | proposed |
| REQ-1905 | 5 | complete |
| REQ-1877 | 4 | complete |
| REQ-803, REQ-1897 | 3 each | complete |

To do:

- **Consolidate.** Rewrite REQ-1915, REQ-1911 and REQ-1905 as one current statement each,
  with the amendment history kept beneath it. REQ-1915 should probably split: the data
  replicator, eager builds and central state, and the row-level rule are three
  requirements.
- **Correct statuses.** REQ-238 to REQ-241 (warm tables) are recorded as complete; the
  design changed and nothing reads a warm copy. REQ-1900 and REQ-1905 are recorded as
  complete and each gained scope this month. REQ-1907 is recorded as proposed on main
  while its implementation sits on an unmerged branch with newer text.
- **Resolve contradictions.** REQ-1865 still describes row-level behaviour that REQ-1915
  supersedes in part; superseded blocks are tagged, but the two should be read together
  and reconciled. REQ-238 to REQ-241 and REQ-826 now describe one mechanism in two
  vocabularies.
- **Trace tests to requirements.** REQ-1912 to REQ-1916 list no tests. Each needs its
  tests named as they are written, and the traceability exports regenerated.
- **Write scenarios.** REQ-1912, REQ-1913 and REQ-1916 have no scenario.
- **Run the requirements audit** across the whole file once the consolidation is done.
