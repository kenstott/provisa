# ADR 0001 — Federation-engine statelessness and fleet scaling

Status: Accepted (design record; implementation gated on the federation-engine
abstraction, REQ-840–843)

## Context

The federation engine is pluggable (REQ-825): the reference engine is Trino
(coordinator + workers, per-query distributed scale-out); an alternative is an
embedded single-node engine such as DuckDB (`ATTACH` RDBs, scan Parquet/S3), which
has no coordinator/worker split. A single-node engine cannot scale one query across
machines, but a fleet of instances behind a load balancer scales *concurrency*. The
question this ADR settles: what state must such a fleet share, and what coordination
does it need, so that "swap Trino for a DuckDB fleet" is correct rather than a source
of divergence?

## Decision

Three invariants govern any engine admitted to the federation-engine contract.

### 1. The engine owns nothing durable

Everything the engine holds is **ephemeral** or **reconstructable**:

| State | Nature | Where it lives |
| --- | --- | --- |
| Query execution (buffers, spills) | ephemeral | in-process, per instance |
| Catalog / views / secrets | reconstructable | rebuilt from the control-plane registry on startup (same config → same catalog) |
| Materialized data | externalized | the shared materialization store (REQ-830), **never a local engine file** |
| Hot cache | externalized (optional) | shared Redis, or per-instance fakeredis |
| Control-plane registry | source of truth | shared store |

A crashed instance restarts, rebuilds its catalog from config, re-points at the
shared stores, and loses nothing — because it never owned durable data.

### 2. Durable state is shared by connection, selected by config

Each shared component (REQ-830) is reached by a connection in the config. Because all
instances are built from the same config, they point at the **same** materialization
store, hot cache, and control-plane registry — the same way many instances share one
Redis. There are no per-instance copies to diverge.

Consistency per component:

- **Materialization store** — one shared, snapshot-consistent copy (e.g. Iceberg);
  all instances read the same committed snapshot.
- **Control-plane registry** — shared source of truth; per-instance in-memory catalogs
  are eventually-consistent projections (reload/TTL on change). A **read-only** config
  at runtime is trivially consistent — you can then run arbitrarily many copies with no
  config coordination at all; mutable config converges via the shared registry.
- **Hot cache** — sharing is an **optimization, not a requirement**: the hot cache has
  a live fallback (REQ-231) and its rows are governed at query time (REQ-233), so a
  per-instance cache never yields a wrong result, only a lower hit rate and a wider
  invalidation window (bounded by TTL).

### 3. The only cross-instance coordination is single-writer MV refresh

A shared materialization store solves data *divergence* (one copy) but not *who
writes it*: N instances on independent refresh timers would each recompute the same MV
and race on the write. This is the sole coordination point, and it is **scheduling**,
not consistency. It is owned by a shared refresh catalog with a lease-claim protocol
(REQ-879): an atomic conditional UPDATE that dedups by version (REQ-862 stamps) and
excludes live writers, a heartbeat-renewed lease for crash safety, and a fenced commit
so a revived stale writer cannot clobber a newer refresh. Decentralized per-MV election
— no global leader. Held pessimistic locks are rejected (a long refresh holds a
transaction open; a crash leaves a stuck lock).

## The materialization-store guarantee is a deployment choice

Two tiers, chosen per deployment:

- **Shared materialization store** (default, most correct) — single coordinated copy →
  monotonic reads and snapshot-consistent joins → the same query at the same instant
  returns the same answer on any instance, with single-refresh economics.
- **Distributed / per-instance materialization** (eventually correct) — each instance
  materializes locally, at N× compute, and the same query may return different answers on
  different instances until they converge. Convergence requires **two** conditions, not one:
  1. **Deterministic `view_sql`** — else instances compute different content from identical
     source and never agree. Enforced at registration (reject `now()`/`random()`/`uuid`/
     unordered-`LIMIT`; `provisa/mv/determinism.py`).
  2. **Source quiescence** — the source must stop changing long enough within a refresh
     cycle for every instance to materialize the *same* source state. A never-settling,
     high-churn source **never converges** — each instance perpetually reflects a different
     snapshot, so it is permanently divergent, not eventually consistent. Determinism is
     necessary but not sufficient.
  Quiescence is a workload property (not statically checkable), so it is an operational
  constraint on choosing `distributed`; determinism is validated. The distributed tier fits
  low-churn / periodically-batch-loaded sources (dimension tables, daily loads), not
  high-velocity streams — those must use the shared tier.

## Consequences

- A single-node-engine fleet behind a load balancer is correct without distributed
  transactions: engine stateless, durable state shared by connection, one coordination
  point (refresh scheduling).
- The bottlenecks in the fleet model are the shared **sources** (need their own
  scale-out, e.g. read replicas) and the shared **stateful components** (materialization
  store on S3/Iceberg scales; a single shared RDB store or Redis is a choke point) —
  not the engine.
- Requires: the refresh-coordination catalog (REQ-879), the version stamps as the dedup
  key (REQ-862), and — only for the distributed tier — an MV-definition determinism rule.
