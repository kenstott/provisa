# Next: Requirements Gap — Prioritized Build List

Open requirements ranked by build complexity (C), value (V), and integration
burden (I) — where I is what must be running to build and test the change. Scale
1–5 (1 = low/cheap, 5 = high/expensive). Grouped by integration posture, since that
gates *when* the work can happen, not just how hard it is.

## Shipped this cycle

- **Route.CACHE — REQ-865.** `Route.CACHE` first-class route; cache lookup feeds `decide_route`.
- **Cypher mutations — REQ-661–671.** Verified; added `writable_by` ACL (REQ-663) and
  relationship-write rejection (REQ-665).
- **Multi-tenancy — REQ-695–702.** Org schema/role/provisioning/cache wiring; verified + flipped.
- **Freshness module — REQ-856–858.** `provisa/freshness/`: `FreshnessSubject` + `FreshnessPredicate`
  (TTL / PROBE / TRANSITIVE / composable).
- **ADBC port — REQ-711.** Configurable Arrow Flight port.

## Scores

| Open cluster | C | V | I | Verdict |
| --- | --- | --- | --- | --- |
| Freshness gating consumers (859–861) | 2 | 3 | 2 | leaf follow-on |
| Query-cache refinements (863, 864, 866) | 2 | 4 | 2 | leaf, pairs with 865 |
| Mutation-authz core (867–869) | 3 | 5 | 2 | high-value leaf, MUST |
| Mutation-authz adapters + surfaces (870–872) | 4 | 4 | 4 | MUST, adapter-gated |
| Desktop zero-infra (828–830, 815–816) | 3 | 4 | 2 | infra leaf |
| Encryption core (684–689) | 4 | 4 | 3 | subsystem |
| Lineage column-trace (862) | 3 | 3 | 3 | mid |
| M:N join tables (672) | 3 | 3 | 3 | filler |
| Encryption KMS/high-sec (690–694) | 4 | 3 | 5 | cloud-gated, defer |
| Federation Engine / Connector (840–843) | 5 | 5 | 5 | substrate linchpin |
| Execution topology / federate (825–827) | 4 | 5 | 5 | substrate |
| Materialization Store (844–848, 855, 874) | 5 | 4 | 5 | substrate, dependent |
| Cardinality capability + count-route (673, 875) | 3 | 3 | 4 | rides connector + routing |

## Tier 1 — Leaf builds (no live stack)

Unit-testable with mocks or fixtures; no federation substrate required. Do these next.

- **[1] Freshness gating consumers.** REQ-859 ✅ done (2026-07): MV (`MVDefinition.is_fresh_at`)
  and the API/pg cache (`pg_cache._is_fresh`) now expose their state via a `StateSubject` adapter
  and delegate the TTL decision to the one `FreshnessPredicate` — behaviour-preserving, 190
  MV/cache tests green. REQ-860, REQ-861 → **Tier 4** (gated on the probe transport / an executor
  file-read hook).
- **[2] Query-cache refinements.** REQ-864 + REQ-866 ✅ done (2026-07). REQ-864: the result
  cache key is now the NORMALIZED governed SQL (sqlglot parse + simplify — whitespace, keyword
  case, identifier quoting, commutable AND-predicate ordering; literal VALUES preserved), so
  cosmetically-different queries share an entry (`provisa/cache/key.py`). REQ-866: a fail-closed
  `is_cacheable` gate — an empty RLS filter or a `current_setting`-dependent predicate makes the
  query un-cacheable (never read/written), so a per-session value can't leak across personas;
  wired at the endpoint cache check. REQ-863 → **Tier 4** (behavioral routing restructure, needs
  e2e). Deeper key canonicalization (alias renaming, JOIN reordering) needs a schema-aware
  optimizer and is out of scope for 864.
- **[3] Mutation-authz core — REQ-867–869.** ✅ Done (2026-07). Added `Capability.WRITE`
  (REQ-868); `provisa/security/mutation_authz.py` with the protocol write classifiers (GraphQL
  op-type, OpenAPI HTTP method, gRPC `idempotency_level`, Hasura action_type; unknown → write)
  and `authorize_mutation` — per-mutation `writable_by` default-deny + WRITE capability, ADMIN/
  SUPERADMIN bypass (REQ-867); execute-time enforcement wired into `_execute_action_field` with
  `role_id` threaded from both call sites — a `kind=mutation` UDF is a write regardless of the
  invoking surface (read-statement taint), read UDFs pass untouched (REQ-869). 18 unit tests, 391
  security tests green. **Remaining for [6]:** wiring the classifiers into each adapter's
  registration so `kind` is set by contract (not caller-declared) — lands with the adapters.

## Tier 2 — Decoupled subsystems (no substrate)

Larger authorship; testable without the federation substrate, some need an adapter or surface.

- **[4] Desktop zero-infra — REQ-828–830, REQ-815–816.** C3/V4/I2. Mostly done (2026-07):
  **REQ-829** ✅ embedded fakeredis (`make_redis` routes all four Redis clients to a shared-server
  `FakeAsyncRedis`, full command surface, tested). **REQ-815** ✅ minio + minio-init moved from the
  observability overlay to `docker-compose.core.yml` (they're a core runtime dep — Trino's
  S3-backed iceberg catalog runs at startup); telemetry sinks stay in the observability overlay;
  all stack combinations (dev/e2e/dev-install) validated. **REQ-816** ✅ demo (`demo.yml`) and
  test services (`test.yml`) are separate, brought up via harness markers, not assumed in the dev
  stack. **Remaining:** REQ-828 pluggable SQL store (Postgres → DuckDB/SQLite — the larger change
  that removes the last mandatory Docker dep) and REQ-830 stateful-component topology. NOTE: the
  dev/e2e host-port collision hit this session is still open — it needs port parameterization
  (`${PG_PORT:-5432}` …), which is *not* in 815/816's scope; track separately.
- **[5] Encryption core — REQ-684–689.** C4/V4/I3. `EncryptionService` with `NullEncryption` +
  `LocalKeychain` first — unit-testable, no cloud. Separate track; does not touch the substrate.
- **[6] Mutation-authz adapters + surface projection — REQ-870–872.** C4/V4/I4. Depends on [3].
  Admin-only reclassification gated by `ACCESS_CONFIG` (REQ-870); per-protocol association-
  suggesters emitting ranked `mutation → table` candidates (REQ-871); projection of
  `tracked_functions`/`tracked_webhooks` into every surface's native catalog — pgwire `_pg_proc`,
  SQL `information_schema.routines`, Cypher/Bolt `CALL fn() YIELD` — with `writable_by`
  enforcement (REQ-872). Needs the remote adapters and each surface wired.
- **[7] Lineage column-trace — REQ-862.** C3/V3/I3. Column-level trace instrumentation. Mid; value
  is governance/observability, decoupled from the substrate.

## Tier 3 — Substrate (live multi-engine + multi-source)

Highest complexity and integration burden; cannot be validated without Trino plus a second
engine plus real sources. This is one dependency chain — build it in order.

- **[8] Federation Engine / Connector abstraction — REQ-840–843.** C5/V5/I5. The linchpin:
  pluggable engines and the `capability()` / `catalog_add` / `land` / `typemap` connector
  contract. Gates everything below it. Schedule deliberately; needs multiple engines wired up.
- **[9] Execution topology / federate() — REQ-825–827.** C4/V5/I5. The stateless four-primitive
  flow (825), `federate(datasource, table)` strategy selection — virtual | scan | materialized
  (826) — and the routing consequences (827). Depends on the connector contract [8].
- **[10] Materialization Store — REQ-844–848, 855, 874.** C5/V4/I5. The per-tenant durable
  MATERIALIZED store: result/warm/MV caches that participate in federation. Depends on connector
  `land`/`attach` [8], `federate()` [9], and freshness (module shipped; gate REQ-855 uses the
  freshness leaf). REQ-874 adds incremental delta-fetch (PROBE == DELTA for monotonic entries).
- **[11] Cardinality capability + cheap-count route — REQ-673, REQ-875.** C3/V3/I4. Rides on the
  connector contract [8] and routing. REQ-673: `cardinality(source, table) -> Estimate{value,
  exact, method}` — cheap native stat first, `COUNT(*)` only when cheap, `unknown` when sizing
  is expensive (fail-open); sparse/opt-in, authored only for expensive-count sources. REQ-875:
  route a bare `count(*)` over an unmaterialized source with an EXACT native count to the native
  call instead of a full pull — gated by shape, exactness, and Stage-2 RLS (fail-closed).
  Consumers: `federate()` strategy, hot-table promotion, catalog/EXPLAIN, cold `graph-counts`.

## Tier 4 — Deferred to last (gated; scheduled, not dropped)

Every requirement here is still on the plan and must be completed. They sit last because each
is blocked on earlier work or carries integration risk that makes it wrong to build now. Ordered
by when they become buildable.

- **[12] Cache-key deep canonicalization (part of REQ-864 follow-on).** Alias renaming and
  commutable JOIN reordering in the cache key. Needs a schema-aware optimizer pass; builds once
  the connector/schema surface from [8] is available. Low priority — the shipped normalization
  already covers cross-client cosmetic variation.
- **[13] Planning-pipeline phase ordering — REQ-863.** Order the pipeline so routing consumes the
  post-optimization IR (hot-CTE inlining collapsing a federated query to DIRECT). Behavioral
  restructure of `pgwire/_pipeline.py` + the router; needs e2e. Schedule after the routing/
  `federate()` work [9] settles so it is not chasing a moving pipeline.
- **[14] Source-level freshness gating — REQ-860.** A source declaring a PROBE freshness predicate
  that gates queries before execution. Its valuable mode depends on the REQ-855 probe transport
  (inside materialization store [10]); TTL-only gating would just duplicate `Source.cache_ttl`.
  Lands after [10].
- **[15] On-stale file producer — REQ-861 (MAY).** Optional producer argv that runs when a file
  source is stale, before the file is read. Needs a file-read hook in the execution path (files
  are read by the engine, not Python) — arrives with the connector/execution work [8]/[9].
- **[16] Encryption KMS / high-security — REQ-690–694.** Needs AWS/Azure/GCP credentials (I5).
  Builds on encryption core [5]; scheduled after it on the encryption track.
- **[17] M:N join tables — REQ-672.** Real modeling value but needs a join-table source to
  exercise; build alongside the substrate sources once [8] is up.
- **[18] Cypher writes — REQ-818 (accepted).** Remaining Cypher-write item; slot with the authz
  adapters [6] once the mutation model [3] is in place.
- **[19] Compose host-port coexistence — REQ-876 (new).** Part 1 done (2026-07): every published
  host port in `core.yml`/`dev.yml` is now `${VAR:-default}`, so a second stack binds a different
  port by setting env (defaults unchanged; validated `PG_PORT=15432` offsets postgres while the
  default stays 5432). **Remaining:** (2) each harness project sets its own offset port set before
  compose up; (3) the `PROVISA_CONFIG` fixture's Postgres host:port becomes env-interpolated so the
  in-process app reaches the offset postgres (`TRINO_PORT`/`ZAYCHIK_PORT` are already env-driven).
  Needs runnable stacks to verify — scheduled here rather than shipped half-wired. This is the fix
  for the dev/e2e collision hit this session.

## Critical path & parallel tracks

**Critical path (substrate):** Federation Engine / Connector [8] → federate() [9] →
Materialization Store [10]. The freshness module (shipped) plus gating [1] feed the
materialization freshness gate (REQ-855) inside [10]; cardinality + count-route [11] ride on the
connector contract from [8].

**Parallel tracks that do not touch the substrate** — run alongside the critical path:

1. **Governance/authz:** mutation-authz core [3] → adapters + surfaces [6]. Generalizes REQ-663.
2. **Query-cache:** refinements [2] complete the Route.CACHE feature.
3. **Encryption:** core [5], KMS deferred.
4. **Desktop/infra:** zero-infra [4], which also unblocks independent test provisioning.

**Sequence:** freshness gating [1] and query-cache refinements [2] and mutation-authz core [3] are
the cheapest high-value leaves — do them first, in parallel where hands allow. Desktop zero-infra
[4] clears the test-provisioning debt. Then the substrate chain [8]→[9]→[10], with [11] and the
authz adapters [6] landing as their gates come online. Tier 4 [12]–[18] closes out last: each is
scheduled (nothing is dropped), gated on the work above, and slots in as its dependency lands.
