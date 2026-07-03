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

- **[1] Freshness gating consumers — REQ-859–861.** C2/V3/I2. Direct follow-on to the freshness
  module just shipped. Make MV, Source, and the API/pg cache conform to `FreshnessSubject`
  (REQ-859); add source-level freshness gating so a query reading a pull-through source is
  gated before execution (REQ-860); optional on-stale producer command for file sources
  (REQ-861). Closes the loop on the freshness leaf and de-risks materialization-store
  freshness (REQ-855).
- **[2] Query-cache refinements — REQ-863, REQ-864, REQ-866.** C2/V4/I2. Pairs with the shipped
  `Route.CACHE`. REQ-864: key the result cache on a NORMALIZED governed IR (canonicalize
  cosmetic form) instead of the compiled-SQL string, so semantically-identical queries share
  an entry. REQ-863: order the planning pipeline so routing consumes the post-optimization IR
  (hot-CTE inlining can collapse a federated query to DIRECT). REQ-866: the cache-isolation
  invariant — largely satisfied by 865's persona-resolved key (role_id + resolved RLS); mostly
  verify + flip, add fail-closed tests.
- **[3] Mutation-authz core — REQ-867–869.** C3/V5/I2. MUST, and the generalization of the
  `writable_by` gate shipped for Cypher writes (REQ-663). Protocol-agnostic layer: table-scoped
  mutation sub-resources with per-mutation `writable_by` (empty = default-deny) + a global
  `WRITE`/`EXECUTE_MUTATION` capability (`provisa/security/rights.py`), execute-time enforcement
  in `_execute_action_field`, and pure-function write classifiers (GraphQL op-type, OpenAPI HTTP
  method, gRPC `idempotency_level`, Hasura action_type; unknown → write) plus read-statement
  taint. All unit-testable — no live adapter. Highest-ROI leaf; do while the ACL model is fresh.

## Tier 2 — Decoupled subsystems (no substrate)

Larger authorship; testable without the federation substrate, some need an adapter or surface.

- **[4] Desktop zero-infra — REQ-828–830, REQ-815–816.** C3/V4/I2. Pluggable SQL store
  (Postgres → DuckDB/SQLite, REQ-828) and embedded fakeredis (REQ-829) so the full backend runs
  on a laptop with no Docker, exercising the same hot-cache/rate-limit/invalidation paths as
  production instead of silent no-ops. REQ-830 names the five stateful components to isolate.
  REQ-815/816 (Docker Compose organization) is where the **integration-tests-must-independently-
  provision** follow-up lands: parameterize core host ports (`${PG_PORT:-5432}` …) + a dedicated
  test project so dev/e2e/integration stacks coexist.
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

## Backlog / deferred

- **Encryption KMS / high-security — REQ-690–694.** I5, needs AWS/Azure/GCP credentials. Defer
  behind encryption core [5].
- **M:N join tables — REQ-672.** Filler; real modeling value but needs a join-table source to
  exercise.
- **Cypher writes REQ-818, Docker REQ-815/816** — accepted; 815/816 folded into [4].

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
authz adapters [6] landing as their gates come online.
