# Next: Requirements Gap — Prioritized Build List

Ranking of non-complete requirements by build complexity (C), value (V), and
integration burden (I) — where I is what must be running to build and test the
change. Scale 1–5 (1 = low/cheap, 5 = high/expensive). Grouped by integration
posture, since that gates *when* the work can happen, not just how hard it is.

## Scores

| REQ cluster | C | V | I | Verdict |
| --- | --- | --- | --- | --- |
| Route.CACHE (865) | 1 | 4 | 2 | ✅ done |
| Cypher mutations verify+flip (661–671) | 1 | 3 | 3 | ✅ done |
| Multi-tenancy refs+flip (695–702) | 1 | 5 | 4 | ✅ done |
| Remote-schema mutation authz — core (867–869) | 3 | 5 | 2 | high value MUST, leaf |
| Remote-schema mutation authz — adapters+surfaces (870–872) | 4 | 4 | 4 | MUST, adapter-gated |
| Freshness module (856–858) | 2 | 4 | 2 | leaf unblocker |
| ADBC port (711) | 1 | 2 | 2 | trivial filler |
| M:N join tables (672) | 3 | 3 | 3 | mid |
| GQL count_query (673) | 1 | 2 | 2 | filler |
| Encryption core (684–689) | 4 | 4 | 3 | subsystem |
| Encryption KMS/high-sec (690–694) | 4 | 3 | 5 | cloud-gated |
| Federation Engine / Connector (840–843) | 5 | 5 | 5 | substrate |
| Materialization Store (844–848, 855) | 5 | 4 | 5 | substrate, dependent |

## Tier A — Finish what is already built (verify + flip) — ✅ COMPLETE

All three done (2026-07): implemented + tested + flipped to `complete`.

- **[1] Route.CACHE — REQ-865.** ✅ Added `Route.CACHE`; `decide_route` gains
  `cache_hit`/`no_cache` and evaluates the cache as the first candidate route;
  endpoint feeds the cache lookup into routing (`provisa/transpiler/router.py`,
  `provisa/api/data/endpoint.py`).
- **[2] Cypher mutations — REQ-661–671.** ✅ Verified translator/pipeline; filled two
  gaps — `writable_by` column ACL on Cypher writes (REQ-663, new `write_acl_error`
  delegating to the GraphQL check) and explicit relationship-write rejection with a
  UDF-syntax hint (REQ-665).
- **[3] Multi-tenancy — REQ-695–702.** ✅ Verified org schema/role/provisioning/cache
  wiring; added `code:` refs; fixed stale `init_schema` mocks; flipped.

## Tier B — Leaf builds (self-contained, unit-testable in isolation)

Low integration burden; testable with mocks or fixtures, no live federation stack.

- **[4] Freshness module — REQ-856–858.** ✅ Done (2026-07). Built `provisa/freshness/`:
  `FreshnessSubject` protocol + `FreshnessPredicate` strategies (TTL, PROBE, TRANSITIVE,
  composable TTL+PROBE) returning fresh/stale/failed — pure decision, no side effects.
  Unblocks materialization-store freshness (REQ-855). Consumers REQ-859–861 (MV/Source/
  cache conform to the protocol; source-level + file-producer gating) remain the follow-on.
- **[5] ADBC port — REQ-711.** ✅ Done (2026-07). `adbc_connect()` gains an optional
  `port` param (default 8815); the hardcoded `grpc://host:8815` now uses it. Default +
  custom-port tests added.
- **[6] GQL count_query (673), M:N join tables (672).** Filler / mid. REQ-672 has real
  modeling value but needs a join-table source to exercise.
- **[7] Remote-schema mutation authz — universal core — REQ-867–869.** C3/V5/I2. MUST
  authorization subsystem, and the direct generalization of the Cypher-write
  `writable_by` gate just landed (REQ-663). Build the protocol-agnostic layer:
  table-scoped mutation sub-resources with per-mutation `writable_by` (empty =
  default-deny) + a global `WRITE`/`EXECUTE_MUTATION` capability on the Capability
  enum (`provisa/security/rights.py`), and execute-time enforcement in
  `_execute_action_field` (`provisa/api/data/endpoint.py`) requiring `WRITE` AND
  `role_id ∈ mutation.writable_by`. The per-protocol write classifiers (GraphQL
  op-type, OpenAPI HTTP method, gRPC `idempotency_level`, Hasura `action_type`;
  unknown → write) and read-statement taint (a SELECT over a `kind=mutation` UDF
  promotes to write) are pure functions — unit-testable with fixtures, no live
  adapter needed. Leaf despite the MUST breadth; do it right after Tier A while the
  `writable_by` model is fresh.

## Tier C — New subsystem (large, but decoupled)

High authorship cost; testable without the federation substrate.

- **[8] Encryption core — REQ-684–689.** C4/V4/I3. Build `EncryptionService` with
  `NullEncryption` plus `LocalKeychain` first — unit-testable, no cloud. The
  KMS / high-security variants (REQ-690–694) split off as I5 (need AWS/Azure/GCP
  credentials); defer to a second phase.
- **[9] Remote-schema mutation authz — adapters + surface projection — REQ-870–872.**
  C4/V4/I4. Depends on the universal core (item [7]). Three legs: (a) admin-only
  reclassification of a mutation to read-safe, gated by `ACCESS_CONFIG` and recorded
  as a governance decision — no caller opt-out (REQ-870, `provisa/security/access_config.py`,
  `provisa/api/admin/`); (b) per-protocol association-suggesters emitting ranked
  `mutation → table` candidates (GraphQL return-type walk, OpenAPI path/operationId/
  tags, gRPC response-message stem), admin-confirmed, default-deny (REQ-871,
  `provisa/graphql_remote/mapper.py`); (c) project `tracked_functions`/`tracked_webhooks`
  into every surface's native catalog — pgwire `_pg_proc`, SQL
  `information_schema.routines`, Cypher/Bolt `CALL fn() YIELD` — routing through the
  shared executor with `writable_by` enforcement (REQ-872). Needs the remote adapters
  and each surface wired to exercise classification, suggestion, and cross-surface
  invocation.

## Tier D — Substrate (needs a live multi-engine, multi-source stack)

Highest complexity and integration burden; cannot be validated without Trino plus
a second engine plus real sources.

- **[10] Federation Engine / Connector abstraction — REQ-840–843.** C5/V5/I5. The
  biggest lever (pluggable engines, governance-parity foundation), but validating
  `capability()` / `catalog_add` / `land` / `typemap` requires multiple engines
  wired up. Schedule deliberately.
- **[11] Materialization Store — REQ-844–848, 855.** C5/V4/I5. Depends on the connector
  `land` / `attach` (item [10]) and on freshness (item [4]). Sequence strictly after
  both. Zero code today.

## Recommended sequence

~~Route.CACHE → Cypher / multi-tenancy flips~~ (done) → **Remote-schema mutation authz
core (867–869)** → Freshness module → Encryption core → mutation authz adapters +
surface projection (870–872) → (decision point) Federation Engine → Materialization
Store.

Do the mutation-authz core next: it is a MUST security subsystem, unit-testable as a
leaf, and generalizes the `writable_by` enforcement just landed for Cypher writes
(REQ-663) into the protocol-agnostic layer — highest-ROI follow-on while that model is
fresh. Its adapter/surface legs (870–872) are adapter-gated and slot alongside
Encryption on a separate track from the substrate.

Encryption can run on a separate track since it does not touch the substrate.
Defer KMS variants to backlog.

Dependency edges that matter: Freshness (Tier B) gates Materialization Store (Tier D);
the mutation-authz core (item [7]) gates its adapter/surface legs (item [9]). Build the
leaves first so the gated work is not blocked mid-flight.
