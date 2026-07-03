# Next: Requirements Gap — Prioritized Build List

Ranking of non-complete requirements by build complexity (C), value (V), and
integration burden (I) — where I is what must be running to build and test the
change. Scale 1–5 (1 = low/cheap, 5 = high/expensive). Grouped by integration
posture, since that gates *when* the work can happen, not just how hard it is.

## Scores

| REQ cluster | C | V | I | Verdict |
| --- | --- | --- | --- | --- |
| Route.CACHE (865) | 1 | 4 | 2 | do now |
| Cypher mutations verify+flip (661–671) | 1 | 3 | 3 | high ROI |
| Multi-tenancy refs+flip (695–702) | 1 | 5 | 4 | high value, PG-gated |
| Freshness module (856–858) | 2 | 4 | 2 | leaf unblocker |
| ADBC port (711) | 1 | 2 | 2 | trivial filler |
| M:N join tables (672) | 3 | 3 | 3 | mid |
| GQL count_query (673) | 1 | 2 | 2 | filler |
| Encryption core (684–689) | 4 | 4 | 3 | subsystem |
| Encryption KMS/high-sec (690–694) | 4 | 3 | 5 | cloud-gated |
| Federation Engine / Connector (840–843) | 5 | 5 | 5 | substrate |
| Materialization Store (844–848, 855) | 5 | 4 | 5 | substrate, dependent |

## Tier A — Finish what is already built (verify + flip)

Code exists; the blocker is test infrastructure, not authorship.

- **[1] Route.CACHE — REQ-865.** C1/V4. Only concrete unmet claim in the CODE-PRESENT
  set. Cache store, key isolation (`provisa/cache/key.py:57`), and normalization
  already exist; add the enum member plus the `decide_route` branch. Completes the
  in-flight query-cache feature. Cheapest high-value change on the board.
- **[2] Cypher mutations — REQ-661–671.** C1/V3/I3. `provisa/cypher/translator.py`
  present; needs a writable connector plus a running pipeline (`test_mutations.py`
  e2e) to verify.
- **[3] Multi-tenancy — REQ-695–702.** C1/V5/I4. Code landed in `provisa/core/db.py:77`
  (commit 221dd56); spec lacks `code:` refs. Highest value here (org isolation,
  MUST), but full verification needs PG with multiple org schemas plus Redis ACL.

## Tier B — Leaf builds (self-contained, unit-testable in isolation)

Low integration burden; testable with mocks or fixtures, no live federation stack.

- **[4] Freshness module — REQ-856–858.** C2/V4/I2. Extract the freshness logic already
  on models (REQ-859–861) into `provisa/freshness/` as a `FreshnessSubject` /
  `FreshnessPredicate` protocol. It is a prerequisite for materialization-store
  freshness (REQ-855), so building it first de-risks the substrate tier.
- **[5] ADBC port — REQ-711.** C1/V2. Parameterize the hardcoded `8815` in
  `provisa-client/provisa_client/adbc.py:62`. Filler; do it while touching the
  client.
- **[6] GQL count_query (673), M:N join tables (672).** Filler / mid. REQ-672 has real
  modeling value but needs a join-table source to exercise.

## Tier C — New subsystem (large, but decoupled)

High authorship cost; testable without the federation substrate.

- **[7] Encryption core — REQ-684–689.** C4/V4/I3. Build `EncryptionService` with
  `NullEncryption` plus `LocalKeychain` first — unit-testable, no cloud. The
  KMS / high-security variants (REQ-690–694) split off as I5 (need AWS/Azure/GCP
  credentials); defer to a second phase.

## Tier D — Substrate (needs a live multi-engine, multi-source stack)

Highest complexity and integration burden; cannot be validated without Trino plus
a second engine plus real sources.

- **[8] Federation Engine / Connector abstraction — REQ-840–843.** C5/V5/I5. The
  biggest lever (pluggable engines, governance-parity foundation), but validating
  `capability()` / `catalog_add` / `land` / `typemap` requires multiple engines
  wired up. Schedule deliberately.
- **[9] Materialization Store — REQ-844–848, 855.** C5/V4/I5. Depends on the connector
  `land` / `attach` (item [8]) and on freshness (item [4]). Sequence strictly after
  both. Zero code today.

## Recommended sequence

Route.CACHE → Cypher / multi-tenancy flips → Freshness module → Encryption core →
(decision point) Federation Engine → Materialization Store.

Encryption can run on a separate track since it does not touch the substrate.
Defer KMS variants to backlog.

The dependency edge that matters: Freshness (Tier B) gates Materialization Store
(Tier D). Build the leaf first so the substrate work is not blocked mid-flight.
