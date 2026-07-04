# Manual Smoke-Test Plan

Human-run smoke tests for the features shipped/landed per `next.md`. Each test is
independent. Record PASS/FAIL and any deviation. Prerequisite unless noted: the dev
stack is up (`docker-compose.core.yml` + `docker-compose.dev.yml`), app reachable.

## 0. Stack bring-up

| Step | Action | Expected |
| --- | --- | --- |
| 0.1 | Start core + dev stack. | All containers healthy; app answers on its HTTP port. |
| 0.2 | Confirm minio + minio-init came up from `docker-compose.core.yml` (not the observability overlay). | Trino's S3-backed Iceberg catalog is reachable at startup; no missing-bucket error. |
| 0.3 | Confirm pgwire listener on port 5439. | `psql`/DBeaver can connect to the catalog. |

## 1. Query-cache (Route.CACHE — REQ-865, 864, 866)

| Step | Action | Expected |
| --- | --- | --- |
| 1.1 | Run a governed read query twice via the data endpoint. | 2nd run is a cache hit (faster; cache route taken). |
| 1.2 | Re-run the same query with only cosmetic changes — extra whitespace, different keyword case, reordered `AND` predicates. | Still a cache hit (normalized SQL key). |
| 1.3 | Run a query whose RLS filter is empty OR depends on `current_setting(...)`. | Query is **not** cached — never read from or written to cache (fail-closed `is_cacheable`). |
| 1.4 | As persona A prime the cache, then switch to persona B and run the same shape. | No cross-persona leak of per-session values. |

## 2. Cypher mutations + writable_by (REQ-661–671, 663, 665, 818)

Run against `/data/cypher`.

| Step | Action | Expected |
| --- | --- | --- |
| 2.1 | `CREATE` a node as a role listed in `writable_by`. | Succeeds; row appears through the mutation pipeline (RLS + hooks applied). |
| 2.2 | `SET` / `DELETE` a node as an authorized role. | Succeeds. |
| 2.3 | Same `CREATE` as a role **not** in `writable_by`. | Rejected (default-deny). |
| 2.4 | `MERGE`, `DETACH`, or `REMOVE`. | Rejected (unsupported write forms). |
| 2.5 | A relationship write. | Rejected (REQ-665). |
| 2.6 | Read-only `MATCH ... RETURN`. | Passes untouched. |

## 3. Bolt / Neo4j surface

| Step | Action | Expected |
| --- | --- | --- |
| 3.1 | Connect Neo4j Browser (or Bloom) to Provisa's Bolt endpoint. | Connects; federated graph visible. |
| 3.2 | Run a Cypher read over the federated graph. | Returns rows. |

## 4. Multi-tenancy (REQ-695–702)

| Step | Action | Expected |
| --- | --- | --- |
| 4.1 | Provision a new org. | Org schema/role created; cache wired. |
| 4.2 | As a user in org X, query a table. | Only org-X rows returned. |
| 4.3 | Attempt to reach org-Y data with org-X credentials. | Denied — no cross-tenant leakage. |

## 5. Mutation authorization core (REQ-867–869)

| Step | Action | Expected |
| --- | --- | --- |
| 5.1 | Invoke a `kind=mutation` UDF as a non-writer role. | Denied (default-deny + WRITE capability required). |
| 5.2 | Same as ADMIN / SUPERADMIN. | Allowed (bypass). |
| 5.3 | Invoke a `kind=mutation` UDF from a read surface (e.g. inside a read statement). | Still treated as a write and enforced (read-statement taint). |
| 5.4 | Invoke a read UDF. | Passes untouched. |

## 6. Encryption (REQ-684, 685, 686 partial, 688, 689)

| Step | Action | Expected |
| --- | --- | --- |
| 6.1 | Run a query so it is written to `query_audit_log`. | Stored `query_text_enc` column is **ciphertext**, not plaintext. |
| 6.2 | As an authorized admin, read the audit query text. | Decrypts and round-trips to the original SQL. |
| 6.3 | Inspect a hot table cached in Redis (REQ-688). | Payload at rest is encrypted, not readable plaintext. |
| 6.4 | Inspect an API-source auth column (REQ-686 partial). | Stored value is encrypted; runtime accessor decrypts on use. |
| 6.5 | Tamper with a stored ciphertext / use the wrong key. | Decryption fails closed — no silent plaintext. |

## 7. Freshness gating (REQ-856–859)

| Step | Action | Expected |
| --- | --- | --- |
| 7.1 | Query an MV within its TTL. | Served as fresh. |
| 7.2 | Let an MV/cache entry exceed TTL. | Treated as stale (single `FreshnessPredicate` decides — MV and API/pg cache agree). |

## 8. Desktop zero-infra (REQ-828, 829, 815, 816)

| Step | Action | Expected |
| --- | --- | --- |
| 8.1 | Boot in embedded mode (fakeredis, no external Redis). | All four Redis clients route to the shared FakeAsyncRedis; app works. |
| 8.2 | Single-tenant SQLite/embedded run. | `Database` abstraction + `schema_org.create_all` build the schema; queries work with no Docker Postgres. |
| 8.3 | Confirm demo/test services are NOT assumed in the dev stack. | Dev stack comes up without demo.yml/test.yml. |

## 9. Lineage column-trace (REQ-862)

| Step | Action | Expected |
| --- | --- | --- |
| 9.1 | Refresh an MV and inspect the `mv.refresh.column_lineage` span. | Carries per-output-column derivation, definition-version hash, input-version + fidelity kind, and trace_id. |
| 9.2 | Inspect `mv_refresh_log` ledger columns. | `definition_version` / `input_version` / `input_version_kind` / `trace_id` populated. |

## 10. pgwire catalog (DBeaver)

| Step | Action | Expected |
| --- | --- | --- |
| 10.1 | Open the catalog in DBeaver via port 5439. | Tables/columns list correctly. |
| 10.2 | Inspect a foreign-key relationship. | FK arrows render (ANY-array parse + format_type lookup correct). |

## 11. ADBC / Arrow Flight (REQ-711)

| Step | Action | Expected |
| --- | --- | --- |
| 11.1 | Connect an ADBC/Arrow-Flight client on the configured port. | Connects on the configured (non-default) port; query returns Arrow batches. |

## 12. MV refresh coordination (REQ-879)

| Step | Action | Expected |
| --- | --- | --- |
| 12.1 | Run two app instances sharing one materialization store; trigger a refresh due window. | Exactly one instance claims the lease and refreshes — no double-refresh. |
| 12.2 | Kill the writer mid-refresh. | Lease expires; another instance takes over; fenced commit discards the dead writer's work. |

---

### Sign-off

| Section | Result | Tester | Notes |
| --- | --- | --- | --- |
| 0 Stack | | | |
| 1 Cache | | | |
| 2 Cypher | | | |
| 3 Bolt | | | |
| 4 Multi-tenancy | | | |
| 5 Mutation authz | | | |
| 6 Encryption | | | |
| 7 Freshness | | | |
| 8 Desktop | | | |
| 9 Lineage | | | |
| 10 pgwire | | | |
| 11 ADBC | | | |
| 12 MV coord | | | |
