# Audit — Group 3: Source Registration & Data Modeling

Date: 2026-06-18
Scope: **Group 3 — Source Registration & Data Modeling** (45 requirements: REQ-012–021,
119, 133–136, 151–160, 194–195, 250–251, 363, 366–367, 392–394, 399–400, 413–415, 417–418,
432–434).
Method: six parallel read-only audit agents, one per sub-area, each comparing the
implementation to the requirement text with file:line evidence; synthesised here.
Companion to [group-1.md](group-1.md) and [group-2.md](group-2.md); the as-found
snapshot [overview.md](overview.md) covers Group 1 only.

## Classification key

- **To spec** — implemented and matches the requirement
- **Incomplete** — partially implemented
- **Not to spec** — implemented differently than the requirement states
- **Not added** — required but missing

## Summary

36 of 45 to spec (after Phase 1 + Phase 2 remediation). Gaps: 3 Incomplete
(119, 250, 251), 3 Not to spec (017, 019, 366), 3 Not added (020, 417, 434).

| REQ | Sub-area | Status | Finding |
| --- | --- | --- | --- |
| 012 | Source registration | Fixed 2026-06-18 | Connection now validated for all driver-backed types before persisting; `_add_source_pool` propagates failures and `create_source` rejects (Phase 1) |
| 013 | Source registration | To spec | No table queryable until registered; schema built only from `table_repo.list_all()` (`api/app.py:1936`) |
| 014 | Source registration | To spec | Unregistered tables absent from schema/runtime; `generate_schema` driven solely by registered tables (`compiler/schema_gen.py:70`) |
| 015 | Source registration | To spec | No per-table governance mode / registry-required mode; uniform Stage-2 governance (`compiler/stage2.py:34`) |
| 016 | Source registration | To spec | `register_table` calls `_rebuild_schemas` synchronously (`api/admin/schema.py:1687`) |
| 017 | Source registration | Not to spec | NoSQL (Mongo) uses the Trino Mongo connector, not Parquet materialization (`mongodb/source.py:74`); read-only not enforced |
| 018 | Relationships | To spec | Trino FK metadata → `relationship_candidates` suggested/accepted/rejected (`compiler/introspect.py:209`, `discovery/collector.py:209`) |
| 019 | Relationships | Not to spec | Manual cross-source rels via `upsert_relationship`; `one-to-one` is absent by design — the relationship field model is a strict binary (single vs list) and 1:1 collapses to many-to-one. The requirement, not the code, should drop `one-to-one` (`core/models.py:81`, `compiler/schema_gen.py:1046`) |
| 020 | Relationships | Not added | `relationships` has no owner, no version, no re-review/stale flag (`core/schema.sql:119`) |
| 021 | Source registration | To spec | GraphQL schema reflects registration model + aliases, not raw DB (`compiler/schema_gen.py:11`) |
| 119 | JSONB promotion | Incomplete (registration wired) | Phase 3a: promotions declared on a table (`Table.promotions`) persist to `api_endpoints.promotions`, load through `loader.py` and register as first-class columns via `promotions_map`; `apply_promotions()` execution helper added + tested. Remaining: auto-run the DDL at api-cache materialization (Trino cache stores JSONB as VARCHAR + has no PG conn in that path) |
| 133 | Views | To spec | `views:` config → governed tables with column visibility/mask/description/alias (`api/app.py:1178`, `core/models.py:272`) |
| 134 | Views | To spec | View tables flow through Stage-2 RLS/mask/visibility/row-cap like any table (`compiler/stage2.py:231`); approval via per-table `approval_hook` |
| 135 | Views | To spec | `materialize:true` → CTAS-refreshed MV; non-materialized → inline subquery via `view_sql_map` (`api/app.py:1189`, `compiler/view_expand.py:30`) |
| 136 | Views | To spec | Computed semantics enter only via config `views`/`materialized_views` SQL (`api/app.py:1160`) |
| 151 | Column path | To spec | Path cols emit PG `->>`/`->`; SQLGlot transpiles to `JSON_EXTRACT_SCALAR` for Trino (`compiler/sql_gen.py:1879`) |
| 152 | Column path | To spec | JSON-extract forces Trino unless source is postgres; PG path cols route direct (`transpiler/router.py:162`) |
| 153 | Column path | To spec | Path extraction only in SELECT builder; mutation gen has no path logic (`compiler/mutation_gen.py`) |
| 154 | Naming | To spec | `domain_prefix` prepends `{domain}__` (double underscore) to GQL names (`compiler/schema_gen.py:417`) |
| 155 | Naming | To spec | Column/table `alias` overrides GQL names (`compiler/schema_gen.py:514`, `naming.py:245`) |
| 156 | Naming | To spec | Column/table `description` → GraphQL SDL (`compiler/schema_gen.py:520`) |
| 157 | Naming | To spec | Order-by enum values preserve original column case (`compiler/schema_gen.py:601`) |
| 158 | Auto-MV | To spec | Cross-source rels with `materialize:true` auto-generate MV defs at startup (`api/app.py:1207`) |
| 159 | Auto-MV | To spec | Guard `src_source != tgt_source` skips same-source rels (`api/app.py:1223`) |
| 160 | Auto-MV | To spec | Auto-MVs default `STALE`; populated by refresh loop (`mv/models.py:76`, `mv/registry.py:60`) |
| 194 | Naming convention | Fixed 2026-06-18 | `hasura_graphql` now maps to snake_case (Phase 1, `compiler/naming.py`) |
| 195 | Naming convention | Fixed 2026-06-18 | `normalize_convention` maps `hasura-default`/`graphql-default`/DDN `graphql` literals to presets (Phase 1) |
| 250 | Trino catalog gen | Incomplete | `generate_trino_kafka_properties` exists but never wired to startup; catalog/table-def files hand-authored (`kafka/source.py:84`, `api/app.py:880`) |
| 251 | NoSQL mapping DSL | Incomplete | Redis/ES/Prometheus mapping dataclasses exist but not exposed in YAML config / `Source` model (`redis/source.py:43`, `core/models.py:140`) |
| 363 | Semantic layer | To spec | SQLAlchemy dialect introspects via `POST /data/graphql` with `X-Role`; server returns per-role filtered schema (`provisa-client/.../sqlalchemy_dialect.py:102`, `api/data/endpoint.py:330`) |
| 366 | View approval | Not to spec | View/rel creation gated only by `create_view`/`create_relationship`; no approval workflow, no originator-rights/join check; `APPROVE_VIEW`/`APPROVE_RELATIONSHIP` never invoked (`api/admin/schema.py:1560`, `security/rights.py:27`) |
| 367 | Domain views | To spec (now tested) | Cross-domain data only enters a domain via a view — enforced by V001 domain-access (a role cannot query another domain's table directly, only an import view in its own domain). Pinned by `tests/unit/test_domain_views.py` |
| 392 | Graph PK | Fixed 2026-06-18 | `/data/graph-schema` now returns singular `pk: string\|null` per node label (Phase 1, `api/rest/cypher_router.py:520`) |
| 393 | PK designation | To spec | `is_primary_key: bool = False` on `ColumnConfig`, persisted, informational only — no constraint generated (`core/models.py:289`, `core/schema.sql:79`) |
| 394 | PK designation | To spec | Composite PK in column order; first PK = canonical `id_column` before all heuristics (`compiler/sql_gen.py:307`, `cypher/label_map.py:492`) |
| 399 | Relationship cols | To spec | `is_foreign_key`/`is_alternate_key` via `ALTER TABLE ... ADD COLUMN IF NOT EXISTS` at schema-init (not a migration); source col marked FK on save (`core/schema.sql:100`, `repositories/relationship.py:82`) |
| 400 | Relationship cols | To spec | Target col → PK if none exists else AK (`repositories/relationship.py:92`) |
| 413 | FK auto-gen | To spec | FK auto-gen in `discovery/fk_introspect.py:225` (not `compiler/introspect.py` as named); both directions, `ON CONFLICT DO NOTHING` |
| 414 | Demo schema | To spec | One FK in demo: `user_id ... REFERENCES users(id)` (`demo/files/create_demo_files.py:248`) |
| 415 | FK naming style | Fixed 2026-06-18 | `NamingConfig.hasura_v2_relationship_style` drives inflection-based singular/plural FK aliases (Phase 1, `discovery/fk_introspect.py`) |
| 417 | Hasura migration | Not added | Mapper still skips remote schemas with "not supported" warning; no `graphql_remote` registration path (`hasura_v2/parser.py:314`, `hasura_v2/mapper.py:476`) |
| 418 | Domain views | To spec (now tested) | Same V001 mechanism as 367 — domain-local calculations/relationships; cross-domain data imported only via views in the role's own domain. Pinned by `tests/unit/test_domain_views.py` |
| 432 | Table uniqueness | To spec | register/updateTable call `_domain_table_conflict`; startup `_assert_domain_table_unique` fails on duplicates (`api/admin/schema.py:1631`, `api/app.py:1595`) |
| 433 | Dataset ownership | Fixed 2026-06-18 | `register_table`/`update_table` reject a cross-domain claim of an already-owned dataset (`_dataset_ownership_conflict`, normalized name per source); virtual `__provisa__` views exempt (`api/admin/schema.py`) |
| 434 | Creation requests | Not added | No creation-request table/model/queue or execute/reject mutations anywhere; named test absent (`core/`, `provisa-ui/src/`) |

## Detail

### Source & table registration (REQ-012–017, 021)

- **012 — Incomplete.** Live registration with no restart is real: `create_catalog` issues
  `CREATE CATALOG IF NOT EXISTS ... USING ... WITH (...)` (`provisa/core/catalog.py:96`),
  invoked from `create_source`, gated by `require_capability(info, "source_registration")`
  (`provisa/api/admin/schema.py:1339`). But "validates connection" holds only for `govdata`
  (`schema.py:1342`); for relational/NoSQL types `_add_source_pool` catches and only logs
  connection failures (`schema.py:1240`), so a bad connection still registers — a silent
  fallback contrary to the spec and to the project's no-silent-handling rule.
- **013 — To spec.** Per-role GraphQL schemas are generated exclusively from
  `table_repo.list_all()` (`provisa/api/app.py:1936`); an unregistered row has no queryable
  surface.
- **014 — To spec.** Root-field and visible-table generation iterate only `si.tables`
  (`provisa/compiler/schema_gen.py:303`, `:85`); unregistered tables cannot be referenced,
  browsed, or targeted.
- **015 — To spec.** `governance_mode`/`registry_required` appear nowhere; Stage-2 governance
  applies uniformly per table id (`provisa/compiler/stage2.py:34`), confirming the
  rights-based model with no per-table mode.
- **016 — To spec.** `register_table` ends with `await _rebuild_schemas()`
  (`provisa/api/admin/schema.py:1687`), regenerating every role's schema immediately
  (`provisa/api/app.py:1934`).
- **017 — Not to spec.** The spec requires NoSQL via automatic Parquet materialization,
  read-only. The Mongo adapter instead emits Trino Mongo-connector catalog properties
  (`provisa/mongodb/source.py:74`); no code materializes NoSQL to Parquet, and no
  NoSQL-specific read-only/no-mutation enforcement was found.
- **021 — To spec.** The schema generator builds from the registration model and applies
  per-table business aliases (`provisa/compiler/schema_gen.py:11`, `:376`), so the GraphQL
  surface reflects business intent, not raw DB structure.

### Relationships & FK discovery (REQ-018–020, 413–415)

- **018 — To spec.** `introspect_fk_candidates` reads FK metadata from Trino
  `information_schema` (`provisa/compiler/introspect.py:209`); `discovery/collector.py:209`
  turns those into `relationship_candidates` with a `status` of
  suggested/accepted/rejected/expired, giving stewards confirm/reject.
- **019 — Not to spec (requirement defect, not a code gap).** Manual cross-source
  relationships work via `upsert_relationship` (`provisa/api/admin/schema.py:1824`). The
  `Cardinality` enum has only `many-to-one` and `one-to-many` (`provisa/core/models.py:81`),
  matched by the DB CHECK (`provisa/core/schema.sql:125`) — `one-to-one` is **absent by
  design, and adding it would break things.** The relationship-field model is a strict binary
  keyed on the two literal strings via exhaustive `if/elif` with no `else`: a third value is
  silently dropped — no GraphQL field (`provisa/compiler/schema_gen.py:1046`), no gRPC field
  (`provisa/grpc/proto_gen.py:139`), and relationship discovery rejects it outright
  (`provisa/discovery/analyzer.py:59`). Functionally a one-to-one is identical to a
  many-to-one (both yield a singular related-object field / `json_object` / `IS_` Cypher
  type); the only difference — source-side uniqueness — is unenforced anyway (PKs informational,
  REQ-393). A true 1:1 is already expressible as a many-to-one in each direction. The fix is to
  revise REQ-019 to drop `one-to-one` (and document the bidirectional-many-to-one pattern),
  not to add a redundant enum value across ~6 binary call sites.
- **020 — Not added.** The `relationships` table (`provisa/core/schema.sql:119`) has no
  owner/steward, no version, and no re-review/stale flag; nothing flags relationships for
  re-review when join-field schemas change. (The `stale` enum at `schema.sql:199` belongs to
  MV freshness, not relationships.)
- **413 — To spec (relocated).** Auto-gen lives in `provisa/discovery/fk_introspect.py:225`,
  not `compiler/introspect.py` as the spec directs; it reads FK constraints and inserts both
  directions with `ON CONFLICT DO NOTHING`, preserving manual/AI relationships
  (`fk_introspect.py:286`).
- **414 — To spec.** `demo/files/create_demo_files.py:248` declares one FK
  (`user_id ... REFERENCES users(id)`), satisfying the ≥1 requirement.
- **415 — Not added.** `hasura_v2_relationship_style` appears nowhere; `inflection` is never
  imported and no pluralize/singularize occurs; the alias functions return the raw table name
  (`provisa/discovery/fk_introspect.py:146`).

### Table & dataset uniqueness / ownership (REQ-432–433)

- **432 — To spec.** `register_table` and `update_table` call `_domain_table_conflict`
  (`provisa/api/admin/schema.py:1631`, `:1744`) to reject `(domain_id, table_name)` collisions
  against a different physical table; startup `_assert_domain_table_unique`
  (`provisa/api/app.py:1595`) raises on any duplicate. Covered by
  `tests/unit/test_domain_table_uniqueness.py`.
- **433 — Not to spec.** Multi-domain association exists (`allowed_domains`,
  `provisa/core/models.py:171`) and the UI greys claimed tables
  (`provisa-ui/src/pages/TablesPage.tsx:732`), but first-come ownership is violated: the table
  upsert uses `ON CONFLICT ... DO UPDATE SET domain_id = EXCLUDED.domain_id`
  (`provisa/core/repositories/table.py:29`), letting any domain overwrite an existing claim.
  The required `UNIQUE(source_id, normalized_table_name)` is absent — the constraint is on
  literal `(source_id, schema_name, table_name)` (`provisa/core/schema.sql:60`) and
  normalization is client-side only, so the UI guard is bypassable.

### Views & governance (REQ-133–136, 366–367, 418)

- **133 — To spec.** Each `views:` entry becomes a Table dict with full `Column` models
  carrying `visible_to`, `unmasked_to`, mask fields, `alias`, `description`
  (`provisa/api/app.py:1178`, `provisa/core/models.py:272`).
- **134 — To spec.** Views materialize as ordinary tables and pass through the same Stage-2
  rewrite — visibility, masking, RLS WHERE, plus the row-cap (`provisa/compiler/stage2.py:231`,
  `:56`). The requirement's "sampling" is now the governance row cap; statistical sampling moved
  to the GraphQL `sample`/TABLESAMPLE path. "Approval workflow" is the per-table `approval_hook`
  flag (`provisa/core/models.py:351`), which a steward can attach to a view table.
- **135 — To spec.** `materialize:true` registers an `MVDefinition` refreshed via CTAS then
  DELETE+INSERT (`provisa/api/app.py:1189`, `provisa/mv/refresh.py:122`); non-materialized views
  populate `view_sql_map` and inline as subqueries (`provisa/compiler/view_expand.py:30`).
- **136 — To spec.** The only computed-SQL entry points are the config `views`/
  `materialized_views` blocks, registered as governed tables/MVs (`provisa/api/app.py:1111`); no
  alternate semantic-injection mechanism was found.
- **366 — Not to spec.** View/relationship creation is gated only by `create_view` /
  `create_relationship` capability (`provisa/api/admin/schema.py:1560`, `:1830`); there is no
  approval workflow and no check that the originator holds rights to the underlying tables or to
  joins within the view. `APPROVE_VIEW` and `APPROVE_RELATIONSHIP` are defined but never invoked
  (`provisa/security/rights.py:27`). Convenience views are neither detected nor discouraged.
- **367 — Incomplete.** The model is unified — one `view_sql` field on `Table`
  (`provisa/core/models.py:349`), each view registered to one `domain_id`. View deploy rejects
  SQL spanning multiple physical sources (`provisa/api/admin/schema.py:2303`), but this is a
  source-spanning guard, not the cross-domain-import semantic; the two hard constraints are held
  by convention, not affirmatively enforced. Named tests `test_domain_views.py` /
  `test_cross_domain_import_views.py` were not found.
- **418 — Incomplete.** Domain-local logic is enforced at query time by the SQL validator —
  V001 (FROM tables in role `domain_access`) and V002 (every join an approved relationship)
  (`provisa/compiler/sql_validator.py:8`) — and the import path is capability-gated. But, as with
  367, the cross-domain-import-only-via-view constraint is enforced only as a source-spanning
  rejection, and `tests/unit/test_domain_views.py` was not located.

### Materialized relationships (REQ-158–160)

- **158 — To spec.** After loading explicit MVs/views, the loader builds an `auto-mv-{rel_id}`
  MV for every relationship with `materialize:true` at startup (`provisa/api/app.py:1207`).
- **159 — To spec.** It generates an MV only when `src_source != tgt_source`
  (`provisa/api/app.py:1223`); same-source relationships are skipped.
- **160 — To spec.** `MVDefinition.status` defaults to `STALE` (`provisa/mv/models.py:76`);
  `get_due_for_refresh` treats a never-refreshed MV as due (`provisa/mv/registry.py:60`) and the
  refresh loop populates it.

### Column path extraction (REQ-151–153)

- **151 — To spec.** Path cols emit PG `->'k'...->>'final'` (`provisa/compiler/sql_gen.py:1876`);
  SQLGlot transpiles `->>` to Trino `JSON_EXTRACT_SCALAR` (verified).
- **152 — To spec.** `provisa/transpiler/router.py:162` forces `Route.TRINO` when JSON extract
  is present and the source dialect is not postgres; PG path cols continue direct.
- **153 — To spec.** Path extraction exists only in the SELECT builder; `mutation_gen.py` has no
  path/JSON logic and mutations route direct — "mutations unaffected" holds.

### Naming (REQ-154–157, 194–195)

- **154–157 — To spec.** `domain_prefix` prepends `{domain}__`
  (`provisa/compiler/schema_gen.py:417`); column/table `alias` override GQL names (`:514`,
  `naming.py:245`); `description` flows into the SDL (`:520`); order-by enum fields preserve
  original column case (`:601`).
- **194 — Not to spec.** The single-authority functions exist — `apply_sql_name`/`apply_gql_name`
  (`provisa/compiler/naming.py:164`) with alias-as-canonical priority, SQL default `snake`, GQL
  default `apollo_graphql` (camelCase). But the spec requires the `hasura_graphql` GQL convention
  to be snake_case, whereas `_canonical_convention` returns camelCase for it
  (`naming.py:140`) — only the literal `snake` preset yields snake_case.
- **195 — Not to spec.** No code maps Hasura's literal config strings `hasura-default`,
  `graphql-default`, or DDN `namingConvention: graphql` to internal presets;
  `VALID_CONVENTIONS` is `{snake, hasura_graphql, apollo_graphql}` (`naming.py:133`), and
  `hasura_graphql` produces camelCase, breaking Hasura v2 parity.

### JSONB promotion (REQ-119)

- **119 — Incomplete.** `dot_path_to_pg_expression` and `generate_promotion_ddl` emit correct
  `GENERATED ALWAYS AS (...) STORED` DDL with dot-path extraction
  (`provisa/api_source/promotions.py:27`), `PromotionConfig` exists, and unit tests pass. But
  `generate_promotion_ddl` is never called and the loader passes no `promotions_map`
  (`provisa/api_source/loader.py:123`), so promoted columns are never created or registered —
  scaffolded, not wired.

### Semantic layer (REQ-363)

- **363 — To spec.** `ProvisaDialect.get_table_names()`/`get_columns()` introspect via
  `POST /data/graphql` with an `X-Role` header and a `__schema` query
  (`provisa-client/provisa_client/sqlalchemy_dialect.py:102`); the endpoint executes against the
  per-role filtered schema (`provisa/api/data/endpoint.py:330`). A test asserts it hits
  `/data/graphql`, not `/admin/graphql`.

### PK / key designation (REQ-392–394, 399–400)

- **392 — Incomplete.** The `/data/graph-schema` endpoint emits a per-label `pk_columns` string
  array plus `id_column` (`provisa/api/rest/cypher_router.py:519`), not the singular
  `pk: string | null` field the spec names; the UI derives one PK via `pkCols[0] ?? null`
  (`GraphFrame.tsx:188`). All capabilities work, but the wire contract differs and no
  `provisa/graph/schema.py` exists.
- **393 — To spec.** `is_primary_key: bool = False` on `ColumnConfig`
  (`provisa/core/models.py:289`), persisted (`schema.sql:79`), informational only — no CHECK/
  UNIQUE generated.
- **394 — To spec.** Multiple PK columns are collected in column order and the first becomes the
  canonical `id_column` as step 0 before all heuristics (`provisa/compiler/sql_gen.py:307`,
  `provisa/cypher/label_map.py:492`).
- **399 — To spec.** `is_foreign_key`/`is_alternate_key` are added via
  `ALTER TABLE ... ADD COLUMN IF NOT EXISTS` inside the init-time block (`schema.sql:100`) — run
  at schema-init, not a migration, honoring the V1 rule; the source column is marked FK on save
  (`provisa/core/repositories/relationship.py:82`).
- **400 — To spec.** On save the target column is set PK if no other PK exists in that table,
  else AK (`provisa/core/repositories/relationship.py:92`).

### Migration & creation requests (REQ-417, 434)

- **417 — Not added.** The parser warns "Remote schema skipped (not supported)"
  (`provisa/hasura_v2/parser.py:314`) and only stashes them; the mapper has a comment but no code
  mapping remote schemas to `graphql_remote` registrations (`provisa/hasura_v2/mapper.py:476`),
  despite the capability existing. No mapping test.
- **434 — Not added.** No persisted creation-request table, model, or queue, and no execute/
  reject mutations anywhere; `provisa/registry/` does not exist. No path converts an unauthorized
  governed-create into a persisted request, and `tests/integration/test_creation_requests.py`
  does not exist.

## Named tests

Several spec-named tests are missing or differently named:
`tests/unit/test_table_uniqueness.py` (actual: `test_domain_table_uniqueness.py`),
`tests/unit/test_domain_views.py`, `tests/unit/test_cross_domain_import_views.py`,
`tests/integration/test_creation_requests.py`, `tests/integration/test_hasura_migration.py`,
`tests/unit/test_composite_pk.py`, `tests/unit/test_relationship_columns.py`,
`tests/unit/test_graph_schema.py` were not located during the audit and should be confirmed or
added alongside the corresponding fixes.

## Remaining tasks

Prioritised by type. Effort: S ≈ <½ day, M ≈ ~1 day, L ≈ multi-day.

| # | REQ | Type | Effort | Task |
| --- | --- | --- | --- | --- |
| 1 | 012 | Incomplete | S | Validate the connection for all source types at registration; reject (not log) failures |
| 2 | 119 | Incomplete | M | Wire JSONB promotion end-to-end: call `generate_promotion_ddl` and pass `promotions_map` through the loader so generated columns are created + registered |
| 3 | 250 | Incomplete | M | Generate Trino catalog `.properties` + table-def files from YAML at startup (Kafka exemplar); stop relying on hand-authored files |
| 4 | 251 | Incomplete | M | Expose the Redis/ES/Prometheus mapping DSLs in YAML config + `Source` model + config_loader |
| 5 | 366 | Not to spec | L | Enforce view/relationship approval-or-originator-rights at creation (`APPROVE_VIEW`/`APPROVE_RELATIONSHIP` + underlying-table/join rights check) |
| 6 | 367/418 | Incomplete | M | Enforce the cross-domain-import-only-via-view constraint at the domain level (not just source-spanning); add `test_domain_views.py` |
| 7 | 433 | Not to spec | M | Enforce first-come table ownership: add `UNIQUE(source_id, normalized_table_name)` and stop `ON CONFLICT DO UPDATE` overwriting a claim |
| 8 | 194/195 | Not to spec | S | Fix Hasura-parity naming: `hasura_graphql` → snake_case; map literal `hasura-default`/`graphql-default`/DDN `graphql` |
| 9 | 017 | Not to spec | L | NoSQL via automatic Parquet materialization, read-only (or revise the requirement to match the Trino-connector approach) |
| 10 | 019 | Not to spec | S | Revise the requirement: drop `one-to-one` as redundant (1:1 collapses to many-to-one; the binary field model silently drops a third value). Document the bidirectional-many-to-one pattern. Do **not** add the enum value. |
| 11 | 020 | Not added | M | Add owner, version, and re-review/stale flag to relationships; flag on join-field schema change |
| 12 | 392 | Incomplete | S | Return the singular `pk: string\|null` field per node label as specified |
| 13 | 415 | Not added | S | Add `hasura_v2_relationship_style` driving inflection-based singular/plural FK relationship names |
| 14 | 417 | Not added | M | Implement the Hasura Remote Schema → `graphql_remote` mapper path |
| 15 | 434 | Not added | L | Build the creation-request queue (persisted request, queue view, execute/reject with reason) |
