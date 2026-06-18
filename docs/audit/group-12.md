# Audit — Group 12: Migration & Compatibility

Date: 2026-06-18
Scope: **Group 12 — Migration & Compatibility (Hasura)** (REQ-182–193, REQ-212–222),
spanning the Hasura v2 converter (`provisa/hasura_v2/`), the DDN v3 converter
(`provisa/ddn/`), the shared import layer (`provisa/import_shared/`), and the
Hasura-parity query/mutation features (`provisa/compiler/`, `provisa/scheduler/`,
`provisa/api/`).
Method: read implementation against requirement text with file:line evidence found
via Grep/Read. Companion to [group-2.md](group-2.md).

## Classification key

- **To spec** — implemented and matches the requirement
- **Incomplete** — partially implemented
- **Not to spec** — implemented differently than the requirement states
- **Not added** — required but missing

## Summary

| REQ | Sub-area | Status | Finding |
| --- | --- | --- | --- |
| 182 | Hasura Migration Converters | To spec | v2 parser + mapper read a metadata dir and emit validated Provisa config `provisa/hasura_v2/mapper.py:405` |
| 183 | Hasura Migration Converters | To spec | DDN HML parser + mapper convert ObjectTypes/Models/Relationships/permissions/connectors `provisa/ddn/mapper.py:454` |
| 184 | Hasura Migration Converters | Incomplete | Shared converter covers `_eq.._nin`, `_like/_ilike`, `_and/_or/_not`, `_is_null`; no `_regex`, and session vars emit `${X-Hasura-...}` not `current_setting('provisa.<name>')` `provisa/import_shared/filters.py:19` |
| 185 | Hasura Migration Converters | To spec | `select_permissions[].columns` per role → `visible_to`; `*` handled `provisa/hasura_v2/mapper.py:232` |
| 186 | Hasura Migration Converters | To spec | `insert/update_permissions[].columns` per role → `writable_by` `provisa/hasura_v2/mapper.py:250` |
| 187 | Hasura Migration Converters | To spec | `select_permissions[].filter` → `rls_rules[]`; empty filter skipped `provisa/hasura_v2/mapper.py:294` |
| 188 | Hasura Migration Converters | To spec | `object_relationships` → many-to-one, `array_relationships` → one-to-many `provisa/hasura_v2/mapper.py:306` |
| 189 | Hasura Migration Converters | To spec | DDN field→column via `dataConnectorTypeMapping[].fieldMapping` for cols/rels/perms `provisa/ddn/mapper.py:118` |
| 190 | Hasura Migration Converters | Not to spec | `--auth-env-file` flag exists but mapper only reads `AUTH_PROVIDER`/project_id/realm; no `jwk_url→oauth`, `claims_map→role_mapping`, admin-secret→superuser, webhook warning `provisa/hasura_v2/mapper.py:511` |
| 191 | Hasura Migration Converters | Not to spec | DDN aggregates folded into `table.description` annotation; no `provisa-aggregates.yaml` sidecar `provisa/ddn/mapper.py:524` |
| 192 | Hasura Migration Converters | To spec | Warnings for event_triggers/remote_schemas/cron/skipped kinds; webhook actions → Webhook, DB actions → Function `provisa/hasura_v2/mapper.py:359` |
| 193 | Hasura Migration Converters | To spec | Both CLIs call `ProvisaConfig.model_validate(...)` before write `provisa/hasura_v2/cli.py:142`, `provisa/ddn/cli.py:120` |
| 212 | Low-Complexity | To spec | `compile_upsert` emits `INSERT ... ON CONFLICT (...) DO UPDATE`; `upsert_<table>` field with `on_conflict` arg `provisa/compiler/mutation_gen.py:121` |
| 213 | Low-Complexity | To spec | `distinct_on` arg injects `SELECT DISTINCT ON (...)`; enum built per table `provisa/compiler/sql_gen.py:1986` |
| 214 | Low-Complexity | To spec | `ColumnPreset(source: header/now/literal)` applied + stripped before SQL `provisa/compiler/mutation_gen.py:52` |
| 215 | Low-Complexity | To spec | `Role.parent_role_id`; `flatten_roles` merges caps + domain_access up the chain `provisa/core/models.py:417` |
| 216 | Low-Complexity | To spec | `build_scheduler` registers cron jobs via APScheduler `CronTrigger.from_crontab` `provisa/scheduler/jobs.py:489` |
| 217 | Low-Complexity | To spec | Batch mutations loop selections, append results sequentially `provisa/compiler/mutation_gen.py:351` |
| 218 | Medium-Complexity | To spec | `first/after/last/before` args; `edges`/`pageInfo`/`PageInfo` relay types `provisa/compiler/schema_gen.py:255` |
| 219 | Medium-Complexity | To spec | SSE subscribe endpoint + `add_listener` LISTEN/NOTIFY change detection `provisa/api/data/subscribe.py:238` |
| 220 | Medium-Complexity | To spec | `ensure_pg_notify_triggers` installs pg triggers; `EventTrigger` config carries ops/retry `provisa/subscriptions/pg_triggers.py` via `provisa/api/app.py:2051` |
| 221 | Medium-Complexity | To spec | `enum_detect` introspects `pg_enum`/`pg_type`, builds `GraphQLEnumType` `provisa/compiler/enum_detect.py:60` |
| 222 | Medium-Complexity | To spec | `create_rest_router` mounts `GET /data/rest/{table}` with `where.col.op` params `provisa/api/rest/generator.py:119` |

Counts: 18 To spec, 1 Incomplete (184), 2 Not to spec (190, 191), 0 Not added.
The two converter CLIs and all 11 parity features are present and tested; the gaps
are in auth migration (190) and the aggregate sidecar (191), plus partial filter
operator/session-var coverage (184).

## Detail

### Hasura Migration Converters (REQ-182–193)

- **REQ-182 (To spec)** — `convert_metadata` builds sources, roles, tables, RLS,
  relationships, functions, webhooks, event/scheduled triggers, auth, domains and
  returns a `ProvisaConfig` `provisa/hasura_v2/mapper.py:405`; parser handles flat
  and `databases/` layouts `provisa/hasura_v2/parser.py:250`.
- **REQ-183 (To spec)** — `convert_hml` maps connectors→sources, models+objecttypes→
  tables, model/type permissions, relationships, commands→functions
  `provisa/ddn/mapper.py:454`.
- **REQ-184 (Incomplete)** — `_OPERATORS` covers `_eq _neq _gt _lt _gte _lte _like
  _nlike _ilike _nilike _in _nin _is_null` plus `_and/_or/_not/_exists`
  `provisa/import_shared/filters.py:19`. Two deviations from the requirement: (1)
  `_regex` is not in the operator table (named in the spec); (2) session variables
  render as `${X-Hasura-Name}` placeholders `provisa/import_shared/filters.py:39`
  rather than `current_setting('provisa.<name>')`. The DDN converter has the same
  session-var shape `provisa/ddn/mapper.py:319`.
- **REQ-185 (To spec)** — per-role columns become `visible_to`, with `*` collapsed to
  a wildcard Column `provisa/hasura_v2/mapper.py:232`.
- **REQ-186 (To spec)** — insert + update permission columns append to `writable_by`
  `provisa/hasura_v2/mapper.py:250`.
- **REQ-187 (To spec)** — non-trivial select filters become `RLSRule` via
  `bool_expr_to_sql`; `TRUE` (empty `{}`) skipped `provisa/hasura_v2/mapper.py:294`.
- **REQ-188 (To spec)** — `object_relationships` → `Cardinality.many_to_one`,
  `array_relationships` → `Cardinality.one_to_many`, physical columns used
  `provisa/hasura_v2/mapper.py:306`.
- **REQ-189 (To spec)** — `_build_field_to_column_map` reads
  `dataConnectorTypeMapping[].fieldMapping`; `_resolve_column` applied at column, RLS,
  and relationship sites `provisa/ddn/mapper.py:118`.
- **REQ-190 (Not to spec)** — the `--auth-env-file` flag and `_load_auth_env` exist
  `provisa/hasura_v2/cli.py:100`, but mapper auth handling reads only
  `AUTH_PROVIDER`, `FIREBASE_PROJECT_ID`, `KEYCLOAK_URL/REALM`
  `provisa/hasura_v2/mapper.py:511`. None of the required mappings are present:
  `jwk_url → provider: oauth`, `claims_map → role_mapping[]`, admin secret →
  `superuser`, webhook auth → warning.
- **REQ-191 (Not to spec)** — `_map_aggregate_expressions` produces an `AggConfig`,
  but it is written into `table.description` as `[aggregates: ...]`
  `provisa/ddn/mapper.py:524`; no `provisa-aggregates.yaml` sidecar is emitted by the
  mapper or CLI `provisa/ddn/cli.py:128`.
- **REQ-192 (To spec)** — `collector.warn` fires for event triggers
  `provisa/hasura_v2/mapper.py:349`, non-HTTP actions
  `provisa/hasura_v2/mapper.py:388`, and DDN skipped kinds
  `provisa/ddn/mapper.py:540`; webhook actions map to `Webhook`, others to `Function`
  `provisa/hasura_v2/mapper.py:359`. Conversion does not fail.
- **REQ-193 (To spec)** — both CLIs validate before writing
  `provisa/hasura_v2/cli.py:142`, `provisa/ddn/cli.py:120`.

### Low-Complexity Parity Features (REQ-212–217)

- **REQ-212 (To spec)** — `compile_upsert` emits `ON CONFLICT (...) DO UPDATE SET`
  over non-conflict columns `provisa/compiler/mutation_gen.py:121`; schema adds the
  `upsert_<table>` field with `on_conflict` arg `provisa/compiler/schema_gen.py:1222`.
- **REQ-213 (To spec)** — `distinct_on` arg injects `DISTINCT ON (...)` after SELECT,
  including the nested/lateral path `provisa/compiler/sql_gen.py:1086`,
  `provisa/compiler/sql_gen.py:1986`; per-table enum at
  `provisa/compiler/schema_gen.py:633`.
- **REQ-214 (To spec)** — `ColumnPreset` supports `header/now/literal`
  `provisa/core/models.py:346`; `apply_column_presets` injects values before SQL gen
  `provisa/compiler/mutation_gen.py:52`.
- **REQ-215 (To spec)** — `parent_role_id` on `Role`
  `provisa/core/models.py:410`; `flatten_roles` merges parent caps/domain_access/
  max_rows `provisa/core/models.py:417`.
- **REQ-216 (To spec)** — `build_scheduler` registers cron jobs from
  `ScheduledTrigger.cron` via APScheduler `provisa/scheduler/jobs.py:489`.
- **REQ-217 (To spec)** — mutation compilation iterates selections and appends each
  result, executing sequentially `provisa/compiler/mutation_gen.py:351`.

### Medium-Complexity Parity Features (REQ-218–222)

- **REQ-218 (To spec)** — relay args `after/before` `provisa/compiler/schema_gen.py:1160`
  and `first/last`; `PageInfo` with `hasNextPage/endCursor`, `edges`, `_connection`
  fields `provisa/compiler/schema_gen.py:255`.
- **REQ-219 (To spec)** — SSE subscription path uses asyncpg `conn.add_listener`
  `provisa/api/data/subscribe.py:238` with a streaming SSE responder
  `provisa/api/data/subscription_sse.py:204`.
- **REQ-220 (To spec)** — `ensure_pg_notify_triggers` installs DB triggers at startup
  `provisa/api/app.py:2051`; `EventTrigger` config carries operations + retry
  `provisa/core/models.py:468`.
- **REQ-221 (To spec)** — `enum_detect` queries `pg_enum`/`pg_type` and builds
  `GraphQLEnumType` instances + enum filter fields
  `provisa/compiler/enum_detect.py:60`.
- **REQ-222 (To spec)** — `create_rest_router` generates `GET /data/rest/{table}`,
  parsing `where.column.op=value` params `provisa/api/rest/generator.py:119`,
  mounted from `provisa/api/app.py:2813`.

## Named tests

All four spec-named files exist and pass (118 tests, `pytest` run 2026-06-18):

- `tests/unit/test_hasura_v2.py` — 50 tests
- `tests/unit/test_ddn.py` — 31 tests
- `tests/unit/test_upsert_distinct_on.py` — 17 tests
- `tests/unit/test_column_presets.py` — 20 tests

No named test is missing. Coverage gaps align with the two Not-to-spec findings:
REQ-190 auth-env mapping and REQ-191 sidecar output have no behavioral tests because
the behavior is unimplemented.

## Remaining tasks

| # | REQ | Type | Effort | Task |
| --- | --- | --- | --- | --- |
| 1 | 190 | Not to spec | M | Map `jwk_url → provider: oauth`, `claims_map → role_mapping[]`, admin secret → `superuser`, and emit a warning for webhook auth in `convert_metadata` auth handling (`provisa/hasura_v2/mapper.py:511`) |
| 2 | 191 | Not to spec | M | Emit `provisa-aggregates.yaml` sidecar from the DDN CLI rather than folding aggregates into `table.description` (`provisa/ddn/cli.py:128`, `provisa/ddn/mapper.py:524`) |
| 3 | 184 | Incomplete | S | Add `_regex` to the shared operator table (`provisa/import_shared/filters.py:19`) |
| 4 | 184 | Incomplete | S | Convert session variables to `current_setting('provisa.<name>')` instead of `${X-Hasura-...}` placeholders (`provisa/import_shared/filters.py:39`, `provisa/ddn/mapper.py:319`) |
