# Extension seams

What a new instance of each extension pattern takes today, read from the code at `e67dcc607`
(2026-10-19). For each pattern: the files a new instance touches, whether each is a declared seam
(a registry or declaration the pattern is meant to extend) or a core edit (the query pipeline,
governance, security), the tests and gates that hold it, and what would turn each core edit into
a seam. Paths are repo-relative; line numbers are at that commit.

Legend: **seam**, a declaration or registry meant to be extended; **list**, a hand-kept list
outside core that must be edited and that nothing checks against the others; **core**, the query
pipeline (`compiler/`, `pgwire/_pipeline.py`, `transpiler/`), governance or security code.

## Summary

| Pattern | Verdict | Core edits today |
|---|---|---|
| Branded source over an existing protocol | Seam on the backend; UI lists are hand-kept | none |
| Fully new source type | Core edits for any type that is not a plain RDBMS, or a SaaS behind a pgwire bridge | router, `_pipeline.py`, materialization, write path |
| Named demo bundle | Seam | none |
| Model change: a new instance | Seam (pure data) | none |
| Model change: a new kind (mask type, rule target, write operation) | Core edits | `masking.py`, `stage2.py`, `row_filters.py`, `rls.py`, `write_admission.py` |
| Operator setting | Seam | only the reader, when the setting changes query behaviour |
| UI page in the shared layout | Seam, registered by hand in 3–4 places | none (a new right touches `security/rights.py`) |
| Fake kind | Seam for the compiler and governance; hand-kept `isinstance` chains inside `provisa/fakes/` | none |
| Derived SQL function evaluated by Provisa (`embed()`, `cosine_similarity`) | Core edits | `stage2.py`, `_pipeline.py`, `transpile.py` |
| Auth provider | Mostly a seam | `security/role_probe.py`, `pgwire/server.py` |
| Role-mapping rule kind | Local edit outside core | none (meta-role composition: `security/meta_role.py`) |
| Protocol/transport | Seam at the pipeline for a buffered transport; core edits for streaming and gates | `_pipeline.py` terminal duties, `high_security.py`, `role_probe.py`, `limits.py` |

## 1. New source or connector

### 1a. Branded source over an existing protocol (GitHub over `graphql_remote`, Stripe over `openapi`)

| Step | Files | Kind |
|---|---|---|
| Brand entry | `provisa/graphql_remote/brands.py:113-149` (`BRANDS`, `Brand` at :43); `provisa/openapi/brands.py:68-76` | seam |
| Baked schema or spec | `provisa/graphql_remote/_known_schemas/<id>.json.gz` (`scripts/bake_graphql_brand_schema.py:22`); `provisa/openapi/_known_specs/<id>.json.gz`, optional `.additions.json`/`.exclusions.json`; packaged by glob, `pyproject.toml:324-326` | seam (data) |
| UI type, carrier and auth | `provisa-ui/src/pages/sources/constants.ts`: `SOURCE_TYPES` :195-200, `BRAND_CARRIER` :318-323, `BRAND_AUTH` :329-334 | list (duplicates `BRANDS`; `GET …/brands` at `graphql_remote_router.py:636-643` is served and never called by the UI) |
| Logo | `provisa-ui/src/components/SourceLogo.tsx:92-94` | list |
| Counts | `provisa-ui/src/i18n/locales/*/tour.json:88` (13 locales); `docs/sources.md` and its 9 translations | list (English tour count and the docs index are tested; the other locales' tour counts are not) |
| Generated | `README.md`, `site/*.html`, `site/assets/source-marks.*.svg` by `scripts/gen-site-source-picker.mjs` (`--check` in `ui-unit-tests.yml:52-53`) | generated |

Tests: `tests/unit/test_graphql_brand_source.py`, `test_openapi_brand_source.py`,
`provisa-ui/src/__tests__/sourceBrand.test.ts`, `provisa-ui/e2e/source-to-query-stripe.spec.ts`.

Limits: `Brand.auth()` always sends a bearer token (`graphql_remote/brands.py:72-73`,
`openapi/brands.py:55-56`); `BrandAuth` is `"token" | "none"` (`constants.ts:328`); a brand on a
carrier that is not yet a brand carrier needs backend registration code.

To make it clean: the UI reads its brand list, carrier, auth and category from `GET …/brands`
instead of `BRAND_CARRIER`/`BRAND_AUTH`; the tour counts in 13 locales and the docs counts are
written by `gen-site-source-picker.mjs` (or interpolate `{{count}}` from `SOURCE_TYPES.length`).

### 1b. Fully new source type

| Step | Files | Kind |
|---|---|---|
| Type | `provisa/core/models.py:63-207` (`SourceType`) | seam |
| Dialect, live-only | `provisa/core/source_registry.py:29` (`SOURCE_TO_DIALECT`), :91 (`LIVE_ONLY_TYPES`) | seam |
| Reader | `provisa/executor/drivers/registry.py:128` (`_DRIVER_FACTORIES`) or :172; `provisa/source_adapters/registry.py:29` (`_ADAPTER_MAP`); `provisa/events/source_loader.py:43`; `provisa/events/app_wiring.py:189-296`, :334-395 | seam, several by hand |
| Landing strategy | `provisa/federation/strategy.py:83-85` (`_CONNECTOR_PGWIRE_REPLICA`), :89-157 (`_MATERIALIZE_ONLY`) | seam |
| Engine attach, one class per engine | DuckDB `connector_duckdb.py:491` listed at `engine.py:594/659`; pgserver `connector_duckdb.py:1220` at `engine.py:749/798`; ClickHouse `clickhouse_connectors.py:151` at `engine.py:849/892`; Trino `trino_connectors.py:1120-1160` or `_TRINO_JDBC_TYPES` :223 | seam, listed by hand per engine |
| Calcite pgwire bridge | `provisa/federation/pgwire_replica.py:55-57` (`PGWIRE_REPLICA_TYPES`, a second copy of the strategy set), :124, :490; `provisa/runtime_deps/pgwire_bundles.py:59-64`; `provisa/executor/writable.py:64-73`; `provisa/federation/served_tables.py:35` | list |
| Admin dispatch | `if t == …` chains in `provisa/api/admin/introspect.py` (:276, :365, :1465, :1896, :1959), `schema_mutation.py:3355`, `config_loader.py:571,583`, `_import_set.py:69`, `app_rebuild.py:310` | list |
| UI | `constants.ts:42` (`SOURCE_TYPES`) plus a field-shape set (:250, :303, :307); a `<X>Fields.tsx` dispatched in `SourceFormFieldsExtended.tsx`; `SourcesPage.tsx`, `sourceSeed.ts:198`, `SourceLogo.tsx`; locales ×13 | list |
| Routing | `provisa/transpiler/router.py:49-83` (`VIRTUAL_SOURCES`), needed when the type registers a direct driver that must not route DIRECT | **core** |
| Native partial execution | `provisa/pgwire/_pipeline.py:513-525` (Qdrant DIRECT), :632 (`_similarity_needs_engine`); `transpiler/transpile.py:351` | **core** |
| Read per statement | `provisa/api/data/materialization.py:123-136,198-199` (Cosmos DB `_is_native_read`) | **core** |
| Write path | `provisa/executor/write_capability.py:94,98,145`; `provisa/api/data/pgwire_write.py:103-105` | **core** |

Found automatically: engine reach (`live_source_types`/`reachable_source_types`, `engine.py:1541-1585`;
`complete_reach` :315-344), served at `source_kinds_router.py:44-62` and read by the picker
(`sourceHelpers.ts:60-83`).

Tests: per type (`tests/unit/test_jira_connector.py`, `test_cosmosdb_source.py`,
`test_qdrant_source.py`), `test_dialect_expansion.py:87-90`, `provisa-ui/e2e/engine-swap.spec.ts`
(registrars imported by hand at :114-154). No test checks that every `SourceType` has a UI entry,
an engine-swap case or a docs row.

To make it clean:
1. Router: derive `VIRTUAL_SOURCES` from a driver flag (`DirectDriver.routes_direct = False`) or from `_MATERIALIZE_ONLY`.
2. `_pipeline.py`: an optional driver hook (`route_override(decision, sql)`, `handles_similarity`) looked up in the driver registry.
3. `materialization.py`: a loader attribute `reads_per_statement(table)` looked up by type.
4. Write path: `write_capability(table)` on the adapter module.
5. One `SourceKind` descriptor per type (dialect, driver, adapter or loader, strategy set, pgwire factory and operands, writable, field shape), found by package scan; the enum, the per-engine connector lists, `PGWIRE_REPLICA_TYPES` and the introspect chains become projections of it.
6. A unit test that every `SourceType` has a `SOURCE_TYPES` entry and a `docs/sources.md` row.

## 2. New named demo bundle

| Step | Files | Kind |
|---|---|---|
| The demo | `demo/named/<name>/`: `config.yaml`, `demo.yaml`, `tour.yaml`, `queries/*.graphql`, `card.png`, optional compose files, `setup.yaml`, `seed.py`, `db/`; scaffolded by `scripts/new_demo.py:107-141` | seam |
| Tour words | `provisa-ui/src/i18n/locales/*/tour.json` (`tour.steps.<name><Step>`, `tour.demos.<name>`), English by `new_demo.py:144-153` | seam |
| Site | `site/demos.html` by `scripts/gen-site-demos.mjs` (`--check` in `ui-unit-tests.yml:47-48`) | generated |
| Cloud sandbox | `packaging/linux/demo-stores/sandbox-tier-bundles` (or `regions-tier-bundles`) and the same string in `packaging/linux/first-launch.sh:950`/:1009 (matched by `test_deploy_cloud_store_env.py:127-130`) | list (two copies) |
| Build-time secrets | `.github/workflows/demo-bundles.yml:79-86` | list |
| Published table | `docs/named-demos.md:633-642` ("Three demos are published"; 17 are) | list (stale) |

Found automatically: `start-ui-install.sh` (`--demo`, `--list-demos`), `demo-bundles.yml:45-62`,
`gen-site-demos.mjs:37-60`, `provisa/core/demo_bundles.py:417-426`, and the globbed tests
(`test_named_demo_tour.py:66`, `test_named_demo_columns.py:25`,
`test_named_demo_relationship_types.py:25`, `test_named_demo_example_queries.py:171`).

Gates: `scripts/check_demo.py:210-219` runs seven checks but no workflow runs it;
`tests/unit/test_new_demo_scripts.py:125` runs its static checks for `weblogs`, `sonnets` and
`files` only.

To make it clean: parametrize `test_new_demo_scripts.py:125` over every published demo;
`first-launch.sh` reads the `demo-stores/*-bundles` files (as `sandbox.tf:33-34` does); generate
or delete the published table in `docs/named-demos.md`; declare build secrets in `demo.yaml`
(`build_env: [...]`) and pass them generically.

## 3. Model changes

### 3a. A new instance (another table, relationship, role, row rule, mask entry, write grant)

| Step | Files | Kind |
|---|---|---|
| Declaration | `provisa/core/models.py`: `ProvisaConfig` :2913-2925, `Table.write_grants` :1295, `Column.masks` :911 | seam |
| Apply | `provisa/core/config_loader.py` (tables :741, relationships :917/:1002, rules :1022, roles :1125); masks and grants ride on the table upsert (`repositories/table.py:630`) | seam |
| Admin API | `provisa/api/admin/schema_mutation.py` (roles :3071, tables :3083/:3204, grants :3738, rules :3840, relationships :4141-4153); REST `roles_router.py:160`, `derived_roles_router.py:90`, `masks_router.py:51` | seam |
| UI | `pages/security/SecurityRlsPage.tsx`, `RoleFormParts.tsx`, `DerivedRoles.tsx`, `pages/tables/WriteGrantsPanel.tsx`, `components/MaskListEditor.tsx`, `pages/RelationshipsPage.tsx` | seam |

Picked up on every rebuild (`_rebuild_schemas`, `provisa/api/app.py:2753`): rules through
`materialize_rls` (`security/inheritance.py:218`) and `build_rls_context` (`compiler/rls.py:97`);
masks through `_load_masking_rules` (`app_loaders.py:1300-1415`) into `compiler/stage2.py:186-190`;
grants through `stage2.py:248-249` into `compiler/write_admission.py:196`. Pure data: no code edits.

### 3b. A new kind of element

| Kind | Files | Kind of edit |
|---|---|---|
| Mask type | `security/masking.py`: `MaskType` :34-40 and three `if/elif` chains (:100-134, :181-201, :290-308), `rule_of` :333-341; `core/models.py:879` (a second allow-list); `compiler/stage2.py:1150` and `MaskType.fake` branches :383, :407, :876, :1159; `compiler/sql_validator.py:710`; `api/admin/types.py:554-578`; UI `lib/masks.ts:20`, `MaskListEditor.tsx:110-114`; `docs/configuration.md:756-762` | **core** |
| Row-rule target | `models.RLSRule` :1912-1937; `schema.sql:489-503`; `repositories/rls.py:44`; `schema_mutation.upsert_rls_rule` :3877-3909; `security/inheritance.py:218`; `security/row_filters.py:79-120`; `compiler/rls.py:49-66,108-116`; UI `SecurityRlsPage.tsx`, `RlsRuleRead.tsx` | **core** |
| Write operation | `models.py:1088` (`WRITE_OPERATIONS`), :1104, :1111; `schema.sql:513` (CHECK); `compiler/write_admission.py:137-142`, :422, :598-673; `executor/write_capability.py:58`; `WriteGrantsPanel.tsx:34` | **core** |
| Relationship cardinality | `models.py:210` (`Cardinality`; m:n is refused by design, :211-218); `RelationshipsPage.tsx:349` | seam plus whichever core reader uses it |

Each kind is an `if/elif` over a string; nothing is discovered.

To make it clean:
1. Masks: a `MASK_KINDS` registry (fields, `validate`, `sql`, `py`, traits such as `substitutes`, `preserves_numeric`) that `stage2`, `sql_validator`, `models.Mask` and `rule_of` read; the admin API and UI fetch the kinds and their fields from one endpoint.
2. Row-rule targets: one `RLS_TARGETS` table (name, column, resolver) that `resolve_table_rules`, `materialize_rls`, the mutation and `RLSRule._described` iterate.
3. Write operations: one `WRITE_OPERATIONS`, imported by `write_admission.py` and `write_capability.py` and served to the UI; a unit test holds the SQL CHECK to it.

Gates: `required_description` on roles and rules (`models.py:1819`, :1926); save-time predicate
validation (`compiler/rls_validate.py`, called at `schema_mutation.py:3910`); `tests/unit/test_rls.py`,
`test_rls_validate.py`, `test_write_grants_*.py`, `test_write_admission_every_surface.py`. No test
holds the UI's mask kinds or write operations to the backend's.

## 4. New operator setting (and its UI surface)

| Step | Files | Kind |
|---|---|---|
| Declaration | one `Setting(...)` in `provisa/core/settings_catalog.py:40` or a split file (`settings_catalog_{telemetry,engine,redirect,tiers,connectors,sandbox}.py`, joined at :1218-1226); dataclass `core/settings_registry.py:80-119` | seam |
| Registry, resolution, persistence | `settings_registry.py` (`_ensure_loaded` :181, `parse` :309, `resolve` :424, `store` :721, `describe` :769) | none (generic) |
| API | `provisa/api/admin/settings_catalog_router.py` (GET :90, PUT :148) | none (generic) |
| UI | `provisa-ui/src/components/admin/SettingField.tsx` (`SettingsCatalogPanel` :761, mounted at `pages/AdminPage.tsx:527`) | none (generic) |
| Words | `settings.<key>.label`/`.help` in `provisa-ui/src/i18n/locales/en/settings.json`, then 12 locales | seam |
| New card | `CARDS` (`settings_registry.py:52-65`) and `adminPage.setting.card.*` | list |
| Cross-setting rules | `_AT_LEAST` (`settings_registry.py:624`); `api/admin/settings_guards.py` (:34, :44, :110, :153, :208) | list |
| Reader | `settings_registry.value(key)` (:516) or `on_change(keys, fn)` (:581) where the behaviour lives | **core** only when the setting changes query behaviour, which is inherent |

A second path duplicates the catalog: `GET/PUT /admin/settings` (`settings_router.py:244`, :648)
and the hand-built cards in `components/admin/settingsCards.tsx` (:100, :321, :373, :447). Per-org
overrides are a third (`core/org_settings.py:46`).

Gates: `tests/unit/test_settings_registry.py:705` (well-formed declarations, `card ∈ CARDS`,
`req` set); `test_settings_readers.py:191` (no direct env read of a declared setting);
`test_ui_locale_parity.py:65`. Nothing checks that every declared setting has its label and help
in `en/settings.json`; a missing one renders the raw key.

To make it clean: a unit test that every `all_settings()` key has `label` and `help` (and map
`keys`, list `columns`), and every card its title; move `settings_guards` and `_AT_LEAST` onto
`Setting` fields; retire the hand-built `/admin/settings` blocks and cards onto the catalog;
generate a settings reference page from `all_settings()`.

## 5. UI page in the shared layout

| Step | Files | Kind |
|---|---|---|
| Route | `provisa-ui/src/App.tsx`: `lazy()` import (:50-113) and a `<Route>` inside `CapabilityGate` (:303-584), or an admin `[path, capability]` row (:597-636) plus `ROUTE_TO_SECTION` (`pages/AdminPage.tsx:76`) and a tab branch (:307-548) | list |
| Nav | `provisa-ui/src/components/navGroups.ts:72` (`NAV_GROUPS`) | list |
| Words | `provisa-ui/src/i18n/locales/en/<ns>.json` and 12 locales, merged by glob in `src/i18n/index.ts` | seam |
| Tour (optional) | `src/tour/tourSteps.ts:223` (`TOUR_STEPS`), :738 (`TOUR_SCOPES`), :700 (`TOPIC_IDS`) | list |
| Layout | `components/list/SplitPane.tsx`, `ListTable.tsx`, `useListSortGroup.ts`; `components/form/FormLayout.tsx` (`FormRead`, `FormLayout`, `FormField`, `FormSections`, `FormFooter`); `components/detail/DetailFields.tsx` | none (shared) |
| Section tabs | per page (`pages/tables/TableSectionTabs.tsx`, `pages/sources/SourceSectionTabs.tsx`) | list |
| A new right | `src/types/auth.ts:12`; `provisa/security/rights.py:41`; role seed `provisa/core/schema.sql:1379` | **core** (security) |

Guards: automatic, over every file: `formLayoutGuard.test.ts`, `panelHeadGuard.test.ts`,
`collapsibleGuard.test.ts`; by route: `tourRouteCapabilities.test.ts`,
`adminNavCapabilities.test.ts`, `tourScopes.test.ts`. Opt-in lists, which a new page joins only by
hand: `ListPagesShared.test.ts:18` (`LIST_PAGES`), `readViewGuard.test.ts:31` (`PANELS`). No
guard covers "minimal modals" (6 `pages/*.tsx` use `<Modal`), and no repo test holds the 13
locales to English's keys and `.tm.json` (`locale-diff.mjs --validate` lives in the skill).

To make it clean:
1. One `PAGES` descriptor (`path`, `component`, `capability`, `navGroup`, `labelKey`, `layout: "list" | "explorer"`) that `App.tsx`, `navGroups.ts` and `AdminPage` map over.
2. `ListPagesShared` and `readViewGuard` take their files from it (`layout === "list"`).
3. A vitest for key parity across the 13 locales and `.tm.json`.
4. A generic `SectionTabs<T>` in `components/form/`.

## 6. New fake kind, or new derived SQL function

### 6a. Fake kind

| Case | Files | Kind |
|---|---|---|
| A Faker method | discovered by `provisa/fakes/methods.py:282` (`method_names`); computed in Python for DuckDB (`fakes/duckdb_functions.py`) and PL/Python for PG (`fakes/pg_functions.py:66`); Trino needs `trino-functions/.../FakeMethods.java` `METHODS` (:290/:342) and `ARGUMENTS` (:237) | seam plus a Java list |
| A Provisa kind | `provisa/fakes/kinds.py` (dataclass :41-197, `FakeKind` :199, `INDEPENDENT` :222, `_BUILDERS` :748, `RULE_ONLY` :390); `fakes/checks.py:176-218`; `fakes/read_sql.py:236-375`; `fakes/catalog.py:33` (`KINDS`, served by `api/admin/fakes_router.py:49`); a new category's words in `en/testData.json:15` | seam, synchronized by hand |
| A new UDF | DuckDB `register`, PG `FUNCTIONS_SQL`, Trino `ProvisaFunctionsPlugin.java`/`FakeFunctions.java` | seam ×3 |

No core edit: `compiler/stage2.py:390` (`_bind_fakes`) and `_project_faked` call the generic
`parse`/`faked_projection`; routing (`transpiler/router.py:205-218`) and the engine gate
(`federation/runtime.py:122-124`) are generic. Inside `provisa/fakes/` a missing `checks.py`
branch fails open (falls through to `frozenset()`), a missing `read_sql.py` branch fails closed
(`read_sql.py:371`).

Tests: `test_fake_kinds.py`, `test_fake_reads.py`, `test_fake_engine_functions.py`,
`test_fake_pg_functions.py`, `test_fake_method_arguments.py:32` (Python and Java `ARGUMENTS`
equal); `tests/integration/test_fake_method_coverage_e2e.py:17` (methods on Trino, integration
lane only). Nothing holds `catalog.KINDS`, `checks` and `read_sql` to `_BUILDERS`.

To make it clean: one `KindSpec` per kind that `_BUILDERS`, `catalog.KINDS`, `checks` and
`read_sql` dispatch through; until then a unit test that every kind has all three, and the
`checks.py` fallthrough raises; a unit test that parses `FakeMethods.java` and compares it to
`method_names() - UNSUPPORTED`.

### 6b. Derived SQL function

There is no function registry. An ordinary function passes through sqlglot
(`transpiler/transpile.py:325-330`), with hand-written per-engine rewrites (Trino :52-222, DuckDB
:229-262); an unsupported function fails only at the engine. The `sql()` fake's allow-list is
`provisa/fakes/sql_subset.py:81,112,121`.

A function Provisa evaluates or governs is wired into core by name:

| Function | Core sites |
|---|---|
| `cosine_similarity` (`provisa/vector/similarity.py`) | governance `compiler/stage2.py:545-555`; `pgwire/_pipeline.py:1751-1759`, :1994, :4764; routing `_pipeline.py:626-635`, :2462; lowering `transpile.py:347-352`; `federation/runtime.py:140` |
| `embed()` (`provisa/vector/embed_fn.py`) | fold `_pipeline.py:1760-1771`; after execution :3099-3104; result type :5186; plan field :215 |

To make it clean: `provisa/functions/registry.py` with a `ProvisaFunction` protocol (`name`,
`govern`, `fold`, `after_rows`, `needs_engine(dialect)`, `lower(sql, dialect)`, optional
per-dialect `unsupported`), with `similarity` and `embed_fn` registered, and the five sites
replaced by loops over it.

## 7. New auth provider, or role-mapping rule

| Step | Files | Kind |
|---|---|---|
| Provider class | `provisa/auth/models.py:76-125` (`AuthProvider`); `provisa/auth/providers/*.py` | seam |
| Construction | `provisa/auth/wiring.py:53-135` (an `if provider_name ==` chain) | list |
| Config | `provisa/core/models.py:2275-2305` (`AuthConfig`: one `dict | None` field per provider; `role_mapping: list[dict]` untyped, :2296) | list |
| Admin API and form | `provisa/api/admin/auth_settings.py:36-310` (`AUTH_PROVIDERS`; `oidc` missing though wiring accepts it); UI `components/admin/AuthTab.tsx` renders from it; `pages/LoginPage.tsx` has per-provider sign-in branches | seam (form), list (login) |
| First run, Helm, Hasura import | `provisa/api/setup_router.py:323`; `helm/provisa/templates/_auth.tpl:22`; `provisa/hasura_v2/mapper.py:1015-1027` | list |
| IdP-signed tokens | `provisa/security/role_probe.py:50` (`IDP_SIGNED`) | **core** (security) |
| pgwire password as token | `provisa/pgwire/server.py:278` (`_OIDC_PROVIDERS`) | **core** (transport auth) |
| Claim mapping rule | `provisa/auth/role_mapping.py:20-46` (`exact`/`contains`); called at five sites (`middleware.py:1080`, `bolt/session.py:313`, `auth/bearer.py:104`, `api/mcp/server.py:127`, `pgwire/server.py:2102`) | local edit, outside core |
| Derived-role rule | `auth/derived_roles.py:39-50`; `DerivedRole` (`models.py:1890`) | local edit |
| Meta-role composition | `provisa/security/meta_role.py` (RLS fold :298, masks :379, grants :147) | **core** |

Downstream code (RLS, masking, compiler) reads `AuthIdentity` and roles only, so a provider needs
no governance edit.

Tests: `tests/unit/test_auth_settings_registry.py:24-33` (pins the `AUTH_PROVIDERS` order),
`test_auth_providers.py`, `test_helm_auth.py`, `tests/integration/test_ldap_auth_provider.py`,
`test_auth_surface_conformance.py:342-350` (per surface, not per provider).

Defects found on the way: `docs/configuration.md:1111` lists five providers; its `role_mapping`
example (:1122-1125) uses `contains:` and `provisa_role:` keys that `resolve_role` never reads
(it reads `rule["value"]`, `role_mapping.py:34`), so that config raises `KeyError`;
`hasura_v2/mapper.py:1039` emits rules with no `value`.

To make it clean:
1. Class attributes on `AuthProvider` (`idp_signed`, `password_field_is_token`) replace `IDP_SIGNED` and `_OIDC_PROVIDERS`.
2. A `PROVIDERS` registry replaces the wiring chain.
3. `AUTH_PROVIDERS[*].config_fields` derived from each provider's settings dataclass; `AuthConfig` takes provider blocks generically; setup and Helm read the same list.
4. A typed `RoleMappingRule` with a `type` discriminator and a dispatch table, with the docs and mapper keys corrected.

## 8. New protocol or transport

| Step | Files | Kind |
|---|---|---|
| Listener and banner | `provisa/api/app_startup.py:314-561` (one block per transport, TLS by `_resolve_tls` :295) | list |
| Port | `provisa/core/settings_catalog.py:636-697` (MCP: `PROVISA_MCP_PORT`) | seam |
| HTTP router | `provisa/api/app.py` (:4429-4690) | seam |
| Pipeline | `_govern_and_route` (`pgwire/_pipeline.py:1287`), `_govern_and_route_compiled` (:4176), `_execute_plan` (:3027) | seam (called as is) |
| Streaming terminal | residency, live permits, cache tee, row-limit settle and `finalize_audit` redone in `grpc/server.py:1086-1180`, :1194+, and in `pgwire/server.py`, `flight/server.py`, `rest/cypher_router.py` | **core** (copied per transport) |
| Request thread (REQ-1882) | HTTP automatic (`RequestThreadMiddleware`, `app.py:4817`); others wire it themselves (`grpc/server.py:250-300`, `flight/server.py:393`, `mcp/server.py:369-392`, `LOOP_POOL`) | per transport |
| Timeout name | `provisa/core/limits.py:28-40` (`REQUEST_TRANSPORTS`; an unknown name raises, :43-47), :82-88, :92-100; the setting's keys derive from it | **core** |
| Time travel | `request_time_travel` (`api/data/endpoint_helpers.py:450`), GraphQL and SQL-HTTP only; REST, Flight, gRPC and MCP each their own; JSON:API, Cypher-HTTP, Bolt and pgwire none | per transport |
| High security | `provisa/security/high_security.py`: HTTP default-deny (:39-55); `pgwire_start_allowed` :63, `bolt_start_allowed` :73, `mcp_start_allowed` :83; `high_security_wire_reject` :105 called by hand | **core** (security) |
| Browser probe | `provisa/security/role_probe.py:53` (`NOT_A_BROWSER`), :70 | **core** (security) |
| Per-role artifact | `api/role_surface.py:33-41` (`RoleSurface`: fixed `proto`, `table_path_map`); `app_loaders.py:1665-1694`, :1848 | **core** (schema build) |
| Other lists | UI `settingsCards.tsx:70-82` (`TRANSPORTS`); `tour/tourSteps.ts:67`; NL targets `nl/executor.py:32`, `nl/job.py:31`, `nl/prompt.py:28`, `nl/runner.py:52,852`; `otel_setup.py:451-456` | list |

Gates that fail on their own: `test_auth_surface_conformance.py:398-430` (every
`validator_for_scheme(` caller is a listed surface); `test_request_timeouts.py:47,148`. The
every-transport integration tests (`test_request_thread_exclusivity_e2e.py`,
`test_request_timeout_every_transport.py`, `test_row_filter_every_transport.py`,
`test_redirect_every_transport_e2e.py`, `test_write_admission_every_surface.py` and nine more)
each keep their own transport list, so a new transport is not tested until it is added to each.

To make it clean:
1. A `Transport` declaration (name, port setting, `start`, TLS prefix, `is_browser`, high-security policy, audit surface, timeout key) in one module; `app_startup`, `REQUEST_TRANSPORTS`, `NOT_A_BROWSER`, the otel map and the UI's list derive from it.
2. One shared streaming terminal in `_pipeline.py` (`stream_plan(plan, state)` owning residency, permits, tee and audit); transports only serialize rows.
3. `high_security.wire_start_allowed(state, transport)` reads the declared policy.
4. A transport-neutral `time_travel_from(mapping)` beside `request_time_travel`, part of the transport contract.
5. `RoleSurface.artifacts: dict[str, Any]` filled by builders a transport registers.
6. The every-transport tests read the registry, so a new transport is covered or fails.

## Recurring causes

- A kind is a string compared in `if/elif` chains in several files (mask types, rule targets,
  write operations, auth providers, source types in introspect). A registry with the per-kind
  behaviour, which the chains iterate, is the common fix.
- The UI keeps its own copy of a backend list (brands, mask kinds, write operations, transports,
  source types). Serving the list, or a test that holds the copies equal, is the common fix.
- Gates are opt-in lists (layout guards, demo checks, every-transport tests). Deriving the list
  from the registry turns a forgotten entry into a failure.
- Core edits concentrate in four places: `pgwire/_pipeline.py` (per-source and per-function
  hooks, the streaming terminal), `compiler/stage2.py` (mask and function governance),
  `security/` (provider and transport names), and `transpiler/router.py` (`VIRTUAL_SOURCES`).
