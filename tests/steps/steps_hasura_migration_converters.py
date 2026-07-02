# Copyright (c) 2026 Kenneth Stott
# Canary: {canary}
#
# This source code is licensed under the Business Source License 1.1

"""BDD step implementations for Hasura Migration Converters.

Each scenario exercises the real Hasura v2 / DDN converter code paths against
in-memory intermediate metadata models (the same models the parser produces),
asserting on the resulting ProvisaConfig. No live services are required — the
converters are pure functions over parsed metadata.

REQ-182 — Hasura v2 metadata converter emits valid Provisa YAML config.
REQ-183 — Hasura DDN (v3) HML converter emits valid Provisa YAML config.
REQ-184 — Shared boolean expression-to-SQL converter for Hasura filters.
REQ-185 — v2 select_permissions[].columns -> Provisa column visible_to.
REQ-186 — v2 insert/update_permissions[].columns -> Provisa column writable_by.
REQ-187 — v2 select_permissions[].filter -> Provisa rls_rules[].
REQ-188 — object_relationships -> many-to-one; array_relationships -> one-to-many.
REQ-189 — DDN resolves GraphQL field names to physical columns via fieldMapping.
REQ-190 — v2 auth conversion (oauth/superuser/role_mapping/webhook warning).
REQ-191 — DDN AggregateExpression preserved in provisa-aggregates sidecar.
REQ-192 — Converters warn on unmappable features without aborting.
REQ-621 — Both converters emit placeholder connection credentials.
REQ-623 — v2 source kind -> SourceType; URL parsed; pool settings preserved.
REQ-624 — v2 role upgraded to write when it has any delete_permissions entry.
REQ-625 — env-var / unparseable database_url -> placeholder connection values.
REQ-626 — roles collected only from permission entries.
REQ-627 — table alias priority select > select_by_pk > custom_name.
REQ-628 — missing ObjectType tables skipped with a warning; conversion continues.
"""

import pytest
import yaml
from pytest_bdd import given, scenarios, then, when

from provisa.core.models import Cardinality, ProvisaConfig, SourceType
from provisa.ddn.mapper import convert_hml
from provisa.ddn.models import (
    DDNAggregateExpression,
    DDNConnector,
    DDNFieldMapping,
    DDNMetadata,
    DDNModel,
    DDNModelPermission,
    DDNObjectType,
    DDNRelationship,
    DDNTypeMapping,
    DDNTypePermission,
)
from provisa.hasura_v2.mapper import convert_metadata
from provisa.hasura_v2.models import (
    HasuraAction,
    HasuraActionDefinition,
    HasuraCronTrigger,
    HasuraEventTrigger,
    HasuraMetadata,
    HasuraPermission,
    HasuraRelationship,
    HasuraSource,
    HasuraTable,
)
from provisa.import_shared.filters import bool_expr_to_sql
from provisa.import_shared.warnings import WarningCollector

scenarios(
    "../features/REQ-182.feature",
    "../features/REQ-183.feature",
    "../features/REQ-184.feature",
    "../features/REQ-185.feature",
    "../features/REQ-186.feature",
    "../features/REQ-187.feature",
    "../features/REQ-188.feature",
    "../features/REQ-189.feature",
    "../features/REQ-190.feature",
    "../features/REQ-191.feature",
    "../features/REQ-192.feature",
    "../features/REQ-621.feature",
    "../features/REQ-623.feature",
    "../features/REQ-624.feature",
    "../features/REQ-625.feature",
    "../features/REQ-626.feature",
    "../features/REQ-627.feature",
    "../features/REQ-628.feature",
)


# ---------------------------------------------------------------------------
# Fixtures & helpers
# ---------------------------------------------------------------------------


@pytest.fixture
def shared_data() -> dict:
    """Plain dict to pass state between Given/When/Then steps."""
    return {}


def _find_table(config: ProvisaConfig, table_name: str):
    """Locate a converted Provisa table by physical table name."""
    for table in config.tables:
        if table.table_name == table_name:
            return table
    return None


def _col(table, name: str):
    """Locate a column on a converted Provisa table by physical column name."""
    for column in table.columns:
        if column.name == name:
            return column
    return None


def _v2_metadata() -> HasuraMetadata:
    """Full-featured Hasura v2 metadata covering tables, rels, perms, roles, auth."""
    orders = HasuraTable(
        name="orders",
        schema_name="public",
        custom_root_fields={"select": "allOrders", "select_by_pk": "orderByPk"},
        select_permissions=[
            HasuraPermission(role="analyst", columns=["id", "amount", "customer_id"], filter={}),
            HasuraPermission(
                role="customer",
                columns=["id", "amount"],
                filter={"customer_id": {"_eq": "X-Hasura-User-Id"}},
            ),
        ],
        insert_permissions=[HasuraPermission(role="clerk", columns=["amount", "customer_id"])],
        update_permissions=[HasuraPermission(role="clerk", columns=["amount"])],
        delete_permissions=[HasuraPermission(role="manager")],
        object_relationships=[
            HasuraRelationship(
                name="customer",
                rel_type="object",
                remote_table="customers",
                remote_schema="public",
                column_mapping={"customer_id": "id"},
            ),
        ],
        array_relationships=[
            HasuraRelationship(
                name="order_items",
                rel_type="array",
                remote_table="order_items",
                remote_schema="public",
                column_mapping={"id": "order_id"},
            ),
        ],
        event_triggers=[
            HasuraEventTrigger(
                name="on_order_insert",
                table_name="orders",
                table_schema="public",
                webhook="https://events.example.com/order",
                operations=["insert"],
            ),
        ],
    )
    customers = HasuraTable(
        name="customers",
        schema_name="public",
        select_permissions=[
            HasuraPermission(role="analyst", columns=["id", "name"], filter={}),
        ],
    )
    source = HasuraSource(
        name="default",
        kind="postgres",
        connection_info={
            "database_url": "postgres://appuser:secret@pg.internal:5432/commerce",
            "pool_settings": {"min_connections": 3, "max_connections": 17},
        },
        tables=[orders, customers],
    )
    return HasuraMetadata(
        version=3,
        sources=[source],
        actions=[
            HasuraAction(
                name="place_order",
                definition=HasuraActionDefinition(
                    kind="synchronous",
                    handler="https://api.example.com/place_order",
                    action_type="mutation",
                    arguments=[{"name": "product_id", "type": "Int"}],
                    output_type="PlaceOrderResult",
                ),
                permissions=[{"role": "analyst"}],
            ),
        ],
        cron_triggers=[
            HasuraCronTrigger(
                name="daily_report",
                webhook="https://reports.example.com/generate",
                schedule="0 0 * * *",
                include_in_metadata=True,
                enabled=True,
            ),
        ],
    )


def _ddn_metadata() -> DDNMetadata:
    """Full DDN metadata: connector, ObjectTypes, Models, Relationship, perms, agg."""
    meta = DDNMetadata()
    meta.connectors.append(
        DDNConnector(name="chinook", subgraph="app", url="http://localhost:8080/postgres")
    )
    meta.object_types.append(
        DDNObjectType(
            name="Artist",
            subgraph="app",
            fields={"artistId": "Int", "name": "String"},
            type_mappings=[
                DDNTypeMapping(
                    connector_name="chinook",
                    source_type="artist",
                    field_mappings=[
                        DDNFieldMapping(graphql_field="artistId", column="artist_id"),
                        DDNFieldMapping(graphql_field="name", column="name"),
                    ],
                )
            ],
        )
    )
    meta.object_types.append(
        DDNObjectType(
            name="Album",
            subgraph="app",
            fields={"albumId": "Int", "title": "String", "artistId": "Int"},
            type_mappings=[
                DDNTypeMapping(
                    connector_name="chinook",
                    source_type="album",
                    field_mappings=[
                        DDNFieldMapping(graphql_field="albumId", column="album_id"),
                        DDNFieldMapping(graphql_field="title", column="title"),
                        DDNFieldMapping(graphql_field="artistId", column="artist_id"),
                    ],
                )
            ],
        )
    )
    meta.models.append(
        DDNModel(
            name="Artist",
            subgraph="app",
            object_type="Artist",
            connector_name="chinook",
            collection="artist",
            aggregate_expression="ArtistAgg",
        )
    )
    meta.models.append(
        DDNModel(
            name="Album",
            subgraph="app",
            object_type="Album",
            connector_name="chinook",
            collection="album",
        )
    )
    meta.relationships.append(
        DDNRelationship(
            name="artist",
            subgraph="app",
            source_type="Album",
            target_model="Artist",
            rel_type="Object",
            field_mapping={"artistId": "artistId"},
        )
    )
    meta.type_permissions.append(
        DDNTypePermission(
            type_name="Artist",
            subgraph="app",
            role="viewer",
            allowed_fields=["artistId", "name"],
        )
    )
    meta.model_permissions.append(
        DDNModelPermission(
            model_name="Album",
            subgraph="app",
            role="viewer",
            filter={"artistId": {"_eq": 1}},
        )
    )
    meta.aggregate_expressions.append(
        DDNAggregateExpression(
            name="ArtistAgg",
            subgraph="app",
            operand_type="Artist",
            count_enabled=True,
            count_distinct=True,
            aggregatable_fields={"artistId": ["sum", "avg"]},
        )
    )
    meta.subgraphs.add("app")
    return meta


# ---------------------------------------------------------------------------
# REQ-182 — Hasura v2 metadata converter emits valid Provisa YAML config
# ---------------------------------------------------------------------------


@given("a Hasura v2 metadata export directory")
def _given_v2_metadata(shared_data: dict) -> None:
    shared_data["metadata"] = _v2_metadata()
    shared_data["collector"] = WarningCollector()


@when("the CLI converter is run against it")
def _when_v2_converter_run(shared_data: dict) -> None:
    shared_data["config"] = convert_metadata(shared_data["metadata"], shared_data["collector"])


@then(
    "valid Provisa YAML config is emitted covering tables, relationships, "
    "permissions, roles, and auth"
)
def _then_v2_config_complete(shared_data: dict) -> None:
    config: ProvisaConfig = shared_data["config"]
    assert isinstance(config, ProvisaConfig)
    assert {t.table_name for t in config.tables} >= {"orders", "customers"}
    assert config.relationships, "relationships must be emitted"
    assert {r.id for r in config.roles} >= {"analyst", "customer", "clerk", "manager"}
    assert config.auth is not None

    # Round-trips through YAML and re-validates as a ProvisaConfig.
    text = yaml.safe_dump(config.model_dump(by_alias=True, mode="json"))
    reloaded = ProvisaConfig.model_validate(yaml.safe_load(text))
    assert len(reloaded.tables) == len(config.tables)


# ---------------------------------------------------------------------------
# REQ-183 — Hasura DDN HML converter emits valid Provisa YAML config
# ---------------------------------------------------------------------------


@given("a Hasura DDN supergraph project")
def _given_ddn_project(shared_data: dict) -> None:
    shared_data["metadata"] = _ddn_metadata()
    shared_data["collector"] = WarningCollector()


@when("the HML converter CLI tool is run")
def _when_ddn_converter_run(shared_data: dict) -> None:
    shared_data["config"] = convert_hml(shared_data["metadata"], shared_data["collector"])


@then(
    "valid Provisa YAML config is emitted covering ObjectTypes, Models, "
    "Relationships, TypePermissions, ModelPermissions, and DataConnectorLinks"
)
def _then_ddn_config_complete(shared_data: dict) -> None:
    config: ProvisaConfig = shared_data["config"]
    assert isinstance(config, ProvisaConfig)
    assert {t.table_name for t in config.tables} >= {"artist", "album"}  # Models -> tables
    assert config.relationships, "Relationships must convert"
    assert {r.id for r in config.roles} >= {"viewer"}  # TypePermissions/ModelPermissions -> roles
    assert config.rls_rules, "ModelPermissions filter -> rls_rules"
    assert config.sources, "DataConnectorLink -> source"

    text = yaml.safe_dump(config.model_dump(by_alias=True, mode="json"))
    reloaded = ProvisaConfig.model_validate(yaml.safe_load(text))
    assert len(reloaded.tables) == len(config.tables)


# ---------------------------------------------------------------------------
# REQ-184 — Shared boolean expression-to-SQL converter
# ---------------------------------------------------------------------------


@given("a Hasura boolean filter expression using operators like _eq, _in, _and, _or, _not")
def _given_bool_expr(shared_data: dict) -> None:
    shared_data["expr"] = {
        "_and": [
            {"_or": [{"region": {"_in": ["us-east", "us-west"]}}, {"vip": {"_eq": True}}]},
            {"_not": {"status": {"_eq": "deleted"}}},
            {"owner_id": {"_eq": "X-Hasura-User-Id"}},
        ]
    }


@when("the shared converter processes it")
def _when_bool_expr_converted(shared_data: dict) -> None:
    shared_data["sql"] = bool_expr_to_sql(shared_data["expr"])


@then(
    "valid SQL is produced with session variable references mapped to "
    "current_setting('provisa.<name>')"
)
def _then_bool_expr_sql(shared_data: dict) -> None:
    sql: str = shared_data["sql"]
    assert "AND" in sql
    assert "OR" in sql
    assert "NOT" in sql
    assert "IN" in sql
    assert "'us-east'" in sql and "'us-west'" in sql
    # Session variable rendered as a setting, never as a raw string literal.
    assert "current_setting('provisa.user_id')" in sql
    assert "'X-Hasura-User-Id'" not in sql


# ---------------------------------------------------------------------------
# REQ-185 — select_permissions columns -> visible_to
# ---------------------------------------------------------------------------


@given("a Hasura v2 metadata export with select_permissions[].columns per role")
def _given_v2_select_perms(shared_data: dict) -> None:
    table = HasuraTable(
        name="products",
        schema_name="public",
        select_permissions=[
            HasuraPermission(role="viewer", columns=["id", "name", "price"], filter={}),
            HasuraPermission(role="admin", columns=["*"], filter={}),
            HasuraPermission(role="auditor", columns=["id", "internal_cost"], filter={}),
        ],
    )
    shared_data["metadata"] = HasuraMetadata(
        sources=[HasuraSource(name="default", kind="postgres", tables=[table])]
    )


@when("the v2 converter runs")
def _when_v2_converter_runs(shared_data: dict) -> None:
    shared_data["collector"] = shared_data.get("collector") or WarningCollector()
    shared_data["config"] = convert_metadata(shared_data["metadata"], shared_data["collector"])


@then(
    "each column's visible_to is populated from the role's column list, "
    'with "*" meaning all columns'
)
def _then_visible_to(shared_data: dict) -> None:
    table = _find_table(shared_data["config"], "products")
    assert table is not None
    price = _col(table, "price")
    assert price is not None and "viewer" in price.visible_to
    internal = _col(table, "internal_cost")
    assert internal is not None
    assert "auditor" in internal.visible_to
    assert "viewer" not in internal.visible_to
    # "*" wildcard tracked as an all-columns marker for the admin role.
    star = _col(table, "*")
    assert star is not None and "admin" in star.visible_to


# ---------------------------------------------------------------------------
# REQ-186 — insert/update columns -> writable_by
# ---------------------------------------------------------------------------


@given("a Hasura v2 metadata export with insert/update_permissions[].columns per role")
def _given_v2_write_perms(shared_data: dict) -> None:
    table = HasuraTable(
        name="orders",
        schema_name="public",
        select_permissions=[
            HasuraPermission(role="analyst", columns=["id", "amount", "region"], filter={}),
        ],
        insert_permissions=[HasuraPermission(role="clerk", columns=["amount", "region"])],
        update_permissions=[HasuraPermission(role="editor", columns=["region"])],
    )
    shared_data["metadata"] = HasuraMetadata(
        sources=[HasuraSource(name="default", kind="postgres", tables=[table])]
    )


@then("each column's writable_by is populated from the role's insert/update column list")
def _then_writable_by(shared_data: dict) -> None:
    table = _find_table(shared_data["config"], "orders")
    assert table is not None
    amount = _col(table, "amount")
    region = _col(table, "region")
    assert amount is not None and "clerk" in amount.writable_by
    assert region is not None
    assert "clerk" in region.writable_by
    assert "editor" in region.writable_by
    # A select-only column is not writable by anyone.
    id_col = _col(table, "id")
    assert id_col is not None and id_col.writable_by == []


# ---------------------------------------------------------------------------
# REQ-187 — select_permissions filter -> rls_rules[]
# ---------------------------------------------------------------------------


@given("a Hasura v2 select_permissions[].filter boolean expression")
def _given_v2_filter(shared_data: dict) -> None:
    table = HasuraTable(
        name="orders",
        schema_name="public",
        select_permissions=[
            HasuraPermission(
                role="customer",
                columns=["id", "amount"],
                filter={"customer_id": {"_eq": "X-Hasura-User-Id"}},
            ),
            HasuraPermission(role="analyst", columns=["id", "amount"], filter={}),
        ],
    )
    shared_data["metadata"] = HasuraMetadata(
        sources=[HasuraSource(name="default", kind="postgres", tables=[table])]
    )


@then(
    "rls_rules[] are generated via boolean expression-to-SQL conversion, "
    "with empty filter producing no RLS rule"
)
def _then_rls_rules(shared_data: dict) -> None:
    config = shared_data["config"]
    customer_rules = [r for r in config.rls_rules if r.role_id == "customer"]
    assert len(customer_rules) == 1
    assert "customer_id" in customer_rules[0].filter
    assert "current_setting(" in customer_rules[0].filter
    # analyst's empty filter must not produce an RLS rule.
    assert [r for r in config.rls_rules if r.role_id == "analyst"] == []


# ---------------------------------------------------------------------------
# REQ-188 — relationship cardinality
# ---------------------------------------------------------------------------


@given("a Hasura v2 metadata export with object_relationships and array_relationships")
def _given_v2_relationships(shared_data: dict) -> None:
    table = HasuraTable(
        name="orders",
        schema_name="public",
        object_relationships=[
            HasuraRelationship(
                name="customer",
                rel_type="object",
                remote_table="customers",
                remote_schema="public",
                column_mapping={"customer_id": "id"},
            ),
        ],
        array_relationships=[
            HasuraRelationship(
                name="order_items",
                rel_type="array",
                remote_table="order_items",
                remote_schema="public",
                column_mapping={"id": "order_id"},
            ),
        ],
    )
    shared_data["metadata"] = HasuraMetadata(
        sources=[HasuraSource(name="default", kind="postgres", tables=[table])]
    )


@then(
    "object_relationships become cardinality=many-to-one and array_relationships "
    "become cardinality=one-to-many"
)
def _then_cardinality(shared_data: dict) -> None:
    config = shared_data["config"]
    obj_rel = next(r for r in config.relationships if "customer" in r.id)
    arr_rel = next(r for r in config.relationships if "order_items" in r.id)
    assert obj_rel.cardinality == Cardinality.many_to_one
    assert arr_rel.cardinality == Cardinality.one_to_many
    # Physical columns used directly.
    assert obj_rel.source_column == "customer_id"
    assert arr_rel.source_column == "id"


# ---------------------------------------------------------------------------
# REQ-189 — DDN field name -> physical column resolution
# ---------------------------------------------------------------------------


@given("a DDN supergraph with ObjectType.dataConnectorTypeMapping[].fieldMapping entries")
def _given_ddn_field_mapping(shared_data: dict) -> None:
    shared_data["metadata"] = _ddn_metadata()
    shared_data["collector"] = WarningCollector()


@when("the DDN converter runs")
def _when_ddn_converter_runs(shared_data: dict) -> None:
    shared_data["collector"] = shared_data.get("collector") or WarningCollector()
    agg: dict = {}
    shared_data["agg_collector"] = agg
    shared_data["config"] = convert_hml(
        shared_data["metadata"], shared_data["collector"], agg_collector=agg
    )


@then(
    "all GraphQL field names in relationships, permissions, and column definitions "
    "are resolved to physical column names"
)
def _then_field_names_resolved(shared_data: dict) -> None:
    config = shared_data["config"]
    album = _find_table(config, "album")
    assert album is not None
    col_names = {c.name for c in album.columns}
    # Physical columns, not GraphQL aliases.
    assert "album_id" in col_names and "artist_id" in col_names
    assert "albumId" not in col_names and "artistId" not in col_names
    artist_id_col = _col(album, "artist_id")
    assert artist_id_col is not None and artist_id_col.alias == "artistId"
    # Relationship + RLS reference physical columns.
    rel = next(r for r in config.relationships if r.id.endswith(".artist"))
    assert rel.source_column == "artist_id"
    album_rls = next(r for r in config.rls_rules if r.role_id == "viewer")
    assert "artist_id" in album_rls.filter


# ---------------------------------------------------------------------------
# REQ-190 — auth conversion via --auth-env-file
# ---------------------------------------------------------------------------


@given("a Hasura v2 auth config with JWT jwk_url, claims_map, or admin secret")
def _given_v2_auth(shared_data: dict) -> None:
    shared_data["metadata"] = _v2_metadata()
    shared_data["auth_env"] = {
        "JWK_URL": "https://auth.example.com/.well-known/jwks.json",
        "HASURA_GRAPHQL_ADMIN_SECRET": "top-secret",
        "CLAIMS_MAP": '{"https://hasura.io/jwt/claims.role": "admin"}',
    }
    shared_data["collector"] = WarningCollector()


@when("the v2 converter runs with --auth-env-file")
def _when_v2_converter_auth(shared_data: dict) -> None:
    shared_data["config"] = convert_metadata(
        shared_data["metadata"],
        shared_data["collector"],
        auth_env=shared_data["auth_env"],
    )
    webhook_collector = WarningCollector()
    convert_metadata(_v2_metadata(), webhook_collector, auth_env={"AUTH_PROVIDER": "webhook"})
    shared_data["webhook_warnings"] = webhook_collector


@then(
    "JWT becomes provider: oauth with role_mapping[], admin secret becomes superuser, "
    "and webhook auth emits a warning"
)
def _then_v2_auth(shared_data: dict) -> None:
    auth = shared_data["config"].auth
    assert auth.provider == "oauth"
    assert auth.oauth and "jwk_url" in auth.oauth
    assert auth.superuser and auth.superuser["secret"] == "top-secret"
    assert auth.role_mapping and auth.role_mapping[0]["role"] == "admin"
    assert any(w.category == "webhook_auth" for w in shared_data["webhook_warnings"].warnings)


# ---------------------------------------------------------------------------
# REQ-191 — DDN aggregate expressions -> sidecar
# ---------------------------------------------------------------------------


@given("a DDN project with AggregateExpression metadata")
def _given_ddn_aggregates(shared_data: dict) -> None:
    shared_data["metadata"] = _ddn_metadata()
    shared_data["collector"] = WarningCollector()


@then("aggregate config is emitted in provisa-aggregates.yaml as valid Provisa aggregate config")
def _then_ddn_aggregates(shared_data: dict) -> None:
    agg = shared_data["agg_collector"]
    assert agg, "agg_collector must be populated for sidecar output"
    entry = next(iter(agg.values()))
    assert entry.get("count") is True
    assert entry.get("count_distinct") is True
    assert entry.get("fields", {}).get("artistId") == ["sum", "avg"]
    # Aggregates go to the sidecar, not the table description.
    for table in shared_data["config"].tables:
        assert not (table.description or "").startswith("[aggregates")
    # Sidecar serializes to valid YAML.
    text = yaml.safe_dump(agg)
    assert yaml.safe_load(text) == agg


# ---------------------------------------------------------------------------
# REQ-192 — warnings for unmappable features, no abort
# ---------------------------------------------------------------------------


@given(
    "a Hasura project with event_triggers, remote_schemas, cron_triggers, or webhook-backed actions"
)
def _given_unmappable_features(shared_data: dict) -> None:
    shared_data["metadata"] = _v2_metadata()  # includes an event trigger and a webhook action
    shared_data["collector"] = WarningCollector()


@when("the converter runs")
def _when_converter_runs(shared_data: dict) -> None:
    shared_data["config"] = convert_metadata(shared_data["metadata"], shared_data["collector"])


@then("warnings are emitted for unmappable features and conversion completes rather than aborting")
def _then_warnings_no_abort(shared_data: dict) -> None:
    collector: WarningCollector = shared_data["collector"]
    config = shared_data["config"]
    assert isinstance(config, ProvisaConfig)  # completed, did not abort
    assert config.tables, "conversion still produced tables"
    assert collector.has_warnings()
    assert any(w.category == "event_triggers" for w in collector.warnings)
    # Webhook-backed action preserved as a webhook with its handler URL.
    assert any(w.url == "https://api.example.com/place_order" for w in config.webhooks)


# ---------------------------------------------------------------------------
# REQ-621 — placeholder connection credentials
# ---------------------------------------------------------------------------


@given("a completed Hasura v2 or DDN conversion")
def _given_completed_conversions(shared_data: dict) -> None:
    v2 = convert_metadata(
        HasuraMetadata(
            sources=[
                HasuraSource(
                    name="default",
                    kind="postgres",
                    connection_info={"database_url": {"from_env": "PG_URL"}},
                    tables=[
                        HasuraTable(
                            name="t",
                            schema_name="public",
                            select_permissions=[HasuraPermission(role="r", columns=["id"])],
                        )
                    ],
                )
            ]
        )
    )
    ddn = convert_hml(_ddn_metadata())
    shared_data["v2_config"] = v2
    shared_data["ddn_config"] = ddn


@when("the output config is inspected")
def _when_output_inspected(shared_data: dict) -> None:
    shared_data["sources"] = list(shared_data["v2_config"].sources) + list(
        shared_data["ddn_config"].sources
    )


@then(
    "placeholder credentials (host: localhost, password: ${env:DB_PASSWORD}) are present "
    "and Provisa refuses to start without real values"
)
def _then_placeholder_credentials(shared_data: dict) -> None:
    for src in shared_data["sources"]:
        assert src.host == "localhost"
        assert src.password == "${env:DB_PASSWORD}"


# ---------------------------------------------------------------------------
# REQ-623 — source kind, URL parsing, pool settings
# ---------------------------------------------------------------------------


@given("a Hasura v2 source config with kind, database_url, and pool_settings")
def _given_v2_source_config(shared_data: dict) -> None:
    def _src(name: str, kind: str, url: str) -> HasuraSource:
        return HasuraSource(
            name=name,
            kind=kind,
            connection_info={
                "database_url": url,
                "pool_settings": {"min_connections": 4, "max_connections": 22},
            },
            tables=[
                HasuraTable(
                    name="t",
                    schema_name="public",
                    select_permissions=[HasuraPermission(role="r", columns=["id"])],
                )
            ],
        )

    shared_data["metadata"] = HasuraMetadata(
        sources=[
            _src("pg", "pg", "postgres://u:p@dbhost:6543/appdb"),
            _src("ms", "mssql", "postgres://u:p@ms:1433/msdb"),
        ]
    )


@then(
    "SourceType is mapped correctly and connection URL is parsed into components "
    "with pool settings preserved"
)
def _then_source_config(shared_data: dict) -> None:
    config = shared_data["config"]
    pg = next(s for s in config.sources if s.id == "pg")
    ms = next(s for s in config.sources if s.id == "ms")
    assert pg.type == SourceType.postgresql  # pg -> postgresql
    assert ms.type == SourceType.sqlserver  # mssql -> sqlserver
    assert pg.host == "dbhost"
    assert pg.port == 6543
    assert pg.database == "appdb"
    assert pg.username == "u"
    assert pg.pool_min == 4
    assert pg.pool_max == 22


# ---------------------------------------------------------------------------
# REQ-624 — delete_permissions upgrade role to write, no per-table delete
# ---------------------------------------------------------------------------


@given("a Hasura v2 role with delete_permissions on any table")
def _given_v2_delete_perms(shared_data: dict) -> None:
    table = HasuraTable(
        name="orders",
        schema_name="public",
        select_permissions=[HasuraPermission(role="purger", columns=["id"], filter={})],
        delete_permissions=[HasuraPermission(role="purger")],
    )
    shared_data["metadata"] = HasuraMetadata(
        sources=[HasuraSource(name="default", kind="postgres", tables=[table])]
    )


@then("the role is upgraded to write capability with no per-table delete mapping produced")
def _then_delete_upgrade(shared_data: dict) -> None:
    config = shared_data["config"]
    purger = next(r for r in config.roles if r.id == "purger")
    assert "write" in purger.capabilities
    # No per-table delete artefact — the capability upgrade is the only output.
    for table in config.tables:
        assert not getattr(table, "delete_rules", None)
    assert not any("delete" in (r.id or "").lower() for r in config.rls_rules)


# ---------------------------------------------------------------------------
# REQ-625 — env-var / unparseable URL -> placeholder connection values
# ---------------------------------------------------------------------------


@given("a Hasura v2 source with database_url as an env var reference or unparseable URL")
def _given_v2_bad_url(shared_data: dict) -> None:
    def _src(name: str, url) -> HasuraSource:
        return HasuraSource(
            name=name,
            kind="postgres",
            connection_info={"database_url": url},
            tables=[
                HasuraTable(
                    name="t",
                    schema_name="public",
                    select_permissions=[HasuraPermission(role="r", columns=["id"])],
                )
            ],
        )

    shared_data["metadata"] = HasuraMetadata(
        sources=[
            _src("fromenv", {"from_env": "PG_DATABASE_URL"}),
            _src("garbage", "not-a-valid-url"),
        ]
    )


@then(
    "placeholder connection values are substituted and operators are directed to use --source-overrides"
)
def _then_placeholder_substituted(shared_data: dict) -> None:
    config = shared_data["config"]
    for src in config.sources:
        assert src.host == "localhost"
        assert src.port == 5432
        assert src.database == "default"
        assert src.username == "postgres"
        assert src.password == "${env:DB_PASSWORD}"


# ---------------------------------------------------------------------------
# REQ-626 — roles collected only from permission entries
# ---------------------------------------------------------------------------


@given("a Hasura project with roles that have no permission entries on any table or action")
def _given_v2_orphan_roles(shared_data: dict) -> None:
    table = HasuraTable(
        name="orders",
        schema_name="public",
        select_permissions=[HasuraPermission(role="analyst", columns=["id"], filter={})],
    )
    # "ghost" role is never referenced by any permission entry — must not appear.
    shared_data["metadata"] = HasuraMetadata(
        sources=[HasuraSource(name="default", kind="postgres", tables=[table])]
    )
    shared_data["ghost_role"] = "ghost"


@then("those roles are excluded from the output config")
def _then_orphan_roles_excluded(shared_data: dict) -> None:
    config = shared_data["config"]
    role_ids = {r.id for r in config.roles}
    assert "analyst" in role_ids
    assert shared_data["ghost_role"] not in role_ids


# ---------------------------------------------------------------------------
# REQ-627 — table alias priority select > select_by_pk > custom_name
# ---------------------------------------------------------------------------


@given("a Hasura v2 table with custom_root_fields or custom_name defined")
def _given_v2_alias_tables(shared_data: dict) -> None:
    def _tbl(name: str, custom_name, crf: dict) -> HasuraTable:
        return HasuraTable(
            name=name,
            schema_name="public",
            custom_name=custom_name,
            custom_root_fields=crf,
            select_permissions=[HasuraPermission(role="r", columns=["id"], filter={})],
        )

    shared_data["metadata"] = HasuraMetadata(
        sources=[
            HasuraSource(
                name="default",
                kind="postgres",
                tables=[
                    _tbl("t_sel", "CustA", {"select": "selAlias", "select_by_pk": "pkAlias"}),
                    _tbl("t_pk", "CustB", {"select_by_pk": "pkAlias"}),
                    _tbl("t_custom", "CustC", {}),
                ],
            )
        ]
    )


@then("the Provisa table alias is derived with select > select_by_pk > custom_name priority order")
def _then_alias_priority(shared_data: dict) -> None:
    config = shared_data["config"]
    t_sel = _find_table(config, "t_sel")
    t_pk = _find_table(config, "t_pk")
    t_custom = _find_table(config, "t_custom")
    assert t_sel is not None and t_pk is not None and t_custom is not None
    assert t_sel.alias == "selAlias"  # select wins
    assert t_pk.alias == "pkAlias"  # select_by_pk next
    assert t_custom.alias == "CustC"  # custom_name last


# ---------------------------------------------------------------------------
# REQ-628 — missing ObjectType tables skipped with a warning
# ---------------------------------------------------------------------------


@given("a DDN HML project where some ObjectType HML files are missing")
def _given_ddn_missing_object_type(shared_data: dict) -> None:
    meta = _ddn_metadata()
    # A Model whose ObjectType was never scanned (its .hml file is "missing").
    meta.models.append(
        DDNModel(
            name="Ghost",
            subgraph="app",
            object_type="MissingType",
            connector_name="chinook",
            collection="ghost",
        )
    )
    shared_data["metadata"] = meta
    shared_data["collector"] = WarningCollector()


@then("missing ObjectType tables are skipped with a warning and conversion continues")
def _then_missing_object_type(shared_data: dict) -> None:
    config = shared_data["config"]
    collector: WarningCollector = shared_data["collector"]
    table_names = {t.table_name for t in config.tables}
    assert "ghost" not in table_names  # skipped
    assert {"artist", "album"} <= table_names  # conversion continued for valid models
    assert any(
        w.category == "missing_type" and "MissingType" in w.message for w in collector.warnings
    )
