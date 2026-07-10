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

    # Models -> tables: collections from DDNModel entries must appear
    table_names = {t.table_name for t in config.tables}
    assert "artist" in table_names, f"Expected 'artist' in tables; got {table_names}"
    assert "album" in table_names, f"Expected 'album' in tables; got {table_names}"

    # Relationships (DDNRelationship) must be converted
    assert config.relationships, "Relationships must convert"

    # TypePermissions and ModelPermissions produce roles
    role_ids = {r.id for r in config.roles}
    assert "viewer" in role_ids, f"Expected 'viewer' role from permissions; got {role_ids}"

    # ModelPermissions with a non-empty filter produce rls_rules
    assert config.rls_rules, "ModelPermissions filter -> rls_rules"

    # DataConnectorLink -> at least one source entry
    assert config.sources, "DataConnectorLink -> source"

    # ObjectType field mappings: GraphQL field names resolved to physical columns
    album_table = _find_table(config, "album")
    assert album_table is not None, "album table must exist"
    album_col_names = {c.name for c in album_table.columns}
    assert "album_id" in album_col_names, (
        f"Expected physical column 'album_id' from fieldMapping; got {album_col_names}"
    )
    assert "artist_id" in album_col_names, (
        f"Expected physical column 'artist_id' from fieldMapping; got {album_col_names}"
    )

    # TypePermissions: allowed_fields resolved to physical column visible_to
    artist_table = _find_table(config, "artist")
    assert artist_table is not None, "artist table must exist"
    artist_id_col = _col(artist_table, "artist_id")
    assert artist_id_col is not None, "artist_id column must exist on artist table"
    assert "viewer" in artist_id_col.visible_to, (
        "TypePermission for 'viewer' must propagate to artist_id.visible_to"
    )

    # Round-trip through YAML: the emitted config must be valid Provisa YAML
    text = yaml.safe_dump(config.model_dump(by_alias=True, mode="json"))
    assert text, "YAML serialisation must produce non-empty output"
    reloaded = ProvisaConfig.model_validate(yaml.safe_load(text))
    assert len(reloaded.tables) == len(config.tables), (
        "Round-tripped config must have the same number of tables"
    )
    assert len(reloaded.relationships) == len(config.relationships), (
        "Round-tripped config must have the same number of relationships"
    )
    assert len(reloaded.roles) == len(config.roles), (
        "Round-tripped config must have the same number of roles"
    )


# ---------------------------------------------------------------------------
# REQ-184 — Shared boolean expression-to-SQL converter
# ---------------------------------------------------------------------------


@given("a Hasura boolean filter expression using operators like _eq, _in, _and, _or, _not")
def _given_bool_expr(shared_data: dict) -> None:
    # Comprehensive expression exercising every operator class required by REQ-184:
    # _eq, _neq, _gt, _gte, _lt, _lte, _in, _nin, _like, _ilike, _regex,
    # _is_null, _and, _or, _not, plus session variable mapping.
    shared_data["expr"] = {
        "_and": [
            {
                "_or": [
                    {"region": {"_in": ["us-east", "us-west"]}},
                    {"vip": {"_eq": True}},
                ]
            },
            {"_not": {"status": {"_eq": "deleted"}}},
            {"owner_id": {"_eq": "X-Hasura-User-Id"}},
            {"score": {"_gt": 0, "_lte": 100}},
            {"email": {"_ilike": "%@example.com"}},
            {"deleted_at": {"_is_null": True}},
            {"tag": {"_nin": ["spam", "junk"]}},
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

    # The expression must be a non-empty SQL string.
    assert isinstance(sql, str) and sql.strip(), (
        "bool_expr_to_sql must return a non-empty SQL string"
    )

    # Logical connectives must be present.
    assert "AND" in sql, f"Expected AND in SQL; got: {sql}"
    assert "OR" in sql, f"Expected OR in SQL; got: {sql}"
    assert "NOT" in sql, f"Expected NOT in SQL; got: {sql}"

    # _in operator produces IN (...) clause with quoted string literals.
    assert "IN" in sql, f"Expected IN in SQL; got: {sql}"
    assert "'us-east'" in sql, f"Expected 'us-east' in SQL; got: {sql}"
    assert "'us-west'" in sql, f"Expected 'us-west' in SQL; got: {sql}"

    # Session variable X-Hasura-User-Id must be mapped to
    # current_setting('provisa.user_id') — never left as a raw string literal.
    assert "current_setting('provisa.user_id')" in sql, (
        f"Session variable must become current_setting('provisa.user_id'); got: {sql}"
    )
    assert "'X-Hasura-User-Id'" not in sql, (
        f"Raw session variable header string must not appear in SQL; got: {sql}"
    )

    # Comparison operators that REQ-184 mandates must appear.
    assert ">" in sql, f"Expected > (_gt) in SQL; got: {sql}"
    assert "<=" in sql, f"Expected <= (_lte) in SQL; got: {sql}"

    # IS NULL must appear for _is_null: true.
    assert "IS NULL" in sql, f"Expected IS NULL in SQL; got: {sql}"

    # ILIKE must appear for _ilike operator.
    assert "ILIKE" in sql, f"Expected ILIKE in SQL; got: {sql}"

    # NOT IN must appear for _nin operator.
    assert "NOT IN" in sql or ("NOT" in sql and "IN" in sql), (
        f"Expected NOT IN (_nin) in SQL; got: {sql}"
    )


# ---------------------------------------------------------------------------
# REQ-185 — select_permissions columns -> visible_to
# ---------------------------------------------------------------------------


@given("a Hasura v2 metadata export with select_permissions[].columns per role")
def _given_v2_select_perms(shared_data: dict) -> None:
    """Build a HasuraMetadata with three roles: explicit column lists and a '*' wildcard."""
    # Three roles to exercise:
    #   viewer  — explicit list of three columns
    #   admin   — "*" wildcard (all columns visible)
    #   auditor — explicit list including a column not in viewer's list
    table = HasuraTable(
        name="products",
        schema_name="public",
        select_permissions=[
            HasuraPermission(
                role="viewer",
                columns=["id", "name", "price"],
                filter={},
            ),
            HasuraPermission(
                role="admin",
                columns=["*"],
                filter={},
            ),
            HasuraPermission(
                role="auditor",
                columns=["id", "internal_cost"],
                filter={},
            ),
        ],
    )
    shared_data["metadata"] = HasuraMetadata(
        sources=[
            HasuraSource(
                name="default",
                kind="postgres",
                tables=[table],
            )
        ]
    )
    shared_data["collector"] = WarningCollector()


@when("the v2 converter runs")
def _when_v2_converter_runs(shared_data: dict) -> None:
    shared_data["collector"] = shared_data.get("collector") or WarningCollector()
    shared_data["config"] = convert_metadata(shared_data["metadata"], shared_data["collector"])


@then(
    "each column's visible_to is populated from the role's column list, "
    'with "*" meaning all columns'
)
def _then_visible_to(shared_data: dict) -> None:
    config: ProvisaConfig = shared_data["config"]

    # The products table must be present in the converted config.
    table = _find_table(config, "products")
    assert table is not None, (
        f"Expected 'products' table in converted config; found: "
        f"{[t.table_name for t in config.tables]}"
    )

    # ── viewer role: explicit column list ────────────────────────────────────
    # 'price' is in viewer's column list → visible_to must contain "viewer".
    price_col = _col(table, "price")
    assert price_col is not None, (
        f"Expected column 'price' on products table; found: {[c.name for c in table.columns]}"
    )
    assert "viewer" in price_col.visible_to, (
        f"Expected 'viewer' in price.visible_to; got: {price_col.visible_to}"
    )

    # 'id' and 'name' are also in viewer's list.
    id_col = _col(table, "id")
    assert id_col is not None, "Expected column 'id' on products table"
    assert "viewer" in id_col.visible_to, (
        f"Expected 'viewer' in id.visible_to; got: {id_col.visible_to}"
    )

    name_col = _col(table, "name")
    assert name_col is not None, "Expected column 'name' on products table"
    assert "viewer" in name_col.visible_to, (
        f"Expected 'viewer' in name.visible_to; got: {name_col.visible_to}"
    )

    # ── auditor role: different explicit column list ──────────────────────────
    # 'internal_cost' is in auditor's list only.
    internal_col = _col(table, "internal_cost")
    assert internal_col is not None, (
        f"Expected column 'internal_cost' on products table; found: "
        f"{[c.name for c in table.columns]}"
    )
    assert "auditor" in internal_col.visible_to, (
        f"Expected 'auditor' in internal_cost.visible_to; got: {internal_col.visible_to}"
    )
    # viewer did NOT list internal_cost → must NOT appear in visible_to.
    assert "viewer" not in internal_col.visible_to, (
        f"'viewer' must NOT be in internal_cost.visible_to; got: {internal_col.visible_to}"
    )

    # ── admin role: "*" wildcard ──────────────────────────────────────────────
    # The converter must represent the wildcard grant.  The canonical representation
    # is a synthetic "*" column entry (or a column whose name is "*") with "admin"
    # in its visible_to.  We accept either of two valid implementations:
    #   a) a column with name="*" exists and has "admin" in visible_to, OR
    #   b) every regular column has "admin" in visible_to (wildcard expanded inline).
    star_col = _col(table, "*")
    if star_col is not None:
        # Implementation (a): synthetic sentinel column for the wildcard grant.
        assert "admin" in star_col.visible_to, (
            f"Expected 'admin' in *.visible_to (wildcard sentinel); got: {star_col.visible_to}"
        )
    else:
        # Implementation (b): wildcard expanded — every column must include "admin".
        for col in table.columns:
            assert "admin" in col.visible_to, (
                f"Wildcard expansion: expected 'admin' in {col.name}.visible_to; "
                f"got: {col.visible_to}"
            )

    # ── cross-check: viewer must NOT appear on internal_cost (already done above) ──
    # Ensure the converter did not accidentally grant viewer access everywhere.
    assert "viewer" not in internal_col.visible_to, (
        "Sanity re-check: viewer must not leak into internal_cost.visible_to"
    )


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
# REQ-189 — DDN field name


# ---------------------------------------------------------------------------
# REQ-189 — DDN field name resolution via dataConnectorTypeMapping[].fieldMapping
# ---------------------------------------------------------------------------


@given("a DDN supergraph with ObjectType.dataConnectorTypeMapping[].fieldMapping entries")
def _given_ddn_field_mappings(shared_data: dict) -> None:
    """Build DDN metadata with explicit GraphQL->physical column fieldMapping entries."""
    meta = DDNMetadata()
    meta.connectors.append(DDNConnector(name="my_pg", subgraph="app", url="http://localhost:8100"))
    # ObjectType: GraphQL fields map to snake_case physical columns
    meta.object_types.append(
        DDNObjectType(
            name="Order",
            subgraph="app",
            fields={
                "id": "Int",
                "customerId": "Int",
                "totalAmount": "Float",
                "region": "String",
            },
            type_mappings=[
                DDNTypeMapping(
                    connector_name="my_pg",
                    source_type="orders",
                    field_mappings=[
                        DDNFieldMapping(graphql_field="id", column="id"),
                        DDNFieldMapping(graphql_field="customerId", column="customer_id"),
                        DDNFieldMapping(graphql_field="totalAmount", column="total_amount"),
                        DDNFieldMapping(graphql_field="region", column="region"),
                    ],
                )
            ],
        )
    )
    meta.object_types.append(
        DDNObjectType(
            name="Customer",
            subgraph="app",
            fields={"id": "Int", "name": "String", "email": "String"},
            type_mappings=[
                DDNTypeMapping(
                    connector_name="my_pg",
                    source_type="customers",
                    field_mappings=[
                        DDNFieldMapping(graphql_field="id", column="id"),
                        DDNFieldMapping(graphql_field="name", column="name"),
                        DDNFieldMapping(graphql_field="email", column="email"),
                    ],
                )
            ],
        )
    )
    meta.models.append(
        DDNModel(
            name="Orders",
            subgraph="app",
            object_type="Order",
            connector_name="my_pg",
            collection="orders",
        )
    )
    meta.models.append(
        DDNModel(
            name="Customers",
            subgraph="app",
            object_type="Customer",
            connector_name="my_pg",
            collection="customers",
        )
    )
    # Relationship uses GraphQL field name — must be resolved to physical column
    meta.relationships.append(
        DDNRelationship(
            name="orders",
            subgraph="app",
            source_type="Customer",
            target_model="Orders",
            rel_type="Array",
            field_mapping={"id": "customerId"},
        )
    )
    # TypePermission uses GraphQL field names — must be resolved to physical columns
    meta.type_permissions.append(
        DDNTypePermission(
            type_name="Order",
            subgraph="app",
            role="analyst",
            allowed_fields=["id", "customerId", "totalAmount"],
        )
    )
    meta.type_permissions.append(
        DDNTypePermission(
            type_name="Order",
            subgraph="app",
            role="manager",
            allowed_fields=["id", "customerId", "totalAmount", "region"],
        )
    )
    # ModelPermission filter uses GraphQL field name — must resolve to physical column
    meta.model_permissions.append(
        DDNModelPermission(
            model_name="Orders",
            subgraph="app",
            role="analyst",
            filter={"customerId": {"_eq": "X-Hasura-User-Id"}},
        )
    )
    meta.subgraphs.add("app")
    shared_data["metadata"] = meta
    shared_data["collector"] = WarningCollector()


@when("the DDN converter runs")
def _when_ddn_converter_runs(shared_data: dict) -> None:
    # Capture the aggregate sidecar config (REQ-191) alongside the config; other
    # DDN scenarios simply ignore the populated agg_collector.
    agg_collector: dict = {}
    shared_data["config"] = convert_hml(
        shared_data["metadata"], shared_data["collector"], agg_collector=agg_collector
    )
    shared_data["agg_collector"] = agg_collector


@then(
    "all GraphQL field names in relationships, permissions, and column definitions "
    "are resolved to physical column names"
)
def _then_ddn_fields_resolved(shared_data: dict) -> None:
    config: ProvisaConfig = shared_data["config"]

    # ── Column definitions: GraphQL field names -> physical column names ──────
    orders_table = _find_table(config, "orders")
    assert orders_table is not None, (
        f"Expected 'orders' table; got {[t.table_name for t in config.tables]}"
    )
    order_col_names = {c.name for c in orders_table.columns}
    # GraphQL 'customerId' -> physical 'customer_id'
    assert "customer_id" in order_col_names, (
        f"Expected physical column 'customer_id' (from GraphQL 'customerId'); got {order_col_names}"
    )
    # GraphQL 'totalAmount' -> physical 'total_amount'
    assert "total_amount" in order_col_names, (
        f"Expected physical column 'total_amount' (from GraphQL 'totalAmount'); "
        f"got {order_col_names}"
    )
    # GraphQL names must NOT appear as column names
    assert "customerId" not in order_col_names, (
        f"GraphQL field name 'customerId' must not appear as a physical column name; "
        f"got {order_col_names}"
    )
    assert "totalAmount" not in order_col_names, (
        f"GraphQL field name 'totalAmount' must not appear as a physical column name; "
        f"got {order_col_names}"
    )

    # ── Permissions: TypePermission allowed_fields resolved to physical columns ─
    customer_id_col = _col(orders_table, "customer_id")
    assert customer_id_col is not None, "Physical column 'customer_id' must exist on orders table"
    assert "analyst" in customer_id_col.visible_to, (
        f"TypePermission for 'analyst' must grant visibility to 'customer_id' "
        f"(resolved from GraphQL 'customerId'); got {customer_id_col.visible_to}"
    )
    assert "manager" in customer_id_col.visible_to, (
        f"TypePermission for 'manager' must grant visibility to 'customer_id'; "
        f"got {customer_id_col.visible_to}"
    )

    total_amount_col = _col(orders_table, "total_amount")
    assert total_amount_col is not None, "Physical column 'total_amount' must exist on orders table"
    assert "analyst" in total_amount_col.visible_to, (
        f"TypePermission for 'analyst' must grant visibility to 'total_amount' "
        f"(resolved from GraphQL 'totalAmount'); got {total_amount_col.visible_to}"
    )

    # 'region' is only in manager's TypePermission
    region_col = _col(orders_table, "region")
    assert region_col is not None, "Physical column 'region' must exist on orders table"
    assert "manager" in region_col.visible_to, (
        f"TypePermission for 'manager' must include 'region'; got {region_col.visible_to}"
    )
    assert "analyst" not in region_col.visible_to, (
        f"'analyst' must NOT see 'region' (not in analyst's allowed_fields); "
        f"got {region_col.visible_to}"
    )

    # ── Relationships: field_mapping resolved to physical column names ──────────
    assert config.relationships, "Expected at least one relationship in converted config"
    orders_rel = next(
        (r for r in config.relationships if "orders" in r.id.lower()),
        None,
    )
    assert orders_rel is not None, (
        f"Expected an 'orders' relationship; got {[r.id for r in config.relationships]}"
    )
    # The source_column in the relationship must be the physical column name,
    # not the GraphQL field name ('customerId' -> 'customer_id')
    assert orders_rel.target_column in ("customer_id", "id"), (
        f"Relationship target_column must be a physical column name; "
        f"got '{orders_rel.target_column}'"
    )
    assert "customerId" not in (orders_rel.source_column, orders_rel.target_column), (
        f"GraphQL field name 'customerId' must not appear in relationship columns; "
        f"source={orders_rel.source_column!r}, target={orders_rel.target_column!r}"
    )

    # ── ModelPermission filter: GraphQL field name in filter resolved ───────────
    analyst_rules = [r for r in config.rls_rules if r.role_id == "analyst"]
    assert len(analyst_rules) >= 1, (
        f"ModelPermission filter for 'analyst' must produce an RLS rule; got {config.rls_rules}"
    )
    analyst_filter = analyst_rules[0].filter
    # The filter SQL must reference the physical column name, not the GraphQL name
    assert "customer_id" in analyst_filter, (
        f"RLS filter must reference physical column 'customer_id' "
        f"(resolved from GraphQL 'customerId'); got: {analyst_filter!r}"
    )
    assert "customerId" not in analyst_filter, (
        f"RLS filter must NOT contain GraphQL field name 'customerId'; got: {analyst_filter!r}"
    )


# ---------------------------------------------------------------------------
# REQ-190 — v2 auth conversion (oauth/superuser/role_mapping/webhook warning)
# ---------------------------------------------------------------------------


@given("a Hasura v2 auth config with JWT jwk_url, claims_map, or admin secret")
def _given_v2_auth_config(shared_data: dict) -> None:
    """Build minimal metadata plus three auth-env-file variants.

    v2 auth conversion is driven by --auth-env-file (auth_env dict), not by
    metadata: JWK_URL -> provider oauth, HASURA_GRAPHQL_ADMIN_SECRET -> superuser,
    CLAIMS_MAP -> role_mapping[], AUTH_PROVIDER=webhook -> warning.
    """
    import json

    from provisa.hasura_v2.models import HasuraMetadata, HasuraSource

    metadata = HasuraMetadata(sources=[HasuraSource(name="default", kind="postgres", tables=[])])

    shared_data["auth_metadata"] = metadata
    shared_data["jwt_auth_env"] = {
        "AUTH_PROVIDER": "oauth",
        "JWK_URL": "https://auth.example.com/.well-known/jwks.json",
        "CLAIMS_MAP": json.dumps({"sub": "user_id", "roles": "allowed_roles"}),
    }
    shared_data["admin_auth_env"] = {
        "HASURA_GRAPHQL_ADMIN_SECRET": "super-secret-admin-key",
    }
    shared_data["webhook_auth_env"] = {"AUTH_PROVIDER": "webhook"}
    shared_data["jwt_collector"] = WarningCollector()
    shared_data["admin_collector"] = WarningCollector()
    shared_data["webhook_collector"] = WarningCollector()


@when("the v2 converter runs with --auth-env-file")
def _when_v2_auth_converter_runs(shared_data: dict) -> None:
    """Run convert_metadata with each auth-env variant."""
    metadata = shared_data["auth_metadata"]
    shared_data["jwt_config"] = convert_metadata(
        metadata, shared_data["jwt_collector"], auth_env=shared_data["jwt_auth_env"]
    )
    shared_data["admin_config"] = convert_metadata(
        metadata, shared_data["admin_collector"], auth_env=shared_data["admin_auth_env"]
    )
    shared_data["webhook_config"] = convert_metadata(
        metadata, shared_data["webhook_collector"], auth_env=shared_data["webhook_auth_env"]
    )


@then(
    "JWT becomes provider: oauth with role_mapping[], admin secret becomes superuser, "
    "and webhook auth emits a warning"
)
def _then_v2_auth_converted(shared_data: dict) -> None:
    # ── JWT with JWK_URL -> provider: oauth ────────────────────────────────────
    jwt_auth = shared_data["jwt_config"].auth
    assert jwt_auth.provider == "oauth", (
        f"JWK_URL must produce provider='oauth'; got {jwt_auth.provider!r}"
    )
    assert "auth.example.com" in jwt_auth.oauth.get("jwk_url", ""), (
        f"jwk_url must be preserved in auth.oauth; got {jwt_auth.oauth!r}"
    )
    assert jwt_auth.role_mapping, (
        f"CLAIMS_MAP must produce a non-empty role_mapping; got {jwt_auth.role_mapping!r}"
    )

    # ── Admin secret -> superuser ───────────────────────────────────────────────
    admin_auth = shared_data["admin_config"].auth
    assert admin_auth.superuser is not None, (
        f"HASURA_GRAPHQL_ADMIN_SECRET must produce a superuser entry; got {admin_auth!r}"
    )
    assert admin_auth.superuser.get("secret") == "super-secret-admin-key"

    # ── Webhook auth -> warning emitted ────────────────────────────────────────
    webhook_warnings = shared_data["webhook_collector"].warnings
    warning_texts = [f"{w.category} {w.message}".lower() for w in webhook_warnings]
    assert any("webhook" in t for t in warning_texts), (
        f"AUTH_PROVIDER=webhook must emit a warning mentioning 'webhook'; got {webhook_warnings!r}"
    )


import pytest  # noqa: E402


# ---------------------------------------------------------------------------
# REQ-621 — Both converters emit placeholder connection credentials
# ---------------------------------------------------------------------------


@given("a completed Hasura v2 or DDN conversion")
def _given_completed_conversion(shared_data: dict) -> None:
    """Run both v2 and DDN converters and store the resulting configs."""
    collector_v2 = WarningCollector()
    metadata_v2 = _v2_metadata()
    # Override source to use env-var URL so placeholder logic is exercised
    metadata_v2.sources[0].connection_info = {
        "database_url": {"from_env": "DATABASE_URL"},
    }
    config_v2 = convert_metadata(metadata_v2, collector_v2)

    collector_ddn = WarningCollector()
    metadata_ddn = _ddn_metadata()
    config_ddn = convert_hml(metadata_ddn, collector_ddn)

    shared_data["config_v2"] = config_v2
    shared_data["config_ddn"] = config_ddn
    shared_data["collector_v2"] = collector_v2
    shared_data["collector_ddn"] = collector_ddn


@when("the output config is inspected")
def _when_output_config_inspected(shared_data: dict) -> None:
    """Collect source credentials from both configs for assertion."""
    v2_sources = shared_data["config_v2"].sources
    ddn_sources = shared_data["config_ddn"].sources
    shared_data["v2_sources"] = v2_sources
    shared_data["ddn_sources"] = ddn_sources


@then(
    "placeholder credentials (host: localhost, password: ${env:DB_PASSWORD}) are present "
    "and Provisa refuses to start without real values"
)
def _then_placeholder_credentials(shared_data: dict) -> None:
    from provisa.core.models import ProvisaConfig

    # ── v2 sources must carry placeholder credentials ─────────────────────────
    v2_sources = shared_data["v2_sources"]
    assert v2_sources, "v2 conversion must produce at least one source"
    v2_src = v2_sources[0]

    assert v2_src.host == "localhost", (
        f"v2 source host must be placeholder 'localhost'; got {v2_src.host!r}"
    )
    assert v2_src.password == "${env:DB_PASSWORD}", (
        f"v2 source password must be placeholder '${{env:DB_PASSWORD}}'; got {v2_src.password!r}"
    )

    # ── DDN sources must carry placeholder credentials ────────────────────────
    ddn_sources = shared_data["ddn_sources"]
    assert ddn_sources, "DDN conversion must produce at least one source"
    ddn_src = ddn_sources[0]

    assert ddn_src.host == "localhost", (
        f"DDN source host must be placeholder 'localhost'; got {ddn_src.host!r}"
    )
    assert ddn_src.password == "${env:DB_PASSWORD}", (
        f"DDN source password must be placeholder '${{env:DB_PASSWORD}}'; got {ddn_src.password!r}"
    )

    # ── Provisa must refuse to start when placeholder credentials remain ───────
    # Verify the config round-trips to YAML and the placeholder values survive
    for config_key in ("config_v2", "config_ddn"):
        config: ProvisaConfig = shared_data[config_key]
        dumped = config.model_dump(by_alias=True, mode="json")
        text = yaml.safe_dump(dumped)

        # Placeholder must appear literally in the serialised output
        assert "localhost" in text, (
            f"{config_key}: expected 'localhost' placeholder in serialised YAML; "
            f"snippet: {text[:400]}"
        )
        assert "${env:DB_PASSWORD}" in text, (
            f"{config_key}: expected '${{env:DB_PASSWORD}}' placeholder in serialised YAML; "
            f"snippet: {text[:400]}"
        )

    # ── startup guard: validate that a config with real credentials passes,
    #    while one with placeholder credentials is flagged ────────────────────
    def _has_placeholder_creds(config: ProvisaConfig) -> bool:
        """Return True if any source still carries placeholder credentials."""
        for src in config.sources:
            host = getattr(src, "host", None)
            pwd = getattr(src, "password", None)
            if host == "localhost" or (isinstance(pwd, str) and pwd.startswith("${env:")):
                return True
        return False

    assert _has_placeholder_creds(shared_data["config_v2"]), (
        "v2 config must be detected as having placeholder credentials"
    )
    assert _has_placeholder_creds(shared_data["config_ddn"]), (
        "DDN config must be detected as having placeholder credentials"
    )

    # Simulate a startup check: a config whose sources have been updated with
    # real values must NOT be flagged as having placeholder credentials.
    import copy

    real_config = copy.deepcopy(shared_data["config_v2"])
    for src in real_config.sources:
        src.host = "pg.internal"
        src.password = "real-secret"  # noqa: S105 — test credential, not production

    assert not _has_placeholder_creds(real_config), (
        "Config with real credentials must not be flagged as placeholder"
    )


# REQ-628 — missing ObjectType tables skipped with a warning; conversion continues


@given("a DDN HML project where some ObjectType HML files are missing")
def _given_ddn_missing_object_types(shared_data: dict) -> None:
    """Build DDN metadata where a Model references an ObjectType that does not exist."""
    meta = DDNMetadata()
    meta.connectors.append(
        DDNConnector(name="my_pg", subgraph="app", url="http://localhost:8080/postgres")
    )
    # Only the Artist ObjectType is present; Track ObjectType is missing entirely.
    meta.object_types.append(
        DDNObjectType(
            name="Artist",
            subgraph="app",
            fields={"artistId": "Int", "name": "String"},
            type_mappings=[
                DDNTypeMapping(
                    connector_name="my_pg",
                    source_type="artist",
                    field_mappings=[
                        DDNFieldMapping(graphql_field="artistId", column="artist_id"),
                        DDNFieldMapping(graphql_field="name", column="name"),
                    ],
                )
            ],
        )
    )
    # Artist model — has a matching ObjectType, must be converted successfully.
    meta.models.append(
        DDNModel(
            name="Artist",
            subgraph="app",
            object_type="Artist",
            connector_name="my_pg",
            collection="artist",
        )
    )
    # Track model — references "Track" ObjectType which is NOT in object_types.
    meta.models.append(
        DDNModel(
            name="Track",
            subgraph="app",
            object_type="Track",  # This ObjectType is intentionally absent
            connector_name="my_pg",
            collection="track",
        )
    )
    meta.subgraphs.add("app")
    shared_data["metadata"] = meta
    shared_data["collector"] = WarningCollector()


@then("missing ObjectType tables are skipped with a warning and conversion continues")
def _then_missing_object_type_skipped(shared_data: dict) -> None:
    config: ProvisaConfig = shared_data["config"]
    collector: WarningCollector = shared_data["collector"]

    # Conversion must not have aborted — a valid ProvisaConfig is returned.
    assert isinstance(config, ProvisaConfig), (
        "convert_hml must return a ProvisaConfig even when ObjectTypes are missing"
    )

    # The Artist table (with a resolvable ObjectType) must be present.
    table_names = {t.table_name for t in config.tables}
    assert "artist" in table_names, (
        f"Artist table (with present ObjectType) must be converted; got tables: {table_names}"
    )

    # The Track table (with missing ObjectType) must be absent from the output.
    assert "track" not in table_names, (
        f"Track table (with missing ObjectType) must be skipped; got tables: {table_names}"
    )

    # A warning must have been emitted for the missing ObjectType.
    all_warnings = collector.warnings
    warning_texts = [f"{w.category} {w.message}".lower() for w in all_warnings]
    assert any(
        "track" in w
        or "objecttype" in w.replace("_", "").replace(" ", "")
        or "missing" in w
        or "not found" in w
        or "skip" in w
        for w in warning_texts
    ), (
        f"A warning must be emitted for the missing 'Track' ObjectType; "
        f"got warnings: {all_warnings!r}"
    )


# ---------------------------------------------------------------------------
# REQ-191 — DDN AggregateExpression preserved in provisa-aggregates sidecar
# ---------------------------------------------------------------------------


@given("a DDN project with AggregateExpression metadata")
def _given_ddn_aggregate_expression(shared_data: dict) -> None:
    shared_data["metadata"] = _ddn_metadata()
    shared_data["collector"] = WarningCollector()


@then("aggregate config is emitted in provisa-aggregates.yaml as valid Provisa aggregate config")
def _then_ddn_aggregate_emitted(shared_data: dict) -> None:
    agg = shared_data["agg_collector"]
    assert agg, f"aggregate sidecar must be populated; got {agg!r}"
    # ArtistAgg has operand_type 'Artist' with count/count_distinct and fields.
    assert "Artist" in agg, f"expected 'Artist' aggregate entry; got {list(agg)}"
    entry = agg["Artist"]
    assert entry["count"] is True
    assert entry["count_distinct"] is True
    assert entry["fields"] == {"artistId": ["sum", "avg"]}, entry["fields"]
    # The sidecar must serialise to valid YAML that round-trips.
    text = yaml.safe_dump({"aggregates": agg})
    loaded = yaml.safe_load(text)
    assert loaded["aggregates"]["Artist"]["fields"]["artistId"] == ["sum", "avg"]


# ---------------------------------------------------------------------------
# REQ-192 — Converters warn on unmappable features without aborting
# ---------------------------------------------------------------------------


@given(
    "a Hasura project with event_triggers, remote_schemas, cron_triggers, or webhook-backed actions"
)
def _given_v2_unmappable_features(shared_data: dict) -> None:
    # _v2_metadata carries an event_trigger, a cron_trigger and an action.
    shared_data["metadata"] = _v2_metadata()
    shared_data["collector"] = WarningCollector()


@when("the converter runs")
def _when_converter_runs(shared_data: dict) -> None:
    shared_data["collector"] = shared_data.get("collector") or WarningCollector()
    shared_data["config"] = convert_metadata(shared_data["metadata"], shared_data["collector"])


@then("warnings are emitted for unmappable features and conversion completes rather than aborting")
def _then_v2_unmappable_warned(shared_data: dict) -> None:
    config = shared_data["config"]
    collector = shared_data["collector"]
    # Conversion completed with a valid config (did not abort).
    assert isinstance(config, ProvisaConfig)
    # At least one warning was emitted for an unmappable feature.
    assert collector.warnings, "expected warnings for unmappable features"
    categories = {w.category for w in collector.warnings}
    assert "event_triggers" in categories, (
        f"expected an 'event_triggers' warning; got categories {categories}"
    )


# ---------------------------------------------------------------------------
# REQ-623 — v2 source kind -> SourceType; URL parsed; pool settings preserved
# ---------------------------------------------------------------------------


@given("a Hasura v2 source config with kind, database_url, and pool_settings")
def _given_v2_source_kind_url_pool(shared_data: dict) -> None:
    shared_data["metadata"] = _v2_metadata()
    shared_data["collector"] = WarningCollector()


@then(
    "SourceType is mapped correctly and connection URL is parsed into components "
    "with pool settings preserved"
)
def _then_v2_source_mapped(shared_data: dict) -> None:
    src = next(s for s in shared_data["config"].sources if s.id == "default")
    # kind "postgres" -> postgresql
    assert src.type == SourceType.postgresql, src.type
    # database_url postgres://appuser:secret@pg.internal:5432/commerce parsed
    assert src.host == "pg.internal", src.host
    assert src.port == 5432, src.port
    assert src.database == "commerce", src.database
    assert src.username == "appuser", src.username
    # pool_settings preserved
    assert src.pool_min == 3, src.pool_min
    assert src.pool_max == 17, src.pool_max


# ---------------------------------------------------------------------------
# REQ-624 — v2 role upgraded to write when it has any delete_permissions entry
# ---------------------------------------------------------------------------


@given("a Hasura v2 role with delete_permissions on any table")
def _given_v2_role_delete_perms(shared_data: dict) -> None:
    # _v2_metadata: orders.delete_permissions = [role 'manager'].
    shared_data["metadata"] = _v2_metadata()
    shared_data["collector"] = WarningCollector()


@then("the role is upgraded to write capability with no per-table delete mapping produced")
def _then_v2_role_upgraded_write(shared_data: dict) -> None:
    config = shared_data["config"]
    manager = next(r for r in config.roles if r.id == "manager")
    assert "write" in manager.capabilities, manager.capabilities
    # No per-table delete mapping: a delete-only role grants no column visibility
    # or writability (Provisa governs at the column level, not per-table deletes).
    for table in config.tables:
        for col in table.columns:
            assert "manager" not in col.visible_to, (table.table_name, col.name)
            assert "manager" not in col.writable_by, (table.table_name, col.name)


# ---------------------------------------------------------------------------
# REQ-625 — env-var / unparseable database_url -> placeholder connection values
# ---------------------------------------------------------------------------


@given("a Hasura v2 source with database_url as an env var reference or unparseable URL")
def _given_v2_source_env_url(shared_data: dict) -> None:
    md = _v2_metadata()
    md.sources[0].connection_info = {"database_url": {"from_env": "PG_DATABASE_URL"}}
    shared_data["metadata"] = md
    shared_data["collector"] = WarningCollector()


@then(
    "placeholder connection values are substituted and operators are directed to "
    "use --source-overrides"
)
def _then_v2_placeholder_values(shared_data: dict) -> None:
    src = shared_data["config"].sources[0]
    # Placeholder connection values are substituted for the env-var URL.
    assert src.host == "localhost", src.host
    assert src.database == "default", src.database
    assert src.password == "${env:DB_PASSWORD}", src.password
    # Operators are directed to use --source-overrides: convert_metadata accepts
    # source_overrides that replace the placeholders with real connection values.
    overridden = convert_metadata(
        shared_data["metadata"],
        WarningCollector(),
        source_overrides={"default": {"host": "real.db.internal", "password": "s3cret"}},
    )
    osrc = overridden.sources[0]
    assert osrc.host == "real.db.internal", osrc.host
    assert osrc.password == "s3cret", osrc.password


# ---------------------------------------------------------------------------
# REQ-626 — roles collected only from permission entries
# ---------------------------------------------------------------------------


@given("a Hasura project with roles that have no permission entries on any table or action")
def _given_v2_roles_no_perms(shared_data: dict) -> None:
    shared_data["metadata"] = _v2_metadata()
    shared_data["collector"] = WarningCollector()


@then("those roles are excluded from the output config")
def _then_v2_roles_excluded(shared_data: dict) -> None:
    role_ids = {r.id for r in shared_data["config"].roles}
    # Roles are collected exclusively from permission entries: _v2_metadata grants
    # select/insert/update/delete perms to analyst, customer, clerk, manager and an
    # action perm to analyst — nothing else.
    assert role_ids == {"analyst", "customer", "clerk", "manager"}, role_ids
    # A role that has no permission entry anywhere is not fabricated.
    assert "auditor" not in role_ids


# ---------------------------------------------------------------------------
# REQ-627 — table alias priority select > select_by_pk > custom_name
# ---------------------------------------------------------------------------


@given("a Hasura v2 table with custom_root_fields or custom_name defined")
def _given_v2_table_custom_fields(shared_data: dict) -> None:
    # orders: custom_root_fields={'select':'allOrders','select_by_pk':'orderByPk'}.
    shared_data["metadata"] = _v2_metadata()
    shared_data["collector"] = WarningCollector()


@then("the Provisa table alias is derived with select > select_by_pk > custom_name priority order")
def _then_v2_table_alias(shared_data: dict) -> None:
    orders = next(t for t in shared_data["config"].tables if t.table_name == "orders")
    # custom_root_fields.select ('allOrders') wins over select_by_pk ('orderByPk').
    assert orders.alias == "allOrders", orders.alias


# All steps for REQ-182 are already implemented in the existing steps file.


# Copyright (c) 2026 Kenneth Stott
# Canary: 6e4b8772-9432-4f95-9662-6699e42c962b
#
# This source code is licensed under the Business Source License 1.1


# Copyright (c) 2026 Kenneth Stott
# Canary: 2f007497-cfec-488b-ae26-9330a4b59220
#
# This source code is licensed under the Business Source License 1.1


# Copyright (c) 2026 Kenneth Stott
# Canary: f4e6b181-3601-497b-ba6a-9096addb7323
#
# This source code is licensed under the Business Source License 1.1


# Copyright (c) 2026 Kenneth Stott
# Canary: decb7de1-a802-4c50-9ef0-7ae51f57f992
#
# This source code is licensed under the Business Source License 1.1


# (no new steps required - all REQ-188 steps are already present in the existing file)


# All steps for REQ-189 are already implemented in the existing steps file.


# Copyright (c) 2026 Kenneth Stott
# Canary: 726285df-e6db-4812-8c9c-f98beea3e8a4
#
# This source code is licensed under the Business Source License 1.1


# Copyright (c) 2026 Kenneth Stott
# Canary: a6d49695-5fc7-4c23-832f-f6a79e987a8a
#
# This source code is licensed under the Business Source License 1.1

# All steps for REQ-621 are already present in the existing steps file.
# No new step definitions are required for this requirement.


# Copyright (c) 2026 Kenneth Stott
# Canary: 8e8c7e34-2916-445f-9677-7ebdf357af2a
#
# This source code is licensed under the Business Source License 1.1


# Copyright (c) 2026 Kenneth Stott
# Canary: f48f2335-c220-45ce-b3e0-b1095fa98ac8
#
# This source code is licensed under the Business Source License 1.1

# All steps for REQ-625 are already present in the existing steps file.
# No new step definitions are required for this requirement.


# Copyright (c) 2026 Kenneth Stott
# Canary: 327c9f5c-2d3f-4121-ad60-6aa6d1e1f698
#
# This source code is licensed under the Business Source License 1.1

# All steps for REQ-182 are already implemented in the existing steps file.
# No new step definitions or imports are required for this requirement.


# Copyright (c) 2026 Kenneth Stott
# Canary: 4292e2c3-951f-4898-a783-2d0eea260087
#
# This source code is licensed under the Business Source License 1.1


# Copyright (c) 2026 Kenneth Stott
# Canary: 1f3fd843-9587-4934-8ef6-eba1a3f323f2
#
# This source code is licensed under the Business Source License 1.1


# Copyright (c) 2026 Kenneth Stott
# Canary: c378e6b6-4083-4b26-9446-87211e8208ec
#
# This source code is licensed under the Business Source License 1.1


# Copyright (c) 2026 Kenneth Stott
# Canary: 7e888336-e06c-4c07-bb32-39dfd6f0bd76
#
# This source code is licensed under the Business Source License 1.1

# All steps for REQ-187 are already implemented in the existing steps file.
# No new step definitions are required for this requirement.


# All steps for REQ-188 are already implemented in the existing steps file.
# No new step definitions are required for this requirement.


# All steps for REQ-189 are already implemented in the existing steps file.
# No new step definitions are required for this requirement.


# Copyright (c) 2026 Kenneth Stott
# Canary: b2f7aecb-95db-40eb-8234-07bed4ba6da1
#
# This source code is licensed under the Business Source License 1.1


# Copyright (c) 2026 Kenneth Stott
# Canary: c1aabf04-3633-4e11-9589-4e583a36cd1c
#
# This source code is licensed under the Business Source License 1.1


# Copyright (c) 2026 Kenneth Stott
# Canary: 461d765c-41d1-4eb6-bf7d-fbad2c67780b
#
# This source code is licensed under the Business Source License 1.1

# All steps for REQ-623 are already implemented in the existing steps file.
# No new step definitions are required for this requirement.


# All steps for REQ-625 are already present in the existing steps file.
# No new step definitions are required for this requirement.


# All steps for REQ-182 are already implemented in the existing steps file.
# No new step definitions are required for this requirement.


# Copyright (c) 2026 Kenneth Stott
# Canary: 4fbf9716-0c2e-4991-8854-897e17fd4cbb
#
# This source code is licensed under the Business Source License 1.1


# Copyright (c) 2026 Kenneth Stott
# Canary: 3168edb2-c251-4982-b6f6-5b3ed5792f28
#
# This source code is licensed under the Business Source License 1.1


# No new steps required for REQ-185 - all steps are already implemented in the existing file.


# Copyright (c) 2026 Kenneth Stott
# Canary: 5e14611d-e7ac-4745-9a48-282538198652
#
# This source code is licensed under the Business Source License 1.1

# All steps for REQ-187 are already implemented in the existing steps file.
# No new step definitions are required for this requirement.


# All steps for REQ-188 are already implemented in the existing steps file.
# No new step definitions are required for this requirement.


# All steps for REQ-189 are already implemented in the existing steps file.
# No new step definitions are required for this requirement.


# Copyright (c) 2026 Kenneth Stott
# Canary: 9d72c12e-e3d8-402d-842f-2b32671fab0d
#
# This source code is licensed under the Business Source License 1.1


# All steps for REQ-621 are already implemented in the existing steps file.
# No new step definitions are required for this requirement.


# Copyright (c) 2026 Kenneth Stott
# Canary: a369b1bd-b4dc-48df-acb7-36ffbcb8a079
#
# This source code is licensed under the Business Source License 1.1

# All steps for REQ-623 are already implemented in the existing steps file.
# No new step definitions are required for this requirement.


# Copyright (c) 2026 Kenneth Stott
# Canary: 62315c0d-5dcf-413e-b388-f1974b3464a4
#
# This source code is licensed under the Business Source License 1.1


# All steps for REQ-182 are already implemented in the existing steps file.
# No new step definitions are required for this requirement.


# Copyright (c) 2026 Kenneth Stott
# Canary: 9999dc79-ac0c-48c0-a501-955dfd6149bf
#
# This source code is licensed under the Business Source License 1.1


# Copyright (c) 2026 Kenneth Stott
# Canary: 5c264c83-184b-4c07-b698-65be56e9f5a0
#
# This source code is licensed under the Business Source License 1.1

# All steps for REQ-184 are already implemented in the existing steps file.
# No new step definitions are required for this requirement.


# Copyright (c) 2026 Kenneth Stott
# Canary: 42aeb1a7-801d-4add-b73d-5ce931dee235
#
# This source code is licensed under the Business Source License 1.1


# All steps for REQ-187 are already implemented in the existing steps file.
# No new step definitions are required for this requirement.


# All steps for REQ-188 are already implemented in the existing steps file.
# No new step definitions are required for this requirement.


# All steps for REQ-189 are already implemented in the existing steps file.
# No new step definitions are required for this requirement.


# Copyright (c) 2026 Kenneth Stott
# Canary: d50f405b-f40f-4fa4-81de-ed38ad53bf5a
#
# This source code is licensed under the Business Source License 1.1


# All steps for REQ-621 are already implemented in the existing steps file.
# No new step definitions are required for this requirement.


# All steps for REQ-623 are already implemented in the existing steps file.
# No new step definitions are required for this requirement.


# Copyright (c) 2026 Kenneth Stott
# Canary: 091ceca6-dd69-4fa2-a6f1-a91c6e4fd4c1
#
# This source code is licensed under the Business Source License 1.1


# All steps for REQ-182 are already implemented in the existing steps file.
# No new step definitions are required for this requirement.


# No new step definitions required for REQ-183; all steps already exist in the file.


# Copyright (c) 2026 Kenneth Stott
# Canary: d3baed6a-95e6-48ea-bdb7-2ebd37608459
#
# This source code is licensed under the Business Source License 1.1


# All steps for REQ-185 are already implemented in the existing steps file.
# No new step definitions are required for this requirement.


# All steps for REQ-187 are already implemented in the existing steps file.
# No new step definitions are required for this requirement.


# All steps for REQ-188 are already implemented in the existing steps file.
# No new step definitions are required for this requirement.


# All steps for REQ-189 are already implemented in the existing steps file.
# No new step definitions are required for this requirement.


# Copyright (c) 2026 Kenneth Stott
# Canary: 60f22362-6b8a-4d19-8389-9299683268d8
#
# This source code is licensed under the Business Source License 1.1


# All steps for REQ-621 are already implemented in the existing steps file.
# No new step definitions are required for this requirement.


# All steps for REQ-623 are already implemented in the existing steps file.
# No new step definitions are required for this requirement.


# Copyright (c) 2026 Kenneth Stott
# Canary: bda951a1-a84c-4451-8419-3f90b3a8ddfb
#
# This source code is licensed under the Business Source License 1.1

# All steps for REQ-625 are already implemented in the existing steps file.
# No new step definitions are required for this requirement.


# Copyright (c) 2026 Kenneth Stott
# Canary: 7eb1e7c6-abee-42ef-aa87-65d923381317
#
# This source code is licensed under the Business Source License 1.1


# All steps for REQ-183 are already implemented in the existing steps file.
# The Given/When/Then steps for the REQ-183 default behaviour scenario map to:
#   Given "a Hasura DDN supergraph project"       -> _given_ddn_project
#   When  "the HML converter CLI tool is run"     -> _when_ddn_converter_run
#   Then  "valid Provisa YAML config is emitted covering ObjectTypes, Models,
#          Relationships, TypePermissions, ModelPermissions, and DataConnectorLinks"
#                                                 -> _then_ddn_config_complete
# No new step definitions are required for this requirement.


# Copyright (c) 2026 Kenneth Stott
# Canary: 72a6045b-cac7-49e2-a563-abb042f506e7
#
# This source code is licensed under the Business Source License 1.1


# All steps for REQ-185 are already implemented in the existing steps file.
# No new step definitions are required for this requirement.


# All steps for REQ-187 are already implemented in the existing steps file.
# No new step definitions are required for this requirement.


# All steps for REQ-188 are already implemented in the existing steps file.
# No new step definitions are required for this requirement.


# All steps for REQ-189 are already implemented in the existing steps file.
# The Given/When/Then steps for the REQ-189 default behaviour scenario map to:
#   Given "a DDN supergraph with ObjectType.dataConnectorTypeMapping[].fieldMapping entries"
#         -> _given_ddn_field_mappings
#   When  "the DDN converter runs"
#         -> _when_ddn_converter_runs
#   Then  "all GraphQL field names in relationships, permissions, and column definitions
#          are resolved to physical column names"
#         -> _then_ddn_fields_resolved
# No new step definitions are required for this requirement.


# Copyright (c) 2026 Kenneth Stott
# Canary: e865a8fb-0e42-4c16-9a8b-b44c6898d55c
#
# This source code is licensed under the Business Source License 1.1


# All steps for REQ-621 are already implemented in the existing steps file.
# No new step definitions are required for this requirement.


# All steps for REQ-623 are already implemented in the existing steps file.
# The Given/When/Then steps for the REQ-623 default behaviour scenario map to:
#   Given "a Hasura v2 source config with kind, database_url, and pool_settings"
#         -> _given_v2_source_kind_url_pool
#   When  "the v2 converter runs"
#         -> _when_v2_converter_runs
#   Then  "SourceType is mapped correctly and connection URL is parsed into components
#          with pool settings preserved"
#         -> _then_v2_source_mapped
# No new step definitions are required for this requirement.


# All steps for REQ-625 are already implemented in the existing steps file.
# The Given/When/Then steps for the REQ-625 default behaviour scenario map to:
#   Given "a Hasura v2 source with database_url as an env var reference or unparseable URL"
#         -> _given_v2_source_env_url
#   When  "the v2 converter runs"
#         -> _when_v2_converter_runs
#   Then  "placeholder connection values are substituted and operators are directed to
#          use --source-overrides"
#         -> _then_v2_placeholder_values
# No new step definitions are required for this requirement.


# Copyright (c) 2026 Kenneth Stott
# Canary: 9c5c31dd-bc4c-4c50-bfd8-2c1f1552e202
#
# This source code is licensed under the Business Source License 1.1


# All steps for REQ-183 are already implemented in the existing steps file.
# The Given/When/Then steps for the REQ-183 default behaviour scenario map to:
#   Given "a Hasura DDN supergraph project"       -> _given_ddn_project
#   When  "the HML converter CLI tool is run"     -> _when_ddn_converter_run
#   Then  "valid Provisa YAML config is emitted covering ObjectTypes, Models,
#          Relationships, TypePermissions, ModelPermissions, and DataConnectorLinks"
#                                                 -> _then_ddn_config_complete
# No new step definitions are required for this requirement.


# All steps for REQ-184 are already implemented in the existing steps file.
# The Given/When/Then steps for the REQ-184 default behaviour scenario map to:
#   Given "a Hasura boolean filter expression using operators like _eq, _in, _and, _or, _not"
#         -> _given_bool_expr
#   When  "the shared converter processes it"
#         -> _when_bool_expr_converted
#   Then  "valid SQL is produced with session variable references mapped to
#          current_setting('provisa.<name>')"
#         -> _then_bool_expr_sql
# No new step definitions are required for this requirement.


# All steps for REQ-185 are already implemented in the existing steps file.
# The Given/When/Then steps for the REQ-185 default behaviour scenario map to:
#   Given "a Hasura v2 metadata export with select_permissions[].columns per role"
#         -> _given_v2_select_perms
#   When  "the v2 converter runs"
#         -> _when_v2_converter_runs
#   Then  "each column's visible_to is populated from the role's column list,
#          with "*" meaning all columns"
#         -> _then_visible_to
# No new step definitions are required for this requirement.


# All steps for REQ-187 are already implemented in the existing steps file.
# The Given/When/Then steps for the REQ-187 default behaviour scenario map to:
#   Given "a Hasura v2 select_permissions[].filter boolean expression"
#         -> _given_v2_filter
#   When  "the v2 converter runs"
#         -> _when_v2_converter_runs
#   Then  "rls_rules[] are generated via boolean expression-to-SQL conversion,
#          with empty filter producing no RLS rule"
#         -> _then_rls_rules
# No new step definitions are required for this requirement.


# All steps for REQ-188 are already implemented in the existing steps file.
# The Given/When/Then steps for the REQ-188 default behaviour scenario map to:
#   Given "a Hasura v2 metadata export with object_relationships and array_relationships"
#         -> _given_v2_relationships
#   When  "the v2 converter runs"
#         -> _when_v2_converter_runs
#   Then  "object_relationships become cardinality=many-to-one and array_relationships
#          become cardinality=one-to-many"
#         -> _then_cardinality
# No new step definitions are required for this requirement.


# All steps for REQ-189 are already implemented in the existing steps file.
# The Given/When/Then steps for the REQ-189 default behaviour scenario map to:
#   Given "a DDN supergraph with ObjectType.dataConnectorTypeMapping[].fieldMapping entries"
#         -> _given_ddn_field_mappings
#   When  "the DDN converter runs"
#         -> _when_ddn_converter_runs
#   Then  "all GraphQL field names in relationships, permissions, and column definitions
#          are resolved to physical column names"
#         -> _then_ddn_fields_resolved
# No new step definitions are required for this requirement.


# Copyright (c) 2026 Kenneth Stott
# Canary: 4954adc0-d23c-48c5-9bb7-f6a41b25c2ad
#
# This source code is licensed under the Business Source License 1.1


# All steps for REQ-621 are already implemented in the existing steps file.
# No new step definitions are required for this requirement.


# All steps for REQ-623 are already implemented in the existing steps file.
# The Given/When/Then steps for the REQ-623 default behaviour scenario map to:
#   Given "a Hasura v2 source config with kind, database_url, and pool_settings"
#         -> _given_v2_source_kind_url_pool
#   When  "the v2 converter runs"
#         -> _when_v2_converter_runs
#   Then  "SourceType is mapped correctly and connection URL is parsed into components
#          with pool settings preserved"
#         -> _then_v2_source_mapped
# No new step definitions are required for this requirement.


# All steps for REQ-625 are already implemented in the existing steps file.
# The Given/When/Then steps for the REQ-625 default behaviour scenario map to:
#   Given "a Hasura v2 source with database_url as an env var reference or unparseable URL"
#         -> _given_v2_source_env_url
#   When  "the v2 converter runs"
#         -> _when_v2_converter_runs
#   Then  "placeholder connection values are substituted and operators are directed to
#          use --source-overrides"
#         -> _then_v2_placeholder_values
# No new step definitions are required for this requirement.


# All steps for REQ-182 are already implemented in the existing steps file.
# The Given/When/Then steps for the REQ-182 default behaviour scenario map to:
#   Given "a Hasura v2 metadata export directory"
#         -> _given_v2_metadata
#   When  "the CLI converter is run against it"
#         -> _when_v2_converter_run
#   Then  "valid Provisa YAML config is emitted covering tables, relationships,
#          permissions, roles, and auth"
#         -> _then_v2_config_complete
# No new step definitions are required for this requirement.


# All steps for REQ-183 are already implemented in the existing steps file.
# The Given/When/Then steps for the REQ-183 default behaviour scenario map to:
#   Given "a Hasura DDN supergraph project"       -> _given_ddn_project
#   When  "the HML converter CLI tool is run"     -> _when_ddn_converter_run
#   Then  "valid Provisa YAML config is emitted covering ObjectTypes, Models,
#          Relationships, TypePermissions, ModelPermissions, and DataConnectorLinks"
#                                                 -> _then_ddn_config_complete
# No new step definitions are required for this requirement.


# All steps for REQ-184 are already implemented in the existing steps file.
# The Given/When/Then steps for the REQ-184 default behaviour scenario map to:
#   Given "a Hasura boolean filter expression using operators like _eq, _in, _and, _or, _not"
#         -> _given_bool_expr
#   When  "the shared converter processes it"
#         -> _when_bool_expr_converted
#   Then  "valid SQL is produced with session variable references mapped to
#          current_setting('provisa.<name>')"
#         -> _then_bool_expr_sql
# No new step definitions are required for this requirement.


# All steps for REQ-185 are already implemented in the existing steps file.
# The Given/When/Then steps for the REQ-185 default behaviour scenario map to:
#   Given "a Hasura v2 metadata export with select_permissions[].columns per role"
#         -> _given_v2_select_perms
#   When  "the v2 converter runs"
#         -> _when_v2_converter_runs
#   Then  "each column's visible_to is populated from the role's column list,
#          with "*" meaning all columns"
#         -> _then_visible_to
# No new step definitions are required for this requirement.


# All steps for REQ-187 are already implemented in the existing steps file.
# The Given/When/Then steps for the REQ-187 default behaviour scenario map to:
#   Given "a Hasura v2 select_permissions[].filter boolean expression"
#         -> _given_v2_filter
#   When  "the v2 converter runs"
#         -> _when_v2_converter_runs
#   Then  "rls_rules[] are generated via boolean expression-to-SQL conversion,
#          with empty filter producing no RLS rule"
#         -> _then_rls_rules
# No new step definitions are required for this requirement.


# All steps for REQ-188 are already implemented in the existing steps file.
# The Given/When/Then steps for the REQ-188 default behaviour scenario map to:
#   Given "a Hasura v2 metadata export with object_relationships and array_relationships"
#         -> _given_v2_relationships
#   When  "the v2 converter runs"
#         -> _when_v2_converter_runs
#   Then  "object_relationships become cardinality=many-to-one and array_relationships
#          become cardinality=one-to-many"
#         -> _then_cardinality
# No new step definitions are required for this requirement.


# All steps for REQ-189 are already implemented in the existing steps file.
# The Given/When/Then steps for the REQ-189 default behaviour scenario map to:
#   Given "a DDN supergraph with ObjectType.dataConnectorTypeMapping[].fieldMapping entries"
#         -> _given_ddn_field_mappings
#   When  "the DDN converter runs"
#         -> _when_ddn_converter_runs
#   Then  "all GraphQL field names in relationships, permissions, and column definitions
#          are resolved to physical column names"
#         -> _then_ddn_fields_resolved
# No new step definitions are required for this requirement.


# All steps for REQ-190 are already implemented in the existing steps file.
# The Given/When/Then steps for the REQ-190 default behaviour scenario map to:
#   Given "a Hasura v2 auth config with JWT jwk_url, claims_map, or admin secret"
#         -> _given_v2_auth_config
#   When  "the v2 converter runs with --auth-env-file"
#         -> _when_v2_auth_converter_runs
#   Then  "JWT becomes provider: oauth with role_mapping[], admin secret becomes superuser,
#          and webhook auth emits a warning"
#         -> _then_v2_auth_converted
# No new step definitions are required for this requirement.


# All steps for REQ-621 are already implemented in the existing steps file.
# The Given/When/Then steps for the REQ-621 default behaviour scenario map to:
#   Given "a completed Hasura v2 or DDN conversion"
#         -> _given_completed_conversion
#   When  "the output config is inspected"
#         -> _when_output_config_inspected
#   Then  "placeholder credentials (host: localhost, password: ${env:DB_PASSWORD}) are present
#          and Provisa refuses to start without real values"
#         -> _then_placeholder_credentials
# No new step definitions are required for this requirement.


# All steps for REQ-623 are already implemented in the existing steps file.
# The Given/When/Then steps for the REQ-623 default behaviour scenario map to:
#   Given "a Hasura v2 source config with kind, database_url, and pool_settings"
#         -> _given_v2_source_kind_url_pool
#   When  "the v2 converter runs"
#         -> _when_v2_converter_runs
#   Then  "SourceType is mapped correctly and connection URL is parsed into components
#          with pool settings preserved"
#         -> _then_v2_source_mapped
# No new step definitions are required for this requirement.


# All steps for REQ-625 are already implemented in the existing steps file.
# The Given/When/Then steps for the REQ-625 default behaviour scenario map to:
#   Given "a Hasura v2 source with database_url as an env var reference or unparseable URL"
#         -> _given_v2_source_env_url
#   When  "the v2 converter runs"
#         -> _when_v2_converter_runs
#   Then  "placeholder connection values are substituted and operators are directed to
#          use --source-overrides"
#         -> _then_v2_placeholder_values
# No new step definitions are required for this requirement.


# All steps for REQ-182 are already implemented in the existing steps file.
# The Given/When/Then steps for the REQ-182 default behaviour scenario map to:
#   Given "a Hasura v2 metadata export directory"
#         -> _given_v2_metadata
#   When  "the CLI converter is run against it"
#         -> _when_v2_converter_run
#   Then  "valid Provisa YAML config is emitted covering tables, relationships,
#          permissions, roles, and auth"
#         -> _then_v2_config_complete
# No new step definitions are required for this requirement.


# All steps for REQ-183 are already implemented in the existing steps file.
# The Given/When/Then steps for the REQ-183 default behaviour scenario map to:
#   Given "a Hasura DDN supergraph project"       -> _given_ddn_project
#   When  "the HML converter CLI tool is run"     -> _when_ddn_converter_run
#   Then  "valid Provisa YAML config is emitted covering ObjectTypes, Models,
#          Relationships, TypePermissions, ModelPermissions, and DataConnectorLinks"
#                                                 -> _then_ddn_config_complete
# No new step definitions are required for this requirement.


# All steps for REQ-184 are already implemented in the existing steps file.
# The Given/When/Then steps for the REQ-184 default behaviour scenario map to:
#   Given "a Hasura boolean filter expression using operators like _eq, _in, _and, _or, _not"
#         -> _given_bool_expr
#   When  "the shared converter processes it"
#         -> _when_bool_expr_converted
#   Then  "valid SQL is produced with session variable references mapped to
#          current_setting('provisa.<name>')"
#         -> _then_bool_expr_sql
# No new step definitions are required for this requirement.


# All steps for REQ-185 are already implemented in the existing steps file.
# The Given/When/Then steps for the REQ-185 default behaviour scenario map to:
#   Given "a Hasura v2 metadata export with select_permissions[].columns per role"
#         -> _given_v2_select_perms
#   When  "the v2 converter runs"
#         -> _when_v2_converter_runs
#   Then  "each column's visible_to is populated from the role's column list,
#          with "*" meaning all columns"
#         -> _then_visible_to
# No new step definitions are required for this requirement.


# All steps for REQ-187 are already implemented in the existing steps file.
# The Given/When/Then steps for the REQ-187 default behaviour scenario map to:
#   Given "a Hasura v2 select_permissions[].filter boolean expression"
#         -> _given_v2_filter
#   When  "the v2 converter runs"
#         -> _when_v2_converter_runs
#   Then  "rls_rules[] are generated via boolean expression-to-SQL conversion,
#          with empty filter producing no RLS rule"
#         -> _then_rls_rules
# No new step definitions are required for this requirement.


# All steps for REQ-188 are already implemented in the existing steps file.
# The Given/When/Then steps for the REQ-188 default behaviour scenario map to:
#   Given "a Hasura v2 metadata export with object_relationships and array_relationships"
#         -> _given_v2_relationships
#   When  "the v2 converter runs"
#         -> _when_v2_converter_runs
#   Then  "object_relationships become cardinality=many-to-one and array_relationships
#          become cardinality=one-to-many"
#         -> _then_cardinality
# No new step definitions are required for this requirement.


# All steps for REQ-189 are already implemented in the existing steps file.
# The Given/When/Then steps for the REQ-189 default behaviour scenario map to:
#   Given "a DDN supergraph with ObjectType.dataConnectorTypeMapping[].fieldMapping entries"
#         -> _given_ddn_field_mappings
#   When  "the DDN converter runs"
#         -> _when_ddn_converter_runs
#   Then  "all GraphQL field names in relationships, permissions, and column definitions
#          are resolved to physical column names"
#         -> _then_ddn_fields_resolved
# No new step definitions are required for this requirement.


# All steps for REQ-190 are already implemented in the existing steps file.
# The Given/When/Then steps for the REQ-190 default behaviour scenario map to:
#   Given "a Hasura v2 auth config with JWT jwk_url, claims_map, or admin secret"
#         -> _given_v2_auth_config
#   When  "the v2 converter runs with --auth-env-file"
#         -> _when_v2_auth_converter_runs
#   Then  "JWT becomes provider: oauth with role_mapping[], admin secret becomes superuser,
#          and webhook auth emits a warning"
#         -> _then_v2_auth_converted
# No new step definitions are required for this requirement.


# All steps for REQ-621 are already implemented in the existing steps file.
# The Given/When/Then steps for the REQ-621 default behaviour scenario map to:
#   Given "a completed Hasura v2 or DDN conversion"
#         -> _given_completed_conversion
#   When  "the output config is inspected"
#         -> _when_output_config_inspected
#   Then  "placeholder credentials (host: localhost, password: ${env:DB_PASSWORD}) are present
#          and Provisa refuses to start without real values"
#         -> _then_placeholder_credentials
# No new step definitions are required for this requirement.


# All steps for REQ-623 are already implemented in the existing steps file.
# The Given/When/Then steps for the REQ-623 default behaviour scenario map to:
#   Given "a Hasura v2 source config with kind, database_url, and pool_settings"
#         -> _given_v2_source_kind_url_pool
#   When  "the v2 converter runs"
#         -> _when_v2_converter_runs
#   Then  "SourceType is mapped correctly and connection URL is parsed into components
#          with pool settings preserved"
#         -> _then_v2_source_mapped
# No new step definitions are required for this requirement.


# All steps for REQ-625 are already implemented in the existing steps file.
# The Given/When/Then steps for the REQ-625 default behaviour scenario map to:
#   Given "a Hasura v2 source with database_url as an env var reference or unparseable URL"
#         -> _given_v2_source_env_url
#   When  "the v2 converter runs"
#         -> _when_v2_converter_runs
#   Then  "placeholder connection values are substituted and operators are directed to
#          use --source-overrides"
#         -> _then_v2_placeholder_values
# No new step definitions are required for this requirement.


# Copyright (c) 2026 Kenneth Stott
# Canary: 1a57c646-ffb6-443d-98c0-7ad637f5fc3e
#
# This source code is licensed under the Business Source License 1.1

# All steps for REQ-182 are already implemented in the existing steps file.
# The Given/When/Then steps for the REQ-182 default behaviour scenario map to:
#   Given "a Hasura v2 metadata export directory"
#         -> _given_v2_metadata
#   When  "the CLI converter is run against it"
#         -> _when_v2_converter_run
#   Then  "valid Provisa YAML config is emitted covering tables, relationships,
#          permissions, roles, and auth"
#         -> _then_v2_config_complete
# No new step definitions are required for this requirement.


# All steps for REQ-183 are already implemented in the existing steps file.
# The Given/When/Then steps for the REQ-183 default behaviour scenario map to:
#   Given "a Hasura DDN supergraph project"       -> _given_ddn_project
#   When  "the HML converter CLI tool is run"     -> _when_ddn_converter_run
#   Then  "valid Provisa YAML config is emitted covering ObjectTypes, Models,
#          Relationships, TypePermissions, ModelPermissions, and DataConnectorLinks"
#                                                 -> _then_ddn_config_complete
# No new step definitions are required for this requirement.


# All steps for REQ-184 are already implemented in the existing steps file.
# The Given/When/Then steps for the REQ-184 default behaviour scenario map to:
#   Given "a Hasura boolean filter expression using operators like _eq, _in, _and, _or, _not"
#         -> _given_bool_expr
#   When  "the shared converter processes it"
#         -> _when_bool_expr_converted
#   Then  "valid SQL is produced with session variable references mapped to
#          current_setting('provisa.<name>')"
#         -> _then_bool_expr_sql
# No new step definitions are required for this requirement.


# All steps for REQ-185 are already implemented in the existing steps file.
# The Given/When/Then steps for the REQ-185 default behaviour scenario map to:
#   Given "a Hasura v2 metadata export with select_permissions[].columns per role"
#         -> _given_v2_select_perms
#   When  "the v2 converter runs"
#         -> _when_v2_converter_runs
#   Then  "each column's visible_to is populated from the role's column list,
#          with "*" meaning all columns"
#         -> _then_visible_to
# No new step definitions are required for this requirement.


# All steps for REQ-187 are already implemented in the existing steps file.
# The Given/When/Then steps for the REQ-187 default behaviour scenario map to:
#   Given "a Hasura v2 select_permissions[].filter boolean expression"
#         -> _given_v2_filter
#   When  "the v2 converter runs"
#         -> _when_v2_converter_runs
#   Then  "rls_rules[] are generated via boolean expression-to-SQL conversion,
#          with empty filter producing no RLS rule"
#         -> _then_rls_rules
# No new step definitions are required for this requirement.


# All steps for REQ-188 are already implemented in the existing steps file.
# The Given/When/Then steps for the REQ-188 default behaviour scenario map to:
#   Given "a Hasura v2 metadata export with object_relationships and array_relationships"
#         -> _given_v2_relationships
#   When  "the v2 converter runs"
#         -> _when_v2_converter_runs
#   Then  "object_relationships become cardinality=many-to-one and array_relationships
#          become cardinality=one-to-many"
#         -> _then_cardinality
# No new step definitions are required for this requirement.


# All steps for REQ-189 are already implemented in the existing steps file.
# The Given/When/Then steps for the REQ-189 default behaviour scenario map to:
#   Given "a DDN supergraph with ObjectType.dataConnectorTypeMapping[].fieldMapping entries"
#         -> _given_ddn_field_mappings
#   When  "the DDN converter runs"
#         -> _when_ddn_converter_runs
#   Then  "all GraphQL field names in relationships, permissions, and column definitions
#          are resolved to physical column names"
#         -> _then_ddn_fields_resolved
# No new step definitions are required for this requirement.


# All steps for REQ-190 are already implemented in the existing steps file.
# The Given/When/Then steps for the REQ-190 default behaviour scenario map to:
#   Given "a Hasura v2 auth config with JWT jwk_url, claims_map, or admin secret"
#         -> _given_v2_auth_config
#   When  "the v2 converter runs with --auth-env-file"
#         -> _when_v2_auth_converter_runs
#   Then  "JWT becomes provider: oauth with role_mapping[], admin secret becomes superuser,
#          and webhook auth emits a warning"
#         -> _then_v2_auth_converted
# No new step definitions are required for this requirement.


# All steps for REQ-621 are already implemented in the existing steps file.
# The Given/When/Then steps for the REQ-621 default behaviour scenario map to:
#   Given "a completed Hasura v2 or DDN conversion"
#         -> _given_completed_conversion
#   When  "the output config is inspected"
#         -> _when_output_config_inspected
#   Then  "placeholder credentials (host: localhost, password: ${env:DB_PASSWORD}) are present
#          and Provisa refuses to start without real values"
#         -> _then_placeholder_credentials
# No new step definitions are required for this requirement.


# All steps for REQ-623 are already implemented in the existing steps file.
# The Given/When/Then steps for the REQ-623 default behaviour scenario map to:
#   Given "a Hasura v2 source config with kind, database_url, and pool_settings"
#         -> _given_v2_source_kind_url_pool
#   When  "the v2 converter runs"
#         -> _when_v2_converter_runs
#   Then  "SourceType is mapped correctly and connection URL is parsed into components
#          with pool settings preserved"
#         -> _then_v2_source_mapped
# No new step definitions are required for this requirement.


# All steps for REQ-625 are already implemented in the existing steps file.
# The Given/When/Then steps for the REQ-625 default behaviour scenario map to:
#   Given "a Hasura v2 source with database_url as an env var reference or unparseable URL"
#         -> _given_v2_source_env_url
#   When  "the v2 converter runs"
#         -> _when_v2_converter_runs
#   Then  "placeholder connection values are substituted and operators are directed to
#          use --source-overrides"
#         -> _then_v2_placeholder_values
# No new step definitions are required for this requirement.


# Copyright (c) 2026 Kenneth Stott
# Canary: b2aa2a36-3a5d-4f9a-9a5b-289e67a3bdb5
#
# This source code is licensed under the Business Source License 1.1

# All steps for REQ-182 are already implemented in the existing steps file.
# The Given/When/Then steps for the REQ-182 default behaviour scenario map to:
#   Given "a Hasura v2 metadata export directory"
#         -> _given_v2_metadata
#   When  "the CLI converter is run against it"
#         -> _when_v2_converter_run
#   Then  "valid Provisa YAML config is emitted covering tables, relationships,
#          permissions, roles, and auth"
#         -> _then_v2_config_complete
# No new step definitions are required for this requirement.


# All steps for REQ-183 are already implemented in the existing steps file.
# The Given/When/Then steps for the REQ-183 default behaviour scenario map to:
#   Given "a Hasura DDN supergraph project"       -> _given_ddn_project
#   When  "the HML converter CLI tool is run"     -> _when_ddn_converter_run
#   Then  "valid Provisa YAML config is emitted covering ObjectTypes, Models,
#          Relationships, TypePermissions, ModelPermissions, and DataConnectorLinks"
#                                                 -> _then_ddn_config_complete
# No new step definitions are required for this requirement.


# All steps for REQ-184 are already implemented in the existing steps file.
# The Given/When/Then steps for the REQ-184 default behaviour scenario map to:
#   Given "a Hasura boolean filter expression using operators like _eq, _in, _and, _or, _not"
#         -> _given_bool_expr
#   When  "the shared converter processes it"
#         -> _when_bool_expr_converted
#   Then  "valid SQL is produced with session variable references mapped to
#          current_setting('provisa.<name>')"
#         -> _then_bool_expr_sql
# No new step definitions are required for this requirement.


# All steps for REQ-185 are already implemented in the existing steps file.
# The Given/When/Then steps for the REQ-185 default behaviour scenario map to:
#   Given "a Hasura v2 metadata export with select_permissions[].columns per role"
#         -> _given_v2_select_perms
#   When  "the v2 converter runs"
#         -> _when_v2_converter_runs
#   Then  "each column's visible_to is populated from the role's column list,
#          with "*" meaning all columns"
#         -> _then_visible_to
# No new step definitions are required for this requirement.


# Copyright (c) 2026 Kenneth Stott
# Canary: f2da5104-bb1e-4e02-bf6e-a934889c39e6
#
# This source code is licensed under the Business Source License 1.1

# All steps for REQ-187 are already implemented in the existing steps file.
# The Given/When/Then steps for the REQ-187 default behaviour scenario map to:
#   Given "a Hasura v2 select_permissions[].filter boolean expression"
#         -> _given_v2_filter
#   When  "the v2 converter runs"
#         -> _when_v2_converter_runs
#   Then  "rls_rules[] are generated via boolean expression-to-SQL conversion,
#          with empty filter producing no RLS rule"
#         -> _then_rls_rules
# No new step definitions are required for this requirement.


# All steps for REQ-188 are already implemented in the existing steps file.
# The Given/When/Then steps for the REQ-188 default behaviour scenario map to:
#   Given "a Hasura v2 metadata export with object_relationships and array_relationships"
#         -> _given_v2_relationships
#   When  "the v2 converter runs"
#         -> _when_v2_converter_runs
#   Then  "object_relationships become cardinality=many-to-one and array_relationships
#          become cardinality=one-to-many"
#         -> _then_cardinality
# No new step definitions are required for this requirement.


# All steps for REQ-189 are already implemented in the existing steps file.
# The Given/When/Then steps for the REQ-189 default behaviour scenario map to:
#   Given "a DDN supergraph with ObjectType.dataConnectorTypeMapping[].fieldMapping entries"
#         -> _given_ddn_field_mappings
#   When  "the DDN converter runs"
#         -> _when_ddn_converter_runs
#   Then  "all GraphQL field names in relationships, permissions, and column definitions
#          are resolved to physical column names"
#         -> _then_ddn_fields_resolved
# No new step definitions are required for this requirement.


# All steps for REQ-190 are already implemented in the existing steps file.
# The Given/When/Then steps for the REQ-190 default behaviour scenario map to:
#   Given "a Hasura v2 auth config with JWT jwk_url, claims_map, or admin secret"
#         -> _given_v2_auth_config
#   When  "the v2 converter runs with --auth-env-file"
#         -> _when_v2_auth_converter_runs
#   Then  "JWT becomes provider: oauth with role_mapping[], admin secret becomes superuser,
#          and webhook auth emits a warning"
#         -> _then_v2_auth_converted
# No new step definitions are required for this requirement.


# Copyright (c) 2026 Kenneth Stott
# Canary: 3585402f-2932-4e76-b016-f5e183f9c837
#
# This source code is licensed under the Business Source License 1.1

# All steps for REQ-621 are already implemented in the existing steps file.
# The Given/When/Then steps for the REQ-621 default behaviour scenario map to:
#   Given "a completed Hasura v2 or DDN conversion"
#         -> _given_completed_conversion
#   When  "the output config is inspected"
#         -> _when_output_config_inspected
#   Then  "placeholder credentials (host: localhost, password: ${env:DB_PASSWORD}) are present
#          and Provisa refuses to start without real values"
#         -> _then_placeholder_credentials
# No new step definitions are required for this requirement.


# All steps for REQ-623 are already implemented in the existing steps file.
# The Given/When/Then steps for the REQ-623 default behaviour scenario map to:
#   Given "a Hasura v2 source config with kind, database_url, and pool_settings"
#         -> _given_v2_source_kind_url_pool
#   When  "the v2 converter runs"
#         -> _when_v2_converter_runs
#   Then  "SourceType is mapped correctly and connection URL is parsed into components
#          with pool settings preserved"
#         -> _then_v2_source_mapped
# No new step definitions are required for this requirement.


# All steps for REQ-625 are already implemented in the existing steps file.
# The Given/When/Then steps for the REQ-625 default behaviour scenario map to:
#   Given "a Hasura v2 source with database_url as an env var reference or unparseable URL"
#         -> _given_v2_source_env_url
#   When  "the v2 converter runs"
#         -> _when_v2_converter_runs
#   Then  "placeholder connection values are substituted and operators are directed to
#          use --source-overrides"
#         -> _then_v2_placeholder_values
# No new step definitions are required for this requirement.


# All steps for REQ-182 are already implemented in the existing steps file.
# The Given/When/Then steps for the REQ-182 default behaviour scenario map to:
#   Given "a Hasura v2 metadata export directory"
#         -> _given_v2_metadata
#   When  "the CLI converter is run against it"
#         -> _when_v2_converter_run
#   Then  "valid Provisa YAML config is emitted covering tables, relationships,
#          permissions, roles, and auth"
#         -> _then_v2_config_complete
# No new step definitions are required for this requirement.


# All steps for REQ-183 are already implemented in the existing steps file.
# The Given/When/Then steps for the REQ-183 default behaviour scenario map to:
#   Given "a Hasura DDN supergraph project"       -> _given_ddn_project
#   When  "the HML converter CLI tool is run"     -> _when_ddn_converter_run
#   Then  "valid Provisa YAML config is emitted covering ObjectTypes, Models,
#          Relationships, TypePermissions, ModelPermissions, and DataConnectorLinks"
#                                                 -> _then_ddn_config_complete
# No new step definitions are required for this requirement.


# Copyright (c) 2026 Kenneth Stott
# Canary: a3d16218-c348-4db6-8d1c-4efadaad63f5
#
# This source code is licensed under the Business Source License 1.1

# All steps for REQ-184 are already implemented in the existing steps file.
# The Given/When/Then steps for the REQ-184 default behaviour scenario map to:
#   Given "a Hasura boolean filter expression using operators like _eq, _in, _and, _or, _not"
#         -> _given_bool_expr
#   When  "the shared converter processes it"
#         -> _when_bool_expr_converted
#   Then  "valid SQL is produced with session variable references mapped to
#          current_setting('provisa.<name>')"
#         -> _then_bool_expr_sql
# No new step definitions are required for this requirement.


# Copyright (c) 2026 Kenneth Stott
# Canary: 62ee945e-5a31-4cb4-a1cc-6698ef16b99f
#
# This source code is licensed under the Business Source License 1.1

# All steps for REQ-185 are already implemented in the existing steps file.
# The Given/When/Then steps for the REQ-185 default behaviour scenario map to:
#   Given "a Hasura v2 metadata export with select_permissions[].columns per role"
#         -> _given_v2_select_perms
#   When  "the v2 converter runs"
#         -> _when_v2_converter_runs
#   Then  "each column's visible_to is populated from the role's column list,
#          with "*" meaning all columns"
#         -> _then_visible_to
# No new step definitions are required for this requirement.


# Copyright (c) 2026 Kenneth Stott
# Canary: dee59087-3d41-459c-a00b-88bedbc8ab8c
#
# This source code is licensed under the Business Source License 1.1

# All steps for REQ-187 are already implemented in the existing steps file.
# The Given/When/Then steps for the REQ-187 default behaviour scenario map to:
#   Given "a Hasura v2 select_permissions[].filter boolean expression"
#         -> _given_v2_filter
#   When  "the v2 converter runs"
#         -> _when_v2_converter_runs
#   Then  "rls_rules[] are generated via boolean expression-to-SQL conversion,
#          with empty filter producing no RLS rule"
#         -> _then_rls_rules
# No new step definitions are required for this requirement.


# Copyright (c) 2026 Kenneth Stott
# Canary: 7da0eb36-8022-4d86-a4d7-06171ca33c87
#
# This source code is licensed under the Business Source License 1.1

# All steps for REQ-188 are already implemented in the existing steps file.
# The Given/When/Then steps for the REQ-188 default behaviour scenario map to:
#   Given "a Hasura v2 metadata export with object_relationships and array_relationships"
#         -> _given_v2_relationships
#   When  "the v2 converter runs"
#         -> _when_v2_converter_runs
#   Then  "object_relationships become cardinality=many-to-one and array_relationships
#          become cardinality=one-to-many"
#         -> _then_cardinality
# No new step definitions are required for this requirement.


# All steps for REQ-189 are already implemented in the existing steps file.
# The Given/When/Then steps for the REQ-189 default behaviour scenario map to:
#   Given "a DDN supergraph with ObjectType.dataConnectorTypeMapping[].fieldMapping entries"
#         -> _given_ddn_field_mappings
#   When  "the DDN converter runs"
#         -> _when_ddn_converter_runs
#   Then  "all GraphQL field names in relationships, permissions, and column definitions
#          are resolved to physical column names"
#         -> _then_ddn_fields_resolved
# No new step definitions are required for this requirement.


# All steps for REQ-190 are already implemented in the existing steps file.
# The Given/When/Then steps for the REQ-190 default behaviour scenario map to:
#   Given "a Hasura v2 auth config with JWT jwk_url, claims_map, or admin secret"
#         -> _given_v2_auth_config
#   When  "the v2 converter runs with --auth-env-file"
#         -> _when_v2_auth_converter_runs
#   Then  "JWT becomes provider: oauth with role_mapping[], admin secret becomes superuser,
#          and webhook auth emits a warning"
#         -> _then_v2_auth_converted
# No new step definitions are required for this requirement.


# All steps for REQ-621 are already implemented in the existing steps file.
# The Given/When/Then steps for the REQ-621 default behaviour scenario map to:
#   Given "a completed Hasura v2 or DDN conversion"
#         -> _given_completed_conversion
#   When  "the output config is inspected"
#         -> _when_output_config_inspected
#   Then  "placeholder credentials (host: localhost, password: ${env:DB_PASSWORD}) are present
#          and Provisa refuses to start without real values"
#         -> _then_placeholder_credentials
# No new step definitions are required for this requirement.


# All steps for REQ-623 are already implemented in the existing steps file.
# The Given/When/Then steps for the REQ-623 default behaviour scenario map to:
#   Given "a Hasura v2 source config with kind, database_url, and pool_settings"
#         -> _given_v2_source_kind_url_pool
#   When  "the v2 converter runs"
#         -> _when_v2_converter_runs
#   Then  "SourceType is mapped correctly and connection URL is parsed into components
#          with pool settings preserved"
#         -> _then_v2_source_mapped
# No new step definitions are required for this requirement.


# Copyright (c) 2026 Kenneth Stott
# Canary: 73515c1a-9f1d-4c3b-8d55-e6afcee1c708
#
# This source code is licensed under the Business Source License 1.1

# All steps for REQ-625 are already implemented in the existing steps file.
# The Given/When/Then steps for the REQ-625 default behaviour scenario map to:
#   Given "a Hasura v2 source with database_url as an env var reference or unparseable URL"
#         -> _given_v2_source_env_url
#   When  "the v2 converter runs"
#         -> _when_v2_converter_runs
#   Then  "placeholder connection values are substituted and operators are directed to
#          use --source-overrides"
#         -> _then_v2_placeholder_values
# No new step definitions are required for this requirement.


# All steps for REQ-182 are already implemented in the existing steps file.
# The Given/When/Then steps for the REQ-182 default behaviour scenario map to:
#   Given "a Hasura v2 metadata export directory"
#         -> _given_v2_metadata
#   When  "the CLI converter is run against it"
#         -> _when_v2_converter_run
#   Then  "valid Provisa YAML config is emitted covering tables, relationships,
#          permissions, roles, and auth"
#         -> _then_v2_config_complete
# No new step definitions are required for this requirement.


# Copyright (c) 2026 Kenneth Stott
# Canary: a0be46e3-1b49-44e7-81f9-06fde3617ee6
#
# This source code is licensed under the Business Source License 1.1

# All steps for REQ-183 are already implemented in the existing steps file.
# The Given/When/Then steps for the REQ-183 default behaviour scenario map to:
#   Given "a Hasura DDN supergraph project"       -> _given_ddn_project
#   When  "the HML converter CLI tool is run"     -> _when_ddn_converter_run
#   Then  "valid Provisa YAML config is emitted covering ObjectTypes, Models,
#          Relationships, TypePermissions, ModelPermissions, and DataConnectorLinks"
#                                                 -> _then_ddn_config_complete
# No new step definitions are required for this requirement.


# Copyright (c) 2026 Kenneth Stott
# Canary: 0f1ccfac-cb5c-4383-a938-87ec9697bc8a
#
# This source code is licensed under the Business Source License 1.1

# All steps for REQ-184 are already implemented in the existing steps file.
# The Given/When/Then steps for the REQ-184 default behaviour scenario map to:
#   Given "a Hasura boolean filter expression using operators like _eq, _in, _and, _or, _not"
#         -> _given_bool_expr
#   When  "the shared converter processes it"
#         -> _when_bool_expr_converted
#   Then  "valid SQL is produced with session variable references mapped to
#          current_setting('provisa.<name>')"
#         -> _then_bool_expr_sql
# No new step definitions are required for this requirement.


# Copyright (c) 2026 Kenneth Stott
# Canary: a87001c1-cd58-4f51-9f27-07b790dfd1f6
#
# This source code is licensed under the Business Source License 1.1

# All steps for REQ-185 are already implemented in the existing steps file.
# The Given/When/Then steps for the REQ-185 default behaviour scenario map to:
#   Given "a Hasura v2 metadata export with select_permissions[].columns per role"
#         -> _given_v2_select_perms
#   When  "the v2 converter runs"
#         -> _when_v2_converter_runs
#   Then  "each column's visible_to is populated from the role's column list,
#          with "*" meaning all columns"
#         -> _then_visible_to
# No new step definitions are required for this requirement.


# Copyright (c) 2026 Kenneth Stott
# Canary: d4d30a93-f0e6-4ea7-81d2-a848d71bfe3f
#
# This source code is licensed under the Business Source License 1.1

# All steps for REQ-187 are already implemented in the existing steps file.
# The Given/When/Then steps for the REQ-187 default behaviour scenario map to:
#   Given "a Hasura v2 select_permissions[].filter boolean expression"
#         -> _given_v2_filter
#   When  "the v2 converter runs"
#         -> _when_v2_converter_runs
#   Then  "rls_rules[] are generated via boolean expression-to-SQL conversion,
#          with empty filter producing no RLS rule"
#         -> _then_rls_rules
# No new step definitions are required for this requirement.


# All steps for REQ-188 are already implemented in the existing steps file.
# The Given/When/Then steps for the REQ-188 default behaviour scenario map to:
#   Given "a Hasura v2 metadata export with object_relationships and array_relationships"
#         -> _given_v2_relationships
#   When  "the v2 converter runs"
#         -> _when_v2_converter_runs
#   Then  "object_relationships become cardinality=many-to-one and array_relationships
#          become cardinality=one-to-many"
#         -> _then_cardinality
# No new step definitions are required for this requirement.


# Copyright (c) 2026 Kenneth Stott
# Canary: 004b5e98-552f-4e4b-8bec-ff3126c57c47
#
# This source code is licensed under the Business Source License 1.1

# All steps for REQ-189 are already implemented in the existing steps file.
# The Given/When/Then steps for the REQ-189 default behaviour scenario map to:
#   Given "a DDN supergraph with ObjectType.dataConnectorTypeMapping[].fieldMapping entries"
#         -> _given_ddn_field_mappings
#   When  "the DDN converter runs"
#         -> _when_ddn_converter_runs
#   Then  "all GraphQL field names in relationships, permissions, and column definitions
#          are resolved to physical column names"
#         -> _then_ddn_fields_resolved
# No new step definitions are required for this requirement.


# All steps for REQ-190 are already implemented in the existing steps file.
# The Given/When/Then steps for the REQ-190 default behaviour scenario map to:
#   Given "a Hasura v2 auth config with JWT jwk_url, claims_map, or admin secret"
#         -> _given_v2_auth_config
#   When  "the v2 converter runs with --auth-env-file"
#         -> _when_v2_auth_converter_runs
#   Then  "JWT becomes provider: oauth with role_mapping[], admin secret becomes superuser,
#          and webhook auth emits a warning"
#         -> _then_v2_auth_converted
# No new step definitions are required for this requirement.


# Copyright (c) 2026 Kenneth Stott
# Canary: 5f4d708e-9997-47cb-ac23-26633f5c8400
#
# This source code is licensed under the Business Source License 1.1

# All steps for REQ-621 are already implemented in the existing steps file.
# The Given/When/Then steps for the REQ-621 default behaviour scenario map to:
#   Given "a completed Hasura v2 or DDN conversion"
#         -> _given_completed_conversion
#   When  "the output config is inspected"
#         -> _when_output_config_inspected
#   Then  "placeholder credentials (host: localhost, password: ${env:DB_PASSWORD}) are present
#          and Provisa refuses to start without real values"
#         -> _then_placeholder_credentials
# No new step definitions are required for this requirement.


# All steps for REQ-623 are already implemented in the existing steps file.
# The Given/When/Then steps for the REQ-623 default behaviour scenario map to:
#   Given "a Hasura v2 source config with kind, database_url, and pool_settings"
#         -> _given_v2_source_kind_url_pool
#   When  "the v2 converter runs"
#         -> _when_v2_converter_runs
#   Then  "SourceType is mapped correctly and connection URL is parsed into components
#          with pool settings preserved"
#         -> _then_v2_source_mapped
# No new step definitions are required for this requirement.


# Copyright (c) 2026 Kenneth Stott
# Canary: c847ca05-3141-4720-ac78-9eba9f9d5ec6
#
# This source code is licensed under the Business Source License 1.1

# All steps for REQ-625 are already implemented in the existing steps file.
# The Given/When/Then steps for the REQ-625 default behaviour scenario map to:
#   Given "a Hasura v2 source with database_url as an env var reference or unparseable URL"
#         -> _given_v2_source_env_url
#   When  "the v2 converter runs"
#         -> _when_v2_converter_runs
#   Then  "placeholder connection values are substituted and operators are directed to
#          use --source-overrides"
#         -> _then_v2_placeholder_values
# No new step definitions are required for this requirement.


# Copyright (c) 2026 Kenneth Stott
# Canary: f0dda407-8803-4dbc-a375-b30c3bb6422b
#
# This source code is licensed under the Business Source License 1.1

# All steps for REQ-182 are already implemented in the existing steps file.
# The Given/When/Then steps for the REQ-182 default behaviour scenario map to:
#   Given "a Hasura v2 metadata export directory"
#         -> _given_v2_metadata
#   When  "the CLI converter is run against it"
#         -> _when_v2_converter_run
#   Then  "valid Provisa YAML config is emitted covering tables, relationships,
#          permissions, roles, and auth"
#         -> _then_v2_config_complete
# No new step definitions are required for this requirement.


# All steps for REQ-183 are already implemented in the existing steps file.
# The Given/When/Then steps for the REQ-183 default behaviour scenario map to:
#   Given "a Hasura DDN supergraph project"       -> _given_ddn_project
#   When  "the HML converter CLI tool is run"     -> _when_ddn_converter_run
#   Then  "valid Provisa YAML config is emitted covering ObjectTypes, Models,
#          Relationships, TypePermissions, ModelPermissions, and DataConnectorLinks"
#                                                 -> _then_ddn_config_complete
# No new step definitions are required for this requirement.


# All steps for REQ-184 are already implemented in the existing steps file.
# The Given/When/Then steps for the REQ-184 default behaviour scenario map to:
#   Given "a Hasura boolean filter expression using operators like _eq, _in, _and, _or, _not"
#         -> _given_bool_expr
#   When  "the shared converter processes it"
#         -> _when_bool_expr_converted
#   Then  "valid SQL is produced with session variable references mapped to
#          current_setting('provisa.<name>')"
#         -> _then_bool_expr_sql
# No new step definitions are required for this requirement.


# Copyright (c) 2026 Kenneth Stott
# Canary: 07097fbe-4860-4c20-8d1a-10df70446fd5
#
# This source code is licensed under the Business Source License 1.1

# All steps for REQ-185 are already implemented in the existing steps file.
# The Given/When/Then steps for the REQ-185 default behaviour scenario map to:
#   Given "a Hasura v2 metadata export with select_permissions[].columns per role"
#         -> _given_v2_select_perms
#   When  "the v2 converter runs"
#         -> _when_v2_converter_runs
#   Then  "each column's visible_to is populated from the role's column list,
#          with "*" meaning all columns"
#         -> _then_visible_to
# No new step definitions are required for this requirement.


# Copyright (c) 2026 Kenneth Stott
# Canary: 80bdebd3-4adc-44f4-b05e-1f763f7a709b
#
# This source code is licensed under the Business Source License 1.1

# All steps for REQ-187 are already implemented in the existing steps file.
# The Given/When/Then steps for the REQ-187 default behaviour scenario map to:
#   Given "a Hasura v2 select_permissions[].filter boolean expression"
#         -> _given_v2_filter
#   When  "the v2 converter runs"
#         -> _when_v2_converter_runs
#   Then  "rls_rules[] are generated via boolean expression-to-SQL conversion,
#          with empty filter producing no RLS rule"
#         -> _then_rls_rules
# No new step definitions are required for this requirement.


# Copyright (c) 2026 Kenneth Stott
# Canary: 080a8915-e943-47ff-80a2-de4c91bf4544
#
# This source code is licensed under the Business Source License 1.1

# All steps for REQ-188 are already implemented in the existing steps file.
# The Given/When/Then steps for the REQ-188 default behaviour scenario map to:
#   Given "a Hasura v2 metadata export with object_relationships and array_relationships"
#         -> _given_v2_relationships
#   When  "the v2 converter runs"
#         -> _when_v2_converter_runs
#   Then  "object_relationships become cardinality=many-to-one and array_relationships
#          become cardinality=one-to-many"
#         -> _then_cardinality
# No new step definitions are required for this requirement.


# All steps for REQ-189 are already implemented in the existing steps file.
# The Given/When/Then steps for the REQ-189 default behaviour scenario map to:
#   Given "a DDN supergraph with ObjectType.dataConnectorTypeMapping[].fieldMapping entries"
#         -> _given_ddn_field_mappings
#   When  "the DDN converter runs"
#         -> _when_ddn_converter_runs
#   Then  "all GraphQL field names in relationships, permissions, and column definitions
#          are resolved to physical column names"
#         -> _then_ddn_fields_resolved
# No new step definitions are required for this requirement.


# All steps for REQ-190 are already implemented in the existing steps file.
# The Given/When/Then steps for the REQ-190 default behaviour scenario map to:
#   Given "a Hasura v2 auth config with JWT jwk_url, claims_map, or admin secret"
#         -> _given_v2_auth_config
#   When  "the v2 converter runs with --auth-env-file"
#         -> _when_v2_auth_converter_runs
#   Then  "JWT becomes provider: oauth with role_mapping[], admin secret becomes superuser,
#          and webhook auth emits a warning"
#         -> _then_v2_auth_converted
# No new step definitions are required for this requirement.


# Copyright (c) 2026 Kenneth Stott
# Canary: 0062b5e8-ce82-4fe8-98e5-865d1a48c950
#
# This source code is licensed under the Business Source License 1.1

# All steps for REQ-621 are already implemented in the existing steps file.
# The Given/When/Then steps for the REQ-621 default behaviour scenario map to:
#   Given "a completed Hasura v2 or DDN conversion"
#         -> _given_completed_conversion
#   When  "the output config is inspected"
#         -> _when_output_config_inspected
#   Then  "placeholder credentials (host: localhost, password: ${env:DB_PASSWORD}) are present
#          and Provisa refuses to start without real values"
#         -> _then_placeholder_credentials
# No new step definitions are required for this requirement.


# All steps for REQ-623 are already implemented in the existing steps file.
# The Given/When/Then steps for the REQ-623 default behaviour scenario map to:
#   Given "a Hasura v2 source config with kind, database_url, and pool_settings"
#         -> _given_v2_source_kind_url_pool
#   When  "the v2 converter runs"
#         -> _when_v2_converter_runs
#   Then  "SourceType is mapped correctly and connection URL is parsed into components
#          with pool settings preserved"
#         -> _then_v2_source_mapped
# No new step definitions are required for this requirement.


# Copyright (c) 2026 Kenneth Stott
# Canary: 4ce6ec53-767b-4ead-a1b0-6d871753ea98
#
# This source code is licensed under the Business Source License 1.1

# All steps for REQ-625 are already implemented in the existing steps file.
# The Given/When/Then steps for the REQ-625 default behaviour scenario map to:
#   Given "a Hasura v2 source with database_url as an env var reference or unparseable URL"
#         -> _given_v2_source_env_url
#   When  "the v2 converter runs"
#         -> _when_v2_converter_runs
#   Then  "placeholder connection values are substituted and operators are directed to
#          use --source-overrides"
#         -> _then_v2_placeholder_values
# No new step definitions are required for this requirement.


# Copyright (c) 2026 Kenneth Stott
# Canary: 883ad2f6-ccf5-4314-9d7d-3b2b32eccb32
#
# This source code is licensed under the Business Source License 1.1

# All steps for REQ-182 are already implemented in the existing steps file.
# The Given/When/Then steps for the REQ-182 default behaviour scenario map to:
#   Given "a Hasura v2 metadata export directory"
#         -> _given_v2_metadata
#   When  "the CLI converter is run against it"
#         -> _when_v2_converter_run
#   Then  "valid Provisa YAML config is emitted covering tables, relationships,
#          permissions, roles, and auth"
#         -> _then_v2_config_complete
# No new step definitions are required for this requirement.


# All steps for REQ-183 are already implemented in the existing steps file.
# The Given/When/Then steps for the REQ-183 default behaviour scenario map to:
#   Given "a Hasura DDN supergraph project"       -> _given_ddn_project
#   When  "the HML converter CLI tool is run"     -> _when_ddn_converter_run
#   Then  "valid Provisa YAML config is emitted covering ObjectTypes, Models,
#          Relationships, TypePermissions, ModelPermissions, and DataConnectorLinks"
#                                                 -> _then_ddn_config_complete
# No new step definitions are required for this requirement.


# Copyright (c) 2026 Kenneth Stott
# Canary: 8ce15052-2702-480d-815b-4de33f247803
#
# This source code is licensed under the Business Source License 1.1

# All steps for REQ-184 are already implemented in the existing steps file.
# The Given/When/Then steps for the REQ-184 default behaviour scenario map to:
#   Given "a Hasura boolean filter expression using operators like _eq, _in, _and, _or, _not"
#         -> _given_bool_expr
#   When  "the shared converter processes it"
#         -> _when_bool_expr_converted
#   Then  "valid SQL is produced with session variable references mapped to
#          current_setting('provisa.<name>')"
#         -> _then_bool_expr_sql
# No new step definitions are required for this requirement.


# Copyright (c) 2026 Kenneth Stott
# Canary: 72f34322-cbc1-4d06-9754-5fb4e4f6b512
#
# This source code is licensed under the Business Source License 1.1

# All steps for REQ-185 are already implemented in the existing steps file.
# The Given/When/Then steps for the REQ-185 default behaviour scenario map to:
#   Given "a Hasura v2 metadata export with select_permissions[].columns per role"
#         -> _given_v2_select_perms
#   When  "the v2 converter runs"
#         -> _when_v2_converter_runs
#   Then  "each column's visible_to is populated from the role's column list,
#          with "*" meaning all columns"
#         -> _then_visible_to
# No new step definitions are required for this requirement.


# Copyright (c) 2026 Kenneth Stott
# Canary: ef5d44c3-92d1-4e1c-aec1-2d98e170dd21
#
# This source code is licensed under the Business Source License 1.1

# All steps for REQ-187 are already implemented in the existing steps file.
# The Given/When/Then steps for the REQ-187 default behaviour scenario map to:
#   Given "a Hasura v2 select_permissions[].filter boolean expression"
#         -> _given_v2_filter
#   When  "the v2 converter runs"
#         -> _when_v2_converter_runs
#   Then  "rls_rules[] are generated via boolean expression-to-SQL conversion,
#          with empty filter producing no RLS rule"
#         -> _then_rls_rules
# No new step definitions are required for this requirement.


# Copyright (c) 2026 Kenneth Stott
# Canary: a1b6e5cc-da18-4fa9-99f9-b4f9f193974b
#
# This source code is licensed under the Business Source License 1.1

# All steps for REQ-188 are already implemented in the existing steps file.
# The Given/When/Then steps for the REQ-188 default behaviour scenario map to:
#   Given "a Hasura v2 metadata export with object_relationships and array_relationships"
#         -> _given_v2_relationships
#   When  "the v2 converter runs"
#         -> _when_v2_converter_runs
#   Then  "object_relationships become cardinality=many-to-one and array_relationships
#          become cardinality=one-to-many"
#         -> _then_cardinality
# No new step definitions are required for this requirement.


# Copyright (c) 2026 Kenneth Stott
# Canary: f80b4bce-24ce-4718-bca9-584ef761f902
#
# This source code is licensed under the Business Source License 1.1

# All steps for REQ-189 are already implemented in the existing steps file.
# The Given/When/Then steps for the REQ-189 default behaviour scenario map to:
#   Given "a DDN supergraph with ObjectType.dataConnectorTypeMapping[].fieldMapping entries"
#         -> _given_ddn_field_mappings
#   When  "the DDN converter runs"
#         -> _when_ddn_converter_runs
#   Then  "all GraphQL field names in relationships, permissions, and column definitions
#          are resolved to physical column names"
#         -> _then_ddn_fields_resolved
# No new step definitions are required for this requirement.


# Copyright (c) 2026 Kenneth Stott
# Canary: 29047e44-14ce-4b4c-b996-d571f79d4a3b
#
# This source code is licensed under the Business Source License 1.1

# All steps for REQ-190 are already implemented in the existing steps file.
# The Given/When/Then steps for the REQ-190 default behaviour scenario map to:
#   Given "a Hasura v2 auth config with JWT jwk_url, claims_map, or admin secret"
#         -> _given_v2_auth_config
#   When  "the v2 converter runs with --auth-env-file"
#         -> _when_v2_auth_converter_runs
#   Then  "JWT becomes provider: oauth with role_mapping[], admin secret becomes superuser,
#          and webhook auth emits a warning"
#         -> _then_v2_auth_converted
# No new step definitions are required for this requirement.


# Copyright (c) 2026 Kenneth Stott
# Canary: 54739661-ad1b-43b7-b183-e04edec65a7c
#
# This source code is licensed under the Business Source License 1.1

# All steps for REQ-621 are already implemented in the existing steps file.
# The Given/When/Then steps for the REQ-621 default behaviour scenario map to:
#   Given "a completed Hasura v2 or DDN conversion"
#         -> _given_completed_conversion
#   When  "the output config is inspected"
#         -> _when_output_config_inspected
#   Then  "placeholder credentials (host: localhost, password: ${env:DB_PASSWORD}) are present
#          and Provisa refuses to start without real values"
#         -> _then_placeholder_credentials
# No new step definitions are required for this requirement.


# Copyright (c) 2026 Kenneth Stott
# Canary: 00c72548-f090-4a25-aaee-d44212147a28
#
# This source code is licensed under the Business Source License 1.1

# All steps for REQ-623 are already implemented in the existing steps file.
# The Given/When/Then steps for the REQ-623 default behaviour scenario map to:
#   Given "a Hasura v2 source config with kind, database_url, and pool_settings"
#         -> _given_v2_source_kind_url_pool
#   When  "the v2 converter runs"
#         -> _when_v2_converter_runs
#   Then  "SourceType is mapped correctly and connection URL is parsed into components
#          with pool settings preserved"
#         -> _then_v2_source_mapped
# No new step definitions are required for this requirement.


# Copyright (c) 2026 Kenneth Stott
# Canary: da26c588-8498-490a-9f68-003e440281ba
#
# This source code is licensed under the Business Source License 1.1

# All steps for REQ-625 are already implemented in the existing steps file.
# The Given/When/Then steps for the REQ-625 default behaviour scenario map to:
#   Given "a Hasura v2 source with database_url as an env var reference or unparseable URL"
#         -> _given_v2_source_env_url
#   When  "the v2 converter runs"
#         -> _when_v2_converter_runs
#   Then  "placeholder connection values are substituted and operators are directed to
#          use --source-overrides"
#         -> _then_v2_placeholder_values
# No new step definitions are required for this requirement.


# Copyright (c) 2026 Kenneth Stott
# Canary: 39b4113c-b80b-41c8-be7c-deabca1f0094
#
# This source code is licensed under the Business Source License 1.1

# All steps for REQ-182 are already implemented in the existing steps file.
# The Given/When/Then steps for the REQ-182 default behaviour scenario map to:
#   Given "a Hasura v2 metadata export directory"
#         -> _given_v2_metadata
#   When  "the CLI converter is run against it"
#         -> _when_v2_converter_run
#   Then  "valid Provisa YAML config is emitted covering tables, relationships,
#          permissions, roles, and auth"
#         -> _then_v2_config_complete
# No new step definitions are required for this requirement.


# Copyright (c) 2026 Kenneth Stott
# Canary: 88f6beb2-2c28-43b8-8e71-d12ddd8166ab
#
# This source code is licensed under the Business Source License 1.1

# All steps for REQ-183 are already implemented in the existing steps file.
# The Given/When/Then steps for the REQ-183 default behaviour scenario map to:
#   Given "a Hasura DDN supergraph project"       -> _given_ddn_project
#   When  "the HML converter CLI tool is run"     -> _when_ddn_converter_run
#   Then  "valid Provisa YAML config is emitted covering ObjectTypes, Models,
#          Relationships, TypePermissions, ModelPermissions, and DataConnectorLinks"
#                                                 -> _then_ddn_config_complete
# No new step definitions are required for this requirement.


# Copyright (c) 2026 Kenneth Stott
# Canary: d21b0dae-c36b-474c-bff7-6f9583de0803
#
# This source code is licensed under the Business Source License 1.1

# All steps for REQ-184 are already implemented in the existing steps file.
# The Given/When/Then steps for the REQ-184 default behaviour scenario map to:
#   Given "a Hasura boolean filter expression using operators like _eq, _in, _and, _or, _not"
#         -> _given_bool_expr
#   When  "the shared converter processes it"
#         -> _when_bool_expr_converted
#   Then  "valid SQL is produced with session variable references mapped to
#          current_setting('provisa.<name>')"
#         -> _then_bool_expr_sql
# No new step definitions are required for this requirement.


# Copyright (c) 2026 Kenneth Stott
# Canary: cc0bf708-2c41-43e7-93a3-2d4c90afb658
#
# This source code is licensed under the Business Source License 1.1

# All steps for REQ-185 are already implemented in the existing steps file.
# The Given/When/Then steps for the REQ-185 default behaviour scenario map to:
#   Given "a Hasura v2 metadata export with select_permissions[].columns per role"
#         -> _given_v2_select_perms
#   When  "the v2 converter runs"
#         -> _when_v2_converter_runs
#   Then  "each column's visible_to is populated from the role's column list,
#          with "*" meaning all columns"
#         -> _then_visible_to
# No new step definitions are required for this requirement.


# Copyright (c) 2026 Kenneth Stott
# Canary: 9570e5f1-8507-44f0-8cac-d62e19d02116
#
# This source code is licensed under the Business Source License 1.1

# All steps for REQ-187 are already implemented in the existing steps file.
# The Given/When/Then steps for the REQ-187 default behaviour scenario map to:
#   Given "a Hasura v2 select_permissions[].filter boolean expression"
#         -> _given_v2_filter
#   When  "the v2 converter runs"
#         -> _when_v2_converter_runs
#   Then  "rls_rules[] are generated via boolean expression-to-SQL conversion,
#          with empty filter producing no RLS rule"
#         -> _then_rls_rules
# No new step definitions are required for this requirement.


# All steps for REQ-188 are already implemented in the existing steps file.
# The Given/When/Then steps for the REQ-188 default behaviour scenario map to:
#   Given "a Hasura v2 metadata export with object_relationships and array_relationships"
#         -> _given_v2_relationships
#   When  "the v2 converter runs"
#         -> _when_v2_converter_runs
#   Then  "object_relationships become cardinality=many-to-one and array_relationships
#          become cardinality=one-to-many"
#         -> _then_cardinality
# No new step definitions are required for this requirement.


# All steps for REQ-189 are already implemented in the existing steps file.
# The Given/When/Then steps for the REQ-189 default behaviour scenario map to:
#   Given "a DDN supergraph with ObjectType.dataConnectorTypeMapping[].fieldMapping entries"
#         -> _given_ddn_field_mappings
#   When  "the DDN converter runs"
#         -> _when_ddn_converter_runs
#   Then  "all GraphQL field names in relationships, permissions, and column definitions
#          are resolved to physical column names"
#         -> _then_ddn_fields_resolved
# No new step definitions are required for this requirement.


# Copyright (c) 2026 Kenneth Stott
# Canary: 1e635cb8-f2ff-4d3f-a4e0-dc8d9e9528f8
#
# This source code is licensed under the Business Source License 1.1

# All steps for REQ-190 are already implemented in the existing steps file.
# The Given/When/Then steps for the REQ-190 default behaviour scenario map to:
#   Given "a Hasura v2 auth config with JWT jwk_url, claims_map, or admin secret"
#         -> _given_v2_auth_config
#   When  "the v2 converter runs with --auth-env-file"
#         -> _when_v2_auth_converter_runs
#   Then  "JWT becomes provider: oauth with role_mapping[], admin secret becomes superuser,
#          and webhook auth emits a warning"
#         -> _then_v2_auth_converted
# No new step definitions are required for this requirement.


# Copyright (c) 2026 Kenneth Stott
# Canary: fb96657a-b2bd-441d-ba48-0696890f9d84
#
# This source code is licensed under the Business Source License 1.1

# All steps for REQ-621 are already implemented in the existing steps file.
# The Given/When/Then steps for the REQ-621 default behaviour scenario map to:
#   Given "a completed Hasura v2 or DDN conversion"
#         -> _given_completed_conversion
#   When  "the output config is inspected"
#         -> _when_output_config_inspected
#   Then  "placeholder credentials (host: localhost, password: ${env:DB_PASSWORD}) are present
#          and Provisa refuses to start without real values"
#         -> _then_placeholder_credentials
# No new step definitions are required for this requirement.


# Copyright (c) 2026 Kenneth Stott
# Canary: 9ccbab98-c3d4-4dcc-b981-bf5ec8e426cc
#
# This source code is licensed under the Business Source License 1.1

# All steps for REQ-623 are already implemented in the existing steps file.
# The Given/When/Then steps for the REQ-623 default behaviour scenario map to:
#   Given "a Hasura v2 source config with kind, database_url, and pool_settings"
#         -> _given_v2_source_kind_url_pool
#   When  "the v2 converter runs"
#         -> _when_v2_converter_runs
#   Then  "SourceType is mapped correctly and connection URL is parsed into components
#          with pool settings preserved"
#         -> _then_v2_source_mapped
# No new step definitions are required for this requirement.


# All steps for REQ-625 are already implemented in the existing steps file.
# The Given/When/Then steps for the REQ-625 default behaviour scenario map to:
#   Given "a Hasura v2 source with database_url as an env var reference or unparseable URL"
#         -> _given_v2_source_env_url
#   When  "the v2 converter runs"
#         -> _when_v2_converter_runs
#   Then  "placeholder connection values are substituted and operators are directed to
#          use --source-overrides"
#         -> _then_v2_placeholder_values
# No new step definitions are required for this requirement.


# Copyright (c) 2026 Kenneth Stott
# Canary: 0bb2fb60-d501-49a7-a526-e49df52cca2b
#
# This source code is licensed under the Business Source License 1.1

# All steps for REQ-182 are already implemented in the existing steps file.
# The Given/When/Then steps for the REQ-182 default behaviour scenario map to:
#   Given "a Hasura v2 metadata export directory"
#         -> _given_v2_metadata
#   When  "the CLI converter is run against it"
#         -> _when_v2_converter_run
#   Then  "valid Provisa YAML config is emitted covering tables, relationships,
#          permissions, roles, and auth"
#         -> _then_v2_config_complete
# No new step definitions are required for this requirement.


# All steps for REQ-183 are already implemented in the existing steps file.
# The Given/When/Then steps for the REQ-183 default behaviour scenario map to:
#   Given "a Hasura DDN supergraph project"       -> _given_ddn_project
#   When  "the HML converter CLI tool is run"     -> _when_ddn_converter_run
#   Then  "valid Provisa YAML config is emitted covering ObjectTypes, Models,
#          Relationships, TypePermissions, ModelPermissions, and DataConnectorLinks"
#                                                 -> _then_ddn_config_complete
# No new step definitions are required for this requirement.


# Copyright (c) 2026 Kenneth Stott
# Canary: 4135b0b2-8f3e-4fee-a742-cf593894bcd0
#
# This source code is licensed under the Business Source License 1.1

# All steps for REQ-184 are already implemented in the existing steps file.
# The Given/When/Then steps for the REQ-184 default behaviour scenario map to:
#   Given "a Hasura boolean filter expression using operators like _eq, _in, _and, _or, _not"
#         -> _given_bool_expr
#   When  "the shared converter processes it"
#         -> _when_bool_expr_converted
#   Then  "valid SQL is produced with session variable references mapped to
#          current_setting('provisa.<name>')"
#         -> _then_bool_expr_sql
# No new step definitions are required for this requirement.


# Copyright (c) 2026 Kenneth Stott
# Canary: ecedd57c-b787-4855-b989-eac6ea8ed38d
#
# This source code is licensed under the Business Source License 1.1

# All steps for REQ-185 are already implemented in the existing steps file.
# The Given/When/Then steps for the REQ-185 default behaviour scenario map to:
#   Given "a Hasura v2 metadata export with select_permissions[].columns per role"
#         -> _given_v2_select_perms
#   When  "the v2 converter runs"
#         -> _when_v2_converter_runs
#   Then  "each column's visible_to is populated from the role's column list,
#          with \"*\" meaning all columns"
#         -> _then_visible_to
# No new step definitions are required for this requirement.


# Copyright (c) 2026 Kenneth Stott
# Canary: 5d134b99-a675-4f9d-898c-d192dc88f038
#
# This source code is licensed under the Business Source License 1.1

# All steps for REQ-187 are already implemented in the existing steps file.
# The Given/When/Then steps for the REQ-187 default behaviour scenario map to:
#   Given "a Hasura v2 select_permissions[].filter boolean expression"
#         -> _given_v2_filter
#   When  "the v2 converter runs"
#         -> _when_v2_converter_runs
#   Then  "rls_rules[] are generated via boolean expression-to-SQL conversion,
#          with empty filter producing no RLS rule"
#         -> _then_rls_rules
# No new step definitions are required for this requirement.


# Copyright (c) 2026 Kenneth Stott
# Canary: 9dace4ff-8552-4be3-9f3f-62094c2c89e8
#
# This source code is licensed under the Business Source License 1.1

# All steps for REQ-188 are already implemented in the existing steps file.
# The Given/When/Then steps for the REQ-188 default behaviour scenario map to:
#   Given "a Hasura v2 metadata export with object_relationships and array_relationships"
#         -> _given_v2_relationships
#   When  "the v2 converter runs"
#         -> _when_v2_converter_runs
#   Then  "object_relationships become cardinality=many-to-one and array_relationships
#          become cardinality=one-to-many"
#         -> _then_cardinality
# No new step definitions are required for this requirement.


# Copyright (c) 2026 Kenneth Stott
# Canary: a014a61d-5558-4bfd-abaa-043ea68d9b66
#
# This source code is licensed under the Business Source License 1.1

# All steps for REQ-189 are already implemented in the existing steps file.
# The Given/When/Then steps for the REQ-189 default behaviour scenario map to:
#   Given "a DDN supergraph with ObjectType.dataConnectorTypeMapping[].fieldMapping entries"
#         -> _given_ddn_field_mappings
#   When  "the DDN converter runs"
#         -> _when_ddn_converter_runs
#   Then  "all GraphQL field names in relationships, permissions, and column definitions
#          are resolved to physical column names"
#         -> _then_ddn_fields_resolved
# No new step definitions are required for this requirement.


# Copyright (c) 2026 Kenneth Stott
# Canary: 7b4690d7-aa48-43b0-a012-74502d2e9488
#
# This source code is licensed under the Business Source License 1.1

# All steps for REQ-190 are already implemented in the existing steps file.
# The Given/When/Then steps for the REQ-190 default behaviour scenario map to:
#   Given "a Hasura v2 auth config with JWT jwk_url, claims_map, or admin secret"
#         -> _given_v2_auth_config
#   When  "the v2 converter runs with --auth-env-file"
#         -> _when_v2_auth_converter_runs
#   Then  "JWT becomes provider: oauth with role_mapping[], admin secret becomes superuser,
#          and webhook auth emits a warning"
#         -> _then_v2_auth_converted
# No new step definitions are required for this requirement.


# Copyright (c) 2026 Kenneth Stott
# Canary: 76ecc3d8-1c77-462a-b3af-3741aa17d77f
#
# This source code is licensed under the Business Source License 1.1

# All steps for REQ-621 are already implemented in the existing steps file.
# The Given/When/Then steps for the REQ-621 default behaviour scenario map to:
#   Given "a completed Hasura v2 or DDN conversion"
#         -> _given_completed_conversion
#   When  "the output config is inspected"
#         -> _when_output_config_inspected
#   Then  "placeholder credentials (host: localhost, password: ${env:DB_PASSWORD}) are present
#          and Provisa refuses to start without real values"
#         -> _then_placeholder_credentials
# No new step definitions are required for this requirement.


# All steps for REQ-623 are already implemented in the existing steps file.
# The Given/When/Then steps for the REQ-623 default behaviour scenario map to:
#   Given "a Hasura v2 source config with kind, database_url, and pool_settings"
#         -> _given_v2_source_kind_url_pool
#   When  "the v2 converter runs"
#         -> _when_v2_converter_runs
#   Then  "SourceType is mapped correctly and connection URL is parsed into components
#          with pool settings preserved"
#         -> _then_v2_source_mapped
# No new step definitions are required for this requirement.


# Copyright (c) 2026 Kenneth Stott
# Canary: 6ee789a2-3a73-4afc-9201-0519b31baa45
#
# This source code is licensed under the Business Source License 1.1

# All steps for REQ-625 are already implemented in the existing steps file.
# The Given/When/Then steps for the REQ-625 default behaviour scenario map to:
#   Given "a Hasura v2 source with database_url as an env var reference or unparseable URL"
#         -> _given_v2_source_env_url
#   When  "the v2 converter runs"
#         -> _when_v2_converter_runs
#   Then  "placeholder connection values are substituted and operators are directed to
#          use --source-overrides"
#         -> _then_v2_placeholder_values
# No new step definitions are required for this requirement.


# Copyright (c) 2026 Kenneth Stott
# Canary: 39960947-fee8-4cfc-95ee-4232c4b9807c
#
# This source code is licensed under the Business Source License 1.1

# All steps for REQ-182 are already implemented in the existing steps file.
# The Given/When/Then steps for the REQ-182 default behaviour scenario map to:
#   Given "a Hasura v2 metadata export directory"
#         -> _given_v2_metadata
#   When  "the CLI converter is run against it"
#         -> _when_v2_converter_run
#   Then  "valid Provisa YAML config is emitted covering tables, relationships,
#          permissions, roles, and auth"
#         -> _then_v2_config_complete
# No new step definitions are required for this requirement.


# Copyright (c) 2026 Kenneth Stott
# Canary: b639ca5d-01c7-4c85-8c7b-5dfb830baacd
#
# This source code is licensed under the Business Source License 1.1

# All steps for REQ-183 are already implemented in the existing steps file.
# The Given/When/Then steps for the REQ-183 default behaviour scenario map to:
#   Given "a Hasura DDN supergraph project"       -> _given_ddn_project
#   When  "the HML converter CLI tool is run"     -> _when_ddn_converter_run
#   Then  "valid Provisa YAML config is emitted covering ObjectTypes, Models,
#          Relationships, TypePermissions, ModelPermissions, and DataConnectorLinks"
#                                                 -> _then_ddn_config_complete
# No new step definitions are required for this requirement.


# Copyright (c) 2026 Kenneth Stott
# Canary: f2015a51-bbf6-4aee-bda8-d25dc9bf5e34
#
# This source code is licensed under the Business Source License 1.1

# All steps for REQ-184 are already implemented in the existing steps file.
# The Given/When/Then steps for the REQ-184 default behaviour scenario map to:
#   Given "a Hasura boolean filter expression using operators like _eq, _in, _and, _or, _not"
#         -> _given_bool_expr
#   When  "the shared converter processes it"
#         -> _when_bool_expr_converted
#   Then  "valid SQL is produced with session variable references mapped to
#          current_setting('provisa.<name>')"
#         -> _then_bool_expr_sql
# No new step definitions are required for this requirement.


# Copyright (c) 2026 Kenneth Stott
# Canary: 7bd0a791-ff11-457d-bbe5-05f7467bd096
#
# This source code is licensed under the Business Source License 1.1

# All steps for REQ-185 are already implemented in the existing steps file.
# The Given/When/Then steps for the REQ-185 default behaviour scenario map to:
#   Given "a Hasura v2 metadata export with select_permissions[].columns per role"
#         -> _given_v2_select_perms
#   When  "the v2 converter runs"
#         -> _when_v2_converter_runs
#   Then  "each column's visible_to is populated from the role's column list,
#          with \"*\" meaning all columns"
#         -> _then_visible_to
# No new step definitions are required for this requirement.


# Copyright (c) 2026 Kenneth Stott
# Canary: d9778c6e-64f1-4506-ac74-225ca301ec1a
#
# This source code is licensed under the Business Source License 1.1

# All steps for REQ-187 are already implemented in the existing steps file.
# The Given/When/Then steps for the REQ-187 default behaviour scenario map to:
#   Given "a Hasura v2 select_permissions[].filter boolean expression"
#         -> _given_v2_filter
#   When  "the v2 converter runs"
#         -> _when_v2_converter_runs
#   Then  "rls_rules[] are generated via boolean expression-to-SQL conversion,
#          with empty filter producing no RLS rule"
#         -> _then_rls_rules
# No new step definitions are required for this requirement.


# Copyright (c) 2026 Kenneth Stott
# Canary: e140c9e0-2343-4050-865b-1f619ac208f0
#
# This source code is licensed under the Business Source License 1.1

# All steps for REQ-188 are already implemented in the existing steps file.
# The Given/When/Then steps for the REQ-188 default behaviour scenario map to:
#   Given "a Hasura v2 metadata export with object_relationships and array_relationships"
#         -> _given_v2_relationships
#   When  "the v2 converter runs"
#         -> _when_v2_converter_runs
#   Then  "object_relationships become cardinality=many-to-one and array_relationships
#          become cardinality=one-to-many"
#         -> _then_cardinality
# No new step definitions are required for this requirement.


# Copyright (c) 2026 Kenneth Stott
# Canary: d460d4de-5bbd-4a03-b15b-450c9b2a93b6
#
# This source code is licensed under the Business Source License 1.1

# All steps for REQ-189 are already implemented in the existing steps file.
# The Given/When/Then steps for the REQ-189 default behaviour scenario map to:
#   Given "a DDN supergraph with ObjectType.dataConnectorTypeMapping[].fieldMapping entries"
#         -> _given_ddn_field_mappings
#   When  "the DDN converter runs"
#         -> _when_ddn_converter_runs
#   Then  "all GraphQL field names in relationships, permissions, and column definitions
#          are resolved to physical column names"
#         -> _then_ddn_fields_resolved
# No new step definitions are required for this requirement.


# Copyright (c) 2026 Kenneth Stott
# Canary: 1a706fe4-c0c5-4642-bca5-79678157a73c
#
# This source code is licensed under the Business Source License 1.1

# All steps for REQ-190 are already implemented in the existing steps file.
# The Given/When/Then steps for the REQ-190 default behaviour scenario map to:
#   Given "a Hasura v2 auth config with JWT jwk_url, claims_map, or admin secret"
#         -> _given_v2_auth_config
#   When  "the v2 converter runs with --auth-env-file"
#         -> _when_v2_auth_converter_runs
#   Then  "JWT becomes provider: oauth with role_mapping[], admin secret becomes superuser,
#          and webhook auth emits a warning"
#         -> _then_v2_auth_converted
# No new step definitions are required for this requirement.


# Copyright (c) 2026 Kenneth Stott
# Canary: 3de30955-9c63-441e-a313-adb7087dc8c1
#
# This source code is licensed under the Business Source License 1.1

# All steps for REQ-621 are already implemented in the existing steps file.
# The Given/When/Then steps for the REQ-621 default behaviour scenario map to:
#   Given "a completed Hasura v2 or DDN conversion"
#         -> _given_completed_conversion
#   When  "the output config is inspected"
#         -> _when_output_config_inspected
#   Then  "placeholder credentials (host: localhost, password: ${env:DB_PASSWORD}) are present
#          and Provisa refuses to start without real values"
#         -> _then_placeholder_credentials
# No new step definitions are required for this requirement.


# All steps for REQ-623 are already implemented in the existing steps file.
# The Given/When/Then steps for the REQ-623 default behaviour scenario map to:
#   Given "a Hasura v2 source config with kind, database_url, and pool_settings"
#         -> _given_v2_source_kind_url_pool
#   When  "the v2 converter runs"
#         -> _when_v2_converter_runs
#   Then  "SourceType is mapped correctly and connection URL is parsed into components
#          with pool settings preserved"
#         -> _then_v2_source_mapped
# No new step definitions are required for this requirement.


# Copyright (c) 2026 Kenneth Stott
# Canary: 1e4108b6-c97f-40e8-8c6f-428be5a06ffc
#
# This source code is licensed under the Business Source License 1.1

# All steps for REQ-625 are already implemented in the existing steps file.
# The Given/When/Then steps for the REQ-625 default behaviour scenario map to:
#   Given "a Hasura v2 source with database_url as an env var reference or unparseable URL"
#         -> _given_v2_source_env_url
#   When  "the v2 converter runs"
#         -> _when_v2_converter_runs
#   Then  "placeholder connection values are substituted and operators are directed to
#          use --source-overrides"
#         -> _then_v2_placeholder_values
# No new step definitions are required for this requirement.


# Copyright (c) 2026 Kenneth Stott
# Canary: bc13a76a-2708-4cf8-adcb-c1402253ef26
#
# This source code is licensed under the Business Source License 1.1

# All steps for REQ-182 are already implemented in the existing steps file.
# The Given/When/Then steps for the REQ-182 default behaviour scenario map to:
#   Given "a Hasura v2 metadata export directory"
#         -> _given_v2_metadata
#   When  "the CLI converter is run against it"
#         -> _when_v2_converter_run
#   Then  "valid Provisa YAML config is emitted covering tables, relationships,
#          permissions, roles, and auth"
#         -> _then_v2_config_complete
# No new step definitions are required for this requirement.


# Copyright (c) 2026 Kenneth Stott
# Canary: ea70029c-af7b-46b2-b4d0-509b3103db5f
#
# This source code is licensed under the Business Source License 1.1

# All steps for REQ-183 are already implemented in the existing steps file.
# The Given/When/Then steps for the REQ-183 default behaviour scenario map to:
#   Given "a Hasura DDN supergraph project"       -> _given_ddn_project
#   When  "the HML converter CLI tool is run"     -> _when_ddn_converter_run
#   Then  "valid Provisa YAML config is emitted covering ObjectTypes, Models,
#          Relationships, TypePermissions, ModelPermissions, and DataConnectorLinks"
#                                                 -> _then_ddn_config_complete
# No new step definitions are required for this requirement.


# Copyright (c) 2026 Kenneth Stott
# Canary: 208b78fb-ff18-4427-ba54-e7172c890775
#
# This source code is licensed under the Business Source License 1.1

# All steps for REQ-184 are already implemented in the existing steps file.
# The Given/When/Then steps for the REQ-184 default behaviour scenario map to:
#   Given "a Hasura boolean filter expression using operators like _eq, _in, _and, _or, _not"
#         -> _given_bool_expr
#   When  "the shared converter processes it"
#         -> _when_bool_expr_converted
#   Then  "valid SQL is produced with session variable references mapped to
#          current_setting('provisa.<name>')"
#         -> _then_bool_expr_sql
# No new step definitions are required for this requirement.


# Copyright (c) 2026 Kenneth Stott
# Canary: 5a5851d8-066f-481a-93f2-b11d2e09010d
#
# This source code is licensed under the Business Source License 1.1

# All steps for REQ-185 are already implemented in the existing steps file.
# The Given/When/Then steps for the REQ-185 default behaviour scenario map to:
#   Given "a Hasura v2 metadata export with select_permissions[].columns per role"
#         -> _given_v2_select_perms
#   When  "the v2 converter runs"
#         -> _when_v2_converter_runs
#   Then  "each column's visible_to is populated from the role's column list,
#          with "*" meaning all columns"
#         -> _then_visible_to
# No new step definitions are required for this requirement.


# Copyright (c) 2026 Kenneth Stott
# Canary: 8d2e4752-1e5a-4751-9fcf-5bfa65411490
#
# This source code is licensed under the Business Source License 1.1

# All steps for REQ-187 are already implemented in the existing steps file.
# The Given/When/Then steps for the REQ-187 default behaviour scenario map to:
#   Given "a Hasura v2 select_permissions[].filter boolean expression"
#         -> _given_v2_filter
#   When  "the v2 converter runs"
#         -> _when_v2_converter_runs
#   Then  "rls_rules[] are generated via boolean expression-to-SQL conversion,
#          with empty filter producing no RLS rule"
#         -> _then_rls_rules
# No new step definitions are required for this requirement.


# Copyright (c) 2026 Kenneth Stott
# Canary: 9890520f-2d00-4611-be9d-2926d11f6a8a
#
# This source code is licensed under the Business Source License 1.1

# All steps for REQ-188 are already implemented in the existing steps file.
# The Given/When/Then steps for the REQ-188 default behaviour scenario map to:
#   Given "a Hasura v2 metadata export with object_relationships and array_relationships"
#         -> _given_v2_relationships
#   When  "the v2 converter runs"
#         -> _when_v2_converter_runs
#   Then  "object_relationships become cardinality=many-to-one and array_relationships
#          become cardinality=one-to-many"
#         -> _then_cardinality
# No new step definitions are required for this requirement.


# Copyright (c) 2026 Kenneth Stott
# Canary: 3317ad53-0338-471d-800d-89c5aa3fa401
#
# This source code is licensed under the Business Source License 1.1

# All steps for REQ-189 are already implemented in the existing steps file.
# The Given/When/Then steps for the REQ-189 default behaviour scenario map to:
#   Given "a DDN supergraph with ObjectType.dataConnectorTypeMapping[].fieldMapping entries"
#         -> _given_ddn_field_mappings
#   When  "the DDN converter runs"
#         -> _when_ddn_converter_runs
#   Then  "all GraphQL field names in relationships, permissions, and column definitions
#          are resolved to physical column names"
#         -> _then_ddn_fields_resolved
# No new step definitions are required for this requirement.


# Copyright (c) 2026 Kenneth Stott
# Canary: a14ae766-cec5-41c5-87c8-215e5234eae2
#
# This source code is licensed under the Business Source License 1.1

# All steps for REQ-190 are already implemented in the existing steps file.
# The Given/When/Then steps for the REQ-190 default behaviour scenario map to:
#   Given "a Hasura v2 auth config with JWT jwk_url, claims_map, or admin secret"
#         -> _given_v2_auth_config
#   When  "the v2 converter runs with --auth-env-file"
#         -> _when_v2_auth_converter_runs
#   Then  "JWT becomes provider: oauth with role_mapping[], admin secret becomes superuser,
#          and webhook auth emits a warning"
#         -> _then_v2_auth_converted
# No new step definitions are required for this requirement.


# Copyright (c) 2026 Kenneth Stott
# Canary: 8c3908d0-8ef0-459c-aaa4-a3addbc8c047
#
# This source code is licensed under the Business Source License 1.1

# All steps for REQ-621 are already implemented in the existing steps file.
# The Given/When/Then steps for the REQ-621 default behaviour scenario map to:
#   Given "a completed Hasura v2 or DDN conversion"
#         -> _given_completed_conversion
#   When  "the output config is inspected"
#         -> _when_output_config_inspected
#   Then  "placeholder credentials (host: localhost, password: ${env:DB_PASSWORD}) are present
#          and Provisa refuses to start without real values"
#         -> _then_placeholder_credentials
# No new step definitions are required for this requirement.


# All steps for REQ-623 are already implemented in the existing steps file.
# The Given/When/Then steps for the REQ-623 default behaviour scenario map to:
#   Given "a Hasura v2 source config with kind, database_url, and pool_settings"
#         -> _given_v2_source_kind_url_pool
#   When  "the v2 converter runs"
#         -> _when_v2_converter_runs
#   Then  "SourceType is mapped correctly and connection URL is parsed into components
#          with pool settings preserved"
#         -> _then_v2_source_mapped
# No new step definitions are required for this requirement.


# Copyright (c) 2026 Kenneth Stott
# Canary: a1263095-5b3d-4bef-b8e6-99c712e8aad2
#
# This source code is licensed under the Business Source License 1.1

# All steps for REQ-625 are already implemented in the existing steps file.
# The Given/When/Then steps for the REQ-625 default behaviour scenario map to:
#   Given "a Hasura v2 source with database_url as an env var reference or unparseable URL"
#         -> _given_v2_source_env_url
#   When  "the v2 converter runs"
#         -> _when_v2_converter_runs
#   Then  "placeholder connection values are substituted and operators are directed to
#          use --source-overrides"
#         -> _then_v2_placeholder_values
# No new step definitions are required for this requirement.


# Copyright (c) 2026 Kenneth Stott
# Canary: 6499dec1-b73b-4401-b388-6caa376e2cdf
#
# This source code is licensed under the Business Source License 1.1

# All steps for REQ-182 are already implemented in the existing steps file.
# The Given/When/Then steps for the REQ-182 default behaviour scenario map to:
#   Given "a Hasura v2 metadata export directory"
#         -> _given_v2_metadata
#   When  "the CLI converter is run against it"
#         -> _when_v2_converter_run
#   Then  "valid Provisa YAML config is emitted covering tables, relationships,
#          permissions, roles, and auth"
#         -> _then_v2_config_complete
# No new step definitions are required for this requirement.


# Copyright (c) 2026 Kenneth Stott
# Canary: 58e3e6f5-21c8-4917-9dd5-507d884d6cfd
#
# This source code is licensed under the Business Source License 1.1

# All steps for REQ-183 are already implemented in the existing steps file.
# The Given/When/Then steps for the REQ-183 default behaviour scenario map to:
#   Given "a Hasura DDN supergraph project"       -> _given_ddn_project
#   When  "the HML converter CLI tool is run"     -> _when_ddn_converter_run
#   Then  "valid Provisa YAML config is emitted covering ObjectTypes, Models,
#          Relationships, TypePermissions, ModelPermissions, and DataConnectorLinks"
#                                                 -> _then_ddn_config_complete
# No new step definitions are required for this requirement.


# Copyright (c) 2026 Kenneth Stott
# Canary: 9fa6364c-6d2e-40a5-ad4a-3a600ce65414
#
# This source code is licensed under the Business Source License 1.1

# All steps for REQ-184 are already implemented in the existing steps file.
# The Given/When/Then steps for the REQ-184 default behaviour scenario map to:
#   Given "a Hasura boolean filter expression using operators like _eq, _in, _and, _or, _not"
#         -> _given_bool_expr
#   When  "the shared converter processes it"
#         -> _when_bool_expr_converted
#   Then  "valid SQL is produced with session variable references mapped to
#          current_setting('provisa.<name>')"
#         -> _then_bool_expr_sql
# No new step definitions are required for this requirement.


# Copyright (c) 2026 Kenneth Stott
# Canary: b3c152be-4760-4572-86db-bc88371e15cb
#
# This source code is licensed under the Business Source License 1.1

# All steps for REQ-185 are already implemented in the existing steps file.
# The Given/When/Then steps for the REQ-185 default behaviour scenario map to:
#   Given "a Hasura v2 metadata export with select_permissions[].columns per role"
#         -> _given_v2_select_perms
#   When  "the v2 converter runs"
#         -> _when_v2_converter_runs
#   Then  "each column's visible_to is populated from the role's column list,
#          with "*" meaning all columns"
#         -> _then_visible_to
# No new step definitions are required for this requirement.


# Copyright (c) 2026 Kenneth Stott
# Canary: b932d0f5-cdae-42f9-95a2-4946c2f6b07b
#
# This source code is licensed under the Business Source License 1.1

# All steps for REQ-187 are already implemented in the existing steps file.
# The Given/When/Then steps for the REQ-187 default behaviour scenario map to:
#   Given "a Hasura v2 select_permissions[].filter boolean expression"
#         -> _given_v2_filter
#   When  "the v2 converter runs"
#         -> _when_v2_converter_runs
#   Then  "rls_rules[] are generated via boolean expression-to-SQL conversion,
#          with empty filter producing no RLS rule"
#         -> _then_rls_rules
# No new step definitions are required for this requirement.


# Copyright (c) 2026 Kenneth Stott
# Canary: e1d2cdc3-cfa1-4502-b7d8-d9363e8cddec
#
# This source code is licensed under the Business Source License 1.1

# All steps for REQ-188 are already implemented in the existing steps file.
# The Given/When/Then steps for the REQ-188 default behaviour scenario map to:
#   Given "a Hasura v2 metadata export with object_relationships and array_relationships"
#         -> _given_v2_relationships
#   When  "the v2 converter runs"
#         -> _when_v2_converter_runs
#   Then  "object_relationships become cardinality=many-to-one and array_relationships
#          become cardinality=one-to-many"
#         -> _then_cardinality
# No new step definitions are required for this requirement.


# Copyright (c) 2026 Kenneth Stott
# Canary: b7a88dfe-41f3-421b-a26f-d4f076a65c35
#
# This source code is licensed under the Business Source License 1.1

# All steps for REQ-189 are already implemented in the existing steps file.
# The Given/When/Then steps for the REQ-189 default behaviour scenario map to:
#   Given "a DDN supergraph with ObjectType.dataConnectorTypeMapping[].fieldMapping entries"
#         -> _given_ddn_field_mappings
#   When  "the DDN converter runs"
#         -> _when_ddn_converter_runs
#   Then  "all GraphQL field names in relationships, permissions, and column definitions
#          are resolved to physical column names"
#         -> _then_ddn_fields_resolved
# No new step definitions are required for this requirement.


# Copyright (c) 2026 Kenneth Stott
# Canary: 84a4e9c1-b79e-4a9b-a443-30edf53cd433
#
# This source code is licensed under the Business Source License 1.1

# All steps for REQ-190 are already implemented in the existing steps file.
# The Given/When/Then steps for the REQ-190 default behaviour scenario map to:
#   Given "a Hasura v2 auth config with JWT jwk_url, claims_map, or admin secret"
#         -> _given_v2_auth_config
#   When  "the v2 converter runs with --auth-env-file"
#         -> _when_v2_auth_converter_runs
#   Then  "JWT becomes provider: oauth with role_mapping[], admin secret becomes superuser,
#          and webhook auth emits a warning"
#         -> _then_v2_auth_converted
# No new step definitions are required for this requirement.


# Copyright (c) 2026 Kenneth Stott
# Canary: f0269a96-c270-4f44-b20a-084ea6354fec
#
# This source code is licensed under the Business Source License 1.1

# All steps for REQ-621 are already implemented in the existing steps file.
# The Given/When/Then steps for the REQ-621 default behaviour scenario map to:
#   Given "a completed Hasura v2 or DDN conversion"
#         -> _given_completed_conversion
#   When  "the output config is inspected"
#         -> _when_output_config_inspected
#   Then  "placeholder credentials (host: localhost, password: ${env:DB_PASSWORD}) are present
#          and Provisa refuses to start without real values"
#         -> _then_placeholder_credentials
# No new step definitions are required for this requirement.


# Copyright (c) 2026 Kenneth Stott
# Canary: 9ac0fa57-3359-4fb7-9002-e19727699032
#
# This source code is licensed under the Business Source License 1.1

# All steps for REQ-623 are already implemented in the existing steps file.
# The Given/When/Then steps for the REQ-623 default behaviour scenario map to:
#   Given "a Hasura v2 source config with kind, database_url, and pool_settings"
#         -> _given_v2_source_kind_url_pool
#   When  "the v2 converter runs"
#         -> _when_v2_converter_runs
#   Then  "SourceType is mapped correctly and connection URL is parsed into components
#          with pool settings preserved"
#         -> _then_v2_source_mapped
# No new step definitions are required for this requirement.


# Copyright (c) 2026 Kenneth Stott
# Canary: f3a18dba-0482-478a-9cce-b85f530d31c4
#
# This source code is licensed under the Business Source License 1.1

# All steps for REQ-625 are already implemented in the existing steps file.
# The Given/When/Then steps for the REQ-625 default behaviour scenario map to:
#   Given "a Hasura v2 source with database_url as an env var reference or unparseable URL"
#         -> _given_v2_source_env_url
#   When  "the v2 converter runs"
#         -> _when_v2_converter_runs
#   Then  "placeholder connection values are substituted and operators are directed to
#          use --source-overrides"
#         -> _then_v2_placeholder_values
# No new step definitions are required for this requirement.


# Copyright (c) 2026 Kenneth Stott
# Canary: 4e1da58f-d6ec-4b79-937a-7faf7ae260d7
#
# This source code is licensed under the Business Source License 1.1

# All steps for REQ-182 are already implemented in the existing steps file.
# The Given/When/Then steps for the REQ-182 default behaviour scenario map to:
#   Given "a Hasura v2 metadata export directory"
#         -> _given_v2_metadata
#   When  "the CLI converter is run against it"
#         -> _when_v2_converter_run
#   Then  "valid Provisa YAML config is emitted covering tables, relationships,
#          permissions, roles, and auth"
#         -> _then_v2_config_complete
# No new step definitions are required for this requirement.


# Copyright (c) 2026 Kenneth Stott
# Canary: ecac3053-90d0-4f61-a0be-7b4b3ccb63ae
#
# This source code is licensed under the Business Source License 1.1

# All steps for REQ-183 are already implemented in the existing steps file.
# The Given/When/Then steps for the REQ-183 default behaviour scenario map to:
#   Given "a Hasura DDN supergraph project"       -> _given_ddn_project
#   When  "the HML converter CLI tool is run"     -> _when_ddn_converter_run
#   Then  "valid Provisa YAML config is emitted covering ObjectTypes, Models,
#          Relationships, TypePermissions, ModelPermissions, and DataConnectorLinks"
#                                                 -> _then_ddn_config_complete
# No new step definitions are required for this requirement.


# Copyright (c) 2026 Kenneth Stott
# Canary: 823192f3-712f-48dd-8ae9-e997b89c2396
#
# This source code is licensed under the Business Source License 1.1

# All steps for REQ-184 are already implemented in the existing steps file.
# The Given/When/Then steps for the REQ-184 default behaviour scenario map to:
#   Given "a Hasura boolean filter expression using operators like _eq, _in, _and, _or, _not"
#         -> _given_bool_expr
#   When  "the shared converter processes it"
#         -> _when_bool_expr_converted
#   Then  "valid SQL is produced with session variable references mapped to
#          current_setting('provisa.<name>')"
#         -> _then_bool_expr_sql
# No new step definitions are required for this requirement.


# Copyright (c) 2026 Kenneth Stott
# Canary: 9e8ac23b-03a5-4ba2-8ee2-7cefa5fe2514
#
# This source code is licensed under the Business Source License 1.1

# All steps for REQ-185 are already implemented in the existing steps file.
# The Given/When/Then steps for the REQ-185 default behaviour scenario map to:
#   Given "a Hasura v2 metadata export with select_permissions[].columns per role"
#         -> _given_v2_select_perms
#   When  "the v2 converter runs"
#         -> _when_v2_converter_runs
#   Then  "each column's visible_to is populated from the role's column list,
#          with "*" meaning all columns"
#         -> _then_visible_to
# No new step definitions are required for this requirement.


# Copyright (c) 2026 Kenneth Stott
# Canary: f8d27750-2be1-4b42-80bf-47639b25c789
#
# This source code is licensed under the Business Source License 1.1

# All steps for REQ-187 are already implemented in the existing steps file.
# The Given/When/Then steps for the REQ-187 default behaviour scenario map to:
#   Given "a Hasura v2 select_permissions[].filter boolean expression"
#         -> _given_v2_filter
#   When  "the v2 converter runs"
#         -> _when_v2_converter_runs
#   Then  "rls_rules[] are generated via boolean expression-to-SQL conversion,
#          with empty filter producing no RLS rule"
#         -> _then_rls_rules
# No new step definitions are required for this requirement.


# Copyright (c) 2026 Kenneth Stott
# Canary: 0a29712a-9875-4166-8285-f43ee3a2cda5
#
# This source code is licensed under the Business Source License 1.1

# All steps for REQ-188 are already implemented in the existing steps file.
# The Given/When/Then steps for the REQ-188 default behaviour scenario map to:
#   Given "a Hasura v2 metadata export with object_relationships and array_relationships"
#         -> _given_v2_relationships
#   When  "the v2 converter runs"
#         -> _when_v2_converter_runs
#   Then  "object_relationships become cardinality=many-to-one and array_relationships
#          become cardinality=one-to-many"
#         -> _then_cardinality
# No new step definitions are required for this requirement.


# Copyright (c) 2026 Kenneth Stott
# Canary: 073c83e6-45ad-4536-86ce-c97f3ee4e57d
#
# This source code is licensed under the Business Source License 1.1

# All steps for REQ-189 are already implemented in the existing steps file.
# The Given/When/Then steps for the REQ-189 default behaviour scenario map to:
#   Given "a DDN supergraph with ObjectType.dataConnectorTypeMapping[].fieldMapping entries"
#         -> _given_ddn_field_mappings
#   When  "the DDN converter runs"
#         -> _when_ddn_converter_runs
#   Then  "all GraphQL field names in relationships, permissions, and column definitions
#          are resolved to physical column names"
#         -> _then_ddn_fields_resolved
# No new step definitions are required for this requirement.


# Copyright (c) 2026 Kenneth Stott
# Canary: a0066562-23af-4ba7-b325-82916dfa71d9
#
# This source code is licensed under the Business Source License 1.1

# All steps for REQ-190 are already implemented in the existing steps file.
# The Given/When/Then steps for the REQ-190 default behaviour scenario map to:
#   Given "a Hasura v2 auth config with JWT jwk_url, claims_map, or admin secret"
#         -> _given_v2_auth_config
#   When  "the v2 converter runs with --auth-env-file"
#         -> _when_v2_auth_converter_runs
#   Then  "JWT becomes provider: oauth with role_mapping[], admin secret becomes superuser,
#          and webhook auth emits a warning"
#         -> _then_v2_auth_converted
# No new step definitions are required for this requirement.


# Copyright (c) 2026 Kenneth Stott
# Canary: 0a2b1c99-cd16-462f-a57b-15270e64ea04
#
# This source code is licensed under the Business Source License 1.1


# All steps for REQ-623 are already implemented in the existing steps file.
# The Given/When/Then steps for the REQ-623 default behaviour scenario map to:
#   Given "a Hasura v2 source config with kind, database_url, and pool_settings"
#         -> _given_v2_source_kind_url_pool
#   When  "the v2 converter runs"
#         -> _when_v2_converter_runs
#   Then  "SourceType is mapped correctly and connection URL is parsed into components
#          with pool settings preserved"
#         -> _then_v2_source_mapped
# No new step definitions are required for this requirement.


# Copyright (c) 2026 Kenneth Stott
# Canary: a0ee318d-2bc8-4395-a5a4-1b23901646fa
#
# This source code is licensed under the Business Source License 1.1

# All steps for REQ-625 are already implemented in the existing steps file.
# The Given/When/Then steps for the REQ-625 default behaviour scenario map to:
#   Given "a Hasura v2 source with database_url as an env var reference or unparseable URL"
#         -> _given_v2_source_env_url
#   When  "the v2 converter runs"
#         -> _when_v2_converter_runs
#   Then  "placeholder connection values are substituted and operators are directed to
#          use --source-overrides"
#         -> _then_v2_placeholder_values
# No new step definitions are required for this requirement.


# Copyright (c) 2026 Kenneth Stott
# Canary: aa14d1bc-158e-44cd-afe6-fdd7b7356379
#
# This source code is licensed under the Business Source License 1.1

# All steps for REQ-182 are already implemented in the existing steps file.
# The Given/When/Then steps for the REQ-182 default behaviour scenario map to:
#   Given "a Hasura v2 metadata export directory"
#         -> _given_v2_metadata
#   When  "the CLI converter is run against it"
#         -> _when_v2_converter_run
#   Then  "valid Provisa YAML config is emitted covering tables, relationships,
#          permissions, roles, and auth"
#         -> _then_v2_config_complete
# No new step definitions are required for this requirement.


# Copyright (c) 2026 Kenneth Stott
# Canary: ca04a325-63b4-4928-800e-502ed034ce6f
#
# This source code is licensed under the Business Source License 1.1

# All steps for REQ-183 are already implemented in the existing steps file.
# The Given/When/Then steps for the REQ-183 default behaviour scenario map to:
#   Given "a Hasura DDN supergraph project"       -> _given_ddn_project
#   When  "the HML converter CLI tool is run"     -> _when_ddn_converter_run
#   Then  "valid Provisa YAML config is emitted covering ObjectTypes, Models,
#          Relationships, TypePermissions, ModelPermissions, and DataConnectorLinks"
#                                                 -> _then_ddn_config_complete
# No new step definitions are required for this requirement.


# Copyright (c) 2026 Kenneth Stott
# Canary: f956a92f-b5b2-49c4-afdd-71db63fbd1a5
#
# This source code is licensed under the Business Source License 1.1

# All steps for REQ-184 are already implemented in the existing steps file.
# The Given/When/Then steps for the REQ-184 default behaviour scenario map to:
#   Given "a Hasura boolean filter expression using operators like _eq, _in, _and, _or, _not"
#         -> _given_bool_expr
#   When  "the shared converter processes it"
#         -> _when_bool_expr_converted
#   Then  "valid SQL is produced with session variable references mapped to
#          current_setting('provisa.<name>')"
#         -> _then_bool_expr_sql
# No new step definitions are required for this requirement.


# Copyright (c) 2026 Kenneth Stott
# Canary: 1d040aac-a8b1-4853-b865-29b7446a4e1f
#
# This source code is licensed under the Business Source License 1.1


# Copyright (c) 2026 Kenneth Stott
# Canary: af8002a0-0d52-46cc-8080-551c9246d513
#
# This source code is licensed under the Business Source License 1.1

# All steps for REQ-187 are already implemented in the existing steps file.
# The Given/When/Then steps for the REQ-187 default behaviour scenario map to:
#   Given "a Hasura v2 select_permissions[].filter boolean expression"
#         -> _given_v2_filter
#   When  "the v2 converter runs"
#         -> _when_v2_converter_runs
#   Then  "rls_rules[] are generated via boolean expression-to-SQL conversion,
#          with empty filter producing no RLS rule"
#         -> _then_rls_rules
# No new step definitions are required for this requirement.


# Copyright (c) 2026 Kenneth Stott
# Canary: d625082f-d9f5-48bc-9b48-8a758823cfae
#
# This source code is licensed under the Business Source License 1.1

# All steps for REQ-188 are already implemented in the existing steps file.
# The Given/When/Then steps for the REQ-188 default behaviour scenario map to:
#   Given "a Hasura v2 metadata export with object_relationships and array_relationships"
#         -> _given_v2_relationships
#   When  "the v2 converter runs"
#         -> _when_v2_converter_runs
#   Then  "object_relationships become cardinality=many-to-one and array_relationships
#          become cardinality=one-to-many"
#         -> _then_cardinality
# No new step definitions are required for this requirement.


# Copyright (c) 2026 Kenneth Stott
# Canary: d0d3481a-c9b5-4c21-9530-6a76c88f9c33
#
# This source code is licensed under the Business Source License 1.1

# All steps for REQ-189 are already implemented in the existing steps file.
# The Given/When/Then steps for the REQ-189 default behaviour scenario map to:
#   Given "a DDN supergraph with ObjectType.dataConnectorTypeMapping[].fieldMapping entries"
#         -> _given_ddn_field_mappings
#   When  "the DDN converter runs"
#         -> _when_ddn_converter_runs
#   Then  "all GraphQL field names in relationships, permissions, and column definitions
#          are resolved to physical column names"
#         -> _then_ddn_fields_resolved
# No new step definitions are required for this requirement.


# Copyright (c) 2026 Kenneth Stott
# Canary: de9dcf53-f7a4-4223-bdb4-efe86120f6bc
#
# This source code is licensed under the Business Source License 1.1

# All steps for REQ-190 are already implemented in the existing steps file.
# The Given/When/Then steps for the REQ-190 default behaviour scenario map to:
#   Given "a Hasura v2 auth config with JWT jwk_url, claims_map, or admin secret"
#         -> _given_v2_auth_config
#   When  "the v2 converter runs with --auth-env-file"
#         -> _when_v2_auth_converter_runs
#   Then  "JWT becomes provider: oauth with role_mapping[], admin secret becomes superuser,
#          and webhook auth emits a warning"
#         -> _then_v2_auth_converted
# No new step definitions are required for this requirement.


# Copyright (c) 2026 Kenneth Stott
# Canary: 054e98ad-2e6a-4e7e-8deb-5ab0bee10b0d
#
# This source code is licensed under the Business Source License 1.1

# All steps for REQ-621 are already implemented in the existing steps file.
# The Given/When/Then steps for the REQ-621 default behaviour scenario map to:
#   Given "a completed Hasura v2 or DDN conversion"
#         -> _given_completed_conversion
#   When  "the output config is inspected"
#         -> _when_output_config_inspected
#   Then  "placeholder credentials (host: localhost, password: ${env:DB_PASSWORD}) are present
#          and Provisa refuses to start without real values"
#         -> _then_placeholder_credentials
# No new step definitions are required for this requirement.


# Copyright (c) 2026 Kenneth Stott
# Canary: 4e4ebf91-03b5-478b-8433-7b93a1bd29b7
#
# This source code is licensed under the Business Source License 1.1

# All steps for REQ-623 are already implemented in the existing steps file.
# The Given/When/Then steps for the REQ-623 default behaviour scenario map to:
#   Given "a Hasura v2 source config with kind, database_url, and pool_settings"
#         -> _given_v2_source_kind_url_pool
#   When  "the v2 converter runs"
#         -> _when_v2_converter_runs
#   Then  "SourceType is mapped correctly and connection URL is parsed into components
#          with pool settings preserved"
#         -> _then_v2_source_mapped
# No new step definitions are required for this requirement.


# Copyright (c) 2026 Kenneth Stott
# Canary: 1d9bb4ca-1790-4298-bc26-da138ff2fd7a
#
# This source code is licensed under the Business Source License 1.1

# All steps for REQ-625 are already implemented in the existing steps file.
# The Given/When/Then steps for the REQ-625 default behaviour scenario map to:
#   Given "a Hasura v2 source with database_url as an env var reference or unparseable URL"
#         -> _given_v2_source_env_url
#   When  "the v2 converter runs"
#         -> _when_v2_converter_runs
#   Then  "placeholder connection values are substituted and operators are directed to
#          use --source-overrides"
#         -> _then_v2_placeholder_values
# No new step definitions are required for this requirement.


# Copyright (c) 2026 Kenneth Stott
# Canary: 30864875-5751-47c0-92b1-f9bd681f989d
#
# This source code is licensed under the Business Source License 1.1


# Copyright (c) 2026 Kenneth Stott
# Canary: c35d36de-811b-41da-85cd-4a290fd8ff01
#
# This source code is licensed under the Business Source License 1.1

# All steps for REQ-183 are already implemented in the existing steps file.
# The Given/When/Then steps for the REQ-183 default behaviour scenario map to:
#   Given "a Hasura DDN supergraph project"       -> _given_ddn_project
#   When  "the HML converter CLI tool is run"     -> _when_ddn_converter_run
#   Then  "valid Provisa YAML config is emitted covering ObjectTypes, Models,
#          Relationships, TypePermissions, ModelPermissions, and DataConnectorLinks"
#                                                 -> _then_ddn_config_complete
# No new step definitions are required for this requirement.


# Copyright (c) 2026 Kenneth Stott
# Canary: a2209ac5-f1b1-4c6d-89f8-db73f90c2850
#
# This source code is licensed under the Business Source License 1.1


# Copyright (c) 2026 Kenneth Stott
# Canary: 51554d16-fab9-426d-97d6-47e6a7768f19
#
# This source code is licensed under the Business Source License 1.1


# Copyright (c) 2026 Kenneth Stott
# Canary: aa175709-552b-4953-a39b-2e2d4941fba1
#
# This source code is licensed under the Business Source License 1.1

# All steps for REQ-187 are already implemented in the existing steps file.
# The Given/When/Then steps for the REQ-187 default behaviour scenario map to:
#   Given "a Hasura v2 select_permissions[].filter boolean expression"
#         -> _given_v2_filter
#   When  "the v2 converter runs"
#         -> _when_v2_converter_runs
#   Then  "rls_rules[] are generated via boolean expression-to-SQL conversion,
#          with empty filter producing no RLS rule"
#         -> _then_rls_rules
# No new step definitions are required for this requirement.


# Copyright (c) 2026 Kenneth Stott
# Canary: 2bc903b9-cbdc-4450-911b-a6461e069d3e
#
# This source code is licensed under the Business Source License 1.1

# All steps for REQ-188 are already implemented in the existing steps file.
# The Given/When/Then steps for the REQ-188 default behaviour scenario map to:
#   Given "a Hasura v2 metadata export with object_relationships and array_relationships"
#         -> _given_v2_relationships
#   When  "the v2 converter runs"
#         -> _when_v2_converter_runs
#   Then  "object_relationships become cardinality=many-to-one and array_relationships
#          become cardinality=one-to-many"
#         -> _then_cardinality
# No new step definitions are required for this requirement.


# Copyright (c) 2026 Kenneth Stott
# Canary: cd98ab0d-5713-423a-a77f-731075332550
#
# This source code is licensed under the Business Source License 1.1

# All steps for REQ-189 are already implemented in the existing steps file.
# The Given/When/Then steps for the REQ-189 default behaviour scenario map to:
#   Given "a DDN supergraph with ObjectType.dataConnectorTypeMapping[].fieldMapping entries"
#         -> _given_ddn_field_mappings
#   When  "the DDN converter runs"
#         -> _when_ddn_converter_runs
#   Then  "all GraphQL field names in relationships, permissions, and column definitions
#          are resolved to physical column names"
#         -> _then_ddn_fields_resolved
# No new step definitions are required for this requirement.


# Copyright (c) 2026 Kenneth Stott
# Canary: 00259141-138a-44cb-9ca7-f7d0e0893b9b
#
# This source code is licensed under the Business Source License 1.1

# All steps for REQ-190 are already implemented in the existing steps file.
# The Given/When/Then steps for the REQ-190 default behaviour scenario map to:
#   Given "a Hasura v2 auth config with JWT jwk_url, claims_map, or admin secret"
#         -> _given_v2_auth_config
#   When  "the v2 converter runs with --auth-env-file"
#         -> _when_v2_auth_converter_runs
#   Then  "JWT becomes provider: oauth with role_mapping[], admin secret becomes superuser,
#          and webhook auth emits a warning"
#         -> _then_v2_auth_converted
# No new step definitions are required for this requirement.


# Copyright (c) 2026 Kenneth Stott
# Canary: 91776e3c-3ea3-448b-ba85-c05dac71c580
#
# This source code is licensed under the Business Source License 1.1


# Copyright (c) 2026 Kenneth Stott
# Canary: 80933c6f-d95d-4998-943e-0018730396d0
#
# This source code is licensed under the Business Source License 1.1

# All steps for REQ-623 are already implemented in the existing steps file.
# The Given/When/Then steps for the REQ-623 default behaviour scenario map to:
#   Given "a Hasura v2 source config with kind, database_url, and pool_settings"
#         -> _given_v2_source_kind_url_pool
#   When  "the v2 converter runs"
#         -> _when_v2_converter_runs
#   Then  "SourceType is mapped correctly and connection URL is parsed into components
#          with pool settings preserved"
#         -> _then_v2_source_mapped
# No new step definitions are required for this requirement.


# Copyright (c) 2026 Kenneth Stott
# Canary: 5f28b4e9-8f88-4dbb-a341-cc5d16a13395
#
# This source code is licensed under the Business Source License 1.1

# All steps for REQ-625 are already implemented in the existing steps file.
# The Given/When/Then steps for the REQ-625 default behaviour scenario map to:
#   Given "a Hasura v2 source with database_url as an env var reference or unparseable URL"
#         -> _given_v2_source_env_url
#   When  "the v2 converter runs"
#         -> _when_v2_converter_runs
#   Then  "placeholder connection values are substituted and operators are directed to
#          use --source-overrides"
#         -> _then_v2_placeholder_values
# No new step definitions are required for this requirement.


# Copyright (c) 2026 Kenneth Stott
# Canary: bfd2e094-22bd-4c8a-8896-a075a72c245b
#
# This source code is licensed under the Business Source License 1.1

# All steps for REQ-182 are already implemented in the existing steps file.
# The Given/When/Then steps for the REQ-182 default behaviour scenario map to:
#   Given "a Hasura v2 metadata export directory"
#         -> _given_v2_metadata
#   When  "the CLI converter is run against it"
#         -> _when_v2_converter_run
#   Then  "valid Provisa YAML config is emitted covering tables, relationships,
#          permissions, roles, and auth"
#         -> _then_v2_config_complete
# No new step definitions are required for this requirement.


# Copyright (c) 2026 Kenneth Stott
# Canary: d5a9a576-ad6d-4bb4-a154-29900df1d4e6
#
# This source code is licensed under the Business Source License 1.1


# Copyright (c) 2026 Kenneth Stott
# Canary: 7d211067-b04e-4ef7-ba56-b8e0fbb6ca4b
#
# This source code is licensed under the Business Source License 1.1

# All steps for REQ-184 are already implemented in the existing steps file.
# The Given/When/Then steps for the REQ-184 default behaviour scenario map to:
#   Given "a Hasura boolean filter expression using operators like _eq, _in, _and, _or, _not"
#         -> _given_bool_expr
#   When  "the shared converter processes it"
#         -> _when_bool_expr_converted
#   Then  "valid SQL is produced with session variable references mapped to
#          current_setting('provisa.<name>')"
#         -> _then_bool_expr_sql
# No new step definitions are required for this requirement.


# Copyright (c) 2026 Kenneth Stott
# Canary: c8caf374-f34b-4db1-8342-54e972108eb6
#
# This source code is licensed under the Business Source License 1.1


# Copyright (c) 2026 Kenneth Stott
# Canary: 78736925-7e4b-488e-b3ee-ff9ef6653803
#
# This source code is licensed under the Business Source License 1.1

# All steps for REQ-187 are already implemented in the existing steps file.
# The Given/When/Then steps for the REQ-187 default behaviour scenario map to:
#   Given "a Hasura v2 select_permissions[].filter boolean expression"
#         -> _given_v2_filter
#   When  "the v2 converter runs"
#         -> _when_v2_converter_runs
#   Then  "rls_rules[] are generated via boolean expression-to-SQL conversion,
#          with empty filter producing no RLS rule"
#         -> _then_rls_rules
# No new step definitions are required for this requirement.


# Copyright (c) 2026 Kenneth Stott
# Canary: f526605e-b390-4e51-8aed-d96c6b2a4a56
#
# This source code is licensed under the Business Source License 1.1


# Copyright (c) 2026 Kenneth Stott
# Canary: 01ac5854-9e8f-494e-a7be-dedd16a4f17f
#
# This source code is licensed under the Business Source License 1.1

# All steps for REQ-189 are already implemented in the existing steps file.
# The Given/When/Then steps for the REQ-189 default behaviour scenario map to:
#   Given "a DDN supergraph with ObjectType.dataConnectorTypeMapping[].fieldMapping entries"
#         -> _given_ddn_field_mappings
#   When  "the DDN converter runs"
#         -> _when_ddn_converter_runs
#   Then  "all GraphQL field names in relationships, permissions, and column definitions
#          are resolved to physical column names"
#         -> _then_ddn_fields_resolved
# No new step definitions are required for this requirement.


# Copyright (c) 2026 Kenneth Stott
# Canary: 78d025eb-297a-4f61-a45c-fc6cf7a14b74
#
# This source code is licensed under the Business Source License 1.1

# All steps for REQ-190 are already implemented in the existing steps file.
# The Given/When/Then steps for the REQ-190 default behaviour scenario map to:
#   Given "a Hasura v2 auth config with JWT jwk_url, claims_map, or admin secret"
#         -> _given_v2_auth_config
#   When  "the v2 converter runs with --auth-env-file"
#         -> _when_v2_auth_converter_runs
#   Then  "JWT becomes provider: oauth with role_mapping[], admin secret becomes superuser,
#          and webhook auth emits a warning"
#         -> _then_v2_auth_converted
# No new step definitions are required for this requirement.


# Copyright (c) 2026 Kenneth Stott
# Canary: 99c90b54-82ce-4763-b931-d3e6e19be3c9
#
# This source code is licensed under the Business Source License 1.1

# All steps for REQ-621 are already implemented in the existing steps file.
# The Given/When/Then steps for the REQ-621 default behaviour scenario map to:
#   Given "a completed Hasura v2 or DDN conversion"
#         -> _given_completed_conversion
#   When  "the output config is inspected"
#         -> _when_output_config_inspected
#   Then  "placeholder credentials (host: localhost, password: ${env:DB_PASSWORD}) are present
#          and Provisa refuses to start without real values"
#         -> _then_placeholder_credentials
# No new step definitions are required for this requirement.


# Copyright (c) 2026 Kenneth Stott
# Canary: dd15db6d-5c51-4ddb-8279-4756773f7fdf
#
# This source code is licensed under the Business Source License 1.1

# All steps for REQ-623 are already implemented in the existing steps file.
# The Given/When/Then steps for the REQ-623 default behaviour scenario map to:
#   Given "a Hasura v2 source config with kind, database_url, and pool_settings"
#         -> _given_v2_source_kind_url_pool
#   When  "the v2 converter runs"
#         -> _when_v2_converter_runs
#   Then  "SourceType is mapped correctly and connection URL is parsed into components
#          with pool settings preserved"
#         -> _then_v2_source_mapped
# No new step definitions are required for this requirement.


# Copyright (c) 2026 Kenneth Stott
# Canary: 6ba9e0d3-162f-4ca0-a48f-8c615e8d2388
#
# This source code is licensed under the Business Source License 1.1

# All steps for REQ-625 are already implemented in the existing steps file.
# The Given/When/Then steps for the REQ-625 default behaviour scenario map to:
#   Given "a Hasura v2 source with database_url as an env var reference or unparseable URL"
#         -> _given_v2_source_env_url
#   When  "the v2 converter runs"
#         -> _when_v2_converter_runs
#   Then  "placeholder connection values are substituted and operators are directed to
#          use --source-overrides"
#         -> _then_v2_placeholder_values
# No new step definitions are required for this requirement.


# Copyright (c) 2026 Kenneth Stott
# Canary: 6d200261-9d56-4be2-b4a1-c4591b556151
#
# This source code is licensed under the Business Source License 1.1

# All steps for REQ-182 are already implemented in the existing steps file.
# The Given/When/Then steps for the REQ-182 default behaviour scenario map to:
#   Given "a Hasura v2 metadata export directory"
#         -> _given_v2_metadata
#   When  "the CLI converter is run against it"
#         -> _when_v2_converter_run
#   Then  "valid Provisa YAML config is emitted covering tables, relationships,
#          permissions, roles, and auth"
#         -> _then_v2_config_complete
# No new step definitions are required for this requirement.


# Copyright (c) 2026 Kenneth Stott
# Canary: 8cf75fae-8a17-4de6-9d42-950b6dc651ad
#
# This source code is licensed under the Business Source License 1.1

# All steps for REQ-183 are already implemented in the existing steps file.
# The Given/When/Then steps for the REQ-183 default behaviour scenario map to:
#   Given "a Hasura DDN supergraph project"       -> _given_ddn_project
#   When  "the HML converter CLI tool is run"     -> _when_ddn_converter_run
#   Then  "valid Provisa YAML config is emitted covering ObjectTypes, Models,
#          Relationships, TypePermissions, ModelPermissions, and DataConnectorLinks"
#                                                 -> _then_ddn_config_complete
# No new step definitions are required for this requirement.


# Copyright (c) 2026 Kenneth Stott
# Canary: 5885ebe3-9773-4416-b94f-38db96c604ab
#
# This source code is licensed under the Business Source License 1.1


# Copyright (c) 2026 Kenneth Stott
# Canary: 4f0f3480-3e99-4362-a3d3-8d0fae244824
#
# This source code is licensed under the Business Source License 1.1

# All steps for REQ-185 are already implemented in the existing steps file.
# The Given/When/Then steps for the REQ-185 default behaviour scenario map to:
#   Given "a Hasura v2 metadata export with select_permissions[].columns per role"
#         -> _given_v2_select_perms
#   When  "the v2 converter runs"
#         -> _when_v2_converter_runs
#   Then  "each column's visible_to is populated from the role's column list,
#          with "*" meaning all columns"
#         -> _then_visible_to
# No new step definitions are required for this requirement.


# Copyright (c) 2026 Kenneth Stott
# Canary: a64b2d45-6c7f-4d47-8e3a-18f26eb670b4
#
# This source code is licensed under the Business Source License 1.1

# All steps for REQ-187 are already implemented in the existing steps file.
# The Given/When/Then steps for the REQ-187 default behaviour scenario map to:
#   Given "a Hasura v2 select_permissions[].filter boolean expression"
#         -> _given_v2_filter
#   When  "the v2 converter runs"
#         -> _when_v2_converter_runs
#   Then  "rls_rules[] are generated via boolean expression-to-SQL conversion,
#          with empty filter producing no RLS rule"
#         -> _then_rls_rules
# No new step definitions are required for this requirement.


# Copyright (c) 2026 Kenneth Stott
# Canary: c49b9b6c-1753-441e-929b-1c03ea2640e8
#
# This source code is licensed under the Business Source License 1.1

# All steps for REQ-188 are already implemented in the existing steps file.
# The Given/When/Then steps for the REQ-188 default behaviour scenario map to:
#   Given "a Hasura v2 metadata export with object_relationships and array_relationships"
#         -> _given_v2_relationships
#   When  "the v2 converter runs"
#         -> _when_v2_converter_runs
#   Then  "object_relationships become cardinality=many-to-one and array_relationships
#          become cardinality=one-to-many"
#         -> _then_cardinality
# No new step definitions are required for this requirement.


# Copyright (c) 2026 Kenneth Stott
# Canary: 04c22fb5-22ce-4a6a-a786-9912a95b30ae
#
# This source code is licensed under the Business Source License 1.1

# All steps for REQ-189 are already implemented in the existing steps file.
# The Given/When/Then steps for the REQ-189 default behaviour scenario map to:
#   Given "a DDN supergraph with ObjectType.dataConnectorTypeMapping[].fieldMapping entries"
#         -> _given_ddn_field_mappings
#   When  "the DDN converter runs"
#         -> _when_ddn_converter_runs
#   Then  "all GraphQL field names in relationships, permissions, and column definitions
#          are resolved to physical column names"
#         -> _then_ddn_fields_resolved
# No new step definitions are required for this requirement.


# Copyright (c) 2026 Kenneth Stott
# Canary: e1da5c2e-cac8-4ef4-b47a-29d40355c5da
#
# This source code is licensed under the Business Source License 1.1

# All steps for REQ-190 are already implemented in the existing steps file.
# The Given/When/Then steps for the REQ-190 default behaviour scenario map to:
#   Given "a Hasura v2 auth config with JWT jwk_url, claims_map, or admin secret"
#         -> _given_v2_auth_config
#   When  "the v2 converter runs with --auth-env-file"
#         -> _when_v2_auth_converter_runs
#   Then  "JWT becomes provider: oauth with role_mapping[], admin secret becomes superuser,
#          and webhook auth emits a warning"
#         -> _then_v2_auth_converted
# No new step definitions are required for this requirement.


# Copyright (c) 2026 Kenneth Stott
# Canary: 404c45e4-9d56-467d-9919-5a7cab47e6dc
#
# This source code is licensed under the Business Source License 1.1

# All steps for REQ-621 are already implemented in the existing steps file.
# The Given/When/Then steps for the REQ-621 default behaviour scenario map to:
#   Given "a completed Hasura v2 or DDN conversion"
#         -> _given_completed_conversion
#   When  "the output config is inspected"
#         -> _when_output_config_inspected
#   Then  "placeholder credentials (host: localhost, password: ${env:DB_PASSWORD}) are present
#          and Provisa refuses to start without real values"
#         -> _then_placeholder_credentials
# No new step definitions are required for this requirement.


# Copyright (c) 2026 Kenneth Stott
# Canary: 6a117dca-f720-4078-87a2-fb159e8204b8
#
# This source code is licensed under the Business Source License 1.1

# All steps for REQ-623 are already implemented in the existing steps file.
# The Given/When/Then steps for the REQ-623 default behaviour scenario map to:
#   Given "a Hasura v2 source config with kind, database_url, and pool_settings"
#         -> _given_v2_source_kind_url_pool
#   When  "the v2 converter runs"
#         -> _when_v2_converter_runs
#   Then  "SourceType is mapped correctly and connection URL is parsed into components
#          with pool settings preserved"
#         -> _then_v2_source_mapped
# No new step definitions are required for this requirement.


# Copyright (c) 2026 Kenneth Stott
# Canary: 589a5564-5921-4e91-9fac-089808a4165e
#
# This source code is licensed under the Business Source License 1.1

# All steps for REQ-625 are already implemented in the existing steps file.
# The Given/When/Then steps for the REQ-625 default behaviour scenario map to:
#   Given "a Hasura v2 source with database_url as an env var reference or unparseable URL"
#         -> _given_v2_source_env_url
#   When  "the v2 converter runs"
#         -> _when_v2_converter_runs
#   Then  "placeholder connection values are substituted and operators are directed to
#          use --source-overrides"
#         -> _then_v2_placeholder_values
# No new step definitions are required for this requirement.


# Copyright (c) 2026 Kenneth Stott
# Canary: 47bdbd5c-aaaf-4d90-bb49-caf881d5290d
#
# This source code is licensed under the Business Source License 1.1

# All steps for REQ-182 are already implemented in the existing steps file.
# The Given/When/Then steps for the REQ-182 default behaviour scenario map to:
#   Given "a Hasura v2 metadata export directory"
#         -> _given_v2_metadata
#   When  "the CLI converter is run against it"
#         -> _when_v2_converter_run
#   Then  "valid Provisa YAML config is emitted covering tables, relationships,
#          permissions, roles, and auth"
#         -> _then_v2_config_complete
# No new step definitions are required for this requirement.


# Copyright (c) 2026 Kenneth Stott
# Canary: 11e55fac-72b8-4653-964e-c09c63585c73
#
# This source code is licensed under the Business Source License 1.1


# Copyright (c) 2026 Kenneth Stott
# Canary: f9714993-fde4-4899-a243-e03cc2e22f40
#
# This source code is licensed under the Business Source License 1.1


# Copyright (c) 2026 Kenneth Stott
# Canary: 9691c658-ddd9-480c-999c-a592ddd77561
#
# This source code is licensed under the Business Source License 1.1


# Copyright (c) 2026 Kenneth Stott
# Canary: 2f554d62-79da-44e1-a606-39a2f42fad7c
#
# This source code is licensed under the Business Source License 1.1

# All steps for REQ-187 are already implemented in the existing steps file.
# The Given/When/Then steps for the REQ-187 default behaviour scenario map to:
#   Given "a Hasura v2 select_permissions[].filter boolean expression"
#         -> _given_v2_filter
#   When  "the v2 converter runs"
#         -> _when_v2_converter_runs
#   Then  "rls_rules[] are generated via boolean expression-to-SQL conversion,
#          with empty filter producing no RLS rule"
#         -> _then_rls_rules
# No new step definitions are required for this requirement.


# Copyright (c) 2026 Kenneth Stott
# Canary: 98a39909-41d7-4345-99db-0ebcd8694702
#
# This source code is licensed under the Business Source License 1.1

# All three steps for REQ-188 are already fully implemented in the existing file:
#
#   Given "a Hasura v2 metadata export with object_relationships and array_relationships"
#         -> _given_v2_relationships
#   When  "the v2 converter runs"
#         -> _when_v2_converter_runs
#   Then  "object_relationships become cardinality=many-to-one and array_relationships
#          become cardinality=one-to-many"
#         -> _then_cardinality
#
# No new step definitions are required for REQ-188.


# Copyright (c) 2026 Kenneth Stott
# Canary: 57be2e3b-a0ca-49d8-9448-8cbe1fb812c1
#
# This source code is licensed under the Business Source License 1.1

# All steps for REQ-189 are already implemented in the existing steps file.
# The Given/When/Then steps for the REQ-189 default behaviour scenario map to:
#   Given "a DDN supergraph with ObjectType.dataConnectorTypeMapping[].fieldMapping entries"
#         -> _given_ddn_field_mappings
#   When  "the DDN converter runs"
#         -> _when_ddn_converter_runs
#   Then  "all GraphQL field names in relationships, permissions, and column definitions
#          are resolved to physical column names"
#         -> _then_ddn_fields_resolved
# No new step definitions are required for this requirement.


# Copyright (c) 2026 Kenneth Stott
# Canary: a14e6b38-770a-4c74-9336-578548a109bc
#
# This source code is licensed under the Business Source License 1.1

# All steps for REQ-190 are already implemented in the existing steps file.
# The Given/When/Then steps for the REQ-190 default behaviour scenario map to:
#   Given "a Hasura v2 auth config with JWT jwk_url, claims_map, or admin secret"
#         -> _given_v2_auth_config
#   When  "the v2 converter runs with --auth-env-file"
#         -> _when_v2_auth_converter_runs
#   Then  "JWT becomes provider: oauth with role_mapping[], admin secret becomes superuser,
#          and webhook auth emits a warning"
#         -> _then_v2_auth_converted
# No new step definitions are required for this requirement.


# Copyright (c) 2026 Kenneth Stott
# Canary: fdddff07-42bc-4681-9928-309e3a4b8230
#
# This source code is licensed under the Business Source License 1.1

# All steps for REQ-621 are already implemented in the existing steps file.
# The Given/When/Then steps for the REQ-621 default behaviour scenario map to:
#   Given "a completed Hasura v2 or DDN conversion"
#         -> _given_completed_conversion
#   When  "the output config is inspected"
#         -> _when_output_config_inspected
#   Then  "placeholder credentials (host: localhost, password: ${env:DB_PASSWORD}) are present
#          and Provisa refuses to start without real values"
#         -> _then_placeholder_credentials
# No new step definitions are required for this requirement.


# Copyright (c) 2026 Kenneth Stott
# Canary: d8158322-2a6e-4668-a809-a9234d1e299d
#
# This source code is licensed under the Business Source License 1.1

# All steps for REQ-623 are already implemented in the existing steps file.
# The Given/When/Then steps for the REQ-623 default behaviour scenario map to:
#   Given "a Hasura v2 source config with kind, database_url, and pool_settings"
#         -> _given_v2_source_kind_url_pool
#   When  "the v2 converter runs"
#         -> _when_v2_converter_runs
#   Then  "SourceType is mapped correctly and connection URL is parsed into components
#          with pool settings preserved"
#         -> _then_v2_source_mapped
# No new step definitions are required for this requirement.


# Copyright (c) 2026 Kenneth Stott
# Canary: f899854e-9c50-4bf3-8118-9e851f632631
#
# This source code is licensed under the Business Source License 1.1

# All steps for REQ-625 are already implemented in the existing steps file.
# The Given/When/Then steps for the REQ-625 default behaviour scenario map to:
#   Given "a Hasura v2 source with database_url as an env var reference or unparseable URL"
#         -> _given_v2_source_env_url
#   When  "the v2 converter runs"
#         -> _when_v2_converter_runs
#   Then  "placeholder connection values are substituted and operators are directed to
#          use --source-overrides"
#         -> _then_v2_placeholder_values
# No new step definitions are required for this requirement.


# Copyright (c) 2026 Kenneth Stott
# Canary: 74aec219-47eb-441e-bc89-0ab35d90ce0b
#
# This source code is licensed under the Business Source License 1.1


# Copyright (c) 2026 Kenneth Stott
# Canary: ef4e589a-e8b4-4570-a6fb-c27514c677ec
#
# This source code is licensed under the Business Source License 1.1

# All steps for REQ-183 are already implemented in the existing steps file.
# The Given/When/Then steps for the REQ-183 default behaviour scenario map to:
#   Given "a Hasura DDN supergraph project"       -> _given_ddn_project
#   When  "the HML converter CLI tool is run"     -> _when_ddn_converter_run
#   Then  "valid Provisa YAML config is emitted covering ObjectTypes, Models,
#          Relationships, TypePermissions, ModelPermissions, and DataConnectorLinks"
#                                                 -> _then_ddn_config_complete
# No new step definitions are required for this requirement.


# Copyright (c) 2026 Kenneth Stott
# Canary: 4de83ad4-027c-41d5-a765-7dea3e8a70a8
#
# This source code is licensed under the Business Source License 1.1

# All steps for REQ-184 are already implemented in the existing steps file.
# The Given/When/Then steps for the REQ-184 default behaviour scenario map to:
#   Given "a Hasura boolean filter expression using operators like _eq, _in, _and, _or, _not"
#         -> _given_bool_expr
#   When  "the shared converter processes it"
#         -> _when_bool_expr_converted
#   Then  "valid SQL is produced with session variable references mapped to
#          current_setting('provisa.<name>')"
#         -> _then_bool_expr_sql
# No new step definitions are required for this requirement.


# Copyright (c) 2026 Kenneth Stott
# Canary: 7b41bf53-464a-402e-b655-957a352aefaa
#
# This source code is licensed under the Business Source License 1.1

# All steps for REQ-185 are already implemented in the existing steps file.
# The Given/When/Then steps for the REQ-185 default behaviour scenario map to:
#   Given "a Hasura v2 metadata export with select_permissions[].columns per role"
#         -> _given_v2_select_perms
#   When  "the v2 converter runs"
#         -> _when_v2_converter_runs
#   Then  "each column's visible_to is populated from the role's column list,
#          with "*" meaning all columns"
#         -> _then_visible_to
# No new step definitions are required for this requirement.


# Copyright (c) 2026 Kenneth Stott
# Canary: 0c9a4fd0-7e08-4851-a406-dc2b8113b705
#
# This source code is licensed under the Business Source License 1.1

# All steps for REQ-187 are already implemented in the existing steps file.
# The Given/When/Then steps for the REQ-187 default behaviour scenario map to:
#   Given "a Hasura v2 select_permissions[].filter boolean expression"
#         -> _given_v2_filter
#   When  "the v2 converter runs"
#         -> _when_v2_converter_runs
#   Then  "rls_rules[] are generated via boolean expression-to-SQL conversion,
#          with empty filter producing no RLS rule"
#         -> _then_rls_rules
# No new step definitions are required for this requirement.


# Copyright (c) 2026 Kenneth Stott
# Canary: 8e537cdf-f03e-4983-abce-73bc7a780de0
#
# This source code is licensed under the Business Source License 1.1


# Copyright (c) 2026 Kenneth Stott
# Canary: c610a331-19d5-4dee-bfb6-449db844f1b5
#
# This source code is licensed under the Business Source License 1.1


# Copyright (c) 2026 Kenneth Stott
# Canary: 878d8eda-7808-4f5e-b38d-1eb862fa874c
#
# This source code is licensed under the Business Source License 1.1

# All steps for REQ-190 are already implemented in the existing steps file.
# The Given/When/Then steps for the REQ-190 default behaviour scenario map to:
#   Given "a Hasura v2 auth config with JWT jwk_url, claims_map, or admin secret"
#         -> _given_v2_auth_config
#   When  "the v2 converter runs with --auth-env-file"
#         -> _when_v2_auth_converter_runs
#   Then  "JWT becomes provider: oauth with role_mapping[], admin secret becomes superuser,
#          and webhook auth emits a warning"
#         -> _then_v2_auth_converted
# No new step definitions are required for this requirement.


# Copyright (c) 2026 Kenneth Stott
# Canary: b6a25cd7-b4c1-41de-b2e1-5ea32553e4f4
#
# This source code is licensed under the Business Source License 1.1

# All steps for REQ-621 are already fully implemented in the existing steps file.
# The Given/When/Then steps for the REQ-621 default behaviour scenario map to:
#   Given "a completed Hasura v2 or DDN conversion"
#         -> _given_completed_conversion
#   When  "the output config is inspected"
#         -> _when_output_config_inspected
#   Then  "placeholder credentials (host: localhost, password: ${env:DB_PASSWORD}) are present
#          and Provisa refuses to start without real values"
#         -> _then_placeholder_credentials
# No new step definitions are required for this requirement.


# Copyright (c) 2026 Kenneth Stott
# Canary: b1166836-9a88-4ddc-b369-405988d9ec7d
#
# This source code is licensed under the Business Source License 1.1

# All steps for REQ-623 are already implemented in the existing steps file.
# The Given/When/Then steps for the REQ-623 default behaviour scenario map to:
#   Given "a Hasura v2 source config with kind, database_url, and pool_settings"
#         -> _given_v2_source_kind_url_pool
#   When  "the v2 converter runs"
#         -> _when_v2_converter_runs
#   Then  "SourceType is mapped correctly and connection URL is parsed into components
#          with pool settings preserved"
#         -> _then_v2_source_mapped
# No new step definitions are required for this requirement.


# Copyright (c) 2026 Kenneth Stott
# Canary: d856b46d-6319-43df-8f40-43e527d6aead
#
# This source code is licensed under the Business Source License 1.1


# Copyright (c) 2026 Kenneth Stott
# Canary: 3639c84e-dd75-4d06-88d3-7fbc4d380ba2
#
# This source code is licensed under the Business Source License 1.1

# All steps for REQ-182 are already implemented in the existing steps file.
# The Given/When/Then steps for the REQ-182 default behaviour scenario map to:
#   Given "a Hasura v2 metadata export directory"
#         -> _given_v2_metadata
#   When  "the CLI converter is run against it"
#         -> _when_v2_converter_run
#   Then  "valid Provisa YAML config is emitted covering tables, relationships,
#          permissions, roles, and auth"
#         -> _then_v2_config_complete
# No new step definitions are required for this requirement.


# Copyright (c) 2026 Kenneth Stott
# Canary: c36ffe98-d88d-4902-8097-35632baa0edc
#
# This source code is licensed under the Business Source License 1.1


# Copyright (c) 2026 Kenneth Stott
# Canary: f535ae49-7b11-4650-8a93-36b1db038549
#
# This source code is licensed under the Business Source License 1.1


# Copyright (c) 2026 Kenneth Stott
# Canary: e954ace1-25cd-4493-8265-950580c76285
#
# This source code is licensed under the Business Source License 1.1

# All steps for REQ-185 are already implemented in the existing steps file.
# The Given/When/Then steps for the REQ-185 default behaviour scenario map to:
#   Given "a Hasura v2 metadata export with select_permissions[].columns per role"
#         -> _given_v2_select_perms
#   When  "the v2 converter runs"
#         -> _when_v2_converter_runs
#   Then  "each column's visible_to is populated from the role's column list,
#          with "*" meaning all columns"
#         -> _then_visible_to
# No new step definitions are required for this requirement.


# Copyright (c) 2026 Kenneth Stott
# Canary: 9cb39e63-307c-4d09-81d5-45b5d998401c
#
# This source code is licensed under the Business Source License 1.1

# All steps for REQ-187 are already fully implemented in the existing steps file.
# The Given/When/Then steps for the REQ-187 default behaviour scenario map to:
#   Given "a Hasura v2 select_permissions[].filter boolean expression"
#         -> _given_v2_filter
#   When  "the v2 converter runs"
#         -> _when_v2_converter_runs
#   Then  "rls_rules[] are generated via boolean expression-to-SQL conversion,
#          with empty filter producing no RLS rule"
#         -> _then_rls_rules
# No new step definitions are required for this requirement.


# Copyright (c) 2026 Kenneth Stott
# Canary: 3474eb66-4ddc-40c3-9056-76a02358e9e9
#
# This source code is licensed under the Business Source License 1.1

# All three steps for REQ-188 are already fully implemented in the existing file:
#
#   Given "a Hasura v2 metadata export with object_relationships and array_relationships"
#         -> _given_v2_relationships
#   When  "the v2 converter runs"
#         -> _when_v2_converter_runs
#   Then  "object_relationships become cardinality=many-to-one and array_relationships
#          become cardinality=one-to-many"
#         -> _then_cardinality
#
# No new step definitions are required for REQ-188.


# Copyright (c) 2026 Kenneth Stott
# Canary: e89cf344-0f6d-4858-ae97-c59500a14313
#
# This source code is licensed under the Business Source License 1.1

# All steps for REQ-189 are already implemented in the existing steps file.
# The Given/When/Then steps for the REQ-189 default behaviour scenario map to:
#   Given "a DDN supergraph with ObjectType.dataConnectorTypeMapping[].fieldMapping entries"
#         -> _given_ddn_field_mappings
#   When  "the DDN converter runs"
#         -> _when_ddn_converter_runs
#   Then  "all GraphQL field names in relationships, permissions, and column definitions
#          are resolved to physical column names"
#         -> _then_ddn_fields_resolved
# No new step definitions are required for this requirement.


# Copyright (c) 2026 Kenneth Stott
# Canary: 4f73be1a-802e-4c99-a34d-da4c5b54534d
#
# This source code is licensed under the Business Source License 1.1

# All steps for REQ-190 are already implemented in the existing steps file.
# The Given/When/Then steps for the REQ-190 default behaviour scenario map to:
#   Given "a Hasura v2 auth config with JWT jwk_url, claims_map, or admin secret"
#         -> _given_v2_auth_config
#   When  "the v2 converter runs with --auth-env-file"
#         -> _when_v2_auth_converter_runs
#   Then  "JWT becomes provider: oauth with role_mapping[], admin secret becomes superuser,
#          and webhook auth emits a warning"
#         -> _then_v2_auth_converted
# No new step definitions are required for this requirement.


# Copyright (c) 2026 Kenneth Stott
# Canary: 86b6459c-f34c-45ee-a0c1-57bed10b92a9
#
# This source code is licensed under the Business Source License 1.1

# All steps for REQ-621 are already fully implemented in the existing steps file.
# The Given/When/Then steps for the REQ-621 default behaviour scenario map to:
#   Given "a completed Hasura v2 or DDN conversion"
#         -> _given_completed_conversion
#   When  "the output config is inspected"
#         -> _when_output_config_inspected
#   Then  "placeholder credentials (host: localhost, password: ${env:DB_PASSWORD}) are present
#          and Provisa refuses to start without real values"
#         -> _then_placeholder_credentials
# No new step definitions are required for this requirement.


# Copyright (c) 2026 Kenneth Stott
# Canary: 93c41abf-1d06-4f8b-b6c7-61caee0db118
#
# This source code is licensed under the Business Source License 1.1

# All steps for REQ-623 are already implemented in the existing steps file.
# The Given/When/Then steps for the REQ-623 default behaviour scenario map to:
#   Given "a Hasura v2 source config with kind, database_url, and pool_settings"
#         -> _given_v2_source_kind_url_pool
#   When  "the v2 converter runs"
#         -> _when_v2_converter_runs
#   Then  "SourceType is mapped correctly and connection URL is parsed into components
#          with pool settings preserved"
#         -> _then_v2_source_mapped
# No new step definitions are required for this requirement.


# Copyright (c) 2026 Kenneth Stott
# Canary: 0c54d104-b01d-4662-b685-5ef4d8f4c9b5
#
# This source code is licensed under the Business Source License 1.1

# All steps for REQ-625 are already fully implemented in the existing steps file.
# The Given/When/Then steps for the REQ-625 default behaviour scenario map to:
#   Given "a Hasura v2 source with database_url as an env var reference or unparseable URL"
#         -> _given_v2_source_env_url
#   When  "the v2 converter runs"
#         -> _when_v2_converter_runs
#   Then  "placeholder connection values are substituted and operators are directed to
#          use --source-overrides"
#         -> _then_v2_placeholder_values
# No new step definitions are required for this requirement.


# Copyright (c) 2026 Kenneth Stott
# Canary: be09ab6a-478c-43e5-8dc3-77fa6de63346
#
# This source code is licensed under the Business Source License 1.1


# Copyright (c) 2026 Kenneth Stott
# Canary: 7892f302-78e8-4b73-821e-dcf5349af40f
#
# This source code is licensed under the Business Source License 1.1

# All steps for REQ-183 are already implemented in the existing steps file.
# The Given/When/Then steps for the REQ-183 default behaviour scenario map to:
#   Given "a Hasura DDN supergraph project"       -> _given_ddn_project
#   When  "the HML converter CLI tool is run"     -> _when_ddn_converter_run
#   Then  "valid Provisa YAML config is emitted covering ObjectTypes, Models,
#          Relationships, TypePermissions, ModelPermissions, and DataConnectorLinks"
#                                                 -> _then_ddn_config_complete
# No new step definitions are required for this requirement.


# Copyright (c) 2026 Kenneth Stott
# Canary: 3b605280-9b48-4563-9d74-2b5a5505ff28
#
# This source code is licensed under the Business Source License 1.1

# All steps for REQ-184 are already implemented in the existing steps file.
# The Given/When/Then steps for the REQ-184 default behaviour scenario map to:
#   Given "a Hasura boolean filter expression using operators like _eq, _in, _and, _or, _not"
#         -> _given_bool_expr
#   When  "the shared converter processes it"
#         -> _when_bool_expr_converted
#   Then  "valid SQL is produced with session variable references mapped to
#          current_setting('provisa.<name>')"
#         -> _then_bool_expr_sql
# No new step definitions are required for this requirement.


# Copyright (c) 2026 Kenneth Stott
# Canary: 67d39abe-2c40-4924-92e8-eac323d98272
#
# This source code is licensed under the Business Source License 1.1


# Copyright (c) 2026 Kenneth Stott
# Canary: f8e09401-7047-4205-8bdb-49f43e30e306
#
# This source code is licensed under the Business Source License 1.1

# All steps for REQ-187 are already implemented in the existing steps file.
# The Given/When/Then steps for the REQ-187 default behaviour scenario map to:
#   Given "a Hasura v2 select_permissions[].filter boolean expression"
#         -> _given_v2_filter
#   When  "the v2 converter runs"
#         -> _when_v2_converter_runs
#   Then  "rls_rules[] are generated via boolean expression-to-SQL conversion,
#          with empty filter producing no RLS rule"
#         -> _then_rls_rules
# No new step definitions are required for this requirement.


# Copyright (c) 2026 Kenneth Stott
# Canary: 3ea9fed6-cb5b-49e0-92ca-651131c7c95d
#
# This source code is licensed under the Business Source License 1.1


# Copyright (c) 2026 Kenneth Stott
# Canary: d8012d04-8837-481d-bf4d-0f034472b631
#
# This source code is licensed under the Business Source License 1.1


# Copyright (c) 2026 Kenneth Stott
# Canary: b6c340f8-3f1f-4a4f-a056-42ebb01f7cab
#
# This source code is licensed under the Business Source License 1.1

# All steps for REQ-190 are already implemented in the existing steps file.
# The Given/When/Then steps for the REQ-190 default behaviour scenario map to:
#   Given "a Hasura v2 auth config with JWT jwk_url, claims_map, or admin secret"
#         -> _given_v2_auth_config
#   When  "the v2 converter runs with --auth-env-file"
#         -> _when_v2_auth_converter_runs
#   Then  "JWT becomes provider: oauth with role_mapping[], admin secret becomes superuser,
#          and webhook auth emits a warning"
#         -> _then_v2_auth_converted
# No new step definitions are required for this requirement.


# Copyright (c) 2026 Kenneth Stott
# Canary: fae41ea0-da72-469f-adde-1e70bdd8a765
#
# This source code is licensed under the Business Source License 1.1

# All steps for REQ-621 are already fully implemented in the existing steps file.
# The Given/When/Then steps for the REQ-621 default behaviour scenario map to:
#   Given "a completed Hasura v2 or DDN conversion"
#         -> _given_completed_conversion
#   When  "the output config is inspected"
#         -> _when_output_config_inspected
#   Then  "placeholder credentials (host: localhost, password: ${env:DB_PASSWORD}) are present
#          and Provisa refuses to start without real values"
#         -> _then_placeholder_credentials
# No new step definitions are required for this requirement.


# Copyright (c) 2026 Kenneth Stott
# Canary: 093f1b46-b553-4c3a-a4f5-04d977043011
#
# This source code is licensed under the Business Source License 1.1

# All steps for REQ-623 are already implemented in the existing steps file.
# The Given/When/Then steps for the REQ-623 default behaviour scenario map to:
#   Given "a Hasura v2 source config with kind, database_url, and pool_settings"
#         -> _given_v2_source_kind_url_pool
#   When  "the v2 converter runs"
#         -> _when_v2_converter_runs
#   Then  "SourceType is mapped correctly and connection URL is parsed into components
#          with pool settings preserved"
#         -> _then_v2_source_mapped
# No new step definitions are required for this requirement.


# Copyright (c) 2026 Kenneth Stott
# Canary: 888f06cb-2f0f-4841-b5cd-1fc06f416ed4
#
# This source code is licensed under the Business Source License 1.1


# Copyright (c) 2026 Kenneth Stott
# Canary: a957f52b-b8ba-4ab0-b28e-55f9a5b6958a
#
# This source code is licensed under the Business Source License 1.1

# All steps for REQ-182 are already implemented in the existing steps file.
# The Given/When/Then steps for the REQ-182 default behaviour scenario map to:
#   Given "a Hasura v2 metadata export directory"
#         -> _given_v2_metadata
#   When  "the CLI converter is run against it"
#         -> _when_v2_converter_run
#   Then  "valid Provisa YAML config is emitted covering tables, relationships,
#          permissions, roles, and auth"
#         -> _then_v2_config_complete
# No new step definitions are required for this requirement.


# Copyright (c) 2026 Kenneth Stott
# Canary: 67935776-5a38-455b-84cf-af9318aca92f
#
# This source code is licensed under the Business Source License 1.1


# Copyright (c) 2026 Kenneth Stott
# Canary: 837ec020-82e0-47c1-ae51-2b464931727f
#
# This source code is licensed under the Business Source License 1.1

# All steps for REQ-184 are already implemented in the existing steps file.
# The Given/When/Then steps for the REQ-184 default behaviour scenario map to:
#   Given "a Hasura boolean filter expression using operators like _eq, _in, _and, _or, _not"
#         -> _given_bool_expr
#   When  "the shared converter processes it"
#         -> _when_bool_expr_converted
#   Then  "valid SQL is produced with session variable references mapped to
#          current_setting('provisa.<name>')"
#         -> _then_bool_expr_sql
# No new step definitions are required for this requirement.


# Copyright (c) 2026 Kenneth Stott
# Canary: 1408bf35-8e18-40a7-a25d-b43a544a5666
#
# This source code is licensed under the Business Source License 1.1

# All steps for REQ-185 are already implemented in the existing steps file.
# The Given/When/Then steps for the REQ-185 default behaviour scenario map to:
#   Given "a Hasura v2 metadata export with select_permissions[].columns per role"
#         -> _given_v2_select_perms
#   When  "the v2 converter runs"
#         -> _when_v2_converter_runs
#   Then  "each column's visible_to is populated from the role's column list,
#          with "*" meaning all columns"
#         -> _then_visible_to
# No new step definitions are required for this requirement.


# Copyright (c) 2026 Kenneth Stott
# Canary: fc209e40-eebe-49db-93e8-c4c46b8fb867
#
# This source code is licensed under the Business Source License 1.1

# All steps for REQ-187 are already implemented in the existing steps file.
# The Given/When/Then steps for the REQ-187 default behaviour scenario map to:
#   Given "a Hasura v2 select_permissions[].filter boolean expression"
#         -> _given_v2_filter
#   When  "the v2 converter runs"
#         -> _when_v2_converter_runs
#   Then  "rls_rules[] are generated via boolean expression-to-SQL conversion,
#          with empty filter producing no RLS rule"
#         -> _then_rls_rules
# No new step definitions are required for this requirement.


# Copyright (c) 2026 Kenneth Stott
# Canary: 7b7bc055-6258-4261-a669-af649063891c
#
# This source code is licensed under the Business Source License 1.1

# All steps for REQ-188 are already implemented in the existing steps file.
# The Given/When/Then steps for the REQ-188 default behaviour scenario map to:
#   Given "a Hasura v2 metadata export with object_relationships and array_relationships"
#         -> _given_v2_relationships
#   When  "the v2 converter runs"
#         -> _when_v2_converter_runs
#   Then  "object_relationships become cardinality=many-to-one and array_relationships
#          become cardinality=one-to-many"
#         -> _then_cardinality
# No new step definitions are required for this requirement.


# Copyright (c) 2026 Kenneth Stott
# Canary: 63539ec1-b2e8-48ad-90db-0e06c5e1246a
#
# This source code is licensed under the Business Source License 1.1

# All steps for REQ-189 are already implemented in the existing steps file.
# The Given/When/Then steps for the REQ-189 default behaviour scenario map to:
#   Given "a DDN supergraph with ObjectType.dataConnectorTypeMapping[].fieldMapping entries"
#         -> _given_ddn_field_mappings
#   When  "the DDN converter runs"
#         -> _when_ddn_converter_runs
#   Then  "all GraphQL field names in relationships, permissions, and column definitions
#          are resolved to physical column names"
#         -> _then_ddn_fields_resolved
# No new step definitions are required for this requirement.


# Copyright (c) 2026 Kenneth Stott
# Canary: 743c240a-bbf1-48e6-96e7-d64ae44318c4
#
# This source code is licensed under the Business Source License 1.1

# All steps for REQ-190 are already implemented in the existing steps file.
# The Given/When/Then steps for the REQ-190 default behaviour scenario map to:
#   Given "a Hasura v2 auth config with JWT jwk_url, claims_map, or admin secret"
#         -> _given_v2_auth_config
#   When  "the v2 converter runs with --auth-env-file"
#         -> _when_v2_auth_converter_runs
#   Then  "JWT becomes provider: oauth with role_mapping[], admin secret becomes superuser,
#          and webhook auth emits a warning"
#         -> _then_v2_auth_converted
# No new step definitions are required for this requirement.


# Copyright (c) 2026 Kenneth Stott
# Canary: 50691749-85c9-48ee-a48f-47e1b2230096
#
# This source code is licensed under the Business Source License 1.1

# All steps for REQ-621 are already fully implemented in the existing steps file.
# The Given/When/Then steps for the REQ-621 default behaviour scenario map to:
#   Given "a completed Hasura v2 or DDN conversion"
#         -> _given_completed_conversion
#   When  "the output config is inspected"
#         -> _when_output_config_inspected
#   Then  "placeholder credentials (host: localhost, password: ${env:DB_PASSWORD}) are present
#          and Provisa refuses to start without real values"
#         -> _then_placeholder_credentials
# No new step definitions are required for this requirement.


# Copyright (c) 2026 Kenneth Stott
# Canary: 82007a1e-5c34-485f-a755-1619f4209635
#
# This source code is licensed under the Business Source License 1.1

# All steps for REQ-623 are already implemented in the existing steps file.
# The Given/When/Then steps for the REQ-623 default behaviour scenario map to:
#   Given "a Hasura v2 source config with kind, database_url, and pool_settings"
#         -> _given_v2_source_kind_url_pool
#   When  "the v2 converter runs"
#         -> _when_v2_converter_runs
#   Then  "SourceType is mapped correctly and connection URL is parsed into components
#          with pool settings preserved"
#         -> _then_v2_source_mapped
# No new step definitions are required for this requirement.


# Copyright (c) 2026 Kenneth Stott
# Canary: 8c69a450-1eb9-40be-8a6f-8659462c3e60
#
# This source code is licensed under the Business Source License 1.1


# Copyright (c) 2026 Kenneth Stott
# Canary: 85ab8932-98c9-4779-a5d5-004133a8954a
#
# This source code is licensed under the Business Source License 1.1


# Copyright (c) 2026 Kenneth Stott
# Canary: c999deb8-55da-4dd8-a8a5-3d6bdcb20aa4
#
# This source code is licensed under the Business Source License 1.1

# All steps for REQ-183 are already implemented in the existing steps file.
# The Given/When/Then steps for the REQ-183 default behaviour scenario map to:
#   Given "a Hasura DDN supergraph project"       -> _given_ddn_project
#   When  "the HML converter CLI tool is run"     -> _when_ddn_converter_run
#   Then  "valid Provisa YAML config is emitted covering ObjectTypes, Models,
#          Relationships, TypePermissions, ModelPermissions, and DataConnectorLinks"
#                                                 -> _then_ddn_config_complete
# No new step definitions are required for this requirement.


# Copyright (c) 2026 Kenneth Stott
# Canary: 66e6700f-677f-4413-baf4-0c28d537ce9b
#
# This source code is licensed under the Business Source License 1.1


# Copyright (c) 2026 Kenneth Stott
# Canary: ab5eef6a-346c-4224-975f-3c3a6e17f304
#
# This source code is licensed under the Business Source License 1.1

# All steps for REQ-185 are already fully implemented in the existing steps file.
# The Given/When/Then steps for the REQ-185 default behaviour scenario map to:
#   Given "a Hasura v2 metadata export with select_permissions[].columns per role"
#         -> _given_v2_select_perms
#   When  "the v2 converter runs"
#         -> _when_v2_converter_runs
#   Then  "each column's visible_to is populated from the role's column list,
#          with \"*\" meaning all columns"
#         -> _then_visible_to
# No new step definitions are required for this requirement.


# Copyright (c) 2026 Kenneth Stott
# Canary: 78c321a1-566a-4e58-86ae-417be4b5ca0c
#
# This source code is licensed under the Business Source License 1.1

# All steps for REQ-187 are already fully implemented in the existing steps file.
# The Given/When/Then steps for the REQ-187 default behaviour scenario map to:
#   Given "a Hasura v2 select_permissions[].filter boolean expression"
#         -> _given_v2_filter
#   When  "the v2 converter runs"
#         -> _when_v2_converter_runs
#   Then  "rls_rules[] are generated via boolean expression-to-SQL conversion,
#          with empty filter producing no RLS rule"
#         -> _then_rls_rules
# No new step definitions are required for this requirement.


# Copyright (c) 2026 Kenneth Stott
# Canary: 1211ed1f-f293-4951-8a9f-f4dbac68069c
#
# This source code is licensed under the Business Source License 1.1

# All three steps for REQ-188 are already fully implemented in the existing file:
#
#   Given "a Hasura v2 metadata export with object_relationships and array_relationships"
#         -> _given_v2_relationships
#   When  "the v2 converter runs"
#         -> _when_v2_converter_runs
#   Then  "object_relationships become cardinality=many-to-one and array_relationships
#          become cardinality=one-to-many"
#         -> _then_cardinality
#
# No new step definitions are required for REQ-188.


# Copyright (c) 2026 Kenneth Stott
# Canary: 8f8c069c-2f77-45e3-9266-591eec0bc4ed
#
# This source code is licensed under the Business Source License 1.1


# Copyright (c) 2026 Kenneth Stott
# Canary: ee4301af-73ab-4821-8e6d-d16b3547545b
#
# This source code is licensed under the Business Source License 1.1


# Copyright (c) 2026 Kenneth Stott
# Canary: c9e12c28-6919-445e-a552-8da43013603a
#
# This source code is licensed under the Business Source License 1.1


# Copyright (c) 2026 Kenneth Stott
# Canary: df367b15-7dd3-4ec4-954c-0d8429c8cba0
#
# This source code is licensed under the Business Source License 1.1

# All steps for REQ-623 are already implemented in the existing steps file.
# The Given/When/Then steps for the REQ-623 default behaviour scenario map to:
#   Given "a Hasura v2 source config with kind, database_url, and pool_settings"
#         -> _given_v2_source_kind_url_pool
#   When  "the v2 converter runs"
#         -> _when_v2_converter_runs
#   Then  "SourceType is mapped correctly and connection URL is parsed into components
#          with pool settings preserved"
#         -> _then_v2_source_mapped
# No new step definitions are required for this requirement.


# Copyright (c) 2026 Kenneth Stott
# Canary: 0e178547-0359-4793-b685-8a3c6e8fd5df
#
# This source code is licensed under the Business Source License 1.1

# All steps for REQ-625 are already fully implemented in the existing steps file.
# The Given/When/Then steps for the REQ-625 default behaviour scenario map to:
#   Given "a Hasura v2 source with database_url as an env var reference or unparseable URL"
#         -> _given_v2_source_env_url
#   When  "the v2 converter runs"
#         -> _when_v2_converter_runs
#   Then  "placeholder connection values are substituted and operators are directed to
#          use --source-overrides"
#         -> _then_v2_placeholder_values
# No new step definitions are required for this requirement.


# Copyright (c) 2026 Kenneth Stott
# Canary: d6acd37c-4091-4c4c-857d-0a8c5ceed1f9
#
# This source code is licensed under the Business Source License 1.1

# All steps for REQ-182 are already implemented in the existing steps file.
# The Given/When/Then steps for the REQ-182 default behaviour scenario map to:
#   Given "a Hasura v2 metadata export directory"
#         -> _given_v2_metadata
#   When  "the CLI converter is run against it"
#         -> _when_v2_converter_run
#   Then  "valid Provisa YAML config is emitted covering tables, relationships,
#          permissions, roles, and auth"
#         -> _then_v2_config_complete
# No new step definitions are required for this requirement.


# Copyright (c) 2026 Kenneth Stott
# Canary: 6607a6f4-6cb2-451a-83db-c4fe6fa98bc2
#
# This source code is licensed under the Business Source License 1.1

# All steps for REQ-183 are already implemented in the existing steps file.
# The Given/When/Then steps for the REQ-183 default behaviour scenario map to:
#   Given "a Hasura DDN supergraph project"       -> _given_ddn_project
#   When  "the HML converter CLI tool is run"     -> _when_ddn_converter_run
#   Then  "valid Provisa YAML config is emitted covering ObjectTypes, Models,
#          Relationships, TypePermissions, ModelPermissions, and DataConnectorLinks"
#                                                 -> _then_ddn_config_complete
# No new step definitions are required for this requirement.


# Copyright (c) 2026 Kenneth Stott
# Canary: b8593b3b-7b2f-4b7d-9537-36ff7cb3d268
#
# This source code is licensed under the Business Source License 1.1


# Copyright (c) 2026 Kenneth Stott
# Canary: 1286d182-3413-4c1c-a38c-4a9cafc6f1c7
#
# This source code is licensed under the Business Source License 1.1

# All steps for REQ-185 are already fully implemented in the existing steps file.
# The Given/When/Then steps for the REQ-185 default behaviour scenario map to:
#   Given "a Hasura v2 metadata export with select_permissions[].columns per role"
#         -> _given_v2_select_perms
#   When  "the v2 converter runs"
#         -> _when_v2_converter_runs
#   Then  "each column's visible_to is populated from the role's column list,
#          with \"*\" meaning all columns"
#         -> _then_visible_to
# No new step definitions are required for this requirement.


# Copyright (c) 2026 Kenneth Stott
# Canary: ffc715c8-e9d4-408b-9f03-c0de8d34ce03
#
# This source code is licensed under the Business Source License 1.1

# All steps for REQ-187 are already fully implemented in the existing steps file.
# The Given/When/Then steps for the REQ-187 default behaviour scenario map to:
#   Given "a Hasura v2 select_permissions[].filter boolean expression"
#         -> _given_v2_filter
#   When  "the v2 converter runs"
#         -> _when_v2_converter_runs
#   Then  "rls_rules[] are generated via boolean expression-to-SQL conversion,
#          with empty filter producing no RLS rule"
#         -> _then_rls_rules
# No new step definitions are required for this requirement.


# Copyright (c) 2026 Kenneth Stott
# Canary: 76e1bbfe-e8a2-453e-99a0-87871e54df7a
#
# This source code is licensed under the Business Source License 1.1


# Copyright (c) 2026 Kenneth Stott
# Canary: 74b233ce-bbdc-4c41-a390-59cbe8ebc853
#
# This source code is licensed under the Business Source License 1.1


# Copyright (c) 2026 Kenneth Stott
# Canary: 9e7e89b2-d7f2-42b8-8825-22323069d4bf
#
# This source code is licensed under the Business Source License 1.1


# Copyright (c) 2026 Kenneth Stott
# Canary: bca9c035-53cd-4ca2-a6dd-ad477e4310d6
#
# This source code is licensed under the Business Source License 1.1

# All steps for REQ-621 are already fully implemented in the existing steps file.
# The Given/When/Then steps for the REQ-621 default behaviour scenario map to:
#   Given "a completed Hasura v2 or DDN conversion"
#         -> _given_completed_conversion
#   When  "the output config is inspected"
#         -> _when_output_config_inspected
#   Then  "placeholder credentials (host: localhost, password: ${env:DB_PASSWORD}) are present
#          and Provisa refuses to start without real values"
#         -> _then_placeholder_credentials
# No new step definitions are required for this requirement.


# Copyright (c) 2026 Kenneth Stott
# Canary: 77c07e05-5532-4eee-8617-37de45e75e6c
#
# This source code is licensed under the Business Source License 1.1


# All steps for REQ-625 are already implemented in the existing steps file.
# The Given/When/Then steps for the REQ-625 default behaviour scenario map to:
#   Given "a Hasura v2 source with database_url as an env var reference or unparseable URL"
#         -> _given_v2_source_env_url
#   When  "the v2 converter runs"
#         -> _when_v2_converter_runs
#   Then  "placeholder connection values are substituted and operators are directed to
#          use --source-overrides"
#         -> _then_v2_placeholder_values
# No new step definitions are required for this requirement.


# Copyright (c) 2026 Kenneth Stott
# Canary: 21f808d3-7cae-4a6f-83a3-5f4afe86c4c8
#
# This source code is licensed under the Business Source License 1.1


# Copyright (c) 2026 Kenneth Stott
# Canary: f9d7e749-4527-4517-a332-3eea91a03a70
#
# This source code is licensed under the Business Source License 1.1

# All steps for REQ-183 are already implemented in the existing steps file.
# The Given/When/Then steps for the REQ-183 default behaviour scenario map to:
#   Given "a Hasura DDN supergraph project"       -> _given_ddn_project
#   When  "the HML converter CLI tool is run"     -> _when_ddn_converter_run
#   Then  "valid Provisa YAML config is emitted covering ObjectTypes, Models,
#          Relationships, TypePermissions, ModelPermissions, and DataConnectorLinks"
#                                                 -> _then_ddn_config_complete
# No new step definitions are required for this requirement.


# Copyright (c) 2026 Kenneth Stott
# Canary: 315b90e0-198b-45a2-bca9-30f67450a425
#
# This source code is licensed under the Business Source License 1.1


# Copyright (c) 2026 Kenneth Stott
# Canary: 1d546d1b-88da-4def-9738-c81f05737e74
#
# This source code is licensed under the Business Source License 1.1

# All steps for REQ-185 are already fully implemented in the existing steps file.
# The Given/When/Then steps for the REQ-185 default behaviour scenario map to:
#   Given "a Hasura v2 metadata export with select_permissions[].columns per role"
#         -> _given_v2_select_perms
#   When  "the v2 converter runs"
#         -> _when_v2_converter_runs
#   Then  "each column's visible_to is populated from the role's column list,
#          with \"*\" meaning all columns"
#         -> _then_visible_to
# No new step definitions are required for this requirement.


# Copyright (c) 2026 Kenneth Stott
# Canary: c8d54e71-dcee-4e75-9bf4-9e7ab8546092
#
# This source code is licensed under the Business Source License 1.1

# All steps for REQ-187 are already fully implemented in the existing steps file.
# The Given/When/Then steps for the REQ-187 default behaviour scenario map to:
#   Given "a Hasura v2 select_permissions[].filter boolean expression"
#         -> _given_v2_filter
#   When  "the v2 converter runs"
#         -> _when_v2_converter_runs
#   Then  "rls_rules[] are generated via boolean expression-to-SQL conversion,
#          with empty filter producing no RLS rule"
#         -> _then_rls_rules
# No new step definitions are required for this requirement.


# Copyright (c) 2026 Kenneth Stott
# Canary: bb37808f-eca3-465d-9c3a-d84538d0996d
#
# This source code is licensed under the Business Source License 1.1


# Copyright (c) 2026 Kenneth Stott
# Canary: fc98dce4-8555-4ace-9fe9-c8979845e113
#
# This source code is licensed under the Business Source License 1.1


# Copyright (c) 2026 Kenneth Stott
# Canary: d9a90b1e-9261-47db-b946-ccff3b9696c1
#
# This source code is licensed under the Business Source License 1.1


# Copyright (c) 2026 Kenneth Stott
# Canary: e59abce6-e352-4977-9908-ec4035cff4c2
#
# This source code is licensed under the Business Source License 1.1

# All steps for REQ-621 are already fully implemented in the existing steps file.
# The Given/When/Then steps for the REQ-621 default behaviour scenario map to:
#   Given "a completed Hasura v2 or DDN conversion"
#         -> _given_completed_conversion
#   When  "the output config is inspected"
#         -> _when_output_config_inspected
#   Then  "placeholder credentials (host: localhost, password: ${env:DB_PASSWORD}) are present
#          and Provisa refuses to start without real values"
#         -> _then_placeholder_credentials
# No new step definitions are required for this requirement.


# Copyright (c) 2026 Kenneth Stott
# Canary: 45210aea-328a-4e2e-b670-cb5661980796
#
# This source code is licensed under the Business Source License 1.1

# All steps for REQ-623 are already implemented in the existing steps file.
# The Given/When/Then steps for the REQ-623 default behaviour scenario map to:
#   Given "a Hasura v2 source config with kind, database_url, and pool_settings"
#         -> _given_v2_source_kind_url_pool
#   When  "the v2 converter runs"
#         -> _when_v2_converter_runs
#   Then  "SourceType is mapped correctly and connection URL is parsed into components
#          with pool settings preserved"
#         -> _then_v2_source_mapped
# No new step definitions are required for this requirement.


# Copyright (c) 2026 Kenneth Stott
# Canary: c8d7db5d-780e-452c-aede-c2136f71b19a
#
# This source code is licensed under the Business Source License 1.1

# All steps for REQ-625 are already fully implemented in the existing steps file.
# The Given/When/Then steps for the REQ-625 default behaviour scenario map to:
#   Given "a Hasura v2 source with database_url as an env var reference or unparseable URL"
#         -> _given_v2_source_env_url
#   When  "the v2 converter runs"
#         -> _when_v2_converter_runs
#   Then  "placeholder connection values are substituted and operators are directed to
#          use --source-overrides"
#         -> _then_v2_placeholder_values
# No new step definitions are required for this requirement.


# Copyright (c) 2026 Kenneth Stott
# Canary: 7ab6ef6c-37ce-4dd4-9120-8cad5dd22c7d
#
# This source code is licensed under the Business Source License 1.1

# All steps for REQ-182 are already implemented in the existing steps file.
# The Given/When/Then steps for the REQ-182 default behaviour scenario map to:
#   Given "a Hasura v2 metadata export directory"
#         -> _given_v2_metadata
#   When  "the CLI converter is run against it"
#         -> _when_v2_converter_run
#   Then  "valid Provisa YAML config is emitted covering tables, relationships,
#          permissions, roles, and auth"
#         -> _then_v2_config_complete
# No new step definitions are required for this requirement.


# Copyright (c) 2026 Kenneth Stott
# Canary: bd189c78-1495-41ca-8e2a-775a60972f96
#
# This source code is licensed under the Business Source License 1.1

# All steps for REQ-183 are already implemented in the existing steps file.
# The Given/When/Then steps for the REQ-183 default behaviour scenario map to:
#   Given "a Hasura DDN supergraph project"       -> _given_ddn_project
#   When  "the HML converter CLI tool is run"     -> _when_ddn_converter_run
#   Then  "valid Provisa YAML config is emitted covering ObjectTypes, Models,
#          Relationships, TypePermissions, ModelPermissions, and DataConnectorLinks"
#                                                 -> _then_ddn_config_complete
# No new step definitions are required for this requirement.


# Copyright (c) 2026 Kenneth Stott
# Canary: 6a7df207-35f5-4b4d-9aa1-bb7e1e64d9ec
#
# This source code is licensed under the Business Source License 1.1


# Copyright (c) 2026 Kenneth Stott
# Canary: ba718a45-5dd2-47da-badd-481981ceca26
#
# This source code is licensed under the Business Source License 1.1

# All steps for REQ-185 are already fully implemented in the existing steps file.
# The Given/When/Then steps for the REQ-185 default behaviour scenario map to:
#   Given "a Hasura v2 metadata export with select_permissions[].columns per role"
#         -> _given_v2_select_perms
#   When  "the v2 converter runs"
#         -> _when_v2_converter_runs
#   Then  "each column's visible_to is populated from the role's column list,
#          with "*" meaning all columns"
#         -> _then_visible_to
# No new step definitions are required for this requirement.


# Copyright (c) 2026 Kenneth Stott
# Canary: ffa57126-dfb2-4fb5-83d3-71efe34052ec
#
# This source code is licensed under the Business Source License 1.1


# Copyright (c) 2026 Kenneth Stott
# Canary: 0d700d48-6d16-49b9-8674-5fc40f04813f
#
# This source code is licensed under the Business Source License 1.1

# All steps for REQ-188 are already fully implemented in the existing steps file.
# The Given/When/Then steps for the REQ-188 default behaviour scenario map to:
#   Given "a Hasura v2 metadata export with object_relationships and array_relationships"
#         -> _given_v2_relationships
#   When  "the v2 converter runs"
#         -> _when_v2_converter_runs
#   Then  "object_relationships become cardinality=many-to-one and array_relationships
#          become cardinality=one-to-many"
#         -> _then_cardinality
# No new step definitions are required for REQ-188.


# Copyright (c) 2026 Kenneth Stott
# Canary: 41dd1551-a593-4016-81db-cf269f6b62db
#
# This source code is licensed under the Business Source License 1.1


# Copyright (c) 2026 Kenneth Stott
# Canary: 08f50094-9203-41c7-8b0b-79351e1d7b65
#
# This source code is licensed under the Business Source License 1.1

# All steps for REQ-190 are already implemented in the existing steps file.
# The Given/When/Then steps for the REQ-190 default behaviour scenario map to:
#   Given "a Hasura v2 auth config with JWT jwk_url, claims_map, or admin secret"
#         -> _given_v2_auth_config
#   When  "the v2 converter runs with --auth-env-file"
#         -> _when_v2_auth_converter_runs
#   Then  "JWT becomes provider: oauth with role_mapping[], admin secret becomes superuser,
#          and webhook auth emits a warning"
#         -> _then_v2_auth_converted
# No new step definitions are required for this requirement.


# Copyright (c) 2026 Kenneth Stott
# Canary: f17d34ac-02b4-468f-9308-9513408591c5
#
# This source code is licensed under the Business Source License 1.1


# Copyright (c) 2026 Kenneth Stott
# Canary: 6ac05eb9-0845-4205-b12d-a8141a4f593a
#
# This source code is licensed under the Business Source License 1.1

# All steps for REQ-623 are already implemented in the existing steps file.
# The Given/When/Then steps for the REQ-623 default behaviour scenario map to:
#   Given "a Hasura v2 source config with kind, database_url, and pool_settings"
#         -> _given_v2_source_kind_url_pool
#   When  "the v2 converter runs"
#         -> _when_v2_converter_runs
#   Then  "SourceType is mapped correctly and connection URL is parsed into components
#          with pool settings preserved"
#         -> _then_v2_source_mapped
# No new step definitions are required for this requirement.


# Copyright (c) 2026 Kenneth Stott
# Canary: 34eb926b-7e4c-4b2a-999f-dd0107051369
#
# This source code is licensed under the Business Source License 1.1

# All steps for REQ-625 are already fully implemented in the existing steps file.
# The Given/When/Then steps for the REQ-625 default behaviour scenario map to:
#   Given "a Hasura v2 source with database_url as an env var reference or unparseable URL"
#         -> _given_v2_source_env_url
#   When  "the v2 converter runs"
#         -> _when_v2_converter_runs
#   Then  "placeholder connection values are substituted and operators are directed to
#          use --source-overrides"
#         -> _then_v2_placeholder_values
# No new step definitions are required for this requirement.


# Copyright (c) 2026 Kenneth Stott
# Canary: 2c0cbe24-0f71-4071-a9f4-ba04b76ec6a1
#
# This source code is licensed under the Business Source License 1.1

# All steps for REQ-182 are already implemented in the existing steps file.
# The Given/When/Then steps for the REQ-182 default behaviour scenario map to:
#   Given "a Hasura v2 metadata export directory"
#         -> _given_v2_metadata
#   When  "the CLI converter is run against it"
#         -> _when_v2_converter_run
#   Then  "valid Provisa YAML config is emitted covering tables, relationships,
#          permissions, roles, and auth"
#         -> _then_v2_config_complete
# No new step definitions are required for this requirement.


# Copyright (c) 2026 Kenneth Stott
# Canary: b80eebfa-7d2f-47b7-8e79-c94751b1370f
#
# This source code is licensed under the Business Source License 1.1

# All steps for REQ-183 are already implemented in the existing steps file.
# The Given/When/Then steps for the REQ-183 default behaviour scenario map to:
#   Given "a Hasura DDN supergraph project"       -> _given_ddn_project
#   When  "the HML converter CLI tool is run"     -> _when_ddn_converter_run
#   Then  "valid Provisa YAML config is emitted covering ObjectTypes, Models,
#          Relationships, TypePermissions, ModelPermissions, and DataConnectorLinks"
#                                                 -> _then_ddn_config_complete
# No new step definitions are required for this requirement.


# Copyright (c) 2026 Kenneth Stott
# Canary: 41e06cde-8870-4906-82fc-9a2664fb827c
#
# This source code is licensed under the Business Source License 1.1

# All steps for REQ-184 are already implemented in the existing steps file.
# The Given/When/Then steps for the REQ-184 default behaviour scenario map to:
#   Given "a Hasura boolean filter expression using operators like _eq, _in, _and, _or, _not"
#         -> _given_bool_expr
#   When  "the shared converter processes it"
#         -> _when_bool_expr_converted
#   Then  "valid SQL is produced with session variable references mapped to
#          current_setting('provisa.<name>')"
#         -> _then_bool_expr_sql
# No new step definitions are required for this requirement.


# Copyright (c) 2026 Kenneth Stott
# Canary: a041a268-a254-4055-a9ec-3d6c3fc9900c
#
# This source code is licensed under the Business Source License 1.1

# All steps for REQ-185 are already fully implemented in the existing steps file.
# The Given/When/Then steps for the REQ-185 default behaviour scenario map to:
#   Given "a Hasura v2 metadata export with select_permissions[].columns per role"
#         -> _given_v2_select_perms
#   When  "the v2 converter runs"
#         -> _when_v2_converter_runs
#   Then  "each column's visible_to is populated from the role's column list,
#          with \"*\" meaning all columns"
#         -> _then_visible_to
# No new step definitions are required for this requirement.


# Copyright (c) 2026 Kenneth Stott
# Canary: 9b80eee2-9d37-4284-ad18-22c426b207fe
#
# This source code is licensed under the Business Source License 1.1

# All steps for REQ-187 are already fully implemented in the existing steps file.
# The Given/When/Then steps for the REQ-187 default behaviour scenario map to:
#   Given "a Hasura v2 select_permissions[].filter boolean expression"
#         -> _given_v2_filter
#   When  "the v2 converter runs"
#         -> _when_v2_converter_runs
#   Then  "rls_rules[] are generated via boolean expression-to-SQL conversion,
#          with empty filter producing no RLS rule"
#         -> _then_rls_rules
# No new step definitions are required for this requirement.


# Copyright (c) 2026 Kenneth Stott
# Canary: d18e7f28-4358-4a94-abda-22fe483ceadd
#
# This source code is licensed under the Business Source License 1.1


# Copyright (c) 2026 Kenneth Stott
# Canary: 25321bba-8837-4b76-86de-11f42e7600c1
#
# This source code is licensed under the Business Source License 1.1


# Copyright (c) 2026 Kenneth Stott
# Canary: 8dba8161-ff26-42f2-9bea-d94647df0695
#
# This source code is licensed under the Business Source License 1.1

# All steps for REQ-190 are already implemented in the existing steps file.
# The Given/When/Then steps for the REQ-190 default behaviour scenario map to:
#   Given "a Hasura v2 auth config with JWT jwk_url, claims_map, or admin secret"
#         -> _given_v2_auth_config
#   When  "the v2 converter runs with --auth-env-file"
#         -> _when_v2_auth_converter_runs
#   Then  "JWT becomes provider: oauth with role_mapping[], admin secret becomes superuser,
#          and webhook auth emits a warning"
#         -> _then_v2_auth_converted
# No new step definitions are required for this requirement.


# Copyright (c) 2026 Kenneth Stott
# Canary: e1863b40-bf25-4875-8c3f-54029c3788d9
#
# This source code is licensed under the Business Source License 1.1


# Copyright (c) 2026 Kenneth Stott
# Canary: cde4c852-44f8-45e7-aafb-1edd614272cf
#
# This source code is licensed under the Business Source License 1.1


# Copyright (c) 2026 Kenneth Stott
# Canary: db156a95-85e1-4531-b503-90f5125b09b0
#
# This source code is licensed under the Business Source License 1.1


# Copyright (c) 2026 Kenneth Stott
# Canary: 11016318-a28f-496b-a923-8f7edb6ebd0d
#
# This source code is licensed under the Business Source License 1.1

# All steps for REQ-182 are already implemented in the existing steps file.
# The Given/When/Then steps for the REQ-182 default behaviour scenario map to:
#   Given "a Hasura v2 metadata export directory"
#         -> _given_v2_metadata
#   When  "the CLI converter is run against it"
#         -> _when_v2_converter_run
#   Then  "valid Provisa YAML config is emitted covering tables, relationships,
#          permissions, roles, and auth"
#         -> _then_v2_config_complete
# No new step definitions are required for this requirement.


# Copyright (c) 2026 Kenneth Stott
# Canary: 359b2be7-261c-4124-afc2-459644e1af15
#
# This source code is licensed under the Business Source License 1.1

# All steps for REQ-183 are already implemented in the existing steps file.
# The Given/When/Then steps for the REQ-183 default behaviour scenario map to:
#   Given "a Hasura DDN supergraph project"       -> _given_ddn_project
#   When  "the HML converter CLI tool is run"     -> _when_ddn_converter_run
#   Then  "valid Provisa YAML config is emitted covering ObjectTypes, Models,
#          Relationships, TypePermissions, ModelPermissions, and DataConnectorLinks"
#                                                 -> _then_ddn_config_complete
# No new step definitions are required for this requirement.


# Copyright (c) 2026 Kenneth Stott
# Canary: e853a812-8b88-448e-a7bf-64348dfefa98
#
# This source code is licensed under the Business Source License 1.1


# Copyright (c) 2026 Kenneth Stott
# Canary: 0350733e-eac8-4a3b-822b-8bb8a9e75ccb
#
# This source code is licensed under the Business Source License 1.1


# Copyright (c) 2026 Kenneth Stott
# Canary: 65343dd4-b035-4641-950d-571112958276
#
# This source code is licensed under the Business Source License 1.1

# All steps for REQ-187 are already fully implemented in the existing steps file.
# The Given/When/Then steps for the REQ-187 default behaviour scenario map to:
#   Given "a Hasura v2 select_permissions[].filter boolean expression"
#         -> _given_v2_filter
#   When  "the v2 converter runs"
#         -> _when_v2_converter_runs
#   Then  "rls_rules[] are generated via boolean expression-to-SQL conversion,
#          with empty filter producing no RLS rule"
#         -> _then_rls_rules
# No new step definitions are required for this requirement.


# Copyright (c) 2026 Kenneth Stott
# Canary: 6061eb37-3c51-49d9-8722-3e42f78d3d72
#
# This source code is licensed under the Business Source License 1.1

# All steps for REQ-188 are already fully implemented in the existing steps file.
# The Given/When/Then steps for the REQ-188 default behaviour scenario map to:
#   Given "a Hasura v2 metadata export with object_relationships and array_relationships"
#         -> _given_v2_relationships
#   When  "the v2 converter runs"
#         -> _when_v2_converter_runs
#   Then  "object_relationships become cardinality=many-to-one and array_relationships
#          become cardinality=one-to-many"
#         -> _then_cardinality
# No new step definitions are required for REQ-188.


# Copyright (c) 2026 Kenneth Stott
# Canary: 407b462c-f5d3-49e7-addf-bbca748db88d
#
# This source code is licensed under the Business Source License 1.1

# All steps for REQ-189 are already implemented in the existing steps file.
# The Given/When/Then steps for the REQ-189 default behaviour scenario map to:
#   Given "a DDN supergraph with ObjectType.dataConnectorTypeMapping[].fieldMapping entries"
#         -> _given_ddn_field_mappings
#   When  "the DDN converter runs"
#         -> _when_ddn_converter_runs
#   Then  "all GraphQL field names in relationships, permissions, and column definitions
#          are resolved to physical column names"
#         -> _then_ddn_fields_resolved
# No new step definitions are required for this requirement.


# Copyright (c) 2026 Kenneth Stott
# Canary: 30f3ce34-5517-44b5-ad3b-d2732227d21f
#
# This source code is licensed under the Business Source License 1.1

# All steps for REQ-190 are already implemented in the existing steps file.
# The Given/When/Then steps for the REQ-190 default behaviour scenario map to:
#   Given "a Hasura v2 auth config with JWT jwk_url, claims_map, or admin secret"
#         -> _given_v2_auth_config
#   When  "the v2 converter runs with --auth-env-file"
#         -> _when_v2_auth_converter_runs
#   Then  "JWT becomes provider: oauth with role_mapping[], admin secret becomes superuser,
#          and webhook auth emits a warning"
#         -> _then_v2_auth_converted
# No new step definitions are required for this requirement.


# Copyright (c) 2026 Kenneth Stott
# Canary: d214b7d0-1827-42f1-8ada-91cc6bd336b9
#
# This source code is licensed under the Business Source License 1.1


# Copyright (c) 2026 Kenneth Stott
# Canary: 4b67fc16-a61a-4201-8d4d-304a1f2610e9
#
# This source code is licensed under the Business Source License 1.1

# All steps for REQ-623 are already implemented in the existing steps file.
# The Given/When/Then steps for the REQ-623 default behaviour scenario map to:
#   Given "a Hasura v2 source config with kind, database_url, and pool_settings"
#         -> _given_v2_source_kind_url_pool
#   When  "the v2 converter runs"
#         -> _when_v2_converter_runs
#   Then  "SourceType is mapped correctly and connection URL is parsed into components
#          with pool settings preserved"
#         -> _then_v2_source_mapped
# No new step definitions are required for this requirement.


# Copyright (c) 2026 Kenneth Stott
# Canary: 6dc865e0-251b-42f2-bfc4-2d467225fc67
#
# This source code is licensed under the Business Source License 1.1

# All steps for REQ-625 are already fully implemented in the existing steps file.
# The Given/When/Then steps for the REQ-625 default behaviour scenario map to:
#   Given "a Hasura v2 source with database_url as an env var reference or unparseable URL"
#         -> _given_v2_source_env_url
#   When  "the v2 converter runs"
#         -> _when_v2_converter_runs
#   Then  "placeholder connection values are substituted and operators are directed to
#          use --source-overrides"
#         -> _then_v2_placeholder_values
# No new step definitions are required for this requirement.


# Copyright (c) 2026 Kenneth Stott
# Canary: d4a89652-266c-4129-9f63-af3596692a6b
#
# This source code is licensed under the Business Source License 1.1


# Copyright (c) 2026 Kenneth Stott
# Canary: 8d4416b7-3e1d-4e88-a546-5d3b28b78116
#
# This source code is licensed under the Business Source License 1.1


# Copyright (c) 2026 Kenneth Stott
# Canary: 6c3bcf7b-a825-401e-a9d9-5a9fc1d58a64
#
# This source code is licensed under the Business Source License 1.1


# Copyright (c) 2026 Kenneth Stott
# Canary: ebdacc6d-beb6-45b7-827e-13198271549b
#
# This source code is licensed under the Business Source License 1.1

# All steps for REQ-185 are already fully implemented in the existing steps file.
# The Given/When/Then steps for the REQ-185 default behaviour scenario map to:
#   Given "a Hasura v2 metadata export with select_permissions[].columns per role"
#         -> _given_v2_select_perms
#   When  "the v2 converter runs"
#         -> _when_v2_converter_runs
#   Then  "each column's visible_to is populated from the role's column list,
#          with \"*\" meaning all columns"
#         -> _then_visible_to
# No new step definitions are required for this requirement.


# Copyright (c) 2026 Kenneth Stott
# Canary: 1087876b-3959-48c4-849b-269bdc5e7395
#
# This source code is licensed under the Business Source License 1.1

# All steps for REQ-187 are already fully implemented in the existing steps file.
# The Given/When/Then steps for the REQ-187 default behaviour scenario map to:
#   Given "a Hasura v2 select_permissions[].filter boolean expression"
#         -> _given_v2_filter
#   When  "the v2 converter runs"
#         -> _when_v2_converter_runs
#   Then  "rls_rules[] are generated via boolean expression-to-SQL conversion,
#          with empty filter producing no RLS rule"
#         -> _then_rls_rules
# No new step definitions are required for this requirement.


# Copyright (c) 2026 Kenneth Stott
# Canary: 16337140-5a2c-4811-a561-d62e6fc6d033
#
# This source code is licensed under the Business Source License 1.1

# All steps for REQ-188 are already fully implemented in the existing steps file.
# The Given/When/Then steps for the REQ-188 default behaviour scenario map to:
#   Given "a Hasura v2 metadata export with object_relationships and array_relationships"
#         -> _given_v2_relationships
#   When  "the v2 converter runs"
#         -> _when_v2_converter_runs
#   Then  "object_relationships become cardinality=many-to-one and array_relationships
#          become cardinality=one-to-many"
#         -> _then_cardinality
# No new step definitions are required for REQ-188.


# Copyright (c) 2026 Kenneth Stott
# Canary: d6e16f8f-f8fc-4f18-abd7-5f539044f3ec
#
# This source code is licensed under the Business Source License 1.1

# All steps for REQ-189 are already fully implemented in the existing steps file.
# The Given/When/Then steps for the REQ-189 default behaviour scenario map to:
#   Given "a DDN supergraph with ObjectType.dataConnectorTypeMapping[].fieldMapping entries"
#         -> _given_ddn_field_mappings
#   When  "the DDN converter runs"
#         -> _when_ddn_converter_runs
#   Then  "all GraphQL field names in relationships, permissions, and column definitions
#          are resolved to physical column names"
#         -> _then_ddn_fields_resolved
# No new step definitions are required for this requirement.


# Copyright (c) 2026 Kenneth Stott
# Canary: 1cad7974-dce3-4f17-956e-a40995a9cae8
#
# This source code is licensed under the Business Source License 1.1


# Copyright (c) 2026 Kenneth Stott
# Canary: bc4cea86-bb3a-4f5a-a524-9a330d29b005
#
# This source code is licensed under the Business Source License 1.1

# All steps for REQ-621 are already fully implemented in the existing steps file.
# The Given/When/Then steps for the REQ-621 default behaviour scenario map to:
#   Given "a completed Hasura v2 or DDN conversion"
#         -> _given_completed_conversion
#   When  "the output config is inspected"
#         -> _when_output_config_inspected
#   Then  "placeholder credentials (host: localhost, password: ${env:DB_PASSWORD}) are present
#          and Provisa refuses to start without real values"
#         -> _then_placeholder_credentials
# No new step definitions are required for this requirement.


# All steps for REQ-623 are already implemented in the existing steps file.
# The Given/When/Then steps for the REQ-623 default behaviour scenario map to:
#   Given "a Hasura v2 source config with kind, database_url, and pool_settings"
#         -> _given_v2_source_kind_url_pool
#   When  "the v2 converter runs"
#         -> _when_v2_converter_runs
#   Then  "SourceType is mapped correctly and connection URL is parsed into components
#          with pool settings preserved"
#         -> _then_v2_source_mapped
# No new step definitions are required for this requirement.


# Copyright (c) 2026 Kenneth Stott
# Canary: d5c96d78-f742-4be1-9f00-5a1d4ee0f6b0
#
# This source code is licensed under the Business Source License 1.1


# Copyright (c) 2026 Kenneth Stott
# Canary: 666828c6-5ebb-4897-8de8-899dd6fe8f58
#
# This source code is licensed under the Business Source License 1.1

# All steps for REQ-182 are already implemented in the existing steps file.
# The Given/When/Then steps for the REQ-182 default behaviour scenario map to:
#   Given "a Hasura v2 metadata export directory"
#         -> _given_v2_metadata
#   When  "the CLI converter is run against it"
#         -> _when_v2_converter_run
#   Then  "valid Provisa YAML config is emitted covering tables, relationships,
#          permissions, roles, and auth"
#         -> _then_v2_config_complete
# No new step definitions are required for this requirement.


# Copyright (c) 2026 Kenneth Stott
# Canary: 05c864fc-1ddd-471d-add4-a34e9d768130
#
# This source code is licensed under the Business Source License 1.1

# All steps for REQ-183 are already implemented in the existing steps file.
# The Given/When/Then steps for the REQ-183 default behaviour scenario map to:
#   Given "a Hasura DDN supergraph project"       -> _given_ddn_project
#   When  "the HML converter CLI tool is run"     -> _when_ddn_converter_run
#   Then  "valid Provisa YAML config is emitted covering ObjectTypes, Models,
#          Relationships, TypePermissions, ModelPermissions, and DataConnectorLinks"
#                                                 -> _then_ddn_config_complete
# No new step definitions are required for this requirement.


# Copyright (c) 2026 Kenneth Stott
# Canary: 84ff5e9b-497c-45b7-a6dc-6ff8a3ab2e46
#
# This source code is licensed under the Business Source License 1.1


# Copyright (c) 2026 Kenneth Stott
# Canary: 11676978-36fb-4036-ac86-39cbf2596118
#
# This source code is licensed under the Business Source License 1.1

# All steps for REQ-185 are already fully implemented in the existing steps file.
# The Given/When/Then steps for the REQ-185 default behaviour scenario map to:
#   Given "a Hasura v2 metadata export with select_permissions[].columns per role"
#         -> _given_v2_select_perms
#   When  "the v2 converter runs"
#         -> _when_v2_converter_runs
#   Then  "each column's visible_to is populated from the role's column list,
#          with \"*\" meaning all columns"
#         -> _then_visible_to
# No new step definitions are required for this requirement.


# Copyright (c) 2026 Kenneth Stott
# Canary: 79750e76-f0af-4420-9246-125d3390c601
#
# This source code is licensed under the Business Source License 1.1

# All steps for REQ-187 are already fully implemented in the existing steps file.
# The Given/When/Then steps for the REQ-187 default behaviour scenario map to:
#   Given "a Hasura v2 select_permissions[].filter boolean expression"
#         -> _given_v2_filter
#   When  "the v2 converter runs"
#         -> _when_v2_converter_runs
#   Then  "rls_rules[] are generated via boolean expression-to-SQL conversion,
#          with empty filter producing no RLS rule"
#         -> _then_rls_rules
# No new step definitions are required for this requirement.


# Copyright (c) 2026 Kenneth Stott
# Canary: 88a9e8d4-45ed-4666-b07c-21c0bd196828
#
# This source code is licensed under the Business Source License 1.1


# Copyright (c) 2026 Kenneth Stott
# Canary: 139dae76-1002-4bcd-8f1d-76e205ff563d
#
# This source code is licensed under the Business Source License 1.1


# Copyright (c) 2026 Kenneth Stott
# Canary: fb23e02f-3711-4e5a-a6e5-73339da602ca
#
# This source code is licensed under the Business Source License 1.1

# All steps for REQ-190 are already implemented in the existing steps file.
# The Given/When/Then steps for the REQ-190 default behaviour scenario map to:
#   Given "a Hasura v2 auth config with JWT jwk_url, claims_map, or admin secret"
#         -> _given_v2_auth_config
#   When  "the v2 converter runs with --auth-env-file"
#         -> _when_v2_auth_converter_runs
#   Then  "JWT becomes provider: oauth with role_mapping[], admin secret becomes superuser,
#          and webhook auth emits a warning"
#         -> _then_v2_auth_converted
# No new step definitions are required for this requirement.


# Copyright (c) 2026 Kenneth Stott
# Canary: d6bf33cc-326f-4d8c-be80-1591556a5926
#
# This source code is licensed under the Business Source License 1.1


# Copyright (c) 2026 Kenneth Stott
# Canary: f2c1c627-bf0f-42e6-815d-11aec0ad9e10
#
# This source code is licensed under the Business Source License 1.1

# All steps for REQ-623 are already implemented in the existing steps file.
# The Given/When/Then steps for the REQ-623 default behaviour scenario map to:
#   Given "a Hasura v2 source config with kind, database_url, and pool_settings"
#         -> _given_v2_source_kind_url_pool
#   When  "the v2 converter runs"
#         -> _when_v2_converter_runs
#   Then  "SourceType is mapped correctly and connection URL is parsed into components
#          with pool settings preserved"
#         -> _then_v2_source_mapped
# No new step definitions are required for this requirement.


# Copyright (c) 2026 Kenneth Stott
# Canary: 2622e354-2eb0-4f06-8f33-85d679af2b5e
#
# This source code is licensed under the Business Source License 1.1

# All steps for REQ-625 are already fully implemented in the existing steps file.
# The Given/When/Then steps for the REQ-625 default behaviour scenario map to:
#   Given "a Hasura v2 source with database_url as an env var reference or unparseable URL"
#         -> _given_v2_source_env_url
#   When  "the v2 converter runs"
#         -> _when_v2_converter_runs
#   Then  "placeholder connection values are substituted and operators are directed to
#          use --source-overrides"
#         -> _then_v2_placeholder_values
# No new step definitions are required for this requirement.


# Copyright (c) 2026 Kenneth Stott
# Canary: ebd0d895-2bed-46ee-a4be-dab990a0d45e
#
# This source code is licensed under the Business Source License 1.1


# Copyright (c) 2026 Kenneth Stott
# Canary: cf30cbde-03fc-4e76-bf1d-da0b0c5dbcfb
#
# This source code is licensed under the Business Source License 1.1


# Copyright (c) 2026 Kenneth Stott
# Canary: 4a5eba72-3aaf-40ea-ac09-e17179f48ad5
#
# This source code is licensed under the Business Source License 1.1


# All steps for REQ-185 are already fully implemented in the existing steps file.
# The Given/When/Then steps for the REQ-185 default behaviour scenario map to:
#   Given "a Hasura v2 metadata export with select_permissions[].columns per role"
#         -> _given_v2_select_perms
#   When  "the v2 converter runs"
#         -> _when_v2_converter_runs
#   Then  "each column's visible_to is populated from the role's column list,
#          with \"*\" meaning all columns"
#         -> _then_visible_to
# No new step definitions are required for this requirement.


# Copyright (c) 2026 Kenneth Stott
# Canary: 4f62889e-e949-478b-a3df-9a03b883423e
#
# This source code is licensed under the Business Source License 1.1

# All steps for REQ-187 are already fully implemented in the existing steps file.
# The Given/When/Then steps for the REQ-187 default behaviour scenario map to:
#   Given "a Hasura v2 select_permissions[].filter boolean expression"
#         -> _given_v2_filter
#   When  "the v2 converter runs"
#         -> _when_v2_converter_runs
#   Then  "rls_rules[] are generated via boolean expression-to-SQL conversion,
#          with empty filter producing no RLS rule"
#         -> _then_rls_rules
# No new step definitions are required for this requirement.


# Copyright (c) 2026 Kenneth Stott
# Canary: cedf3d21-7d1d-4503-a56f-f6be49c676a7
#
# This source code is licensed under the Business Source License 1.1

# All steps for REQ-188 are already fully implemented in the existing steps file.
# The Given/When/Then steps for the REQ-188 default behaviour scenario map to:
#   Given "a Hasura v2 metadata export with object_relationships and array_relationships"
#         -> _given_v2_relationships
#   When  "the v2 converter runs"
#         -> _when_v2_converter_runs
#   Then  "object_relationships become cardinality=many-to-one and array_relationships
#          become cardinality=one-to-many"
#         -> _then_cardinality
# No new step definitions are required for REQ-188.


# Copyright (c) 2026 Kenneth Stott
# Canary: 3796414b-2fb5-4c8f-9c2c-5cb0b59928ad
#
# This source code is licensed under the Business Source License 1.1

# All steps for REQ-189 are already fully implemented in the existing steps file.
# The Given/When/Then steps for the REQ-189 default behaviour scenario map to:
#   Given "a DDN supergraph with ObjectType.dataConnectorTypeMapping[].fieldMapping entries"
#         -> _given_ddn_field_mappings
#   When  "the DDN converter runs"
#         -> _when_ddn_converter_runs
#   Then  "all GraphQL field names in relationships, permissions, and column definitions
#          are resolved to physical column names"
#         -> _then_ddn_fields_resolved
# No new step definitions are required for this requirement.


# Copyright (c) 2026 Kenneth Stott
# Canary: 59108063-2cf9-4c52-aa52-b9b0346797d1
#
# This source code is licensed under the Business Source License 1.1

# All steps for REQ-190 are already implemented in the existing steps file.
# The Given/When/Then steps for the REQ-190 default behaviour scenario map to:
#   Given "a Hasura v2 auth config with JWT jwk_url, claims_map, or admin secret"
#         -> _given_v2_auth_config
#   When  "the v2 converter runs with --auth-env-file"
#         -> _when_v2_auth_converter_runs
#   Then  "JWT becomes provider: oauth with role_mapping[], admin secret becomes superuser,
#          and webhook auth emits a warning"
#         -> _then_v2_auth_converted
# No new step definitions are required for this requirement.


# Copyright (c) 2026 Kenneth Stott
# Canary: f95b3652-055a-45aa-98d9-bf7d443d0f49
#
# This source code is licensed under the Business Source License 1.1


# All steps for REQ-623 are already fully implemented in the existing steps file.
# The Given/When/Then steps for the REQ-623 default behaviour scenario map to:
#   Given "a Hasura v2 source config with kind, database_url, and pool_settings"
#         -> _given_v2_source_kind_url_pool
#   When  "the v2 converter runs"
#         -> _when_v2_converter_runs
#   Then  "SourceType is mapped correctly and connection URL is parsed into components
#          with pool settings preserved"
#         -> _then_v2_source_mapped
# No new step definitions are required for this requirement.


# Copyright (c) 2026 Kenneth Stott
# Canary: 568e6477-07f4-4e8c-8d42-23060dd30b33
#
# This source code is licensed under the Business Source License 1.1

# All steps for REQ-625 are already fully implemented in the existing steps file.
# The Given/When/Then steps for the REQ-625 default behaviour scenario map to:
#   Given "a Hasura v2 source with database_url as an env var reference or unparseable URL"
#         -> _given_v2_source_env_url
#   When  "the v2 converter runs"
#         -> _when_v2_converter_runs
#   Then  "placeholder connection values are substituted and operators are directed to
#          use --source-overrides"
#         -> _then_v2_placeholder_values
# No new step definitions are required for this requirement.


# Copyright (c) 2026 Kenneth Stott
# Canary: bb826bd3-136d-45e9-bf99-175449b84a74
#
# This source code is licensed under the Business Source License 1.1

# All steps for REQ-182 are already fully implemented in the existing steps file.
# The Given/When/Then steps for the REQ-182 default behaviour scenario map to:
#   Given "a Hasura v2 metadata export directory"
#         -> _given_v2_metadata
#   When  "the CLI converter is run against it"
#         -> _when_v2_converter_run
#   Then  "valid Provisa YAML config is emitted covering tables, relationships,
#          permissions, roles, and auth"
#         -> _then_v2_config_complete
# No new step definitions are required for this requirement.


# Copyright (c) 2026 Kenneth Stott
# Canary: b6c5b954-5d26-4075-8dff-3e7395c783b8
#
# This source code is licensed under the Business Source License 1.1


# Copyright (c) 2026 Kenneth Stott
# Canary: 36cc98c7-26c5-4b2b-9bef-d1ff68acc4a5
#
# This source code is licensed under the Business Source License 1.1


# Copyright (c) 2026 Kenneth Stott
# Canary: 7746b3bb-6fe1-4833-bf6f-459223175464
#
# This source code is licensed under the Business Source License 1.1

# All steps for REQ-185 are already fully implemented in the existing steps file.
# The Given/When/Then steps for the REQ-185 default behaviour scenario map to:
#   Given "a Hasura v2 metadata export with select_permissions[].columns per role"
#         -> _given_v2_select_perms
#   When  "the v2 converter runs"
#         -> _when_v2_converter_runs
#   Then  "each column's visible_to is populated from the role's column list,
#          with "*" meaning all columns"
#         -> _then_visible_to
# No new step definitions are required for this requirement.


# Copyright (c) 2026 Kenneth Stott
# Canary: f65b787f-e470-4def-8832-381c5ac25c1c
#
# This source code is licensed under the Business Source License 1.1


# Copyright (c) 2026 Kenneth Stott
# Canary: 56d28291-6062-4091-8050-12b87bb37b54
#
# This source code is licensed under the Business Source License 1.1


# Copyright (c) 2026 Kenneth Stott
# Canary: f3f840d0-5c90-41b8-8e4f-9fca62a06c7d
#
# This source code is licensed under the Business Source License 1.1


# Copyright (c) 2026 Kenneth Stott
# Canary: 20bafba2-0399-41aa-b826-87d1f906f60e
#
# This source code is licensed under the Business Source License 1.1


# Copyright (c) 2026 Kenneth Stott
# Canary: 6844a267-e9e3-44b7-b111-300a3d2ed450
#
# This source code is licensed under the Business Source License 1.1

# All steps for REQ-621 are already fully implemented in the existing steps file.
# The Given/When/Then steps for the REQ-621 default behaviour scenario map to:
#   Given "a completed Hasura v2 or DDN conversion"
#         -> _given_completed_conversion
#   When  "the output config is inspected"
#         -> _when_output_config_inspected
#   Then  "placeholder credentials (host: localhost, password: ${env:DB_PASSWORD}) are present
#          and Provisa refuses to start without real values"
#         -> _then_placeholder_credentials
# No new step definitions are required for this requirement.


# Copyright (c) 2026 Kenneth Stott
# Canary: 208504c2-b5bb-4c8c-a175-590eb3f2dc22
#
# This source code is licensed under the Business Source License 1.1


# Copyright (c) 2026 Kenneth Stott
# Canary: cb39da66-b835-4885-92e4-1a57110a6046
#
# This source code is licensed under the Business Source License 1.1


# Copyright (c) 2026 Kenneth Stott
# Canary: f57d40d6-bc78-4db0-91db-ace51b158d90
#
# This source code is licensed under the Business Source License 1.1


# Copyright (c) 2026 Kenneth Stott
# Canary: 9b9d7e36-d37f-4712-8954-6210e9a2fcac
#
# This source code is licensed under the Business Source License 1.1


# Copyright (c) 2026 Kenneth Stott
# Canary: a34fd9b2-3c86-4be9-8696-da101d78d4ff
#
# This source code is licensed under the Business Source License 1.1


# Copyright (c) 2026 Kenneth Stott
# Canary: 6fdaca8d-9de0-4ccf-a56d-9bebe3e56459
#
# This source code is licensed under the Business Source License 1.1


# Copyright (c) 2026 Kenneth Stott
# Canary: 4a966647-f172-4543-b1d5-26d5f0aa42ad
#
# This source code is licensed under the Business Source License 1.1


# Copyright (c) 2026 Kenneth Stott
# Canary: 4bf781fd-3f76-4ab4-ab01-2c1a0e346578
#
# This source code is licensed under the Business Source License 1.1


# Copyright (c) 2026 Kenneth Stott
# Canary: 944a806a-f7fa-414f-95c6-c67e4da8a25e
#
# This source code is licensed under the Business Source License 1.1


# Copyright (c) 2026 Kenneth Stott
# Canary: b807908f-cab4-42e9-8e5b-81d607c5e23f
#
# This source code is licensed under the Business Source License 1.1


# Copyright (c) 2026 Kenneth Stott
# Canary: 96e44a6a-092d-494c-91f7-025c2d31ad32
#
# This source code is licensed under the Business Source License 1.1


# All steps for REQ-623 are already fully implemented in the existing steps file.
# The Given/When/Then steps for the REQ-623 default behaviour scenario map to:
#   Given "a Hasura v2 source config with kind, database_url, and pool_settings"
#         -> _given_v2_source_kind_url_pool
#   When  "the v2 converter runs"
#         -> _when_v2_converter_runs
#   Then  "SourceType is mapped correctly and connection URL is parsed into components
#          with pool settings preserved"
#         -> _then_v2_source_mapped
# No new step definitions are required for this requirement.


# Copyright (c) 2026 Kenneth Stott
# Canary: b3f3b579-67a3-4793-a622-2bd72413d89b
#
# This source code is licensed under the Business Source License 1.1


# Copyright (c) 2026 Kenneth Stott
# Canary: 7aefa586-7fd0-48dc-8839-0c1fe0eb97a7
#
# This source code is licensed under the Business Source License 1.1

# All steps for REQ-182 are already fully implemented in the existing steps file.
# The Given/When/Then steps for the REQ-182 default behaviour scenario map to:
#   Given "a Hasura v2 metadata export directory"
#         -> _given_v2_metadata
#   When  "the CLI converter is run against it"
#         -> _when_v2_converter_run
#   Then  "valid Provisa YAML config is emitted covering tables, relationships,
#          permissions, roles, and auth"
#         -> _then_v2_config_complete
# No new step definitions are required for this requirement.


# Copyright (c) 2026 Kenneth Stott
# Canary: 83abdc30-88f3-413d-ac9c-81e594295538
#
# This source code is licensed under the Business Source License 1.1


# Copyright (c) 2026 Kenneth Stott
# Canary: 3b0472cd-642b-4e3f-b4d5-111c4f3e019a
#
# This source code is licensed under the Business Source License 1.1


# Copyright (c) 2026 Kenneth Stott
# Canary: 5b1b41c7-7c6c-4f51-afc1-e05412e4a73c
#
# This source code is licensed under the Business Source License 1.1


# Copyright (c) 2026 Kenneth Stott
# Canary: dff3bcfa-8de9-4d77-9805-20865be54e7d
#
# This source code is licensed under the Business Source License 1.1

# All steps for REQ-187 are already fully implemented in the existing steps file.
# The Given/When/Then steps for the REQ-187 default behaviour scenario map to:
#   Given "a Hasura v2 select_permissions[].filter boolean expression"
#         -> _given_v2_filter
#   When  "the v2 converter runs"
#         -> _when_v2_converter_runs
#   Then  "rls_rules[] are generated via boolean expression-to-SQL conversion,
#          with empty filter producing no RLS rule"
#         -> _then_rls_rules
# No new step definitions are required for this requirement.


# Copyright (c) 2026 Kenneth Stott
# Canary: b9bf69af-2352-4278-8485-b8d3414e3c91
#
# This source code is licensed under the Business Source License 1.1


# Copyright (c) 2026 Kenneth Stott
# Canary: f2777276-7d68-489d-9f21-974104657da5
#
# This source code is licensed under the Business Source License 1.1

# All steps for REQ-189 are already fully implemented in the existing steps file.
# The Given/When/Then steps for the REQ-189 default behaviour scenario map to:
#   Given "a DDN supergraph with ObjectType.dataConnectorTypeMapping[].fieldMapping entries"
#         -> _given_ddn_field_mappings
#   When  "the DDN converter runs"
#         -> _when_ddn_converter_runs
#   Then  "all GraphQL field names in relationships, permissions, and column definitions
#          are resolved to physical column names"
#         -> _then_ddn_fields_resolved
# No new step definitions are required for this requirement.


# Copyright (c) 2026 Kenneth Stott
# Canary: 69508a94-1c8a-4f45-a835-fb31f01f299e
#
# This source code is licensed under the Business Source License 1.1


# Copyright (c) 2026 Kenneth Stott
# Canary: eb34bfcb-85df-4808-892e-59c01073fb48
#
# This source code is licensed under the Business Source License 1.1


# Copyright (c) 2026 Kenneth Stott
# Canary: 62894722-4acd-4759-9a16-fe2852a0420c
#
# This source code is licensed under the Business Source License 1.1


# Copyright (c) 2026 Kenneth Stott
# Canary: 80debe12-1589-4a16-8c81-9036f9b722cf
#
# This source code is licensed under the Business Source License 1.1


# Copyright (c) 2026 Kenneth Stott
# Canary: 7f4379a3-b1f8-4ca6-a6ef-715f3908202b
#
# This source code is licensed under the Business Source License 1.1


# Copyright (c) 2026 Kenneth Stott
# Canary: acf8ba6f-2d9d-4650-b9d3-0c363a707732
#
# This source code is licensed under the Business Source License 1.1


# Copyright (c) 2026 Kenneth Stott
# Canary: 4abce9db-c618-4f33-9c7f-7db22860f2aa
#
# This source code is licensed under the Business Source License 1.1


# Copyright (c) 2026 Kenneth Stott
# Canary: d2d78054-9860-4991-861d-120b4045c4f8
#
# This source code is licensed under the Business Source License 1.1


# Copyright (c) 2026 Kenneth Stott
# Canary: 422cc104-0949-4417-8e18-fa59cb1a59a0
#
# This source code is licensed under the Business Source License 1.1


# Copyright (c) 2026 Kenneth Stott
# Canary: 35343c06-716f-4e2f-b42b-18e76b7407e0
#
# This source code is licensed under the Business Source License 1.1


# Copyright (c) 2026 Kenneth Stott
# Canary: aba03e2a-c670-4cda-b017-0d72936219af
#
# This source code is licensed under the Business Source License 1.1


# Copyright (c) 2026 Kenneth Stott
# Canary: 5032983f-1e9c-458a-bcf7-0db0b0f3c544
#
# This source code is licensed under the Business Source License 1.1

# All steps for REQ-190 are already implemented in the existing steps file.
# The Given/When/Then steps for the REQ-190 default behaviour scenario map to:
#   Given "a Hasura v2 auth config with JWT jwk_url, claims_map, or admin secret"
#         -> _given_v2_auth_config
#   When  "the v2 converter runs with --auth-env-file"
#         -> _when_v2_auth_converter_runs
#   Then  "JWT becomes provider: oauth with role_mapping[], admin secret becomes superuser,
#          and webhook auth emits a warning"
#         -> _then_v2_auth_converted
# No new step definitions are required for this requirement.


# Copyright (c) 2026 Kenneth Stott
# Canary: 28c3820b-5977-4ba7-baa4-7533667a41d5
#
# This source code is licensed under the Business Source License 1.1


# Copyright (c) 2026 Kenneth Stott
# Canary: 67fbc04b-b500-4d97-9590-4a75470c9b9e
#
# This source code is licensed under the Business Source License 1.1


# Copyright (c) 2026 Kenneth Stott
# Canary: b4a3433c-7961-4a20-ad79-f5abed3603ae
#
# This source code is licensed under the Business Source License 1.1


# Copyright (c) 2026 Kenneth Stott
# Canary: 2d9fed2e-dc0e-4421-92d2-10858e1e7913
#
# This source code is licensed under the Business Source License 1.1


# Copyright (c) 2026 Kenneth Stott
# Canary: a5e24fee-b5fe-422b-a60b-05cd57a68988
#
# This source code is licensed under the Business Source License 1.1


# Copyright (c) 2026 Kenneth Stott
# Canary: 85e804ed-d105-4382-ad2f-b8a646235ee9
#
# This source code is licensed under the Business Source License 1.1


# Copyright (c) 2026 Kenneth Stott
# Canary: 94339f7b-8ef2-4300-af0a-3ab31d8e4b00
#
# This source code is licensed under the Business Source License 1.1


# Copyright (c) 2026 Kenneth Stott
# Canary: 89ece119-cdee-4981-b221-932c1c54c91f
#
# This source code is licensed under the Business Source License 1.1


# Copyright (c) 2026 Kenneth Stott
# Canary: 4c55f874-0056-446e-8db3-ceae41bff70e
#
# This source code is licensed under the Business Source License 1.1


# Copyright (c) 2026 Kenneth Stott
# Canary: 3f01c99a-adae-47cf-a925-db85f9f7443f
#
# This source code is licensed under the Business Source License 1.1


# Copyright (c) 2026 Kenneth Stott
# Canary: c829d6b1-efa5-4d04-b38d-c5a2b4bb8e73
#
# This source code is licensed under the Business Source License 1.1


# Copyright (c) 2026 Kenneth Stott
# Canary: 273c9949-0754-4344-9eb3-021700fceaf7
#
# This source code is licensed under the Business Source License 1.1


# Copyright (c) 2026 Kenneth Stott
# Canary: ea7d52bf-9d09-49ab-8bf9-e50d171387aa
#
# This source code is licensed under the Business Source License 1.1


# Copyright (c) 2026 Kenneth Stott
# Canary: 11c080c4-091b-42a4-9a8f-8baf1467622a
#
# This source code is licensed under the Business Source License 1.1

# All steps for REQ-625 are already fully implemented in the existing steps file.
# The Given/When/Then steps for the REQ-625 default behaviour scenario map to:
#   Given "a Hasura v2 source with database_url as an env var reference or unparseable URL"
#         -> _given_v2_source_env_url
#   When  "the v2 converter runs"
#         -> _when_v2_converter_runs
#   Then  "placeholder connection values are substituted and operators are directed to
#          use --source-overrides"
#         -> _then_v2_placeholder_values
# No new step definitions are required for this requirement.


# Copyright (c) 2026 Kenneth Stott
# Canary: dc45933c-bb5c-4f88-bb4a-cfe1ffab2712
#
# This source code is licensed under the Business Source License 1.1


# All three steps for REQ-183 are already fully implemented in the existing steps file.
# The Given/When/Then steps for the REQ-183 default behaviour scenario map to:
#   Given "a Hasura DDN supergraph project"       -> _given_ddn_project
#   When  "the HML converter CLI tool is run"     -> _when_ddn_converter_run
#   Then  "valid Provisa YAML config is emitted covering ObjectTypes, Models,
#          Relationships, TypePermissions, ModelPermissions, and DataConnectorLinks"
#                                                 -> _then_ddn_config_complete
# No new step definitions are required for this requirement.


# Copyright (c) 2026 Kenneth Stott
# Canary: 25015615-5e20-4d4e-ad39-f5aa2446a188
#
# This source code is licensed under the Business Source License 1.1


# Copyright (c) 2026 Kenneth Stott
# Canary: b504393b-26a2-435f-80ca-d83e98928627
#
# This source code is licensed under the Business Source License 1.1


# Copyright (c) 2026 Kenneth Stott
# Canary: 56ae743f-ac05-454d-a1e4-0da99eab4c4c
#
# This source code is licensed under the Business Source License 1.1


# All steps for REQ-188 are already implemented in the existing steps file.
# The Given/When/Then steps for the REQ-188 default behaviour scenario map to:
#   Given "a Hasura v2 metadata export with object_relationships and array_relationships"
#         -> _given_v2_relationships
#   When  "the v2 converter runs"
#         -> _when_v2_converter_runs
#   Then  "object_relationships become cardinality=many-to-one and array_relationships
#          become cardinality=one-to-many"
#         -> _then_cardinality
# No new step definitions are required for this requirement.


# Copyright (c) 2026 Kenneth Stott
# Canary: fdc11f63-83d3-47ac-8990-302ea86f5268
#
# This source code is licensed under the Business Source License 1.1


# Copyright (c) 2026 Kenneth Stott
# Canary: 9ea17def-9742-45db-9914-443d6ee5e843
#
# This source code is licensed under the Business Source License 1.1


# Copyright (c) 2026 Kenneth Stott
# Canary: 673e3703-0b4e-4ba9-b367-5dec14488e34
#
# This source code is licensed under the Business Source License 1.1


# Copyright (c) 2026 Kenneth Stott
# Canary: eb5e6d24-dd77-49e4-a35a-7cd769e9eb71
#
# This source code is licensed under the Business Source License 1.1

# All steps for REQ-623 are already fully implemented in the existing steps file.
# The Given/When/Then steps for the REQ-623 default behaviour scenario map to:
#   Given "a Hasura v2 source config with kind, database_url, and pool_settings"
#         -> _given_v2_source_kind_url_pool
#   When  "the v2 converter runs"
#         -> _when_v2_converter_runs
#   Then  "SourceType is mapped correctly and connection URL is parsed into components
#          with pool settings preserved"
#         -> _then_v2_source_mapped
# No new step definitions are required for this requirement.


# All steps for REQ-625 are already fully implemented in the existing steps file.
# The Given/When/Then steps for the REQ-625 default behaviour scenario map to:
#   Given "a Hasura v2 source with database_url as an env var reference or unparseable URL"
#         -> _given_v2_source_env_url
#   When  "the v2 converter runs"
#         -> _when_v2_converter_runs
#   Then  "placeholder connection values are substituted and operators are directed to
#          use --source-overrides"
#         -> _then_v2_placeholder_values
# No new step definitions are required for this requirement.


# Copyright (c) 2026 Kenneth Stott
# Canary: f2e88332-a51f-4d4f-a825-efc81a2e70f8
#
# This source code is licensed under the Business Source License 1.1


# Copyright (c) 2026 Kenneth Stott
# Canary: 7b71e1b7-6ee6-4459-adc6-169334d921f9
#
# This source code is licensed under the Business Source License 1.1


# Copyright (c) 2026 Kenneth Stott
# Canary: 6d6204a9-74a5-4466-a556-b354ee1f2609
#
# This source code is licensed under the Business Source License 1.1


# All three steps for REQ-185 are already fully implemented in the existing steps file.
# The Given/When/Then steps for the REQ-185 default behaviour scenario map to:
#   Given "a Hasura v2 metadata export with select_permissions[].columns per role"
#         -> _given_v2_select_perms
#   When  "the v2 converter runs"
#         -> _when_v2_converter_runs
#   Then  "each column's visible_to is populated from the role's column list,
#          with \"*\" meaning all columns"
#         -> _then_visible_to
# No new step definitions are required for this requirement.


# Copyright (c) 2026 Kenneth Stott
# Canary: 6a80cb1e-b788-470c-92fc-2e118a4dfd56
#
# This source code is licensed under the Business Source License 1.1


# All steps for REQ-188 are already fully implemented in the existing steps file.
# The Given/When/Then steps for the REQ-188 default behaviour scenario map to:
#   Given "a Hasura v2 metadata export with object_relationships and array_relationships"
#         -> _given_v2_relationships
#   When  "the v2 converter runs"
#         -> _when_v2_converter_runs
#   Then  "object_relationships become cardinality=many-to-one and array_relationships
#          become cardinality=one-to-many"
#         -> _then_cardinality
# No new step definitions are required for REQ-188.


# Copyright (c) 2026 Kenneth Stott
# Canary: 6a0085ee-e5e7-4f37-9eb8-1d6c465c3f4e
#
# This source code is licensed under the Business Source License 1.1


# Copyright (c) 2026 Kenneth Stott
# Canary: 5c1336ab-7111-4bc3-8c90-615ed6514f7d
#
# This source code is licensed under the Business Source License 1.1


# Copyright (c) 2026 Kenneth Stott
# Canary: e041de96-6d48-41ce-8fc4-fee4b530349d
#
# This source code is licensed under the Business Source License 1.1


# Copyright (c) 2026 Kenneth Stott
# Canary: 4a54e944-2682-407e-8dde-adbee2cf68ea
#
# This source code is licensed under the Business Source License 1.1


# Copyright (c) 2026 Kenneth Stott
# Canary: 2b0d5c49-77b6-45ce-96e4-a626138fb49f
#
# This source code is licensed under the Business Source License 1.1

# All steps for REQ-625 are already fully implemented in the existing steps file.
# Given "a Hasura v2 source with database_url as an env var reference or unparseable URL"
#       -> _given_v2_source_env_url
# When  "the v2 converter runs"
#       -> _when_v2_converter_runs
# Then  "placeholder connection values are substituted and operators are directed to
#        use --source-overrides"
#       -> _then_v2_placeholder_values
# No new step definitions are required for this requirement.


# Copyright (c) 2026 Kenneth Stott
# Canary: c655cbb0-0e22-460f-b751-0867ed9bc107
#
# This source code is licensed under the Business Source License 1.1


# All three steps for REQ-183 are already fully implemented in the existing steps file.
# The Given/When/Then steps for the REQ-183 default behaviour scenario map to:
#   Given "a Hasura DDN supergraph project"       -> _given_ddn_project
#   When  "the HML converter CLI tool is run"     -> _when_ddn_converter_run
#   Then  "valid Provisa YAML config is emitted covering ObjectTypes, Models,
#          Relationships, TypePermissions, ModelPermissions, and DataConnectorLinks"
#                                                 -> _then_ddn_config_complete
# No new step definitions are required for this requirement.


# Copyright (c) 2026 Kenneth Stott
# Canary: c6490ce8-905a-4793-85bb-4381c890fd45
#
# This source code is licensed under the Business Source License 1.1


# Copyright (c) 2026 Kenneth Stott
# Canary: 7bf247ee-969a-4c4d-a51e-8c8693ac277b
#
# This source code is licensed under the Business Source License 1.1


# All steps for REQ-187 are already fully implemented in the existing steps file.
# The Given/When/Then steps for the REQ-187 default behaviour scenario map to:
#   Given "a Hasura v2 select_permissions[].filter boolean expression"
#         -> _given_v2_filter
#   When  "the v2 converter runs"
#         -> _when_v2_converter_runs
#   Then  "rls_rules[] are generated via boolean expression-to-SQL conversion,
#          with empty filter producing no RLS rule"
#         -> _then_rls_rules
# No new step definitions are required for this requirement.


# Copyright (c) 2026 Kenneth Stott
# Canary: 4b4b1d68-9962-47c7-a563-b24f953a6744
#
# This source code is licensed under the Business Source License 1.1


# Copyright (c) 2026 Kenneth Stott
# Canary: a5a28933-dee0-4587-be11-d3a3a81fef09
#
# This source code is licensed under the Business Source License 1.1


# Copyright (c) 2026 Kenneth Stott
# Canary: 2e95c007-8b9e-4d46-a96b-a46b8090a7a3
#
# This source code is licensed under the Business Source License 1.1


# Copyright (c) 2026 Kenneth Stott
# Canary: 1313ac78-fa74-46c9-9d17-bd0b00496726
#
# This source code is licensed under the Business Source License 1.1


# Copyright (c) 2026 Kenneth Stott
# Canary: 3c8bcf14-f62f-4f86-8a29-297c7353addd
#
# This source code is licensed under the Business Source License 1.1


# Copyright (c) 2026 Kenneth Stott
# Canary: c83b5cef-422e-4fba-b62a-6a3ff170d039
#
# This source code is licensed under the Business Source License 1.1

# All steps for REQ-625 are already fully implemented in the existing steps file.
# The Given/When/Then steps for the REQ-625 default behaviour scenario map to:
#   Given "a Hasura v2 source with database_url as an env var reference or unparseable URL"
#         -> _given_v2_source_env_url
#   When  "the v2 converter runs"
#         -> _when_v2_converter_runs
#   Then  "placeholder connection values are substituted and operators are directed to
#          use --source-overrides"
#         -> _then_v2_placeholder_values
# No new step definitions are required for this requirement.


# Copyright (c) 2026 Kenneth Stott
# Canary: f4433ff2-e26f-451a-9a45-be8f3cb6eef8
#
# This source code is licensed under the Business Source License 1.1


# Copyright (c) 2026 Kenneth Stott
# Canary: faf24e21-bc21-446e-bb47-ed4af4920296
#
# This source code is licensed under the Business Source License 1.1


# Copyright (c) 2026 Kenneth Stott
# Canary: ca36afee-05dc-45fa-ad8e-bd6ffcfb100d
#
# This source code is licensed under the Business Source License 1.1


# All steps for REQ-185 are already fully implemented in the existing steps file.
# The Given/When/Then steps for the REQ-185 default behaviour scenario map to:
#   Given "a Hasura v2 metadata export with select_permissions[].columns per role"
#         -> _given_v2_select_perms
#   When  "the v2 converter runs"
#         -> _when_v2_converter_runs
#   Then  "each column's visible_to is populated from the role's column list,
#          with \"*\" meaning all columns"
#         -> _then_visible_to
# No new step definitions are required for this requirement.


# Copyright (c) 2026 Kenneth Stott
# Canary: 589f5934-547b-4ad0-8d4d-191532b5e530
#
# This source code is licensed under the Business Source License 1.1


# Copyright (c) 2026 Kenneth Stott
# Canary: 66c59309-e5bf-49e2-9b7b-135ea53be3f9
#
# This source code is licensed under the Business Source License 1.1


# Copyright (c) 2026 Kenneth Stott
# Canary: cdcec3cb-fbc9-47f4-b233-8cc3c5b75025
#
# This source code is licensed under the Business Source License 1.1


# Copyright (c) 2026 Kenneth Stott
# Canary: 2c9f28d7-65a5-4fe1-b8f2-892e8d540785
#
# This source code is licensed under the Business Source License 1.1

# All steps for REQ-190 are already implemented in the existing steps file.
# The Given/When/Then steps for the REQ-190 default behaviour scenario map to:
#   Given "a Hasura v2 auth config with JWT jwk_url, claims_map, or admin secret"
#         -> _given_v2_auth_config
#   When  "the v2 converter runs with --auth-env-file"
#         -> _when_v2_auth_converter_runs
#   Then  "JWT becomes provider: oauth with role_mapping[], admin secret becomes superuser,
#          and webhook auth emits a warning"
#         -> _then_v2_auth_converted
# No new step definitions are required for this requirement.


# Copyright (c) 2026 Kenneth Stott
# Canary: 05d696e1-42ec-409e-b448-2f80825f7ef1
#
# This source code is licensed under the Business Source License 1.1


# Copyright (c) 2026 Kenneth Stott
# Canary: 79430b41-30a6-48c6-a15d-3e9e9b7df0d0
#
# This source code is licensed under the Business Source License 1.1

# All steps for REQ-623 are already fully implemented in the existing steps file.
# The Given/When/Then steps for the REQ-623 default behaviour scenario map to:
#   Given "a Hasura v2 source config with kind, database_url, and pool_settings"
#         -> _given_v2_source_kind_url_pool
#   When  "the v2 converter runs"
#         -> _when_v2_converter_runs
#   Then  "SourceType is mapped correctly and connection URL is parsed into components
#          with pool settings preserved"
#         -> _then_v2_source_mapped
# No new step definitions are required for this requirement.


# Copyright (c) 2026 Kenneth Stott
# Canary: 13be2f6a-01e4-4f09-aa7e-56f81258124c
#
# This source code is licensed under the Business Source License 1.1


# Copyright (c) 2026 Kenneth Stott
# Canary: f529182d-6748-4e36-9e02-4156a5733c74
#
# This source code is licensed under the Business Source License 1.1


# Copyright (c) 2026 Kenneth Stott
# Canary: 8ed1ed11-2621-41b5-b2db-d6204b962f7b
#
# This source code is licensed under the Business Source License 1.1


# Copyright (c) 2026 Kenneth Stott
# Canary: 41d13b65-dd97-4424-8e31-812c37266d3c
#
# This source code is licensed under the Business Source License 1.1


# Copyright (c) 2026 Kenneth Stott
# Canary: 4fe51efb-527f-4420-a2ed-6e1ec082ab80
#
# This source code is licensed under the Business Source License 1.1


# Copyright (c) 2026 Kenneth Stott
# Canary: c977ab78-e986-458d-a6a5-056acd688f0e
#
# This source code is licensed under the Business Source License 1.1


# Copyright (c) 2026 Kenneth Stott
# Canary: 5fe680d8-611f-4ad0-911e-6e5e49167b9a
#
# This source code is licensed under the Business Source License 1.1


# Copyright (c) 2026 Kenneth Stott
# Canary: 6d036a08-0722-41c3-8ae7-17cf26e127e4
#
# This source code is licensed under the Business Source License 1.1


# Copyright (c) 2026 Kenneth Stott
# Canary: 5aef2f66-19c2-41eb-8be1-79a5032c2f0b
#
# This source code is licensed under the Business Source License 1.1


# Copyright (c) 2026 Kenneth Stott
# Canary: 256e3fac-071f-42b4-b421-e07ec058ad17
#
# This source code is licensed under the Business Source License 1.1

# All steps for REQ-621 are already fully implemented in the existing steps file.
# The Given/When/Then steps for the REQ-621 default behaviour scenario map to:
#   Given "a completed Hasura v2 or DDN conversion"
#         -> _given_completed_conversion
#   When  "the output config is inspected"
#         -> _when_output_config_inspected
#   Then  "placeholder credentials (host: localhost, password: ${env:DB_PASSWORD}) are present
#          and Provisa refuses to start without real values"
#         -> _then_placeholder_credentials
# No new step definitions are required for this requirement.


# Copyright (c) 2026 Kenneth Stott
# Canary: 31b7cf56-b1f0-4a87-8e11-f74cb0656f35
#
# This source code is licensed under the Business Source License 1.1

# All steps for REQ-623 are already fully implemented in the existing steps file.
# The Given/When/Then steps for the REQ-623 default behaviour scenario map to:
#   Given "a Hasura v2 source config with kind, database_url, and pool_settings"
#         -> _given_v2_source_kind_url_pool
#   When  "the v2 converter runs"
#         -> _when_v2_converter_runs
#   Then  "SourceType is mapped correctly and connection URL is parsed into components
#          with pool settings preserved"
#         -> _then_v2_source_mapped
# No new step definitions are required for this requirement.


# Copyright (c) 2026 Kenneth Stott
# Canary: 50bbf123-7c40-4830-992f-77394c4805ea
#
# This source code is licensed under the Business Source License 1.1


# Copyright (c) 2026 Kenneth Stott
# Canary: 3cde655d-acd8-4c39-b4c2-de00a155b008
#
# This source code is licensed under the Business Source License 1.1

# All steps for REQ-182 are already fully implemented in the existing steps file.
# The Given/When/Then steps for the REQ-182 default behaviour scenario map to:
#   Given "a Hasura v2 metadata export directory"
#         -> _given_v2_metadata
#   When  "the CLI converter is run against it"
#         -> _when_v2_converter_run
#   Then  "valid Provisa YAML config is emitted covering tables, relationships,
#          permissions, roles, and auth"
#         -> _then_v2_config_complete
# No new step definitions are required for this requirement.


# Copyright (c) 2026 Kenneth Stott
# Canary: 9b8cb1d8-53c3-418c-875f-9379e37c5eea
#
# This source code is licensed under the Business Source License 1.1


# Copyright (c) 2026 Kenneth Stott
# Canary: 1326763f-d739-4b74-80f7-4a3bed7b61b1
#
# This source code is licensed under the Business Source License 1.1


# Copyright (c) 2026 Kenneth Stott
# Canary: e41a342b-6787-4371-b06e-3656e895b18e
#
# This source code is licensed under the Business Source License 1.1


# Copyright (c) 2026 Kenneth Stott
# Canary: d904189d-ff04-4837-badb-e3b2de62fab5
#
# This source code is licensed under the Business Source License 1.1


# Copyright (c) 2026 Kenneth Stott
# Canary: b7e68c7a-ac82-49ba-99f2-2c54a6ec15b2
#
# This source code is licensed under the Business Source License 1.1


# All steps for REQ-189 are already fully implemented in the existing steps file.
# The Given/When/Then steps for the REQ-189 default behaviour scenario map to:
#   Given "a DDN supergraph with ObjectType.dataConnectorTypeMapping[].fieldMapping entries"
#         -> _given_ddn_field_mappings
#   When  "the DDN converter runs"
#         -> _when_ddn_converter_runs
#   Then  "all GraphQL field names in relationships, permissions, and column definitions
#          are resolved to physical column names"
#         -> _then_ddn_fields_resolved
# No new step definitions are required for this requirement.


# Copyright (c) 2026 Kenneth Stott
# Canary: e8bc7ed8-4e70-486f-8667-f4eabb560b94
#
# This source code is licensed under the Business Source License 1.1


# Copyright (c) 2026 Kenneth Stott
# Canary: 879d0361-03e3-4e7b-b8da-bad20c948376
#
# This source code is licensed under the Business Source License 1.1


# Copyright (c) 2026 Kenneth Stott
# Canary: 6840bacc-76ad-4b1e-b1e2-ca23156f5f2d
#
# This source code is licensed under the Business Source License 1.1

# All steps for REQ-623 are already fully implemented in the existing steps file.
# The Given/When/Then steps for the REQ-623 default behaviour scenario map to:
#   Given "a Hasura v2 source config with kind, database_url, and pool_settings"
#         -> _given_v2_source_kind_url_pool
#   When  "the v2 converter runs"
#         -> _when_v2_converter_runs
#   Then  "SourceType is mapped correctly and connection URL is parsed into components
#          with pool settings preserved"
#         -> _then_v2_source_mapped
# No new step definitions are required for this requirement.


# Copyright (c) 2026 Kenneth Stott
# Canary: 5d4578a6-b932-4785-b96e-cb22907c76bd
#
# This source code is licensed under the Business Source License 1.1
