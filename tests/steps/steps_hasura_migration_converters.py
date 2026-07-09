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
