# Copyright (c) 2026 Kenneth Stott
# Canary: 9a3c5e7b-1d24-4f8a-b6e0-4c2d8f1a7b39
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-194 (amended 2026-11-07): the names table of one model version -- every object's name on
every surface, clashes settled under the clash policy, every name checked."""

import pytest

from provisa.compiler import naming
from provisa.compiler.names_table import (
    FrozenAlias,
    NameRow,
    NamesClashRefused,
    build_names_table,
    model_name_rows,
)
from provisa.compiler.sql_types import CompilationContext, JoinMeta, TableMeta


@pytest.fixture(autouse=True)
def _convention():
    naming.configure(gql="apollo_graphql", sql="snake")
    yield
    naming.configure(gql="apollo_graphql", sql="snake")


_LONG = "c" * 70


def _orders() -> TableMeta:
    return TableMeta(
        table_id=1,
        field_name="orders",
        type_name="Orders",
        source_id="sales-pg",
        catalog_name="sales_pg",
        schema_name="public",
        table_name="orders",
        domain_id="sales",
    )


def _customers() -> TableMeta:
    return TableMeta(
        table_id=2,
        field_name="customers",
        type_name="Customers",
        source_id="sales-pg",
        catalog_name="sales_pg",
        schema_name="public",
        table_name="customers",
        domain_id="sales",
    )


def _fixture_model(*, clash: bool) -> tuple[CompilationContext, list[dict]]:
    """Two tables of the sales domain: orders (a reserved-word column, a 70-character column,
    and -- with ``clash`` -- two columns ``Status`` and ``status`` one SQL name apart only by
    case) and customers, joined many-to-one."""
    ctx = CompilationContext()
    orders, customers = _orders(), _customers()
    ctx.tables = {"orders": orders, "customers": customers}
    columns = {"id": "id", "order": "order", _LONG: _LONG, "customer_id": "customer_id"}
    if clash:
        columns |= {"Status": "Status", "status": "status"}
    for physical, sql in columns.items():
        ctx.physical_to_sql[(1, physical)] = sql
        ctx.physical_to_gql[(1, physical)] = naming.apply_gql_name(physical)
    ctx.physical_to_sql[(2, "id")] = "id"
    ctx.physical_to_gql[(2, "id")] = "id"
    ctx.pk_columns = {1: ["id"], 2: ["id"]}
    ctx.write_ops = {1: frozenset({"insert"})}
    ctx.joins[("Orders", "customer")] = JoinMeta(
        source_column="customer_id",
        target_column="id",
        source_column_type="integer",
        target_column_type="integer",
        target=customers,
        cardinality="many-to-one",
    )
    tables = [
        {"id": 1, "source_id": "sales-pg", "alias": None, "view_sql": None, "columns": []},
        {"id": 2, "source_id": "sales-pg", "alias": None, "view_sql": None, "columns": []},
    ]
    return ctx, tables


def _build(policy: str = "refuse", *, clash: bool = False):
    ctx, tables = _fixture_model(clash=clash)
    rows = model_name_rows(
        ctx,
        tables,
        commands=[{"name": "top_customers", "kind": "query", "field": "topCustomers"}],
        enums={"order_status": ["pending", "1st"]},
        sql_name_of={1: "orders", 2: "customers"},
    )
    return build_names_table(rows, policy)


def test_every_object_named_on_every_surface():
    table = _build()
    assert table.names_of("table", "1") == {
        "sql": "orders",
        "graphql_field": "orders",
        "graphql_type": "Orders",
        "graphql_aggregate": "ordersAggregate",
        "graphql_group_by": "ordersGroupBy",
        "graphql_connection": "orders_connection",
        "graphql_insert": "insertOrders",
        "cypher_label": "Orders",
        "proto_message": "Orders",
        "proto_field": "orders",
        "proto_rpc": "QueryOrders",
        "pg_constraint": "pk_orders",
    }
    assert table.names_of("column", "1:customer_id") == {
        "sql": "customer_id",
        "graphql_field": "customerId",
        "cypher_property": "customerId",
        "proto_field": "customerId",
    }
    assert table.names_of("relationship", "Orders.customer") == {
        "graphql_field": "customer",
        "cypher_rel_type": "IS_CUSTOMER",
    }
    assert table.names_of("source", "sales-pg") == {"catalog": "sales_pg"}
    assert table.names_of("command", "top_customers") == {
        "graphql_field": "topCustomers",
        "proto_rpc": "CallTopCustomers",
    }
    assert table.names_of("enum", "order_status") == {"graphql_type": "order_status_Enum"}
    assert table.names_of("enum_value", "order_status:1st") == {"graphql_enum_value": "V_1ST"}


def test_case_only_clash_is_refused_naming_both_objects():
    with pytest.raises(NamesClashRefused) as refused:
        _build(clash=True)
    (clash,) = [c for c in refused.value.clashes if c.surface == "sql"]
    assert {clash.holder, clash.newcomer} == {
        "sales-pg.public.orders.Status",
        "sales-pg.public.orders.status",
    }
    assert "sales-pg.public.orders.Status" in str(refused.value)
    assert "sales-pg.public.orders.status" in str(refused.value)


def test_reserved_word_is_warned_on_its_object():
    warnings = [w for w in _build().warnings if w.object_id == "1:order"]
    assert [(w.surface, w.code) for w in warnings] == [
        ("sql", "reserved"),
        ("cypher_property", "reserved"),
    ]
    assert "reserved word on sql: it must be quoted" in warnings[0].message


def test_name_over_the_sql_length_limit_is_warned():
    warnings = [w for w in _build().warnings if w.object_id == f"1:{_LONG}"]
    assert ("sql", "too_long") in [(w.surface, w.code) for w in warnings]
    assert "over sql's limit of 63" in next(w.message for w in warnings if w.code == "too_long")


def test_disambiguate_policy_freezes_an_alias():
    table = _build("disambiguate", clash=True)
    assert table.frozen_aliases == (FrozenAlias("column", "1:status", "sql", "status_2"),)
    assert table.names_of("column", "1:status")["sql"] == "status_2"
    assert table.names_of("column", "1:Status")["sql"] == "Status"
    warned = next(w for w in table.warnings if w.code == "disambiguated" and w.surface == "sql")
    assert warned.object_id == "1:status"
    assert "status_2" in warned.message and "sales-pg.public.orders.Status" in warned.message


def test_disambiguate_derives_from_the_distinguishing_attribute():
    def table(oid: str, source: str) -> NameRow:
        return NameRow(
            "table",
            oid,
            "sql",
            "orders",
            "orders",
            f"{source}.public.orders",
            scope=("sql", "sales"),
            attribute=source,
        )

    names = build_names_table([table("1", "pg-a"), table("2", "pg-b")], "disambiguate")
    assert names.frozen_aliases == (FrozenAlias("table", "2", "sql", "pg_b_orders"),)


def test_disambiguate_refuses_a_kind_with_no_alias_to_freeze():
    def command(name: str) -> NameRow:
        return NameRow(
            "command",
            name,
            "graphql_field",
            "topPets",
            name,
            f"command {name}",
            scope=("graphql_query",),
        )

    with pytest.raises(NamesClashRefused, match="command b and command a"):
        build_names_table([command("a"), command("b")], "disambiguate")


def test_cross_surface_clash_column_and_relationship_on_one_type():
    rows = [
        NameRow(
            "column",
            "1:customer",
            "graphql_field",
            "customer",
            "customer",
            "orders.customer",
            scope=("graphql_type_field", "Orders"),
        ),
        NameRow(
            "relationship",
            "Orders.customer",
            "graphql_field",
            "customer",
            "customer",
            "relationship Orders.customer",
            scope=("graphql_type_field", "Orders"),
        ),
    ]
    with pytest.raises(NamesClashRefused, match="relationship Orders.customer and orders.customer"):
        build_names_table(rows)


def test_taken_sql_address_is_refused():
    rows = [
        NameRow("table", "1", "sql", "orders", "orders", "a.public.orders", scope=("sql", "sales")),
        NameRow("table", "7", "sql", "Orders", "Orders", "b.public.Orders", scope=("sql", "sales")),
    ]
    with pytest.raises(NamesClashRefused, match="b.public.Orders and a.public.orders"):
        build_names_table(rows)


def test_one_object_under_one_name_twice_is_no_clash():
    row = NameRow("table", "1", "sql", "orders", "orders", "a.public.orders", scope=("sql", "s"))
    assert len(build_names_table([row, row]).rows) == 2


def test_sql_index_ignores_case():
    table = _build()
    found = table.resolve_sql(("sql", "sales"), "ORDERS")
    assert found is not None and found.object_id == "1"
    assert table.resolve_sql(("sql_column", "1"), "Customer_ID").object_id == "1:customer_id"
    assert table.resolve_sql(("sql", "sales"), "nothing") is None


def test_alias_shadowing_another_objects_name_is_warned():
    rows = [
        NameRow("column", "1:a", "sql", "b", "a", "t.a", scope=("sql_column", "1"), alias=True),
        NameRow("column", "1:b", "sql", "b_old", "b", "t.b", scope=("sql_column", "1"), alias=True),
    ]
    warned = [w for w in build_names_table(rows).warnings if w.code == "alias_shadows"]
    assert [(w.object_id, w.message) for w in warned] == [
        ("1:a", "t.a: its alias 'b' is t.b's physical name; set a different alias")
    ]


def test_lossy_conversion_is_warned_once():
    rows = [
        NameRow(
            "column",
            "1:Order_ID",
            "sql",
            "order_id",
            "Order_ID",
            "t.Order_ID",
            scope=("sql_column", "1"),
        ),
        NameRow("column", "1:Order_ID", "graphql_field", "orderID", "Order_ID", "t.Order_ID"),
    ]
    assert [w.code for w in build_names_table(rows).warnings] == ["lossy"]


def test_unknown_policy_refused():
    with pytest.raises(ValueError, match="unknown clash policy"):
        build_names_table([], "rename")


def _sources_table(*sources: tuple[str, str]):
    """The names table of one table per (source id, catalog), in that order."""
    ctx = CompilationContext()
    for i, (source, catalog) in enumerate(sources, start=1):
        field = f"t{i}"
        ctx.tables[field] = TableMeta(
            table_id=i,
            field_name=field,
            type_name=f"T{i}",
            source_id=source,
            catalog_name=catalog,
            schema_name="public",
            table_name=field,
            domain_id="sales",
        )
    return build_names_table(model_name_rows(ctx, []))


def test_sources_sharing_a_catalog_they_are_pinned_to_are_no_clash():
    """A checker reads through the control plane's catalog, which is not named after it."""
    table = _sources_table(
        ("provisa-admin", "provisa_admin"),
        ("dq-checker", "provisa_admin"),
        ("dq-soda", "provisa_admin"),
    )
    assert table.names_of("source", "dq-checker") == {"catalog": "provisa_admin"}
    assert table.names_of("source", "dq-soda") == {"catalog": "provisa_admin"}


def test_the_catalog_address_stays_with_the_source_it_is_named_after():
    table = _sources_table(("dq-checker", "provisa_admin"), ("provisa-admin", "provisa_admin"))
    row = table.resolve_sql(("catalog",), "PROVISA_ADMIN")
    assert row is not None and row.object_id == "provisa-admin"


def test_two_sources_collapsing_to_one_catalog_are_refused():
    with pytest.raises(NamesClashRefused) as refused:
        _sources_table(("sales-pg", "sales_pg"), ("sales_pg", "sales_pg"))
    message = str(refused.value)
    assert "source sales-pg" in message and "source sales_pg" in message


def test_two_sources_collapsing_to_one_org_scoped_catalog_are_refused():
    with pytest.raises(NamesClashRefused):
        _sources_table(("sales-pg", "org_acme__sales_pg"), ("sales_pg", "org_acme__sales_pg"))


def test_a_user_source_taking_the_control_planes_catalog_name_is_refused():
    with pytest.raises(NamesClashRefused):
        _sources_table(("provisa-admin", "provisa_admin"), ("provisa_admin", "provisa_admin"))


def _reference_model(*, governed: bool):
    """Orders with a remote-style column ``customer`` and a relationship named ``customer``."""
    ctx = CompilationContext()
    orders = _orders()
    ctx.tables = {"orders": orders}
    ctx.physical_to_gql[(1, "customer")] = "customer"
    ctx.physical_to_sql[(1, "customer")] = "customer"
    if governed:
        ctx.gql_governed_object_cols.add((1, "customer"))
    ctx.joins[("Orders", "customer")] = JoinMeta(
        source_column="customer",
        target_column="id",
        source_column_type="integer",
        target_column_type="integer",
        target=_customers(),
        cardinality="many-to-one",
    )
    return ctx


def test_a_field_pointing_at_a_registered_type_leaves_the_name_to_its_relationship():
    table = build_names_table(model_name_rows(_reference_model(governed=True), []))
    assert table.names_of("relationship", "Orders.customer")["graphql_field"] == "customer"
    assert "graphql_field" not in table.names_of("column", "1:customer")


def test_a_data_column_and_a_relationship_of_one_name_are_still_refused():
    with pytest.raises(NamesClashRefused, match="relationship Orders.customer"):
        build_names_table(model_name_rows(_reference_model(governed=False), []))
