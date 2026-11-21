# Copyright (c) 2026 Kenneth Stott
# Canary: 7d1b2c4e-93a5-4f60-8e21-5c0a9b3e7f12
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-194 (amended 2026-11-07): every client-facing name kind has one rule in the naming
authority. Each rule is pinned by its own vectors and, where a site still builds the name by
itself (naming audit rows B4-B15), by parity with that site's output, so moving the site onto
the rule changes no name."""

import pytest

from provisa.compiler import naming

_SAMPLES = ["orders", "activeUsers", "sa__orders", "sa__userByName", "pet_companions", "x"]


@pytest.fixture(autouse=True)
def _convention():
    naming.configure(gql="apollo_graphql", sql="snake")
    yield
    naming.configure(gql="apollo_graphql", sql="snake")


# ── mutation field names (B7) ──


@pytest.mark.parametrize(
    ("op", "field", "convention", "expected"),
    [
        ("insert", "orders", "apollo_graphql", "insertOrders"),
        ("insert", "orders", "hasura_graphql", "insert_orders"),
        ("delete", "sa__orders", "apollo_graphql", "sa__deleteOrders"),
        ("update", "sa__orders", "snake", "sa__update_orders"),
    ],
)
def test_mutation_field_name(op, field, convention, expected):
    assert naming.mutation_field_name(op, field, convention) == expected


@pytest.mark.parametrize("convention", ["apollo_graphql", "hasura_graphql", "snake"])
@pytest.mark.parametrize("field", _SAMPLES)
def test_mutation_field_name_matches_actions_schema(field, convention):
    from provisa.compiler.actions_schema import _mutation_name

    for op in ("insert", "update", "delete"):
        assert naming.mutation_field_name(op, field, convention) == _mutation_name(
            op, field, convention
        )


# ── domain-prefixed names (B8) and their inverses ──


def test_prefixed_field_and_type_name():
    assert naming.prefixed_field_name("sa", "orders") == "sa__orders"
    assert naming.prefixed_type_name("sa", "orders") == "SA__Orders"
    assert naming.prefixed_type_name("sales_analytics", "userByName") == (
        "SALES_ANALYTICS__UserByName"
    )


@pytest.mark.parametrize("field", ["orders", "userByName", "pet_companions"])
def test_prefixed_names_invert(field):
    composed = naming.prefixed_field_name("sa", field)
    assert naming.bare_field_name(composed, "sales_analytics") == field
    assert naming.prefixed_type_name("sa", field) == naming.to_type_name(composed)


def test_prefixed_type_name_matches_schema_gen_alias_form():
    # schema_gen._assign_names: f"{alias.upper()}__{to_type_name(field)}" for an aliased domain.
    for field in ["orders", "userByName"]:
        assert naming.prefixed_type_name("sa", field) == "SA__" + naming.to_type_name(field)


# ── root-field variants (B9) ──


@pytest.mark.parametrize(
    ("convention", "kind", "expected"),
    [
        ("apollo_graphql", "aggregate", "ordersAggregate"),
        ("apollo_graphql", "group_by", "ordersGroupBy"),
        ("apollo_graphql", "connection", "orders_connection"),
        ("hasura_graphql", "aggregate", "orders_aggregate"),
        ("snake", "group_by", "orders_group_by"),
        ("snake", "connection", "orders_connection"),
    ],
)
def test_root_field_variant(convention, kind, expected):
    assert naming.root_field_variant("orders", kind, convention) == expected


def test_root_field_variant_refuses_unknown_kind():
    with pytest.raises(ValueError, match="not a root-field variant"):
        naming.root_field_variant("orders", "count", "apollo_graphql")


# ── GraphQL enum names (B10) ──


@pytest.mark.parametrize(
    ("raw", "type_name", "value"),
    [
        ("order_status", "order_status_Enum", "ORDER_STATUS"),
        ("1st-class", "e_1st_class_Enum", "V_1ST_CLASS"),
        ("in progress", "in_progress_Enum", "IN_PROGRESS"),
        ("", "_Enum", "V_"),
    ],
)
def test_gql_enum_names(raw, type_name, value):
    assert naming.gql_enum_type_name(raw) == type_name
    assert naming.gql_enum_value_name(raw) == value


@pytest.mark.parametrize("raw", ["order_status", "1st-class", "in progress", "", "a.b", "Ünï"])
def test_gql_enum_names_match_enum_detect(raw):
    from provisa.compiler.enum_detect import _sanitize_enum_name, _sanitize_enum_value

    assert naming.gql_enum_type_name(raw) == _sanitize_enum_name(raw)
    assert naming.gql_enum_value_name(raw) == _sanitize_enum_value(raw)


# ── Cypher relationship types (B11, B12, B13) ──


@pytest.mark.parametrize(
    ("field", "cardinality", "expected"),
    [
        ("animalBreed", "many-to-one", "IS_ANIMAL_BREED"),
        ("owner", "one-to-one", "IS_OWNER"),
        ("tableColumns", "one-to-many", "HAS_TABLE_COLUMNS"),
        ("_queries", None, "HAS_QUERIES"),
    ],
)
def test_cql_rel_type(field, cardinality, expected):
    assert naming.cql_rel_type(field, cardinality) == expected


@pytest.mark.parametrize("field", ["animalBreed", "tableColumns", "_queries", "pet_owner"])
@pytest.mark.parametrize("cardinality", ["many-to-one", "one-to-many", "one-to-one", None])
def test_cql_rel_type_matches_label_map(field, cardinality):
    from provisa.gql.label_map import _to_rel_type

    assert naming.cql_rel_type(field, cardinality) == _to_rel_type(field, cardinality)


def test_cql_rel_type_takes_the_label_map_rule_for_ops_joins():
    # B13 (context.py ops joins) upper-cased the field whole: HAS_TABLECOLUMNS. The authority
    # keeps the label map's rule (B11), which splits the words: HAS_TABLE_COLUMNS.
    assert naming.cql_rel_type("tableColumns", "one-to-many") == "HAS_TABLE_COLUMNS"


@pytest.mark.parametrize(
    ("label", "expected"),
    [
        ("bonded pair", "BONDED_PAIR"),
        ("bondedPair", "BONDED_PAIR"),
        ("pet__companions", "PET_COMPANIONS"),
    ],
)
def test_cql_junction_rel_type(label, expected):
    assert naming.cql_junction_rel_type(label) == expected


@pytest.mark.parametrize("label", ["bonded pair", "bondedPair", "LITTER-MATE", "x1Y", "_a__b_"])
def test_cql_junction_rel_type_matches_label_map(label):
    from provisa.gql.label_map import _upper_snake

    assert naming.cql_junction_rel_type(label) == _upper_snake(label)


# ── proto names (B4, B5) ──


@pytest.mark.parametrize(
    ("gql_type", "proto"),
    [("PS__Pets", "PsPets"), ("Orders", "Orders"), ("SA__UserByName", "SaUserByName")],
)
def test_proto_type_name(gql_type, proto):
    assert naming.proto_type_name(gql_type) == proto


def test_proto_names_match_proto_gen():
    from provisa.grpc.proto_gen import _to_proto_field_name, _to_proto_type_name, command_rpc_name

    for name in ["PS__Pets", "Orders", "SA__UserByName", "ps__pets", "orders"]:
        assert naming.proto_type_name(name) == _to_proto_type_name(name)
        assert naming.proto_field_name(name) == _to_proto_field_name(name)
    for fn in ["active_users", "activeUsers", "sa__top_pets"]:
        assert naming.proto_command_name(fn) == command_rpc_name(fn)


def test_proto_command_rpc_keeps_inner_capitals():
    assert naming.proto_command_rpc("activeUsers") == "CallActiveUsers"
    assert naming.proto_command_rpc("active_users") == "CallActiveUsers"


def test_proto_table_rpcs():
    assert naming.proto_table_rpcs("PsPets") == {
        "query": "QueryPsPets",
        "batch": "QueryPsPetsBatch",
        "aggregate": "QueryPsPetsAggregate",
        "group_by": "QueryPsPetsGroupBy",
        "insert": "InsertPsPets",
    }


# ── pg constraint names (B15) ──


def test_pg_constraint_names():
    assert naming.pg_pk_constraint_name("orders") == "pk_orders"
    assert naming.pg_fk_constraint_name("orders", "customer_id") == "fk_orders__customer_id"
    assert naming.pg_fk_constraint_name("lines", "orders") == "fk_lines__orders"
    assert naming.pg_meta_fk_constraint_name("orders") == "fk_orders___meta"


# ── name suggestions ──


@pytest.mark.parametrize(
    ("gql", "cql", "convention", "expected"),
    [
        ("ownedBy", "OWNED_BY", "apollo_graphql", ("owned", "OWNED")),
        ("partOf", "PART_OF", "apollo_graphql", ("part", "PART")),
        ("owner", "OWNER", "apollo_graphql", ("ownerBy", "OWNER_BY")),
        (None, "OPENED", "apollo_graphql", ("openedBy", "OPENED_BY")),
        (None, "OPENED", "hasura_graphql", ("opened_by", "OPENED_BY")),
        (None, None, "apollo_graphql", ("", "")),
    ],
)
def test_suggest_reverse_rel_alias(gql, cql, convention, expected):
    assert naming.suggest_reverse_rel_alias(gql, cql, convention) == expected


def test_suggest_table_alias_prefixes_source_then_counts():
    held = {"orders"}
    assert naming.suggest_table_alias("Orders", "pg-b", held.__contains__, "snake") == "pg_b_orders"
    held |= {"pg_b_orders", "pg_b_orders_1"}
    assert (
        naming.suggest_table_alias("Orders", "pg-b", held.__contains__, "snake") == "pg_b_orders_2"
    )
    assert naming.suggest_table_alias("Orders", "pg-b", set().__contains__, "snake") == "orders"


def test_suggest_column_alias_matches_register_form():
    from provisa.api.admin.schema_mutation import _column_alias

    for col in ["orderId", "order_id", "HTTPServer", "Name"]:
        assert naming.suggest_column_alias(col) == _column_alias(col)
    assert naming.suggest_column_alias("orderId") == "order_id"
    assert naming.suggest_column_alias("order_id") is None


def test_suggest_metric_and_junction_names():
    assert naming.suggest_metric_name("orders", "amount", "sum") == "orders_amount_sum"
    assert naming.suggest_metric_name("Orders", "unitPrice", "avg") == "orders_unit_price_avg"
    assert naming.suggest_junction_name("pets", "owners") == "pets_owners"


@pytest.mark.parametrize(
    ("operation_id", "alias"),
    [("findPetsByStatus", "pet_by_status"), ("listPets", "pet"), ("getOrderById", "order_by_id")],
)
def test_openapi_operation_alias(operation_id, alias):
    assert naming.openapi_operation_alias(operation_id) == alias


def test_rel_field_name_unchanged_by_the_word_split():
    assert naming.rel_field_name("findPetsByStatus", "many-to-one") == "petByStatus"
    assert naming.rel_field_name("orders", "one-to-many") == "orders"


# ── clash policy: disambiguation ──


def test_disambiguate_from_the_distinguishing_attribute():
    none_taken = set().__contains__
    assert naming.disambiguate("relationship", "customers", "buyer_id", none_taken) == (
        "customers_by_buyer_id"
    )
    assert naming.disambiguate("table", "orders", "pg-b", none_taken) == "pg_b_orders"
    assert naming.disambiguate("junction", "pets", "owners", none_taken) == "pets_owners"


def test_disambiguate_counts_only_without_an_attribute():
    assert naming.disambiguate("column", "name", None, {"name"}.__contains__) == "name_2"
    assert naming.disambiguate("column", "name", None, {"name", "name_2"}.__contains__) == "name_3"
    taken = {"pg_b_orders"}.__contains__
    assert naming.disambiguate("table", "orders", "pg-b", taken) == "pg_b_orders_2"


# ── name checks ──


@pytest.mark.parametrize(
    ("name", "surface", "codes"),
    [
        ("select", "sql", ["reserved"]),
        ("orders", "sql", []),
        ("1st_quarter", "sql", ["leading_digit"]),
        ("unit price", "sql", ["illegal_char"]),
        ("Orders", "sql", ["illegal_char"]),
        ("a" * 64, "sql", ["too_long"]),
        ("a" * 63, "sql", []),
        ("match", "cypher", ["reserved"]),
        ("Query", "graphql", ["reserved"]),
        ("__typename", "graphql", ["reserved"]),
        ("ordersAggregate", "graphql", []),
        ("fk_" + "x" * 70, "pg_constraint", ["too_long"]),
    ],
)
def test_name_problems(name, surface, codes):
    assert [code for code, _ in naming.name_problems(name, surface)] == codes


def test_name_problems_refuses_unknown_surface():
    with pytest.raises(ValueError, match="not a surface"):
        naming.name_problems("x", "rest")


@pytest.mark.parametrize(
    ("physical", "codes"),
    [
        ("order_id", []),
        ("orderId", []),
        ("Order_ID", ["lossy"]),
        ("H_T_T_P_server", ["lossy", "reads_badly"]),
        ("http_server", []),
        ("a__b__c__d", ["lossy", "long_prefix_chain"]),
    ],
)
def test_conversion_problems(physical, codes):
    assert [code for code, _ in naming.conversion_problems(physical)] == codes
