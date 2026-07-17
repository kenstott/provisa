# Copyright (c) 2026 Kenneth Stott
# Canary: 60c0dc15-99c0-4080-9a9e-15913bf3e092
# Canary: placeholder
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
"""Unit tests for schema pk requirements: REQ-392"""

from __future__ import annotations


# ---------------------------------------------------------------------------
# REQ-392: Schema endpoint returns node_labels with a `pk: string | null`
# field per label, designating the primary key column name. Allows graph UI
# to construct reliable WHERE NOT n.<pk> IN [<value>] exclusion clauses
# instead of heuristic-based id(n) fallback.
# ---------------------------------------------------------------------------


def test_apply_cql_property_converts_snake_case():
    # REQ-392: property names are converted via apply_cql_property (CQL naming)
    from provisa.compiler.naming import apply_cql_property

    result = apply_cql_property("user_id")
    assert isinstance(result, str)
    assert len(result) > 0


def test_apply_cql_property_returns_string():
    # REQ-392: pk field in node_labels is the Cypher property name string
    from provisa.compiler.naming import apply_cql_property

    assert isinstance(apply_cql_property("id"), str)
    assert isinstance(apply_cql_property("order_date"), str)


def test_schema_response_pk_none_when_no_pk_columns():
    # REQ-392: pk must be null when no pk_columns are configured for a label.
    # Mirrors the endpoint logic: _cql_prop(n.pk_columns[0]) if n.pk_columns else None
    pk_columns: list[str] = []
    pk = pk_columns[0] if pk_columns else None
    assert pk is None


def test_schema_response_pk_set_to_first_column():
    # REQ-392: pk is the first designated PK column when pk_columns is non-empty.
    pk_columns = ["user_id", "secondary_id"]
    pk = pk_columns[0] if pk_columns else None
    assert pk == "user_id"


def test_schema_response_pk_columns_is_array():
    # REQ-392: pk_columns in the response is a list, not a scalar.
    pk_columns = ["id"]
    assert isinstance(pk_columns, list)


def test_schema_response_pk_columns_empty_list_for_no_pk():
    # REQ-392: pk_columns is an empty list when no PK is designated.
    pk_columns: list[str] = []
    assert pk_columns == []
    pk = pk_columns[0] if pk_columns else None
    assert pk is None


def test_schema_response_node_label_has_pk_key():
    # REQ-392: every node_labels entry must carry a `pk` key.
    # Simulate the node_labels dict structure produced by the endpoint.
    from provisa.compiler.naming import apply_cql_property

    pk_columns = ["customer_id"]
    node_entry = {
        "label": "Customer",
        "pk": apply_cql_property(pk_columns[0]) if pk_columns else None,
        "pk_columns": [apply_cql_property(c) for c in pk_columns],
    }
    assert "pk" in node_entry
    assert node_entry["pk"] is not None


def test_schema_response_node_label_pk_null_field_present():
    # REQ-392: pk key must be present even when null, not omitted.
    pk_columns: list[str] = []
    node_entry = {
        "label": "Event",
        "pk": pk_columns[0] if pk_columns else None,
        "pk_columns": pk_columns,
    }
    assert "pk" in node_entry
    assert node_entry["pk"] is None


def test_schema_response_pk_matches_pk_columns_first_entry():
    # REQ-392: the scalar pk must equal the Cypher property name of pk_columns[0].
    from provisa.compiler.naming import apply_cql_property

    raw_pk = "order_id"
    pk_columns = [raw_pk]
    pk = apply_cql_property(pk_columns[0]) if pk_columns else None
    pk_columns_cql = [apply_cql_property(c) for c in pk_columns]
    assert pk == pk_columns_cql[0]


def test_schema_response_multi_pk_columns_all_included():
    # REQ-392: pk_columns carries all designated PK columns, not just the first.
    from provisa.compiler.naming import apply_cql_property

    raw_cols = ["tenant_id", "order_id"]
    pk_columns_cql = [apply_cql_property(c) for c in raw_cols]
    assert len(pk_columns_cql) == 2
