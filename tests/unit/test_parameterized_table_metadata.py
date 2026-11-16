# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1986, A PARAMETERIZED TABLE STAYS A TABLE; ITS LIMITS ARE STATED IN ITS METADATA: where a
table's facts are published, a table that requires parameters says that it does, which ones, of
what type, each opaque or a real column, and that it gives no row count, profile or sample
without them. An ordinary table says nothing of the kind.

The one place that answers is provisa.core.parameters. The meta schema's views say the same in
SQL, read here from the real views on a real control plane."""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from provisa.api._meta_views import _META_TABLE_VIEWS
from provisa.core import schema_org
from provisa.core.parameters import (
    ParameterFact,
    RequiresParameters,
    describe_limits,
    is_opaque,
    limits_statement,
    not_read_reason,
    parameter_facts,
    required_of,
    whole_read_refusal,
)

_ORG_SCHEMA = "org_default"

ORDER_BY_ID, ORDERS, PETS_BY_STATUS, EVENTS = 1, 2, 3, 4


def _parameter(name: str, kind: str, source: bool, steward: bool = False, dtype: str = "text"):
    return {
        "column_name": name,
        "data_type": dtype,
        "native_filter_type": kind,
        "native_filter_required": source,
        "required_by_steward": steward,
    }


def _column(name: str, dtype: str = "integer") -> dict:
    return {
        "column_name": name,
        "data_type": dtype,
        "native_filter_type": None,
        "native_filter_required": None,
        "required_by_steward": False,
    }


# get_order_by_id requires an opaque path parameter; orders is ordinary; find_pets_by_status has
# one optional parameter, and one a steward made required; events requires a real column.
_COLUMNS: dict[int, list[dict]] = {
    ORDER_BY_ID: [_column("id"), _parameter("_nf_orderId", "path_param", True, dtype="integer")],
    ORDERS: [_column("id"), _column("total")],
    PETS_BY_STATUS: [
        _column("id"),
        _parameter("_nf_status", "query_param", False),
        _parameter("_nf_tags", "query_param", False, steward=True),
    ],
    EVENTS: [_column("id"), _parameter("_nf_tenant", "partition_key", True)],
}
_NAMES = {
    ORDER_BY_ID: "get_order_by_id",
    ORDERS: "orders",
    PETS_BY_STATUS: "find_pets_by_status",
    EVENTS: "events",
}
_DESCRIPTIONS = {ORDER_BY_ID: "One order", ORDERS: "The orders"}

_ORDER_LIMITS = (
    "Requires parameters: orderId (integer, opaque). "
    "Gives no row count, profile or sample without them."
)


# -- the one place that answers ---------------------------------------------------------------------


@pytest.mark.parametrize(
    ("kind", "opaque"),
    [
        ("path_param", True),
        ("query_param", True),
        ("graphql_arg", True),
        ("grpc_input", True),
        ("query_arg", True),
        ("partition_key", False),
        ("column_value", False),
    ],
)
def test_a_parameter_is_opaque_unless_it_is_a_real_column_of_the_table(kind, opaque):
    from provisa.core.parameters import PARAMETER_KINDS

    assert kind in PARAMETER_KINDS
    assert is_opaque(_parameter("_nf_x", kind, True)) is opaque


def test_every_kind_of_parameter_is_classified():
    from provisa.core.parameters import PARAMETER_KINDS, REAL_COLUMN_KINDS

    assert REAL_COLUMN_KINDS < PARAMETER_KINDS


def test_a_tables_parameters_are_stated_with_name_type_kind_opaque_and_who_requires_each():
    assert parameter_facts(_COLUMNS[PETS_BY_STATUS]) == [
        ParameterFact("status", "_nf_status", "text", "query_param", True, False, False),
        ParameterFact("tags", "_nf_tags", "text", "query_param", True, False, True),
    ]
    facts = parameter_facts(_COLUMNS[EVENTS])
    assert [(f.name, f.opaque, f.required) for f in facts] == [("tenant", False, True)]
    assert facts[0].as_dict() == {
        "name": "tenant",
        "column": "_nf_tenant",
        "type": "text",
        "kind": "partition_key",
        "opaque": False,
        "required": True,
        "required_by_source": True,
        "required_by_steward": False,
    }
    assert parameter_facts(_COLUMNS[ORDERS]) == []


def test_a_parameter_held_as_a_column_and_as_its_argument_is_one_parameter():
    held = [
        {**_parameter("breed_name", "graphql_arg", True)},
        {**_parameter("_nf_breed_name", "graphql_arg", True)},
    ]
    assert [f.column for f in parameter_facts(held)] == ["breed_name"]
    assert [f.column for f in parameter_facts(reversed(held))] == ["breed_name"]


def test_a_table_requires_parameters_when_the_source_or_a_steward_requires_one():
    assert required_of(parameter_facts(_COLUMNS[ORDER_BY_ID])) == ["orderId"]
    assert required_of(parameter_facts(_COLUMNS[PETS_BY_STATUS])) == ["tags"]
    assert required_of(parameter_facts(_COLUMNS[ORDERS])) == []


def test_the_refusal_of_a_whole_read_is_one_code_naming_the_table_and_its_parameters():
    assert whole_read_refusal("orders", []) is None
    refusal = whole_read_refusal("get_order_by_id", ["orderId", "region"])
    assert isinstance(refusal, RequiresParameters)
    assert refusal.code == "table.requires_parameters"
    assert refusal.params == {"table": "get_order_by_id", "parameters": "orderId, region"}
    assert str(refusal) == (
        "Table 'get_order_by_id' cannot be counted or profiled: it requires orderId, region"
    )
    assert not_read_reason(["orderId"]) in str(whole_read_refusal("t", ["orderId"]))


def test_the_comment_states_the_limits_after_the_description_and_only_for_such_a_table():
    assert limits_statement(parameter_facts(_COLUMNS[ORDER_BY_ID])) == _ORDER_LIMITS
    assert describe_limits("One order", _COLUMNS[ORDER_BY_ID]) == f"One order {_ORDER_LIMITS}"
    assert describe_limits(None, _COLUMNS[ORDER_BY_ID]) == _ORDER_LIMITS
    assert describe_limits("The orders", _COLUMNS[ORDERS]) == "The orders"
    assert describe_limits(None, _COLUMNS[ORDERS]) is None
    assert "tenant (text, real column)" in limits_statement(parameter_facts(_COLUMNS[EVENTS]))
    # An optional parameter is no limit: it is left out of the sentence.
    assert limits_statement(parameter_facts(_COLUMNS[PETS_BY_STATUS])) == (
        "Requires parameters: tags (text, opaque). "
        "Gives no row count, profile or sample without them."
    )


# -- the catalogs a client reads --------------------------------------------------------------------


def test_the_graphql_type_description_states_the_limits():
    from provisa.compiler.schema_gen import _type_description

    def info(table_id: int, **more) -> SimpleNamespace:
        return SimpleNamespace(
            description=_DESCRIPTIONS.get(table_id),
            modeling_role=None,
            modeling_history=None,
            tags=(),
            native_filter_columns=[c for c in _COLUMNS[table_id] if c["native_filter_type"]],
            **more,
        )

    assert _type_description(info(ORDER_BY_ID)) == f"One order {_ORDER_LIMITS}"  # pyright: ignore[reportArgumentType]
    assert _type_description(info(ORDERS)) == "The orders"  # pyright: ignore[reportArgumentType]
    fact = info(ORDER_BY_ID)
    fact.modeling_role = "fact"
    assert _type_description(fact) == f"One order {_ORDER_LIMITS} [fact]"  # pyright: ignore[reportArgumentType]


def test_the_pgwire_catalog_comment_states_the_limits():
    from provisa.pgwire import catalog_populate

    raw = [
        {
            "id": tid,
            "description": _DESCRIPTIONS.get(tid),
            "columns": _COLUMNS[tid],
            "modeling_role": None,
            "modeling_history": None,
            "tags": (),
        }
        for tid in (ORDER_BY_ID, ORDERS)
    ]
    described = _pgwire_descriptions(catalog_populate, raw)
    assert described[ORDER_BY_ID] == f"One order {_ORDER_LIMITS}"
    assert described[ORDERS] == "The orders"


def _pgwire_descriptions(catalog_populate, raw: list[dict]) -> dict[int, str]:
    """Each table's comment as pgwire's catalog describes it (its pg_description row), by table
    id: the table's oid here is its id."""

    class _Db:
        rows: list[tuple] = []

        def executemany(self, _sql: str, rows: list[tuple]) -> None:
            self.rows = rows

    db = _Db()
    idx = SimpleNamespace(
        ns_map={},
        tables=[(None, None, None, rt["id"], rt["id"]) for rt in raw],
        col_attnum={},
        metric_oids={},
    )
    catalog_populate._populate_pg_description(db, idx, raw)
    return {oid: text for oid, _class, sub, text in db.rows if sub == 0}


def test_the_graph_schema_states_each_labels_parameters():
    from provisa.gql.label_map import NodeMapping

    def node(table_id: int) -> NodeMapping:
        return NodeMapping(
            label=_NAMES[table_id],
            type_name=_NAMES[table_id],
            domain_label="Shop",
            table_label=_NAMES[table_id],
            table_id=table_id,
            source_id="src",
            id_column="id",
            pk_columns=["id"],
            catalog_name="",
            schema_name="shop",
            table_name=_NAMES[table_id],
            properties={"id": "id"},
            parameters=tuple(parameter_facts(_COLUMNS[table_id])),
        )

    assert node(ORDER_BY_ID).required_parameters == ["orderId"]
    assert node(ORDERS).required_parameters == []
    assert node(ORDERS).parameters == ()
    # An optional parameter alone does not make a label one that requires parameters.
    optional = NodeMapping(
        **{
            **{f: getattr(node(ORDERS), f) for f in NodeMapping.__dataclass_fields__},
            "parameters": tuple(parameter_facts([_parameter("_nf_status", "query_param", False)])),
        }
    )
    assert optional.required_parameters == []
    assert [f.name for f in optional.parameters] == ["status"]


def test_the_compiled_context_holds_each_tables_parameters():
    """What the label map, the profiler and data quality read a table's limits from."""
    import inspect

    from provisa.compiler import context
    from provisa.compiler.sql_types import CompilationContext

    assert CompilationContext().parameters == {}
    assert "ctx.parameters[t.table_id] = tuple(" in inspect.getsource(
        context._register_table_in_ctx
    )


# -- the meta schema's views, on a real control plane -----------------------------------------------


@pytest.fixture(scope="module")
def live(embedded_postgres, control_plane_template):
    from tests.pg_control_plane import clone

    plane = clone(embedded_postgres, control_plane_template)
    engine = plane.engine(f"{plane.url}?options=-csearch_path%3D{_ORG_SCHEMA},public")
    t = schema_org
    with engine.begin() as conn:
        conn.execute(t.sources.insert().values(id="src", type="postgresql"))
        conn.execute(t.domains.insert().values(id="shop", description="The shop"))
        for table_id, name in _NAMES.items():
            conn.execute(
                t.registered_tables.insert().values(
                    id=table_id,
                    source_id="src",
                    domain_id="shop",
                    schema_name="shop",
                    table_name=name,
                    description=_DESCRIPTIONS.get(table_id),
                )
            )
            for c in _COLUMNS[table_id]:
                conn.execute(
                    t.table_columns.insert().values(table_id=table_id, domain_id="shop", **c)
                )
        for view in ("registered_tables", "table_columns", "table_details"):
            conn.exec_driver_sql(_META_TABLE_VIEWS[view].replace("%", "%%"))
    yield engine
    plane.close()


def _rows(engine, sql: str) -> list[tuple]:
    with engine.begin() as conn:
        return [tuple(r) for r in conn.exec_driver_sql(sql).fetchall()]


def test_the_meta_view_of_tables_says_which_require_parameters(live):
    assert _rows(
        live, "SELECT table_name, requires_parameters FROM registered_tables_meta ORDER BY id"
    ) == [
        ("get_order_by_id", True),
        ("orders", False),
        ("find_pets_by_status", True),  # a steward made tags required
        ("events", True),
    ]


def test_the_meta_view_of_columns_states_each_parameters_limits(live):
    stated = _rows(
        live,
        "SELECT table_id, column_name, data_type, native_filter_type, "
        "parameter_required_by_source, parameter_required_by_steward, parameter_required, "
        "parameter_opaque FROM table_columns_meta ORDER BY table_id, id",
    )
    assert stated == [
        (ORDER_BY_ID, "id", "integer", None, None, None, None, None),
        (ORDER_BY_ID, "_nf_orderId", "integer", "path_param", True, False, True, True),
        (ORDERS, "id", "integer", None, None, None, None, None),
        (ORDERS, "total", "integer", None, None, None, None, None),
        (PETS_BY_STATUS, "id", "integer", None, None, None, None, None),
        (PETS_BY_STATUS, "_nf_status", "text", "query_param", False, False, False, True),
        (PETS_BY_STATUS, "_nf_tags", "text", "query_param", False, True, True, True),
        (EVENTS, "id", "integer", None, None, None, None, None),
        (EVENTS, "_nf_tenant", "text", "partition_key", True, False, True, False),
    ]


def test_the_views_and_the_one_place_agree_on_every_table(live):
    """The SQL of the views and provisa.core.parameters state the same facts."""
    requires = dict(_rows(live, "SELECT id, requires_parameters FROM registered_tables_meta"))
    opaque = {
        (tid, name): (required, is_opaque_)
        for tid, name, required, is_opaque_ in _rows(
            live,
            "SELECT table_id, column_name, parameter_required, parameter_opaque "
            "FROM table_columns_meta WHERE native_filter_type IS NOT NULL",
        )
    }
    for table_id, columns in _COLUMNS.items():
        facts = parameter_facts(columns)
        assert requires[table_id] is bool(required_of(facts))
        for f in facts:
            assert opaque[(table_id, f.column)] == (f.required, f.opaque)


def test_the_pg_catalog_comment_states_the_limits_as_every_catalog_does(live):
    comments = dict(_rows(live, "SELECT id, comment FROM table_details"))
    for table_id, columns in _COLUMNS.items():
        assert comments[table_id] == describe_limits(_DESCRIPTIONS.get(table_id), columns)
    assert comments[ORDER_BY_ID] == f"One order {_ORDER_LIMITS}"
    assert comments[ORDERS] == "The orders"
    assert comments[EVENTS] == (
        "Requires parameters: tenant (text, real column). "
        "Gives no row count, profile or sample without them."
    )


def test_every_new_meta_column_is_described():
    from provisa.api._catalog_descriptions import COLUMN_DESCRIPTIONS

    assert "requires_parameters" in COLUMN_DESCRIPTIONS["registered_tables"]
    for column in (
        "parameter_required_by_source",
        "parameter_required_by_steward",
        "parameter_required",
        "parameter_opaque",
    ):
        assert column in COLUMN_DESCRIPTIONS["table_columns"]
