# Copyright (c) 2026 Kenneth Stott
# Canary: 86ae1261-8934-4aa5-bb1a-b3dad0ea116a
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A role writes a table by an operation only through its write grant for it (REQ-2074).

A grant names the operation (insert, update, delete), the columns an INSERT may list or an
UPDATE may set, the rows an UPDATE or DELETE may touch, the check every row an INSERT writes or
an UPDATE leaves must satisfy, and the presets it sets itself. A grant that states no rows or
check takes the role's read row filter on the table; TRUE is every row. Every operation is
refused without its grant, outside its columns, outside its rows and against its check."""

# Requirements: REQ-2074, REQ-1925, REQ-868

from __future__ import annotations

import pytest

from provisa.compiler.stage2 import GovernanceContext, apply_governance
from provisa.compiler.write_admission import WriteNotAdmitted, admit_rows
from provisa.security.mutation_authz import ColumnNotWritable

_ORDERS = 1
_COLUMNS = [
    ("id", "integer"),
    ("customer_id", "integer"),
    ("status", "varchar"),
    ("ship_to", "varchar"),
]


def _g(operation, columns=(), rows=None, check=None, presets=()):
    return {
        "role": "customer",
        "operation": operation,
        "columns": list(columns),
        "rows": rows,
        "check": check,
        "presets": list(presets),
    }


def _gov(*grants, rls: str | None = None, can_write: bool = True) -> GovernanceContext:
    gov = GovernanceContext()
    gov.role_id = "customer"
    gov.can_write = can_write
    gov.table_map = {"public.orders": _ORDERS, "orders": _ORDERS}
    gov.all_columns = {_ORDERS: list(_COLUMNS)}
    gov.write_grants = {_ORDERS: {g["operation"]: g for g in grants}}
    gov.rls_rules = {_ORDERS: rls} if rls else {}
    gov.write_ops = {_ORDERS: frozenset({"insert", "update", "delete"})}
    gov.write_returns_rows = {_ORDERS: True}
    gov.write_refused_forms = {_ORDERS: frozenset()}
    return gov


_ME = {"user_id": "7"}
_OWN = "customer_id = current_setting('provisa.user_id')"
_INSERT = _g(
    "insert",
    ["ship_to"],
    check=_OWN,
    presets=[
        {"column": "customer_id", "source": "session", "name": "user_id"},
        {"column": "status", "source": "literal", "value": "placed"},
    ],
)
_UPDATE = _g(
    "update",
    ["status", "ship_to"],
    rows="status IN ('placed', 'packed')",
    check="status IN ('placed', 'packed', 'cancelled')",
)
_DELETE = _g("delete", rows="status = 'placed'")


def _govern(sql: str, gov: GovernanceContext, session=_ME, params=None) -> str:
    return apply_governance(sql, gov, session_vars=session, params=params)


# --- no grant: every operation refused ---------------------------------------------------------


@pytest.mark.parametrize(
    ("sql", "operation"),
    [
        ("INSERT INTO public.orders (ship_to) VALUES ('1 Elm')", "insert"),
        ("UPDATE public.orders SET status = 'packed' WHERE id = 1", "update"),
        ("DELETE FROM public.orders WHERE id = 1", "delete"),
        ("TRUNCATE public.orders", "delete"),
        (
            "MERGE INTO public.orders USING s ON public.orders.id = s.id "
            "WHEN MATCHED THEN UPDATE SET status = s.status",
            "insert",
        ),
    ],
)
def test_each_operation_needs_its_grant(sql, operation):
    with pytest.raises(WriteNotAdmitted, match=f"holds no {operation} grant"):
        _govern(sql, _gov())


def test_an_update_grant_admits_no_insert_and_an_insert_grant_no_delete():
    with pytest.raises(WriteNotAdmitted, match="holds no insert grant"):
        _govern("INSERT INTO public.orders (status) VALUES ('placed')", _gov(_UPDATE))
    with pytest.raises(WriteNotAdmitted, match="holds no delete grant"):
        _govern("DELETE FROM public.orders WHERE id = 1", _gov(_INSERT))


def test_a_grant_still_needs_the_write_right():
    with pytest.raises(WriteNotAdmitted, match="does not hold the 'write' right"):
        _govern("DELETE FROM public.orders WHERE id = 1", _gov(_DELETE, can_write=False))


# --- columns -----------------------------------------------------------------------------------


def test_an_insert_may_list_only_its_grants_columns():
    with pytest.raises(ColumnNotWritable, match="'id'"):
        _govern("INSERT INTO public.orders (id, ship_to) VALUES (9, '1 Elm')", _gov(_INSERT))


def test_an_update_may_set_only_its_grants_columns():
    with pytest.raises(ColumnNotWritable, match="'customer_id'"):
        _govern("UPDATE public.orders SET customer_id = 3 WHERE id = 1", _gov(_UPDATE))


def test_a_preset_column_may_not_be_supplied():
    with pytest.raises(WriteNotAdmitted, match="'customer_id' is set by"):
        _govern(
            "INSERT INTO public.orders (ship_to, customer_id) VALUES ('1 Elm', 3)", _gov(_INSERT)
        )


# --- presets -----------------------------------------------------------------------------------


def test_an_insert_carries_its_presets():
    governed = _govern("INSERT INTO public.orders (ship_to) VALUES ('1 Elm')", _gov(_INSERT))
    assert "customer_id" in governed and "CAST('7' AS INT)" in governed
    assert "'placed'" in governed


def test_a_session_preset_needs_its_session_variable():
    with pytest.raises(WriteNotAdmitted, match="session variable 'user_id'"):
        _govern("INSERT INTO public.orders (ship_to) VALUES ('1 Elm')", _gov(_INSERT), session={})


def test_an_update_carries_its_presets():
    stamped = _g(
        "update",
        ["status"],
        rows="TRUE",
        check="TRUE",
        presets=[{"column": "ship_to", "source": "literal", "value": "held"}],
    )
    governed = _govern("UPDATE public.orders SET status = 'packed' WHERE id = 1", _gov(stamped))
    assert "ship_to = CAST('held' AS VARCHAR)" in governed


def test_a_now_preset_sets_the_time_of_the_write():
    stamped = _g(
        "update",
        ["status"],
        rows="TRUE",
        check="TRUE",
        presets=[{"column": "ship_to", "source": "now"}],
    )
    assert "CURRENT_TIMESTAMP" in _govern(
        "UPDATE public.orders SET status = 'packed' WHERE id = 1", _gov(stamped)
    )


# --- check -------------------------------------------------------------------------------------


def test_an_insert_satisfying_its_check_is_admitted():
    _govern("INSERT INTO public.orders (ship_to) VALUES ('1 Elm')", _gov(_INSERT))


def test_an_insert_failing_its_check_is_refused():
    other = _g("insert", ["ship_to", "customer_id"], check=_OWN)
    with pytest.raises(WriteNotAdmitted, match="outside"):
        _govern("INSERT INTO public.orders (ship_to, customer_id) VALUES ('1 Elm', 3)", _gov(other))


def test_an_update_whose_new_values_fail_its_check_is_refused():
    with pytest.raises(WriteNotAdmitted, match="outside"):
        _govern("UPDATE public.orders SET status = 'shipped' WHERE id = 1", _gov(_UPDATE))


def test_a_check_that_cannot_be_decided_is_refused():
    reads_unsupplied = _g("insert", ["ship_to"], check="status = 'placed'")
    with pytest.raises(WriteNotAdmitted, match="cannot be decided"):
        _govern("INSERT INTO public.orders (ship_to) VALUES ('1 Elm')", _gov(reads_unsupplied))


# --- rows --------------------------------------------------------------------------------------


def test_an_update_touches_only_its_grants_rows():
    governed = _govern("UPDATE public.orders SET status = 'packed' WHERE id = 1", _gov(_UPDATE))
    assert "'placed'" in governed and "'packed'" in governed and "IN" in governed


def test_a_delete_touches_only_its_grants_rows():
    governed = _govern("DELETE FROM public.orders WHERE id = 1", _gov(_DELETE))
    assert "\"status\" = 'placed'" in governed


def test_a_grant_stating_no_rows_takes_the_read_filter():
    governed = _govern("DELETE FROM public.orders WHERE id = 1", _gov(_g("delete"), rls=_OWN))
    assert "\"customer_id\" = CAST(CURRENT_SETTING('provisa.user_id') AS INT)" in governed


def test_a_grant_whose_rows_are_true_ignores_the_read_filter():
    governed = _govern(
        "DELETE FROM public.orders WHERE id = 1", _gov(_g("delete", rows="TRUE"), rls=_OWN)
    )
    assert "customer_id" not in governed


def test_a_truncate_needs_a_delete_grant_over_every_row():
    with pytest.raises(WriteNotAdmitted, match="TRUNCATE"):
        _govern("TRUNCATE public.orders", _gov(_DELETE))
    _govern("TRUNCATE public.orders", _gov(_g("delete", rows="TRUE")))


def test_a_merge_needs_an_insert_and_an_update_grant():
    every = [c for c, _ in _COLUMNS]
    merge = (
        "MERGE INTO public.orders USING s ON public.orders.id = s.id "
        "WHEN MATCHED THEN UPDATE SET status = s.status"
    )
    with pytest.raises(WriteNotAdmitted, match="holds no update grant"):
        _govern(merge, _gov(_g("insert", every, check="TRUE")))
    _govern(
        merge,
        _gov(_g("insert", every, check="TRUE"), _g("update", every, rows="TRUE", check="TRUE")),
    )


def test_an_upsert_needs_an_update_grant_over_every_row():
    upsert = (
        "INSERT INTO public.orders (ship_to) VALUES ('1 Elm') "
        "ON CONFLICT (id) DO UPDATE SET ship_to = EXCLUDED.ship_to"
    )
    with pytest.raises(WriteNotAdmitted, match="holds no update grant"):
        _govern(upsert, _gov(_INSERT))
    with pytest.raises(WriteNotAdmitted, match="ON CONFLICT"):
        _govern(upsert, _gov(_INSERT, _UPDATE))


# --- COPY --------------------------------------------------------------------------------------


def test_a_copy_needs_an_insert_grant_and_its_columns():
    with pytest.raises(WriteNotAdmitted, match="holds no insert grant"):
        admit_rows(_gov(_UPDATE), _ORDERS, "orders", ["status"])
    plain = _g("insert", ["ship_to", "customer_id"], check=_OWN)
    with pytest.raises(ColumnNotWritable):
        admit_rows(_gov(plain), _ORDERS, "orders", ["id"])
    with pytest.raises(WriteNotAdmitted, match="outside"):
        admit_rows(_gov(plain), _ORDERS, "orders", ["ship_to", "customer_id"], [["x", 3]], _ME)
    admit_rows(_gov(plain), _ORDERS, "orders", ["ship_to", "customer_id"], [["x", 7]], _ME)


def test_a_copy_into_a_grant_with_presets_is_refused():
    with pytest.raises(WriteNotAdmitted, match="preset"):
        admit_rows(_gov(_INSERT), _ORDERS, "orders", ["ship_to"])


# --- presets on the GraphQL surface --------------------------------------------------------
# A mutation compiles to the statement every surface's write is; the grant's presets are added
# where it is admitted, and its input offers no field for a preset column.


def _graphql(grant: dict):
    from provisa.compiler.context import build_context
    from provisa.compiler.introspect import ColumnMetadata
    from provisa.compiler.schema_gen import SchemaInput, generate_schema

    table = {
        "id": _ORDERS,
        "source_id": "shop",
        "domain_id": "sales",
        "schema_name": "public",
        "table_name": "orders",
        "columns": [{"column_name": c, "visible_to": ["customer"]} for c, _ in _COLUMNS],
        "write_ops": ["delete", "insert", "update"],
        "write_returns_rows": True,
        "write_refused_forms": [],
        "write_grants": [grant],
    }
    types = {"integer": "integer", "varchar": "varchar(40)"}
    si = SchemaInput(
        tables=[table],
        relationships=[],
        column_types={
            _ORDERS: [
                ColumnMetadata(column_name=c, data_type=types[t], is_nullable=True)
                for c, t in _COLUMNS
            ]
        },
        naming_rules=[],
        role={"id": "customer", "capabilities": ["write"], "domain_access": ["*"]},
        domains=[{"id": "sales", "description": ""}],
        source_types={"shop": "postgresql"},
    )
    return generate_schema(si), build_context(si)


def _mutate(grant: dict, mutation: str, session=_ME) -> str:
    from graphql import parse, validate

    from provisa.compiler.mutation_gen import compile_mutation

    schema, ctx = _graphql(grant)
    doc = parse(mutation)
    errors = validate(schema, doc)
    if errors:
        raise AssertionError(errors[0].message)
    (compiled,) = compile_mutation(doc, ctx, {"shop": "postgresql"})
    return _govern(compiled.sql, _gov(grant), session=session, params=compiled.params)


def test_a_graphql_insert_carries_the_grants_session_and_literal_presets():
    governed = _mutate(
        _INSERT, 'mutation { insertOrders(input: { shipTo: "1 Elm" }) { affected_rows } }'
    )
    assert "customer_id" in governed and "CAST('7' AS INT)" in governed
    assert "'placed'" in governed


def test_a_graphql_input_offers_no_field_for_a_preset_column():
    with pytest.raises(AssertionError, match="customerId"):
        _mutate(
            _INSERT,
            'mutation { insertOrders(input: { shipTo: "x", customerId: 3 }) { affected_rows } }',
        )


def test_a_graphql_insert_without_the_session_variable_is_refused():
    with pytest.raises(WriteNotAdmitted, match="session variable 'user_id'"):
        _mutate(
            _INSERT,
            'mutation { insertOrders(input: { shipTo: "1 Elm" }) { affected_rows } }',
            session={},
        )


def test_a_graphql_update_carries_a_now_preset():
    stamped = _g(
        "update",
        ["status"],
        rows="TRUE",
        check="TRUE",
        presets=[{"column": "ship_to", "source": "now"}],
    )
    governed = _mutate(
        stamped,
        'mutation { updateOrders(set: { status: "packed" }, where: { id: { eq: 1 } }) '
        "{ affected_rows } }",
    )
    assert "ship_to = CURRENT_TIMESTAMP" in governed


def test_an_empty_literal_preset_sets_the_empty_string():
    blank = _g(
        "insert",
        ["ship_to"],
        check="TRUE",
        presets=[{"column": "status", "source": "literal", "value": ""}],
    )
    governed = _govern("INSERT INTO public.orders (ship_to) VALUES ('x')", _gov(blank))
    assert "CAST('' AS VARCHAR)" in governed


def test_a_grant_without_presets_writes_the_statement_as_given():
    plain = _g("insert", ["ship_to"], check="TRUE")
    sql = "INSERT INTO public.orders (ship_to) VALUES ('x')"
    assert _govern(sql, _gov(plain)) == sql


def test_a_column_is_preset_at_most_once():
    from pydantic import ValidationError

    from provisa.core.models import WriteGrant

    with pytest.raises(ValidationError, match="preset at most once"):
        WriteGrant(
            role="r",
            operation="insert",
            presets=[
                {"column": "status", "source": "now"},
                {"column": "status", "source": "literal", "value": "x"},
            ],
        )
