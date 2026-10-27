# Copyright (c) 2026 Kenneth Stott
# Canary: a5c3cce3-f7f1-48ee-b217-717d7a4e7fb9
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A code table's writes bound to its source's commands (REQ-2100): by their input types -- insert
takes a list of the row type, update the keys and a partial row, delete the keys -- a tag deciding
between two that fit, two with no tag refused by name; and the key read an UPDATE or a DELETE makes
of the rows it touches, over its own FROM and WHERE, its SET computed per row."""

# Requirements: REQ-2100
from __future__ import annotations

import pytest
import sqlglot

from provisa.codesource.manifest import ManifestRefused, read_manifest
from provisa.codesource.mutations import (
    DELETE,
    INSERT,
    UPDATE,
    MutationRefused,
    bindings,
    inserted_rows,
    key_read,
    keys_of,
)

pytestmark = pytest.mark.unit

N = {"kind": "number", "integer": True}
S = {"kind": "string"}


def _opt(t: dict) -> dict:
    return {"kind": "union", "name": None, "members": [{"kind": "null"}, t]}


TYPES = {
    "Order": {
        "kind": "object",
        "name": "Order",
        "fields": [
            {"name": "id", "description": "", "optional": False, "type": N},
            {"name": "status", "description": "", "optional": False, "type": S},
        ],
    },
    "OrderKey": {
        "kind": "object",
        "name": "OrderKey",
        "fields": [{"name": "id", "description": "", "optional": False, "type": N}],
    },
    "OrderChanges": {
        "kind": "object",
        "name": "OrderChanges",
        "fields": [{"name": "status", "description": "", "optional": True, "type": _opt(S)}],
    },
}


def _fn(name, tags, params, returns, tag_arguments=()):
    return {
        "name": name,
        "tags": tags,
        "tag_arguments": [{"tag": t, "text": a} for t, a in tag_arguments],
        "description": "",
        "signatures": [
            {
                "parameters": [
                    {"name": p, "optional": False, "rest": False, "type": t} for p, t in params
                ],
                "returns": returns,
            }
        ],
    }


def _ref(name):
    return {"kind": "ref", "name": name}


def _list(t):
    return {"kind": "array", "element": t}


ORDERS = _fn("orders", ["table"], [], _list(_ref("Order")))
ADD = _fn("add", ["command"], [("rows", _list(_ref("Order")))], N)
SET = _fn(
    "set_", ["command"], [("keys", _list(_ref("OrderKey"))), ("changes", _ref("OrderChanges"))], N
)
DROP = _fn("drop", ["command"], [("keys", _list(N))], N)


def _manifest(*exports):
    return read_manifest({"exports": [ORDERS, *exports], "types": TYPES, "diagnostics": []})


def test_each_operation_is_bound_to_the_command_its_input_type_fits():
    bound = bindings(_manifest(ADD, SET, DROP), "orders", ["id"])
    assert {op: b.command for op, b in bound.items()} == {
        INSERT: "add",
        UPDATE: "set_",
        DELETE: "drop",
    }
    assert bound[DELETE].bare_keys and not bound[UPDATE].bare_keys
    assert bound[UPDATE].names == ("keys", "changes")


def test_keys_of_another_type_than_the_key_column_fit_no_operation():
    wrong = _fn("drop_text", ["command"], [("keys", _list(S))], N)
    assert DELETE not in bindings(_manifest(wrong), "orders", ["id"])
    # And with no primary key, only an insert binds.
    assert set(bindings(_manifest(ADD, DROP), "orders", [])) == {INSERT}


def test_two_commands_that_fit_one_operation_are_refused_unless_a_tag_tells_them_apart():
    other = _fn("drop_too", ["command"], [("keys", _list(N))], N)
    with pytest.raises(MutationRefused, match="the commands drop, drop_too all fit delete"):
        bindings(_manifest(DROP, other), "orders", ["id"])
    tagged = _fn("drop_too", ["command"], [("keys", _list(N))], N, (("delete", "orders"),))
    assert bindings(_manifest(DROP, tagged), "orders", ["id"])[DELETE].command == "drop_too"


def test_a_tag_naming_a_command_that_does_not_fit_is_refused_by_name():
    tagged = _fn("add", ["command"], [("rows", _list(_ref("Order")))], N, (("delete", "orders"),))
    with pytest.raises(MutationRefused, match="@delete orders on add: its input type is not"):
        bindings(_manifest(tagged), "orders", ["id"])


def test_a_write_tag_on_anything_but_a_command_is_refused():
    table = _fn("orders2", ["table"], [], _list(_ref("Order")), (("insert", "orders"),))
    with pytest.raises(ManifestRefused, match="@insert binds a command to a table"):
        _manifest(table)


def test_an_updates_key_read_is_its_from_and_where_with_its_set_computed_per_row():
    tree = sqlglot.parse_one(
        "UPDATE orders SET total = total * 2, status = 'held' FROM regions AS r "
        "WHERE r.code = orders.region AND orders.id > 1",
        read="postgres",
    )
    sql, columns = key_read(tree, ["id"])
    assert columns == ["total", "status"]
    assert sql == (
        'SELECT "orders"."id", total * 2 AS "__set_total", \'held\' AS "__set_status" FROM orders '
        "CROSS JOIN regions AS r WHERE r.code = orders.region AND orders.id > 1"
    )


def test_a_deletes_key_read_is_its_where():
    tree = sqlglot.parse_one("DELETE FROM orders WHERE id = 3", read="postgres")
    assert key_read(tree, ["id"]) == ('SELECT "orders"."id" FROM orders WHERE id = 3', [])


def test_an_inserts_rows_are_its_values_literals_and_parameters():
    tree = sqlglot.parse_one(
        "INSERT INTO orders (id, status) VALUES (1, 'open'), ($1, NULL)", read="postgres"
    )
    assert inserted_rows(tree, [2]) == [{"id": 1, "status": "open"}, {"id": 2, "status": None}]
    selected = sqlglot.parse_one("INSERT INTO orders (id) SELECT id FROM o", read="postgres")
    assert inserted_rows(selected, None) is None  # its rows are read
    computed = sqlglot.parse_one("INSERT INTO orders (id) VALUES (1 + 1)", read="postgres")
    with pytest.raises(MutationRefused, match="a literal or a parameter"):
        inserted_rows(computed, None)


def test_keys_are_objects_of_the_key_columns_or_bare_values():
    rows = [(1, "x"), (2, "y")]
    assert keys_of(rows, ["id"], bare=True) == [1, 2]
    assert keys_of(rows, ["id"], bare=False) == [{"id": 1}, {"id": 2}]
