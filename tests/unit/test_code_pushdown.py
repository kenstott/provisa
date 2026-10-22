# Copyright (c) 2026 Kenneth Stott
# Canary: c7a718c2-1871-4eaf-815d-148201bbc174
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Which predicates a code table's function takes (REQ-2100): declared by a parameter's name, by a
tag line, by an inline comment or by ``typing.Annotated``, all resolved by the manifest alike, and
refused by name when the function's types cannot take them."""

# Requirements: REQ-2100
from __future__ import annotations

import json
import subprocess
import sys
import textwrap
from pathlib import Path

import pytest

from provisa.codesource.manifest import (
    PYTHON_RULES,
    ManifestRefused,
    manifest_from_json,
    manifest_json,
    read_manifest,
)
from provisa.codesource.pushdown import Pushdown
from provisa.python.source import READER

pytestmark = pytest.mark.unit

S = {"kind": "string"}
N = {"kind": "number"}
DATE = {"kind": "date"}

ORDER = {
    "kind": "object",
    "name": "Order",
    "fields": [
        {"name": "id", "description": "", "optional": False, "type": N},
        {"name": "status", "description": "", "optional": False, "type": S},
        {"name": "score", "description": "", "optional": False, "type": N},
        {"name": "signup", "description": "", "optional": False, "type": DATE},
        {"name": "region", "description": "", "optional": False, "type": S},
        {"name": "customerName", "description": "", "optional": False, "type": S},
    ],
}


def _opt(t: dict) -> dict:
    return {"kind": "union", "name": None, "members": [{"kind": "undefined"}, t]}


def _param(name: str, t: dict, *, optional: bool = True, pushdown: tuple = ()) -> dict:
    return {
        "name": name,
        "description": "",
        "optional": optional,
        "rest": False,
        "type": _opt(t) if optional else t,
        "pushdown": list(pushdown),
    }


def _orders(*params: dict, kind: str = "table", tags: tuple = (), returns=None) -> dict:
    return {
        "exports": [
            {
                "name": "orders",
                "tags": [kind, *(t for t, _ in tags)],
                "tag_arguments": [{"tag": t, "text": p} for t, p in tags],
                "description": "",
                "signatures": [
                    {
                        "parameters": list(params),
                        "returns": returns
                        or {"kind": "array", "element": {"kind": "ref", "name": "Order"}},
                    }
                ],
            }
        ],
        "types": {"Order": ORDER},
        "diagnostics": [],
    }


def _pushes(raw: dict) -> dict[str, Pushdown | None]:
    return {p.name: p.pushes for p in read_manifest(raw).export("orders").parameters}


def _problems(raw: dict) -> list[str]:
    with pytest.raises(ManifestRefused) as caught:
        read_manifest(raw)
    return caught.value.problems


def test_a_parameter_named_as_a_return_column_takes_its_equality():
    assert _pushes(_orders(_param("status", S), _param("as_of", DATE))) == {
        "status": Pushdown("status", "eq"),
        "as_of": None,
    }


@pytest.mark.parametrize(
    "name, t, pushed",
    [
        ("ne_status", S, Pushdown("status", "ne")),
        ("gt_score", N, Pushdown("score", "gt")),
        ("gte_score", N, Pushdown("score", "gte")),
        ("lt_signup", DATE, Pushdown("signup", "lt")),
        ("lte_signup", DATE, Pushdown("signup", "lte")),
        ("in_region", {"kind": "array", "element": S}, Pushdown("region", "in")),
        ("like_status", S, Pushdown("status", "like")),
    ],
)
def test_a_parameter_named_op_column_takes_that_operator(name, t, pushed):
    assert _pushes(_orders(_param(name, t))) == {name: pushed}


def test_a_range_fills_both_of_its_parameters():
    assert _pushes(_orders(_param("gte_score", N), _param("lt_score", N))) == {
        "gte_score": Pushdown("score", "gte"),
        "lt_score": Pushdown("score", "lt"),
    }


def test_a_name_that_matches_only_after_normalization_is_not_pushed():
    # customerName is the raw column; customer_name is what the snake_case catalog calls it.
    assert _pushes(_orders(_param("customer_name", S), _param("eq_status", S))) == {
        "customer_name": None,
        "eq_status": None,  # eq is spelled by the bare column name, not a prefix
    }
    assert _pushes(_orders(_param("customerName", S))) == {
        "customerName": Pushdown("customerName", "eq")
    }


def test_op_column_is_an_ordinary_parameter_when_column_is_not_a_return_column():
    assert _pushes(_orders(_param("gt_price", N))) == {"gt_price": None}


@pytest.mark.parametrize(
    "param, why",
    [
        (_param("x", S, pushdown=("gt_score",)), "orders(x): gt on a double column takes a double"),
        (
            _param("x", S, pushdown=("in_region",)),
            "orders(x): in on a text column takes a list of text",
        ),
        (
            _param("x", N, pushdown=("like_score",)),
            "orders(x): like takes a text parameter on a text column",
        ),
        (_param("x", N, pushdown=("status",)), "orders(x): eq on a text column takes a text"),
    ],
)
def test_a_declaration_whose_type_does_not_fit_its_column_and_operator_is_refused(param, why):
    assert _problems(_orders(param)) == [why]


@pytest.mark.parametrize(
    "param",
    [
        _param("gt_threshold", N),
        _param("gt_score", S),
        _param("in_region", S),
        _param("like_score", N),
        _param("status", N),
    ],
)
def test_a_name_that_only_looks_like_a_pushdown_stays_a_plain_parameter(param):
    assert _pushes(_orders(param)) == {param["name"]: None}


def test_a_tag_maps_an_ordinarily_named_parameter():
    raw = _orders(
        _param("since", DATE),
        _param("regions", {"kind": "array", "element": S}),
        tags=(("gte_signup", "since"), ("in_region", "regions")),
    )
    assert _pushes(raw) == {
        "since": Pushdown("signup", "gte"),
        "regions": Pushdown("region", "in"),
    }


def test_an_inline_declaration_maps_its_parameter():
    raw = _orders(_param("since", DATE, pushdown=("gte_signup",)))
    assert _pushes(raw) == {"since": Pushdown("signup", "gte")}


def test_a_tag_naming_a_missing_parameter_is_refused():
    assert _problems(_orders(_param("since", DATE), tags=(("gte_signup", "from"),))) == [
        "orders: @gte_signup names from, which is not a parameter"
    ]


def test_a_declaration_naming_no_return_column_is_refused():
    assert _problems(_orders(_param("since", DATE, pushdown=("gte_joined",)))) == [
        "orders(since): gte_joined names no return column of orders"
    ]


def test_two_mappings_for_one_predicate_are_refused():
    raw = _orders(_param("gte_signup", DATE), _param("since", DATE, pushdown=("gte_signup",)))
    assert _problems(raw) == ["orders: gte on signup is pushed to both gte_signup and since"]


def test_a_parameter_declared_twice_is_refused():
    raw = _orders(_param("since", DATE, pushdown=("lt_signup",)), tags=(("gte_signup", "since"),))
    assert _problems(raw) == [
        "orders(since): declared to take two predicates, ['gte_signup', 'lt_signup']"
    ]


def test_a_command_declares_no_pushdown():
    raw = _orders(
        _param("since", DATE, pushdown=("gte_signup",)),
        kind="command",
        returns={"kind": "undefined"},
    )
    assert _problems(raw) == [
        "orders(since): @gte_signup pushes a predicate, and a @command has no columns to filter"
    ]


def test_a_table_typed_by_a_preview_takes_no_declared_pushdown():
    raw = _orders(
        _param("since", DATE, pushdown=("gte_signup",)),
        returns={"kind": "array", "element": {"kind": "any"}},
    )
    assert _problems(raw) == [
        "orders(since): its rows' columns are found by a preview, so no predicate can be pushed "
        "to it"
    ]


def test_a_kept_manifest_keeps_each_parameters_pushdown():
    manifest = read_manifest(_orders(_param("gte_score", N), _param("as_of", DATE)))
    kept = manifest_from_json(json.loads(json.dumps(manifest_json(manifest))))
    assert kept == manifest


# -- the Python reader ---------------------------------------------------------------------------


def _read(tmp_path: Path, source: str) -> dict:
    (tmp_path / "rows.py").write_text(textwrap.dedent(source))
    done = subprocess.run(
        [sys.executable, "-I", str(READER), str(tmp_path), "rows"], capture_output=True, text=True
    )
    assert done.returncode == 0, done.stderr
    return json.loads(done.stdout)


def test_python_declares_pushdown_by_name_docstring_tag_and_annotated(tmp_path):
    raw = _read(
        tmp_path,
        """
        from dataclasses import dataclass
        from datetime import date
        from typing import Annotated


        @dataclass
        class Order:
            id: int
            signup: date
            region: str
            score: float


        def orders(
            since: date | None = None,
            regions: Annotated[list[str] | None, "in_region"] = None,
            gt_score: float | None = None,
            id: int | None = None,
        ) -> list[Order]:
            \"\"\"Orders.

            @table
            @gte_signup since
            \"\"\"
            return []
        """,
    )
    orders = read_manifest(raw, PYTHON_RULES).export("orders")
    assert orders.description == "Orders."
    assert {p.name: p.pushes for p in orders.parameters} == {
        "since": Pushdown("signup", "gte"),
        "regions": Pushdown("region", "in"),
        "gt_score": Pushdown("score", "gt"),
        "id": Pushdown("id", "eq"),
    }


# -- the name tables -----------------------------------------------------------------------------


def test_the_name_tables_pair_each_declared_name_with_its_catalog_and_wire_forms():
    from provisa.codesource.names import name_tables

    manifest = read_manifest(_orders(_param("gte_score", N), _param("as_of", DATE)))
    table = name_tables(manifest)["orders"]
    by_name = {c["name"]: c for c in table["columns"]}
    assert by_name["customerName"] == {
        "name": "customerName",
        "sql": "customer_name",
        "quoted": '"customerName"',
        "folded": "customername",
        "type": "text",
    }
    assert table["schema"]
    assert [p["parameter"] for p in table["parameters"]] == ["gte_score", "as_of"]
    assert table["parameters"][1]["name"] == "_nf_as_of"
    assert table["pushdown"] == {"gte_score": {"column": "score", "op": "gte"}}


def test_the_name_tables_are_provisas_live_mapper(monkeypatch):
    from provisa.compiler import naming
    from provisa.codesource.names import name_tables

    monkeypatch.setattr(naming, "_sql_convention", "apollo_graphql")
    manifest = read_manifest(_orders())
    for column in name_tables(manifest)["orders"]["columns"]:
        assert column["sql"] == naming.apply_sql_name(column["name"])


def test_two_names_one_wire_form_cannot_tell_apart_are_refused():
    from provisa.codesource.names import name_tables

    raw = _orders()
    raw["types"]["Order"]["fields"].append(
        {"name": "ID", "description": "", "optional": False, "type": N}
    )
    with pytest.raises(ManifestRefused) as caught:
        name_tables(read_manifest(raw))
    assert "orders: id and ID are both id on pgwire" in caught.value.problems
