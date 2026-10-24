# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A @table or @stream function's signature (REQ-2100): ``(inputs, context)`` -- an object whose
fields are its inputs, and a ReadContext -- either left out, any other refused by name. Its inputs
are its parameters, each an ``_nf_`` column; a field named like a return column is an input like
any other, every predicate on a column reaching the function through its read context. The name
tables pair each declared name with its catalog and wire forms."""

# Requirements: REQ-2100
from __future__ import annotations

import json
import subprocess
import sys
import textwrap
from pathlib import Path

import pytest

from provisa.codesource.manifest import (
    CONTEXT,
    INPUTS,
    PYTHON_RULES,
    ManifestRefused,
    manifest_from_json,
    manifest_json,
    read_manifest,
)
from provisa.python.source import READER

pytestmark = pytest.mark.unit

S = {"kind": "string"}
N = {"kind": "number"}
DATE = {"kind": "date"}
CTX = {"kind": "ref", "name": "ReadContext"}

ORDER = {
    "kind": "object",
    "name": "Order",
    "fields": [
        {"name": "id", "description": "", "optional": False, "type": N},
        {"name": "status", "description": "", "optional": False, "type": S},
        {"name": "score", "description": "", "optional": False, "type": N},
        {"name": "region", "description": "", "optional": False, "type": S},
        {"name": "customerName", "description": "", "optional": False, "type": S},
    ],
}
INPUTS_TYPE = {
    "kind": "object",
    "name": "OrderInputs",
    "fields": [
        {"name": "region", "description": "The region", "optional": True, "type": S},
        {"name": "as_of", "description": "", "optional": False, "default": True, "type": DATE},
        {"name": "since", "description": "", "optional": False, "type": DATE},
    ],
}
_ORDER_ROWS = {"kind": "array", "element": {"kind": "ref", "name": "Order"}}


def _param(name: str, t: dict) -> dict:
    return {"name": name, "description": "", "optional": False, "rest": False, "type": t}


def _orders(*params: dict, kind: str = "table", tags: tuple = (), returns=None) -> dict:
    return {
        "exports": [
            {
                "name": "orders",
                "tags": [kind, *(t for t, a in tags if not a)],
                "tag_arguments": [{"tag": t, "text": a} for t, a in tags if a],
                "description": "",
                "signatures": [{"parameters": list(params), "returns": returns or _ORDER_ROWS}],
            }
        ],
        "types": {"Order": ORDER, "OrderInputs": INPUTS_TYPE},
        "diagnostics": [],
    }


INPUTS_PARAM = _param("inputs", {"kind": "ref", "name": "OrderInputs"})
CONTEXT_PARAM = _param("context", CTX)


def _problems(raw: dict) -> list[str]:
    with pytest.raises(ManifestRefused) as caught:
        read_manifest(raw)
    return caught.value.problems


@pytest.mark.parametrize(
    ("params", "takes"),
    [
        ((INPUTS_PARAM, CONTEXT_PARAM), (INPUTS, CONTEXT)),
        ((INPUTS_PARAM,), (INPUTS,)),
        ((CONTEXT_PARAM,), (CONTEXT,)),
        ((), ()),
    ],
)
def test_a_table_takes_its_inputs_and_its_context_either_left_out(params, takes):
    export = read_manifest(_orders(*params)).export("orders")
    assert export.takes == takes
    expected = ["region", "as_of", "since"] if INPUTS in takes else []
    assert [p.name for p in export.parameters] == expected


def test_its_inputs_fields_are_its_parameters_required_only_with_no_default():
    export = read_manifest(_orders(INPUTS_PARAM, CONTEXT_PARAM)).export("orders")
    assert {p.name: p.required for p in export.parameters} == {
        "region": False,
        "as_of": False,
        "since": True,
    }
    assert export.parameters[0].description == "The region"


def test_a_field_named_like_a_return_column_is_an_input_like_any_other():
    # REQ-2100: no parameter is a filter; a predicate on region reaches the function in its
    # context, and inputs.region is an input, its own _nf_region column.
    export = read_manifest(_orders(INPUTS_PARAM)).export("orders")
    region = next(p for p in export.parameters if p.name == "region")
    assert not hasattr(region, "pushes")


@pytest.mark.parametrize(
    "params",
    [
        (_param("region", S), _param("score", N)),
        (CONTEXT_PARAM, INPUTS_PARAM),
        (INPUTS_PARAM, CONTEXT_PARAM, _param("extra", S)),
        (INPUTS_PARAM, INPUTS_PARAM),
    ],
)
def test_any_other_signature_is_refused_by_name(params):
    written = ", ".join(p["name"] for p in params)
    assert _problems(_orders(*params)) == [
        "orders: a @table function takes (inputs, context) -- an object whose fields are its "
        f"inputs, and a ReadContext, either left out -- not ({written})"
    ]


def test_inputs_that_are_no_object_are_refused_by_name():
    assert _problems(_orders(_param("region", S))) == [
        "orders(region): its inputs are an object whose fields are its inputs, a dataclass, a "
        "TypedDict or an interface"
    ]


def test_a_stream_takes_the_same_signature():
    rows = {"kind": "iterable", "async": True, "generator": True, "element": ORDER}
    raw = _orders(INPUTS_PARAM, CONTEXT_PARAM, kind="stream", returns=rows)
    raw["exports"][0]["signatures"][0]["returns"]["element"] = {"kind": "ref", "name": "Order"}
    assert read_manifest(raw).export("orders").takes == (INPUTS, CONTEXT)


def test_a_loader_is_given_the_joins_keys_in_its_context():
    export = read_manifest(_orders(INPUTS_PARAM, CONTEXT_PARAM, tags=(("loader", "100"),))).export(
        "orders"
    )
    assert (export.loader, export.loader_batch) == (True, 100)
    assert _problems(_orders(INPUTS_PARAM, tags=(("loader", ""),))) == [
        "orders: a @loader is given the join's keys in its read context, and orders takes no "
        "ReadContext"
    ]


@pytest.mark.parametrize(
    ("kind", "params", "returns"),
    [
        ("command", (_param("since", DATE),), {"kind": "undefined"}),
        (
            "stream",
            (INPUTS_PARAM, CONTEXT_PARAM),
            {"kind": "iterable", "async": True, "generator": True, "element": ORDER},
        ),
        ("columns", (_param("since", {"kind": "array", "element": DATE}),), _ORDER_ROWS),
    ],
)
def test_a_loader_declared_on_any_kind_but_a_table_is_refused_by_name(kind, params, returns):
    """REQ-2100: a loader batches a table's keys. Declared on another kind it would be ignored, so
    it is refused."""
    raw = _orders(*params, kind=kind, tags=(("loader", "100"),), returns=returns)
    assert f"orders: @loader batches a table's keys, and a @{kind} takes none" in _problems(raw)


def test_a_tag_line_provisa_does_not_know_is_refused():
    assert "orders: unknown tag @gte_score" in _problems(
        _orders(INPUTS_PARAM, tags=(("gte_score", "since"),))
    )


def test_a_kept_manifest_keeps_what_each_function_takes():
    manifest = read_manifest(_orders(INPUTS_PARAM, CONTEXT_PARAM))
    kept = manifest_from_json(json.loads(json.dumps(manifest_json(manifest))))
    assert kept == manifest


# -- the Python reader ---------------------------------------------------------------------------


def _read(tmp_path: Path, source: str) -> dict:
    (tmp_path / "rows.py").write_text(textwrap.dedent(source))
    services = Path(READER).parent / "services"
    done = subprocess.run(
        [sys.executable, "-I", str(READER), str(tmp_path), "rows"],
        capture_output=True,
        text=True,
        env={"PYTHONPATH": str(services)},
    )
    assert done.returncode == 0, done.stderr
    return json.loads(done.stdout)


def test_python_reads_its_inputs_dataclass_and_its_read_context(tmp_path):
    raw = _read(
        tmp_path,
        """
        from dataclasses import dataclass, field
        from datetime import date

        from provisa_services import ReadContext


        @dataclass
        class Order:
            id: int
            region: str


        @dataclass
        class OrderInputs:
            since: date
            region: str = "EU"
            tags: list[str] = field(default_factory=list)


        def orders(inputs: OrderInputs, context: ReadContext) -> list[Order]:
            '''Orders.

            @table
            @loader 50
            '''
            return []
        """,
    )
    export = read_manifest(raw, PYTHON_RULES).export("orders")
    assert export.takes == (INPUTS, CONTEXT)
    assert {p.name: p.required for p in export.parameters} == {
        "since": True,
        "region": False,
        "tags": False,
    }
    assert (export.loader, export.loader_batch) == (True, 50)


# -- the name tables -----------------------------------------------------------------------------


def test_the_name_tables_pair_each_declared_name_with_its_catalog_and_wire_forms():
    from provisa.codesource.names import name_tables

    manifest = read_manifest(_orders(INPUTS_PARAM, CONTEXT_PARAM))
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
    assert [p["parameter"] for p in table["parameters"]] == ["region", "as_of", "since"]
    assert table["parameters"][1]["name"] == "_nf_as_of"
    assert table["takes"] == ["inputs", "context"]


def test_the_name_tables_are_provisas_live_mapper(monkeypatch):
    from provisa.codesource.names import name_tables
    from provisa.compiler import naming

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
