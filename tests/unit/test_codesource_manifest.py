# Copyright (c) 2026 Kenneth Stott
# Canary: 0f649546-ad4f-420d-a6ee-2e3a9c98e617
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""What a Node bundle offers (REQ-1984), from the reader's account of its declarations: tagged
exports are tables, commands and live tables; a table returns a collection; a stream is an async
generator; parameters and fields map to types; and everything that breaks a rule is refused by
name, all at once. The reader's output is written here by hand in its own shape
(provisa/node/reader.mjs); tests/integration/test_node_reader.py runs the real reader."""

# Requirements: REQ-1984
from __future__ import annotations

import json
from pathlib import Path

import pytest

from provisa.node import reader
from provisa.codesource.bundle import BundleRefused
from provisa.codesource.manifest import (
    LOOSE,
    Anonymous,
    EnumOf,
    Field,
    ListOf,
    ManifestRefused,
    Named,
    Scalar,
    read_manifest,
)

pytestmark = pytest.mark.unit

S = {"kind": "string"}
N = {"kind": "number"}
DATE = {"kind": "date"}


def opt(t: dict) -> dict:
    return {"kind": "union", "name": None, "members": [{"kind": "undefined"}, t]}


def ref(name: str) -> dict:
    return {"kind": "ref", "name": name}


def promise(t: dict) -> dict:
    return {"kind": "promise", "value": t}


def arr(t: dict) -> dict:
    return {"kind": "array", "element": t}


def it(t: dict, *, async_: bool, generator: bool = True) -> dict:
    return {"kind": "iterable", "async": async_, "generator": generator, "element": t}


def fn(name, tags, params, returns, description="") -> dict:
    return {
        "name": name,
        "tags": tags,
        "description": description,
        "signatures": [
            {
                "parameters": [
                    {"name": p, "optional": o, "rest": False, "type": t} for p, o, t in params
                ],
                "returns": returns,
            }
        ],
    }


ORDER = {
    "kind": "object",
    "name": "Order",
    "fields": [
        {"name": "id", "description": "The order's number.", "optional": False, "type": N},
        {"name": "placed", "description": "", "optional": False, "type": DATE},
        {
            "name": "tier",
            "optional": False,
            "description": "",
            "type": {
                "kind": "union",
                "name": "Tier",
                "members": [
                    {"kind": "literal", "value": "gold"},
                    {"kind": "literal", "value": "silver"},
                ],
            },
        },
        {"name": "lines", "description": "", "optional": False, "type": arr(ref("Line"))},
        {"name": "note", "description": "", "optional": True, "type": opt(S)},
    ],
}
LINE = {
    "kind": "object",
    "name": "Line",
    "fields": [{"name": "sku", "description": "", "optional": False, "type": S}],
}
TYPES = {"Order": ORDER, "Line": LINE}


def inputs(*fields: tuple[str, bool, dict]) -> dict:
    """REQ-2100: a @table or @stream function's inputs object, its fields ``(name, optional,
    type)``."""
    return {
        "kind": "object",
        "name": None,
        "fields": [{"name": n, "optional": o, "description": "", "type": t} for n, o, t in fields],
    }


def raw(*exports, types=TYPES, diagnostics=()) -> dict:
    return {"exports": list(exports), "types": dict(types), "diagnostics": list(diagnostics)}


def refused(*exports, **kw) -> list[str]:
    with pytest.raises(ManifestRefused) as caught:
        read_manifest(raw(*exports, **kw))
    return caught.value.problems


def test_a_table_is_its_functions_parameters_and_its_row_types_fields():
    manifest = read_manifest(
        raw(
            fn(
                "orders",
                ["table"],
                [("inputs", False, inputs(("customerId", False, S), ("since", True, opt(DATE))))],
                promise(arr(ref("Order"))),
                "Orders of one customer.",
            )
        )
    )
    (orders,) = manifest.exports
    assert (orders.kind, orders.description, orders.returns, orders.preview) == (
        "table",
        "Orders of one customer.",
        Named("Order"),
        False,
    )
    assert [(p.name, p.shape, p.required, p.nullable) for p in orders.parameters] == [
        ("customerId", Scalar("text"), True, False),
        ("since", Scalar("timestamp"), False, True),
    ]
    assert manifest.types["Order"].fields == (
        Field("id", Scalar("double"), False, "The order's number."),
        Field("placed", Scalar("timestamp"), False),
        Field("tier", EnumOf("Tier", ("gold", "silver")), False),
        Field("lines", ListOf(Named("Line")), False),
        Field("note", Scalar("text"), True),
    )
    assert manifest.types["Line"].fields == (Field("sku", Scalar("text"), False),)


@pytest.mark.parametrize(
    "returns",
    [
        arr(ref("Order")),
        promise(arr(ref("Order"))),
        it(ref("Order"), async_=False),
        it(ref("Order"), async_=True),
    ],
)
def test_a_table_returns_any_collection_of_rows(returns):
    (table,) = read_manifest(raw(fn("orders", ["table"], [], returns))).exports
    assert table.returns == Named("Order")


def test_a_table_that_returns_a_single_object_is_refused_by_name():
    problems = refused(fn("order", ["table"], [], promise(ref("Order"))))
    assert problems == ["order: @table returns a single value, not a collection of rows"]


def test_a_loose_row_type_is_typed_by_a_preview():
    for loose in ({"kind": "any"}, {"kind": "unknown"}, {"kind": "record", "value": N}):
        (table,) = read_manifest(raw(fn("rows", ["table"], [], arr(loose)))).exports
        assert (table.returns, table.preview) == (LOOSE, True)


def test_a_row_that_is_not_an_object_is_refused():
    assert refused(fn("names", ["table"], [], arr(S))) == [
        "names: its rows are not objects; a row is an object whose fields are columns"
    ]


def test_a_stream_is_an_async_generator_with_the_tables_rules():
    (live,) = read_manifest(
        raw(
            fn(
                "live",
                ["stream"],
                [("inputs", False, inputs(("region", False, S)))],
                it(ref("Order"), async_=True),
            )
        )
    ).exports
    assert (live.kind, live.returns, live.parameters[0].required) == (
        "stream",
        Named("Order"),
        True,
    )
    assert refused(fn("live", ["stream"], [], arr(ref("Order")))) == [
        "live: @stream is an async generator (returns AsyncGenerator<T>)"
    ]


def test_a_command_returns_rows_one_value_or_nothing():
    manifest = read_manifest(
        raw(
            fn("cancel", ["command"], [("id", False, N)], promise({"kind": "undefined"})),
            fn("receipt", ["command"], [], promise(ref("Order"))),
            fn("reprice", ["command"], [], arr(ref("Order"))),
            fn(
                "ping",
                ["command"],
                [],
                {
                    "kind": "object",
                    "name": None,
                    "fields": [
                        {
                            "name": "ok",
                            "description": "",
                            "optional": False,
                            "type": {"kind": "boolean"},
                        }
                    ],
                },
            ),
        )
    )
    assert [(e.name, e.returns, e.many) for e in manifest.exports] == [
        ("cancel", None, False),
        ("receipt", Named("Order"), False),
        ("reprice", Named("Order"), True),
        ("ping", Anonymous((Field("ok", Scalar("boolean"), False),)), False),
    ]


def test_untagged_exports_are_ignored_and_standard_jsdoc_tags_are_known():
    manifest = read_manifest(
        raw(
            fn("helper", [], [], N),
            fn(
                "orders",
                ["table", "deprecated", "param", "returns", "example"],
                [],
                arr(ref("Order")),
            ),
            {"name": "VERSION", "tags": [], "description": "", "signatures": []},
        )
    )
    assert [e.name for e in manifest.exports] == ["orders"]


def test_every_problem_is_reported_at_once_each_by_name():
    problems = refused(
        fn("typo", ["tabel"], [], arr(ref("Order"))),
        fn("both", ["table", "command"], [], arr(ref("Order"))),
        {"name": "VERSION", "tags": ["table"], "description": "", "signatures": []},
        fn(
            "bad_param",
            ["table"],
            [
                (
                    "inputs",
                    False,
                    inputs(("cb", False, {"kind": "unsupported", "text": "() => void"})),
                )
            ],
            arr(ref("Order")),
        ),
        fn(
            "mixed",
            ["command"],
            [("x", False, {"kind": "union", "name": None, "members": [S, N]})],
            {"kind": "undefined"},
        ),
        diagnostics=["Cannot find name 'Foo'."],
    )
    assert problems == [
        "the declarations do not type-check: Cannot find name 'Foo'.",
        "typo: unknown tag @tabel",
        "both: tagged @command, @table; an export is one",
        "VERSION: @table needs one function signature, it has 0",
        "bad_param(cb): the type () => void cannot be mapped",
        "mixed(x): a union of number, string cannot be mapped",
    ]


def test_a_field_whose_type_cannot_be_mapped_is_refused_by_its_place():
    widget = {
        "kind": "object",
        "name": "Widget",
        "fields": [
            {
                "name": "pair",
                "optional": False,
                "description": "",
                "type": {"kind": "unsupported", "text": "[number, string]"},
            }
        ],
    }
    problems = refused(fn("widgets", ["table"], [], arr(ref("Widget"))), types={"Widget": widget})
    assert problems == ["Widget.pair: the type [number, string] cannot be mapped"]


def test_a_self_referencing_type_is_named_once():
    node = {
        "kind": "object",
        "name": "Node",
        "fields": [
            {"name": "id", "description": "", "optional": False, "type": S},
            {"name": "children", "description": "", "optional": False, "type": arr(ref("Node"))},
        ],
    }
    manifest = read_manifest(raw(fn("tree", ["table"], [], arr(ref("Node"))), types={"Node": node}))
    assert manifest.types["Node"].fields[1] == Field("children", ListOf(Named("Node")), False)


# -- the reader's plumbing ------------------------------------------------------------------------


def test_the_declarations_are_the_packages_types_else_index_d_ts(tmp_path):
    (tmp_path / "index.d.ts").write_text("x")
    assert reader.entry_declarations(tmp_path) == tmp_path / "index.d.ts"
    (tmp_path / "dist").mkdir()
    (tmp_path / "dist" / "api.d.ts").write_text("x")
    (tmp_path / "package.json").write_text(json.dumps({"types": "dist/api.d.ts"}))
    assert reader.entry_declarations(tmp_path) == (tmp_path / "dist" / "api.d.ts").resolve()


@pytest.mark.parametrize("package", [{"types": "missing.d.ts"}, {"types": "../outside.d.ts"}, None])
def test_a_bundle_without_its_declarations_is_refused(tmp_path, package):
    if package is not None:
        (tmp_path / "package.json").write_text(json.dumps(package))
    (tmp_path.parent / "outside.d.ts").write_text("x")
    with pytest.raises(BundleRefused) as caught:
        reader.entry_declarations(tmp_path)
    assert caught.value.code == "codesource.no_declarations"


def test_each_runtime_runs_the_reader_and_deno_reads_only_what_it_is_given(tmp_path):
    exe, ts, entry = (
        tmp_path / "rt",
        tmp_path / "ts" / "lib" / "typescript.js",
        tmp_path / "b" / "index.d.ts",
    )
    assert reader.reader_command("node", exe, ts, entry) == [
        str(exe),
        str(reader.READER),
        str(ts),
        str(entry),
    ]
    # Bun installs nothing while it runs code: it would install a package an import misses.
    assert reader.reader_command("bun", exe, ts, entry) == [
        str(exe),
        "--no-install",
        str(reader.READER),
        str(ts),
        str(entry),
    ]
    deno = reader.reader_command("deno", exe, ts, entry)
    assert deno[:2] == [str(exe), "run"]
    assert deno[2] == f"--allow-read={reader.READER.parent},{tmp_path / 'ts'},{tmp_path / 'b'}"
    assert deno[3:] == ["--allow-env", str(reader.READER), str(ts), str(entry)]


@pytest.mark.parametrize("runtime, flags", [("node", []), ("bun", ["--no-install"])])
def test_the_host_runs_the_bundle_installing_nothing(tmp_path, runtime, flags):
    # The host looks for the code's optional @opentelemetry/api (host-core.mjs); Bun, unless told
    # not to, installs a package it does not find, which a sandboxed host has nowhere to write.
    from provisa.node.worker import HOST, host_command

    (tmp_path / "package.json").write_text(json.dumps({"type": "module", "main": "index.js"}))
    (tmp_path / "index.js").write_text("export const f = () => 1;\n")
    exe = tmp_path / "rt" / runtime
    argv = host_command(
        runtime=runtime, executable=exe, bundle=tmp_path, egress=[], host_lib=tmp_path / "lib"
    ).argv("fd:3")
    run = [str(exe), *flags, str(HOST), "fd:3", str(tmp_path / "index.js")]
    assert any(argv[i : i + len(run)] == run for i in range(len(argv)))


def test_a_bundle_is_read_through_the_reader_into_its_manifest(tmp_path):
    (tmp_path / "index.d.ts").write_text("x")
    seen = []

    def run(argv):
        seen.append(argv)
        return raw(fn("orders", ["table"], [], arr(ref("Order"))))

    manifest = reader.read_bundle(
        runtime="node",
        executable=Path("/rt/node"),
        compiler=Path("/ts/lib/typescript.js"),
        bundle=tmp_path,
        run=run,
    )
    assert [e.name for e in manifest.exports] == ["orders"]
    assert seen[0][-1] == str(tmp_path / "index.d.ts")


def test_the_compiler_is_fetched_once_and_only_at_its_pinned_integrity(tmp_path, monkeypatch):
    calls = []

    def fake_fetch(code, registries, http, dest, *, pinned_integrity):
        calls.append((code, pinned_integrity))
        (dest / "lib").mkdir(parents=True)
        (dest / "lib" / "typescript.js").write_text("module.exports = {}")
        return dest

    monkeypatch.setattr(reader, "fetch_npm", fake_fetch)
    first = reader.ensure_typescript(tmp_path, [])
    assert reader.ensure_typescript(tmp_path, []) == first
    assert first == tmp_path / "code" / "typescript" / "6.0.3" / "lib" / "typescript.js"
    assert calls == [(reader.TYPESCRIPT, reader.TYPESCRIPT_INTEGRITY)]


def test_the_reader_ships_with_the_package():
    import tomllib

    pyproject = tomllib.loads((Path(__file__).resolve().parents[2] / "pyproject.toml").read_text())
    assert "reader.mjs" in pyproject["tool"]["setuptools"]["package-data"]["provisa.node"]
    assert reader.READER.is_file()


ADDRESS = {
    "kind": "object",
    "name": "Address",
    "fields": [
        {"name": n, "description": "", "optional": False, "type": S}
        for n in ("street", "city", "country")
    ],
}


def test_a_columns_function_takes_input_columns_as_lists_and_returns_one_object_a_row():
    """REQ-2131: each parameter is one input column, a list of the batch's values; the result is
    a list holding one object per input row, whose fields are the output columns."""
    (transform,) = read_manifest(
        raw(
            fn("clean_address", ["columns"], [("raw_address", False, arr(S))], arr(ref("Address"))),
            types={"Address": ADDRESS},
        )
    ).exports
    assert transform.kind == "columns"
    assert [(p.name, p.shape) for p in transform.parameters] == [
        ("raw_address", ListOf(Scalar("text")))
    ]
    assert transform.returns == Named("Address") and transform.many


def test_a_columns_function_that_does_not_batch_is_refused_by_name():
    problems = refused(
        fn("one_at_a_time", ["columns"], [("raw_address", False, S)], arr(ref("Address"))),
        fn("single", ["columns"], [("raw_address", False, arr(S))], ref("Address")),
        fn("loose", ["columns"], [("raw_address", False, arr(S))], arr({"kind": "any"})),
        fn("nothing_in", ["columns"], [], arr(ref("Address"))),
        types={"Address": ADDRESS},
    )
    assert problems == [
        "one_at_a_time: @columns parameter raw_address is one input column: a list of values",
        "single: @columns returns a list holding one result per input row",
        "loose: @columns declares its output columns: its result is a typed object",
        "nothing_in: @columns takes at least one input column",
    ]
