# Copyright (c) 2026 Kenneth Stott
# Canary: 3c5e8a17-2f94-4d6b-b0e1-7a9d4c2f8b63
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A Rust source (REQ-2227): its public functions read from their doc comments and signatures by
the Rust reader, and judged by the shared rules (REQ-1984's, REQ-2221's lifecycle tags,
REQ-2226's @query and table arguments). The reader is compiled here by the pinned toolchain, its
dependencies fetched from crates.io and checked against its Cargo.lock."""

# Requirements: REQ-2227, REQ-2221, REQ-2226, REQ-1984
from __future__ import annotations

import textwrap
from pathlib import Path

import pytest

from provisa.codesource.bundle import BundleRefused
from provisa.codesource.manifest import (
    RUST_RULES,
    EnumOf,
    Field,
    ListOf,
    Manifest,
    ManifestRefused,
    Named,
    Scalar,
    read_manifest,
)
from provisa.runtime_deps.pgwire_bundles import default_cache_root
from provisa.rustlang.reader import (
    crate_manifest,
    ensure_reader,
    read_code,
    read_document,
    reader_key,
)
from provisa.rustlang.toolchain import Toolchain, ensure_toolchain

pytestmark = pytest.mark.unit

FIXTURE = Path(__file__).parent.parent / "fixtures" / "rust_source"


@pytest.fixture(scope="module")
def toolchain() -> Toolchain:
    """The pinned Rust toolchain, fetched and checked on first use."""
    return ensure_toolchain(default_cache_root(), [])


@pytest.fixture(scope="module")
def reader(toolchain: Toolchain, tmp_path_factory: pytest.TempPathFactory) -> Path:
    return ensure_reader(toolchain, tmp_path_factory.mktemp("cache"))


def _read(reader: Path, toolchain: Toolchain, code: Path) -> Manifest:
    return read_code(reader=reader, code=code, entry=".", toolchain=toolchain.version)


def _crate(tmp_path: Path, source: str, **files: str) -> Path:
    code = tmp_path / "code"
    (code / "src").mkdir(parents=True)
    (code / "Cargo.toml").write_text(
        '[package]\nname = "rows"\nversion = "0.1.0"\nedition = "2021"\n'
    )
    (code / "src" / "lib.rs").write_text(textwrap.dedent(source))
    for name, text in files.items():
        (code / "src" / f"{name}.rs").write_text(textwrap.dedent(text))
    return code


# -- reading --------------------------------------------------------------------------------------


def test_the_codes_tables_commands_and_streams_are_read_with_typed_rows(reader, toolchain):
    manifest = _read(reader, toolchain, FIXTURE)
    assert [(e.name, e.kind) for e in manifest.exports] == [
        ("orders", "table"),
        ("many", "table"),
        ("ticks", "stream"),
        ("refund", "command"),
        ("price", "command"),
        ("outside", "table"),
    ]
    orders = manifest.export("orders")
    assert orders.description == "The customer's orders."
    assert orders.returns == Named("Order")
    assert orders.takes == ("inputs",)
    assert [(p.name, p.shape, p.required, p.description) for p in orders.parameters] == [
        ("customer", Scalar("text"), True, "The customer the orders are for."),
        ("lines", Scalar("bigint"), False, "How many lines each order has; one when left out."),
    ]
    # The row type, declared in a module of the crate, its fields as serde writes them.
    assert manifest.types["Order"].fields == (
        Field("id", Scalar("text"), False),
        Field("status", EnumOf("Status", ("open", "shipped")), False),
        Field("total", Scalar("double"), False),
        Field("note", Scalar("text"), True, "A note, when there is one."),
        Field("lines", ListOf(Named("Line")), False),
    )
    assert manifest.types["Line"].fields[0] == Field(
        "sku", Scalar("text"), False, "The item's stock-keeping unit."
    )
    # An iterator is a table's rows as well as a collection is.
    assert manifest.export("many").returns == Named("Numbered")
    assert manifest.export("ticks").returns == Named("Numbered")
    refund = manifest.export("refund")
    assert [(p.name, p.shape, p.required) for p in refund.parameters] == [
        ("order_id", Scalar("text"), True),
        ("amount", Scalar("double"), True),
    ]
    # A Result is how the function fails: what it returns is what the Result holds.
    assert refund.returns == Named("Refunded") and not refund.many
    assert [f.name for f in manifest.types["Refunded"].fields] == ["orderId", "amount"]
    price = manifest.export("price")
    assert [(p.name, p.required, p.nullable) for p in price.parameters] == [
        ("sku", True, False),
        ("quantity", False, True),
    ]
    assert price.returns == Scalar("double")


def test_the_lifecycle_tags_are_read_from_a_doc_comment(reader, toolchain, tmp_path):
    """REQ-2221: Rust reads @on-start, @on-stop and @health like every other language."""
    code = _crate(
        tmp_path,
        """
        use std::collections::HashMap;

        /// Connect.
        ///
        /// @on-start
        pub fn connect(values: HashMap<String, String>) -> Result<(), String> { Ok(()) }

        /// Close.
        ///
        /// @on-stop
        pub fn close() {}

        /// Check.
        ///
        /// @health
        pub fn check() -> Result<String, std::io::Error> { Ok("ok".into()) }
        """,
    )
    manifest = _read(reader, toolchain, code)
    assert {e.name: e.kind for e in manifest.exports} == {
        "connect": "on-start",
        "close": "on-stop",
        "check": "health",
    }
    on_start, health = manifest.lifecycle("on-start"), manifest.lifecycle("health")
    assert on_start is not None and health is not None
    assert [p.name for p in on_start.parameters] == ["values"]
    assert health.returns == Scalar("text")


def test_a_query_and_a_table_argument_are_read_as_tagged_and_typed(reader, toolchain, tmp_path):
    """REQ-2226: a @query is read like a @command, its tag kept for the rules to judge, and a
    parameter typed TableRef<Row> is read as a governed table whose row is Row."""
    code = _crate(
        tmp_path,
        """
        use serde::{Deserialize, Serialize};

        #[derive(Deserialize)]
        pub struct TableRef<T> { pub name: String, #[serde(skip)] pub row: Option<T> }

        #[derive(Serialize, Deserialize)]
        pub struct Sale { pub region: String, pub amount: f64 }

        /// Totals by region.
        /// @query
        pub fn totals(sales: TableRef<Sale>, top: Option<i64>) -> Vec<Sale> { vec![] }
        """,
    )
    document = read_document(reader, code, toolchain.version)
    (totals,) = document["exports"]
    assert totals["tags"] == ["query"]
    assert totals["description"] == "Totals by region."
    (signature,) = totals["signatures"]
    sales, top = signature["parameters"]
    assert sales["type"] == {"kind": "table_ref", "row": {"kind": "ref", "name": "Sale"}}
    assert top["optional"] is True
    assert signature["returns"] == {"kind": "array", "element": {"kind": "ref", "name": "Sale"}}
    assert [f["name"] for f in document["types"]["Sale"]["fields"]] == ["region", "amount"]


def test_a_loose_row_type_is_refused_by_name(reader, toolchain, tmp_path):
    """REQ-1984: every table declares typed rows; a row that is a Value or a map is refused,
    naming the function."""
    code = _crate(
        tmp_path,
        """
        use std::collections::HashMap;

        /// @table
        pub fn anything() -> Vec<serde_json::Value> { vec![] }

        /// @table
        pub fn mapped() -> Vec<HashMap<String, String>> { vec![] }

        /// @stream
        pub fn loose_stream() -> impl Iterator<Item = serde_json::Value> { std::iter::empty() }
        """,
    )
    with pytest.raises(ManifestRefused) as refused:
        _read(reader, toolchain, code)
    problems = refused.value.problems
    for name in ("anything", "mapped", "loose_stream"):
        assert any(p.startswith(f"{name}: its row type is loose") for p in problems), problems


def test_a_single_field_of_a_typed_row_may_still_be_loose(reader, toolchain, tmp_path):
    code = _crate(
        tmp_path,
        """
        #[derive(serde::Serialize)]
        pub struct Event { pub id: i64, pub payload: serde_json::Value }

        /// @table
        pub fn events() -> Vec<Event> { vec![] }
        """,
    )
    manifest = _read(reader, toolchain, code)
    assert manifest.types["Event"].fields[1] == Field("payload", Scalar("json"), False)


def test_a_fields_name_is_the_one_serde_gives_it(reader, toolchain, tmp_path):
    code = _crate(
        tmp_path,
        """
        use serde::{Deserialize, Serialize};

        #[derive(Serialize)]
        pub struct Audit { pub at: String }

        #[derive(Serialize)]
        #[serde(rename_all = "camelCase")]
        pub struct Row {
            pub first_name: String,
            #[serde(rename = "ID")]
            pub id: u32,
            #[serde(skip)]
            pub secret: String,
            #[serde(flatten)]
            pub audit: Audit,
            pub r#type: String,
        }

        #[derive(Deserialize)]
        pub struct Inputs {
            pub region: String,
            #[serde(default)]
            pub limit: i64,
        }

        #[derive(Serialize)]
        #[serde(rename_all = "SCREAMING_SNAKE_CASE")]
        pub enum Level { LowRisk, #[serde(rename = "hi")] High }

        #[derive(Serialize)]
        pub struct Leveled { pub level: Level }

        /// @table
        pub fn rows(inputs: Inputs) -> Vec<Row> { vec![] }

        /// @table
        pub fn leveled() -> Vec<Leveled> { vec![] }
        """,
    )
    manifest = _read(reader, toolchain, code)
    assert [f.name for f in manifest.types["Row"].fields] == ["firstName", "ID", "at", "type"]
    assert [(p.name, p.required) for p in manifest.export("rows").parameters] == [
        ("region", True),
        ("limit", False),
    ]
    assert manifest.types["Leveled"].fields[0].shape == EnumOf("Level", ("LOW_RISK", "hi"))


def test_code_that_breaks_the_rules_is_refused_with_every_problem(reader, toolchain, tmp_path):
    code = _crate(
        tmp_path,
        """
        #[derive(serde::Serialize)]
        pub struct Big { pub n: i128 }

        /// @table
        pub fn big() -> Vec<Big> { vec![] }

        /// @command
        pub fn borrowed(name: &str) -> String { name.to_string() }

        /// @command
        pub async fn later() -> i64 { 1 }

        /// @command
        pub fn generic<T: Default>() -> i64 { 1 }

        /// @table
        pub fn single() -> Big { Big { n: 1 } }

        /// @tabel
        pub fn misspelt() -> i64 { 1 }

        /// Not offered, and its types are never read.
        pub fn helper() -> std::cell::Cell<i64> { std::cell::Cell::new(1) }
        """,
    )
    with pytest.raises(ManifestRefused) as refused:
        _read(reader, toolchain, code)
    text = "\n".join(refused.value.problems)
    assert "Big.n: the type i128 cannot be mapped" in text
    assert "borrowed(name): the type &str (a parameter is taken by value, not borrowed)" in text
    assert "an async function" in text
    assert "a generic function" in text
    assert "single: @table returns a single value, not a collection of rows" in text
    assert "misspelt: unknown tag @tabel" in text
    assert "helper" not in text


def test_a_tag_is_a_line_of_its_own_and_prose_that_names_one_is_not(reader, toolchain, tmp_path):
    code = _crate(
        tmp_path,
        """
        #[derive(serde::Serialize)]
        pub struct Row { pub n: i64 }

        /// Unlike a @command, this reads.
        pub fn prose() -> Vec<Row> { vec![] }

        /// Read rows.
        ///    @table
        pub fn indented() -> Vec<Row> { vec![] }
        """,
    )
    manifest = _read(reader, toolchain, code)
    assert [(e.name, e.kind, e.description) for e in manifest.exports] == [
        ("indented", "table", "Read rows.")
    ]


def test_two_types_of_one_name_are_refused_by_name(reader, toolchain, tmp_path):
    code = _crate(
        tmp_path,
        """
        mod a;
        mod b;

        /// @table
        pub fn rows() -> Vec<Row> { vec![] }
        """,
        a="#[derive(serde::Serialize)]\npub struct Row { pub n: i64 }\n",
        b="#[derive(serde::Serialize)]\npub struct Row { pub m: i64 }\n",
    )
    with pytest.raises(ManifestRefused, match="two types are named Row, in a and b"):
        _read(reader, toolchain, code)


def test_code_that_does_not_parse_is_refused_with_its_error(reader, toolchain, tmp_path):
    code = _crate(tmp_path, "pub fn broken( {\n")
    with pytest.raises(ManifestRefused, match="the declarations do not type-check: .*lib.rs"):
        _read(reader, toolchain, code)


def test_a_declared_module_with_no_file_is_refused_by_name(reader, toolchain, tmp_path):
    code = _crate(tmp_path, "mod missing;\n")
    with pytest.raises(ManifestRefused, match="module missing: there is no missing.rs"):
        _read(reader, toolchain, code)


def test_a_test_module_is_not_read(reader, toolchain, tmp_path):
    code = _crate(
        tmp_path,
        """
        #[cfg(test)]
        mod tests;

        #[derive(serde::Serialize)]
        pub struct Row { pub n: i64 }

        /// @table
        pub fn rows() -> Vec<Row> { vec![] }
        """,
    )
    assert [e.name for e in _read(reader, toolchain, code).exports] == ["rows"]


# -- the crate ------------------------------------------------------------------------------------


def test_a_crate_is_a_package_with_a_library(tmp_path):
    with pytest.raises(BundleRefused) as missing:
        crate_manifest(tmp_path, "1.95.0")
    assert missing.value.code == "codesource.no_cargo_toml"
    (tmp_path / "Cargo.toml").write_text('[workspace]\nmembers = ["a"]\n')
    with pytest.raises(BundleRefused) as workspace:
        crate_manifest(tmp_path, "1.95.0")
    assert workspace.value.code == "codesource.no_package"


def test_a_rust_version_newer_than_the_pinned_toolchain_is_refused_by_name(tmp_path):
    (tmp_path / "Cargo.toml").write_text(
        '[package]\nname = "rows"\nversion = "0.1.0"\nrust-version = "1.99"\n'
    )
    with pytest.raises(BundleRefused) as refused:
        crate_manifest(tmp_path, "1.95.0")
    assert refused.value.code == "codesource.rust_too_new"
    assert "1.99" in str(refused.value) and "1.95.0" in str(refused.value)


def test_a_crate_with_no_library_is_refused_by_name(reader, toolchain, tmp_path):
    code = tmp_path / "code"
    code.mkdir()
    (code / "Cargo.toml").write_text('[package]\nname = "rows"\nversion = "0.1.0"\n')
    with pytest.raises(BundleRefused) as refused:
        _read(reader, toolchain, code)
    assert refused.value.code == "codesource.no_library"


def test_the_reader_is_compiled_once_per_toolchain(toolchain, tmp_path):
    built: list[list[str]] = []

    def run(argv, env, _cwd):
        built.append(list(argv))
        out = Path(env["CARGO_TARGET_DIR"]) / "release"
        out.mkdir(parents=True)
        (out / "reader").write_text("#!/bin/sh\n")
        return ""

    first = ensure_reader(toolchain, tmp_path, run=run)
    again = ensure_reader(toolchain, tmp_path, run=run)
    assert first == again and len(built) == 1
    assert built[0][1:4] == ["build", "--release", "--locked"]
    assert reader_key("1.95.0") != reader_key("1.96.0")


def test_rust_rules_take_an_iterator_as_a_stream_and_no_standard_tags():
    raw = {
        "exports": [
            {
                "name": "ticks",
                "tags": ["stream"],
                "tag_arguments": [],
                "description": "",
                "signatures": [
                    {
                        "parameters": [],
                        "returns": {
                            "kind": "iterable",
                            "async": False,
                            "generator": True,
                            "errors": False,
                            "element": {"kind": "ref", "name": "Row"},
                        },
                    }
                ],
            },
            {
                "name": "documented",
                "tags": ["param"],
                "tag_arguments": [],
                "description": "",
                "signatures": [],
            },
        ],
        "types": {
            "Row": {
                "name": "Row",
                "fields": [
                    {
                        "name": "n",
                        "optional": False,
                        "type": {"kind": "number", "integer": True},
                        "description": "",
                    }
                ],
            }
        },
    }
    with pytest.raises(ManifestRefused, match="documented: unknown tag @param"):
        read_manifest(raw, RUST_RULES)
    raw["exports"].pop()
    assert read_manifest(raw, RUST_RULES).export("ticks").returns == Named("Row")
