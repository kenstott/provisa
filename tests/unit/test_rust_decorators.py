# Copyright (c) 2026 Kenneth Stott
# Canary: ea9feeed-a52b-421d-a3bc-850fe47100e8
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A Rust source reads every tag a Python source reads but @discover (REQ-2227, REQ-2100): each
tag on a doc comment read by the Rust reader and judged by the shared rules, a function's read
context read from a parameter of a type the code names ``ReadContext``, and a ``@loader``'s map of
each key's hash to its rows read as its rows. Its generated call passes the context and answers a
loader keyed. Serving them is tests/integration/test_rust_decorators.py's."""

# Requirements: REQ-2227, REQ-2100, REQ-2221, REQ-2226, REQ-2249, REQ-1984
from __future__ import annotations

import textwrap
from pathlib import Path

import pytest

from provisa.codesource.manifest import (
    RUST_RULES,
    Manifest,
    ManifestRefused,
    Named,
    Requirement,
    read_manifest,
)
from provisa.runtime_deps.pgwire_bundles import default_cache_root
from provisa.rustlang import build
from provisa.rustlang.reader import ensure_reader, read_code, read_document
from provisa.rustlang.toolchain import Toolchain, ensure_toolchain

pytestmark = pytest.mark.unit

PRELUDE = """
use std::collections::HashMap;

use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
pub struct Row { pub id: i64, pub name: String }

#[derive(Deserialize)]
pub struct Inputs { pub region: Option<String> }

#[derive(Deserialize)]
pub struct ReadContext { pub keys: Option<HashMap<String, HashMap<String, String>>> }

#[derive(Deserialize)]
pub struct TableRef<T> { pub name: String, #[serde(skip)] pub row: Option<T> }
"""


@pytest.fixture(scope="module")
def toolchain() -> Toolchain:
    return ensure_toolchain(default_cache_root(), [])


@pytest.fixture(scope="module")
def reader(toolchain: Toolchain, tmp_path_factory: pytest.TempPathFactory) -> Path:
    return ensure_reader(toolchain, tmp_path_factory.mktemp("cache"))


def _crate(tmp_path: Path, source: str) -> Path:
    code = tmp_path / "code"
    (code / "src").mkdir(parents=True)
    (code / "Cargo.toml").write_text(
        '[package]\nname = "rows"\nversion = "0.1.0"\nedition = "2021"\n'
    )
    (code / "src" / "lib.rs").write_text(PRELUDE + textwrap.dedent(source))
    return code


def _read(reader: Path, toolchain: Toolchain, tmp_path: Path, source: str) -> Manifest:
    code = _crate(tmp_path, source)
    return read_code(reader=reader, code=code, entry=".", toolchain=toolchain.version)


# -- every tag a Python source reads but @discover ------------------------------------------------

#: Each tag of the Python reference (docs/code-sources.md), on a Rust function, and what the
#: manifest makes of it: the function's kind, and the attribute the tag sets.
TAGGED = {
    "table": ("/// @table\npub fn f(inputs: Inputs) -> Vec<Row> { vec![] }", "table"),
    "loader": (
        "/// @table\n/// @loader\n"
        "pub fn f(inputs: Inputs, context: ReadContext) -> HashMap<String, Vec<Row>> "
        "{ HashMap::new() }",
        "table",
    ),
    "stream": (
        "/// @stream\npub fn f() -> impl Iterator<Item = Row> { std::iter::empty() }",
        "stream",
    ),
    "command": ("/// @command\npub fn f(id: i64) -> bool { true }", "command"),
    "insert": (
        "/// @table\npub fn t() -> Vec<Row> { vec![] }\n"
        "/// @command\n/// @insert t\npub fn f(rows: Vec<Row>) -> i64 { 0 }",
        "command",
    ),
    "update": (
        "/// @table\npub fn t() -> Vec<Row> { vec![] }\n"
        "/// @command\n/// @update t\npub fn f(rows: Vec<Row>) -> i64 { 0 }",
        "command",
    ),
    "delete": (
        "/// @table\npub fn t() -> Vec<Row> { vec![] }\n"
        "/// @command\n/// @delete t\npub fn f(rows: Vec<Row>) -> i64 { 0 }",
        "command",
    ),
    "query": ("/// @query\npub fn f(rows: TableRef<Row>) -> Vec<Row> { vec![] }", "query"),
    "columns": ("/// @columns\npub fn f(names: Vec<String>) -> Vec<Row> { vec![] }", "columns"),
    "change": (
        "/// @table\npub fn t() -> Vec<Row> { vec![] }\n"
        "#[derive(Serialize)]\npub struct Changed { pub id: i64, pub removed: bool, pub seq: i64 }\n"
        "/// @change table=t\n/// @change key=id\n/// @change removed=removed\n"
        "/// @change cursor=seq\n"
        "pub fn f(rows: TableRef<Row>, since: Option<i64>) -> Vec<Changed> { vec![] }",
        "change",
    ),
    "on-start": (
        "/// @on-start\npub fn f(values: HashMap<String, String>) -> Result<(), String> { Ok(()) }",
        "on-start",
    ),
    "on-stop": ("/// @on-stop\npub fn f() {}", "on-stop"),
    "health": ('/// @health\npub fn f() -> &\'static str { "ok" }', "health"),
    "requires": (
        "/// @table\n/// @requires REGION\n/// @requires API_KEY:secret\n"
        "pub fn f() -> Vec<Row> { vec![] }",
        "table",
    ),
}


def test_every_python_tag_but_discover_has_a_rust_case():
    python = {
        "table", "loader", "stream", "command", "insert", "update", "delete", "query",
        "columns", "change", "on-start", "on-stop", "health", "requires",
    }  # fmt: skip
    assert set(TAGGED) == python


@pytest.mark.parametrize("tag", sorted(TAGGED))
def test_a_rust_function_reads_each_tag_as_python_does(reader, toolchain, tmp_path, tag):
    source, kind = TAGGED[tag]
    manifest = _read(reader, toolchain, tmp_path, source)
    f = manifest.export("f")
    assert f.kind == kind
    if tag == "loader":
        assert (f.loader, f.loader_batch, f.takes) == (True, None, ("inputs", "context"))
        assert f.returns == Named("Row")
    elif tag in ("insert", "update", "delete"):
        assert f.binds == ((tag, "t"),)
    elif tag == "change":
        assert f.change is not None and f.change.table == "t"
    elif tag == "requires":
        assert manifest.requires == (Requirement("REGION"), Requirement("API_KEY", secret=True))


# -- the read context -----------------------------------------------------------------------------


def test_a_table_or_stream_takes_its_read_context_after_its_inputs(reader, toolchain, tmp_path):
    manifest = _read(
        reader,
        toolchain,
        tmp_path,
        """
        /// @table
        pub fn both(inputs: Inputs, context: ReadContext) -> Vec<Row> { vec![] }

        /// @table
        pub fn hinted(context: ReadContext) -> Vec<Row> { vec![] }

        /// @stream
        pub fn ticks(inputs: Inputs, context: ReadContext) -> impl Iterator<Item = Row> {
            std::iter::empty()
        }
        """,
    )
    assert manifest.export("both").takes == ("inputs", "context")
    assert [p.name for p in manifest.export("both").parameters] == ["region"]
    assert manifest.export("hinted").takes == ("context",)
    assert manifest.export("hinted").parameters == ()
    assert manifest.export("ticks").takes == ("inputs", "context")
    # Provisa's context is named, never read as the code's own type.
    assert "ReadContext" not in manifest.types


def test_the_read_context_is_a_type_named_and_never_read(reader, toolchain, tmp_path):
    code = _crate(
        tmp_path,
        """
        /// @table
        pub fn hinted(context: ReadContext) -> Vec<Row> { vec![] }
        """,
    )
    document = read_document(reader, code, toolchain.version)
    (hinted,) = document["exports"]
    (signature,) = hinted["signatures"]
    assert signature["parameters"][0]["type"] == {"kind": "ref", "name": "ReadContext"}
    assert signature["context"] is True
    assert "ReadContext" not in document["types"]


def test_a_context_before_its_inputs_is_refused_by_name(reader, toolchain, tmp_path):
    with pytest.raises(ManifestRefused) as refused:
        _read(
            reader,
            toolchain,
            tmp_path,
            """
            /// @table
            pub fn rows(context: ReadContext, inputs: Inputs) -> Vec<Row> { vec![] }
            """,
        )
    assert "rows: a @table function takes (inputs, context)" in refused.value.problems[0]


# -- @loader --------------------------------------------------------------------------------------


def test_a_loader_declares_its_batch_size(reader, toolchain, tmp_path):
    manifest = _read(
        reader,
        toolchain,
        tmp_path,
        """
        /// @table
        /// @loader 50
        pub fn scores(inputs: Inputs, context: ReadContext)
            -> Result<HashMap<String, Vec<Row>>, String> { Ok(HashMap::new()) }
        """,
    )
    scores = manifest.export("scores")
    assert (scores.loader, scores.loader_batch, scores.returns) == (True, 50, Named("Row"))


def test_a_loader_without_a_read_context_is_refused_by_name(reader, toolchain, tmp_path):
    with pytest.raises(ManifestRefused) as refused:
        _read(
            reader,
            toolchain,
            tmp_path,
            """
            /// @table
            /// @loader
            pub fn rows(inputs: Inputs) -> HashMap<String, Vec<Row>> { HashMap::new() }
            """,
        )
    assert refused.value.problems == [
        "rows: a @loader is given the join's keys in its read context, and rows takes no "
        "ReadContext"
    ]


def test_a_map_of_rows_is_a_loaders_alone(reader, toolchain, tmp_path):
    with pytest.raises(ManifestRefused) as refused:
        _read(
            reader,
            toolchain,
            tmp_path,
            """
            /// @table
            pub fn rows(context: ReadContext) -> HashMap<String, Vec<Row>> { HashMap::new() }
            """,
        )
    assert refused.value.problems == [
        "rows: @table returns a single value, not a collection of rows"
    ]


def test_the_sdk_registers_a_loader_and_its_batch_size(reader, toolchain, tmp_path):
    code = _crate(
        tmp_path,
        """
        pub fn a(inputs: Inputs, context: ReadContext) -> HashMap<String, Vec<Row>> {
            HashMap::new()
        }

        pub fn b(inputs: Inputs, context: ReadContext) -> HashMap<String, Vec<Row>> {
            HashMap::new()
        }

        pub fn c(context: ReadContext) -> Vec<Row> { vec![] }

        provisa_sdk::table!(a, loader = true);
        provisa_sdk::table!(b, loader = 25, requires = "REGION");
        provisa_sdk::table!(c, loader = false);
        provisa_sdk::stream!(c, loader = 5);
        """,
    )
    (code / "Cargo.toml").write_text(
        '[package]\nname = "rows"\nversion = "0.1.0"\nedition = "2021"\n\n'
        '[dependencies]\nprovisa-sdk = { path = "unused" }\n'
    )
    document = read_document(reader, code, toolchain.version)
    assert document["diagnostics"] == [
        "c: provisa_sdk::table!'s loader is true or a batch size, not false",
        "c: provisa_sdk::stream! takes no option loader",
    ]
    exports = {e["name"]: e for e in document["exports"]}
    assert exports["a"]["tags"] == ["table", "loader"]
    assert exports["b"]["tags"] == ["table"]
    assert {"tag": "loader", "text": "25"} in exports["b"]["tag_arguments"]
    document["diagnostics"] = []
    document["exports"] = [exports["a"], exports["b"]]
    manifest = read_manifest(document, RUST_RULES)
    a, b = manifest.export("a"), manifest.export("b")
    assert (a.loader, a.loader_batch, b.loader, b.loader_batch) == (True, None, True, 25)


# -- the generated calls --------------------------------------------------------------------------


def test_the_generated_call_passes_the_context_and_answers_a_loader_keyed(
    reader, toolchain, tmp_path
):
    code = _crate(
        tmp_path,
        """
        /// @table
        /// @loader
        pub fn scores(inputs: Inputs, context: ReadContext)
            -> Result<HashMap<String, Vec<Row>>, String> { Ok(HashMap::new()) }

        /// @table
        pub fn hinted(context: ReadContext) -> Vec<Row> { vec![] }

        /// @stream
        pub fn ticks(inputs: Inputs, context: ReadContext) -> impl Iterator<Item = Row> {
            std::iter::empty()
        }
        """,
    )
    document = read_document(reader, code, toolchain.version)
    source = build.exports_source(document, read_manifest(document, RUST_RULES))
    assert (
        '("scores", |a: &Args| { keyed(code::r#scores(inputs(a)?, context(a)?)'
        ".map_err(failed)?) }),"
    ) in source
    assert '("hinted", |a: &Args| { rows(code::r#hinted(context(a)?)) }),' in source
    assert '("ticks", |a: &Args| { rows(code::r#ticks(inputs(a)?, context(a)?)) }),' in source
