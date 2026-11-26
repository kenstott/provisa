# Copyright (c) 2026 Kenneth Stott
# Canary: c9fe6071-266c-43bf-ab7a-10726ef55cd9
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The Rust reader's SDK registrations (REQ-1984), on the pinned toolchain: a registration is
known by its path (the SDK crate's name, the name Cargo.toml gives it, a root `use ... as`, or a
name a root `use` brings in) and adds its tags to the root `pub fn` it names; the SDK's type
aliases read as the types they name; a registration naming anything but a root `pub fn` by its
name, an option that is not a literal, a key the registration does not take, an SDK macro
invoked anywhere but at the crate's root, and a macro the SDK does not have are each refused by
name; and cargo check rejects a function whose shape is not its tag's."""

# Requirements: REQ-1984
from __future__ import annotations

from pathlib import Path

import pytest

from tests.fixtures.code_sdk_spellings.rust_crate import SDK, RustToolchain, write_crate

pytestmark = [pytest.mark.integration]

PRELUDE = """pub struct Row {
    pub id: i64,
}

pub struct Inputs {
    pub region: Option<String>,
}

"""


@pytest.fixture(scope="module")
def toolchain(tmp_path_factory) -> RustToolchain:
    return RustToolchain.resolve(tmp_path_factory.mktemp("rust-cache"))


def _read(toolchain: RustToolchain, directory: Path, body: str, **toml: str):
    crate = write_crate(directory, PRELUDE + body, **toml)
    checked = toolchain.check(crate)
    assert checked.returncode == 0, checked.stderr
    return toolchain.read(crate)


def _refused(toolchain: RustToolchain, directory: Path, body: str, **toml: str) -> tuple[str, ...]:
    """What the reader refuses of code cargo check may reject too: reading compiles nothing."""
    got = toolchain.read(write_crate(directory, PRELUDE + body, **toml))
    assert isinstance(got, tuple), got
    return got


def _export(manifest: dict | tuple[str, ...], name: str) -> dict:
    assert isinstance(manifest, dict), manifest
    (found,) = [e for e in manifest["exports"] if e["name"] == name]
    return found


ROWS = "pub fn rows() -> Vec<Row> {\n    Vec::new()\n}\n\n"


# -- the paths a registration is known by -------------------------------------------------------


def test_the_name_cargo_toml_gives_the_sdk_registers(toolchain, tmp_path):
    got = _read(
        toolchain,
        tmp_path / "c",
        ROWS + "p::table!(rows);\n",
        dependency=f'p = {{ package = "provisa-sdk", path = "{SDK}" }}',
    )
    assert _export(got, "rows")["kind"] == "table"


def test_a_target_specific_dependency_names_the_sdk_too(toolchain, tmp_path):
    crate = write_crate(tmp_path / "c", PRELUDE + ROWS + "provisa::table!(rows);\n", dependency="")
    toml = crate / "Cargo.toml"
    toml.write_text(
        toml.read_text()
        + f'\n[target.\'cfg(unix)\'.dependencies]\nprovisa = {{ package = "provisa-sdk", path = "{SDK}" }}\n'
    )
    assert _export(toolchain.read(crate), "rows")["kind"] == "table"


def test_a_root_use_as_names_the_sdk(toolchain, tmp_path):
    body = "use provisa_sdk as p;\n\n" + ROWS + "p::table!(rows);\n"
    assert _export(_read(toolchain, tmp_path / "c", body), "rows")["kind"] == "table"


def test_an_imported_and_a_renamed_macro_register(toolchain, tmp_path):
    body = (
        "use provisa_sdk::{stream as live, table};\n\n"
        + ROWS
        + "pub fn ticks() -> impl Iterator<Item = Row> {\n    std::iter::empty()\n}\n\n"
        "table!(rows);\nlive!(ticks);\n"
    )
    got = _read(toolchain, tmp_path / "c", body)
    assert (_export(got, "rows")["kind"], _export(got, "ticks")["kind"]) == ("table", "stream")


def test_a_glob_import_registers_and_a_root_macro_of_the_same_name_is_the_codes(
    toolchain, tmp_path
):
    body = (
        "use provisa_sdk::*;\n\n"
        + ROWS
        + "pub fn ticks() -> Stream<Row> {\n    Box::new(std::iter::empty())\n}\n\n"
        "macro_rules! table {\n    ($f:ident) => {};\n}\n\n"
        "table!(rows);\nstream!(ticks);\n"
    )
    got = _read(toolchain, tmp_path / "c", body)
    assert isinstance(got, dict), got
    assert [e["name"] for e in got["exports"]] == ["ticks"]


def test_an_unknown_macro_of_another_crate_is_not_the_sdks(toolchain, tmp_path):
    body = "use provisa_sdk::*;\n\n" + ROWS + "table!(rows);\nvec_of!(rows);\n"
    crate = write_crate(tmp_path / "c", PRELUDE + body)
    assert _export(toolchain.read(crate), "rows")["kind"] == "table"


# -- what a registration adds -------------------------------------------------------------------


PUT = "pub fn put(rows: Vec<Row>) -> i64 {\n    rows.len() as i64\n}\n\n"


def test_options_give_their_doc_comment_twins_manifest(toolchain, tmp_path):
    sdk = (
        ROWS
        + PUT
        + 'provisa_sdk::table!(rows, secret = "API_KEY", requires = "REGION");\n'
        + 'provisa_sdk::command!(put, insert = "rows", requires = "REGION");\n'
    )
    doc = (
        "/// @table\n/// @requires API_KEY:secret\n/// @requires REGION\n"
        + ROWS
        + "/// @command\n/// @insert rows\n/// @requires REGION\n"
        + PUT
    )
    got = _read(toolchain, tmp_path / "s", sdk)
    assert isinstance(got, dict), got
    assert got["requires"] == [
        {"name": "API_KEY", "secret": True},
        {"name": "REGION", "secret": False},
    ]
    assert got == _read(toolchain, tmp_path / "d", doc)


def test_the_sdk_aliases_read_as_the_types_they_name(toolchain, tmp_path):
    aliased = (
        "use provisa_sdk::{BigInt, Boolean, Double, Text};\n\n"
        "pub struct Typed {\n    pub a: Text,\n    pub b: BigInt,\n    pub c: Double,\n"
        "    pub d: Boolean,\n}\n\n"
        "/// @table\npub fn rows() -> provisa_sdk::Rows<Typed> {\n    Vec::new()\n}\n"
    )
    plain = (
        "pub struct Typed {\n    pub a: String,\n    pub b: i64,\n    pub c: f64,\n"
        "    pub d: bool,\n}\n\n"
        "/// @table\npub fn rows() -> Vec<Typed> {\n    Vec::new()\n}\n"
    )
    assert _read(toolchain, tmp_path / "a", aliased) == _read(toolchain, tmp_path / "p", plain)


def test_a_change_registration_gives_its_doc_comment_twins_manifest(toolchain, tmp_path):
    function = "pub fn changes(since: Option<i64>) -> Vec<Row> {\n    let _ = since;\n    Vec::new()\n}\n\n"
    sdk = (
        function
        + 'provisa_sdk::change!(changes, table = "t", key = ["a", "b"], removed = "gone", '
        + 'cursor = "seq");\n'
    )
    doc = (
        "/// @change table=t\n/// @change key=a,b\n/// @change removed=gone\n"
        "/// @change cursor=seq\n" + function
    )
    assert _read(toolchain, tmp_path / "s", sdk) == toolchain.read(
        write_crate(tmp_path / "d", PRELUDE + doc)
    )


def test_a_registration_and_a_doc_tag_merge(toolchain, tmp_path):
    body = "/// @table\n" + ROWS + "provisa_sdk::table!(rows);\n"
    assert _export(_read(toolchain, tmp_path / "c", body), "rows")["kind"] == "table"


def test_two_kinds_on_one_function_are_refused(toolchain, tmp_path):
    body = ROWS + "provisa_sdk::table!(rows);\nprovisa_sdk::query!(rows);\n"
    (problem,) = _refused(toolchain, tmp_path / "c", body)
    assert "rows" in problem and "@table" in problem and "@query" in problem


# -- refused by name ----------------------------------------------------------------------------


def test_a_non_literal_option_is_refused_by_name(toolchain, tmp_path):
    body = ROWS + 'const NAME: &str = "API_KEY";\n\nprovisa_sdk::table!(rows, requires = NAME);\n'
    assert _refused(toolchain, tmp_path / "c", body) == (
        "the declarations do not type-check: rows: provisa_sdk::table!'s requires is a "
        "literal, not NAME",
    )


def test_a_key_the_registration_does_not_take_is_refused_by_name(toolchain, tmp_path):
    body = (
        "pub fn ticks() -> impl Iterator<Item = Row> {\n    std::iter::empty()\n}\n\n"
        "provisa_sdk::stream!(ticks, loader = 100);\n"
    )
    assert _refused(toolchain, tmp_path / "c", body) == (
        "the declarations do not type-check: ticks: provisa_sdk::stream! takes no option loader",
    )


def test_a_registration_naming_anything_but_a_root_pub_fn_is_refused_by_name(toolchain, tmp_path):
    body = (
        "mod inner {\n    pub fn rows() -> Vec<super::Row> {\n        Vec::new()\n    }\n}\n\n"
        "fn hidden() -> Vec<Row> {\n    Vec::new()\n}\n\n"
        "provisa_sdk::table!(inner::rows);\nprovisa_sdk::table!(hidden);\n"
    )
    assert _refused(toolchain, tmp_path / "c", body) == (
        "the declarations do not type-check: provisa_sdk::table!(inner::rows): a registration "
        "names a pub fn at the crate's root, not inner::rows",
        "the declarations do not type-check: provisa_sdk::table!(hidden): hidden is not pub, "
        "and Provisa serves a pub fn",
    )


def test_an_sdk_macro_invoked_inside_a_module_or_a_function_is_refused_by_name(toolchain, tmp_path):
    body = (
        "mod inner {\n    pub fn rows() -> Vec<super::Row> {\n        Vec::new()\n    }\n"
        "    provisa_sdk::table!(rows);\n}\n\n"
        "pub fn outer() -> Vec<Row> {\n    provisa_sdk::table!(outer);\n    Vec::new()\n}\n"
    )
    assert _refused(toolchain, tmp_path / "c", body) == (
        "the declarations do not type-check: provisa_sdk::table! is invoked in module inner, "
        "where Provisa does not read it; a registration is an item at the crate's root",
        "the declarations do not type-check: provisa_sdk::table! is invoked inside an item, "
        "where Provisa does not read it; a registration is an item at the crate's root",
    )


def test_a_macro_the_sdk_does_not_have_is_refused_by_name(toolchain, tmp_path):
    body = ROWS + "provisa_sdk::table_v2!(rows);\n"
    assert _refused(toolchain, tmp_path / "c", body) == (
        "the declarations do not type-check: provisa_sdk::table_v2! is not a registration this "
        "server reads; it reads SDK spelling 1",
    )


def test_unreadable_registration_arguments_are_refused_by_name(toolchain, tmp_path):
    body = ROWS + "provisa_sdk::table!(rows requires);\n"
    (problem,) = _refused(toolchain, tmp_path / "c", body)
    assert problem.startswith(
        "the declarations do not type-check: provisa_sdk::table!(rows requires): its arguments "
        "cannot be read:"
    )


# -- cargo check holds each function to its tag's shape -----------------------------------------

BAD_SHAPES = {
    "a table returning one row": "pub fn r() -> Row {\n    Row { id: 0 }\n}\nprovisa_sdk::table!(r);\n",
    "a table of three inputs": (
        "pub fn r(a: Inputs, b: Inputs, c: Inputs) -> Vec<Row> {\n    let _ = (a, b, c);\n"
        "    Vec::new()\n}\nprovisa_sdk::table!(r);\n"
    ),
    "a table returning an Option": (
        "pub fn r() -> Option<Vec<Row>> {\n    None\n}\nprovisa_sdk::table!(r);\n"
    ),
    "a stream returning a Vec": "pub fn r() -> Vec<Row> {\n    Vec::new()\n}\nprovisa_sdk::stream!(r);\n",
    "a health returning a number": "pub fn r() -> i64 {\n    0\n}\nprovisa_sdk::health!(r);\n",
    "an on-start taking a number": "pub fn r(n: i64) {\n    let _ = n;\n}\nprovisa_sdk::on_start!(r);\n",
    "an on-stop returning rows": "pub fn r() -> Vec<Row> {\n    Vec::new()\n}\nprovisa_sdk::on_stop!(r);\n",
    "columns taking a scalar": (
        "pub fn r(n: i64) -> Vec<Row> {\n    let _ = n;\n    Vec::new()\n}\n"
        "provisa_sdk::columns!(r);\n"
    ),
    "a non-literal option": (
        'pub fn r() -> Vec<Row> {\n    Vec::new()\n}\nconst N: &str = "X";\n'
        "provisa_sdk::table!(r, requires = N);\n"
    ),
    "a number as a name": "pub fn r() -> Vec<Row> {\n    Vec::new()\n}\nprovisa_sdk::table!(r, requires = 5);\n",
    "a loader on a stream": (
        "pub fn r() -> impl Iterator<Item = Row> {\n    std::iter::empty()\n}\n"
        "provisa_sdk::stream!(r, loader = 100);\n"
    ),
    "a loader of a text size": (
        'pub fn r() -> Vec<Row> {\n    Vec::new()\n}\nprovisa_sdk::table!(r, loader = "100");\n'
    ),
    "a write option on a table": (
        'pub fn r() -> Vec<Row> {\n    Vec::new()\n}\nprovisa_sdk::table!(r, insert = "t");\n'
    ),
    "a misspelled registration": "pub fn r() -> Vec<Row> {\n    Vec::new()\n}\nprovisa_sdk::tabel!(r);\n",
    "a registration of no function": "provisa_sdk::table!(nothing);\n",
}


@pytest.mark.parametrize("shape", sorted(BAD_SHAPES))
def test_cargo_check_rejects_a_bad_shape(toolchain, tmp_path, shape):
    checked = toolchain.check(write_crate(tmp_path / "c", PRELUDE + BAD_SHAPES[shape]))
    assert checked.returncode != 0, shape
    assert "error" in checked.stderr


def test_cargo_check_passes_every_shape_the_reader_accepts(toolchain, tmp_path):
    body = (
        "use std::collections::{BTreeMap, HashMap};\n\nuse provisa_sdk::{Batch, Results, Stream};\n\n"
        "pub fn a() -> Vec<Row> {\n    Vec::new()\n}\n"
        "pub fn b(i: Inputs) -> Result<Vec<Row>, String> {\n    let _ = i;\n    Ok(Vec::new())\n}\n"
        "pub fn c(i: Inputs) -> impl Iterator<Item = Row> {\n    let _ = i;\n    std::iter::empty()\n}\n"
        "pub fn d() -> Stream<Result<Row, String>> {\n    Box::new(std::iter::empty())\n}\n"
        "pub fn e(keys: Vec<i64>, name: Option<String>) -> i64 {\n    let _ = name;\n"
        "    keys.len() as i64\n}\n"
        "pub fn f(names: Batch<String>) -> Results<Row> {\n"
        "    names.iter().map(|_| Row { id: 0 }).collect()\n}\n"
        "pub fn g(v: HashMap<String, String>) -> Result<(), String> {\n    let _ = v;\n    Ok(())\n}\n"
        "pub fn h() {}\n"
        'pub fn i() -> Result<String, std::io::Error> {\n    Ok("ok".into())\n}\n'
        "pub struct ReadContext {\n    pub limit: Option<i64>,\n}\n"
        "pub fn j(i: Inputs, c: ReadContext) -> HashMap<String, Vec<Row>> {\n"
        "    let _ = (i, c);\n    HashMap::new()\n}\n"
        "pub fn k(c: ReadContext) -> Result<BTreeMap<String, Vec<Row>>, String> {\n"
        "    let _ = c;\n    Ok(BTreeMap::new())\n}\n"
        "pub fn l(i: Inputs, c: ReadContext) -> impl Iterator<Item = Row> {\n"
        "    let _ = (i, c);\n    std::iter::empty()\n}\n"
        "provisa_sdk::table!(a);\nprovisa_sdk::table!(b);\nprovisa_sdk::table!(c);\n"
        'provisa_sdk::stream!(d);\nprovisa_sdk::command!(e, update = "rows");\n'
        "provisa_sdk::columns!(f);\nprovisa_sdk::on_start!(g);\nprovisa_sdk::on_stop!(h);\n"
        "provisa_sdk::health!(i);\nprovisa_sdk::table!(j, loader = true);\n"
        "provisa_sdk::table!(k, loader = 50);\nprovisa_sdk::stream!(l);\n"
    )
    got = _read(toolchain, tmp_path / "c", body)
    assert isinstance(got, dict), got
    assert {e["name"]: e["kind"] for e in got["exports"]} == {
        "a": "table",
        "b": "table",
        "c": "table",
        "d": "stream",
        "e": "command",
        "f": "columns",
        "g": "on-start",
        "h": "on-stop",
        "i": "health",
        "j": "table",
        "k": "table",
        "l": "stream",
    }
    loaders = {e["name"]: (e["loader"], e["loader_batch"]) for e in got["exports"]}
    assert (loaders["j"], loaders["k"]) == ((True, None), (True, 50))
