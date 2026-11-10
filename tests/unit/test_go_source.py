# Copyright (c) 2026 Kenneth Stott
# Canary: 7e3b9c15-6d2a-4f80-b4e7-1c9a5d8f2e36
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A Go source (REQ-2200): its exported functions read from their doc comments and signatures
by the Go reader, and judged by the shared rules. The reader is compiled here by the pinned Go
toolchain, its dependency fetched from the public module proxy and checked against its go.sum."""

# Requirements: REQ-2200
from __future__ import annotations

import textwrap
from pathlib import Path

import pytest

from provisa.codesource.bundle import BundleRefused
from provisa.codesource.manifest import (
    GO_RULES,
    Manifest,
    EnumOf,
    Field,
    ListOf,
    ManifestRefused,
    Named,
    Scalar,
    TableOf,
    read_manifest,
)
from provisa.runtime_deps.code_runtimes import RUNTIMES, CodeRuntimeResolver
from provisa.golang.build import build_module
from provisa.golang.install import check_lockfile
from provisa.golang.reader import (
    READER,
    SERVICES,
    SERVICES_MODULE,
    check_go_line,
    ensure_reader,
    go_environment,
    read_code,
    reader_key,
    services_go_mod,
)

pytestmark = pytest.mark.unit

FIXTURE = Path(__file__).parent.parent / "fixtures" / "go_source"


@pytest.fixture(scope="module")
def go() -> Path:
    """The pinned Go toolchain, fetched and checked on first use."""
    return CodeRuntimeResolver().resolve("go")


@pytest.fixture(scope="module")
def reader(go: Path, tmp_path_factory: pytest.TempPathFactory) -> Path:
    return ensure_reader(
        go, RUNTIMES["go"].version, tmp_path_factory.mktemp("cache"), "https://proxy.golang.org"
    )


def _read(reader: Path, go: Path, code: Path, tmp_path: Path, package: str = ".") -> Manifest:
    """The manifest read from ``code``, or the refusal."""
    return read_code(
        reader=reader,
        go=go,
        code=code,
        package=package,
        gomodcache=tmp_path / "mod",
        scratch=tmp_path,
    )


def _code(tmp_path: Path, source: str) -> Path:
    code = tmp_path / "code"
    code.mkdir()
    (code / "go.mod").write_text("module example.com/rows\n\ngo 1.23\n")
    (code / "rows.go").write_text(textwrap.dedent(source))
    return code


# -- reading ------------------------------------------------------------------------------------


def test_the_lifecycle_tags_are_read_from_a_doc_comment(reader, go, tmp_path):
    """REQ-2221: Go reads @on-start, @on-stop and @health like every other language."""
    code = _code(
        tmp_path,
        """
        package rows

        import "context"

        // Connect.
        //
        // @on-start
        func Connect(ctx context.Context, values map[string]string) error { return nil }

        // Close.
        //
        // @on-stop
        func Close() error { return nil }

        // Check.
        //
        // @health
        func Check() (string, error) { return "ok", nil }
        """,
    )
    manifest = _read(reader, go, code, tmp_path)
    assert {e.name: e.kind for e in manifest.exports} == {
        "Connect": "on-start",
        "Close": "on-stop",
        "Check": "health",
    }
    assert [p.name for p in manifest.lifecycle("on-start").parameters] == ["values"]


def test_the_codes_tables_commands_and_streams_are_read_from_doc_comments_and_types(
    reader, go, tmp_path
):
    manifest = _read(reader, go, FIXTURE, tmp_path)
    assert [(e.name, e.kind) for e in manifest.exports] == [
        ("Orders", "table"),
        ("Many", "table"),
        ("Placed", "stream"),
        ("Ticks", "stream"),
        ("Forever", "stream"),
        ("CancelOrder", "command"),
        ("Boom", "table"),
        ("Outside", "table"),
        ("Fetched", "table"),
        ("Direct", "table"),
        ("Greet", "table"),
        ("CleanAddress", "columns"),
    ]  # Untagged is not one
    orders = manifest.export("Orders")
    assert orders.description == "Orders of one customer."
    assert orders.takes == ("inputs",)
    assert [(p.name, p.shape, p.nullable, p.required) for p in orders.parameters] == [
        ("customer_id", Scalar("text"), False, True),
        ("since", Scalar("timestamp"), True, False),  # a pointer field may be left out
    ]
    assert orders.returns == Named("Order")
    assert manifest.types["Order"].fields == (
        Field("id", Scalar("bigint"), False),
        Field("placed", Scalar("timestamp"), False),
        Field("tier", EnumOf("Tier", ("gold", "silver")), False),
        Field("lines", ListOf(Named("Line")), False),
        Field("note", Scalar("text"), True, "Note is what the customer wrote."),
    )
    assert manifest.export("Many").returns == Named("Counted")
    # The call's context.Context is no input of the function's.
    assert [p.name for p in manifest.export("Placed").parameters] == ["region"]
    assert manifest.export("Forever").parameters == ()
    assert manifest.export("Fetched").takes == ("inputs",)
    command = manifest.export("CancelOrder")
    assert [(p.name, p.shape, p.required) for p in command.parameters] == [
        ("orderID", Scalar("bigint"), True)
    ]
    assert command.returns == Scalar("json") and not command.many
    columns = manifest.export("CleanAddress")
    assert columns.parameters[0].shape == ListOf(Scalar("text"))
    assert columns.returns == Named("Address")


def test_a_fields_name_is_the_one_encoding_json_gives_it(reader, go, tmp_path):
    code = _code(
        tmp_path,
        """
        package rows

        type Base struct {
            Region string `json:"region"`
        }

        type Row struct {
            Base
            ID       int64  `json:"id,omitempty"`
            Plain    string
            Skipped  string `json:"-"`
            Dash     string `json:"-,"`
            internal string
        }

        // @table
        func Rows() []Row { return nil }
        """,
    )
    manifest = _read(reader, go, code, tmp_path)
    assert [f.name for f in manifest.types["Row"].fields] == ["region", "id", "Plain", "-"]


def test_a_loose_row_type_is_refused_by_name(reader, go, tmp_path):
    code = _code(
        tmp_path,
        """
        package rows

        import "iter"

        type Row struct {
            ID      int64          `json:"id"`
            Payload map[string]any `json:"payload"`
        }

        // @table
        func Mapped() []map[string]any { return nil }

        // @table
        func Anything() []any { return nil }

        // @stream
        func Feed() iter.Seq2[map[string]any, error] { return nil }

        // @table
        func Typed() []Row { return nil }
        """,
    )
    with pytest.raises(ManifestRefused) as refused:
        _read(reader, go, code, tmp_path)
    assert sorted(refused.value.problems) == [
        f"{name}: its row type is loose (any, unknown, a Record or a dict); declare a typed row "
        "whose fields are its columns"
        for name in ("Anything", "Feed", "Mapped")
    ]


def test_a_single_field_of_a_typed_row_may_still_be_loose(reader, go, tmp_path):
    code = _code(
        tmp_path,
        """
        package rows

        type Row struct {
            ID      int64          `json:"id"`
            Payload map[string]any `json:"payload"`
        }

        // @table
        func Typed() []Row { return nil }
        """,
    )
    assert _read(reader, go, code, tmp_path).types["Row"].fields == (
        Field("id", Scalar("bigint"), False),
        Field("payload", Scalar("json"), False),
    )


def test_code_that_breaks_the_rules_is_refused_with_every_problem(reader, go, tmp_path):
    code = _code(
        tmp_path,
        """
        package rows

        type Row struct{ ID int64 }

        // @table
        func One() Row { return Row{} }

        // @table
        // @command
        func Both() []Row { return nil }

        // @tabel
        func Typo() []Row { return nil }

        // @command
        func Pipe(in chan int) error { return nil }

        // @command
        func Pair() (Row, Row, error) { return Row{}, Row{}, nil }

        // @command
        func Spread(ids ...int64) error { return nil }

        // @stream
        func Once() []Row { return nil }

        // @command
        func Complex(c complex128) error { return nil }

        // @command
        func Keyed(m map[int]string) error { return nil }
        """,
    )
    with pytest.raises(ManifestRefused) as refused:
        _read(reader, go, code, tmp_path)
    problems = "; ".join(refused.value.problems)
    for expected in (
        "One: @table returns a single value, not a collection of rows",
        "Both: tagged @command, @table; an export is one",
        "Typo: unknown tag @tabel",
        "Pipe(in): the type chan int cannot be mapped",
        "Pair (result): the type 2 results (a function returns one, and an error) cannot be mapped",
        "Spread: the rest parameter ids cannot be a parameter",
        "Once: @stream returns iter.Seq2[T, error]",
        "Complex(c): the type complex128 cannot be mapped",
        "Keyed(m): the type map[int]string cannot be mapped",
    ):
        assert expected in problems, problems


def test_a_tag_is_a_line_of_its_own_and_prose_that_names_one_is_not(reader, go, tmp_path):
    code = _code(
        tmp_path,
        """
        package rows

        type Row struct{ ID int64 }

        // Rows is not a @table, though this line names one.
        func Rows() []Row { return nil }

        // Loaded is one.
        //
        //   @table
        func Loaded() []Row { return nil }
        """,
    )
    manifest = _read(reader, go, code, tmp_path)
    assert [e.name for e in manifest.exports] == ["Loaded"]
    assert manifest.export("Loaded").description == "Loaded is one."


def test_a_stream_yields_a_row_and_an_error_and_a_table_may_be_an_iterator(reader, go, tmp_path):
    code = _code(
        tmp_path,
        """
        package rows

        import "iter"

        type Row struct{ ID int64 }

        // @stream
        func Failing() iter.Seq2[Row, error] { return nil }

        // @table
        func Listed() iter.Seq2[Row, error] { return nil }

        // @table
        func Plain() iter.Seq[Row] { return nil }

        // @stream
        func Rows() iter.Seq[Row] { return nil }

        // @stream
        func Paired() iter.Seq2[Row, string] { return nil }
        """,
    )
    with pytest.raises(ManifestRefused) as refused:
        _read(reader, go, code, tmp_path)
    assert refused.value.problems == [
        "Rows: @stream returns iter.Seq2[T, error]",
        "Paired (row): the type iter.Seq2[Row, string] (its second value is not an error) cannot "
        "be mapped",
        "Paired: its rows are not objects; a row is an object whose fields are columns",
    ]
    (fixed := code / "rows.go").write_text(
        fixed.read_text()
        .replace("func Rows() iter.Seq[Row]", "func Rows() iter.Seq2[Row, error]")
        .replace("iter.Seq2[Row, string]", "iter.Seq2[Row, error]")
    )
    manifest = _read(reader, go, code, tmp_path)
    assert [(e.name, e.kind, e.returns) for e in manifest.exports] == [
        ("Failing", "stream", Named("Row")),
        ("Listed", "table", Named("Row")),
        ("Plain", "table", Named("Row")),
        ("Rows", "stream", Named("Row")),
        ("Paired", "stream", Named("Row")),
    ]


def test_a_parameter_beside_the_context_and_the_inputs_is_refused_by_name(reader, go, tmp_path):
    code = _code(
        tmp_path,
        """
        package rows

        import "context"

        type Row struct{ ID int64 }

        type In struct{ Region string }

        // @table
        func Rows(ctx context.Context, in In) ([]Row, error) { return nil, nil }

        // @table
        func Extra(ctx context.Context, in In, limit int) ([]Row, error) { return nil, nil }

        // @table
        func Scalar(region string) ([]Row, error) { return nil, nil }

        // @table
        func Late(in In, ctx context.Context) ([]Row, error) { return nil, nil }
        """,
    )
    with pytest.raises(ManifestRefused) as refused:
        _read(reader, go, code, tmp_path)
    assert refused.value.problems == [
        "Extra: a @table function takes (inputs, context) -- an object whose fields are its "
        "inputs, and a ReadContext, either left out -- not (in, limit)",
        "Scalar(region): its inputs are an object whose fields are its inputs, a dataclass, a "
        "TypedDict or an interface",
        "Late: a @table function takes (inputs, context) -- an object whose fields are its "
        "inputs, and a ReadContext, either left out -- not (in, ctx)",
    ]


def test_a_go_line_newer_than_the_pinned_toolchain_is_refused_by_name(tmp_path):
    (tmp_path / "go.mod").write_text("module example.com/rows\n\ngo 1.99.0\n")
    with pytest.raises(BundleRefused) as refused:
        check_go_line(tmp_path, "1.27.3")
    assert refused.value.code == "codesource.go_too_new"
    assert "asks for go 1.99.0, and this Provisa's Go is 1.27.3" in str(refused.value)
    (tmp_path / "go.mod").write_text("module example.com/rows\n\ngo 1.27\n")
    check_go_line(tmp_path, "1.27.3")
    (tmp_path / "go.mod").unlink()
    with pytest.raises(BundleRefused, match="the code has no go.mod"):
        check_go_line(tmp_path, "1.27.3")


def test_code_that_does_not_type_check_is_refused_with_its_error(reader, go, tmp_path):
    code = _code(
        tmp_path,
        """
        package rows

        // @table
        func Rows() []Missing { return nil }
        """,
    )
    with pytest.raises(ManifestRefused) as refused:
        _read(reader, go, code, tmp_path)
    assert "undefined: Missing" in "; ".join(refused.value.problems)


def test_a_pattern_naming_several_packages_is_refused_naming_them(reader, go, tmp_path):
    code = _code(tmp_path, "package rows\n")
    (code / "more").mkdir()
    (code / "more" / "more.go").write_text("package more\n")
    with pytest.raises(ManifestRefused) as refused:
        _read(reader, go, code, tmp_path, package="./...")
    assert refused.value.problems == [
        "the declarations do not type-check: ./... names 2 packages (example.com/rows, "
        "example.com/rows/more); a source is one package"
    ]


def test_two_types_of_one_name_are_refused_by_name(reader, go, tmp_path):
    code = _code(
        tmp_path,
        """
        package rows

        import "example.com/rows/other"

        type Row struct{ ID int64 }

        type Both struct {
            Mine   Row       `json:"mine"`
            Theirs other.Row `json:"theirs"`
        }

        // @table
        func Rows() []Both { return nil }
        """,
    )
    (code / "other").mkdir()
    (code / "other" / "other.go").write_text("package other\n\ntype Row struct{ Name string }\n")
    with pytest.raises(ManifestRefused) as refused:
        _read(reader, go, code, tmp_path)
    assert (
        "two types are named Row, in example.com/rows and in example.com/rows/other; a named "
        "type's name is its own" in "; ".join(refused.value.problems)
    )


# -- the reader's build and environment ----------------------------------------------------------


def test_the_reader_is_compiled_once_per_toolchain(tmp_path):
    builds = []

    def build(argv, env, cwd):
        builds.append((argv, env))
        Path(argv[argv.index("-o") + 1]).write_text("binary")
        return ""

    go = tmp_path / "go" / "bin" / "go"
    first = ensure_reader(go, "go1.26.5", tmp_path / "cache", "https://proxy.example", build)
    again = ensure_reader(go, "go1.26.5", tmp_path / "cache", "https://proxy.example", build)
    other = ensure_reader(go, "go1.27.0", tmp_path / "cache", "https://proxy.example", build)
    assert first == again != other
    assert len(builds) == 2
    argv, env = builds[0]
    assert argv[:2] == [str(go), "build"]
    assert env["GOPROXY"] == "https://proxy.example"
    assert env["GOTOOLCHAIN"] == "local" and env["CGO_ENABLED"] == "0"
    assert reader_key("go1.26.5") != reader_key("go1.27.0")
    assert {p.name for p in READER.iterdir()} >= {"go.mod", "go.sum", "main.go"}


def test_a_go_command_reaches_no_network_and_inherits_nothing(tmp_path, monkeypatch):
    monkeypatch.setenv("GOFLAGS", "-mod=mod")
    monkeypatch.setenv("GOPROXY", "https://anywhere.example")
    env = go_environment(
        tmp_path / "go" / "bin" / "go", gocache=tmp_path / "cache", gomodcache=tmp_path / "mod"
    )
    assert env == {
        "PATH": str(tmp_path / "go" / "bin"),
        "HOME": str(tmp_path),
        "GOCACHE": str(tmp_path / "cache"),
        "GOMODCACHE": str(tmp_path / "mod"),
        "GOPATH": str(tmp_path / "gopath"),
        "GOTOOLCHAIN": "local",
        "GOFLAGS": "-mod=readonly",
        "GOPROXY": "off",
        "GOSUMDB": "off",
        "GOWORK": "off",
        "GOENV": "off",
        "CGO_ENABLED": "0",
    }


def _services_lines(go_mod: Path) -> list[str]:
    return [line.strip() for line in go_mod.read_text().splitlines() if SERVICES_MODULE in line]


def test_the_module_is_provided_whatever_the_codes_go_mod_says(tmp_path):
    """REQ-2106: a go.mod silent about provisa_services gets it; one that names it, for the
    author's own stub outside Provisa, gets Provisa's in place of the stub."""
    shipped = f"{SERVICES_MODULE} => {SERVICES}"
    for text in (
        "module example.com/rows\n\ngo 1.23\n",
        "module example.com/rows\n\ngo 1.23\n\nrequire provisa_services v0.0.0\n\n"
        "replace provisa_services => ./mock\n",
        "module example.com/rows\n\ngo 1.23\n\nrequire (\n\tprovisa_services v0.0.0\n)\n\n"
        "replace (\n\tprovisa_services v0.0.0 => ./mock\n)\n",
    ):
        code = tmp_path / "code"
        code.mkdir(exist_ok=True)
        (code / "go.mod").write_text(text)
        (code / "go.sum").write_text("example.com/x v1.0.0 h1:abc=\n")
        made = services_go_mod(code, tmp_path / "scratch")
        assert (code / "go.mod").read_text() == text  # the code's own is never changed
        assert made.with_suffix(".sum").read_text() == "example.com/x v1.0.0 h1:abc=\n"
        assert sorted(_services_lines(made)) == sorted(
            [f"replace {shipped}", f"require {SERVICES_MODULE} v0.0.0"]
        )
        assert "module example.com/rows" in made.read_text()


def test_the_module_needs_no_go_sum_and_any_other_requirement_still_does(tmp_path):
    """REQ-2106: requiring only provisa_services is not requiring something to check."""
    (tmp_path / "go.mod").write_text(
        "module example.com/rows\n\ngo 1.23\n\nrequire provisa_services v0.0.0\n"
    )
    check_lockfile(tmp_path)
    (tmp_path / "go.mod").write_text(
        "module example.com/rows\n\ngo 1.23\n\nrequire (\n\tprovisa_services v0.0.0\n"
        "\texample.com/other v1.2.3\n)\n"
    )
    with pytest.raises(BundleRefused, match="carries no go.sum"):
        check_lockfile(tmp_path)


def test_the_host_build_replaces_the_module_with_provisas_not_the_codes(tmp_path):
    code = tmp_path / "code"
    code.mkdir()
    (code / "go.mod").write_text(
        "module example.com/rows\n\ngo 1.23\n\nrequire provisa_services v0.0.0\n\n"
        "replace provisa_services => ./mock\n"
    )
    into = tmp_path / "into"
    into.mkdir()
    build_module(code, ".", ["Rows"], into)
    assert sorted(_services_lines(into / "go.mod")) == sorted(
        [f"{SERVICES_MODULE} => {SERVICES}", f"{SERVICES_MODULE} v0.0.0"]
    )


def test_code_importing_the_module_is_read(reader, go, tmp_path):
    """REQ-2106: its go.mod says nothing of provisa_services, and the reader type-checks it."""
    code = _code(
        tmp_path,
        """
        package rows

        import (
            "context"

            ps "provisa_services"
        )

        type Row struct {
            Key string `json:"key"`
        }

        // @table
        func Rows(ctx context.Context) ([]Row, error) {
            key, err := ps.Secrets.Get(ctx, "key")
            return []Row{{key}}, err
        }
        """,
    )
    assert [e.name for e in _read(reader, go, code, tmp_path).exports] == ["Rows"]


def test_a_failing_go_command_is_refused_with_its_error(tmp_path):
    def failing(argv, env, cwd):
        raise BundleRefused("go_failed", "go build failed: no such module")

    with pytest.raises(BundleRefused, match="no such module"):
        ensure_reader(tmp_path / "go", "go1.26.5", tmp_path / "cache", "off", failing)
    assert not list((tmp_path / "cache" / "code" / "go-reader").glob("*/reader"))


# -- the rules, on trees written by hand ---------------------------------------------------------


def _export(name: str, tags: list[str], returns: dict, parameters=()) -> dict:
    return {
        "name": name,
        "tags": tags,
        "tag_arguments": [],
        "description": "",
        "signatures": [
            {"parameters": list(parameters), "returns": returns, "context": True, "fails": True}
        ],
    }


def test_go_rules_take_an_iterator_yielding_errors_as_a_stream_and_no_standard_tags():
    row = {"kind": "ref", "name": "Row"}
    raw = {
        "exports": [
            _export(
                "Rows",
                ["stream"],
                {
                    "kind": "iterable",
                    "async": False,
                    "generator": True,
                    "errors": True,
                    "element": row,
                },
            )
        ],
        "types": {"Row": {"name": "Row", "fields": []}},
    }
    assert read_manifest(raw, GO_RULES).export("Rows").kind == "stream"
    raw["exports"].append(
        _export("Doc", ["deprecated", "table"], {"kind": "array", "element": row})
    )
    with pytest.raises(ManifestRefused, match="Doc: unknown tag @deprecated"):
        read_manifest(raw, GO_RULES)


def test_a_query_takes_a_table_and_a_command_is_a_mutation(reader, go, tmp_path):  # REQ-2226
    code = _code(
        tmp_path,
        """
        package rows

        type TableRef[R any] struct {
            Ref     string   `json:"ref"`
            Columns []string `json:"columns"`
        }

        type Order struct {
            ID     int64  `json:"id"`
            Region string `json:"region"`
        }

        type Score struct {
            Region string  `json:"region"`
            Score  float64 `json:"score"`
        }

        // @query
        func Scored(orders TableRef[Order]) ([]Score, error) { return nil, nil }

        // @command
        func Rescore(orders TableRef[Order]) ([]Score, error) { return nil, nil }
        """,
    )
    manifest = _read(reader, go, code, tmp_path)
    assert manifest.export("Scored").kind == "query"
    assert manifest.export("Rescore").kind == "command"
    assert manifest.export("Scored").parameters[0].shape == TableOf(Named("Order"))
