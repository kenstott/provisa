# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The Go reader's SDK registrations (REQ-1984), on the pinned Go: a registration is found by the
SDK's import path whatever the import is called, in any file of the package, and adds its tags to
the function it names; a registration naming anything but an exported top-level function, an
option that is not a constant, an SDK call outside a registration and a function of a newer SDK
are each refused by name; and go build rejects a function whose shape is not its tag's."""

# Requirements: REQ-1984
from __future__ import annotations

import shutil
from pathlib import Path

import pytest

from tests.fixtures.code_sdk_spellings.go_module import SDK, GoToolchain, write_module

pytestmark = [pytest.mark.integration]

PRELUDE = """package source

import (
\t"context"
\t"iter"

\t{imp}
)

var _ context.Context
var _ iter.Seq[int]

type Row struct {{
\tID int64 `json:"id"`
}}

type Inputs struct {{
\tRegion *string `json:"region"`
}}
"""


def source(body: str, imp: str = 'provisa "provisa.dev/sdk/go"') -> str:
    return PRELUDE.format(imp=imp) + body


@pytest.fixture(scope="module")
def toolchain(tmp_path_factory) -> GoToolchain:
    return GoToolchain.resolve(tmp_path_factory.mktemp("go-cache"))


def _read(toolchain: GoToolchain, directory: Path, files: dict[str, str], sdk: Path = SDK):
    module = write_module(directory, files, sdk)
    built = toolchain.build(module)
    assert built.returncode == 0, built.stderr
    return toolchain.read(module)


def _export(manifest: dict, name: str) -> dict:
    (found,) = [e for e in manifest["exports"] if e["name"] == name]
    return found


def test_a_renamed_import_registers_by_the_sdks_path(toolchain, tmp_path):
    body = "func Rows() []Row { return nil }\n\nvar _ = p.Table[p.NoInputs, Row](Rows)\n"
    got = _read(toolchain, tmp_path / "m", {"source.go": source(body, 'p "provisa.dev/sdk/go"')})
    assert isinstance(got, dict), got
    assert _export(got, "Rows")["kind"] == "table"


def test_a_dot_import_registers_too(toolchain, tmp_path):
    body = "func Rows() []Row { return nil }\n\nvar _ = Table[NoInputs, Row](Rows)\n"
    got = _read(toolchain, tmp_path / "m", {"source.go": source(body, '. "provisa.dev/sdk/go"')})
    assert isinstance(got, dict), got
    assert _export(got, "Rows")["kind"] == "table"


def test_a_registration_in_another_file_of_the_package_is_read(toolchain, tmp_path):
    files = {
        "source.go": source("func Rows() []Row { return nil }\n\nvar _ = provisa.Spelling\n"),
        "register.go": 'package source\n\nimport provisa "provisa.dev/sdk/go"\n\n'
        'var _ = provisa.Table[provisa.NoInputs, Row](Rows, provisa.Requires("API_KEY", '
        "provisa.Secret))\n",
    }
    got = _read(toolchain, tmp_path / "m", files)
    assert isinstance(got, dict), got
    assert _export(got, "Rows")["kind"] == "table"
    assert got["requires"] == [{"name": "API_KEY", "secret": True}]


def test_a_registration_and_a_doc_tag_on_one_function_merge(toolchain, tmp_path):
    body = (
        "// Rows is every row.\n//\n// @table\nfunc Rows() []Row { return nil }\n\n"
        "var _ = provisa.Table[provisa.NoInputs, Row](Rows)\n"
    )
    got = _read(toolchain, tmp_path / "m", {"source.go": source(body)})
    assert isinstance(got, dict), got
    assert [e["name"] for e in got["exports"]] == ["Rows"]


@pytest.mark.parametrize(
    ("body", "problem"),
    [
        (
            "var rows = func() []Row { return nil }\n\n"
            "var _ = provisa.Table[provisa.NoInputs, Row](rows)\n",
            "provisa.Table(rows): a registration names a top-level function of this package, "
            "not rows",
        ),
        (
            "var _ = provisa.Table[provisa.NoInputs, Row](func() []Row { return nil })\n",
            "provisa.Table(func() []Row {...}): a registration names a top-level function of "
            "this package, not func() []Row {...}",
        ),
        (
            "type Source struct{}\n\nfunc (Source) Rows() []Row { return nil }\n\n"
            "var _ = provisa.Table[provisa.NoInputs, Row](Source{}.Rows)\n",
            "provisa.Table(Source{}.Rows): a registration names a top-level function of this "
            "package, not Source{}.Rows",
        ),
        (
            "func rows() []Row { return nil }\n\n"
            "var _ = provisa.Table[provisa.NoInputs, Row](rows)\n",
            "provisa.Table(rows): rows is not exported, and Provisa serves an exported function",
        ),
        (
            'var name = "API_KEY"\n\nfunc Rows() []Row { return nil }\n\n'
            "var _ = provisa.Table[provisa.NoInputs, Row](Rows, provisa.Requires(name))\n",
            "Rows: provisa.Requires takes a constant, not name",
        ),
        (
            "var secret = provisa.Secret\n\nfunc Rows() []Row { return nil }\n\n"
            'var _ = provisa.Table[provisa.NoInputs, Row](Rows, provisa.Requires("K", secret))\n',
            "Rows: provisa.Requires(K, ...) takes provisa.Secret, a constant, not secret",
        ),
        (
            'var opts = []provisa.Option{provisa.Requires("K")}\n\nfunc Rows() []Row '
            "{ return nil }\n\nvar _ = provisa.Table[provisa.NoInputs, Row](Rows, opts...)\n",
            "Rows: provisa.Table's options are written out, each a call, not spread from a slice",
        ),
        (
            'var option = provisa.Requires("K")\n\nfunc Rows() []Row { return nil }\n\n'
            "var _ = provisa.Table[provisa.NoInputs, Row](Rows, option)\n",
            "Rows: an option of provisa.Table is a call such as provisa.Requires(...), not option",
        ),
        (
            'var table = "pairs"\n\nfunc Changes(since *int64) []Row { return nil }\n\n'
            'var _ = provisa.Change(Changes, table, []string{"id"}, "removed", "seq")\n',
            "Changes: provisa.Change's table is a constant, not table",
        ),
        (
            "func Rows() []Row { return nil }\n\n"
            "func init() { _ = provisa.Table[provisa.NoInputs, Row](Rows) }\n",
            "source.go:22: provisa.Table is called outside a package-level registration "
            "(var _ = provisa.Table(...)), where Provisa does not read it",
        ),
    ],
    ids=[
        "variable",
        "closure",
        "method-value",
        "unexported",
        "non-constant-option",
        "non-constant-secret",
        "spread-options",
        "option-not-a-call",
        "non-constant-change-part",
        "outside-a-registration",
    ],
)
def test_a_registration_provisa_cannot_read_is_refused_by_name(toolchain, tmp_path, body, problem):
    got = _read(toolchain, tmp_path / "m", {"source.go": source(body)})
    assert isinstance(got, tuple), got
    assert f"the declarations do not type-check: {problem}" in got


def test_a_function_of_a_newer_sdk_is_refused_naming_it(toolchain, tmp_path):
    newer = tmp_path / "sdk"
    shutil.copytree(SDK, newer)
    with (newer / "provisa.go").open("a") as written:
        written.write("\n// Catalog is a registration of a later spelling.\n")
        written.write("func Catalog[F any](fn F) Decl { return Decl{} }\n")
    body = "func Rows() []Row { return nil }\n\nvar _ = provisa.Catalog(Rows)\n"
    got = _read(toolchain, tmp_path / "m", {"source.go": source(body)}, sdk=newer)
    assert got == (
        "the declarations do not type-check: provisa.dev/sdk/go.Catalog is not a registration "
        "this server reads; it reads SDK spelling 1",
    )


@pytest.mark.parametrize(
    ("body", "rejection"),
    [
        (
            "func Rows(a, b Inputs) []Row { return nil }\n\n"
            "var _ = provisa.Table[Inputs, Row](Rows)\n",
            "does not satisfy provisa.ReadFunc[Inputs, Row]",
        ),
        (
            "func Rows(in Inputs, ctx context.Context) []Row { return nil }\n\n"
            "var _ = provisa.Table[Inputs, Row](Rows)\n",
            "does not satisfy provisa.ReadFunc[Inputs, Row]",
        ),
        (
            "func Rows() Row { return Row{} }\n\nvar _ = provisa.Table[provisa.NoInputs, Row](Rows)\n",
            "does not satisfy provisa.ReadFunc[provisa.NoInputs, Row]",
        ),
        (
            "func Ticks(in Inputs) []Row { return nil }\n\nvar _ = provisa.Stream[Inputs, Row](Ticks)\n",
            "does not satisfy provisa.StreamFunc[Inputs, Row]",
        ),
        (
            "func Ticks(in Inputs) iter.Seq[Row] { return nil }\n\n"
            "var _ = provisa.Stream[Inputs, Row](Ticks)\n",
            "does not satisfy provisa.StreamFunc[Inputs, Row]",
        ),
        (
            "func Status() int { return 0 }\n\nvar _ = provisa.Health(Status)\n",
            "does not satisfy provisa.HealthFunc",
        ),
        (
            "func Stop(reason string) {}\n\nvar _ = provisa.OnStop(Stop)\n",
            "does not satisfy provisa.StopFunc",
        ),
        (
            "func Start(a, b string) {}\n\nvar _ = provisa.OnStart(Start)\n",
            "does not satisfy provisa.StartFunc",
        ),
        (
            "func Rows() []Row { return nil }\n\n"
            'var _ = provisa.Table[provisa.NoInputs, Row](Rows, provisa.Insert("rows"))\n',
            "does not implement provisa.Option",
        ),
        (
            "var _ = provisa.Table[provisa.NoInputs, Row](42)\n",
            "does not satisfy provisa.ReadFunc[provisa.NoInputs, Row]",
        ),
    ],
    ids=[
        "two-inputs",
        "context-last",
        "one-row",
        "stream-of-a-slice",
        "stream-without-errors",
        "health-not-text",
        "on-stop-with-a-parameter",
        "on-start-two-parameters",
        "write-option-on-a-table",
        "not-a-function",
    ],
)
def test_go_build_rejects_a_shape_its_tag_does_not_allow(toolchain, tmp_path, body, rejection):
    module = write_module(tmp_path / "m", {"source.go": source(body)})
    built = toolchain.build(module)
    assert built.returncode != 0
    assert rejection in built.stderr, built.stderr
