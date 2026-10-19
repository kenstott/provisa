# Copyright (c) 2026 Kenneth Stott
# Canary: facf14eb-20af-4d8a-93b2-ea02da47b57c
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A Python source (REQ-1993): its functions read from their docstrings and hints, judged by the
shared rules, and served by its host over every code source's protocol. The reader and the host
run here under this interpreter, unsandboxed; the sandboxed runs are the integration tests'."""

# Requirements: REQ-1993
from __future__ import annotations

import json
import shutil
import subprocess
import sys
import textwrap
import time
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path

import pytest

from provisa.codesource import worker as w
from provisa.codesource.arrow_rows import row_schema, schema_text
from provisa.codesource.bundle import BundleRefused
from provisa.codesource.manifest import (
    PYTHON_RULES,
    EnumOf,
    Field,
    ListOf,
    ManifestRefused,
    Named,
    Scalar,
    read_manifest,
)
from provisa.core import request_deadline
from provisa.python.source import HOST, READER, entry_module

pytestmark = pytest.mark.unit

FIXTURE = Path(__file__).parent.parent / "fixtures" / "python_source"


def _read(code: Path, entry: str) -> dict:
    done = subprocess.run(
        [sys.executable, "-I", str(READER), str(code), entry], capture_output=True, text=True
    )
    assert done.returncode == 0, done.stderr
    return json.loads(done.stdout)


def _code(tmp_path: Path, source: str) -> Path:
    (tmp_path / "rows.py").write_text(textwrap.dedent(source))
    return tmp_path


# -- reading ------------------------------------------------------------------------------------


def test_the_codes_tables_commands_and_streams_are_read_from_docstrings_and_hints():
    manifest = read_manifest(_read(FIXTURE, "orders"), PYTHON_RULES)
    assert {(e.name, e.kind) for e in manifest.exports} == {
        ("orders", "table"),
        ("many", "table"),
        ("placed", "stream"),
        ("ticks", "stream"),
        ("forever", "stream"),
        ("cancel_order", "command"),
        ("boom", "table"),
        ("outside", "table"),
        ("fetched", "table"),
        ("direct", "table"),
        ("greet", "table"),
    }
    orders = manifest.export("orders")
    assert orders.description == "Orders of one customer."
    assert [(p.name, p.shape, p.nullable, p.required) for p in orders.parameters] == [
        ("customer_id", Scalar("text"), False, True),
        ("since", Scalar("timestamp"), True, False),
    ]
    assert orders.returns == Named("Order")
    assert manifest.types["Order"].fields == (
        Field("id", Scalar("bigint"), False),
        Field("placed", Scalar("timestamp"), False),
        Field("due", Scalar("date"), False),
        Field("total", Scalar("numeric"), False),
        Field("tier", EnumOf("Tier", ("gold", "silver")), False),
        Field("lines", ListOf(Named("Line")), False),
        Field("note", Scalar("text"), True),
    )
    assert manifest.types["Line"].fields == (
        Field("sku", Scalar("text"), False),
        Field("qty", Scalar("bigint"), False),
    )
    assert manifest.export("many").preview  # dict rows: typed by a preview
    assert manifest.export("cancel_order").many is False


def test_a_typed_dict_and_a_named_tuple_are_row_types(tmp_path):
    code = _code(
        tmp_path,
        """
        from typing import NamedTuple, NotRequired, TypedDict

        class Rate(TypedDict):
            pair: str
            mid: float
            note: NotRequired[str]

        class Point(NamedTuple):
            x: int
            y: int

        def rates() -> list[Rate]:
            \"\"\"@table\"\"\"
            return []

        def points() -> list[Point]:
            \"\"\"@table\"\"\"
            return []
        """,
    )
    manifest = read_manifest(_read(code, "rows"), PYTHON_RULES)
    assert manifest.types["Rate"].fields == (
        Field("pair", Scalar("text"), False),
        Field("mid", Scalar("double"), False),
        Field("note", Scalar("text"), True),
    )
    assert manifest.types["Point"].fields == (
        Field("x", Scalar("bigint"), False),
        Field("y", Scalar("bigint"), False),
    )


def test_code_that_breaks_the_rules_is_refused_with_every_problem(tmp_path):
    code = _code(
        tmp_path,
        """
        from dataclasses import dataclass

        @dataclass
        class Order:
            id: int

        def typo() -> list[Order]:
            \"\"\"@tabel\"\"\"

        def one() -> Order:
            \"\"\"@table\"\"\"

        def spread(*ids: int) -> list[Order]:
            \"\"\"@table\"\"\"

        def untyped(region) -> list[Order]:
            \"\"\"@table\"\"\"

        def listing() -> list[Order]:
            \"\"\"@stream\"\"\"
        """,
    )
    with pytest.raises(ManifestRefused) as refused:
        read_manifest(_read(code, "rows"), PYTHON_RULES)
    assert refused.value.problems == [
        "typo: unknown tag @tabel",
        "one: @table returns a single value, not a collection of rows",
        "spread: the rest parameter ids cannot be a parameter",
        "untyped(region): the type no type hint cannot be mapped",
        "listing: @stream is a generator or an async generator",
    ]


def test_a_tag_is_a_line_of_its_own_and_prose_that_names_one_is_not(tmp_path):
    code = _code(
        tmp_path,
        """
        def rows() -> list[dict]:
            \"\"\"Unlike @table in prose, this is a command.

            @command
            \"\"\"
            return []
        """,
    )
    manifest = read_manifest(_read(code, "rows"), PYTHON_RULES)
    assert [(e.name, e.kind) for e in manifest.exports] == [("rows", "command")]
    assert manifest.export("rows").description == "Unlike @table in prose, this is a command."


def test_code_that_cannot_be_imported_is_refused_with_its_error(tmp_path):
    code = _code(tmp_path, "import no_such_dependency\n")
    with pytest.raises(ManifestRefused) as refused:
        read_manifest(_read(code, "rows"), PYTHON_RULES)
    assert refused.value.problems == [
        "the declarations do not type-check: importing rows failed: ModuleNotFoundError: "
        "No module named 'no_such_dependency'"
    ]


def test_functions_imported_from_elsewhere_are_not_the_codes(tmp_path):
    code = _code(
        tmp_path,
        """
        from os.path import join
        from json import dumps
        """,
    )
    assert _read(code, "rows")["exports"] == []


# -- the entry module ---------------------------------------------------------------------------


def test_the_entry_is_the_codes_single_top_level_module_or_package(tmp_path):
    assert entry_module(FIXTURE) == "orders"
    (tmp_path / "rates.py").write_text("")
    (tmp_path / "setup.py").write_text("")
    (tmp_path / "test_rates.py").write_text("")
    assert entry_module(tmp_path) == "rates"


def test_more_than_one_module_is_named_by_the_source_or_refused(tmp_path):
    (tmp_path / "rates.py").write_text("")
    pkg = tmp_path / "orders"
    pkg.mkdir()
    (pkg / "__init__.py").write_text("")
    with pytest.raises(BundleRefused) as refused:
        entry_module(tmp_path)
    assert refused.value.code == "codesource.entry_ambiguous"
    assert "2 top-level modules (orders, rates)" in str(refused.value)
    assert entry_module(tmp_path, "orders") == "orders"
    with pytest.raises(BundleRefused, match="has no module billing"):
        entry_module(tmp_path, "billing")


# -- the host -----------------------------------------------------------------------------------


def _schema(export: str) -> str:
    """The Arrow schema of ``export``'s rows, as a call gives it (REQ-2098)."""
    manifest = read_manifest(_read(FIXTURE, "orders"), PYTHON_RULES)
    return schema_text(row_schema(manifest, manifest.export(export)))


@pytest.fixture
def worker(tmp_path):
    code = tmp_path / "code"
    shutil.copytree(FIXTURE, code)
    command = w.HostCommand(
        argv=lambda address: [sys.executable, "-I", str(HOST), address, str(code), "orders"],
        env=w.private_environment(PYTHONDONTWRITEBYTECODE="1"),
        cwd=code,
    )
    started = w.Worker("python-src", "h", command)
    started.start()
    yield started
    started.stop()


def test_the_host_says_which_functions_it_has_and_not_what_it_imported(worker):
    assert set(worker.exports) == {
        "orders", "many", "placed", "ticks", "forever", "cancel_order", "boom", "outside",
        "fetched", "direct", "greet", "helper",
    }  # fmt: skip


def test_rows_and_their_values_come_back_in_their_own_types(worker):
    since = datetime(2025, 12, 1, tzinfo=UTC)
    [[row]] = list(worker.rows("orders", "table", ["acme", since], schema=_schema("orders")))
    assert row == {
        "id": 2**60,
        "placed": datetime(2026, 1, 2, 3, 4, 5, tzinfo=UTC),
        "due": date(2026, 1, 9),
        "total": Decimal("12.50"),
        "tier": "gold",
        "lines": [{"sku": "acme", "qty": 2}],
        "note": since.isoformat(),
    }


def test_a_large_table_comes_in_batches(worker):
    assert [len(b) for b in worker.rows("many", "table", [2500], schema=_schema("many"))] == [
        1000,
        1000,
        500,
    ]


def test_a_commands_value_and_the_codes_own_error(worker):
    assert worker.command("cancel_order", [7]) == {"cancelled": 7}
    with pytest.raises(w.CodeFailed, match="LookupError: no such customer"):
        list(worker.rows("boom", "table", [], schema=_schema("boom")))
    assert worker.ping()


@pytest.mark.parametrize("export", ["ticks", "placed"])  # a generator, an async generator
def test_a_stream_its_subscriber_leaves_is_stopped_and_the_worker_serves_on(worker, export):
    rows = worker.rows(
        export, "stream", ["eu"] if export == "placed" else [], schema=_schema(export)
    )
    first, second = next(rows), next(rows)
    assert len(first) == len(second) == 1
    rows.close()
    assert worker.ping()


def test_a_deadline_stops_a_call_that_never_answers(worker):
    started = time.monotonic()
    with pytest.raises(TimeoutError):
        with request_deadline.within(0.3):
            list(worker.rows("forever", "stream", [], schema=_schema("forever")))
    assert time.monotonic() - started < 10
    assert worker.ping()


def test_an_argument_left_out_takes_the_functions_own_default(worker):
    """An optional parameter the read gives no value for is absent, never None: the function's
    default applies, even for one after a later given argument."""
    rows = worker.rows(
        "greet",
        "table",
        ["ada", w.ABSENT, "?"],
        ["name", "greeting", "punctuation"],
        schema=_schema("greet"),
    )
    assert list(rows) == [[{"text": "hello ada?"}]]
    rows = worker.rows(
        "greet",
        "table",
        ["ada", "hi", w.ABSENT],
        ["name", "greeting", "punctuation"],
        schema=_schema("greet"),
    )
    assert list(rows) == [[{"text": "hi ada!"}]]
