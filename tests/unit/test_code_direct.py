# Copyright (c) 2026 Kenneth Stott
# Canary: e1386bbf-4bb5-48e5-852b-8098412e8b79
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A read of a code table (REQ-2100, REQ-863): the function is called with the read's arguments
and the values its pushed predicates give, a pre-filter only; its rows are landed and the engine
runs the whole statement over them, the pushed predicates again among it. The residual the code
host applies for an engine that attaches it is the same statement over the same rows.

The worker is a real host process (the Python host, this interpreter, unsandboxed); the source's
preparation is stood in with the code's manifest, read by the real reader."""

# Requirements: REQ-2100
from __future__ import annotations

import json
import subprocess
import sys
import textwrap
from datetime import date
from decimal import Decimal
from types import SimpleNamespace

import pyarrow as pa
import pytest

from provisa.codehost import residual
from provisa.codesource import source as code_source
from provisa.codesource import worker as w
from provisa.codesource.manifest import PYTHON_RULES, read_manifest
from provisa.python.source import HOST, READER
from provisa.transpiler.router import Route, decide_route

pytestmark = pytest.mark.unit

PUSHDOWN = {
    "region": {"column": "region", "op": "eq"},
    "gte_score": {"column": "score", "op": "gte"},
    "since": {"column": "signup", "op": "gte"},
    "regions": {"column": "region", "op": "in"},
    "like_name": {"column": "name", "op": "like"},
}


def _pushed(where: str) -> dict:
    return residual.pushed(
        f"SELECT id FROM s.scores AS t WHERE {where}", "duckdb", "scores", PUSHDOWN
    )


# -- what a read pushes ----------------------------------------------------------------------------


def test_top_level_and_conjuncts_on_columns_fill_their_parameters():
    assert _pushed(
        "region = 'EU' AND t.score >= 0.5 AND signup >= DATE '2026-01-01' AND name LIKE 'A%'"
    ) == {
        "region": "EU",
        "gte_score": 0.5,
        "since": date(2026, 1, 1),
        "like_name": "A%",
    }


def test_a_comparison_written_literal_first_is_read_the_other_way():
    assert _pushed("0.5 <= score") == {"gte_score": 0.5}


def test_in_fills_a_list():
    assert _pushed("region IN ('EU', 'UK')") == {"regions": ["EU", "UK"]}


@pytest.mark.parametrize(
    "where",
    [
        "region = 'EU' OR score >= 0.5",
        "NOT (region = 'EU')",
        "upper(region) = 'EU'",
        "score >= id",
        "score > 0.5",  # gt is not what gte_score takes
    ],
)
def test_nothing_under_or_or_not_or_on_an_expression_is_pushed(where):
    assert _pushed(where) == {}


# -- the residual ----------------------------------------------------------------------------------


def _rows() -> pa.Table:
    return pa.table(
        {
            "id": pa.array([1, 2, 3], pa.int64()),
            "region": ["EU", "US", "EU"],
            "amount": pa.array(["10.50", "3.25", "7"], pa.utf8()),
        }
    ).cast(
        pa.schema(
            [
                pa.field("id", pa.int64()),
                pa.field("region", pa.utf8()),
                pa.field("amount", pa.utf8(), metadata={b"provisa.ir": b"numeric"}),
            ]
        )
    )


def test_the_statement_is_applied_to_the_rows_with_exact_numerics():
    answer = residual.answer(
        'SELECT t.id, t.amount FROM "s"."scores" AS t WHERE t.amount > 5 ORDER BY t.amount DESC '
        "LIMIT 1",
        "duckdb",
        "scores",
        _rows(),
    )
    assert answer.to_pylist() == [{"id": 1, "amount": Decimal("10.50")}]


def test_an_aggregate_and_a_schema_qualified_column_are_the_rows():
    answer = residual.answer(
        "SELECT s.scores.region, count(*) AS n FROM s.scores GROUP BY s.scores.region "
        "ORDER BY region",
        "duckdb",
        "scores",
        _rows(),
    )
    assert answer.to_pylist() == [{"region": "EU", "n": 2}, {"region": "US", "n": 1}]


def test_a_parameters_column_holds_its_value_on_every_row():
    arguments = pa.table({"as_of": pa.array(["2026"]), "x": pa.array([None], pa.utf8())})
    rows = residual.with_arguments(_rows(), arguments)
    answer = residual.answer(
        "SELECT id FROM scores WHERE _nf_as_of = '2026' AND _nf_x IS NULL ORDER BY id",
        "duckdb",
        "scores",
        rows,
    )
    assert answer.column("id").to_pylist() == [1, 2, 3]


# -- a direct read ---------------------------------------------------------------------------------

CODE = """
from dataclasses import dataclass
from decimal import Decimal


@dataclass
class Score:
    id: int
    region: str
    score: float
    amount: Decimal
    asked: str


def scores(
    region: str | None = None, gte_score: float | None = None, as_of: str | None = None
) -> list[Score]:
    \"\"\"@table\"\"\"
    asked = f"region={region};gte_score={gte_score};as_of={as_of}"
    # The values pushed are ignored: the residual still filters.
    return [
        Score(1, "EU", 0.9, Decimal("10.50"), asked),
        Score(2, "US", 0.4, Decimal("3.25"), asked),
        Score(3, "EU", 0.6, Decimal("7.00"), asked),
    ]
"""


def _column(name, data_type, *, kind=None):
    return {
        "column_name": name,
        "name": name,
        "data_type": data_type,
        "path": None,
        "native_filter_type": kind,
        "native_filter_required": False if kind else None,
    }


TABLE = {
    "id": 9,
    "table_name": "scores",
    "source_id": "scores-src",
    "schema_name": "main",
    "query_template": None,
    "columns": [
        _column("id", "bigint"),
        _column("region", "text"),
        _column("score", "double"),
        _column("amount", "numeric"),
        _column("asked", "text"),
        _column("_nf_region", "text", kind="query_arg"),
        _column("_nf_gte_score", "double", kind="query_arg"),
        _column("_nf_as_of", "text", kind="query_arg"),
    ],
}


@pytest.fixture
def prepared(tmp_path, monkeypatch):
    code = tmp_path / "code"
    code.mkdir()
    (code / "scores.py").write_text(textwrap.dedent(CODE))
    done = subprocess.run(
        [sys.executable, "-I", str(READER), str(code), "scores"], capture_output=True, text=True
    )
    assert done.returncode == 0, done.stderr
    manifest = read_manifest(json.loads(done.stdout), PYTHON_RULES)
    command = w.HostCommand(
        argv=lambda address: [sys.executable, "-I", str(HOST), address, str(code), "scores"],
        env=w.private_environment(PYTHONDONTWRITEBYTECODE="1"),
        cwd=code,
    )
    ready = code_source.Prepared("scores-src", "h1", manifest, command, "acme")
    source = SimpleNamespace(id="scores-src", type=SimpleNamespace(value="python"), mapping={})

    async def _registered(_state):
        return [source]

    async def _prepare(*_args):
        return ready

    async def _context(_state):
        return None

    monkeypatch.setattr("provisa.federation.registry_view.registered_sources", _registered)
    monkeypatch.setattr(code_source, "prepare", _prepare)
    monkeypatch.setattr(code_source, "context_for", _context)
    yield ready
    w.pool.stop_source("scores-src")


def _engine_answer(landed, sql: str, name: str = "scores") -> pa.Table:
    """``sql`` run over the rows ``landed`` (a PipelineRead) as the engine runs a statement over a
    landed table."""
    import duckdb

    with duckdb.connect() as con:
        con.register(name, pa.Table.from_pylist(landed.rows))
        return con.execute(sql).to_arrow_table()


async def test_a_read_lands_what_its_pushes_pre_filter_and_the_engine_answers_it(prepared):
    # REQ-2100 (amended 2026-10-23): the function is given what the statement lets it pre-filter,
    # its rows are landed, and the engine runs the whole statement over them.
    from provisa.codesource.table import pushed_arguments, read

    statement = (
        'SELECT "id", "amount", "asked" FROM "main"."scores" WHERE "region" = \'EU\' '
        'AND "score" >= 0.5 ORDER BY "amount" DESC'
    )
    given = {**await pushed_arguments(None, TABLE, "scores", statement), "as_of": "2026-10"}
    landed = await read(None, TABLE, given)
    answer = _engine_answer(
        landed,
        "SELECT id, amount, asked FROM scores WHERE region = 'EU' AND score >= 0.5 "
        "ORDER BY amount DESC",
    )
    assert answer.column("id").to_pylist() == [1, 3]
    assert answer.column("amount").to_pylist() == [Decimal("10.50"), Decimal("7.00")]
    assert set(answer.column("asked").to_pylist()) == {"region=EU;gte_score=0.5;as_of=2026-10"}


async def test_an_argument_the_read_names_itself_is_the_one_the_function_is_given(prepared):
    from provisa.codesource.table import pushed_arguments, read

    statement = "SELECT asked FROM main.scores WHERE region = 'EU' LIMIT 1"
    given = {**await pushed_arguments(None, TABLE, "scores", statement), "region": "US"}
    landed = await read(None, TABLE, given)
    assert {r["asked"] for r in landed.rows} == {"region=US;gte_score=None;as_of=None"}


# -- routing and landing ---------------------------------------------------------------------------


def _route(sources, types, floor=None):
    return decide_route(
        sources=set(sources),
        source_types=types,
        source_dialects={s: "duckdb" for s in sources},
        operator_floor=floor or {},
        statement="SELECT 1",
    )


def test_a_read_of_a_code_source_is_landed_for_the_engine():
    # REQ-863: the code function is the source's access path, and its rows are landed.
    for sources, types in (({"scores-src"}, {"scores-src": "python"}), ({"js"}, {"js": "node"})):
        decision = _route(sources, types)
        assert decision.route == Route.ENGINE
        assert decision.reason.startswith("landed: trusted_to_answer: incomplete dataset from ")
    across = _route({"scores-src", "pg"}, {"scores-src": "python", "pg": "postgresql"})
    assert across.route == Route.ENGINE


LOOSE_CODE = """
def currencies() -> list[dict]:
    \"\"\"@table\"\"\"
    return [{"code": c, "rate": r} for c, r in (("EUR", 0.9), ("GBP", 0.8), ("USD", 1.0))]
"""


async def test_a_table_typed_by_a_preview_is_read_into_its_registered_columns(
    tmp_path, monkeypatch
):
    code = tmp_path / "code"
    code.mkdir()
    (code / "fx.py").write_text(LOOSE_CODE)
    done = subprocess.run(
        [sys.executable, "-I", str(READER), str(code), "fx"], capture_output=True, text=True
    )
    assert done.returncode == 0, done.stderr
    manifest = read_manifest(json.loads(done.stdout), PYTHON_RULES)
    command = w.HostCommand(
        argv=lambda address: [sys.executable, "-I", str(HOST), address, str(code), "fx"],
        env=w.private_environment(PYTHONDONTWRITEBYTECODE="1"),
        cwd=code,
    )
    ready = code_source.Prepared("fx-src", "h1", manifest, command, "acme")
    source = SimpleNamespace(id="fx-src", type=SimpleNamespace(value="python"), mapping={})

    async def _registered(_state):
        return [source]

    async def _prepare(*_args):
        return ready

    async def _context(_state):
        return None

    monkeypatch.setattr("provisa.federation.registry_view.registered_sources", _registered)
    monkeypatch.setattr(code_source, "prepare", _prepare)
    monkeypatch.setattr(code_source, "context_for", _context)
    table = {
        "table_name": "currencies",
        "source_id": "fx-src",
        "columns": [_column("code", "text"), _column("rate", "double")],
    }
    from provisa.codesource.table import read

    try:
        landed = await read(None, table, None)
    finally:
        w.pool.stop_source("fx-src")
    answer = _engine_answer(
        landed, "SELECT code FROM currencies WHERE rate < 1 ORDER BY code", "currencies"
    )
    assert answer.column("code").to_pylist() == ["EUR", "GBP"]
