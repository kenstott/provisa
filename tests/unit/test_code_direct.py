# Copyright (c) 2026 Kenneth Stott
# Canary: e1386bbf-4bb5-48e5-852b-8098412e8b79
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A code source's own statement (REQ-2100, REQ-863): a statement over one code source's tables is
read DIRECT, each table's function called with the arguments the statement gives it, and DuckDB
applies the whole statement to their rows; nothing is landed. A statement routed to the engine
after all has its tables landed then, their arguments a pre-filter. The residual a code host
applies for an engine that attaches it is the same statement over the same rows.

The worker is a real host process (the Python host, this interpreter, unsandboxed); the source's
preparation is stood in with the code's manifest, read by the real reader."""

# Requirements: REQ-2100
from __future__ import annotations

import json
import subprocess
import sys
import textwrap
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


@pytest.fixture(autouse=True)
def _bounded():
    """The process's DuckDB under the operator's bounds, spilling where dev mode spills."""
    from provisa.codesource.worker import duckdb_bounds
    from provisa.core.org_files import data_dir

    residual.configure(**duckdb_bounds(), spill_dir=str(data_dir() / "code-spill"))


COLUMNS = ["id", "region", "score", "signup", "name"]


def _context(where: str, tail: str = "", select: str = "id") -> dict:
    return residual.read_context(
        f"SELECT {select} FROM s.scores AS t WHERE {where} {tail}", "duckdb", "scores", COLUMNS
    )


# -- what a read asks of the rows: its read context --------------------------------------------


def test_top_level_and_conjuncts_on_columns_are_its_filters():
    assert _context(
        "region = 'EU' AND t.score >= 0.5 AND signup >= DATE '2026-01-01' AND name LIKE 'A%'"
    )["filters"] == [
        {"column": "region", "op": "eq", "value": "EU"},
        {"column": "score", "op": "gte", "value": 0.5},
        {"column": "signup", "op": "gte", "value": {"$day": "2026-01-01"}},
        {"column": "name", "op": "like", "value": "A%"},
    ]


def test_a_comparison_written_literal_first_is_read_the_other_way():
    assert _context("0.5 <= score")["filters"] == [{"column": "score", "op": "gte", "value": 0.5}]


def test_in_is_a_list():
    assert _context("region IN ('EU', 'UK')")["filters"] == [
        {"column": "region", "op": "in", "value": ["EU", "UK"]}
    ]


@pytest.mark.parametrize(
    "where",
    ["region = 'EU' OR score >= 0.5", "NOT (region = 'EU')", "upper(region) = 'EU'", "score >= id"],
)
def test_nothing_under_or_or_not_or_on_an_expression_is_a_filter_and_no_limit_is_given(where):
    context = _context(where, "ORDER BY id LIMIT 3")
    assert context["filters"] == []
    # A limit after a condition the function is not given could cut rows the statement keeps.
    assert context["limit"] is None


def test_the_limit_is_given_with_the_offset_folded_in_when_it_limits_the_rows_alone():
    context = _context(
        "region = 'EU' AND _nf_as_of = '2026'", "ORDER BY score DESC LIMIT 3 OFFSET 2"
    )
    assert context["limit"] == 5
    assert context["order_by"] == [{"column": "score", "descending": True, "nulls_first": False}]


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT region, count(*) FROM s.scores WHERE id > 1 GROUP BY region LIMIT 2",
        "SELECT DISTINCT region FROM s.scores LIMIT 2",
        "SELECT t.id FROM s.scores t JOIN s.regions r ON r.region = t.region ORDER BY t.id LIMIT 2",
        "SELECT id FROM s.scores ORDER BY lower(name) LIMIT 2",
    ],
)
def test_a_limit_on_anything_but_the_tables_own_rows_is_not_given(sql):
    assert residual.read_context(sql, "duckdb", "scores", COLUMNS)["limit"] is None


def test_its_columns_are_the_ones_the_statement_names_every_one_for_a_star():
    assert _context("region = 'EU'", select="id, score")["columns"] == ["id", "region", "score"]
    assert _context("region = 'EU'", select="*")["columns"] == COLUMNS


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
    ).read_all()
    assert answer.to_pylist() == [{"id": 1, "amount": Decimal("10.50")}]


def test_an_aggregate_and_a_schema_qualified_column_are_the_rows():
    answer = residual.answer(
        "SELECT s.scores.region, count(*) AS n FROM s.scores GROUP BY s.scores.region "
        "ORDER BY region",
        "duckdb",
        "scores",
        _rows(),
    ).read_all()
    assert answer.to_pylist() == [{"region": "EU", "n": 2}, {"region": "US", "n": 1}]


def test_a_parameters_column_holds_its_value_on_every_row():
    arguments = pa.table({"as_of": pa.array(["2026"]), "x": pa.array([None], pa.utf8())})
    rows = residual.with_arguments(_rows(), arguments)
    answer = residual.answer(
        "SELECT id FROM scores WHERE _nf_as_of = '2026' AND _nf_x IS NULL ORDER BY id",
        "duckdb",
        "scores",
        rows,
    ).read_all()
    assert answer.column("id").to_pylist() == [1, 2, 3]


# -- a direct read ---------------------------------------------------------------------------------

CODE = """
import operator
from dataclasses import dataclass
from decimal import Decimal

from provisa_services import ReadContext

ROWS = [(1, "EU", 0.9, "10.50"), (2, "US", 0.4, "3.25"), (3, "EU", 0.6, "7.00")]
OPS = {"eq": operator.eq, "ne": operator.ne, "gt": operator.gt, "gte": operator.ge,
       "lt": operator.lt, "lte": operator.le, "in": lambda a, b: a in b}


@dataclass
class Score:
    id: int
    region: str
    score: float
    amount: Decimal
    asked: str


@dataclass
class ScoreInputs:
    as_of: str | None = None
    #: ignore: the context is ignored; apply: every filter, the order, then the limit; partial:
    #: the first filter only, and the limit ignored.
    mode: str | None = None


def scores(inputs: ScoreInputs, context: ReadContext) -> list[Score]:
    \"\"\"@table\"\"\"
    filters = ",".join(f"{f.column} {f.op} {f.value}" for f in context.filters)
    order = ",".join(o.column + (" desc" if o.descending else "") for o in context.order_by)
    asked = f"as_of={inputs.as_of};filters={filters};order={order};limit={context.limit}"
    rows = [Score(i, r, s, Decimal(a), asked) for i, r, s, a in ROWS]
    if inputs.mode in ("apply", "partial"):
        applied = context.filters if inputs.mode == "apply" else context.filters[:1]
        for f in applied:
            rows = [r for r in rows if OPS[f.op](getattr(r, f.column), f.value)]
    if inputs.mode == "apply":
        for o in reversed(context.order_by):
            rows.sort(key=lambda r: getattr(r, o.column), reverse=o.descending)
        if context.limit is not None:
            rows = rows[: context.limit]
    return rows


@dataclass
class Region:
    region: str
    label: str


def regions() -> list[Region]:
    \"\"\"@table\"\"\"
    return [Region("EU", "Europe"), Region("US", "United States")]
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
        _column("_nf_as_of", "text", kind="query_arg"),
        _column("_nf_mode", "text", kind="query_arg"),
    ],
}


REGIONS = {
    "id": 10,
    "table_name": "regions",
    "source_id": "scores-src",
    "schema_name": "main",
    "query_template": None,
    "columns": [_column("region", "text"), _column("label", "text")],
}


def _state(tables=(TABLE, REGIONS), floored=(), kept=()):
    return SimpleNamespace(
        tables=list(tables),
        source_types={"scores-src": "python", "pg": "postgresql"},
        replica_routes=SimpleNamespace(floored=set(floored), kept_for_good=set(kept)),
    )


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


async def test_a_direct_read_gives_the_function_its_inputs_and_context_and_applies_the_rest(
    prepared,
):
    from provisa.codesource.direct import read

    answer = await read(
        _state(),
        "scores-src",
        'SELECT "id", "amount", "asked" FROM "main"."scores" WHERE "region" = \'EU\' '
        'AND "score" >= 0.5 AND "_nf_as_of" = \'2026-10\' ORDER BY "amount" DESC',
    )
    assert answer.column("id").to_pylist() == [1, 3]
    assert answer.column("amount").to_pylist() == [Decimal("10.50"), Decimal("7.00")]
    assert set(answer.column("asked").to_pylist()) == {
        "as_of=2026-10;filters=region eq EU,score gte 0.5;order=amount desc;limit=None"
    }


@pytest.mark.parametrize("mode", ["ignore", "apply", "partial"])
@pytest.mark.parametrize(
    "where",
    [
        "region = 'EU' AND score >= 0.5",
        "score > 0.3",
        "region = 'EU' OR score < 0.5",  # no filter is given, and so no limit
    ],
)
async def test_whatever_part_of_its_context_a_function_applies_the_read_answers_alike(
    prepared, mode, where
):
    """REQ-2100: a function that ignores its context, applies all of it, or applies one filter
    and ignores the limit answers each read as a read that pushes nothing does."""
    from provisa.codesource.direct import read

    def answered(how: str) -> list:
        return f"SELECT id FROM main.scores WHERE ({where}) AND _nf_mode = '{how}' ORDER BY score DESC LIMIT 1"

    unpushed = await read(_state(), "scores-src", answered("ignore"))
    pushed = await read(_state(), "scores-src", answered(mode))
    assert pushed.column("id").to_pylist() == unpushed.column("id").to_pylist()
    assert unpushed.num_rows == 1


async def test_a_statement_over_two_of_the_sources_tables_is_joined_in_duckdb(prepared):
    from provisa.codesource.direct import read

    answer = await read(
        _state(),
        "scores-src",
        "SELECT s.id, r.label FROM main.scores AS s JOIN main.regions AS r "
        "ON r.region = s.region WHERE r.label <> 'Europe' ORDER BY s.id",
    )
    assert answer.to_pylist() == [{"id": 2, "label": "United States"}]


async def test_the_driver_answers_its_statement_with_its_bound_values(prepared, monkeypatch):
    from provisa.codesource.direct import CodeDriver

    monkeypatch.setattr("provisa.api.app.state", _state())
    driver = CodeDriver()
    driver.bind_source("scores-src")
    sql = "SELECT id FROM main.scores WHERE region = $1 ORDER BY id"
    first = await driver.execute(sql, ["EU"])
    # The terminal may run it again (the row after its limit): it answers the same.
    again = await driver.execute(sql, ["EU"])
    assert first.rows == again.rows == [(1,), (3,)]
    assert first.column_names == ["id"]


def test_a_statement_is_read_direct_when_it_reads_one_code_sources_tables_each_once():
    from provisa.codesource.direct import direct_tables, parameters

    one = "SELECT id FROM main.scores WHERE _nf_region = 'EU'"
    assert [t["table_name"] for t in direct_tables(_state(), {9}, one)] == ["scores"]
    assert parameters(_state(), {9}, one) == {"_nf_as_of", "_nf_mode"}
    both = "SELECT 1 FROM main.scores JOIN main.regions ON true"
    assert len(direct_tables(_state(), {9, 10}, both)) == 2
    # Read twice: two argument sets one row set cannot hold.
    twice = "SELECT 1 FROM main.scores AS a JOIN main.scores AS b ON a.id = b.id"
    assert direct_tables(_state(), {9}, twice) == []
    # Another source beside it: the engine's.
    pg = {"id": 11, "table_name": "t", "source_id": "pg", "schema_name": "public", "columns": []}
    across = "SELECT 1 FROM main.scores JOIN public.t ON true"
    assert direct_tables(_state((TABLE, pg)), {9, 11}, across) == []
    # A table the statement reads with no registered row: not known to be the code's alone.
    assert direct_tables(_state(), {9, 99}, across) == []
    # On its replica (the operator's floor): read from the replica.
    assert direct_tables(_state(floored={9}), {9}, one) == []


async def test_the_plan_of_a_code_sources_own_read_is_its_complete_dataset(prepared):
    from provisa.codesource.proxy import CodeProxy
    from provisa.federation.connector_contract import Read

    read = Read(table=TABLE, name="scores", statement="SELECT id FROM main.scores")
    plan = await CodeProxy().plan(_state(), read)
    assert plan.complete_dataset and plan.incomplete_because is None
    # A table read from its landed rows -- a declared landing TTL, or the operator's replica --
    # is no read the code answers whole: the engine reads where it landed.
    landed = Read(
        table={**TABLE, "cache_ttl": 300}, name="scores", statement="SELECT id FROM main.scores"
    )
    plan = await CodeProxy().plan(_state(), landed)
    assert not plan.complete_dataset and plan.incomplete_because
    plan = await CodeProxy().plan(_state(floored={TABLE["id"]}), read)
    assert not plan.complete_dataset


async def test_a_statement_the_engine_runs_after_all_lands_its_tables_then(monkeypatch):
    from provisa.api.data import materialization

    landed = []

    async def _land(tn, table, state, rewrites, inline, arguments, statement):
        landed.append((tn, table["table_name"], arguments, statement))
        inline[tn] = SimpleNamespace(rows=[], columns=[])

    monkeypatch.setattr(materialization, "_mat_pipeline_table", _land)
    monkeypatch.setattr(
        "provisa.cache.values_cte.build_values_ctes_sql",
        lambda sql, entries: f"{sql} /*{sorted(entries)}*/",
    )
    sql, params = await materialization.land_direct_reads(
        "SELECT id FROM main.scores WHERE _nf_as_of = $1 AND id > $2",
        ["2026", 1],
        _state((TABLE,)),
        {9},
    )
    # The parameter's condition is the call's argument, taken out with its bound value.
    assert [(tn, name, args) for tn, name, args, _ in landed] == [
        ("scores", "scores", {"as_of": "2026"})
    ]
    assert "_nf_as_of" not in sql and sql.endswith("/*['scores']*/")
    assert params == [1]


async def test_a_landed_read_asks_for_the_whole_table_under_its_inputs(prepared):
    # REQ-2100: a statement the engine runs (across sources, or a gate it failed): the function
    # is given its inputs and an empty context, its rows are landed, and the engine runs the
    # whole statement over them.
    from provisa.codesource.table import read

    landed = await read(None, TABLE, {"as_of": "2026-10"})
    assert {r["asked"] for r in landed.rows} == {"as_of=2026-10;filters=;order=;limit=None"}
    answer = _engine_answer(
        landed,
        "SELECT id, amount, asked FROM scores WHERE region = 'EU' AND score >= 0.5 "
        "ORDER BY amount DESC",
    )
    assert answer.column("id").to_pylist() == [1, 3]
    assert answer.column("amount").to_pylist() == [Decimal("10.50"), Decimal("7.00")]


async def test_a_direct_read_answers_the_governed_statement_alone(prepared):
    # The driver is reached only from the DIRECT terminal, which runs the statement the one
    # governed pipeline produced (route_governed, _route_compiled): a role's row rule and mask on
    # a code table are in the statement DuckDB applies to the function's rows.
    from provisa.codesource.direct import read
    from provisa.compiler.rls import RLSContext
    from provisa.compiler.sql_gen import CompilationContext, TableMeta
    from provisa.security.masking import MaskingRule, MaskType
    from provisa.transpiler.transpile import transpile
    from tests.helpers import governed_for_role

    ctx = CompilationContext()
    ctx.tables = {
        "scores": TableMeta(
            table_id=9,
            field_name="scores",
            type_name="Scores",
            source_id="scores-src",
            catalog_name="scores_src",
            schema_name="main",
            domain_id="main",
            table_name="scores",
        )
    }
    ctx.joins = {}
    governed = governed_for_role(
        'SELECT "id", "region" FROM "main"."scores" ORDER BY "id"',
        ctx,
        "analyst",
        rls=RLSContext(rules={9: "score > 0.5"}),
        masking_rules={
            (9, "analyst"): {
                "region": (MaskingRule(mask_type=MaskType.constant, value="**"), "text")
            }
        },
    )
    answer = await read(_state(), "scores-src", transpile(governed, "duckdb"))
    assert answer.to_pylist() == [{"id": 1, "region": "**"}, {"id": 3, "region": "**"}]


# -- routing ---------------------------------------------------------------------------------------


def _route(sources, types, floor=None):
    return decide_route(
        sources=set(sources),
        source_types=types,
        source_dialects={s: "duckdb" for s in sources},
        operator_floor=floor or {},
        statement="SELECT 1",
        # A code source's proxy plans its own statement as its complete dataset.
        complete_dataset=True,
    )


def test_a_read_of_one_code_source_goes_direct_and_a_read_across_sources_to_the_engine():
    assert _route({"scores-src"}, {"scores-src": "python"}).route == Route.DIRECT
    assert _route({"js"}, {"js": "node"}).route == Route.DIRECT
    across = _route({"scores-src", "pg"}, {"scores-src": "python", "pg": "postgresql"})
    assert across.route == Route.ENGINE
    floored = _route({"scores-src"}, {"scores-src": "python"}, {"scores-src": "replicate"})
    assert floored.route == Route.ENGINE


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


async def test_a_code_statement_is_complete_only_when_it_is_read_direct():
    """The router's complete-dataset input for a code source is the materialization step's own
    test (direct_tables): a statement it lands -- one that also reaches another source's table, as
    an INSERT ... SELECT's read does, or a table declaring a landing TTL -- is read where it landed
    by the engine, never handed to the code's driver as SQL over the landed table."""
    from provisa.pgwire._pipeline import _proxy_plan

    target = {**REGIONS, "id": 11, "source_id": "pg", "table_name": "targets"}
    landing = {**TABLE, "cache_ttl": 300}
    sql = "SELECT s.id FROM main.scores AS s"
    across = await _proxy_plan({"scores-src"}, sql, None, _state((TABLE, target)), (9, 11))
    assert across == {
        "complete_dataset": False,
        "incomplete": "its tables are read where they landed",
    }
    landed = await _proxy_plan({"scores-src"}, sql, None, _state((landing,)), (9,))
    assert landed["complete_dataset"] is False
