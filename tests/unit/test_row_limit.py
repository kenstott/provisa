# Copyright (c) 2026 Kenneth Stott
# Canary: ebc9d018-bb55-4de2-81ab-415e59f0aaa2
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A read that a row limit cut short says so (REQ-1949): which limit bounds a governed read,
the statement that asks for the row after it, and the check where a buffered answer is made."""

# Requirements: REQ-1949

from __future__ import annotations

import asyncio
from types import SimpleNamespace

import duckdb
import pytest

from provisa.compiler.row_limit import ROLE, TABLE, RowLimit, binds, cut_warning, next_row_sql
from provisa.compiler.stage2 import GovernanceContext, apply_governance
from provisa.core.statement_warnings import collecting
from provisa.executor.result import QueryResult
from provisa.pgwire import _pipeline
from provisa.transpiler.router import Route


@pytest.mark.parametrize(
    ("sql", "bound"),
    [
        ("SELECT id FROM t", True),
        ("SELECT id FROM t LIMIT 11", True),
        ("SELECT id FROM t LIMIT $1", True),
        ("SELECT id FROM t LIMIT 10", False),  # the statement asked for exactly that many
        ("SELECT id FROM t LIMIT 3", False),
        ("SELECT id FROM a UNION ALL SELECT id FROM b", True),
    ],
)
def test_a_ceiling_bounds_a_statement_unless_its_own_limit_is_within_it(sql, bound):
    assert binds(sql, 10) is bound


def _governed(sql: str, role: int | None, table: int | None = None) -> tuple[str, RowLimit | None]:
    gov = GovernanceContext()
    gov.table_map = {"t": 1}
    gov.all_columns = {1: [("id", "integer")]}
    gov.limit_ceiling = role
    if table is not None:
        gov.table_ceilings = {1: table}
    return apply_governance(sql, gov), gov.row_limit


def test_governance_records_the_limit_that_bounds_the_read_and_whose_it_is():
    sql, limit = _governed("SELECT id FROM t", 10)
    assert sql.endswith("LIMIT 10") and limit == RowLimit(10, ROLE)
    # A table's smaller ceiling is the one that bounds, and is named as the table's.
    sql, limit = _governed("SELECT id FROM t", 10, table=4)
    assert sql.endswith("LIMIT 4") and limit == RowLimit(4, TABLE)
    sql, limit = _governed("SELECT id FROM t", 4, table=10)
    assert sql.endswith("LIMIT 4") and limit == RowLimit(4, ROLE)
    # The statement's own LIMIT within the ceiling: the statement bounds itself, nothing is cut.
    sql, limit = _governed("SELECT id FROM t LIMIT 3", 10)
    assert sql.endswith("LIMIT 3") and limit is None
    # No ceiling at all (full results): none.
    assert _governed("SELECT id FROM t", None)[1] is None


def test_the_statement_never_asks_for_more_than_the_limit():
    """The governed statement is bounded AT the limit, as before this requirement: whatever
    reads it -- a buffered answer, a stream, a landed result -- gets no row past it."""
    for own in ("", " LIMIT 500", " LIMIT $1"):
        assert "LIMIT 10" in _governed(f"SELECT id FROM t{own}", 10)[0]


@pytest.fixture
def engine():
    con = duckdb.connect()
    con.execute("CREATE TABLE t AS SELECT range::INTEGER AS id FROM range(11)")
    con.execute("CREATE TABLE exact AS SELECT range::INTEGER AS id FROM range(10)")
    return con


def test_the_next_row_exists_exactly_when_rows_were_left_out(engine):
    """One row past the limit is a cut read; a table of exactly the limit is not."""
    past = next_row_sql("SELECT id FROM t ORDER BY id LIMIT 10", 10, "duckdb")
    whole = next_row_sql("SELECT id FROM exact ORDER BY id LIMIT 10", 10, "duckdb")
    assert past is not None and whole is not None
    assert engine.execute(past).fetchall() == [(10,)]
    assert engine.execute(whole).fetchall() == []


def test_the_next_row_respects_the_statements_own_offset_and_shape(engine):
    skipped = next_row_sql("SELECT id FROM t ORDER BY id LIMIT 10 OFFSET 1", 10, "duckdb")
    assert skipped is not None and engine.execute(skipped).fetchall() == []
    capped = next_row_sql(
        "SELECT * FROM (SELECT id FROM t ORDER BY id LIMIT 500) AS _govern_capped LIMIT 10",
        10,
        "duckdb",
    )
    assert capped is not None and len(engine.execute(capped).fetchall()) == 1
    union = next_row_sql("SELECT id FROM t UNION ALL SELECT id FROM exact LIMIT 10", 10, "duckdb")
    assert union is not None and len(engine.execute(union).fetchall()) == 1


@pytest.mark.parametrize(
    "sql",
    ["SELECT id FROM t", "SELECT id FROM t LIMIT 7", "SELECT id FROM t LIMIT $1"],
)
def test_a_statement_not_bounded_at_the_limit_has_no_next_row_statement(sql):
    assert next_row_sql(sql, 10, "postgres") is None


def test_the_warning_names_the_limit_and_whose_it_is():
    role, table = cut_warning(RowLimit(10, ROLE)), cut_warning(RowLimit(4, TABLE))
    assert (role.code, role.params) == ("statement.rows_cut", {"limit": 10, "kind": "role"})
    assert (table.code, table.params) == ("statement.rows_cut", {"limit": 4, "kind": "table"})
    assert "cut at 10 rows" in role.message and "the role" in role.message
    assert "cut at 4 rows" in table.message and "a table it reads" in table.message


def _plan(route=Route.ENGINE, limit: RowLimit | None = RowLimit(3, ROLE)) -> _pipeline._Plan:
    plan = _pipeline._Plan(
        route=route,
        sql="SELECT id FROM t LIMIT 3",
        source_id="pg",
        dialect="postgres",
        physical_sql='SELECT id FROM "pg"."public"."t" LIMIT 3',
    )
    plan.row_limit = limit
    plan.stamp = "governed"  # as a plan the one pipeline minted
    return plan


_STATE = SimpleNamespace(federation_engine=SimpleNamespace(dialect="duckdb"))


def _answer(rows: int, **kw) -> QueryResult:
    return QueryResult(rows=[(i,) for i in range(rows)], column_names=["id"], **kw)


@pytest.mark.parametrize(
    ("rows", "more", "warned"),
    [(3, True, True), (3, False, False), (2, True, False)],
    ids=["filled_and_more", "filled_exactly", "within_the_limit"],
)
def test_an_answer_says_it_was_cut_only_when_rows_were_left_out(monkeypatch, rows, more, warned):
    asked = []

    async def were_cut(plan, state):
        asked.append(plan)
        return more

    monkeypatch.setattr(_pipeline, "rows_were_cut", were_cut)
    plan = _plan()
    with collecting() as found:
        asyncio.run(_pipeline._warn_if_cut(plan, _answer(rows), _STATE))
    expected = [cut_warning(RowLimit(3, ROLE))] if warned else []
    # On the plan (the surfaces that answer from it) and in the request's collector (HTTP, Bolt).
    assert plan.warnings == expected and found == expected
    # An answer within the limit is known whole: nothing more is read for it.
    assert len(asked) == (1 if rows == 3 else 0)


def test_a_read_no_limit_bounds_and_a_landed_result_are_not_checked_here(monkeypatch):
    async def never(plan, state):
        raise AssertionError("not asked")

    monkeypatch.setattr(_pipeline, "rows_were_cut", never)
    asyncio.run(_pipeline._warn_if_cut(_plan(limit=None), _answer(3), _STATE))
    landed = QueryResult(rows=[], column_names=[], redirect={"row_count": 3})
    asyncio.run(_pipeline._warn_if_cut(_plan(), landed, _STATE))


def test_a_warning_found_with_no_collector_open_is_kept_on_the_plan(monkeypatch):
    """A surface that answers from the plan (pgwire, Flight, gRPC) opens no collector while the
    rows are read: the warning is not an error there, it is on the plan."""

    async def were_cut(plan, state):
        return True

    monkeypatch.setattr(_pipeline, "rows_were_cut", were_cut)
    plan = _plan()
    asyncio.run(_pipeline._warn_if_cut(plan, _answer(3), _STATE))
    assert plan.warnings == [cut_warning(RowLimit(3, ROLE))]


@pytest.mark.parametrize("route", [Route.ENGINE, Route.DIRECT])
def test_the_next_row_is_asked_through_the_terminal_the_answer_came_from(monkeypatch, route):
    ran = []

    async def terminal(plan, state):
        ran.append(plan)
        return _answer(1)

    monkeypatch.setattr(_pipeline, "_run_plan_terminal", terminal)
    plan = _plan(route)
    assert asyncio.run(_pipeline.rows_were_cut(plan, _STATE)) is True
    (asking,) = ran
    read = asking.physical_sql if route == Route.ENGINE else asking.sql
    assert "LIMIT 1" in read and "OFFSET 3" in read
    # Nothing is landed, audited or checked again for it, and the plan itself is untouched.
    assert asking.materialize is None and asking.audit is None and asking.row_limit is None
    assert plan.sql.endswith("LIMIT 3") and plan.physical_sql.endswith("LIMIT 3")

    async def none_left(plan, state):
        return _answer(0)

    monkeypatch.setattr(_pipeline, "_run_plan_terminal", none_left)
    assert asyncio.run(_pipeline.rows_were_cut(_plan(route), _STATE)) is False


def test_a_check_that_fails_is_said_in_the_answer_never_passed_over(monkeypatch):
    """The answer is read and stands; that its row limit could not be checked is one of its
    warnings, with why -- not an error, and not silence."""

    async def broken(plan, state):
        raise ConnectionError("source went away")

    monkeypatch.setattr(_pipeline, "_run_plan_terminal", broken)
    plan = _plan()
    with collecting() as found:
        asyncio.run(_pipeline._warn_if_cut(plan, _answer(3), _STATE))
    (warning,) = found
    assert plan.warnings == [warning] and warning.code == "statement.rows_cut_unchecked"
    assert warning.params == {"limit": 3, "kind": "role", "reason": "source went away"}

    # A statement an optimization left without the governed bound outermost: said the same way.
    unbounded = _plan()
    unbounded.physical_sql = "SELECT id FROM t"
    asyncio.run(_pipeline._warn_if_cut(unbounded, _answer(3), _STATE))
    assert [w.code for w in unbounded.warnings] == ["statement.rows_cut_unchecked"]
    assert "outermost LIMIT 3" in unbounded.warnings[0].params["reason"]


def test_the_requests_deadline_ends_the_request_as_it_does_the_read(monkeypatch):
    async def too_late(plan, state):
        raise TimeoutError("request timed out")

    monkeypatch.setattr(_pipeline, "_run_plan_terminal", too_late)
    with pytest.raises(TimeoutError):
        asyncio.run(_pipeline._warn_if_cut(_plan(), _answer(3), _STATE))


def test_a_cut_answer_is_never_kept_and_a_kept_answer_is_not_checked_again(monkeypatch):
    """The cut is settled before the answer is stored: a warned answer is not stored as the
    statement's answer, so a repeat is read and says so again, and a hit is a whole answer."""
    import inspect

    stored = []

    async def were_cut(plan, state):
        return True

    async def store(plan, state, result):
        stored.append(list(plan.warnings))

    async def no_hit(plan, state):
        return None

    monkeypatch.setattr(_pipeline, "rows_were_cut", were_cut)
    monkeypatch.setattr(_pipeline, "store_executed_result", store)
    monkeypatch.setattr(_pipeline, "check_response_cache", no_hit)
    monkeypatch.setattr(
        "provisa.federation.live_concurrency.acquire_plan_permits",
        lambda state, plan: __import__("contextlib").nullcontext(),
    )

    async def execute():
        return _answer(3)

    plan = _plan()
    asyncio.run(_pipeline.serve_buffered_through_cache(plan, _STATE, execute))
    assert [[w.code for w in at_store] for at_store in stored] == [["statement.rows_cut"]]
    # ... and the tee refuses a plan that carries a warning.
    assert _pipeline._cache_tee(plan, _STATE, None, None) is None
    # The chokepoint settles it in the same order.
    chokepoint = inspect.getsource(_pipeline._execute_plan_in_org)
    assert chokepoint.index("_warn_if_cut(") < chokepoint.index("store_executed_result(")

    async def never(plan, state):
        raise AssertionError("a kept answer was whole when it was stored")

    monkeypatch.setattr(_pipeline, "rows_were_cut", never)
    hit = _answer(3, cache_entry=object())
    asyncio.run(_pipeline._warn_if_cut(_plan(), hit, _STATE))


def test_a_temporary_table_filled_from_a_cut_read_is_checked_as_any_read():
    """The read that fills a temporary table is the plan's own governed SELECT, answered by
    the chokepoint like any read (REQ-615): it carries the one warning, none of its own."""
    import inspect

    from provisa.pgwire import temp_exec

    bound = inspect.getsource(_pipeline._execute_plan_bound)
    assert bound.index("_execute_plan_with_secrets(") < bound.index("apply(plan.temp")
    assert "rows_capped" not in inspect.getsource(temp_exec)


def test_the_catalog_says_it_in_every_locale():
    import json
    import pathlib

    locales = pathlib.Path(__file__).parents[2] / "provisa-ui/src/i18n/locales"
    english = json.loads((locales / "en/serverErrors.json").read_text())["serverErrors"]
    text = english["statement"]["rows_cut"]
    assert "{{limit}}" in text and "{{kind}}" in text
    for catalog in sorted(locales.glob("*/serverErrors.json")):
        statement = json.loads(catalog.read_text())["serverErrors"]["statement"]
        said, unchecked = statement["rows_cut"], statement["rows_cut_unchecked"]
        assert "{{limit}}" in said and "{{kind}}" in said, catalog
        assert all(p in unchecked for p in ("{{limit}}", "{{kind}}", "{{reason}}")), catalog
        if catalog.parent.name != "en":
            assert said != text and unchecked != english["statement"]["rows_cut_unchecked"]


class _SyncEngine:
    """The engine's synchronous terminal, answering the next-row statement with ``rows``."""

    dialect = "duckdb"

    def __init__(self, rows: list[tuple] | Exception) -> None:
        self._rows = rows
        self.asked: list[str] = []

    def execute_engine_sync(self, sql, params=None, *, session_hints=None, authorization=None):
        self.asked.append(sql)
        if isinstance(self._rows, Exception):
            raise self._rows
        return SimpleNamespace(rows=lambda: self._rows)


def _drain(monkeypatch, plan, batches, engine) -> list:
    import provisa.api.app as app

    monkeypatch.setattr(app, "state", SimpleNamespace(federation_engine=engine))
    return list(_pipeline.audit_on_drain(plan, iter(batches)))


@pytest.mark.parametrize(
    ("batches", "next_rows", "codes"),
    [
        ([[(1,), (2,)], [(3,)]], [(4,)], ["statement.rows_cut"]),
        ([[(1,), (2,)], [(3,)]], [], []),
        ([[(1,), (2,)]], [(4,)], []),
    ],
    ids=["filled_and_more", "filled_exactly", "within_the_limit"],
)
def test_a_stream_that_fills_the_limit_is_asked_for_its_next_row_when_it_ends(
    monkeypatch, batches, next_rows, codes
):
    engine = _SyncEngine(next_rows)
    plan = _plan()
    out = _drain(monkeypatch, plan, batches, engine)
    assert out == batches  # the stream itself is what it was: never a row past the limit
    assert [w.code for w in plan.warnings] == codes
    filled = sum(len(b) for b in batches) == 3
    assert len(engine.asked) == (1 if filled else 0)
    if filled:
        assert "LIMIT 1" in engine.asked[0] and "OFFSET 3" in engine.asked[0]


def test_a_stream_whose_check_fails_says_so_and_still_ends(monkeypatch):
    plan = _plan()
    out = _drain(monkeypatch, plan, [[(1,), (2,), (3,)]], _SyncEngine(ConnectionError("gone")))
    assert len(out) == 1
    assert [w.code for w in plan.warnings] == ["statement.rows_cut_unchecked"]
    direct = _plan(Route.DIRECT)
    _drain(monkeypatch, direct, [[(1,), (2,), (3,)]], _SyncEngine([]))
    assert "not read through the engine" in direct.warnings[0].params["reason"]


def test_a_stream_no_limit_bounds_is_passed_through_untouched(monkeypatch):
    plan = _plan(limit=None)
    batches = iter([[(1,)]])
    assert _pipeline.audit_on_drain(plan, batches) is batches


def test_flight_says_a_late_warning_in_a_last_zero_row_batch():
    """Flight and airport: what is known before the rows rides a zero-row batch ahead of them;
    what the rows showed rides another, the last of the stream."""
    import pyarrow as pa

    from provisa.api.flight import compression

    schema = pa.schema([("id", pa.int64())])
    data = pa.RecordBatch.from_pylist([{"id": 1}], schema=schema)
    early, late = cut_warning(RowLimit(9, TABLE)), cut_warning(RowLimit(3, ROLE))
    warnings = [early]

    def rows():
        yield data
        warnings.append(late)  # the drain's end found it

    out = list(compression._with_warnings(schema, rows(), warnings))
    assert [type(item) for item in out] == [tuple, pa.RecordBatch, tuple]
    assert out[0][0].num_rows == 0 and out[2][0].num_rows == 0
    assert out[0][1] != out[2][1]
    # Nothing to say at either end: the rows alone.
    assert list(compression._with_warnings(schema, iter([data]), [])) == [data]


def test_pgwire_says_a_late_warning_once_after_the_rows():
    from provisa.pgwire.server import ProvisaQueryResult

    early, late = cut_warning(RowLimit(9, TABLE)), cut_warning(RowLimit(3, ROLE))
    plan = _plan(limit=None)
    plan.warnings = [early]
    result = ProvisaQueryResult(_answer(1), "SELECT 1", plan=plan)
    assert result.warnings == (early,) and result.late_warnings() == []
    plan.warnings.append(late)
    assert result.late_warnings() == [late]
    assert result.late_warnings() == []


def test_the_audit_row_says_what_the_limit_did_to_the_answer():
    """REQ-1949: the warning is a fact about what the caller was given, so the statement's
    audit row holds it -- in ``enforced``, beside the limit itself."""
    plan = _plan()
    was = {"row_cap": 3, "table_caps": {}}
    assert _pipeline._enforced_with_limit_outcome(was, plan) is was  # whole: nothing added
    plan.limit_outcome = "cut"
    said = {"limit": 3, "kind": "role", "outcome": "cut"}
    assert _pipeline._enforced_with_limit_outcome(was, plan)() == {**was, "row_limit": said}
    # ``enforced`` may be a resolver the audit writer calls on its own thread.
    assert _pipeline._enforced_with_limit_outcome(lambda: was, plan)()["row_limit"] == said
    plan.limit_outcome = "unchecked"
    assert (
        _pipeline._enforced_with_limit_outcome(was, plan)()["row_limit"]["outcome"] == "unchecked"
    )


def test_the_buffered_answer_is_checked_before_its_audit_row_is_written():
    import inspect

    chokepoint = inspect.getsource(_pipeline._execute_plan_in_org)
    assert chokepoint.index("_warn_if_cut(") < chokepoint.index("finalize_audit(plan, 200")
    assert "_enforced_with_limit_outcome(" in inspect.getsource(_pipeline.finalize_audit)


def test_a_streams_audit_row_holds_what_its_drain_found(monkeypatch):
    import dataclasses

    @dataclasses.dataclass
    class Record:
        enforced: object
        status_code: int = 0
        row_count: int = 0

    written = []
    monkeypatch.setattr(
        "provisa.audit.pipeline.complete_audit_record",
        lambda record, started, status_code, row_count: written.append((record, row_count)),
    )
    plan = _plan()
    plan.audit_deferred = Record({"row_cap": 3})
    out = _drain(monkeypatch, plan, [[(1,), (2,), (3,)]], _SyncEngine([(4,)]))
    assert len(out) == 1
    ((record, rows),) = written
    assert rows == 3
    assert record.enforced() == {
        "row_cap": 3,
        "row_limit": {"limit": 3, "kind": "role", "outcome": "cut"},
    }


def _tee(whole):
    from provisa.cache.raw_sql import ResponseCacheTee

    tee = ResponseCacheTee(SimpleNamespace(), 100, run=None)
    tee.whole = whole
    return tee


def test_a_cut_stream_is_not_kept_as_the_statements_answer(monkeypatch):
    """The response cache's capture ends inside the drain: it asks whether the stream was the
    whole answer before keeping it, so a hit never serves a cut answer with no warning."""
    stream = _answer(3)
    cut = _tee(lambda rows: rows != 3)
    assert [len(b) for b in cut.rows(stream).batches()] == [3]
    assert cut.stored_entry is None
    whole = _tee(lambda rows: True)
    list(whole.rows(_answer(3)).batches())
    assert whole.stored_entry is not None


def test_the_row_after_the_limit_is_asked_once_for_a_stream(monkeypatch):
    """The cache's capture and the drain both see the stream end; the next row is read once."""
    import provisa.api.app as app

    engine = _SyncEngine([(4,)])
    monkeypatch.setattr(app, "state", SimpleNamespace(federation_engine=engine))
    plan = _plan()
    assert _pipeline._stream_answer_whole(plan, 3) is False  # the capture: not kept
    assert _pipeline._stream_answer_whole(plan, 3) is False  # the drain: nothing more read
    assert len(engine.asked) == 1 and [w.code for w in plan.warnings] == ["statement.rows_cut"]
    within = _plan()
    assert _pipeline._stream_answer_whole(within, 2) is True and len(engine.asked) == 1


def test_the_cache_capture_of_a_limited_read_asks_before_it_keeps(monkeypatch):
    import inspect

    assert "tee.whole = functools.partial(_stream_answer_whole, plan)" in inspect.getsource(
        _pipeline._cache_tee
    )


def test_a_stream_on_its_event_loop_asks_through_the_plans_own_terminal(monkeypatch):
    """gRPC's source streams run on the RPC's loop: the row after the limit is asked for as a
    buffered answer's is, once, and only for a stream that filled the limit."""
    asked = []

    async def terminal(plan, state):
        asked.append(plan.sql)
        return _answer(1)

    monkeypatch.setattr(_pipeline, "_run_plan_terminal", terminal)
    plan = _plan(Route.DIRECT)
    asyncio.run(_pipeline.settle_cut_at_stream_end(plan, 2, _STATE))
    assert asked == [] and plan.warnings == []
    asyncio.run(_pipeline.settle_cut_at_stream_end(plan, 3, _STATE))
    asyncio.run(_pipeline.settle_cut_at_stream_end(plan, 3, _STATE))
    assert len(asked) == 1 and "OFFSET 3" in asked[0]
    assert [w.code for w in plan.warnings] == ["statement.rows_cut"]
    assert plan.limit_outcome == "cut"
    asyncio.run(_pipeline.settle_cut_at_stream_end(_plan(limit=None), 3, _STATE))


def test_the_streams_second_read_is_authorized_by_the_plans_own_stamp(monkeypatch):
    seen = {}

    class Engine(_SyncEngine):
        def execute_engine_sync(self, sql, params=None, *, session_hints=None, authorization=None):
            seen["authorization"] = authorization
            return SimpleNamespace(rows=lambda: [])

    plan = _plan()
    plan.stamp = "governed-stamp"
    state = SimpleNamespace(federation_engine=Engine([]))
    assert _pipeline._stream_was_cut(plan, state) is False
    assert seen["authorization"].stamp == "governed-stamp"


def test_grpc_sets_its_trailing_metadata_again_only_for_a_late_warning():
    from provisa.core.statement_warnings import header_value
    from provisa.grpc.server import ProvisaServicer

    class Context:
        def __init__(self):
            self.set = []

        def set_trailing_metadata(self, metadata):
            self.set.append(metadata)

    plan = _plan()
    nag = ("x-provisa-license-notice", "trial")
    ctx = Context()
    ProvisaServicer._say_late_warnings(ctx, [nag], plan)
    assert ctx.set == []  # nothing new to say: what was set stands
    plan.warnings.append(cut_warning(RowLimit(3, ROLE)))
    ProvisaServicer._say_late_warnings(ctx, [nag], plan)
    # Set once more as it was -- the notice kept -- with the warnings as they now stand.
    assert ctx.set == [(nag, ("x-provisa-warnings", header_value(plan.warnings)))]
    said = [nag, ("x-provisa-warnings", header_value(plan.warnings))]
    ProvisaServicer._say_late_warnings(ctx, said, plan)
    assert len(ctx.set) == 1


def test_every_grpc_stream_settles_the_cut_before_its_audit_row():
    import inspect

    from provisa.grpc.server import ProvisaServicer

    source = inspect.getsource(ProvisaServicer._handle_query_bound)
    assert source.count("_say_late_warnings(context, _said, plan)") == 2
    assert "_stream_answer_whole(plan, _delivered)" in source
    assert source.count("settle_cut_at_stream_end(plan, ") == 2
