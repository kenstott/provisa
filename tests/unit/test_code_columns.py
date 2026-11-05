# Copyright (c) 2026 Kenneth Stott
# Canary: 1d300d55-cb4f-4c18-b96c-61bbc84a81a1
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A code source's ``@columns`` function, a batch column transform (REQ-2131).

Given a Python source with clean_address tagged @columns, taking raw_address and returning street,
city and country: applied in a statement's select list, it is called in batches, the batches run in
parallel across the source's workers, each row's street, city and country are the values returned
for it, and a batch answered with fewer results than rows is refused naming clean_address. It is
registered as a command from what the function declares, and is a named step of the projection's
lineage."""

# Requirements: REQ-2131

from __future__ import annotations

import threading
import time
from types import SimpleNamespace

import pytest

from provisa.codesource import columns as columns_mod
from provisa.codesource.columns import BATCH, ColumnTransformRefused, contract
from provisa.codesource.columns_fold import ColumnsCall, apply, fold, reshape
from provisa.codesource.manifest import NODE_RULES, read_manifest
from provisa.executor.result import QueryResult, StreamingQueryResult

pytestmark = pytest.mark.unit

S = {"kind": "string"}
ADDRESS = {
    "kind": "object",
    "name": "Address",
    "fields": [
        {"name": n, "description": "", "optional": False, "type": S}
        for n in ("street", "city", "country")
    ],
}
MANIFEST = read_manifest(
    {
        "exports": [
            {
                "name": "clean_address",
                "tags": ["columns"],
                "description": "Splits an address.",
                "signatures": [
                    {
                        "parameters": [
                            {
                                "name": "raw_address",
                                "optional": False,
                                "rest": False,
                                "type": {"kind": "array", "element": S},
                            }
                        ],
                        "returns": {"kind": "array", "element": {"kind": "ref", "name": "Address"}},
                    }
                ],
            }
        ],
        "types": {"Address": ADDRESS},
        "diagnostics": [],
    },
    NODE_RULES,
)
COMMAND = {
    "name": "clean_address",
    "source_id": "addresses",
    "function_name": "clean_address",
    "impl_kind": "code_columns",
    "arguments": [{"name": "raw_address", "type": "text"}],
    "output_columns": [
        {"name": "street", "type": "text"},
        {"name": "city", "type": "text"},
        {"name": "country", "type": "text"},
    ],
}


def _split(raw: str) -> dict:
    street, city, country = raw.split(",")
    return {"street": street, "city": city, "country": country}


class _Worker:
    def __init__(self, slot: int, seen: list, barrier: threading.Barrier | None, short: bool):
        self.slot, self.seen, self.barrier, self.short = slot, seen, barrier, short

    def columns(self, export, columns, names, *, schema):
        assert (export, names) == ("clean_address", ["raw_address"]) and schema
        if self.barrier is not None and not any(slot == self.slot for slot, _ in self.seen):
            # Every worker in its first call at once: they run side by side.
            self.barrier.wait(timeout=10)
        self.seen.append((self.slot, len(columns[0])))
        answered = [_split(raw) for raw in columns[0]]
        return answered[:-1] if self.short else answered


def _prepared(monkeypatch, workers: int, barrier=None, short=False) -> list:
    seen: list = []

    async def prepared(_state, source_id):
        assert source_id == "addresses"
        return SimpleNamespace(
            manifest=MANIFEST,
            workers=workers,
            worker=lambda slot: _Worker(slot, seen, barrier, short),
        )

    monkeypatch.setattr(columns_mod, "_prepared", prepared)
    return seen


def test_the_contract_is_the_functions_input_and_output_columns():
    assert contract(MANIFEST, MANIFEST.export("clean_address")) == (
        COMMAND["arguments"],
        COMMAND["output_columns"],
    )


def _streamed(rows: int, chunk: int, read: list) -> StreamingQueryResult:
    """A read of ``rows`` addresses, streamed in record batches of ``chunk`` rows; ``read`` holds
    the rows pulled so far, after each pull."""

    def batches():
        for at in range(0, rows, chunk):
            batch = [(i, f"{i} Main St,Springfield,US") for i in range(at, min(at + chunk, rows))]
            read.append(at + len(batch))
            yield batch

    return StreamingQueryResult(
        batches(), ["id", "__provisa_columns_0_0"], column_types=["integer", "text"]
    )


async def _applied(read: StreamingQueryResult):
    folded = fold(
        "SELECT id, clean_address(raw_address) FROM t",
        {"clean_address": COMMAND},
        lambda c, n: None,
    )
    return await apply(
        read, folded.calls, SimpleNamespace(column_transforms={"clean_address": COMMAND})
    )


def _expected(rows: int) -> list[tuple]:
    return [(i, f"{i} Main St", "Springfield", "US") for i in range(rows)]


@pytest.mark.asyncio
async def test_batches_run_in_parallel_across_the_sources_workers_each_row_its_own_result(
    monkeypatch,
):
    rows = 3 * BATCH + 5
    seen = _prepared(monkeypatch, workers=3, barrier=threading.Barrier(3))
    done = await _applied(_streamed(rows, BATCH, []))
    assert done.column_names == ["id", "street", "city", "country"]
    assert done.rows == _expected(rows)  # each row's values, in order
    assert sorted(seen) == [(0, 5), (0, BATCH), (1, BATCH), (2, BATCH)]


@pytest.mark.asyncio
async def test_record_batches_are_cut_again_into_batches_of_1000_rows(monkeypatch):
    seen = _prepared(monkeypatch, workers=2)
    done = await _applied(_streamed(2 * BATCH + 300, 700, []))
    assert done.rows == _expected(2 * BATCH + 300)  # read through: the batches run as it is
    assert sorted(n for _, n in seen) == [300, BATCH, BATCH]


@pytest.mark.asyncio
async def test_the_first_rows_are_answered_before_the_last_are_read(monkeypatch):
    _prepared(monkeypatch, workers=2)
    read: list = []
    batches = (await _applied(_streamed(20 * BATCH, BATCH, read))).batches()
    assert next(batches) == _expected(BATCH)
    assert read[-1] < 20 * BATCH  # the result is not read whole first
    batches.close()


@pytest.mark.asyncio
async def test_no_more_batches_are_held_than_the_source_has_workers(monkeypatch):
    workers, rows = 3, 30 * BATCH
    running, most = [0], [0]
    lock = threading.Lock()

    class Counting(_Worker):
        def columns(self, export, columns, names, *, schema):
            with lock:
                running[0] += 1
                most[0] = max(most[0], running[0])
            time.sleep(0.002)
            with lock:
                running[0] -= 1
            return [_split(raw) for raw in columns[0]]

    async def prepared(_state, _source_id):
        return SimpleNamespace(
            manifest=MANIFEST, workers=workers, worker=lambda slot: Counting(slot, [], None, False)
        )

    monkeypatch.setattr(columns_mod, "_prepared", prepared)
    read: list = []
    out = []
    for k, batch in enumerate((await _applied(_streamed(rows, BATCH, read))).batches()):
        # Before the k-th answered batch is passed on, at most ``workers`` more were read for it.
        assert read[-1] <= (k + 1 + workers) * BATCH
        out.extend(batch)
    assert most[0] <= workers
    assert out == _expected(rows)


@pytest.mark.asyncio
async def test_rows_keep_their_order_whichever_worker_answers_first(monkeypatch):
    class Slow(_Worker):
        def columns(self, export, columns, names, *, schema):
            time.sleep(0.05 if self.slot == 0 else 0)  # the first batch answered last
            return [_split(raw) for raw in columns[0]]

    async def prepared(_state, _source_id):
        return SimpleNamespace(
            manifest=MANIFEST, workers=4, worker=lambda slot: Slow(slot, [], None, False)
        )

    monkeypatch.setattr(columns_mod, "_prepared", prepared)
    assert (await _applied(_streamed(8 * BATCH, BATCH, []))).rows == _expected(8 * BATCH)


@pytest.mark.asyncio
async def test_a_batch_answered_with_fewer_results_than_rows_is_refused_naming_the_function(
    monkeypatch,
):
    _prepared(monkeypatch, workers=1, short=True)
    done = await _applied(_streamed(2, 2, []))
    with pytest.raises(ColumnTransformRefused) as refused:
        done.rows
    assert refused.value.params["function"] == "clean_address"
    assert "returned 1 results for a batch of 2 rows" in str(refused.value)


# -- in a statement -------------------------------------------------------------------------------


def _fold(sql: str, admitted: list | None = None):
    admitted = [] if admitted is None else admitted
    return fold(sql, {"clean_address": COMMAND}, lambda c, n: admitted.append(n))


def test_a_select_list_call_reads_its_inputs_and_stands_for_its_outputs():
    admitted: list = []
    folded = _fold(
        "SELECT s.id, clean_address(s.raw_address), s.name FROM sales.suppliers AS s", admitted
    )
    assert folded.sql == (
        "SELECT s.id, s.raw_address AS __provisa_columns_0_0, s.name FROM sales.suppliers AS s"
    )
    assert folded.calls == (
        ColumnsCall(
            "clean_address",
            ("__provisa_columns_0_0",),
            (("street", "text"), ("city", "text"), ("country", "text")),
        ),
    )
    assert admitted == ["clean_address"]  # admitted as any command is


@pytest.mark.parametrize(
    ("sql", "said"),
    [
        (
            "SELECT id FROM sales.suppliers WHERE clean_address(raw_address) IS NOT NULL",
            "write it as an item of the statement's select list",
        ),
        (
            "SELECT clean_address(raw_address) AS parts FROM sales.suppliers",
            "rename them in an outer projection",
        ),
        (
            "SELECT clean_address(raw_address, name) FROM sales.suppliers",
            "it takes 1 input columns (raw_address), not 2",
        ),
    ],
)
def test_a_call_that_is_not_a_plain_select_list_item_is_refused_naming_it(sql, said):
    with pytest.raises(ColumnTransformRefused) as refused:
        _fold(sql)
    assert refused.value.params["function"] == "clean_address"
    assert said in str(refused.value)


def test_a_call_the_role_may_not_make_is_refused_by_the_command_admission():
    def admit(_command, name):
        raise PermissionError(f"Unknown command: {name!r}")

    with pytest.raises(PermissionError, match="clean_address"):
        fold("SELECT clean_address(raw_address) FROM t", {"clean_address": COMMAND}, admit)


def test_a_statement_that_calls_none_is_left_as_it_is():
    sql = "SELECT id FROM sales.suppliers"
    assert _fold(sql).sql == sql and _fold(sql).calls == ()


@pytest.mark.asyncio
async def test_once_read_each_rows_inputs_become_its_output_columns(monkeypatch):
    _prepared(monkeypatch, workers=1)
    folded = _fold("SELECT id, clean_address(raw_address), name FROM sales.suppliers")
    read = QueryResult(
        rows=[(1, "1 Main St,Springfield,US", "Acme"), (2, "2 High St,Leeds,UK", "Brit")],
        column_names=["id", "__provisa_columns_0_0", "name"],
        column_types=["integer", "text", "text"],
    )
    state = SimpleNamespace(column_transforms={"clean_address": COMMAND})
    done = await apply(read, folded.calls, state)
    assert done.column_names == ["id", "street", "city", "country", "name"]
    assert done.column_types == ["integer", "text", "text", "text", "text"]
    assert done.rows == [
        (1, "1 Main St", "Springfield", "US", "Acme"),
        (2, "2 High St", "Leeds", "UK", "Brit"),
    ]


def test_the_result_shape_is_the_output_columns_in_place_of_the_inputs():
    folded = _fold("SELECT id, clean_address(raw_address) FROM sales.suppliers")
    assert reshape([("id", "integer"), ("__provisa_columns_0_0", "text")], folded.calls) == [
        ("id", "integer"),
        ("street", "text"),
        ("city", "text"),
        ("country", "text"),
    ]


# -- in lineage -----------------------------------------------------------------------------------


def test_each_output_is_a_named_step_of_the_projections_lineage():
    from provisa.lineage.graph import build_column_graph

    graph = build_column_graph(
        "SELECT id, clean_address(raw_address) FROM suppliers",
        commands={"clean_address": COMMAND},
    )
    assert graph.outputs == ["id", "street", "city", "country"]
    for output in ("street", "city", "country"):
        (edge,) = [e for e in graph.edges if e.target == output]
        assert edge.source == "suppliers.raw_address"
        assert [(op.name, op.kind) for op in edge.ops] == [("clean_address", "command")]


# -- registered, and its workers --------------------------------------------------------------------


@pytest.mark.asyncio
async def test_a_command_on_a_code_source_is_registered_from_what_the_function_declares(
    monkeypatch,
):
    from provisa.api.admin import actions_router
    from provisa.api.app import state

    async def registration(_state, source_id, function):
        assert (source_id, function) == ("addresses", "clean_address")
        return COMMAND["arguments"], COMMAND["output_columns"]

    monkeypatch.setattr(state, "source_types", {"addresses": "python"}, raising=False)
    monkeypatch.setattr(columns_mod, "registration", registration)
    body = actions_router.FunctionInput(
        name="clean_address",
        sourceId="addresses",
        functionName="clean_address",
        kind="mutation",
        implKind="source_procedure",
        arguments=[{"name": "sent", "type": "String"}],
    )
    await actions_router._as_column_transform(body)
    assert (body.implKind, body.kind, body.schemaName) == ("code_columns", "query", "code")
    assert body.arguments == COMMAND["arguments"]
    assert body.outputColumns == COMMAND["output_columns"]


@pytest.mark.asyncio
async def test_a_function_that_is_not_a_columns_function_is_refused_naming_it(monkeypatch):
    from provisa.api.admin import actions_router
    from provisa.api.app import state
    from provisa.api.errors import ApiError

    async def registration(_state, source_id, function):
        raise ColumnTransformRefused(function, f"source {source_id!r} has no @columns function")

    monkeypatch.setattr(state, "source_types", {"addresses": "python"}, raising=False)
    monkeypatch.setattr(columns_mod, "registration", registration)
    body = actions_router.FunctionInput(name="orders", sourceId="addresses", functionName="orders")
    with pytest.raises(ApiError) as refused:
        await actions_router._as_column_transform(body)
    assert refused.value.status_code == 422 and refused.value.code == ColumnTransformRefused.code


def test_a_source_runs_the_workers_its_mapping_names_and_one_when_it_names_none():
    from provisa.codesource.bundle import BundleRefused
    from provisa.codesource.source import config_of

    mapping = {"runtime": "python", "code": {"kind": "pypi", "package": "a", "version": "1"}}
    assert config_of("python", mapping).workers == 1
    assert config_of("python", {**mapping, "workers": 4}).workers == 4
    for wrong in (0, -1, "2", True):
        with pytest.raises(BundleRefused):
            config_of("python", {**mapping, "workers": wrong})


def test_a_prepared_source_hands_out_only_the_workers_it_runs():
    from provisa.codesource.source import Prepared

    prepared = Prepared("addresses", "h", MANIFEST, None, "acme", workers=2)  # pyright: ignore[reportArgumentType]
    with pytest.raises(ValueError, match="runs 2 workers, not 3"):
        prepared.worker(2)


@pytest.mark.parametrize("dialect", ["duckdb", "trino", "postgres"])
@pytest.mark.asyncio
async def test_a_probe_of_a_views_shape_runs_no_column_transform(dialect):
    """The boot's probe of a view's columns asks for its shape alone: typed, and no rows, so the
    function is not called (and no model call made) to answer it."""
    import sqlglot

    from provisa.mv.governed_build import _transformed_shape_sql

    async def shape():
        return [("id", "bigint"), ("street", "text")]

    sql = await _transformed_shape_sql(
        SimpleNamespace(id="view-clean"),
        SimpleNamespace(result_shape=shape),
        SimpleNamespace(dialect=dialect),
    )
    tree = sqlglot.parse_one(sql, read=dialect)
    assert [s.alias_or_name for s in tree.selects] == ["id", "street"]
    assert "FALSE" in sql.upper() and "CLEAN" not in sql.upper()


@pytest.mark.parametrize(
    ("sql", "landed"),
    [
        ("SELECT id, clean_address(raw_address) FROM suppliers", True),
        ("SELECT id, raw_address FROM suppliers", False),
    ],
)
def test_a_view_applying_a_column_transform_is_landed_declared_or_not(monkeypatch, sql, landed):
    """Its outputs are computed by Provisa when the view is built: a view applying one is built and
    stored as a materialized view is, though its steward did not declare it one."""
    from provisa.api.admin._table_ops import land_transform_view
    from provisa.api.app import state

    monkeypatch.setattr(state, "column_transforms", {"clean_address": COMMAND}, raising=False)
    view = SimpleNamespace(table_name="suppliers_clean", view_sql=sql, materialize=False)
    land_transform_view(view)
    assert view.materialize is landed


def test_a_column_transform_plan_streams_only_where_its_terminal_applies_the_transform():
    from provisa.pgwire._pipeline import streams_from_terminal

    plan = SimpleNamespace(
        two_step=None, code_write=None, scratch=(), staged=None, embedded=None, columns=object()
    )
    assert not streams_from_terminal(plan)  # e.g. COPY, Flight's ENGINE stream: buffered
    assert streams_from_terminal(plan, applies_columns=True)  # serve_stream_through_cache
