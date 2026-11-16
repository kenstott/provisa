# Copyright (c) 2026 Kenneth Stott
# Canary: 8a10ae6b-7998-4490-8afb-74d672d6170d
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1986, A PARAMETERIZED TABLE STAYS A TABLE; ITS LIMITS ARE STATED IN ITS METADATA: every
part of the product that counts, profiles, samples or scans a table asks the table's metadata
first and takes a stated path for a table that requires a parameter. None finds out by reading.

One test over every such reader. The model holds two tables: ``get_order_by_id``, whose metadata
states the required parameter ``orderId``, and ``orders``, an ordinary table. Every way a reader
reaches a source is replaced by one that records the call and raises. For the parameterized
table the reader must take its stated path with no call recorded; for the ordinary table the
same reader must reach the source, which shows the reader does read and the replacement bites.
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any

import pytest

from provisa.core.parameters import RequiresParameters, parameter_facts
from provisa.profiler.run import PROFILE_ROLE

PARAMETERIZED, ORDINARY = 1, 2
REASON = "cannot be counted or profiled: it requires orderId"


class SourceCalled(Exception):
    """A reader reached the source."""


class _Source:
    def __init__(self) -> None:
        self.calls: list[str] = []

    def called(self, what: str) -> Any:
        async def _call(*_args: Any, **_kwargs: Any) -> Any:
            self.calls.append(what)
            raise SourceCalled(what)

        return _call


_REGISTERED = {
    PARAMETERIZED: {
        "id": PARAMETERIZED,
        "table_name": "get_order_by_id",
        "source_id": "petstore",
        "schema_name": "petstore",
        "domain_id": "shop",
        "columns": [
            {"column_name": "id", "data_type": "integer", "native_filter_type": None},
            {"column_name": "total", "data_type": "integer", "native_filter_type": None},
            {
                "column_name": "_nf_orderId",
                "data_type": "integer",
                "native_filter_type": "path_param",
                "native_filter_required": True,
                "required_by_steward": False,
            },
        ],
    },
    ORDINARY: {
        "id": ORDINARY,
        "table_name": "orders",
        "source_id": "sales",
        "schema_name": "public",
        "domain_id": "shop",
        "columns": [
            {"column_name": "id", "data_type": "integer", "native_filter_type": None},
            {"column_name": "total", "data_type": "integer", "native_filter_type": None},
        ],
    },
}


def _ns(**fields: Any) -> Any:
    """A stand-in for a model object a reader is handed, holding only what it reads of it."""
    return SimpleNamespace(**fields)


def _meta(reg: dict) -> Any:
    return _ns(
        table_id=reg["id"],
        type_name=reg["table_name"].title(),
        field_name=reg["table_name"],
        source_id=reg["source_id"],
        schema_name=reg["schema_name"],
        table_name=reg["table_name"],
        domain_id=reg["domain_id"],
        catalog_name=None,
        alias=None,
    )


class _NoStore:
    """The model store of a reader that must record nothing."""

    def __getattr__(self, name: str) -> Any:
        raise AssertionError(f"the model store was touched: {name}")


@pytest.fixture
def source(monkeypatch) -> _Source:
    """Every way a reader reaches a source, each recording its call and raising."""
    from provisa.dq import runner
    from provisa.pgwire import _pipeline
    from provisa.profiler import run
    from provisa.synthetic import run as synthetic_run

    src = _Source()
    monkeypatch.setattr(run, "_governed", src.called("profiler governed read"))
    monkeypatch.setattr(run, "_route", src.called("profiler routed read"))
    monkeypatch.setattr(_pipeline, "_govern_and_route", src.called("pipeline"))
    monkeypatch.setattr(_pipeline, "_execute_plan", src.called("pipeline execute"))
    monkeypatch.setattr(runner, "_run_worker", src.called("data quality checker"))
    monkeypatch.setattr(synthetic_run, "_land_sample", src.called("closeness sample"))
    return src


@pytest.fixture
def state(monkeypatch) -> SimpleNamespace:
    from provisa.compiler import sql_rewrite

    monkeypatch.setattr(sql_rewrite, "semantic_table_name", lambda meta: meta.table_name)
    metas = {reg["table_name"]: _meta(reg) for reg in _REGISTERED.values()}
    ctx = SimpleNamespace(
        tables=metas,
        parameters={
            tid: tuple(parameter_facts(reg["columns"])) for tid, reg in _REGISTERED.items()
        },
        physical_to_sql={
            (tid, c["column_name"]): c["column_name"]
            for tid, reg in _REGISTERED.items()
            for c in reg["columns"]
            if c["native_filter_type"] is None
        },
        joins={},
        pk_columns={},
        unique_constraints={},
    )
    return SimpleNamespace(
        contexts={PROFILE_ROLE: ctx},
        view_context=ctx,
        tables=list(_REGISTERED.values()),
        graphql_remote_sources={},
        schema_build_cache={
            "column_types": {
                tid: [
                    SimpleNamespace(column_name=c["column_name"], data_type=c["data_type"])
                    for c in reg["columns"]
                    if c["native_filter_type"] is None
                ]
                for tid, reg in _REGISTERED.items()
            }
        },
        model_db=_NoStore(),
    )


def _dataset_table(table_id: int) -> Any:
    reg = _REGISTERED[table_id]
    return _ns(
        table_id=table_id,
        name=reg["table_name"],
        pgwire_name=f"shop.{reg['table_name']}",
        columns=(("id", "integer", True), ("total", "integer", False)),
    )


# -- the readers ----------------------------------------------------------------------------------
# Each takes the model's state and a table id, and returns what the reader answered. A reader asked
# for one table raises RequiresParameters for the parameterized one; a reader that sweeps many
# tables returns a result that says the table was not read, and why.


class _Conn:
    async def __aenter__(self) -> Any:
        return self

    async def __aexit__(self, *exc: Any) -> None:
        return None


def _profiler_store(patch: pytest.MonkeyPatch, state: Any, table_id: int) -> None:
    """The model store a profile of the ORDINARY table reads its tags and decisions from and
    records its run in. The parameterized table keeps the store that refuses every touch: its
    refusal comes before a run exists, and none is recorded."""
    from provisa.profiler import classifications, constraints, run

    if table_id == PARAMETERIZED:
        return

    async def _nothing(*_a: Any, **_k: Any) -> Any:
        return []

    async def _no_tags(*_a: Any, **_k: Any) -> Any:
        return {}

    async def _none(*_a: Any, **_k: Any) -> Any:
        return None

    patch.setattr(state, "model_db", _ns(acquire=lambda: _Conn()))
    patch.setattr(run, "column_tags", _no_tags)
    patch.setattr(run, "declared_watermark", _none)
    patch.setattr(run, "write_results", _none)
    patch.setattr(constraints, "accepted_constraints", _nothing)
    patch.setattr(constraints, "decisions", _nothing)
    patch.setattr(classifications, "decisions", _nothing)
    patch.setattr(classifications, "enabled_kinds", _nothing)


async def _profile_table(state: Any, table_id: int) -> Any:
    from provisa.profiler import run

    with pytest.MonkeyPatch.context() as patch:
        _profiler_store(patch, state, table_id)
        return await run.profile_table(
            state,
            table_id=table_id,
            table_name=_REGISTERED[table_id]["table_name"],
            settings=_ns(sample_above_cells=None),
        )


async def _classify_table(state: Any, table_id: int) -> Any:
    from provisa.profiler import run

    with pytest.MonkeyPatch.context() as patch:
        _profiler_store(patch, state, table_id)
        return await run.classify_table(
            state,
            table_id=table_id,
            table_name=_REGISTERED[table_id]["table_name"],
            values_per_column=10,
            sample_above_cells=None,
        )


async def _resolve_target(state: Any, table_id: int) -> Any:
    """What discovery's sample read, the fakes and the synthetic report resolve before reading."""
    from provisa.profiler import run

    target = run.resolve_target(state, table_id, _REGISTERED[table_id]["table_name"], {})
    return await run._governed(f"SELECT COUNT(*) FROM {target.pgwire_name}")


async def _profiler_run(state: Any, table_id: int) -> Any:
    """A profiler's run over its members: a sweep."""
    from provisa.profiler import run, source as profiler_source

    reg = _REGISTERED[table_id]

    class _Sources(_Conn):
        async def execute_core(self, _statement: Any) -> Any:
            return _ns(fetchone=lambda: (profiler_source.PROFILER_SOURCE_TYPE, {}))

    async def _members(_conn: Any, _source_id: str) -> list[dict]:
        return [{"id": reg["id"], "table_name": reg["table_name"]}]

    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(state, "model_db", _ns(acquire=lambda: _Sources()))
        patch.setattr(profiler_source, "members", _members)
        patch.setattr(profiler_source, "profiler_settings", lambda sid, mapping: None)
        patch.setattr(run, "profile_table", run._governed)
        return await profiler_source.run_source(state, "profiler")


async def _fake_leak_rate(state: Any, table_id: int) -> Any:
    from provisa.fakes import adequacy

    col = {
        "column_name": "id",
        "data_type": "integer",
        "fake": "normal(mean=1, sd=2)",
        "fake_stable": False,
        "fake_stable_version": None,
    }
    return await adequacy._leak(state, _REGISTERED[table_id], col, 0.1)


async def _fake_measurement(state: Any, table_id: int) -> Any:
    from provisa.core.request_context import reset_current_org, set_current_org
    from provisa.fakes import measured
    from provisa.fakes.kinds import Ordered

    token = set_current_org("default")
    try:
        return await measured._measure_table(
            state,
            None,
            "default",
            _REGISTERED[table_id],
            {"total": (Ordered("greater_than", "id"), "integer")},
        )
    finally:
        reset_current_org(token)


async def _synthetic_measure(state: Any, table_id: int) -> Any:
    from provisa.synthetic import run

    return await run._measure(state, _dataset_table(table_id), "total")


async def _synthetic_difference(state: Any, table_id: int) -> Any:
    from provisa.synthetic import run

    return await run._measure_difference(state, _dataset_table(table_id), "total", "id")


async def _synthetic_condition(state: Any, table_id: int) -> Any:
    """A measured conditional fan-out whose child is the table."""
    from provisa.synthetic import run

    edge = _ns(
        relationship="r",
        parent_id=ORDINARY,
        child_id=table_id,
        parent_column="id",
        child_column="id",
    )
    by_id: dict[int, Any] = {
        ORDINARY: _dataset_table(ORDINARY),
        table_id: _dataset_table(table_id),
    }
    return await run._measure_condition(
        state, by_id, [edge], _ns(relationship="r", condition="total > 1")
    )


async def _synthetic_private(state: Any, table_id: int) -> Any:
    from provisa.synthetic import private_run, run

    with pytest.MonkeyPatch.context() as patch:

        async def _private_tables(_tables: Any, _edges: Any, *, governed: Any, **_kw: Any) -> Any:
            return await governed("SELECT COUNT(*)")

        patch.setattr(private_run, "private_tables", _private_tables)
        return await run._private_tables(
            state,
            [_dataset_table(table_id)],
            [],
            _ns(private_epsilon=1.0, seed=1, fanout_conditions=()),
        )


def _planned(table_id: int) -> Any:
    reg = _REGISTERED[table_id]
    dependence = _ns(
        nodes={"id": _ns(kind="number"), "total": _ns(kind="number")},
        copula=["id", "total"],
        shrink=0.0,
        kept_own=[],
    )
    table = _dataset_table(table_id)
    table.profile = _ns(dependence=_ns(spearman={("id", "total"): 0.5}))
    return _ns(table=table, plan=_ns(name=reg["table_name"], columns=[], dependence=dependence))


async def _synthetic_closeness(state: Any, table_id: int) -> Any:
    """The closeness check's real sample: a sweep over the dataset's tables."""
    from provisa.synthetic import generate, run

    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(generate, "distance_columns", lambda plan: ["total"])
        patch.setattr(run, "sample_name", lambda reg: "sample")
        _closes, _real, unchecked = await run._closeness_of(
            state,
            _ns(closeness_threshold=0.1, closeness_draws=1, store_schema="s"),
            [_planned(table_id)],
            _REGISTERED,
            {},
            [],
        )
    return unchecked


async def _synthetic_dependence(state: Any, table_id: int) -> Any:
    """The report's rank correlations read beside the generated rows: a sweep."""
    from provisa.synthetic import run

    return await run._dependence_entries(state, [_planned(table_id)], private=False)


async def _first_rows(state: Any, table_id: int) -> Any:
    """The table's first rows and its profile sample (the sensitive-value scan reads them too)."""
    from provisa.api.admin import table_read

    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(table_read, "_in_service", lambda st, tid: _REGISTERED[tid])
        patch.setattr(table_read, "table_sql_ref", lambda table: table["table_name"])
        return await table_read.read_as_caller(state, table_id, "analyst", limit=20)


async def _viewer_page(state: Any, table_id: int) -> Any:
    """A page of the table viewer that gives the parameter no value."""
    from provisa.api.admin import table_read
    from provisa.api.admin.viewer_statement import ViewerRequest

    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(table_read, "_in_service", lambda st, tid: _REGISTERED[tid])
        patch.setattr(table_read, "table_sql_ref", lambda table: table["table_name"])
        return await table_read.read_as_caller(
            state, table_id, "analyst", request=ViewerRequest(pageSize=10)
        )


async def _dq_scan(state: Any, table_id: int) -> Any:
    """A data-quality contract aimed at the table: resolved, then handed to the checker."""
    from provisa.dq import contract, runner

    reg = _REGISTERED[table_id]
    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(contract, "meta_dataset", lambda meta: f"provisa/shop/{meta.table_name}")
        target = contract.resolve_contract_target(
            f"provisa/shop/{reg['table_name']}", contract.model_contexts(state)
        )
    return await runner._run_worker({"target": target.table_name}, timeout=1.0)


def _label_map() -> Any:
    from provisa.gql.label_map import NodeMapping

    def node(table_id: int) -> NodeMapping:
        reg = _REGISTERED[table_id]
        params = tuple(parameter_facts(reg["columns"]))
        return NodeMapping(
            label=reg["table_name"],
            type_name=reg["table_name"],
            domain_label="Shop",
            table_label=reg["table_name"],
            table_id=table_id,
            source_id=reg["source_id"],
            id_column="id",
            pk_columns=["id"],
            catalog_name="",
            schema_name=reg["schema_name"],
            table_name=reg["table_name"],
            properties={"id": "id"},
            native_filter_columns={f.column: f.type or "text" for f in params},
            parameters=params,
            domain_id=reg["domain_id"],
        )

    return SimpleNamespace(
        nodes={reg["table_name"]: node(tid) for tid, reg in _REGISTERED.items()},
        relationships={},
    )


async def _bolt_counts(state: Any, table_id: int) -> Any:
    """The Neo4j Browser's totals over Bolt: a sweep over every label."""
    from provisa.bolt import session

    label = _REGISTERED[table_id]["table_name"]
    counted: list[str] = []

    async def _execute(cypher: str, params: Any, role_id: str, include_ops: bool) -> Any:
        counted.append(cypher)
        if f":{label})" in cypher:
            from provisa.profiler import run

            await run._governed(cypher)
        return ["cnt"], [[1]], None

    from provisa.core.statement_warnings import collecting

    with pytest.MonkeyPatch.context() as patch, collecting():
        patch.setattr(session, "_bolt_label_map", lambda *a: _label_map())
        patch.setattr(session, "_execute_cypher", _execute)
        await session._graph_counts(None, "analyst", False, None)
    return counted


async def _graph_counts(state: Any, table_id: int) -> Any:
    """The graph browser's counts: a sweep over every label."""
    from provisa.api.rest import cypher_router
    from provisa.profiler import run

    label = _REGISTERED[table_id]["table_name"]
    labels, _rels = cypher_router._countable_labels(_label_map(), set(), lambda domain: True)
    if label in labels:
        await run._governed(f"MATCH (n:{label}) RETURN count(n)")
    return labels


def _refused(answer: Any) -> None:
    raise AssertionError(f"the reader answered {answer!r} where it must refuse")


def _not_read(found: Any) -> Any:
    def check(answer: Any) -> None:
        assert REASON in str(found(answer)), answer

    return check


# (reader, what its answer for the parameterized table must say). A reader asked for the one table
# raises RequiresParameters (check: _refused, never reached); a sweep reports the table.
READERS = [
    pytest.param(_profile_table, _refused, id="profile this table"),
    pytest.param(_classify_table, _refused, id="classify this table"),
    pytest.param(_resolve_target, _refused, id="the profiler's target of a whole read"),
    pytest.param(
        _profiler_run,
        lambda out: (
            _not_read(lambda o: o[0]["not_read"])(out),
            pytest.fail("a run or an error was recorded")
            if out[0]["run_id"] is not None or out[0]["error"] is not None
            else None,
        ),
        id="a profiler's run over its members",
    ),
    pytest.param(_fake_leak_rate, _not_read(lambda r: r.detail), id="fake adequacy sample"),
    pytest.param(_fake_measurement, _not_read(lambda m: m["total"].refused), id="fake measurement"),
    pytest.param(_synthetic_measure, _refused, id="synthetic: measure a column"),
    pytest.param(_synthetic_difference, _refused, id="synthetic: measure a difference"),
    pytest.param(_synthetic_condition, _refused, id="synthetic: measured fan-out condition"),
    pytest.param(_synthetic_private, _refused, id="synthetic: private statistics"),
    pytest.param(
        _synthetic_closeness, _not_read(lambda rows: rows[0]["note"]), id="synthetic: closeness"
    ),
    pytest.param(
        _synthetic_dependence,
        _not_read(lambda rows: rows[0]["note"]),
        id="synthetic: dependence report",
    ),
    pytest.param(_first_rows, _refused, id="first rows, profile sample, sensitive scan"),
    pytest.param(_viewer_page, _refused, id="viewer page without a value"),
    pytest.param(_dq_scan, _refused, id="data quality contract"),
    pytest.param(
        _bolt_counts,
        lambda counted: [c for c in counted if "get_order_by_id" in c] == [] or _refused(counted),
        id="bolt graph counts",
    ),
    pytest.param(
        _graph_counts,
        lambda labels: "get_order_by_id" not in labels or _refused(labels),
        id="graph counts",
    ),
]


@pytest.mark.parametrize(("reader", "says"), READERS)
async def test_a_reader_asks_the_metadata_and_never_reads_a_table_that_requires_a_parameter(
    reader, says, state, source
):
    from provisa.dq.contract import ContractError

    try:
        answer = await reader(state, PARAMETERIZED)
    except RequiresParameters as refusal:
        assert refusal.code == "table.requires_parameters"
        assert refusal.params == {"table": "get_order_by_id", "parameters": "orderId"}
        assert REASON in str(refusal)
    except ContractError as refusal:  # a contract's refusals are its own kind, with the one reason
        assert REASON in str(refusal)
    else:
        says(answer)
    assert source.calls == [], "the source was called for a table that requires a parameter"


@pytest.mark.parametrize(("reader", "says"), READERS)
async def test_the_same_reader_reads_an_ordinary_table(reader, says, state, source):
    """The control: each reader does reach the source for a table that requires nothing, so the
    test above is not passing because the reader never reads."""
    try:
        await reader(state, ORDINARY)
    except Exception:  # noqa: BLE001 - the read raised, or its reader reported it its own way
        pass
    assert source.calls, "the reader never reached the source for an ordinary table"


def test_a_viewer_page_that_gives_the_parameter_its_value_is_not_refused():
    from provisa.api.admin.table_read import refuse_unparameterized
    from provisa.api.admin.viewer_statement import ViewerParam, ViewerRequest

    page = ViewerRequest(params=[ViewerParam(column="_nf_orderId", value=7)])
    refuse_unparameterized(_REGISTERED[PARAMETERIZED], page)
    refuse_unparameterized(_REGISTERED[ORDINARY])


def test_graph_counts_leave_out_only_a_label_whose_parameter_is_required():
    """A label with a parameter column that is optional is counted as any other."""
    from provisa.api.rest import cypher_router

    label_map = _label_map()
    optional = label_map.nodes["get_order_by_id"]
    facts = parameter_facts(
        [
            {
                "column_name": "_nf_status",
                "data_type": "text",
                "native_filter_type": "query_param",
                "native_filter_required": False,
                "required_by_steward": False,
            }
        ]
    )
    label_map.nodes["find_pets_by_status"] = type(optional)(
        **{
            **{f: getattr(optional, f) for f in optional.__dataclass_fields__},
            "label": "find_pets_by_status",
            "parameters": tuple(facts),
            "native_filter_columns": {"_nf_status": "text"},
        }
    )
    labels, _rels = cypher_router._countable_labels(label_map, set(), lambda domain: True)
    assert labels == ["orders", "find_pets_by_status"]
