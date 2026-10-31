# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A GraphQL relationship into a row-level table is the join that supplies its key (REQ-1915).

The bench's ``federated_join`` nests ``orderDoc`` under ``pb__orders``; on the pg engine
``order_docs`` is replicated row by row. A nested field compiled to a correlated subquery names the
table outside any JOIN, so the planner refused the read the SQL surface accepts, and key pushdown
(which reads only JOINs) would have fetched nothing for it. A to-one field whose target is
row-level on the bound engine compiles to the LEFT JOIN the rule binds. A to-many field stays a
list aggregate and is refused: joined rows collapse equal elements of a list."""

# Requirements: REQ-1915, REQ-1865

from __future__ import annotations

import dataclasses
import functools
import importlib.util
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest
from graphql import parse

from provisa.api.errors import RowLevelFilterRequired
from provisa.compiler.params import embed_params_comment
from provisa.compiler.sql_gen import compile_query
from provisa.compiler.sql_rewrite import make_semantic_sql
from provisa.executor.serialize import serialize_rows
from provisa.pgwire import _pipeline

_REPO = Path(__file__).resolve().parents[2]

_FEDERATED_JOIN = """
    query($lo: Int!, $hi: Int!) {
      pb__orders(where: {orderId: {gte: $lo, lte: $hi}}) {
        orderId
        amount
        orderEvents { eventType }
        orderDoc { status }
      }
    }
"""
_VARIABLES = {"lo": 1, "hi": 1000}


@functools.cache
def _perf_context():
    spec = importlib.util.spec_from_file_location(
        "graphql_golden", _REPO / "scripts" / "graphql_golden.py"
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules["graphql_golden"] = module  # its dataclass reads its own module
    spec.loader.exec_module(module)
    return module.model(_REPO / "demo" / "named" / "perf" / "config.yaml")[1]


def _table_id(ctx, name: str) -> int:
    return next(t.table_id for t in ctx.tables.values() if t.table_name.endswith(name))


def _row_level(*names: str):
    """The perf model's context with ``names`` row-level on the bound engine."""
    ctx = _perf_context()
    return dataclasses.replace(
        ctx, row_level_tables=frozenset(_table_id(ctx, name) for name in names)
    )


def _compiled(ctx, text: str = _FEDERATED_JOIN, variables: dict | None = None):
    (compiled,) = compile_query(parse(text), ctx, _VARIABLES if variables is None else variables)
    return compiled


def _semantic(ctx, compiled) -> str:
    return make_semantic_sql(embed_params_comment(compiled.sql, compiled.params), ctx)


def _state(ctx, *names: str) -> SimpleNamespace:
    """The pipeline's view of the same tables: flagged, on an engine with no connector for them."""
    tables = [
        {
            "id": _table_id(ctx, name),
            "source_id": "docs",
            "schema_name": "s",
            "table_name": name,
            "alias": None,
            "row_materialize": True,
            "columns": [
                {
                    "column_name": "order_id",
                    "data_type": "integer",
                    "is_primary_key": True,
                    "native_filter_type": None,
                    "embedding": False,
                }
            ],
        }
        for name in names
    ]
    return SimpleNamespace(
        tables=tables,
        source_types={"docs": "mongodb"},
        federation_engine=SimpleNamespace(connectors={}),
    )


def _decide(ctx, names: tuple[str, ...], text: str = _FEDERATED_JOIN) -> tuple:
    sql = _semantic(ctx, _compiled(ctx, text))
    return _pipeline._pk_bounds(_pipeline._pk_bounds_inputs(sql, _state(ctx, *names)), None)


def test_the_bench_s_federated_join_is_bound_by_its_join():
    assert _decide(_row_level("order_docs"), ("order_docs",)) == ()


def test_the_table_is_the_target_of_the_join_the_rule_names():
    ctx = _row_level("order_docs")
    sql = _semantic(ctx, _compiled(ctx))
    assert 'LEFT JOIN "perf_bench"."order_docs"' in sql
    assert "(SELECT json_object(KEY 'status'" not in sql
    assert "json_agg" in sql  # the to-many field beside it keeps its list form


def test_a_table_the_engine_attaches_keeps_its_one_json_value():
    ctx = _row_level()
    sql = _semantic(ctx, _compiled(ctx))
    assert "LEFT JOIN" not in sql


def test_the_answer_is_the_same_object_and_null_for_a_missing_document():
    ctx = _row_level("order_docs")
    compiled = _compiled(ctx)
    names = [(c.nested_in, c.field_name) for c in compiled.columns]
    status = names.index(("orderDoc", "status"))
    width = len(names)

    def row(order_id, doc_status):
        values = [order_id, 9.5, "[]"] + [None] * (width - 3)
        values[status] = doc_status
        return tuple(values)

    answer = serialize_rows([row(1, "open"), row(2, None)], compiled.columns, compiled.root_field)[
        "data"
    ]["pb__orders"]
    assert [r["orderDoc"] for r in answer] == [{"status": "open"}, None]
    assert [r["orderId"] for r in answer] == [1, 2]


def test_a_to_many_field_into_a_row_level_table_stays_refused():
    """Joined rows collapse equal list elements (``serialize._serialize_with_one_to_many``), so a
    list is not given that form to pass the rule."""
    with pytest.raises(RowLevelFilterRequired) as refused:
        _decide(_row_level("order_events"), ("order_events",))
    assert refused.value.params == {"table": "order_events"}


def test_the_row_level_table_is_still_scoped_by_its_row_rule():
    from provisa.compiler.stage2 import GovernanceContext, apply_governance

    ctx = _row_level("order_docs")
    docs = _table_id(ctx, "order_docs")
    gov = GovernanceContext(
        rls_rules={docs: "status = 'open'"},
        table_map={"order_docs": docs, "perf_bench.order_docs": docs},
    )
    governed = apply_governance(_semantic(ctx, _compiled(ctx)), gov)
    assert "'open'" in governed


def test_the_join_passes_the_relationship_guard():
    from provisa.compiler.sql_validator import approved_joins

    ctx = _row_level("order_docs")
    orders, docs = _table_id(ctx, "orders"), _table_id(ctx, "order_docs")
    assert any({orders, docs} == {j[0], j[1]} for j in approved_joins(ctx))


def test_a_chain_of_to_one_fields_is_joined_down_to_the_row_level_table():
    from provisa.compiler.sql_where import _joins_row_level_to_one

    target = SimpleNamespace(table_id=2, type_name="Doc")
    step = SimpleNamespace(table_id=1, type_name="Mid")
    to_doc = SimpleNamespace(cardinality="many-to-one", target=target)
    to_mid = SimpleNamespace(cardinality="many-to-one", target=step)
    to_mids = SimpleNamespace(cardinality="one-to-many", target=step)
    ctx = SimpleNamespace(row_level_tables=frozenset({2}), joins={("Mid", "doc"): to_doc})
    field = parse("{ mid { doc { status } } }").definitions[0].selection_set.selections[0]
    assert _joins_row_level_to_one(field, to_mid, ctx)
    assert not _joins_row_level_to_one(field, to_mids, ctx)
