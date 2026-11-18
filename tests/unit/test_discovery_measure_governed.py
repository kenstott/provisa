# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-413 (amended 2026-11-18): the key value overlap join count runs through the governed
pipeline as the person who started the run, with only the approved-relationship check left out,
and only for that query: a candidate pair has no relationship yet. Every other rule of the role
holds on each table it reads. A pair whose tables cannot be read is left unmeasured, saying why,
and the run goes on.

The governed tests run the real role build and the real governance stage, with routing stood in
for: what reaches routing is what would be read."""

# Requirements: REQ-413

from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest

from provisa.discovery import gather, measure
from provisa.discovery.columns import UNKNOWN, ColumnFacts, TableFacts, type_groups
from provisa.discovery.direction import from_answer
from provisa.discovery.measure import Measurer
from tests.unit.test_empty_domain_access_reaches_nothing import (
    FINANCE,
    SALES,
    _built_roles,
    _tables,
)

_ROLE = {"id": "r", "capabilities": ["usage"], "domain_access": ["*"]}


def _facts(tid: int, domain: str, name: str) -> TableFacts:
    column = ColumnFacts(
        name="id",
        data_type="integer",
        description=None,
        tags=(),
        classes=(UNKNOWN,),
        sensitive=False,
        samples=(),
        is_primary_key=False,
        groups=type_groups("integer", subtype=None, array_depth=0),
        required_parameter=None,
    )
    return TableFacts(
        table_id=tid,
        domain_id=domain,
        schema_name="public",
        table_name=name,
        description=None,
        tags=(),
        role=None,
        columns=(column,),
    )


ORDERS = _facts(SALES, "sales", "orders")
LEDGER = _facts(FINANCE, "finance", "ledger")


class _Routed(Exception):
    """The statement cleared governance and reached routing; ``sql`` is what it would read."""

    def __init__(self, sql: str) -> None:
        super().__init__(sql)
        self.sql = sql


@pytest.fixture
def state(monkeypatch):
    """The real state after the real role build: two tables of two domains, no relationship."""
    import provisa.api.app as appmod

    monkeypatch.setenv("PROVISA_SINGLE_DOMAIN", "false")
    for name in ("meta_roles", "meta_resolved_at", "masking_rules"):
        monkeypatch.setattr(appmod.state._active_runtime(), name, {}, raising=False)
    built = _built_roles(monkeypatch, [dict(_ROLE)])
    monkeypatch.setattr(built, "tables", _tables(), raising=False)
    return built


@pytest.fixture
def routing():
    from provisa.pgwire import _pipeline

    async def _route(*args, **kwargs):
        raise _Routed(str(kwargs.get("governed_sql") or (args[0] if args else "")))

    with patch.object(_pipeline, "_optimize_and_route", new=AsyncMock(side_effect=_route)) as mock:
        yield mock


def _pair_sql(state) -> str:
    reader = measure.governed_reader(state, "r")
    source, target = reader.column(ORDERS, "id"), reader.column(LEDGER, "id")
    assert isinstance(source, measure.Column) and isinstance(target, measure.Column)
    return measure.join_count_sql(source, target, 500)


def _governed(routing) -> str:
    args = routing.await_args
    return (
        " ".join(str(a) for a in args.args) + " " + " ".join(str(v) for v in args.kwargs.values())
    )


async def test_the_measure_of_two_tables_with_no_relationship_is_governed_and_runs(state, routing):
    sql = _pair_sql(state)
    with pytest.raises(_Routed):
        await gather._measured_as(sql, "r")
    routing.assert_awaited_once()


async def test_the_same_query_as_a_roles_own_is_refused_by_the_relationship_check(state, routing):
    from provisa.pgwire import _pipeline

    with pytest.raises(Exception, match="no approved relationship"):
        await _pipeline._govern_and_route(_pair_sql(state), "r")
    routing.assert_not_awaited()


async def test_a_masked_column_is_measured_under_its_mask(state, routing):
    """The measure sees only what the person may see: the role's mask holds on the column read."""
    from provisa.security.masking import MaskingRule, MaskType

    state.masking_rules = {
        (FINANCE, "r"): {"id": (MaskingRule(mask_type=MaskType.constant, value=424242), "integer")}
    }
    with pytest.raises(_Routed):
        await gather._measured_as(_pair_sql(state), "r")
    assert "424242" in _governed(routing)


async def test_a_row_ruled_table_is_measured_on_the_rows_the_role_may_read(state, routing):
    """Only the relationship check is left out: the role's row rule holds on the table read."""
    from provisa.compiler.rls import RLSContext

    state.rls_contexts["r"] = RLSContext(rules={FINANCE: "id < 777777"})
    with pytest.raises(_Routed):
        await gather._measured_as(_pair_sql(state), "r")
    assert "777777" in _governed(routing)


# --- an unreadable pair --------------------------------------------------------------------------


class _Unreadable:
    """A reader whose pair query cannot be read: its source refuses it."""

    def column(self, table: TableFacts, column: str) -> measure.Column:
        return measure.Column(f'"{table.domain_id}"."{table.table_name}"', f'"{column}"')

    async def rows(self, tables, sql: str) -> list[tuple]:
        raise gather.TableUnreadable("orders, ledger", "the source has no connection")


def _proposal():
    return from_answer("semantics", (SALES, "id"), (FINANCE, "id"), "many-to-one", 0.4, "same name")


async def test_a_pair_that_cannot_be_read_is_left_unmeasured_saying_why():
    measurer = Measurer(
        _Unreadable(), {SALES: ORDERS, FINANCE: LEDGER}, decided=frozenset(), bound=500
    )
    got = await measurer.measured(_proposal())
    m = got.measure
    assert m is not None and got.score is None
    assert m.source.unmeasured == measure.UNREADABLE == m.target.unmeasured
    assert m.unreadable == "table 'orders, ledger' cannot be read: the source has no connection"
    assert m.as_dict()["unreadable"] == m.unreadable


async def test_an_unreadable_pair_leaves_the_run_done_with_that_pair_left_out():
    from provisa.discovery.runner import KeyValues, Tally, judged

    measurer = Measurer(
        _Unreadable(), {SALES: ORDERS, FINANCE: LEDGER}, decided=frozenset(), bound=500
    )
    tally = Tally(total=1, threshold=0.7)
    kept = await judged([_proposal()], [], KeyValues(measure=measurer.all, cutoff=0.3), None, tally)
    assert kept == []
    assert tally.measured == 1 and len(tally.left_out) == 1
    left = tally.left_out[0]
    assert left.proposal.measure is not None and left.proposal.measure.unreadable


def test_only_the_discovery_measure_reads_outside_the_relationship_check():
    """The measure's call is the one discovery read stated as the product's; the others are a
    role's own statements."""
    import inspect

    assert "product_statement=True" in inspect.getsource(gather._measured_as)
    assert "product_statement" not in inspect.getsource(gather._governed_as)
