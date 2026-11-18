# Copyright (c) 2026 Kenneth Stott
# Canary: 3d71c9a4-6e2b-4f85-b0a3-9c5e1f7d2b68
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-413: why a run over the files demo's drop (complaints, building_permits, inspections,
budget_2026__budget) listed budget_2026__budget.neighborhood -> building_permits.neighborhood and
never complaints.neighborhood -> building_permits.neighborhood.

The tables are as a files source registers the drop (types as DuckDB reads the files, no
descriptions, every class unknown, the first sample values of each column), with the values the
generator writes (demo/named/files/generate.py): six neighborhoods, each repeated in every table.

Under the window packing the pair was dropped at the model: an answer that omitted it left it in
no row of the run. Now (REQ-413, amended 2026-11-18) code lists the table pair complaints x
building_permits in exactly one call; an answer naming the column pair scores it like the budget
pair, and an answer to the table pair that omits the column pair scores it zero and lists it.
"""

# Requirements: REQ-413

from __future__ import annotations

from typing import Any

from provisa.discovery.columns import UNKNOWN, ColumnFacts, TableFacts, comparable, type_groups
from provisa.discovery.context import CONTAINED_AT, SHARED_CEILING, Scoring
from provisa.discovery.direction import Known, Proposal, Sampled, settle
from provisa.discovery.measure import Column, Measurer, Unmeasured
from provisa.discovery.proposals import ACCEPTABLE, BELOW_THRESHOLD, Recorded
from provisa.discovery.runner import KeyValues, Tally, execute
from provisa.discovery.windows import plan
from tests.unit.discovery_answers import answer, column_pair
from tests.unit.discovery_engine_reader import EngineReader
from provisa.llm.client import Completion
from provisa.llm.limits import DEFAULT_LIMITS

CUTOFF = 0.2
SAMPLE_SIZE = 10_000

COMPLAINTS, PERMITS, INSPECTIONS, BUDGET = 1, 2, 3, 4

# The rows of each neighborhood the generator writes (seed 2032).
_NEIGHBORHOODS = {
    COMPLAINTS: {
        "Cedar Heights": 58,
        "Eastgate": 147,
        "Harbor Point": 155,
        "Millbrook": 78,
        "Old Town": 50,
        "Riverside": 112,
    },
    PERMITS: {
        "Cedar Heights": 8,
        "Eastgate": 32,
        "Harbor Point": 40,
        "Millbrook": 16,
        "Old Town": 8,
        "Riverside": 24,
    },
    BUDGET: dict.fromkeys(
        ("Cedar Heights", "Eastgate", "Harbor Point", "Millbrook", "Old Town", "Riverside"), 5
    ),
}


def _col(name: str, data_type: str, samples: tuple[str, ...] = (), pk: bool = False):
    return ColumnFacts(
        name=name,
        data_type=data_type,
        description=None,
        tags=(),
        classes=(UNKNOWN,),
        sensitive=False,
        samples=samples,
        is_primary_key=pk,
        groups=type_groups(data_type, subtype=None, array_depth=0),
    )


def _tbl(tid: int, name: str, *columns: ColumnFacts) -> TableFacts:
    return TableFacts(
        table_id=tid,
        domain_id="pet_store",
        schema_name="drop",
        table_name=name,
        description=None,
        tags=(),
        role=None,
        columns=columns,
    )


TABLES = [
    _tbl(
        COMPLAINTS,
        "complaints",
        _col("complaint_id", "bigint", ("10001", "10002", "10003"), pk=True),
        _col("opened_at", "timestamp"),
        _col("status", "varchar", ("open", "closed")),
        _col("closed_at", "timestamp"),
        _col("category", "varchar", ("Blocked sidewalk", "Dust", "Noise", "Trash")),
        _col("neighborhood", "varchar", ("Harbor Point", "Old Town", "Eastgate", "Riverside")),
        _col("address", "varchar", ("238 Pier Ave", "33 Mill Ln", "86 Gate St")),
    ),
    _tbl(
        PERMITS,
        "building_permits",
        _col("permit_id", "varchar", ("BP-0001", "BP-0002", "BP-0003"), pk=True),
        _col("address", "varchar", ("149 Bank St", "59 River Rd")),
        # The first rows read are all of one neighborhood: one sample value.
        _col("neighborhood", "varchar", ("Riverside",)),
        _col("permit_type", "varchar", ("Renovation", "Addition")),
        _col("issued_on", "date"),
        _col("expires_on", "date"),
        _col("status", "varchar", ("active", "expired")),
        _col("valuation", "bigint", ("143000", "597000")),
    ),
    _tbl(
        INSPECTIONS,
        "inspections",
        _col("inspection_id", "bigint", ("1", "2", "3"), pk=True),
        _col("permit_id", "varchar", ("BP-0096", "BP-0081")),
        _col("inspected_on", "date"),
        _col("result", "varchar", ("passed", "failed")),
    ),
    _tbl(
        BUDGET,
        "budget_2026__budget",
        _col("neighborhood", "varchar", ("Riverside", "Old Town", "Millbrook", "Harbor Point")),
        _col("department", "varchar", ("Public Works", "Parks", "Planning")),
        _col("amount", "double"),
    ),
]
BY_ID = {t.table_id: t for t in TABLES}
IDS = set(BY_ID)

THE_PAIR = frozenset({(COMPLAINTS, "neighborhood"), (PERMITS, "neighborhood")})
BUDGET_PAIR = frozenset({(BUDGET, "neighborhood"), (PERMITS, "neighborhood")})


def _item(src: tuple[int, str], tgt: tuple[int, str], confidence: float) -> dict[str, Any]:
    return column_pair(
        (BY_ID[src[0]].table_name, src[1]), (BY_ID[tgt[0]].table_name, tgt[1]), confidence
    )


#: The answer the maintainer's run showed (2026-11-18): the two pairs it listed.
OBSERVED = [
    _item((INSPECTIONS, "permit_id"), (PERMITS, "permit_id"), 0.93),
    _item((BUDGET, "neighborhood"), (PERMITS, "neighborhood"), 0.45),
]


def _values(tid: int, column: str) -> list[str]:
    """A column's values as the generator writes them: each neighborhood repeated as counted, any
    other column a key of its table (each value once)."""
    if column == "neighborhood":
        return [n for n, times in _NEIGHBORHOODS[tid].items() for _ in range(times)]
    return [f"{BY_ID[tid].table_name}-{i}" for i in range(100)]


class _Reader(EngineReader):
    """The measure's engine holding every column a pair can name; records each column read."""

    def __init__(self) -> None:
        super().__init__(
            {
                (t.table_name, c.name): _values(t.table_id, c.name)
                for t in TABLES
                for c in t.columns
                if c.name == "permit_id"
                or (c.name == "neighborhood" and t.table_id in _NEIGHBORHOODS)
            }
        )
        self.read: list[tuple[int, str]] = []

    def column(self, table: TableFacts, column: str) -> Column | Unmeasured:
        self.read.append((table.table_id, column))
        return super().column(table, column)


class _Watcher:
    async def stop_asked(self) -> bool:
        return False

    async def returned(self, tally: Tally, proposals: list[Proposal]) -> None:
        return None


async def _direct(fn, *args):
    return fn(*args)


async def _no_sample(table: TableFacts, column: str) -> Sampled:
    raise AssertionError(f"no direction is left to a sample here: {table.table_name}.{column}")


async def _run(given: list[dict[str, Any]]) -> tuple[list[Recorded], _Reader, list[str]]:
    """A run over the four tables with Key value overlap chosen, the model answering each listed
    table pair with the column pairs of ``given`` naming its tables, or "none": every row the run
    keeps (as :func:`provisa.discovery.run.perform` records them), the measure's reads, and the
    prompts."""
    the_plan = plan(TABLES, None, *DEFAULT_LIMITS)
    reader = _Reader()
    measurer = Measurer(
        reader,
        BY_ID,
        decided=set(),
        bound=SAMPLE_SIZE,
        scoring=Scoring(CONTAINED_AT, SHARED_CEILING),
    )
    prompts: list[str] = []

    def ask(prompt: str, max_tokens: int) -> Completion:
        prompts.append(prompt)
        return Completion(answer(prompt, given), "acme", "m", None, False)

    tally, found = await execute(
        the_plan,
        DEFAULT_LIMITS[1],
        ask,
        _Watcher(),
        threshold=0.7,
        key_values=KeyValues(measurer.all, CUTOFF),
        to_thread=_direct,
    )
    outcome = await settle(found, BY_ID, IDS, IDS, Known(), _no_sample)
    rows = [
        *(Recorded(p.kind, None, p) for p in outcome.kept),
        *tally.left_out,
        *(Recorded(o.proposal.kind, o.reason, o.proposal) for o in outcome.left_out),
    ]
    return rows, reader, prompts


def _row(rows: list[Recorded], pair: frozenset) -> Recorded | None:
    held = [r for r in rows if r.proposal is not None and r.proposal.pair == pair]
    assert len(held) <= 1
    return held[0] if held else None


def test_the_table_pair_is_listed_in_exactly_one_call():
    complaints = BY_ID[COMPLAINTS].column("neighborhood")
    permits = BY_ID[PERMITS].column("neighborhood")
    assert complaints is not None and permits is not None
    assert comparable(complaints, permits, "text")
    the_plan = plan(TABLES, None, *DEFAULT_LIMITS)
    holding = [
        (call, p)
        for call in the_plan.calls
        for p in call.pairs
        if {p.source.table_id, p.target.table_id} == {COMPLAINTS, PERMITS}
    ]
    assert len(holding) == 1
    call, pair = holding[0]
    assert THE_PAIR in {
        frozenset({(pair.source.table_id, a.name), (pair.target.table_id, b.name)})
        for a, b in pair.column_pairs()
    }
    prompt = call.prompt(0.7)
    assert '"source":"building_permits","target":"complaints"' in prompt


async def test_an_answer_omitting_the_column_pair_scores_it_zero_and_lists_it():
    """The red test of J1462, flipped: the model answers the table pair without naming the
    column pair, which is scored zero and listed below the threshold, not left in no row."""
    rows, reader, prompts = await _run(OBSERVED)
    assert any('"name":"complaints"' in p and '"name":"building_permits"' in p for p in prompts)
    ours = _row(rows, THE_PAIR)
    assert ours is not None and ours.proposal is not None
    assert ours.left_out == BELOW_THRESHOLD
    assert ours.proposal.confidence == 0.0
    assert (COMPLAINTS, "neighborhood") not in reader.read  # zero is under the cutoff
    budget = _row(rows, BUDGET_PAIR)
    assert budget is not None and budget.proposal is not None
    assert budget.left_out == BELOW_THRESHOLD
    assert budget.proposal.cardinality == "many-to-many"
    assert budget.proposal.score == SHARED_CEILING


async def test_the_pair_proposed_is_listed_as_the_budget_pair_is():
    answer = [*OBSERVED, _item((COMPLAINTS, "neighborhood"), (PERMITS, "neighborhood"), 0.45)]
    rows, reader, _ = await _run(answer)
    ours, budget = _row(rows, THE_PAIR), _row(rows, BUDGET_PAIR)
    assert ours is not None and budget is not None
    assert ours.proposal is not None and budget.proposal is not None
    assert (COMPLAINTS, "neighborhood") in reader.read
    assert (
        (ours.left_out, ours.proposal.cardinality, ours.proposal.score)
        == (
            budget.left_out,
            budget.proposal.cardinality,
            budget.proposal.score,
        )
        == (BELOW_THRESHOLD, "many-to-many", SHARED_CEILING)
    )
    assert ours.left_out in ACCEPTABLE
