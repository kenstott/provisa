# Copyright (c) 2026 Kenneth Stott
# Canary: 6f1d3b84-2e97-4c50-a8b2-d4e7c0f9a531
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-413: every pair a discovery run proposes is measured on its data, and the measure decides
its cardinality and score.

The shapes are the demo's: neighborhood shared by the budget, the permits and the complaints, a
key of none of them (many-to-many, the values overlapping whole); inspections.permit_id contained
in building_permits.permit_id, which is distinct (a key relationship, whatever the model rated
it); two columns of one name whose values barely meet; and a table that requires a parameter,
measured on what its other column gives and never read."""

# Requirements: REQ-413, REQ-1986

from __future__ import annotations

from types import SimpleNamespace
from typing import Any

import pytest

from provisa.discovery import gather, measure
from provisa.discovery.columns import UNKNOWN, ColumnFacts, TableFacts, type_groups
from provisa.discovery.context import KEY_SCORE, SHARED_CEILING
from provisa.discovery.threshold import DEFAULT as THRESHOLD
from provisa.discovery.direction import from_answer
from provisa.discovery.measure import Measurer, Values
from provisa.discovery.proposals import BELOW_THRESHOLD, NOT_COMPARABLE
from provisa.discovery.runner import KeyValues, Tally, judged
from tests.unit.discovery_engine_reader import EngineReader


def _column(name: str, *, parameter: str | None = None) -> ColumnFacts:
    return ColumnFacts(
        name=name,
        data_type="text",
        description=None,
        tags=(),
        classes=(UNKNOWN,),
        sensitive=False,
        samples=(),
        is_primary_key=False,
        groups=type_groups("text", subtype=None, array_depth=0),
        required_parameter=parameter,
    )


def _table(tid: int, name: str, *columns: ColumnFacts) -> TableFacts:
    return TableFacts(
        table_id=tid,
        domain_id="city",
        schema_name="public",
        table_name=name,
        description=None,
        tags=(),
        role=None,
        columns=columns,
    )


BUDGET = _table(1, "budget", _column("neighborhood"))
PERMITS = _table(2, "building_permits", _column("permit_id"), _column("neighborhood"))
INSPECTIONS = _table(3, "inspections", _column("permit_id"))
COMPLAINTS = _table(4, "complaints", _column("neighborhood"), _column("code"))
REGIONS = _table(5, "regions", _column("code"))
#: A table that requires a parameter: never read (REQ-1986).
VISITS = _table(6, "pet_visits", _column("permit_id"), _column("owner", parameter="owner"))
TABLES = {t.table_id: t for t in (BUDGET, PERMITS, INSPECTIONS, COMPLAINTS, REGIONS, VISITS)}

NEIGHBORHOODS = ["Riverside", "Hillcrest", "Downtown", "Old Town", "Harbor"]
PERMIT_IDS = [f"BP-{n:04d}" for n in range(1, 101)]

#: Each column's values as its table holds them, rows in file order.
DATA: dict[tuple[str, str], list[Any]] = {
    # The budget has a line per department in each neighborhood: no neighborhood is its key.
    ("budget", "neighborhood"): [n for n in NEIGHBORHOODS for _ in range(4)],
    # The permits file is sorted by neighborhood: its first rows hold one value.
    ("building_permits", "neighborhood"): sorted(NEIGHBORHOODS * 20),
    ("building_permits", "permit_id"): PERMIT_IDS,
    ("complaints", "neighborhood"): NEIGHBORHOODS * 7,
    # Inspections refer to 30 of the permits, several inspections each.
    ("inspections", "permit_id"): [p for p in PERMIT_IDS[:30] for _ in range(3)],
    ("complaints", "code"): [f"C-{n}" for n in range(50)],
    ("regions", "code"): [f"R-{n}" for n in range(49)] + ["C-7"],
}


def _semantic(source: tuple[int, str], target: tuple[int, str], confidence: float):
    return from_answer("semantics", source, target, "many-to-one", confidence, "same name")


async def _measured(p, *, bound: int = measure.DISTINCT_BOUND, data=DATA):
    reader = EngineReader(data)
    return await Measurer(reader, TABLES, decided=frozenset(), bound=bound).measured(p), reader


async def test_neighborhood_shared_by_tables_none_of_which_it_keys_is_many_to_many():
    got, _ = await _measured(_semantic((2, "neighborhood"), (1, "neighborhood"), 0.45))
    m = got.measure
    assert m is not None
    assert m.cardinality() == "many-to-many"
    assert got.cardinality == "many-to-many"
    # The values overlap whole both ways, however few a sample of the sorted file showed.
    assert (m.source_in_target, m.source.values) == (5, 5)
    assert (m.target_in_source, m.target.values) == (5, 5)
    assert m.source.distinct is False and m.target.distinct is False
    # A shared value, not a key relationship: under the threshold whatever the model said.
    assert got.score == SHARED_CEILING
    assert got.rank < THRESHOLD
    assert got.confidence == 0.45  # the model's own is kept beside it


async def test_permit_id_contained_in_a_distinct_target_scores_as_a_key_relationship():
    got, _ = await _measured(_semantic((3, "permit_id"), (2, "permit_id"), 0.45))
    m = got.measure
    assert m is not None
    assert got.cardinality == "many-to-one"
    assert (m.source_in_target, m.source.values) == (30, 30)
    assert (m.target_in_source, m.target.values) == (30, 100)
    assert m.target.distinct is True and m.source.distinct is False
    assert got.score == KEY_SCORE and got.rank >= THRESHOLD
    assert got.confidence == 0.45


async def test_a_pair_whose_values_barely_meet_scores_low_however_alike_the_names():
    got, _ = await _measured(_semantic((4, "code"), (5, "code"), 0.95))
    m = got.measure
    assert m is not None
    assert (m.source_in_target, m.source.values) == (1, 50)
    assert got.cardinality == "one-to-one"
    assert got.score is not None and got.score < 0.05
    assert got.rank < THRESHOLD


async def test_a_semantic_pair_whose_source_alone_is_distinct_is_turned_to_start_at_the_referrer():
    got, _ = await _measured(_semantic((2, "permit_id"), (3, "permit_id"), 0.8))
    assert (got.start, got.end) == ((3, "permit_id"), (2, "permit_id"))
    assert got.cardinality == "many-to-one" and got.score == KEY_SCORE


async def test_a_foreign_key_keeps_its_declared_direction_and_is_measured():
    fk = from_answer(
        "foreign_keys", (2, "permit_id"), (3, "permit_id"), "many-to-one", 1.0, "constraint"
    )
    got, _ = await _measured(fk)
    assert (got.start, got.end) == ((2, "permit_id"), (3, "permit_id"))
    assert got.cardinality == "one-to-many"


async def test_a_table_that_requires_a_parameter_is_measured_on_what_it_can_give_and_never_read():
    got, reader = await _measured(_semantic((6, "permit_id"), (2, "permit_id"), 0.8))
    m = got.measure
    assert m is not None
    assert reader.queries == [(("building_permits",), reader.queries[0][1])]
    assert "pet_visits" not in reader.queries[0][1]
    assert m.source.unmeasured == measure.NOT_READ and m.target.unmeasured is None
    assert m.source_in_target is None and m.target_in_source is None
    # Only the target was read: it is distinct, so the pair is many-to-one.
    assert m.target.distinct is True and got.cardinality == "many-to-one"
    # No values were compared: the model's confidence stands.
    assert got.score is None and got.rank == 0.8


async def test_a_column_holding_more_than_the_sample_size_is_counted_on_the_rows_read():
    got, reader = await _measured(_semantic((3, "permit_id"), (2, "permit_id"), 0.5), bound=40)
    m = got.measure
    assert m is not None
    assert m.target.more is True and m.target.values == 40
    # Forty of the inspections' rows were read: each permit they name is found among all the
    # permits, not only among the permits' first forty.
    assert m.source.more is True and m.source.values is not None and m.source.values <= 40
    assert m.source_in_target == m.source.values and got.score == KEY_SCORE
    assert len(reader.queries) == 1


async def test_the_source_read_stops_at_the_sample_size():
    data = {**DATA, ("complaints", "code"): [f"C-{n}" for n in range(100)]}
    got, reader = await _measured(_semantic((4, "code"), (5, "code"), 0.5), bound=40, data=data)
    m = got.measure
    assert m is not None
    assert m.source.values == 40 and m.source.more is True
    assert 'WHERE "code" IS NOT NULL LIMIT 40)' in reader.queries[0][1]


async def test_nulls_never_count_and_the_sample_size_counts_non_null_values():
    data = {
        **DATA,
        ("complaints", "code"): [None] * 10 + ["C-1", "C-2", "C-3"],
        ("regions", "code"): ["C-1", "C-2", "C-3", "C-4", None, None],
    }
    got, _ = await _measured(_semantic((4, "code"), (5, "code"), 0.5), bound=3, data=data)
    m = got.measure
    assert m is not None
    # Three non-null values fill a sample of three, and all are in the target.
    assert (m.source.values, m.source.more, m.source_in_target) == (3, False, 3)
    assert m.contained == 1.0 and m.source.distinct is True
    # The target's nulls are no values: three of its four read, none repeating.
    assert (m.target.values, m.target.more, m.target.distinct) == (3, True, True)


async def test_values_of_a_whole_number_column_are_matched_in_the_engine():
    data = {("complaints", "code"): [1, 2, 2, None], ("regions", "code"): [2, 3, 4]}
    reader = EngineReader(data, sql_type="BIGINT")
    measurer = Measurer(reader, TABLES, decided=frozenset(), bound=measure.DISTINCT_BOUND)
    m = (await measurer.measured(_semantic((4, "code"), (5, "code"), 0.5))).measure
    assert m is not None
    assert (m.source_in_target, m.source.values, m.source.distinct) == (1, 2, False)
    assert (m.target_in_source, m.target.values, m.target.distinct) == (1, 3, True)


async def test_each_pair_is_measured_by_one_query_and_reused_turned_round():
    reader = EngineReader(DATA)
    measurer = Measurer(reader, TABLES, decided=frozenset(), bound=measure.DISTINCT_BOUND)
    await measurer.all(
        [
            _semantic((2, "neighborhood"), (1, "neighborhood"), 0.5),
            _semantic((4, "neighborhood"), (1, "neighborhood"), 0.5),
            _semantic((1, "neighborhood"), (2, "neighborhood"), 0.5),
        ]
    )
    assert [tables for tables, _ in reader.queries] == [
        ("building_permits", "budget"),
        ("complaints", "budget"),
    ]


async def test_a_band_pair_is_judged_on_its_measured_score_and_the_rest_are_kept_with_why():
    reader = EngineReader(DATA)
    tally = Tally(total=1, threshold=THRESHOLD)
    measurer = Measurer(reader, TABLES, decided=frozenset(), bound=measure.DISTINCT_BOUND)
    out = await judged(
        [
            _semantic((3, "permit_id"), (2, "permit_id"), 0.45),
            _semantic((2, "neighborhood"), (1, "neighborhood"), 0.5),
        ],
        [_semantic((4, "code"), (5, "code"), 0.5)],
        KeyValues(measurer.all, 0.2),
        None,
        tally,
    )
    assert [(p.start, p.score) for p in out] == [((3, "permit_id"), KEY_SCORE)]
    assert tally.below_threshold == 1
    reasons = [(r.left_out, r.proposal.start if r.proposal else None) for r in tally.left_out]
    assert (NOT_COMPARABLE, (4, "code")) in reasons
    assert (BELOW_THRESHOLD, (2, "neighborhood")) in reasons
    # A measured proposal left out keeps its measure, to be shown on its row; one whose columns
    # are not compared is never measured.
    by_start = {r.proposal.start: r.proposal for r in tally.left_out if r.proposal is not None}
    assert by_start[(2, "neighborhood")].measure is not None
    assert by_start[(4, "code")].measure is None
    assert all("complaints" not in tables for tables, _ in reader.queries)


# --- the reader: the governed pipeline as the run's role ------------------------------------------


@pytest.fixture
def pipeline(monkeypatch):
    """The governed read as a role, faked at the reader's seams; each read kept with its role."""
    calls: list[tuple[str, str]] = []
    answers: list[list[tuple]] = []

    def published_as(_state, role, table_id):
        if role == "outsider":
            return None
        return '"city"."' + TABLES[table_id].table_name + '"', {"permit_id": "permit_id"}

    async def governed_as(sql, role):
        calls.append((sql, role))
        return answers.pop(0)

    monkeypatch.setattr(gather, "_published_as", published_as)
    monkeypatch.setattr(gather, "_governed_as", governed_as)
    monkeypatch.setattr(gather, "_measured_as", governed_as)
    return SimpleNamespace(calls=calls, answers=answers)


async def test_values_are_read_through_the_governed_pipeline_as_the_runs_role(pipeline):
    reader = gather.values_reader(SimpleNamespace(), "steward")
    pipeline.answers.append([("BP-0001", 3), ("BP-0002", 1)])
    got = await reader.values(INSPECTIONS, "permit_id", measure.DISTINCT_BOUND)
    assert isinstance(got, Values)
    assert got.read == ("BP-0001", "BP-0002") and got.distinct is False and got.more is False
    sql, role = pipeline.calls[0]
    assert role == "steward"
    assert sql == (
        'SELECT "permit_id", COUNT(*) FROM "city"."inspections" WHERE "permit_id" IS NOT NULL '
        f'GROUP BY "permit_id" ORDER BY 2 DESC LIMIT {measure.DISTINCT_BOUND + 1}'
    )


async def test_a_column_whose_most_repeated_value_occurs_once_is_distinct(pipeline):
    reader = gather.values_reader(SimpleNamespace(), "steward")
    pipeline.answers.append([("BP-0001", 1), ("BP-0002", 1)])
    got = await reader.values(PERMITS, "permit_id", measure.DISTINCT_BOUND)
    assert isinstance(got, Values) and got.distinct is True


async def test_a_table_the_role_cannot_read_is_not_measured(pipeline):
    reader = gather.values_reader(SimpleNamespace(), "outsider")
    assert await reader.values(PERMITS, "permit_id", measure.DISTINCT_BOUND) == measure.NOT_READABLE
    assert pipeline.calls == []


async def test_a_table_that_requires_a_parameter_is_never_read_by_the_measure(pipeline):
    reader = gather.values_reader(SimpleNamespace(), "steward")
    assert await reader.values(VISITS, "permit_id", measure.DISTINCT_BOUND) == measure.NOT_READ
    assert pipeline.calls == []


async def test_values_are_looked_up_in_another_column_as_literals_of_their_type(pipeline):
    from datetime import UTC, date, datetime

    reader = gather.values_reader(SimpleNamespace(), "steward")
    pipeline.answers.append([("BP-0001",)])
    found = await reader.found(
        PERMITS, "permit_id", Values(read=("BP-0001", "O'Hare"), more=False, distinct=True)
    )
    assert found == frozenset({"BP-0001"})
    assert pipeline.calls[0][0].endswith("""WHERE "permit_id" IN ('BP-0001', 'O''Hare')""")
    assert gather._literal(12) == "12"
    # REQ-413 (amended 2026-11-16): a date or timestamp column is in no compared group, so its
    # values are never looked up in another column.
    for value in (date(2026, 1, 2), datetime(2026, 1, 2, 3, 4), datetime(2026, 1, 2, tzinfo=UTC)):
        with pytest.raises(TypeError):
            gather._literal(value)


async def test_a_pair_is_measured_by_one_governed_query_joining_a_limited_source(pipeline):
    reader = measure.governed_reader(SimpleNamespace(), "steward")
    measurer = Measurer(reader, TABLES, decided=frozenset(), bound=500)
    pipeline.answers.append([(90, 30, 90, 100, 100, 100, 30, 30)])
    got = await measurer.measured(_semantic((3, "permit_id"), (2, "permit_id"), 0.5))
    assert len(pipeline.calls) == 1
    sql, role = pipeline.calls[0]
    assert role == "steward"
    source = 'SELECT "permit_id" AS v FROM "city"."inspections" WHERE "permit_id" IS NOT NULL'
    assert f"s AS ({source} LIMIT 500)" in sql
    assert 'JOIN "city"."building_permits" AS s_in ON s.v = s_in."permit_id"' in sql
    assert 'JOIN "city"."inspections" AS t_in ON t.v = t_in."permit_id"' in sql
    # A distinct count is taken only of the rows read, never of a whole column.
    assert sql.count("COUNT(DISTINCT") == 4
    assert "COUNT(DISTINCT v) FROM s)" in sql and "COUNT(DISTINCT v) FROM t)" in sql
    m = got.measure
    assert m is not None
    assert (m.source.values, m.source.distinct, m.source.more) == (30, False, False)
    assert (m.target.values, m.target.distinct, m.target.more) == (100, True, False)
    assert (m.source_in_target, m.target_in_source) == (30, 30)


async def test_the_governed_reader_does_not_measure_what_the_role_cannot_read(pipeline):
    assert (
        measure.governed_reader(SimpleNamespace(), "outsider").column(PERMITS, "permit_id")
        == measure.NOT_READABLE
    )
    assert (
        measure.governed_reader(SimpleNamespace(), "steward").column(VISITS, "permit_id")
        == measure.NOT_READ
    )
    assert pipeline.calls == []
