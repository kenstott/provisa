# Copyright (c) 2026 Kenneth Stott
# Canary: 8427bc36-b3f3-40a5-8d22-14626f86c0ab
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-413 (amended 2026-11-16): a pair of columns already joined by a registered relationship,
directly or as a leg of a many-to-many relationship through its junction table, is never proposed;
a pair once accepted or marked not to be recommended again keeps that decision, and a later run
never reopens it as pending. On a real model store (SQLite), with the default demo's junction
shapes: pet_visits (pets to vets), pet_companions (pets to pets) and playgroup_members (pets to
playgroups)."""

# Requirements: REQ-413

from __future__ import annotations

from typing import Any, cast

import pytest
from sqlalchemy import delete, insert, select

from provisa.core import schema_org as so
from provisa.core.database import Database, create_engine_from_url
from provisa.discovery import gather, proposals
from provisa.discovery.columns import ColumnFacts, TableFacts
from provisa.discovery.direction import Known, Sampled, from_answer, settle

_TABLES = (
    "sources",
    "domains",
    "registered_tables",
    "table_columns",
    "tag_assignments",
    "relationships",
    "naming_rules",
    "relationship_candidates",
    "discovery_runs",
    "discovery_run_proposals",
    # What registering an accepted relationship reads and writes besides.
    "glossary_terms",
    "glossary_term_refs",
    "glossary_term_domains",
    "glossary_term_edges",
    "glossary_term_experts",
    "profiler_classifications",
    "rls_rules",
    "roles",
    "data_products",
)

_PETS, _VETS, _PET_VISITS, _PET_COMPANIONS, _PLAYGROUPS, _PLAYGROUP_MEMBERS = 1, 2, 3, 4, 5, 6

_SHAPES: dict[int, tuple[str, list[tuple[str, bool]]]] = {
    _PETS: ("pets", [("id", True), ("name", False)]),
    _VETS: ("vets", [("id", True), ("name", False)]),
    _PET_VISITS: ("pet_visits", [("id", True), ("pet_id", False), ("vet_id", False)]),
    _PET_COMPANIONS: (
        "pet_companions",
        [("id", True), ("pet_id", False), ("companion_pet_id", False), ("relation_type", False)],
    ),
    _PLAYGROUPS: ("playgroups", [("id", True), ("name", False)]),
    _PLAYGROUP_MEMBERS: (
        "playgroup_members",
        [("id", True), ("pet_id", False), ("playgroup_id", False)],
    ),
}

#: The demo's junction relationships: (id, source, target, junction, its two keys).
_JUNCTIONS = (
    ("pets-treated-by-vets", _PETS, _VETS, _PET_VISITS, "pet_id", "vet_id"),
    ("pets-bonded-pair", _PETS, _PETS, _PET_COMPANIONS, "pet_id", "companion_pet_id"),
    ("pets-in-playgroups", _PETS, _PLAYGROUPS, _PLAYGROUP_MEMBERS, "pet_id", "playgroup_id"),
)

#: Every leg those junctions join: the junction's key to the end's key.
_LEGS = {
    (_PET_VISITS, "pet_id", _PETS, "id"),
    (_PET_VISITS, "vet_id", _VETS, "id"),
    (_PET_COMPANIONS, "pet_id", _PETS, "id"),
    (_PET_COMPANIONS, "companion_pet_id", _PETS, "id"),
    (_PLAYGROUP_MEMBERS, "pet_id", _PETS, "id"),
    (_PLAYGROUP_MEMBERS, "playgroup_id", _PLAYGROUPS, "id"),
}


async def _store(tmp_path) -> Database:
    db = Database(
        create_engine_from_url(f"sqlite+pysqlite:///{tmp_path / 'store.db'}"), name="decided"
    )
    with db.engine.begin() as conn:
        so.metadata.create_all(conn, tables=[so.metadata.tables[n] for n in _TABLES])
    async with db.acquire() as conn:
        await conn.execute_core(insert(so.sources).values(id="pg", type="postgresql"))
        await conn.execute_core(insert(so.domains).values(id="pet-store"))
        for tid, (name, columns) in _SHAPES.items():
            await conn.execute_core(
                insert(so.registered_tables).values(
                    id=tid,
                    source_id="pg",
                    domain_id="pet-store",
                    schema_name="public",
                    table_name=name,
                )
            )
            for col, pk in columns:
                await conn.execute_core(
                    insert(so.table_columns).values(
                        table_id=tid,
                        domain_id="pet-store",
                        column_name=col,
                        data_type="integer",
                        is_primary_key=pk,
                    )
                )
    return db


async def _junctions(db: Database) -> None:
    async with db.acquire() as conn:
        for rel_id, source, target, via, via_source, via_target in _JUNCTIONS:
            await conn.execute_core(
                insert(so.relationships).values(
                    id=rel_id,
                    source_table_id=source,
                    source_column="id",
                    target_table_id=target,
                    target_column="id",
                    cardinality="one-to-many",
                    via_table_id=via,
                    via_source_column=via_source,
                    via_target_column=via_target,
                )
            )


async def _known(db: Database) -> Known:
    async with db.acquire() as conn:
        return await gather.known(cast(Any, conn))


async def test_each_leg_of_a_junction_relationship_is_known_as_existing(tmp_path):
    db = await _store(tmp_path)
    await _junctions(db)
    known = await _known(db)
    assert _LEGS <= known.existing


async def test_a_composite_junction_key_is_known_column_by_column(tmp_path):
    db = await _store(tmp_path)
    async with db.acquire() as conn:
        await conn.execute_core(
            insert(so.relationships).values(
                id="pets-treated-by-vets",
                source_table_id=_PETS,
                source_column="id, name",
                target_table_id=_VETS,
                target_column="id, name",
                cardinality="one-to-many",
                via_table_id=_PET_VISITS,
                via_source_column="pet_id, id",
                via_target_column="vet_id, id",
            )
        )
    known = await _known(db)
    assert {
        (_PET_VISITS, "pet_id", _PETS, "id"),
        (_PET_VISITS, "id", _PETS, "name"),
        (_PET_VISITS, "vet_id", _VETS, "id"),
        (_PET_VISITS, "id", _VETS, "name"),
    } <= known.existing


def _facts(tid: int) -> TableFacts:
    name, columns = _SHAPES[tid]
    return TableFacts(
        table_id=tid,
        domain_id="pet-store",
        schema_name="public",
        table_name=name,
        description=None,
        tags=(),
        role=None,
        columns=tuple(
            ColumnFacts(
                name=c,
                data_type="integer",
                description=None,
                tags=(),
                classes=("identifier",),
                sensitive=False,
                samples=(),
                is_primary_key=pk,
                groups=frozenset({"integer"}),
            )
            for c, pk in columns
        ),
    )


async def test_a_junction_leg_is_left_out_as_existing_in_either_direction(tmp_path):
    db = await _store(tmp_path)
    await _junctions(db)
    known = await _known(db)
    tables = {tid: _facts(tid) for tid in _SHAPES}
    ids = set(_SHAPES)
    # One as a foreign key states it; one written the other way round, from the playgroup.
    forward = from_answer(
        "foreign_keys", (_PET_VISITS, "pet_id"), (_PETS, "id"), "many-to-one", 1.0, "fk"
    )
    backward = from_answer(
        "semantics",
        (_PLAYGROUPS, "id"),
        (_PLAYGROUP_MEMBERS, "playgroup_id"),
        "many-to-one",
        0.9,
        "members name their playgroup",
    )

    async def never(table: TableFacts, column: str) -> Sampled:
        raise AssertionError(f"read {table.table_name}.{column}: a known pair needs no sample")

    outcome = await settle([forward, backward], tables, ids, ids, known, never)
    assert outcome.kept == []
    assert sorted(lo.reason for lo in outcome.left_out) == ["existing", "existing"]


async def test_a_pair_accepted_earlier_is_known_as_existing_after_its_relationship_is_gone(
    tmp_path,
):
    db = await _store(tmp_path)
    run = await _run(db)
    pid = await _proposal(db, run, (_PET_VISITS, "pet_id", _PETS, "id"))
    async with db.acquire() as conn:
        await proposals.accept(cast(Any, conn), run, pid, "visit-pet")
        await conn.execute_core(
            delete(so.relationships).where(so.relationships.c.id == "visit-pet")
        )
    known = await _known(db)
    assert (_PET_VISITS, "pet_id", _PETS, "id") in known.existing


async def _run(db: Database) -> int:
    async with db.acquire() as conn:
        await conn.execute_core(insert(so.discovery_runs).values(status="done", scope="{}"))
        latest = select(so.discovery_runs.c.id).order_by(so.discovery_runs.c.id.desc())
        return int((await conn.execute_core(latest)).scalars().first())


async def _proposal(
    db: Database, run_id: int, edge: tuple[int, str, int, str], left_out: str | None = None
) -> int:
    async with db.acquire() as conn:
        await conn.execute_core(
            insert(so.discovery_run_proposals).values(
                run_id=run_id,
                kind="semantics",
                source_table_id=edge[0],
                source_column=edge[1],
                target_table_id=edge[2],
                target_column=edge[3],
                confidence=0.9,
                # Every row a run records holds its cardinality (measured, else as proposed).
                cardinality="many-to-one",
                reasoning="the visit names its pet",
                left_out=left_out,
            )
        )
        latest = select(so.discovery_run_proposals.c.id).order_by(
            so.discovery_run_proposals.c.id.desc()
        )
        return int((await conn.execute_core(latest)).scalars().first())


async def _status(db: Database, edge: tuple[int, str, int, str]) -> str:
    rc = so.relationship_candidates
    async with db.acquire() as conn:
        return str(
            (
                await conn.execute_core(
                    select(rc.c.status).where(
                        rc.c.source_table_id == edge[0],
                        rc.c.source_column == edge[1],
                        rc.c.target_table_id == edge[2],
                        rc.c.target_column == edge[3],
                    )
                )
            ).one()[0]
        )


_EDGE = (_PET_VISITS, "pet_id", _PETS, "id")


@pytest.mark.parametrize("first", ["accept", "not_again"])
@pytest.mark.parametrize("then", ["accept", "not_again"])
async def test_a_decided_pair_is_never_reopened_by_a_later_runs_proposal(tmp_path, first, then):
    db = await _store(tmp_path)
    earlier = await _run(db)
    decided = await _proposal(db, earlier, _EDGE)
    async with db.acquire() as conn:
        if first == "accept":
            await proposals.accept(cast(Any, conn), earlier, decided, "visit-pet")
        else:
            await proposals.not_again(cast(Any, conn), earlier, decided)
    status = await _status(db, _EDGE)
    assert status == ("accepted" if first == "accept" else "rejected")

    later = await _run(db)
    again = await _proposal(db, later, _EDGE)
    async with db.acquire() as conn:
        with pytest.raises(proposals.NotDecidable, match=f"already {status}"):
            if then == "accept":
                await proposals.accept(cast(Any, conn), later, again, "visit-pet-2")
            else:
                await proposals.not_again(cast(Any, conn), later, again)
    assert await _status(db, _EDGE) == status


async def test_a_proposal_left_out_as_rejected_cannot_be_decided(tmp_path):
    db = await _store(tmp_path)
    earlier = await _run(db)
    decided = await _proposal(db, earlier, _EDGE)
    async with db.acquire() as conn:
        await proposals.not_again(cast(Any, conn), earlier, decided)
    assert not proposals.decidable("rejected")
    later = await _run(db)
    again = await _proposal(db, later, _EDGE, left_out="rejected")
    async with db.acquire() as conn:
        with pytest.raises(proposals.NotDecidable):
            await proposals.accept(cast(Any, conn), later, again, "visit-pet")
    assert await _status(db, _EDGE) == "rejected"


async def test_a_pending_pair_is_still_decided_in_a_later_runs_table(tmp_path):
    db = await _store(tmp_path)
    earlier = await _run(db)
    await _proposal(db, earlier, _EDGE)
    async with db.acquire() as conn:
        await conn.execute_core(
            insert(so.relationship_candidates).values(
                source_table_id=_EDGE[0],
                source_column=_EDGE[1],
                target_table_id=_EDGE[2],
                target_column=_EDGE[3],
                cardinality="many-to-one",
                confidence=0.9,
                scope=f"run:{earlier}",
            )
        )
    later = await _run(db)
    # REQ-413 (amended 2026-11-18): a pair still waiting is listed in the later run.
    again = await _proposal(db, later, _EDGE)
    async with db.acquire() as conn:
        assert await proposals.accept(cast(Any, conn), later, again, "visit-pet") == "visit-pet"
    assert await _status(db, _EDGE) == "accepted"


def test_no_route_lists_or_decides_suggestions_apart_from_a_runs_result():
    """REQ-413 (amended 2026-11-16): suggestions are decided only in a Discover run's result; the
    Relationships page lists none, and no route lists them or decides them all at once."""
    from provisa.api.admin import discovery

    paths = {(r.path, m) for r in discovery.router.routes for m in r.methods}  # type: ignore[attr-defined]
    for gone in (
        ("/admin/discover/suggestions", "GET"),
        ("/admin/discover/suggestions/apply", "POST"),
        ("/admin/discover/candidates/accept", "POST"),
        ("/admin/discover/candidates/reject", "POST"),
    ):
        assert gone not in paths
    assert ("/admin/discover/runs/{run_id}/apply", "POST") in paths


class _NoReads:
    """A measure reader that fails the test on any read: a known pair is never measured."""

    def column(self, table: TableFacts, column: str) -> Any:
        raise AssertionError(f"read {table.table_name}.{column}: a known pair is not measured")

    async def rows(self, tables: Any, sql: str) -> list[tuple]:
        raise AssertionError(f"queried {sql}: a known pair is not measured")


def _measurer(known: Known):
    from provisa.discovery.context import CONTAINED_AT, SHARED_CEILING, Scoring
    from provisa.discovery.measure import DISTINCT_BOUND, Measurer

    tables = {tid: _facts(tid) for tid in _SHAPES}
    return Measurer(
        _NoReads(),
        tables,
        decided=known.existing | known.rejected,
        bound=DISTINCT_BOUND,
        scoring=Scoring(CONTAINED_AT, SHARED_CEILING),
    )


async def test_a_junction_leg_is_never_measured_as_a_new_pair(tmp_path):
    """A leg of a junction relationship, proposed either way round, is returned as proposed:
    no column is read, no score or measured cardinality is given, and settling leaves it out as
    existing."""
    db = await _store(tmp_path)
    await _junctions(db)
    known = await _known(db)
    forward = from_answer(
        "semantics", (_PET_VISITS, "vet_id"), (_VETS, "id"), "many-to-one", 0.9, "visit's vet"
    )
    backward = from_answer(
        "semantics", (_PETS, "id"), (_PET_COMPANIONS, "companion_pet_id"), "many-to-one", 0.9, "x"
    )
    measured = await _measurer(known).all([forward, backward])
    assert measured == [forward, backward]
    assert all(p.measure is None and p.score is None for p in measured)

    async def never(table: TableFacts, column: str) -> Sampled:
        raise AssertionError(f"read {table.table_name}.{column}: a known pair needs no sample")

    ids = set(_SHAPES)
    outcome = await settle(measured, {t: _facts(t) for t in _SHAPES}, ids, ids, known, never)
    assert outcome.kept == []
    assert [lo.reason for lo in outcome.left_out] == ["existing", "existing"]


@pytest.mark.parametrize("first", ["accept", "not_again"])
async def test_a_measured_pair_already_decided_stays_decided(tmp_path, first):
    """A pair decided in one run is not measured again in the next; and a later run's row for
    it that carries a measure (scored under the threshold, so still decidable by its reason)
    is refused, leaving the pair's decision as it was."""
    db = await _store(tmp_path)
    earlier = await _run(db)
    decided = await _proposal(db, earlier, _EDGE)
    async with db.acquire() as conn:
        if first == "accept":
            await proposals.accept(cast(Any, conn), earlier, decided, "visit-pet")
        else:
            await proposals.not_again(cast(Any, conn), earlier, decided)
    status = await _status(db, _EDGE)

    known = await _known(db)
    again = from_answer(
        "semantics", (_PET_VISITS, "pet_id"), (_PETS, "id"), "many-to-one", 0.9, "visit's pet"
    )
    assert await _measurer(known).all([again]) == [again]

    later = await _run(db)
    row = await _proposal(db, later, _EDGE, left_out=proposals.BELOW_THRESHOLD)
    async with db.acquire() as conn:
        await conn.execute_core(
            so.discovery_run_proposals.update()
            .where(so.discovery_run_proposals.c.id == row)
            .values(score=0.3)
        )
    assert proposals.decidable(proposals.BELOW_THRESHOLD)
    async with db.acquire() as conn:
        with pytest.raises(proposals.NotDecidable, match=f"already {status}"):
            await proposals.accept(cast(Any, conn), later, row, "visit-pet-2")
        with pytest.raises(proposals.NotDecidable, match=f"already {status}"):
            await proposals.not_again(cast(Any, conn), later, row)
    assert await _status(db, _EDGE) == status
