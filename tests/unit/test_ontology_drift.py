# Copyright (c) 2026 Kenneth Stott
# Canary: 6e1d9c42-7f3a-4b58-9c06-2ad84e5f1b73
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1386 (DRIFT FROM THE MODEL'S HISTORY): the Ontology quality report's drift, graded from two
commits of the org's real repository, with the same computation the report's grounding uses."""

# Requirements: REQ-1386, REQ-1387

from __future__ import annotations

from datetime import UTC, datetime
from types import SimpleNamespace
from typing import cast

import pytest
from fastapi import Request
from sqlalchemy import insert

from provisa.api.admin import glossary_router
from provisa.core import env_files, env_repo, term_drift
from provisa.core.env_project import project
from provisa.core.repositories import glossary as glossary_repo
from provisa.core.schema_org import (
    domains,
    glossary_term_domains,
    glossary_term_edges,
    glossary_term_refs,
    glossary_terms,
    registered_tables,
    sources,
)
from tests.helpers import ready_org_folder

ORG = "acme"
ENV = "prod"
# The month before last's reports close on 2026-09-01: ``NOW`` is in October.
NOW = datetime(2026, 10, 30, 12, 0, tzinfo=UTC)
CUTOFF = datetime(2026, 9, 1, tzinfo=UTC)
# The route's gates are stubbed, so nothing reads the request.
_REQUEST = cast(Request, SimpleNamespace())


@pytest.fixture
def repository(tmp_path, monkeypatch):
    monkeypatch.setenv("PROVISA_DATA_DIR", str(tmp_path / "data"))
    ready_org_folder(ORG)
    return env_repo.ensure_repo(ORG)


def _term(name: str, *, approved: bool = True, edges: tuple = (), **flags) -> dict:
    return {
        "name": name,
        "approved": approved,
        "deprecated": False,
        "retired": False,
        **flags,
        "edges": [{"rel_type": rel, "to": f"terms/{to}.yaml"} for rel, to in edges],
    }


def _table(*terms: str, class_term: str | None = None) -> dict:
    table: dict = {
        "table_name": "orders",
        "glossary": [{"column_name": t, "term": f"terms/{t}.yaml"} for t in terms],
    }
    if class_term is not None:
        table["class_term"] = [{"term": f"terms/{class_term}.yaml"}]
    return table


def _files(terms: dict[str, dict], *tables: dict) -> dict[str, str]:
    tree = {f"terms/{name}.yaml": body for name, body in terms.items()}
    tree |= {f"sales/tables/t{i}.yaml": table for i, table in enumerate(tables)}
    return env_files.dump(tree)


def _commit(repository, monkeypatch, at: datetime, files: dict[str, str]) -> str:
    monkeypatch.setattr(env_repo, "time", SimpleNamespace(time=at.timestamp))
    sha = env_repo.commit_files(repository, ENV, files, "model change", None)
    assert sha is not None
    return sha


def _rows(found: dict) -> dict[str, tuple]:
    return {t["name"]: (t["before"], t["after"], t["change"]) for t in found["terms"]}


@pytest.mark.parametrize(
    ("now", "cutoff"),
    [
        (datetime(2026, 10, 30, 12, 0, tzinfo=UTC), datetime(2026, 9, 1, tzinfo=UTC)),
        (datetime(2026, 10, 1, 0, 0, tzinfo=UTC), datetime(2026, 9, 1, tzinfo=UTC)),
        (datetime(2026, 1, 15, 8, 0, tzinfo=UTC), datetime(2025, 12, 1, tzinfo=UTC)),
    ],
)
def test_the_cutoff_is_the_first_of_last_month_utc(now, cutoff):
    assert term_drift.period_cutoff(now) == cutoff


def test_the_cutoff_is_read_in_utc_whatever_zone_now_is_given_in():
    from datetime import timedelta, timezone

    # 2026-10-01 01:00 at +05:00 is still September in UTC.
    east = datetime(2026, 10, 1, 1, 0, tzinfo=timezone(timedelta(hours=5)))
    assert term_drift.period_cutoff(east) == datetime(2026, 8, 1, tzinfo=UTC)


def test_a_term_that_lost_or_gained_grounding_between_two_commits_is_drift(repository, monkeypatch):
    base = _files(
        {
            "amount": _term("amount"),
            "revenue": _term("revenue", edges=()),
            "price": _term("price", edges=(("KIND_OF", "revenue"),)),
            "steady": _term("steady"),
        },
        _table("amount", "steady", "price"),
    )
    head = _files(
        {
            "amount": _term("amount"),
            "revenue": _term("revenue"),
            "price": _term("price", edges=(("KIND_OF", "revenue"),)),
            "steady": _term("steady"),
        },
        _table("steady", "price"),
    )
    # ``amount`` lost its column. ``revenue`` stays grounded through ``price``.
    sha = _commit(repository, monkeypatch, datetime(2026, 8, 20, tzinfo=UTC), base)
    _commit(repository, monkeypatch, datetime(2026, 10, 5, tzinfo=UTC), head)

    found = term_drift.drift(ORG, ENV, NOW)

    assert found["has_history"] is True
    assert found["cutoff"] == CUTOFF.isoformat()
    assert found["base"]["sha"] == sha
    assert _rows(found) == {"amount": ("grounded", "not_grounded", "lost")}
    assert found["counts"] == {"lost": 1, "gained": 0, "new": 0}


def test_a_term_that_gained_grounding_is_drift(repository, monkeypatch):
    terms = {"amount": _term("amount")}
    _commit(repository, monkeypatch, datetime(2026, 8, 20, tzinfo=UTC), _files(terms))
    _commit(
        repository, monkeypatch, datetime(2026, 10, 5, tzinfo=UTC), _files(terms, _table("amount"))
    )
    found = term_drift.drift(ORG, ENV, NOW)
    assert _rows(found) == {"amount": ("not_grounded", "grounded", "gained")}
    assert found["counts"] == {"lost": 0, "gained": 1, "new": 0}


def test_a_draft_on_every_path_moves_a_term_between_grounded_and_pending(repository, monkeypatch):
    def model(*, middle_approved: bool) -> dict[str, str]:
        terms = {
            "leaf": _term("leaf"),
            "middle": _term("middle", approved=middle_approved, edges=(("KIND_OF", "top"),)),
            "top": _term("top"),
        }
        terms["leaf"]["edges"] = [{"rel_type": "KIND_OF", "to": "terms/middle.yaml"}]
        return _files(terms, _table("leaf"))

    _commit(repository, monkeypatch, datetime(2026, 8, 20, tzinfo=UTC), model(middle_approved=True))
    _commit(
        repository, monkeypatch, datetime(2026, 10, 5, tzinfo=UTC), model(middle_approved=False)
    )
    assert _rows(term_drift.drift(ORG, ENV, NOW)) == {
        "top": ("grounded", "grounded_pending", "lost")
    }


def test_retiring_a_term_loses_its_grounding_and_the_chain_above_it(repository, monkeypatch):
    def model(*, retired: bool) -> dict[str, str]:
        return _files(
            {
                "leaf": _term("leaf", retired=retired, edges=(("KIND_OF", "top"),)),
                "top": _term("top"),
            },
            _table("leaf"),
        )

    _commit(repository, monkeypatch, datetime(2026, 8, 20, tzinfo=UTC), model(retired=False))
    _commit(repository, monkeypatch, datetime(2026, 10, 5, tzinfo=UTC), model(retired=True))
    assert _rows(term_drift.drift(ORG, ENV, NOW)) == {
        "leaf": ("grounded", "not_grounded", "lost"),
        "top": ("grounded", "not_grounded", "lost"),
    }


def test_a_class_term_is_grounded_by_its_table(repository, monkeypatch):
    terms = {"customer": _term("customer")}
    _commit(repository, monkeypatch, datetime(2026, 8, 20, tzinfo=UTC), _files(terms, _table()))
    _commit(
        repository,
        monkeypatch,
        datetime(2026, 10, 5, tzinfo=UTC),
        _files(terms, _table(class_term="customer")),
    )
    assert _rows(term_drift.drift(ORG, ENV, NOW)) == {
        "customer": ("not_grounded", "grounded", "gained")
    }


def test_with_no_commit_by_the_cutoff_there_is_no_history_not_zero_drift(repository, monkeypatch):
    # The whole model is younger than the period; nothing here says nothing drifted.
    _commit(
        repository,
        monkeypatch,
        datetime(2026, 9, 1, 0, 0, 1, tzinfo=UTC),
        _files({"amount": _term("amount")}, _table("amount")),
    )
    found = term_drift.drift(ORG, ENV, NOW)
    assert found == {
        "has_history": False,
        "cutoff": CUTOFF.isoformat(),
        "base": None,
        "terms": None,
        "counts": None,
    }


def test_an_environment_with_no_branch_has_no_history(repository):
    found = term_drift.drift(ORG, ENV, NOW)
    assert found["has_history"] is False
    assert found["terms"] is None and found["counts"] is None


def test_no_change_since_the_base_is_history_with_zero_drift(repository, monkeypatch):
    files = _files({"amount": _term("amount")}, _table("amount"))
    _commit(repository, monkeypatch, datetime(2026, 8, 20, tzinfo=UTC), files)
    found = term_drift.drift(ORG, ENV, NOW)
    assert found["has_history"] is True
    assert found["terms"] == []
    assert found["counts"] == {"lost": 0, "gained": 0, "new": 0}


def test_a_commit_exactly_at_the_cutoff_is_the_base(repository, monkeypatch):
    sha = _commit(repository, monkeypatch, CUTOFF, _files({"amount": _term("amount")}))
    _commit(
        repository,
        monkeypatch,
        datetime(2026, 9, 1, 0, 0, 1, tzinfo=UTC),
        _files({"amount": _term("amount")}, _table("amount")),
    )
    assert term_drift.drift(ORG, ENV, NOW)["base"]["sha"] == sha


def test_the_base_is_the_latest_commit_by_the_cutoff(repository, monkeypatch):
    _commit(repository, monkeypatch, datetime(2026, 7, 1, tzinfo=UTC), _files({"a": _term("a")}))
    sha = _commit(
        repository, monkeypatch, datetime(2026, 8, 31, tzinfo=UTC), _files({"b": _term("b")})
    )
    _commit(repository, monkeypatch, datetime(2026, 9, 2, tzinfo=UTC), _files({"c": _term("c")}))
    base = term_drift.drift(ORG, ENV, NOW)["base"]
    assert base == {"sha": sha, "committed_at": datetime(2026, 8, 31, tzinfo=UTC).isoformat()}


def test_the_base_is_found_past_any_history_page(repository, monkeypatch):
    base = _commit(repository, monkeypatch, datetime(2026, 8, 1, tzinfo=UTC), _files({}, _table()))
    # ``env_repo.history``'s default page is 100 commits.
    for i in range(105):
        _commit(
            repository,
            monkeypatch,
            datetime(2026, 9, 2, 0, i // 60, i % 60, tzinfo=UTC),
            _files({f"t{i}": _term(f"t{i}")}),
        )
    assert term_drift.drift(ORG, ENV, NOW)["base"]["sha"] == base


def test_a_term_absent_at_the_base_is_new(repository, monkeypatch):
    _commit(repository, monkeypatch, datetime(2026, 8, 20, tzinfo=UTC), _files({}, _table()))
    _commit(
        repository,
        monkeypatch,
        datetime(2026, 10, 5, tzinfo=UTC),
        _files({"bound": _term("bound"), "loose": _term("loose")}, _table("bound")),
    )
    found = term_drift.drift(ORG, ENV, NOW)
    assert _rows(found) == {
        "bound": (None, "grounded", "new"),
        "loose": (None, "not_grounded", "new"),
    }
    assert found["counts"] == {"lost": 0, "gained": 0, "new": 2}


def test_a_renamed_term_is_one_lost_and_one_new(repository, monkeypatch):
    _commit(
        repository,
        monkeypatch,
        datetime(2026, 8, 20, tzinfo=UTC),
        _files({"amount": _term("amount")}, _table("amount")),
    )
    _commit(
        repository,
        monkeypatch,
        datetime(2026, 10, 5, tzinfo=UTC),
        _files({"total": _term("total")}, _table("total")),
    )
    assert _rows(term_drift.drift(ORG, ENV, NOW)) == {
        "amount": ("grounded", None, "lost"),
        "total": (None, "grounded", "new"),
    }


def test_a_term_removed_with_no_grounding_to_lose_is_no_drift(repository, monkeypatch):
    _commit(repository, monkeypatch, datetime(2026, 8, 20, tzinfo=UTC), _files({"a": _term("a")}))
    _commit(repository, monkeypatch, datetime(2026, 10, 5, tzinfo=UTC), _files({"b": _term("b")}))
    assert "a" not in _rows(term_drift.drift(ORG, ENV, NOW))


async def _projected_model(conn) -> None:
    await conn.execute_core(insert(domains).values(id="sales", description=""))
    await conn.execute_core(insert(sources).values(id="s1", type="postgres"))
    await conn.execute_core(
        insert(registered_tables).values(
            id=1, source_id="s1", domain_id="sales", schema_name="public", table_name="orders"
        )
    )
    rows = (
        (1, "amount", True, False),
        (2, "price", False, False),
        (3, "revenue", True, False),
        (4, "customer", True, False),
        (5, "legacy", True, True),
        (6, "orphan", True, False),
    )
    for tid, name, approved, retired in rows:
        await conn.execute_core(
            insert(glossary_terms).values(id=tid, name=name, approved=approved, retired=retired)
        )
    await conn.execute_core(insert(glossary_term_domains).values(term_id=6, domain_id="sales"))
    await conn.execute_core(
        insert(glossary_term_refs).values(term_id=1, table_id=1, column_name="amount")
    )
    await conn.execute_core(
        insert(glossary_term_refs).values(term_id=5, table_id=1, column_name="legacy")
    )
    for a, b, rel in ((1, 2, "KIND_OF"), (2, 3, "KIND_OF"), (6, 3, "RELATED_TO")):
        await conn.execute_core(
            insert(glossary_term_edges).values(from_term_id=a, to_term_id=b, rel_type=rel)
        )
    await conn.execute("UPDATE org_default.registered_tables SET class_term_id = 4 WHERE id = 1")


async def test_a_projected_tree_grades_as_the_live_model_does(control_plane):
    """The tree is graded with the live computation over a real projection, so a commit's grade
    and today's are the same statement."""
    db = control_plane.org()
    try:
        async with db.acquire() as conn:
            await _projected_model(conn)
            tree = await project(conn, "org_default")
            live = await glossary_repo.grounding(conn)
            names = {t["id"]: t["name"] for t in await glossary_repo.list_terms(conn)}
    finally:
        await db.close()
    graded = term_drift.graded(env_files.dump(tree))
    assert graded == {names[tid]: grade for tid, grade in live.items()}
    assert {"grounded", "grounded_pending", "not_grounded"} == set(graded.values())


def _wired(monkeypatch, *, allowed):
    monkeypatch.setattr(glossary_router, "_require_glossary_read", lambda request: None)
    monkeypatch.setattr(glossary_router, "require_active_org_id", lambda request: ORG)
    monkeypatch.setattr(glossary_router, "_authority", lambda request: allowed)


def _live_terms(monkeypatch, ids: dict[str, int], scopes: dict[int, set[str]]) -> None:
    class _Acquired:
        async def __aenter__(self):
            return object()

        async def __aexit__(self, *exc):
            return False

    class _Pool:
        def acquire(self):
            return _Acquired()

    async def _pool():
        return _Pool()

    async def _ids(conn):
        return ids

    async def _scopes(conn):
        return scopes

    monkeypatch.setattr(glossary_router, "_pool", _pool)
    monkeypatch.setattr(glossary_router, "_term_ids_by_name", _ids)
    monkeypatch.setattr(glossary_router.glossary_repo, "term_domains", _scopes)


def _drifted(monkeypatch, found: dict) -> None:
    monkeypatch.setattr(term_drift, "drift", lambda org, env: found)


_FOUND = {
    "has_history": True,
    "cutoff": CUTOFF.isoformat(),
    "base": {"sha": "abc", "committed_at": CUTOFF.isoformat()},
    "terms": [
        {"name": "amount", "before": "grounded", "after": "not_grounded", "change": "lost"},
        {"name": "payroll", "before": "grounded", "after": "not_grounded", "change": "lost"},
        {"name": "gone", "before": "grounded", "after": None, "change": "lost"},
        {"name": "fresh", "before": None, "after": "not_grounded", "change": "new"},
    ],
    "counts": {"lost": 3, "gained": 0, "new": 1},
}


async def test_the_route_links_each_term_that_still_exists(monkeypatch):
    _wired(monkeypatch, allowed=None)
    _live_terms(monkeypatch, {"amount": 7, "payroll": 8, "fresh": 9}, {})
    _drifted(monkeypatch, _FOUND)
    told = await glossary_router.ontology_drift(_REQUEST)
    assert {t["name"]: t["term_id"] for t in told["terms"]} == {
        "amount": 7,
        "payroll": 8,
        "gone": None,
        "fresh": 9,
    }


async def test_the_route_tells_only_the_terms_the_caller_reads(monkeypatch):
    """A term out of the caller's domains is not told of, nor counted; a term that no longer
    exists has no domains to read it by, so a scoped caller is not told of it either."""
    _wired(monkeypatch, allowed=frozenset({"sales"}))
    _live_terms(
        monkeypatch,
        {"amount": 7, "payroll": 8, "fresh": 9},
        {7: {"sales"}, 8: {"hr"}, 9: {"sales"}},
    )
    _drifted(monkeypatch, _FOUND)
    told = await glossary_router.ontology_drift(_REQUEST)
    assert [t["name"] for t in told["terms"]] == ["amount", "fresh"]
    assert told["counts"] == {"lost": 1, "gained": 0, "new": 1}


async def test_the_route_says_no_history_rather_than_narrowing_nothing(monkeypatch):
    _wired(monkeypatch, allowed=frozenset({"sales"}))
    none = {
        "has_history": False,
        "cutoff": CUTOFF.isoformat(),
        "base": None,
        "terms": None,
        "counts": None,
    }
    _drifted(monkeypatch, none)
    assert await glossary_router.ontology_drift(_REQUEST) == none


def test_the_route_is_published_under_the_glossary():
    paths = {getattr(r, "path", None) for r in glossary_router.router.routes}
    assert any(p and p.endswith("/drift") for p in paths)
