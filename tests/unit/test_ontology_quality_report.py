# Copyright (c) 2026 Kenneth Stott
# Canary: 9e3b5c71-0a2f-4d86-b4e1-6c8f2a7d3b05
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The Ontology quality standard report (REQ-1386, ONTOLOGY QUALITY IS A STANDARD REPORT).

The three ops report views are created on the control plane's PostgreSQL over a glossary built
for each case, and read back: a hand-built model whose every metric is known, and random models
checked against the REQ-1387 functions the views restate (grounding, approvals_that_ground, the
ontology list's related_to count).
"""

# Requirements: REQ-1386, REQ-1387

from __future__ import annotations

import heapq
import random
from decimal import Decimal

from sqlalchemy import delete, insert, text

from provisa.api._meta_views import _OPS_REPORT_VIEWS
from provisa.api._ontology_quality_views import HUB_MIN_REACHERS, ONTOLOGY_QUALITY_VIEWS
from provisa.api._ontology_views import ONTOLOGY_TABLE_VIEWS
from provisa.api.startup_seed import _adapt_view_ddl
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

_SALES_TABLE = 1
_HR_TABLE = 2


def test_the_report_views_are_ops_reports_and_read_the_ontology_schemas_grounding():
    assert set(ONTOLOGY_QUALITY_VIEWS) <= set(_OPS_REPORT_VIEWS)
    # Grounding is read, never recomputed: the views take it from _terms_ontology.
    detail = ONTOLOGY_QUALITY_VIEWS["ontology_term_quality"]
    assert "FROM _terms_ontology" in detail
    assert "clean AS" not in detail and "grounded AS" not in detail


async def _create_views(conn) -> None:
    for ddl in (*ONTOLOGY_TABLE_VIEWS.values(), *ONTOLOGY_QUALITY_VIEWS.values()):
        await conn.execute(_adapt_view_ddl(ddl))


async def _base(conn) -> None:
    """Domains (sales stewarded, sales_eu under it, hr not) and one table in each of two."""
    await conn.execute_core(
        insert(domains).values(id="sales", description="", steward="sales_steward")
    )
    await conn.execute_core(
        insert(domains).values(id="sales_eu", description="", parent_domain_id="sales")
    )
    await conn.execute_core(insert(domains).values(id="hr", description=""))
    await conn.execute_core(insert(sources).values(id="s1", type="postgres"))
    for tid, domain, name in ((_SALES_TABLE, "sales_eu", "orders"), (_HR_TABLE, "hr", "staff")):
        await conn.execute_core(
            insert(registered_tables).values(
                id=tid, source_id="s1", domain_id=domain, schema_name="public", table_name=name
            )
        )


async def _term(conn, tid, name, *, approved=False, defined=True, domain=None, ref=None, **flags):
    await conn.execute_core(
        insert(glossary_terms).values(
            id=tid,
            name=name,
            definition=f"The {name}." if defined else None,
            approved=approved,
            **flags,
        )
    )
    if ref is not None:
        await conn.execute_core(
            insert(glossary_term_refs).values(term_id=tid, table_id=ref[0], column_name=ref[1])
        )
    if domain is not None:
        await conn.execute_core(insert(glossary_term_domains).values(term_id=tid, domain_id=domain))


async def _edge(conn, a, b, rel_type):
    await conn.execute_core(
        insert(glossary_term_edges).values(from_term_id=a, to_term_id=b, rel_type=rel_type)
    )


_HUB_REACHERS = list(range(12, 12 + HUB_MIN_REACHERS))


async def _model(conn) -> None:
    """amount (a column) is a kind of price, a kind of revenue; turnover is revenue's synonym.
    money reaches amount only through the draft cost_basis. orphan is only RELATED_TO revenue.
    detail (approved, a hub) is part of five terms and reaches data through salary."""
    await _term(conn, 1, "amount", defined=False, ref=(_SALES_TABLE, "amount"))
    await _term(conn, 2, "price", approved=True, domain="sales")
    await _term(conn, 3, "revenue", approved=True, domain="sales")
    await _term(conn, 4, "cost_basis", domain="sales_eu")
    await _term(conn, 5, "money", approved=True, domain="sales_eu")
    await _term(conn, 6, "orphan", domain="sales_eu")
    await _term(conn, 10, "detail", approved=True, domain="hr")
    await _term(conn, 11, "salary", defined=False, ref=(_HR_TABLE, "salary"))
    for tid in _HUB_REACHERS:
        await _term(conn, tid, f"record_{tid}", domain="hr")
    await _term(conn, 17, "turnover", domain="sales")
    await _term(conn, 18, "legacy", domain="sales", retired=True)
    await _edge(conn, 1, 2, "KIND_OF")
    await _edge(conn, 2, 3, "KIND_OF")
    await _edge(conn, 1, 4, "KIND_OF")
    await _edge(conn, 4, 5, "KIND_OF")
    await _edge(conn, 6, 3, "RELATED_TO")
    await _edge(conn, 11, 10, "KIND_OF")
    for tid in _HUB_REACHERS:
        await _edge(conn, 10, tid, "PART_OF")
    await _edge(conn, 17, 3, "SYNONYM_OF")


async def test_each_metric_is_read_from_the_model(control_plane):
    db = control_plane.org()
    try:
        async with db.acquire() as conn:
            await _base(conn)
            await _model(conn)
            await _create_views(conn)

            async def rows(sql):
                return (await conn.execute_core(text(sql))).fetchall()

            detail = {
                r.term_id: r
                for r in await rows("SELECT * FROM ontology_term_quality ORDER BY term_id")
            }
            # In-service terms only: the retired one is not measured.
            assert 18 not in detail
            assert set(detail) == {1, 2, 3, 4, 5, 6, 10, 11, 17, *_HUB_REACHERS}

            def row(tid):
                r = detail[tid]
                return (
                    r.domain_id,
                    r.steward,
                    r.status,
                    r.grounding,
                    r.hops_to_data,
                    r.related_to,
                    r.only_related_to,
                    r.is_hub,
                    r.hubs_between,
                    r.only_through_hub,
                    r.finalised_by,
                    r.undefined_on_path,
                )

            # sales_eu has no steward of its own: it answers to sales's.
            S = "sales_steward"
            assert row(1) == ("sales_eu", S, "draft", "grounded", 0, 0, False, False, 0, False,
                              None, True)  # fmt: skip
            assert row(2) == ("sales", S, "approved", "grounded", 1, 0, False, False, 0, False,
                              None, False)  # fmt: skip
            assert row(3) == ("sales", S, "approved", "grounded", 2, 1, False, False, 0, False,
                              None, False)  # fmt: skip
            # A synonym is the same meaning: no step toward data.
            assert row(17) == ("sales", S, "draft", "grounded", 2, 0, False, False, 0, False,
                               None, False)  # fmt: skip
            assert row(4) == ("sales_eu", S, "draft", "grounded", 1, 0, False, False, 0, False,
                              None, False)  # fmt: skip
            # Every path from money passes through the draft cost_basis, whose approval grounds it.
            assert row(5) == ("sales_eu", S, "approved", "grounded_pending", 2, 0, False, False,
                              0, False, "cost_basis", False)  # fmt: skip
            assert row(6) == ("sales_eu", S, "draft", "not_grounded", None, 1, True, False, 0,
                              False, None, False)  # fmt: skip
            assert row(10) == ("hr", None, "approved", "grounded", 1, 0, False, True, 0, False,
                               None, False)  # fmt: skip
            assert row(11) == ("hr", None, "draft", "grounded", 0, 0, False, False, 0, False,
                               None, True)  # fmt: skip
            for tid in _HUB_REACHERS:
                assert row(tid) == ("hr", None, "draft", "grounded", 2, 0, False, False, 1, True,
                                    None, False)  # fmt: skip

            # One row per (term, domain), keyed by both.
            assert detail[1].id == "1:sales_eu"

            placeholders = {
                (r.placeholder, r.term_id, r.other_term_id, r.domain_id, r.steward,
                 r.related_to_review)
                for r in await rows("SELECT * FROM ontology_placeholders")
            }  # fmt: skip
            assert placeholders == {
                # The RELATED_TO edge, once for each end's domain.
                ("related_to_edge", 6, 3, "sales_eu", S, 6),
                ("related_to_edge", 6, 3, "sales", S, 6),
                ("only_related_to", 6, None, "sales_eu", S, 6),
            }

            summary = {
                (r.scope, r.scope_id): r for r in await rows("SELECT * FROM ontology_quality")
            }
            assert set(summary) == {
                ("domain", "sales"),
                ("domain", "sales_eu"),
                ("domain", "hr"),
                ("steward", S),
                ("steward", None),
            }
            steward = summary[("steward", S)]
            assert steward.id == "steward:sales_steward"
            assert (
                steward.in_service_terms,
                steward.grounded_terms,
                steward.grounded_pct,
                steward.grounded_pending_terms,
                steward.grounded_pending_pct,
                steward.approved_terms,
                steward.approved_grounded_terms,
                steward.approved_grounded_pct,
                steward.related_to_edges,
                steward.only_related_to_terms,
                steward.max_hops_to_data,
                steward.pending_finalisable_terms,
                steward.undefined_on_path_terms,
            ) == (7, 5, Decimal("71.4"), 1, Decimal("14.3"), 3, 3, Decimal("42.9"), 1, 1, 2, 1, 1)
            # amount 0, price 1, revenue 2, cost_basis 1, money 2, turnover 2: the six reaching data
            assert steward.avg_hops_to_data == Decimal("1.33")
            hr = summary[("domain", "hr")]
            assert (
                hr.in_service_terms,
                hr.grounded_pct,
                hr.approved_grounded_terms,
                hr.related_to_edges,
                hr.hub_paths,
                hr.only_through_hub_terms,
                hr.undefined_on_path_terms,
                hr.max_hops_to_data,
            ) == (2 + HUB_MIN_REACHERS, Decimal("100.0"), 1, 0, HUB_MIN_REACHERS,
                  HUB_MIN_REACHERS, 1, 2)  # fmt: skip
            assert summary[("domain", "sales")].related_to_edges == 1
            assert summary[("domain", "sales_eu")].related_to_edges == 1
            # Unstewarded hr's terms count under no steward.
            assert summary[("steward", None)].in_service_terms == 2 + HUB_MIN_REACHERS
    finally:
        await db.close()


async def test_one_reacher_short_of_the_rule_is_no_hub(control_plane):
    db = control_plane.org()
    try:
        async with db.acquire() as conn:
            await _base(conn)
            await _term(conn, 10, "detail", approved=True, domain="hr")
            await _term(conn, 11, "salary", ref=(_HR_TABLE, "salary"))
            await _edge(conn, 11, 10, "KIND_OF")
            reachers = list(range(20, 20 + HUB_MIN_REACHERS - 1))
            for tid in reachers:
                await _term(conn, tid, f"r{tid}", domain="hr")
                await _edge(conn, 10, tid, "PART_OF")
            await _create_views(conn)
            hubs = (
                await conn.execute_core(
                    text(
                        "SELECT term_id FROM ontology_term_quality WHERE is_hub OR hubs_between > 0"
                    )
                )
            ).fetchall()
            assert hubs == []
    finally:
        await db.close()


_REL_TYPES = ("KIND_OF", "PART_OF", "VALID_VALUE_OF", "SYNONYM_OF", "RELATED_TO", "DERIVED_FROM")


def _shortest_hops(terms: dict[int, dict], edges: list[tuple[int, int, str]]) -> dict[int, int]:
    """An oracle for hops to data: 0-1 weighted shortest path down, over in-service terms."""
    live = {tid for tid, t in terms.items() if not (t["deprecated"] or t["retired"])}
    up: dict[int, list[tuple[int, int]]] = {}
    for a, b, rel in edges:
        if a not in live or b not in live or a == b:
            continue
        if rel in ("KIND_OF", "PART_OF", "VALID_VALUE_OF"):
            up.setdefault(a, []).append((b, 1))
        elif rel == "SYNONYM_OF":
            up.setdefault(a, []).append((b, 0))
            up.setdefault(b, []).append((a, 0))
    dist = {tid: 0 for tid in live if terms[tid]["ref"]}
    heap = [(0, tid) for tid in dist]
    while heap:
        d, tid = heapq.heappop(heap)
        if d > dist[tid]:
            continue
        for nxt, w in up.get(tid, ()):
            if d + w < dist.get(nxt, 1 << 30):
                dist[nxt] = d + w
                heapq.heappush(heap, (d + w, nxt))
    return dist


async def test_random_models_agree_with_the_req_1387_functions(control_plane):
    rng = random.Random(1387)
    seen: set[str] = set()  # the cases the models produced, so the comparison is not vacuous
    db = control_plane.org()
    try:
        async with db.acquire() as conn:
            await _base(conn)
            await _create_views(conn)
            for _ in range(25):
                for table in (glossary_term_edges, glossary_term_refs, glossary_term_domains):
                    await conn.execute_core(delete(table))
                await conn.execute_core(delete(glossary_terms))
                terms: dict[int, dict] = {}
                for tid in range(1, 15):
                    status = rng.choice(["draft", "draft", "approved", "approved", "deprecated",
                                         "retired"])  # fmt: skip
                    terms[tid] = {
                        "deprecated": status == "deprecated",
                        "retired": status == "retired",
                        "ref": rng.random() < 0.3,
                    }
                    await _term(
                        conn,
                        tid,
                        f"t{tid:02d}",
                        approved=status == "approved",
                        defined=rng.random() < 0.7,
                        domain=None if terms[tid]["ref"] else "sales",
                        ref=(_SALES_TABLE, f"c{tid}") if terms[tid]["ref"] else None,
                        deprecated=terms[tid]["deprecated"],
                        retired=terms[tid]["retired"],
                    )
                edges: set[tuple[int, int, str]] = set()
                for _n in range(rng.randint(6, 26)):
                    a, b = rng.sample(sorted(terms), 2)
                    edges.add((a, b, rng.choice(_REL_TYPES)))
                for a, b, rel in sorted(edges):
                    await _edge(conn, a, b, rel)

                grounding = await glossary_repo.grounding(conn)
                pending_on = await glossary_repo.pending_on(conn)
                listed = {t["id"]: t for t in await glossary_repo.list_terms(conn)}
                hops = _shortest_hops(terms, sorted(edges))
                view = {
                    r.term_id: r
                    for r in (
                        await conn.execute_core(text("SELECT * FROM ontology_term_quality"))
                    ).fetchall()
                }
                in_service = {t for t, v in terms.items() if not (v["deprecated"] or v["retired"])}
                assert set(view) == in_service
                for tid, r in view.items():
                    assert r.grounding == grounding[tid]
                    # Hops exist exactly where REQ-1387 grounding reaches data.
                    assert (r.hops_to_data is not None) == (grounding[tid] != "not_grounded")
                    assert r.hops_to_data == hops.get(tid)
                    assert r.finalised_by == (
                        ", ".join(sorted(f"t{d:02d}" for d in pending_on[tid]))
                        if pending_on.get(tid)
                        else None
                    )
                    assert r.related_to == listed[tid]["related_to"]
                    seen |= {
                        name
                        for name, hit in (
                            ("finalised", r.finalised_by is not None),
                            ("pending_unfinalisable", r.grounding == "grounded_pending"
                             and r.finalised_by is None),
                            ("synonym_hop", r.hops_to_data is not None and r.hops_to_data >= 2),
                            ("only_related_to", r.only_related_to),
                            ("not_grounded", r.grounding == "not_grounded"),
                        )
                        if hit
                    }  # fmt: skip
                    links = [e for e in edges if tid in (e[0], e[1])]
                    assert r.only_related_to == (
                        grounding[tid] == "not_grounded"
                        and bool(links)
                        and all(e[2] == "RELATED_TO" for e in links)
                    )
    finally:
        await db.close()
    assert seen >= {"finalised", "synonym_hop", "only_related_to", "not_grounded"}, seen
