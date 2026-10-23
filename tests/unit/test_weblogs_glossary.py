# Copyright (c) 2026 Kenneth Stott
# Canary: 144c2faa-23d3-42a2-b842-28a469ac6b42
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The weblogs demo's glossary (REQ-1641, REQ-1387): demo/named/weblogs/config.yaml loaded as a
deployment loads it, into a control plane of the test's own. The terms its columns derive are
approved -- live: in service, defined and grounded -- except four left Proposed on purpose, and
its abstract terms (the shop's ontology) are live, grounded through their edges."""

# Requirements: REQ-1641, REQ-1387

from __future__ import annotations

from types import SimpleNamespace

import contextlib
from pathlib import Path

import pytest
from sqlalchemy import insert, select

from provisa.core import secrets_store
from provisa.core.config_loader import apply_config, parse_config_dict, read_config_with_includes
from provisa.core.repositories import glossary as glossary_repo
from provisa.core.models import DERIVED_SOURCE_ID
from provisa.core.schema_org import glossary_term_edges, sources

CONFIG = Path(__file__).resolve().parents[2] / "demo" / "named" / "weblogs" / "config.yaml"

#: Derived terms left undefined so the Glossary page shows them as Proposed: a log record's raw
#: line, and "query", which joins two meanings for a curator to split. A record's bookkeeping key
#: (file, line offset, entry) is its log table's Class, not terms of its own (REQ-2141).
PROPOSED = {"raw", "query"}
#: The shop's ontology: abstract terms, grounded in derived ones through their edges.
ONTOLOGY = {
    "shop operations",
    "web request",
    "database statement",
    "customer journey",
    "server error",
    "database error",
    "page latency",
    "slow page",
    "abandoned interest",
}


@pytest.fixture
async def loaded(monkeypatch, tmp_path, control_plane):
    """The demo's config applied to an empty control plane: (terms by name, live ids, edges)."""

    @contextlib.asynccontextmanager
    async def _unbound():
        yield

    monkeypatch.setattr(secrets_store, "bound_to_request_org", _unbound)
    from provisa.core import secrets_providers

    # A deployment of one organisation, as the API layer says at import (provisa.api.app): run
    # alone, nothing here imports it, and storing a source's secret asks.
    monkeypatch.setattr(secrets_providers, "_many_organisations", lambda: False)
    from provisa.core.demo_bundles import store_namespaces

    # As a launcher running the demo's own stores sets them (start-ui-install.sh --demo).
    for name, value in store_namespaces(CONFIG.parent).items():
        monkeypatch.setenv(name, value)
    monkeypatch.setenv("PROVISA_NAMED_DEMO_DIR", str(tmp_path))
    plane = control_plane.org()
    async with plane.acquire() as conn:
        # The virtual views' sentinel source, which the boot seeds and the config's views register
        # against.
        await conn.execute_core(insert(sources).values(id=DERIVED_SOURCE_ID, type="trino"))
        await apply_config(parse_config_dict(read_config_with_includes(CONFIG)), conn)
        # REQ-2141: abstract is derived (a term grounding no column and no table), as listed.
        terms = {
            t["name"]: SimpleNamespace(id=t["id"], name=t["name"], is_abstract=t["is_abstract"])
            for t in await glossary_repo.list_terms(conn)
        }
        live = await glossary_repo.live_ids(conn)
        domains = await glossary_repo.term_domains(conn)
        names = {t.id: n for n, t in terms.items()}
        edges = {
            (names[e.from_term_id], e.rel_type, names[e.to_term_id])
            for e in (await conn.execute_core(select(glossary_term_edges))).fetchall()
        }
    return terms, live, domains, edges


async def test_the_derived_terms_are_approved_but_two_left_proposed(loaded):
    terms, live, _domains, _edges = loaded
    derived = {name for name, t in terms.items() if not t.is_abstract}
    assert PROPOSED <= derived
    assert {name for name in derived if terms[name].id not in live} == PROPOSED
    # every non-key column's business name, and each keyed table's Class (REQ-2141)
    assert len(derived) == 72


async def test_the_views_name_their_columns_so_they_derive_the_shops_own_terms(loaded):
    """A view column named after a generic word (name, status, price) qualified its term with
    the view's name ("view viewed not bought name"); named in the view's SQL (product_name, ...)
    it derives the term the base table's column does, or a plain one of its own."""
    terms, live, _domains, _edges = loaded
    assert not [name for name in terms if name.startswith("view ")]
    for name in (
        "page status",
        "customer name",
        "region",
        "product name",
        "product category",
        "product price",
    ):
        assert terms[name].id in live, name


async def test_the_ontology_is_abstract_live_and_in_the_shop_domain(loaded):
    terms, live, domains, _edges = loaded
    abstract = {name for name, t in terms.items() if t.is_abstract}
    assert abstract == ONTOLOGY
    assert all(terms[name].id in live for name in ONTOLOGY)
    assert all(domains[terms[name].id] == {"shop"} for name in ONTOLOGY)


async def test_the_ontology_is_grounded_in_the_derived_terms(loaded):
    _terms, _live, _domains, edges = loaded
    assert {
        ("path", "PART_OF", "web request"),
        ("method", "PART_OF", "web request"),
        ("access log status", "PART_OF", "web request"),
        ("request", "PART_OF", "web request"),
        ("statement", "PART_OF", "database statement"),
        ("duration ms", "PART_OF", "database statement"),
        ("table name", "PART_OF", "database statement"),
        ("views", "PART_OF", "customer journey"),
        ("order", "PART_OF", "customer journey"),
        ("region", "PART_OF", "customer journey"),
        ("server error", "KIND_OF", "web request"),
        ("database error", "KIND_OF", "database statement"),
        ("page latency", "DERIVED_FROM", "duration ms"),
        ("slow page", "DERIVED_FROM", "page latency"),
        ("web request", "PART_OF", "shop operations"),
    } <= edges
