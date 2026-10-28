# Copyright (c) 2026 Kenneth Stott
# Canary: a6ba25f9-ef68-4033-b5ad-97f14856d3cf
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The insurance demo's glossary (REQ-2037, REQ-1641): demo/named/insurance/config.yaml loaded as a
deployment loads it, into a control plane of the test's own. The insurer's vocabulary -- total
insured value, exposure, accumulation, probable maximum loss, expected annual loss -- is approved
and, where an edge leads down to the terms its tables' columns derive, grounded and seen by agents
(REQ-1387)."""

# Requirements: REQ-2037, REQ-1641

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

CONFIG = Path(__file__).resolve().parents[2] / "demo" / "named" / "insurance" / "config.yaml"

#: The insurer's vocabulary: abstract terms.
VOCABULARY = {
    "total insured value",
    "exposure",
    "accumulation",
    "probable maximum loss",
    "expected annual loss",
    "catastrophe peril",
    "line of business",
}
#: The vocabulary with a path down to data (REQ-1387, DOWN IS FROM BROADER TO NARROWER): total
#: insured value is a synonym of the policies' insured value, a policy's TIV is part of the exposure
#: and the accumulation where it sits, and line of business is a synonym of the policies' line.
GROUNDED = {"total insured value", "exposure", "accumulation", "line of business"}
#: No column is one of these, nor a kind, part or value of one: FEMA's ratings rate a county, they
#: are not its expected loss or its perils. Related to the data, they are not grounded in it, so no
#: agent sees them.
UNGROUNDED = VOCABULARY - GROUNDED


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
        # REQ-1387: approved by the config, and seen by an agent (on a path to data).
        statuses = await glossary_repo.term_statuses(conn)
        live = {
            tid
            for tid in (await glossary_repo.agent_visible(conn)).named
            if statuses[tid] == "approved"
        }
        domains = await glossary_repo.term_domains(conn)
        names = {t.id: n for n, t in terms.items()}
        edges = {
            (names[e.from_term_id], e.rel_type, names[e.to_term_id])
            for e in (await conn.execute_core(select(glossary_term_edges))).fetchall()
        }
    return terms, live, domains, edges


async def test_the_vocabulary_is_abstract_live_and_in_the_insurance_domain(loaded):
    terms, live, domains, _edges = loaded
    abstract = {name for name, t in terms.items() if t.is_abstract}
    assert abstract == VOCABULARY
    assert {name for name in VOCABULARY if terms[name].id in live} == GROUNDED
    assert UNGROUNDED == {"probable maximum loss", "expected annual loss", "catastrophe peril"}
    assert all(domains[terms[name].id] == {"insurance"} for name in VOCABULARY)


async def test_the_vocabulary_is_grounded_in_the_terms_the_columns_derive(loaded):
    terms, _live, _domains, edges = loaded
    derived = {name for name, t in terms.items() if not t.is_abstract}
    assert {"insured value", "eal rating", "risk rating", "line"} <= derived
    assert {
        ("total insured value", "SYNONYM_OF", "insured value"),
        ("total insured value", "SYNONYM_OF", "tiv"),
        ("total insured value", "PART_OF", "exposure"),
        ("total insured value", "PART_OF", "accumulation"),
        ("accumulation", "KIND_OF", "exposure"),
        ("probable maximum loss", "RELATED_TO", "accumulation"),
        ("expected annual loss", "RELATED_TO", "eal rating"),
        ("catastrophe peril", "RELATED_TO", "risk rating"),
        ("line of business", "SYNONYM_OF", "line"),
    } <= edges
