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
insured value, exposure, accumulation, probable maximum loss, expected annual loss -- is live: in
service, defined and grounded, through its edges, in the terms its tables' columns derive."""

# Requirements: REQ-2037, REQ-1641

from __future__ import annotations

import contextlib
from pathlib import Path

import pytest
from sqlalchemy import insert, select

from provisa.core import secrets_store
from provisa.core.config_loader import apply_config, parse_config_dict, read_config_with_includes
from provisa.core.repositories import glossary as glossary_repo
from provisa.core.models import DERIVED_SOURCE_ID
from provisa.core.schema_org import glossary_term_edges, glossary_terms, sources

CONFIG = Path(__file__).resolve().parents[2] / "demo" / "named" / "insurance" / "config.yaml"

#: The insurer's vocabulary: abstract terms, grounded in derived ones through their edges.
VOCABULARY = {
    "total insured value",
    "exposure",
    "accumulation",
    "probable maximum loss",
    "expected annual loss",
    "catastrophe peril",
    "line of business",
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
        terms = {
            r.name: r
            for r in (
                await conn.execute_core(
                    select(glossary_terms.c.id, glossary_terms.c.name, glossary_terms.c.is_abstract)
                )
            ).fetchall()
        }
        live = await glossary_repo.live_ids(conn)
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
    assert [name for name in VOCABULARY if terms[name].id not in live] == []
    assert all(domains[terms[name].id] == {"insurance"} for name in VOCABULARY)


async def test_the_vocabulary_is_grounded_in_the_terms_the_columns_derive(loaded):
    terms, _live, _domains, edges = loaded
    derived = {name for name, t in terms.items() if not t.is_abstract}
    assert {"insured value", "eal rating", "risk rating", "line"} <= derived
    assert {
        ("total insured value", "RELATED_TO", "insured value"),
        ("exposure", "RELATED_TO", "total insured value"),
        ("accumulation", "KIND_OF", "exposure"),
        ("probable maximum loss", "RELATED_TO", "accumulation"),
        ("expected annual loss", "RELATED_TO", "eal rating"),
        ("catastrophe peril", "RELATED_TO", "risk rating"),
        ("line of business", "RELATED_TO", "line"),
    } <= edges
