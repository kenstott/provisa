# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The groupexposure demo's glossary (REQ-2140, REQ-1641): demo/named/groupexposure/config.yaml
loaded as a deployment loads it, into a control plane of the test's own. A group of connected
clients -- the term the tour opens the Ontology on -- is grounded: the group owner_group records
for each owner, its entity and its name, is part of it, so its page reaches those columns instead
of saying it reaches none (REQ-1387, DOWN IS FROM BROADER TO NARROWER)."""

# Requirements: REQ-2140, REQ-1641, REQ-1387

from __future__ import annotations

import contextlib
from pathlib import Path

import pytest
import yaml
from sqlalchemy import insert, select

from provisa.core import secrets_store
from provisa.core.config_loader import apply_config, parse_config_dict, read_config_with_includes
from provisa.core.models import DERIVED_SOURCE_ID
from provisa.core.repositories import glossary as glossary_repo
from provisa.core.schema_org import glossary_term_edges, sources

_DEMO = Path(__file__).resolve().parents[2] / "demo" / "named" / "groupexposure"
CONFIG = _DEMO / "config.yaml"
TERM = "group of connected clients"
#: The columns the group of connected clients reaches through its parts: the group at the top of
#: each owner's chain, as owner_group lands it from Wikidata.
PARTS = {"group entity": "group_entity", "group name": "group_name"}


@pytest.fixture
async def loaded(monkeypatch, tmp_path, control_plane):
    """The demo's config applied to an empty control plane: (terms by name, groundings, edges)."""

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
    for name, value in store_namespaces(_DEMO).items():
        monkeypatch.setenv(name, value)
    monkeypatch.setenv("PROVISA_NAMED_DEMO_DIR", str(tmp_path))
    plane = control_plane.org()
    async with plane.acquire() as conn:
        # The virtual views' sentinel source, which the boot seeds and the config's views register
        # against.
        await conn.execute_core(insert(sources).values(id=DERIVED_SOURCE_ID, type="trino"))
        await apply_config(parse_config_dict(read_config_with_includes(CONFIG)), conn)
        terms = {t["name"]: t["id"] for t in await glossary_repo.list_terms(conn)}
        names = {tid: name for name, tid in terms.items()}
        edges = {
            (names[e.from_term_id], e.rel_type, names[e.to_term_id])
            for e in (await conn.execute_core(select(glossary_term_edges))).fetchall()
        }
        details = {name: await glossary_repo.get_term(conn, terms[name]) for name in {TERM, *PARTS}}
    return details, edges


def test_the_tour_opens_the_ontology_on_the_term():
    tour = yaml.safe_load((_DEMO / "tour.yaml").read_text())
    assert TERM in {step.get("term") for step in tour["steps"]}


async def test_a_group_of_connected_clients_is_grounded(loaded):
    details, _ = loaded
    term = details[TERM]
    # What the term page reads: approved and grounded, so no "reaches no physical column" notice.
    assert term["approved"]
    assert term["grounding"] == "grounded"


async def test_it_is_grounded_in_the_group_owner_group_records(loaded):
    details, edges = loaded
    for part, column in PARTS.items():
        assert (part, "PART_OF", TERM) in edges
        assert details[part]["approved"]
        assert ("owner_group", column) in {
            (ref["table_name"], ref["column_name"]) for ref in details[part]["refs"]
        }
    # Related to the large-exposure limit, which grounds nothing: its grounding is its parts'.
    assert (TERM, "RELATED_TO", "large exposure") in edges
