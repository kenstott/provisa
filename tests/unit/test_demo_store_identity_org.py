# Copyright (c) 2026 Kenneth Stott
# Canary: 5f1c8e3a-2d7b-4c69-a8e4-0b9d6f2e7c15
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-2068: the store a template's replicas record as built is read for the template org,
whichever org the reader is bound to. A regions seeding node waits for them bound to its boot
org, whose engine is a region engine of its own; read for that org the identity never matched what
the template's builds recorded, and the seed was never written."""

from __future__ import annotations

import pytest

from provisa.api.demo_replicas import store_identity
from provisa.core import request_context
from provisa.core.request_context import current_org, reset_current_org, set_current_org

_STORE = "postgresql://eu-store/replicas"


@pytest.fixture
def engines(monkeypatch: pytest.MonkeyPatch) -> None:
    """The boot org runs a region engine of its own; the template org runs none."""
    urls = {"regions-tier": "duckdb:////data/regions-tier-eu-engine.duckdb"}
    monkeypatch.setattr(
        request_context, "_active_engine_url_provider", lambda: urls.get(current_org.get())
    )


def _as(org: str) -> str:
    token = set_current_org(org)
    try:
        return store_identity("sbxtplretail", _STORE)
    finally:
        reset_current_org(token)


def test_the_identity_is_the_template_orgs_whoever_reads_it(engines):
    assert _as("regions-tier") == _as("sbxtplretail")


def test_a_build_into_the_embedded_file_is_not_one_into_the_regions_store(engines, tmp_path):
    """A regions node's provisioning build runs the template on the embedded file before its
    region lane is read from the model; keyed by the engine alone, the replicas it wrote there were
    recorded as built in the region's store, which never held them."""
    embedded = f"duckdb:///{tmp_path / 'materialize.duckdb'}"
    token = set_current_org("sbxtplretail")
    try:
        assert store_identity("sbxtplretail", embedded) != store_identity("sbxtplretail", _STORE)
        assert store_identity("sbxtplretail", _STORE) == store_identity("sbxtplretail", _STORE)
    finally:
        reset_current_org(token)
