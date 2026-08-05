# Copyright (c) 2026 Kenneth Stott
# Canary: 0e982c18-3b06-4be5-8561-3d9e93ee2a8b
# Canary: placeholder
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the executable file in the root directory of this source tree.

"""Catalog-name agreement: what a source is recorded under must be what it is created as.

``state.source_catalogs[id]`` is what ``catalog_for()`` returns and what every compiled engine
query names; ``create_catalog`` is what physically makes the catalog. When those two disagree the
source registers cleanly and every query for it then dies on CATALOG_NOT_FOUND — which is exactly
what a SharePoint source did, because its Azure tenant GUID lives in ``database`` and the recorded
name was derived from that field.

These tests drive the real population path rather than restating its arithmetic; a formula copied
into the test cannot detect the formula changing.
"""

from __future__ import annotations

import pytest

from provisa.core.catalog import _to_catalog_name
from provisa.core.models import ProvisaConfig, Source, SourceType


def _config(*sources: Source) -> ProvisaConfig:
    return ProvisaConfig(sources=list(sources), domains=[], tables=[], roles=[])


@pytest.fixture()
def state(monkeypatch):
    from provisa.api.app import state as app_state

    monkeypatch.setattr(app_state, "source_catalogs", {}, raising=False)
    monkeypatch.setattr(app_state, "source_types", {}, raising=False)
    monkeypatch.setattr(app_state, "source_dialects", {}, raising=False)
    monkeypatch.setattr(app_state, "source_cache", {}, raising=False)
    monkeypatch.setattr(app_state, "source_federation_hints", {}, raising=False)
    monkeypatch.setattr(app_state, "federation_engine", None, raising=False)
    monkeypatch.setattr(app_state, "org_id", "default", raising=False)
    return app_state


@pytest.mark.parametrize(
    "source",
    [
        # The SharePoint case: `database` carries the Azure tenant GUID.
        Source(
            id="e2e-sharepoint",
            type=SourceType.sharepoint,
            database="5d2609cc-7eff-4b82-8f83-f0b28c71fafc",
        ),
        # A remote database name is equally not a catalog name.
        Source(id="pet-store-pg", type=SourceType.postgresql, database="provisa"),
        # No `database` at all — the case that always worked.
        Source(id="inquiries-sqlite", type=SourceType.sqlite, path="./x.sqlite"),
    ],
    ids=["sharepoint-tenant-guid", "postgres-db-name", "no-database"],
)
def test_recorded_catalog_is_the_one_create_catalog_makes(state, source):
    from provisa.api.app_loaders import _populate_source_catalog_names

    _populate_source_catalog_names(_config(source))

    assert state.source_catalogs[source.id] == _to_catalog_name(source.id)


def test_non_default_org_prefixes_the_source_id_derived_name(state):
    from provisa.api.app_loaders import _populate_source_catalog_names
    from provisa.api.org_runtime import current_org

    token = current_org.set("tenant-a")
    try:
        _populate_source_catalog_names(
            _config(Source(id="e2e-sharepoint", type=SourceType.sharepoint, database="tenant-guid"))
        )
    finally:
        current_org.reset(token)

    assert state.source_catalogs["e2e-sharepoint"] == "org_tenant-a__e2e_sharepoint"
