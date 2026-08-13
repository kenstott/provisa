# Copyright (c) 2026 Kenneth Stott
# Canary: d9494d23-9e9d-497c-8586-a46b69e6e949
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Trino's gsheets catalog connector (REQ-947).

Google Sheets was reachable only from the DuckDB engine, so a Trino-pinned deployment showed the type
disabled in the source dropdown. Trino ships a ``gsheets`` connector; these tests pin the catalog it
produces and the reach that follows from registering it.
"""

from __future__ import annotations

import pytest

from provisa.core.catalog import _build_catalog_properties
from provisa.core.models import Source, SourceType
from provisa.federation.engine import live_source_types, reachable_source_types
from provisa.federation.trino_connectors import TRINO_CONNECTORS, trino_connector_name


def _source(*, database: str = "meta-sheet-id", mapping: dict | None = None) -> Source:
    return Source(
        id="sheets-1",
        type=SourceType.google_sheets,
        database=database,
        mapping={"credentials_json": "/etc/provisa/sa.json"} if mapping is None else mapping,
    )


class TestRegistration:
    def test_registered_under_the_gsheets_connector_name(self):
        assert trino_connector_name("google_sheets") == "gsheets"

    def test_reachable_on_trino(self):
        assert "google_sheets" in reachable_source_types("trino")

    def test_live_on_trino(self):
        # ATTACH_R — Trino queries the sheet in place, so the dropdown tags it LIVE, not REPLICA.
        assert "google_sheets" in live_source_types("trino")

    def test_read_only(self):
        assert TRINO_CONNECTORS["google_sheets"].capability().write is False


class TestCatalogProperties:
    def test_metadata_sheet_id_comes_from_database(self):
        props = _build_catalog_properties(_source(database="1AbC_xyz"), "")
        assert props["gsheets.metadata-sheet-id"] == "1AbC_xyz"

    def test_credentials_path_comes_from_mapping(self):
        props = _build_catalog_properties(_source(), "")
        assert props["gsheets.credentials-path"] == "/etc/provisa/sa.json"

    def test_case_insensitive_name_matching(self):
        props = _build_catalog_properties(_source(), "")
        assert props["case-insensitive-name-matching"] == "true"

    def test_missing_metadata_sheet_id_raises(self):
        with pytest.raises(ValueError, match="metadata sheet id"):
            _build_catalog_properties(_source(database=""), "")

    def test_missing_credentials_raises(self):
        with pytest.raises(ValueError, match="credentials_json"):
            _build_catalog_properties(_source(mapping={}), "")
