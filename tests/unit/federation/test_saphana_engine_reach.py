# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""What a SAP HANA federation engine reaches (REQ-1922, REQ-2236): only its own store. It reads no
source in place (no Smart Data Access remote source or virtual table is created), every reachable
source lands into it as a replica, it reads no other region's replicas, and a source it cannot
land is refused by name."""

# Requirements: REQ-1922, REQ-2236, REQ-841

from __future__ import annotations

import pytest

from provisa.core.engine_kinds import REGION_READERS
from provisa.core.models import Source, SourceType
from provisa.federation import engine as engine_mod
from provisa.federation.engine import DriverClass, UnreachableSource, build_engine


@pytest.fixture
def hana(monkeypatch: pytest.MonkeyPatch) -> engine_mod.FederationEngine:
    monkeypatch.setattr(engine_mod, "require_engine_extra", lambda kind: None)
    monkeypatch.setenv("PROVISA_ENGINE_URL", "hana+hdbcli://u:p@h:39015/db")
    return build_engine("saphana")


def test_saphana_engine_is_self_only(hana: engine_mod.FederationEngine) -> None:
    assert hana.dialect == "saphana"
    assert hana.driver_class() is DriverClass.SELF_ONLY
    assert not any(c.reads_in_place for c in hana.connectors.values())


def test_saphana_engine_lands_a_relational_source(hana: engine_mod.FederationEngine) -> None:
    assert hana.reachable("postgresql") is True
    assert hana.connector_for("postgresql").reads_in_place is False


def test_saphana_engine_lands_a_hana_source_too(hana: engine_mod.FederationEngine) -> None:
    # A HANA source is replicated like any other: no remote source is attached in place.
    assert hana.reachable("saphana") is True
    assert hana.connector_for("saphana").reads_in_place is False


def test_saphana_engine_refuses_a_source_it_cannot_land_by_name(
    hana: engine_mod.FederationEngine,
) -> None:
    stripe = Source(id="billing", type=SourceType.stripe, host="h", port=443, database="d")
    assert hana.reachable("stripe") is False
    with pytest.raises(UnreachableSource, match="'saphana'.*'stripe'"):
        hana.resolve(stripe)


def test_saphana_engine_reads_no_other_regions_replicas(hana: engine_mod.FederationEngine) -> None:
    assert hana.backend.reads_other_regions is False
    assert "saphana" not in REGION_READERS


def test_saphana_engine_names_tables_schema_qualified(hana: engine_mod.FederationEngine) -> None:
    # A three-part name reaches another tenant database only with cross-database access enabled.
    assert hana.catalog_qualified is False
