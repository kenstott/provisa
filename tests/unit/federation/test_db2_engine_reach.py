# Copyright (c) 2026 Kenneth Stott
# Canary: 650acead-5382-43d3-b2e8-a113c1a7dbcf
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""What a Db2 federation engine reaches (REQ-1922, REQ-2236): only its own store. It reads no
source in place (no Db2 federation server or nickname is created), every reachable source lands
into it as a replica, it reads no other region's replicas, and a source it cannot land is refused
by name."""

# Requirements: REQ-1922, REQ-2236, REQ-841

from __future__ import annotations

import pytest

from provisa.core.engine_kinds import REGION_READERS
from provisa.core.models import Source, SourceType
from provisa.federation import engine as engine_mod
from provisa.federation.engine import DriverClass, UnreachableSource, build_engine


@pytest.fixture
def db2(monkeypatch: pytest.MonkeyPatch) -> engine_mod.FederationEngine:
    monkeypatch.setattr(engine_mod, "require_engine_extra", lambda kind: None)
    monkeypatch.setenv("PROVISA_ENGINE_URL", "db2+ibm_db://u:p@h:50000/db")
    return build_engine("db2")


def test_db2_engine_is_self_only(db2: engine_mod.FederationEngine) -> None:
    assert db2.dialect == "db2"
    assert db2.driver_class() is DriverClass.SELF_ONLY
    assert not any(c.reads_in_place for c in db2.connectors.values())


def test_db2_engine_lands_a_relational_source(db2: engine_mod.FederationEngine) -> None:
    assert db2.reachable("postgresql") is True
    assert db2.connector_for("postgresql").reads_in_place is False


def test_db2_engine_refuses_a_source_it_cannot_land_by_name(
    db2: engine_mod.FederationEngine,
) -> None:
    hubspot = Source(id="crm", type=SourceType.hubspot, host="h", port=443, database="d")
    assert db2.reachable("hubspot") is False
    with pytest.raises(UnreachableSource, match="'db2'.*'hubspot'"):
        db2.resolve(hubspot)


def test_db2_engine_reads_no_other_regions_replicas(db2: engine_mod.FederationEngine) -> None:
    assert db2.backend.reads_other_regions is False
    assert "db2" not in REGION_READERS


def test_db2_engine_names_tables_schema_qualified(db2: engine_mod.FederationEngine) -> None:
    # Db2 LUW takes a three-part name only for a federated object.
    assert db2.catalog_qualified is False
