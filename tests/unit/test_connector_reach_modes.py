# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""REQ-951: a Connector declares its reach mechanism (how rows are OBTAINED) and materializability
(orthogonal). ATTACH_* = engine reads live; DIRECT/FETCH = Provisa reads + lands a refreshed replica
the engine reads (there is no engine 'LAND' — the engine never writes)."""

from __future__ import annotations

from provisa.core.models import Source, SourceType
from provisa.federation.connector import (
    Mechanism,
    TrinoOpenapiConnector,
    TrinoPostgresConnector,
    WarehouseNativeConnector,
)
from provisa.federation.engine import build_trino_engine
from provisa.federation.strategy import Strategy, federate


def test_relational_is_attach_rw_and_materializable():
    c = TrinoPostgresConnector()
    assert c.mechanism is Mechanism.ATTACH_RW  # engine reads the live source, writable
    assert c.reach_modes == frozenset({Mechanism.ATTACH_RW})
    assert c.materializable is True  # can ALSO be cached — orthogonal to the reach mechanism


def test_api_source_is_fetch():
    # openapi: Provisa fetches + lands a replica; the engine reads it → FETCH, not a live attach.
    assert TrinoOpenapiConnector().mechanism is Mechanism.FETCH


def test_self_only_engine_source_is_direct_not_engine_land():
    # A warehouse/self-only engine can't attach an external source live; Provisa reads it (DIRECT)
    # and lands the replica — the engine never lands its own data.
    assert WarehouseNativeConnector("wh", "wh").mechanism is Mechanism.DIRECT


def test_federate_attach_is_virtual_direct_fetch_is_materialized():
    pg = Source(
        id="pg", type=SourceType.postgresql, host="h", port=5432, database="d", username="u"
    )
    api = Source(
        id="api", type=SourceType.openapi, host="h", port=1, database="d", base_url="http://x"
    )
    eng = build_trino_engine()
    assert federate(pg, eng) is Strategy.VIRTUAL  # ATTACH_RW → engine reads live
    assert federate(api, eng) is Strategy.MATERIALIZED  # FETCH → materialized replica
