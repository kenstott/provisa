# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""REQ-947: a Connector declares the full SET of reach modes it supports (reach_modes), not just a
single mechanism — e.g. Trino postgres is both virtual (ATTACH) and materializable (LAND)."""

from __future__ import annotations

from provisa.federation.connector import (
    Mechanism,
    TrinoOpenapiConnector,
    TrinoPostgresConnector,
)
from provisa.federation.engine import build_trino_engine
from provisa.federation.strategy import Strategy, federate

from provisa.core.models import Source, SourceType


def test_reach_modes_defaults_to_primary_mechanism():
    # sqlite/openapi (PG-cache) are materialized only — reach_modes is just {LAND}.
    assert TrinoOpenapiConnector().reach_modes == frozenset({Mechanism.LAND})


def test_trino_relational_is_virtual_and_materializable():
    # The example: Trino's pg connector identifies BOTH a virtual and a materializable reach.
    modes = TrinoPostgresConnector().reach_modes
    assert modes == frozenset({Mechanism.ATTACH, Mechanism.LAND})
    assert Mechanism.ATTACH in modes and Mechanism.LAND in modes


def test_primary_mechanism_unchanged_so_federate_behavior_preserved():
    # Declaring the wider set does NOT change the default federate() strategy (still VIRTUAL for pg).
    assert TrinoPostgresConnector().mechanism is Mechanism.ATTACH
    pg = Source(
        id="pg", type=SourceType.postgresql, host="h", port=5432, database="d", username="u"
    )
    assert federate(pg, build_trino_engine()) is Strategy.VIRTUAL
