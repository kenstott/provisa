# Copyright (c) 2026 Kenneth Stott
# Canary: 6b2e9d41-3f7c-4a85-9e1d-0c4b7a2f8e53
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-862: on a native engine the ops signals are defined over the engine's own telemetry folder
(otel.signals.<table>), never attached as the seeded provisa-otel source: that row names no
Iceberg location, and its attach scanned iceberg_scan('None') and failed at every boot."""

from __future__ import annotations

from types import SimpleNamespace

from provisa.federation.native_backend import NativeEngineBackend


def test_the_walk_attaches_no_provisa_otel_table_and_is_complete():
    attached = []
    backend = NativeEngineBackend(SimpleNamespace(name="duckdb"))
    backend._runtime = SimpleNamespace(attach_source=attached.append)
    otel = SimpleNamespace(id="provisa-otel", type=SimpleNamespace(value="iceberg"), path=None)
    config = SimpleNamespace(
        sources=[otel],
        tables=[
            SimpleNamespace(
                source_id="provisa-otel",
                schema_name="signals",
                table_name=name,
                region=None,
            )
            for name in ("traces", "mv_refresh_lineage")
        ],
    )
    state = SimpleNamespace(
        source_catalogs={"provisa-otel": "otel"},
        replica_routes=SimpleNamespace(kept_for_good=set()),
        foreign_regions={},
        tables=[],
        runtime_sources={},
        model_db=None,
    )
    assert backend._walk_registry(state, config) is True
    assert attached == []
