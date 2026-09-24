# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""REQ-1824: DuckDBFederationRuntime._require_discovery_ready — the guard that fails fast
(SourceStillStartingError) for a files/sharepoint/splunk source's discovery call instead of
letting `_attached_alias` block up to SERVER_READY_SECONDS on the bundled Calcite server, which
for a large `files` directory (thousands of CSVs) can hang the whole HTTP request far longer than
that even. A real query's own attach (`attach_source`) is untouched by this guard."""

from __future__ import annotations

import pytest

from provisa.core.models import Source, SourceType
from provisa.federation import pgwire_replica as pr
from provisa.federation.duckdb_runtime import DuckDBFederationRuntime


@pytest.fixture()
def runtime():
    rt = DuckDBFederationRuntime()
    yield rt
    rt.close()


def _files_source(**kw) -> Source:
    return Source(**{"id": "kaggle-big", "type": SourceType.files, "path": "/data/kaggle", **kw})


def _postgres_source(**kw) -> Source:
    return Source(**{"id": "pg1", "type": SourceType.postgresql, "host": "db", **kw})


class TestDiscoveryReadyGuard:
    def test_raises_for_not_ready_pgwire_replica_source(self, runtime, monkeypatch):
        def _not_ready(source):
            raise pr.SourceStillStartingError(source.id)

        monkeypatch.setattr(pr, "ensure_endpoint_for_discovery", _not_ready)
        with pytest.raises(pr.SourceStillStartingError, match="kaggle-big"):
            runtime.introspect_tables(_files_source(), "kaggle_big")

    def test_noop_for_a_ready_pgwire_replica_source(self, runtime, monkeypatch):
        calls = []
        monkeypatch.setattr(
            pr, "ensure_endpoint_for_discovery", lambda source: calls.append(source.id)
        )
        # Not attached, so introspect_tables returns [] after the readiness check passes —
        # proves the guard did not itself raise or block.
        assert runtime.introspect_tables(_files_source(), "kaggle_big") == []
        assert calls == ["kaggle-big"]

    def test_skips_the_guard_for_a_non_replica_source_type(self, runtime, monkeypatch):
        def _boom(source):
            raise AssertionError("ensure_endpoint_for_discovery must not be called for postgresql")

        monkeypatch.setattr(pr, "ensure_endpoint_for_discovery", _boom)
        runtime._require_discovery_ready(_postgres_source())  # must not raise / must not call _boom

    def test_introspect_schemas_also_guarded(self, runtime, monkeypatch):
        def _not_ready(source):
            raise pr.SourceStillStartingError(source.id)

        monkeypatch.setattr(pr, "ensure_endpoint_for_discovery", _not_ready)
        with pytest.raises(pr.SourceStillStartingError):
            runtime.introspect_schemas(_files_source())
