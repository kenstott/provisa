# Copyright (c) 2026 Kenneth Stott
# Canary: 17097e84-5fa4-4b9b-b55a-96fe994d2650
# Canary: placeholder
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""Unit tests for DuckDBFilesConnector (REQ-1690): a hand-rolled Python ``read_csv_auto`` scanner
here predated REQ-1690's pgwire bridge and was never converted (verified live: an xlsx-only
``files`` source silently only ever matched/read csv, because the extension in ``source.path`` was
never consulted) — ``files`` is one of ``strategy.py``'s ``_CONNECTOR_PGWIRE_REPLICA`` types (with
sharepoint/splunk), attached LIVE via ``_DuckDBPgwireConnector`` exactly like those two. The old
CSV-specific scan behavior this file used to test (camelCase header aliasing, missing-file
wildcard fallback) moved into the bundled Calcite ``FileSchemaFactory`` (LINQ4J-side, not
Python-reachable); the path glob-stripping piece that stayed on the Python side is covered by
``test_replica_strategy.py``'s ``test_files_model_json_strips_glob_from_path`` instead.
"""

from __future__ import annotations

from provisa.core.models import Source, SourceType
from provisa.federation import pgwire_replica as pr
from provisa.federation.connector_base import Mechanism
from provisa.federation.connector_duckdb import DuckDBFilesConnector


def _files_source(**kw) -> Source:
    return Source(**{"id": "local-files", "type": SourceType.files, "path": "/data/reports", **kw})


# ── connector metadata ─────────────────────────────────────────────────────────


def test_files_connector_engine():
    assert DuckDBFilesConnector.engine == "duckdb"


def test_files_connector_source_type():
    assert DuckDBFilesConnector.source_type == "files"


def test_files_connector_mechanism():
    # REQ-1690: files' only real reader is the bundled Calcite FileSchemaFactory, attached live —
    # not a local DuckDB scan.
    assert DuckDBFilesConnector.mechanism == Mechanism.ATTACH_R


# ── details(): pgwire attach (mirrors test_duckdb_splunk_attaches_the_pgwire_endpoint) ─────────


def test_duckdb_files_attaches_the_pgwire_endpoint(monkeypatch):
    """The DuckDB connector attaches the server the endpoint registry started, read-only, under the
    private alias, and names the Calcite schema the tables live in."""
    started: list[str] = []

    def _ensure(source):
        started.append(source.id)
        return pr.PortPair(5441, "127.0.0.1", 5541)

    monkeypatch.setattr(pr, "ensure_endpoint", _ensure)
    src = _files_source(id="local-files")
    details = DuckDBFilesConnector().details(src)
    assert started == ["local-files"]
    assert details["attach"] == (
        "ATTACH 'host=127.0.0.1 port=5441 user=provisa dbname=provisa' "
        'AS "_src_local-files" (TYPE postgres, READ_ONLY)'
    )
    assert details["raw_alias"] == "_src_local-files"
    assert details["remote_schema"] == "local_files"
