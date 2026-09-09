# Copyright (c) 2026 Kenneth Stott
# Canary: 245400fb-99ea-4b9a-88fb-e7a89a4d54f7
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1690: DuckDB attaches a Calcite connector's bundled pgwire server LIVE.

The real bundle (resolved and cached by the runtime_deps resolver, a real JVM), a real DuckDB
``ATTACH ... (TYPE postgres, READ_ONLY)`` through the connector's ``details()``, and real reads:
catalog introspection over pg_catalog, the binary COPY scan, and filter/projection pushdown. The
``pgwire-file`` bundle stands in for sharepoint/splunk — the same Calcite pgwire server, fed a
directory of CSVs instead of a SaaS API, so the test needs no credentials.
"""

from __future__ import annotations

from pathlib import Path

import duckdb
import pytest

from provisa.core.models import Source, SourceType
from provisa.federation import pgwire_replica as pr
from provisa.federation.connector_duckdb import _DuckDBPgwireConnector

pytestmark = [pytest.mark.integration]


class _FilesOverPgwire(_DuckDBPgwireConnector):
    source_type = "files"


@pytest.fixture
def csv_dir(tmp_path: Path) -> Path:
    d = tmp_path / "data"
    d.mkdir()
    (d / "items.csv").write_text("id,name,qty\n1,alpha,10\n2,beta,20\n3,gamma,30\n")
    return d


@pytest.fixture
def source(csv_dir: Path) -> Source:
    return Source(id="probe-files", type=SourceType.files, path=str(csv_dir))


@pytest.fixture
def attached(source: Source):
    details = _FilesOverPgwire().details(source)  # starts the server, waits for its listener
    con = duckdb.connect()
    con.execute("INSTALL postgres")
    con.execute("LOAD postgres")
    con.execute(details["attach"])
    try:
        yield con, details
    finally:
        con.close()
        pr.stop_all_servers()


def test_attach_details_name_the_endpoint_and_schema(source: Source):
    details = _FilesOverPgwire().details(source)
    try:
        assert details["raw_alias"] == "_src_probe-files"
        assert details["remote_schema"] == "probe_files"
        assert details["attach"].endswith('AS "_src_probe-files" (TYPE postgres, READ_ONLY)')
        # the same source reuses its server: one endpoint, one port
        assert _FilesOverPgwire().details(source) == details
    finally:
        pr.stop_all_servers()


def test_duckdb_lists_the_connector_tables(attached):
    con, details = attached
    rows = con.execute(
        "SELECT table_schema, table_name FROM information_schema.tables WHERE table_catalog = ?",
        [details["raw_alias"]],
    ).fetchall()
    assert (details["remote_schema"], "items") in rows


def test_duckdb_reads_rows_and_pushes_filters_down(attached):
    con, details = attached
    rel = f'"{details["raw_alias"]}"."{details["remote_schema"]}"."items"'
    assert con.execute(f"SELECT id, name, qty FROM {rel} ORDER BY id").fetchall() == [
        (1, "alpha", 10),
        (2, "beta", 20),
        (3, "gamma", 30),
    ]
    assert con.execute(f"SELECT name FROM {rel} WHERE qty > 15 ORDER BY qty").fetchall() == [
        ("beta",),
        ("gamma",),
    ]
    assert con.execute(f"SELECT sum(qty) FROM {rel}").fetchone() == (60,)


def test_attach_is_read_only(attached):
    con, details = attached
    rel = f'"{details["raw_alias"]}"."{details["remote_schema"]}"."items"'
    with pytest.raises(duckdb.Error):
        con.execute(f"INSERT INTO {rel} VALUES (4, 'delta', 40)")
