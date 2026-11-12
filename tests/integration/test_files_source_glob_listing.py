# Copyright (c) 2026 Kenneth Stott
# Canary: 7a1e4c9b-2f63-4d8e-b05a-c3d9e6f1a284
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-788: a files source whose path is a glob offers only the files the glob matches, through
the real bundled Calcite file adapter (pgwire-file, a real JVM) attached live by DuckDB — the
path the Register Table form's listing and every read of the source take."""

from __future__ import annotations

from pathlib import Path

import duckdb
import pytest

from provisa.core.models import Source, SourceType
from provisa.federation import pgwire_replica as pr
from provisa.federation.connector_duckdb import _DuckDBPgwireConnector

pytestmark = [pytest.mark.integration]

DROP = Path(__file__).resolve().parents[2] / "demo" / "named" / "files" / "drop"


class _FilesOverPgwire(_DuckDBPgwireConnector):
    source_type = "files"


def _tables(source_id: str, path: str) -> list[str]:
    details = _FilesOverPgwire().details(Source(id=source_id, type=SourceType.files, path=path))
    con = duckdb.connect()
    try:
        con.execute("INSTALL postgres")
        con.execute("LOAD postgres")
        con.execute(details["attach"])
        rows = con.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_catalog = ? "
            "AND table_schema = ?",
            [details["raw_alias"], details["remote_schema"]],
        ).fetchall()
        return sorted(r[0] for r in rows)
    finally:
        con.close()
        pr.stop_all_servers()


def test_a_complaints_glob_offers_the_two_complaints_tables():
    assert _tables("glob-complaints", str(DROP / "complaints*.csv")) == [
        "complaints_2026_08",
        "complaints_2026_09",
    ]


def test_a_star_csv_glob_offers_the_two_csv():
    assert _tables("glob-csv", str(DROP / "*.csv")) == ["complaints_2026_08", "complaints_2026_09"]


def test_a_directory_offers_all_five():
    assert _tables("glob-dir", str(DROP)) == [
        "budget_2026__budget",
        "building_permits",
        "complaints_2026_08",
        "complaints_2026_09",
        "inspections",
    ]
