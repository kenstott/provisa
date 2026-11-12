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


def _attached(source: Source, use) -> object:
    """``use(con, details)`` on a DuckDB connection holding the source's live attach of the real
    adapter, then the attach and the source's server are stopped."""
    details = _FilesOverPgwire().details(source)
    con = duckdb.connect()
    try:
        con.execute("INSTALL postgres")
        con.execute("LOAD postgres")
        con.execute(details["attach"])
        return use(con, details)
    finally:
        con.close()
        pr.stop_all_servers()


def _listed(con, details) -> list[str]:
    rows = con.execute(
        "SELECT table_name FROM information_schema.tables WHERE table_catalog = ? "
        "AND table_schema = ?",
        [details["raw_alias"], details["remote_schema"]],
    ).fetchall()
    return sorted(r[0] for r in rows)


def _tables(source_id: str, path: str) -> list[str]:
    return _attached(Source(id=source_id, type=SourceType.files, path=path), _listed)


def _count(con, details, table: str) -> int:
    return con.execute(
        f'SELECT count(*) FROM "{details["raw_alias"]}"."{details["remote_schema"]}"."{table}"'
    ).fetchone()[0]


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


@pytest.mark.parametrize("outside", ["budget_2026__budget", "building_permits", "inspections"])
def test_a_table_of_a_file_outside_the_glob_is_not_readable(outside):
    """A read through the source reaches only the files its glob matches: a table of a file
    outside it does not exist there (the Excel workbook's sheet table included)."""

    def read(con, details):
        with pytest.raises(duckdb.CatalogException):
            _count(con, details, outside)
        return _count(con, details, "complaints_2026_08")

    assert (
        _attached(
            Source(id="glob-read", type=SourceType.files, path=str(DROP / "complaints*.csv")), read
        )
        == 300
    )


def test_a_table_file_glob_under_a_globbed_source_reads_its_files():
    """A table's own file_glob still makes one table of many files under a globbed source."""
    source = Source(id="glob-table", type=SourceType.files, path=str(DROP / "complaints*.csv"))
    source.file_glob_tables = [{"name": "complaints", "file_glob": "complaints_2026_*.csv"}]

    assert _attached(source, lambda con, details: _count(con, details, "complaints")) == 600
