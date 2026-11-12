# Copyright (c) 2026 Kenneth Stott
# Canary: 0c6b1f52-7d3e-4a8b-9e21-5f4a3c9d8e17
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""A files source whose path is a glob offers, and serves, only the files the glob matches
(REQ-788): discovery, registration, save and reads alike, for a local path and an fsspec URI."""

from __future__ import annotations

import asyncio
from pathlib import Path
from types import SimpleNamespace

import pytest

from provisa.api.admin._file_glob import (
    table_file_glob_refusal,
    table_outside_source_glob_refusal,
)
from provisa.api.admin.schema_common import files_source_glob_refusal
from provisa.api.admin.types import AvailableTableType, SourceInput
from provisa.federation.pgwire_replica import _files_operand
from provisa.file_source.crawler import crawl_directory
from provisa.file_source.files_glob import (
    FileOutsideSourceGlob,
    SourceGlobMatchesNothing,
    matched_files,
    require_source_glob_matches,
    source_glob_files,
    split_source_path,
)

pytestmark = pytest.mark.unit

DROP = Path(__file__).resolve().parents[2] / "demo" / "named" / "files" / "drop"
COMPLAINTS = ["complaints_2026_08.csv", "complaints_2026_09.csv"]
ALL_FIVE = [
    "budget_2026.xlsx",
    "building_permits.parquet",
    "complaints_2026_08.csv",
    "complaints_2026_09.csv",
    "inspections.json",
]


def _source(path: str, glob_tables: list | None = None) -> SimpleNamespace:
    return SimpleNamespace(
        id="drop",
        type=SimpleNamespace(value="files"),
        path=path,
        mapping={},
        file_glob_tables=glob_tables or [],
    )


def _adapter_files(operand: dict) -> list[str]:
    """The files the file adapter lists for ``operand``: its directory's files, narrowed by its
    ``glob`` operand when it has one (the adapter matches the glob against each file's path
    relative to the directory)."""
    directory = Path(operand["directory"])
    if "glob" not in operand:
        return sorted(p.name for p in directory.iterdir() if p.is_file())
    return sorted(p.name for p in directory.glob(operand["glob"]) if p.is_file())


def test_the_demo_drop_folder_holds_five_files():
    assert sorted(p.name for p in DROP.iterdir() if p.is_file()) == ALL_FIVE


# --- discovery: the tables offered ---------------------------------------------------------


def test_a_globbed_source_offers_only_the_files_its_glob_matches():
    operand = _files_operand(_source(str(DROP / "complaints*.csv")))
    assert operand["directory"] == str(DROP)
    assert operand["glob"] == "complaints*.csv"
    assert _adapter_files(operand) == COMPLAINTS


def test_a_star_csv_glob_offers_the_two_csv():
    operand = _files_operand(_source(str(DROP / "*.csv")))
    assert _adapter_files(operand) == COMPLAINTS


def test_a_directory_path_offers_all_five():
    operand = _files_operand(_source(str(DROP)))
    assert operand["directory"] == str(DROP)
    assert "glob" not in operand
    assert _adapter_files(operand) == ALL_FIVE


def test_a_glob_in_a_middle_segment_keeps_the_directory_before_it(tmp_path):
    (tmp_path / "a").mkdir()
    (tmp_path / "a" / "x.csv").write_text("id\n1\n")
    operand = _files_operand(_source(str(tmp_path / "*" / "x.csv")))
    assert operand["directory"] == str(tmp_path)
    assert operand["glob"] == "*/x.csv"


def test_the_crawl_preview_of_a_globbed_source_lists_only_matched_files():
    names = sorted(e["name"] for e in crawl_directory(str(DROP / "complaints*.csv")))
    assert names == ["complaints_2026_08", "complaints_2026_09"]


def test_the_crawl_preview_of_a_star_csv_glob_lists_the_two_csv():
    names = sorted(e["name"] for e in crawl_directory(str(DROP / "*.csv")))
    assert names == ["complaints_2026_08", "complaints_2026_09"]


def test_the_crawl_preview_of_a_directory_is_unchanged():
    names = sorted(e["name"] for e in crawl_directory(str(DROP)))
    # the crawler reads csv/parquet/sqlite; the directory's other files are not its kind
    assert names == ["building_permits", "complaints_2026_08", "complaints_2026_09"]


def test_split_source_path():
    assert split_source_path("/d/complaints*.csv") == ("/d", "complaints*.csv")
    assert split_source_path("/d/sub") == ("/d/sub", None)
    assert split_source_path("/d/a.csv") == ("/d/a.csv", None)
    assert split_source_path("s3://b/p/*.csv") == ("s3://b/p", "*.csv")
    assert split_source_path("memory://b/**/x?.csv") == ("memory://b", "**/x?.csv")


def test_source_glob_files_is_none_for_a_plain_path():
    assert source_glob_files(str(DROP)) is None
    assert source_glob_files(str(DROP / "complaints_2026_08.csv")) is None


# --- save: a glob that matches nothing --------------------------------------------------------


def _input(path: str, mapping_json: str = "{}") -> SourceInput:
    return SourceInput(id="drop", type="files", path=path, mapping_json=mapping_json)


def test_a_glob_that_matches_no_file_is_refused_at_save_by_name():
    glob = str(DROP / "nothing*.csv")
    r = files_source_glob_refusal(_input(glob))
    assert r is not None and r.success is False
    assert r.code == "schema.source_glob_matches_nothing"
    assert r.params["glob"] == glob
    assert glob in r.message


def test_a_glob_that_matches_is_saved():
    assert files_source_glob_refusal(_input(str(DROP / "complaints*.csv"))) is None


def test_a_directory_and_a_single_file_are_saved_as_today():
    assert files_source_glob_refusal(_input(str(DROP))) is None
    assert files_source_glob_refusal(_input(str(DROP / "complaints_2026_08.csv"))) is None


def test_a_crawling_source_is_not_refused_before_its_files_land():
    mapping = '{"crawl": {"start_urls": ["https://example.org"]}}'
    assert files_source_glob_refusal(_input(str(DROP / "nothing*.csv"), mapping)) is None


# --- registration: a file outside the glob is not a table of the source ------------------------


def _patch_source(monkeypatch, source, offered):
    async def _get(conn, sid):
        return source

    async def _offered(source_id, schema_name):
        return [AvailableTableType(name=n, comment=None) for n in offered]

    monkeypatch.setattr("provisa.core.repositories.source.get", _get)
    monkeypatch.setattr("provisa.api.admin.schema_query.offered_tables", _offered)


def _table(name: str, file_glob: str | None = None):
    return SimpleNamespace(
        table_name=name, source_id="drop", schema_name="drop", file_glob=file_glob
    )


def test_registering_a_file_outside_the_glob_is_refused_by_name(monkeypatch):
    glob = str(DROP / "complaints*.csv")
    _patch_source(
        monkeypatch, {"type": "files", "path": glob}, ["complaints_2026_08", "complaints_2026_09"]
    )
    r = asyncio.run(table_outside_source_glob_refusal(None, _table("budget_2026__budget")))
    assert r is not None and r.success is False
    assert r.code == "schema.table_outside_source_glob"
    assert r.params == {"table": "budget_2026__budget", "source": "drop", "glob": glob}
    assert "budget_2026__budget" in r.message and glob in r.message


def test_registering_a_file_the_glob_matches_passes(monkeypatch):
    _patch_source(
        monkeypatch,
        {"type": "files", "path": str(DROP / "complaints*.csv")},
        ["complaints_2026_08", "complaints_2026_09"],
    )
    assert (
        asyncio.run(table_outside_source_glob_refusal(None, _table("complaints_2026_08"))) is None
    )


def test_a_directory_source_is_not_narrowed_at_registration(monkeypatch):
    _patch_source(monkeypatch, {"type": "files", "path": str(DROP)}, [])
    assert (
        asyncio.run(table_outside_source_glob_refusal(None, _table("budget_2026__budget"))) is None
    )


# --- a table's own file_glob under a globbed source -------------------------------------------


def test_a_table_glob_under_a_globbed_source_matches_under_its_directory():
    files = matched_files(str(DROP / "complaints*.csv"), "complaints_2026_*.csv")
    assert [Path(f).name for f in files] == COMPLAINTS


def test_a_table_glob_cannot_reach_past_the_source_glob():
    with pytest.raises(FileOutsideSourceGlob) as exc:
        matched_files(str(DROP / "complaints*.csv"), "*")
    assert exc.value.params["file"].endswith("budget_2026.xlsx")
    assert exc.value.params["glob"] == "*"


def test_registering_a_table_glob_past_the_source_glob_is_refused_by_name(monkeypatch):
    _patch_source(monkeypatch, {"type": "files", "path": str(DROP / "complaints*.csv")}, [])
    r = asyncio.run(table_file_glob_refusal(None, _table("complaints", "*")))
    assert r is not None and r.code == "schema.file_outside_source_glob"
    assert r.params["file"].endswith("budget_2026.xlsx")


def test_registering_a_table_glob_within_the_source_glob_passes(monkeypatch):
    _patch_source(monkeypatch, {"type": "files", "path": str(DROP / "complaints*.csv")}, [])
    assert (
        asyncio.run(table_file_glob_refusal(None, _table("complaints", "complaints_*.csv"))) is None
    )


def test_the_read_operand_refuses_a_table_glob_past_the_source_glob():
    source = _source(
        str(DROP / "complaints*.csv"),
        [{"name": "everything", "file_glob": "*", "source_file_column": None}],
    )
    with pytest.raises(FileOutsideSourceGlob):
        _files_operand(source)


def test_the_read_operand_keeps_a_table_glob_within_the_source_glob():
    source = _source(
        str(DROP / "complaints*.csv"),
        [{"name": "complaints", "file_glob": "complaints_*.csv", "source_file_column": None}],
    )
    tables = _files_operand(source)["tables"]
    assert tables == [{"name": "complaints", "url": str(DROP / "complaints_*.csv")}]


# --- fsspec URIs ------------------------------------------------------------------------------


@pytest.fixture
def memory_drop():
    import fsspec

    fs = fsspec.filesystem("memory")
    root = "/j1256drop"
    for name in ALL_FIVE:
        fs.pipe(f"{root}/{name}", b"id,name\n1,x\n")
    try:
        yield f"memory://{root}"
    finally:
        fs.rm(root, recursive=True)


def test_an_fsspec_glob_decides_its_files(memory_drop):
    files = source_glob_files(f"{memory_drop}/complaints*.csv")
    assert files is not None
    assert [f.rsplit("/", 1)[1] for f in files] == COMPLAINTS
    star = source_glob_files(f"{memory_drop}/*.csv")
    assert star is not None and [f.rsplit("/", 1)[1] for f in star] == COMPLAINTS


def test_an_fsspec_glob_that_matches_nothing_is_refused_by_name(memory_drop):
    glob = f"{memory_drop}/nothing*.csv"
    with pytest.raises(SourceGlobMatchesNothing) as exc:
        require_source_glob_matches(glob)
    assert exc.value.params == {"glob": glob}
    r = files_source_glob_refusal(_input(glob))
    assert r is not None and r.code == "schema.source_glob_matches_nothing"


def test_an_fsspec_table_glob_under_a_globbed_source(memory_drop):
    files = matched_files(f"{memory_drop}/complaints*.csv", "complaints_2026_*.csv")
    assert [f.rsplit("/", 1)[1] for f in files] == COMPLAINTS
    with pytest.raises(FileOutsideSourceGlob) as exc:
        matched_files(f"{memory_drop}/complaints*.csv", "*")
    assert exc.value.params["file"].endswith("budget_2026.xlsx")


def test_a_file_uri_glob_decides_its_files():
    files = source_glob_files(f"file://{DROP}/complaints*.csv")
    assert files is not None and [f.rsplit("/", 1)[1] for f in files] == COMPLAINTS
    names = sorted(e["name"] for e in crawl_directory(f"file://{DROP}/complaints*.csv"))
    assert names == ["complaints_2026_08", "complaints_2026_09"]
