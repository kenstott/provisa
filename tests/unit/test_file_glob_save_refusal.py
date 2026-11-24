# Copyright (c) 2026 Kenneth Stott
# Canary: 45adc5e0-74c8-4a05-bcf2-d17bc6821e36

"""Registering a files-glob table refuses differing files by name, as the config load does (REQ-788)."""

from __future__ import annotations

import asyncio
from types import SimpleNamespace

import pytest

from provisa.api.admin._file_glob import table_file_glob_refusal

pytestmark = pytest.mark.unit


class _Conn:
    def __init__(self, source):
        self._source = source


def _patch_source(monkeypatch, source):
    async def _get(conn, sid):
        return source

    monkeypatch.setattr("provisa.core.repositories.source.get", _get)


def _model(glob):
    return SimpleNamespace(table_name="orders", source_id="files", file_glob=glob)


def test_no_refusal_without_a_glob(monkeypatch):
    assert asyncio.run(table_file_glob_refusal(_Conn(None), _model(None))) is None


def test_differing_files_are_refused_by_name(tmp_path, monkeypatch):
    (tmp_path / "a.csv").write_text("id,name\n1,x\n")
    (tmp_path / "b.csv").write_text("id,name,extra\n2,y,z\n")
    _patch_source(monkeypatch, {"type": "files", "path": str(tmp_path / "a.csv")})
    r = asyncio.run(table_file_glob_refusal(_Conn(None), _model("*.csv")))
    assert r is not None and r.success is False
    assert r.code == "schema.file_columns_differ"
    assert r.params["file"].endswith("b.csv")


def test_matching_files_pass(tmp_path, monkeypatch):
    (tmp_path / "a.csv").write_text("id,name\n1,x\n")
    (tmp_path / "b.csv").write_text("id,name\n2,y\n")
    _patch_source(monkeypatch, {"type": "files", "path": str(tmp_path / "a.csv")})
    assert asyncio.run(table_file_glob_refusal(_Conn(None), _model("*.csv"))) is None


def test_glob_on_a_non_files_source_is_refused(monkeypatch):
    _patch_source(monkeypatch, {"type": "postgresql", "path": None})
    r = asyncio.run(table_file_glob_refusal(_Conn(None), _model("*.csv")))
    assert r is not None and r.code == "schema.file_glob_not_files_source"


def test_a_glob_matching_nothing_is_refused(tmp_path, monkeypatch):
    _patch_source(monkeypatch, {"type": "files", "path": str(tmp_path / "none.csv")})
    r = asyncio.run(table_file_glob_refusal(_Conn(None), _model("*.csv")))
    assert r is not None and r.code == "schema.file_glob_unreadable"


# REQ-2234, A PATTERN WITH NO FILE TYPE: a pattern naming no file type reads as the one type its
# files share; files of more than one type are refused by name, listing the types found.


def test_a_pattern_naming_no_type_over_csv_files_passes(tmp_path, monkeypatch):
    (tmp_path / "complaints_2026_09.csv").write_text("id,amount\n1,10\n")
    (tmp_path / "complaints_2026_10.csv").write_text("id,amount\n2,20\n")
    _patch_source(monkeypatch, {"type": "files", "path": str(tmp_path)})
    assert asyncio.run(table_file_glob_refusal(_Conn(None), _model("complaint*"))) is None


def test_a_pattern_naming_no_type_over_csv_and_parquet_is_refused_naming_both(
    tmp_path, monkeypatch
):
    import pyarrow as pa
    import pyarrow.parquet as pq

    (tmp_path / "complaints_2026_09.csv").write_text("id,amount\n1,10\n")
    pq.write_table(pa.table({"id": [2], "amount": [20]}), tmp_path / "complaints_2026_10.parquet")
    _patch_source(monkeypatch, {"type": "files", "path": str(tmp_path)})
    r = asyncio.run(table_file_glob_refusal(_Conn(None), _model("complaint*")))
    assert r is not None and r.success is False
    assert r.code == "schema.file_types_differ"
    assert r.params == {"glob": "complaint*", "types": "csv, parquet"}
    assert "csv, parquet" in r.message


def test_the_mixed_type_refusal_is_worded_in_every_locale():
    import json
    from pathlib import Path

    locales = Path(__file__).resolve().parents[2] / "provisa-ui" / "src" / "i18n" / "locales"
    catalogs = sorted(locales.glob("*/serverErrors.json"))
    assert len(catalogs) >= 13, catalogs
    for catalog in catalogs:
        said = json.loads(catalog.read_text())["serverErrors"]["schema"]["file_types_differ"]
        assert "{{glob}}" in said and "{{types}}" in said, catalog
