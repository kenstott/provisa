# Copyright (c) 2026 Kenneth Stott
# Canary: 14ca5ebc-252f-4fe3-bf6c-f792c42496ca
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The World Cup demo ships what it declares (REQ-2103), checked without a server.

Every source is frozen: each declared rows file is a table the config serves, under a source that
never expires, and each table the config serves from a source has its declared rows, so a sandbox
reads nothing live. country_alias's rows are the reviewed matching: every name once, West Germany
and Germany FR as Germany, and a team whose results passed to no member under its own name. The
code archive is the one the config names by its content hash, and holds the function's source as
the demo's country_alias/ directory has it."""

# Requirements: REQ-2103, REQ-2089, REQ-2101
from __future__ import annotations

import tarfile
import tempfile
from pathlib import Path

import pytest
import yaml

from provisa.codesource.bundle import content_hash, unpack
from provisa.federation.declared_replica_rows import declared_files, read_file

pytestmark = pytest.mark.unit

_DEMO = Path(__file__).resolve().parents[2] / "demo" / "named" / "worldcup"
_CONFIG = yaml.safe_load((_DEMO / "config.yaml").read_text())
_SOURCES = {s["id"]: s for s in _CONFIG["sources"]}


def _alias() -> dict[str, tuple[str | None, str | None]]:
    [path] = [p for p in declared_files(_DEMO) if p.parent.name == "country_alias"]
    declared = read_file(path)
    assert declared.header == ["name", "canonical_country", "fifa_code"]

    def cell(v: str) -> str | None:
        return None if v == "\\N" else v

    rows = {cells[0]: (cell(cells[1]), cell(cells[2])) for cells in declared.lines}
    assert len(rows) == len(declared.lines), "a name is matched once"
    return rows


def test_every_source_is_frozen():
    for source in _CONFIG["sources"]:
        assert source["cache_ttl"] == -1 and source["replicate"] == 0, source["id"]
        assert "password" not in source and "username" not in source, source["id"]


def test_each_declared_table_is_one_the_config_serves_and_each_served_table_is_declared():
    served = {(t["source_id"], t["schema"], t["table"]) for t in _CONFIG["tables"]}
    declared = {read_file(p).key for p in declared_files(_DEMO)}
    assert declared == served


def test_each_declared_header_is_its_table_s_landed_columns_in_order():
    columns = {
        (t["source_id"], t["schema"], t["table"]): [
            c["name"] for c in t["columns"] if not c["name"].startswith("_nf_")
        ]
        for t in _CONFIG["tables"]
    }
    for path in declared_files(_DEMO):
        declared = read_file(path)
        assert declared.header == columns[declared.key], path.name
        assert declared.lines, f"{path.name} holds no row"


def test_west_germany_and_germany_fr_are_germany():
    alias = _alias()
    assert alias["West Germany"] == alias["Germany FR"] == alias["Germany"] == ("Germany", "GER")
    assert alias["Zaire"] == ("DR Congo", "COD")
    assert alias["Soviet Union"] == ("Russia", "RUS")


def test_a_team_whose_results_passed_to_no_member_keeps_its_own_name():
    alias = _alias()
    assert alias["German DR"] == ("East Germany", "GDR")
    assert alias["Saarland"] == ("Saar", "SAA")
    assert alias["Yemen DPR"] == ("South Yemen", "YMD")


def test_a_code_names_one_canonical_country():
    by_code: dict[str, set[str]] = {}
    for canonical, code in _alias().values():
        if code is not None and canonical is not None:
            by_code.setdefault(code, set()).add(canonical)
    assert {code: names for code, names in by_code.items() if len(names) > 1} == {}


def test_the_code_archive_is_the_one_the_config_names():
    archive = _DEMO / "code" / "country_alias.tar.gz"
    with tempfile.TemporaryDirectory() as scratch:
        found = content_hash(unpack(archive.name, archive.read_bytes(), Path(scratch)))
    assert _SOURCES["country_alias"]["mapping"]["code"] == {
        "kind": "upload",
        "upload": found,
        "filename": archive.name,
    }


def test_the_archive_holds_the_function_as_the_demo_shows_it():
    with tarfile.open(_DEMO / "code" / "country_alias.tar.gz") as archive:
        names = set(archive.getnames())
        shipped = archive.extractfile("country_alias.py")
        assert shipped is not None
        source = shipped.read().decode()
    assert names == {"country_alias.py", "pyproject.toml", "uv.lock"}
    assert source == (_DEMO / "country_alias" / "country_alias.py").read_text()
