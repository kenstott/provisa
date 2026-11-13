# Copyright (c) 2026 Kenneth Stott
# Canary: 8e3b5c21-4f7a-4d09-b6e2-91c0a7d3f5e4
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Combining files into one table is its own action (REQ-788, COMBINING FILES IS ITS OWN ACTION):
before anything is registered the form is told the glob (derived from the files picked, or as
typed), the files it reads, the columns they share and the files whose columns differ."""

# Requirements: REQ-788

from __future__ import annotations

import asyncio

import pytest

from provisa.api.admin._file_glob import file_glob_preview, source_files_path
from provisa.file_source.files_glob import column_agreement, derive_glob, glob_candidates

pytestmark = pytest.mark.unit


def _months(tmp_path, months, header="id,amount"):
    for m in months:
        (tmp_path / f"complaints_2026_{m:02d}.csv").write_text(f"{header}\n1,2\n")


def test_picked_files_derive_the_glob_that_matches_exactly_them(tmp_path):
    _months(tmp_path, range(1, 4))
    (tmp_path / "other.csv").write_text("x\n1\n")
    out = file_glob_preview(
        str(tmp_path),
        None,
        ["complaints_2026_01", "complaints_2026_02", "complaints_2026_03"],
    )
    assert out["glob"] == "complaints_2026_0*.csv"
    assert [f["file"] for f in out["files"]] == [
        "complaints_2026_01.csv",
        "complaints_2026_02.csv",
        "complaints_2026_03.csv",
    ]
    assert out["files"][0]["table"] == "complaints_2026_01"
    assert out["shared_columns"] == ["id", "amount"]
    assert out["differing"] == []
    assert out["extra_files"] == []
    assert out["reference_table"] == "complaints_2026_01"


def test_a_narrower_glob_is_found_when_the_prefix_glob_adds_files(tmp_path):
    _months(tmp_path, range(1, 13))
    out = file_glob_preview(str(tmp_path), None, ["complaints_2026_01", "complaints_2026_03"])
    assert out["glob"] == "complaints_2026_0[13].csv"
    assert out["extra_files"] == []


def test_no_exact_glob_names_the_files_it_would_add(tmp_path):
    for name in ("a1.csv", "b2.csv", "c33.csv"):
        (tmp_path / name).write_text("id\n1\n")
    out = file_glob_preview(str(tmp_path), None, ["a1", "c33"])
    assert out["glob"] == "*.csv"
    assert out["extra_files"] == ["b2.csv"]
    assert [f["file"] for f in out["files"]] == ["a1.csv", "b2.csv", "c33.csv"]


def test_derive_glob_prefers_the_readable_glob_when_both_are_exact():
    every = ["x_01.csv", "x_02.csv"]
    glob, extra = derive_glob(every, lambda g: every if "*" in g or "[" in g else [])
    assert (glob, extra) == ("x_0*.csv", [])


def test_a_typed_glob_matching_nothing_answers_no_files(tmp_path):
    _months(tmp_path, [1])
    out = file_glob_preview(str(tmp_path), "complaints_2025_*.csv", None)
    assert out["files"] == []
    assert out["shared_columns"] == []


def test_a_typed_glob_names_the_file_whose_columns_differ(tmp_path):
    _months(tmp_path, [1, 2])
    (tmp_path / "complaints_2026_03.csv").write_text("id,amount,region\n1,2,x\n")
    out = file_glob_preview(str(tmp_path), "complaints_2026_*.csv", ["complaints_2026_01"])
    assert len(out["files"]) == 3
    assert out["shared_columns"] == ["id", "amount"]
    assert out["differing"] == [
        {"file": "complaints_2026_03.csv", "missing": [], "extra": ["region"], "error": None}
    ]


def test_a_picked_table_naming_no_file_is_answered(tmp_path):
    _months(tmp_path, [1])
    out = file_glob_preview(str(tmp_path), None, ["ghost", "complaints_2026_01"])
    assert out["unmatched_tables"] == ["ghost"]
    assert out["glob"] == ""


def test_a_source_directory_that_is_not_there_holds_no_file(tmp_path):
    _months(tmp_path, [1])
    out = file_glob_preview(str(tmp_path / "gone"), "**/*.csv", None)
    assert out["files"] == []
    assert file_glob_preview(str(tmp_path / "gone"), None, ["ghost"])["unmatched_tables"] == [
        "ghost"
    ]


def test_a_table_listed_by_its_whole_file_name_is_found(tmp_path):
    _months(tmp_path, [1, 2])
    out = file_glob_preview(
        str(tmp_path), None, ["complaints_2026_01.csv", "complaints_2026_02.csv"]
    )
    assert out["glob"] == "complaints_2026_0*.csv"
    assert out["unmatched_tables"] == []


def test_the_odd_file_out_is_the_one_named():
    shared, differing = column_agreement(
        {"a.csv": ["x", "y"], "b.csv": ["x", "y", "z"], "c.csv": ["y", "x"]}
    )
    assert shared == ["x", "y"]
    assert [d["file"] for d in differing] == ["b.csv"]


def test_candidates_cover_files_in_one_subdirectory():
    assert glob_candidates(["2026/a_01.csv", "2026/a_02.csv"])[0] == "2026/a_0*.csv"


def test_a_non_files_source_is_refused_by_name(monkeypatch):
    async def _get(_conn, _sid):
        return {"type": "postgresql", "path": None}

    monkeypatch.setattr("provisa.core.repositories.source.get", _get)
    with pytest.raises(ValueError, match="only on a files source"):
        asyncio.run(source_files_path(None, "pg"))
