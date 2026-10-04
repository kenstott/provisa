# Copyright (c) 2026 Kenneth Stott
# Canary: d308a767-3dd8-4f45-ab5b-b9dac6a4c793
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""A files-glob table's matched files must share a column set (REQ-788)."""

from __future__ import annotations

import pytest

from provisa.file_source.files_glob import FileColumnsDiffer, require_same_columns


def test_matching_files_with_the_same_columns_agree_on_the_first_order():
    agreed = require_same_columns(
        "data/*.csv",
        {
            "data/a.csv": ["id", "name", "amount"],
            "data/b.csv": ["amount", "id", "name"],  # order-free
        },
    )
    assert agreed == ["id", "name", "amount"]  # the first file's order


def test_a_file_with_a_different_column_set_is_refused_by_name():
    with pytest.raises(FileColumnsDiffer) as refused:
        require_same_columns(
            "data/*.csv",
            {
                "data/a.csv": ["id", "name"],
                "data/b.csv": ["id", "name", "extra"],
                "data/c.csv": ["id"],
            },
        )
    assert refused.value.code == "schema.file_columns_differ"
    assert refused.value.params["file"] == "data/b.csv"  # the first that differs
    assert refused.value.params["extra"] == "extra"
    assert refused.value.params["missing"] == ""
    assert refused.value.params["glob"] == "data/*.csv"


def test_a_missing_column_is_named():
    with pytest.raises(FileColumnsDiffer) as refused:
        require_same_columns("d/*.csv", {"d/a.csv": ["id", "name"], "d/b.csv": ["id"]})
    assert refused.value.params["missing"] == "name"
    assert refused.value.params["extra"] == ""


def test_a_single_matched_file_always_agrees():
    assert require_same_columns("d/*.csv", {"d/only.csv": ["x", "y"]}) == ["x", "y"]


def test_no_match_is_the_callers_error_not_a_column_mismatch():
    with pytest.raises(ValueError, match="matched no files"):
        require_same_columns("d/*.csv", {})


def test_the_table_model_carries_the_glob_and_the_source_file_column():
    from provisa.core.models import Table

    t = Table(
        source_id="files",
        domain_id="d",
        schema_name="main",
        table_name="orders",
        columns=[],
        file_glob="orders/*.csv",
        source_file_column="_source_file",
    )
    assert t.file_glob == "orders/*.csv"
    assert t.source_file_column == "_source_file"
    plain = Table(source_id="files", domain_id="d", schema_name="main", table_name="t", columns=[])
    assert plain.file_glob is None and plain.source_file_column is None


def test_matched_files_globs_under_the_source_directory(tmp_path):
    from provisa.file_source.files_glob import matched_files

    d = tmp_path / "orders"
    d.mkdir()
    for n in ("a.csv", "b.csv", "skip.txt"):
        (d / n).write_text("id,name\n1,x\n")
    # source.path names a file in the directory; the glob is relative to that directory.
    got = matched_files(str(d / "a.csv"), "*.csv")
    assert [p.rsplit("/", 1)[-1] for p in got] == ["a.csv", "b.csv"]
    # source.path is the directory itself.
    assert matched_files(str(d), "*.csv") == got


def test_validate_glob_table_reads_each_file_once_and_agrees(tmp_path):
    from provisa.file_source.files_glob import validate_glob_table

    seen: list[str] = []

    def columns_of(path):  # noqa
        seen.append(path)
        return ["id", "name"]

    agreed = validate_glob_table("*.csv", ["a.csv", "b.csv"], columns_of)
    assert agreed == ["id", "name"]
    assert seen == ["a.csv", "b.csv"]


def test_validate_glob_table_refuses_no_match_by_name():
    from provisa.file_source.files_glob import validate_glob_table

    with pytest.raises(ValueError, match="matched no files"):
        validate_glob_table("*.csv", [], lambda p: [])


def test_the_config_load_refuses_a_glob_whose_files_differ(tmp_path):
    import pytest as _pytest

    from provisa.core.config_loader import _validate_file_globs
    from provisa.core.models import Source, SourceType, Table
    from provisa.file_source.files_glob import FileColumnsDiffer

    d = tmp_path / "data"
    d.mkdir()
    (d / "a.csv").write_text("id,name\n1,x\n")
    (d / "b.csv").write_text("id,name,extra\n1,x,y\n")
    source = Source(id="files", type=SourceType("files"), path=str(d / "a.csv"))
    table = Table(
        source_id="files", domain_id="d", schema_name="main", table_name="orders",
        columns=[], file_glob="*.csv",
    )  # fmt: skip
    config = type("C", (), {"sources": [source], "tables": [table]})()
    with _pytest.raises(FileColumnsDiffer):
        _validate_file_globs(config)
    # Same columns across files: no error.
    (d / "b.csv").write_text("id,name\n2,y\n")
    _validate_file_globs(config)


def test_the_config_load_refuses_file_glob_on_a_non_files_source(tmp_path):
    import pytest as _pytest

    from provisa.core.config_loader import _validate_file_globs
    from provisa.core.models import Source, SourceType, Table

    source = Source(id="pg", type=SourceType("postgresql"), host="h")
    table = Table(
        source_id="pg", domain_id="d", schema_name="main", table_name="t",
        columns=[], file_glob="*.csv",
    )  # fmt: skip
    config = type("C", (), {"sources": [source], "tables": [table]})()
    with _pytest.raises(ValueError, match="files source"):
        _validate_file_globs(config)


def test_files_operand_emits_glob_url_tables_from_the_source(tmp_path):
    from types import SimpleNamespace

    from provisa.federation.pgwire_replica import _files_operand

    source = SimpleNamespace(
        id="files",
        type=SimpleNamespace(value="files"),
        path=str(tmp_path / "orders"),
        mapping={},
        file_glob_tables=[
            {"name": "orders", "file_glob": "*.csv", "source_file_column": "_source_file"},
            {"name": "events", "file_glob": "ev/*.json", "source_file_column": None},
        ],
    )
    operand = _files_operand(source)
    tables = {t["name"]: t for t in operand["tables"]}
    assert tables["orders"]["url"].endswith("/orders/*.csv")
    assert tables["orders"]["sourceFileColumn"] == "_source_file"
    assert tables["events"]["url"].endswith("/orders/ev/*.json")
    assert "sourceFileColumn" not in tables["events"]  # None → omitted


def test_files_operand_has_no_tables_without_glob_tables(tmp_path):
    from types import SimpleNamespace

    from provisa.federation.pgwire_replica import _files_operand

    source = SimpleNamespace(
        id="files", type=SimpleNamespace(value="files"),
        path=str(tmp_path / "d"), mapping={}, file_glob_tables=[],
    )  # fmt: skip
    assert "tables" not in _files_operand(source)
