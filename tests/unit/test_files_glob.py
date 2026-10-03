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
