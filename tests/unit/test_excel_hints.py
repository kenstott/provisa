# Copyright (c) 2026 Kenneth Stott
# Canary: 4667b54b-7868-4799-90fb-9f1080babcd5
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A files source's hints for how the file adapter's Excel reader finds its workbooks' tables:
its mapping's excel_hints, given to the adapter as its excelHints operand on either engine, and
refused by name where the reader would not take them."""

from __future__ import annotations

import json

import pytest

from provisa.api.admin.schema_common import excel_hints_refusal
from provisa.api.admin.types import SourceInput
from provisa.core.models import Source, SourceType
from provisa.federation import pgwire_replica as pr
from provisa.file_source.excel_hints import InvalidExcelHints, excel_hints_operand

HINTS = {
    "max_header_rows": 1,
    "workbooks": {"watch_list.xlsx": {"column_gap": 1, "table_gap_rows": 3}},
    "tables": {
        "report__summary": {"header_rows": 2, "min_data_rows": 2},
        "report__totals": {"range": "'Q1 results'!H3:K20", "min_columns": 1},
    },
}
OPERAND = {
    "maxHeaderRows": 1,
    "workbooks": {"watch_list.xlsx": {"columnGap": 1, "tableGapRows": 3}},
    "tables": {
        "report__summary": {"headerRows": 2, "minDataRows": 2},
        "report__totals": {"range": "'Q1 results'!H3:K20", "minColumns": 1},
    },
}


def _files(**kw) -> Source:
    return Source(**{"id": "books", "type": SourceType.files, "path": "/data/books", **kw})


def test_the_hints_reach_the_file_adapter_under_its_own_names():
    operand = pr.build_model_json(_files(mapping={"excel_hints": HINTS}))["schemas"][0]["operand"]
    assert operand["excelHints"] == OPERAND
    assert "excelHints" not in pr.build_model_json(_files())["schemas"][0]["operand"]


def test_trino_is_given_the_same_hints_as_the_bundle():
    from provisa.federation.trino_connectors import TrinoFilesConnector

    source = _files(mapping={"excel_hints": HINTS})
    assert json.loads(TrinoFilesConnector().details(source)["excel-hints"]) == OPERAND
    assert "excel-hints" not in TrinoFilesConnector().details(_files())


def test_hints_left_unset_give_no_operand():
    assert excel_hints_operand("books", {"excel_hints": {}}) is None
    assert excel_hints_operand("books", {"excel_hints": {"tables": {"t": {}}}}) is None
    assert excel_hints_operand("books", {}) is None


@pytest.mark.parametrize(
    "hints, said",
    [
        ({"header_row": 1}, r"excel_hints: unknown hint\(s\) \['header_row'\]"),
        ({"max_header_rows": 0}, r"excel_hints.max_header_rows must be a whole number of 1"),
        ({"min_columns": "2"}, r"excel_hints.min_columns must be a whole number"),
        ({"min_columns": True}, r"excel_hints.min_columns must be a whole number"),
        (
            {"tables": {"t": {"column_gap": 1}}},
            r"excel_hints.tables.t: unknown hint\(s\) \['column_gap'\]",
        ),
        ({"workbooks": {"b.xlsx": {"range": "s!A1:B2"}}}, r"unknown hint\(s\) \['range'\]"),
        ({"tables": {"t": {"range": "A1:B2"}}}, r"is not a range of one sheet"),
        ({"tables": ["t"]}, r"excel_hints.tables must be an object"),
        ("auto", r"mapping.excel_hints must be an object"),
    ],
)
def test_a_hint_the_reader_would_not_take_is_refused_by_name(hints, said):
    with pytest.raises(InvalidExcelHints, match=said):
        excel_hints_operand("books", {"excel_hints": hints})


def test_a_source_saved_with_a_hint_the_reader_would_not_take_is_refused():
    bad = SourceInput(
        id="books",
        type="files",
        path="/data/books",
        mapping_json=json.dumps({"excel_hints": {"column_gap": 0}}),
    )
    refused = excel_hints_refusal(bad)
    assert refused is not None and refused.code == "schema.excel_hints_invalid"
    assert "column_gap" in refused.message
    good = SourceInput(
        id="books",
        type="files",
        path="/data/books",
        mapping_json=json.dumps({"excel_hints": HINTS}),
    )
    assert excel_hints_refusal(good) is None
