# Copyright (c) 2026 Kenneth Stott
# Canary: c07832eb-ea6e-4289-a250-3533dec62af9
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A registered column that is natively a one-level array of one primitive type (REQ-1965) is
described over pgwire as that array, as PostgreSQL describes it; any other nested column stays
JSON."""

from __future__ import annotations

from provisa.compiler.result_shape import _described_type
from provisa.pgwire.server import _SHAPE_COERCERS, _sql_type_to_bvtype
from provisa.transpiler.native_json import NestedColumn


def test_a_typed_array_is_described_as_its_array():
    assert _described_type("json", NestedColumn("integer[]", 1, "integer")) == "integer[]"


def test_a_deeper_or_untyped_nested_column_stays_json():
    assert _described_type("json", NestedColumn("integer[][]", 2, "integer")) == "json"
    assert _described_type("json", NestedColumn("record", 0, None)) == "json"
    assert _described_type("json", None) == "json"


def test_an_array_arriving_as_json_text_is_sent_as_its_values():
    coerce = _SHAPE_COERCERS[_sql_type_to_bvtype("integer[]")]
    assert coerce("[1, 2, 3]") == [1, 2, 3]
    assert coerce([1, 2]) == [1, 2]
