# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""REQ-846/932: the canonical IR type vocabulary — native → IR normalization, no silent default."""

from __future__ import annotations

import pytest

from provisa.core.ir_types import IR_TYPES, is_ir_type, to_ir, to_sqlalchemy


def test_aliases_canonicalize_to_one_ir_name():
    assert to_ir("varchar") == "text"
    assert to_ir("character varying") == "text"
    assert to_ir("string") == "text"
    assert to_ir("int4") == "integer"
    assert to_ir("int8") == "bigint"
    assert to_ir("bool") == "boolean"
    assert to_ir("jsonb") == "text"  # reflection collapses json → text
    assert to_ir("timestamptz") == "timestamp"


def test_strips_qualifier_and_lowercases():
    assert to_ir("VARCHAR(255)") == "text"
    assert to_ir("Numeric(10, 2)") == "numeric"


def test_unknown_type_raises_no_varchar_default():
    with pytest.raises(ValueError, match="not in the IR vocabulary"):
        to_ir("geography")


def test_to_sqlalchemy_maps_ir_and_native():
    assert to_sqlalchemy("bigint").__name__ == "BigInteger"
    assert to_sqlalchemy("varchar").__name__ == "Text"  # native spelling normalized first
    assert to_sqlalchemy("double precision").__name__ == "Float"


def test_is_ir_type():
    assert is_ir_type("varchar") and is_ir_type("timestamp with time zone")
    assert not is_ir_type("geography")


def test_ir_types_are_canonical_names():
    assert "text" in IR_TYPES and "bigint" in IR_TYPES
    assert "varchar" not in IR_TYPES  # varchar is an alias, not a canonical IR name
