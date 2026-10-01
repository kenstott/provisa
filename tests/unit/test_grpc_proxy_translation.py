# Copyright (c) 2026 Kenneth Stott
# Canary: 9b1879d7-b1f3-4696-8d2f-c8cbe21284e7
# Canary: placeholder
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Unit tests for REQ-803: HTTP→gRPC proxy pure helper logic.

Covers:
- PascalCase TypeName → snake_case GraphQL field name conversion
- read_mask dot-notation path filtering (top-level and nested fields)
- native filter arg extraction from body["filter"]
- field selection building for scalars and nested types
"""

from types import SimpleNamespace

import pytest

from provisa.grpc.proto_gen import _to_proto_type_name, _to_proto_field_name
from provisa.grpc.query_ir import ReadMaskError, resolve_read_mask, restrict_json


# ---------------------------------------------------------------------------
# _to_proto_type_name (proto_gen) — the single naming authority the proxy defers
# to for GQL type name → proto PascalCase.  The proxy no longer owns a local copy.
# ---------------------------------------------------------------------------


def test_proto_type_name_domain_prefixed():
    assert _to_proto_type_name("PS__Pets") == "PsPets"


def test_proto_type_name_no_prefix():
    assert _to_proto_type_name("Pets") == "Pets"


def test_proto_type_name_lowercase_prefix():
    # prefix.capitalize() lowercases the rest of the prefix, so "ps" → "Ps"; "pets" unchanged.
    assert _to_proto_type_name("ps__pets") == "Pspets"


def test_to_proto_field_name_replaces_double_underscore():
    assert _to_proto_field_name("ps__pets") == "ps_pets"


def test_to_proto_field_name_no_double_underscore():
    assert _to_proto_field_name("pets") == "pets"


# ---------------------------------------------------------------------------
# read_mask projection logic. The proxy and the native servicer share one read_mask
# (provisa.grpc.query_ir): the mask is resolved against the table's readable columns,
# the query selects only the masked ones, and a dotted sub-path into a JSON-valued
# column restricts that column's value. This runs those functions over row dicts.
# ---------------------------------------------------------------------------


def _apply_read_mask(rows: list[dict], mask_paths: list[str]) -> list[dict]:
    """Project ``rows`` as the proxy does, with the shared read_mask functions. The table's
    columns are the rows' keys; a column holding an object or a list is JSON-valued."""
    columns: dict[str, str] = {}
    for row in rows:
        for name, value in row.items():
            columns.setdefault(name, "jsonb" if isinstance(value, (dict, list)) else "varchar")
    ctx = SimpleNamespace(
        tables={"pets": SimpleNamespace(type_name="Pets", table_id=1)},
        aggregate_columns={1: list(columns.items())},
    )
    mask = resolve_read_mask(ctx, "Pets", mask_paths)
    if mask is None:
        return rows
    selections = dict(zip(mask.columns, mask.restrictions(mask.columns)))
    return [
        {
            c: row[c] if selections[c] is None else restrict_json(row[c], selections[c])
            for c in mask.columns
            if c in row
        }
        for row in rows
    ]


def test_read_mask_top_level_scalar():
    rows = [{"id": 1, "name": "Fido", "status": "active"}]
    result = _apply_read_mask(rows, ["id", "status"])
    assert result == [{"id": 1, "status": "active"}]


def test_read_mask_excludes_unlisted_fields():
    rows = [{"id": 1, "name": "Fido", "status": "active"}]
    result = _apply_read_mask(rows, ["name"])
    assert result == [{"name": "Fido"}]


def test_read_mask_nested_all_subfields():
    """No dot → include nested field with all its sub-fields."""
    rows = [{"id": 1, "_meta": {"source_id": "s", "created_at": "c"}}]
    result = _apply_read_mask(rows, ["_meta"])
    assert result == [{"_meta": {"source_id": "s", "created_at": "c"}}]


def test_read_mask_nested_specific_subfield():
    """dot-notation → restrict nested object to the named sub-field only."""
    rows = [{"id": 1, "_meta": {"source_id": "s", "created_at": "c"}}]
    result = _apply_read_mask(rows, ["_meta.source_id"])
    assert result == [{"_meta": {"source_id": "s"}}]


def test_read_mask_nested_multiple_subfields():
    rows = [{"_meta": {"source_id": "s", "created_at": "c", "updated_at": "u"}}]
    result = _apply_read_mask(rows, ["_meta.source_id", "_meta.created_at"])
    assert result == [{"_meta": {"source_id": "s", "created_at": "c"}}]


def test_read_mask_nested_list_of_objects():
    """Restriction applies element-wise to a repeated (list) nested field."""
    rows = [{"pets": [{"id": 1, "name": "Fido"}, {"id": 2, "name": "Rex"}]}]
    result = _apply_read_mask(rows, ["pets.name"])
    assert result == [{"pets": [{"name": "Fido"}, {"name": "Rex"}]}]


def test_read_mask_empty_paths_returns_all():
    rows = [{"id": 1, "name": "Fido", "status": "active"}]
    result = _apply_read_mask(rows, [])
    assert result == rows


def test_read_mask_top_level_then_nested_dot_overrides_to_all():
    """If a bare top-level path appears first, its value is None (all sub-fields)."""
    rows = [{"_meta": {"source_id": "s", "created_at": "c"}}]
    result = _apply_read_mask(rows, ["_meta", "_meta.source_id"])
    assert result == [{"_meta": {"source_id": "s", "created_at": "c"}}]


def test_read_mask_unknown_field_is_rejected_naming_the_path():
    """A path that is not a readable field is an error (HTTP 400), never silently dropped."""
    rows = [{"id": 1, "name": "Fido"}]
    with pytest.raises(ReadMaskError, match="nonexistent") as failure:
        _apply_read_mask(rows, ["nonexistent"])
    assert failure.value.path == "nonexistent"


# ---------------------------------------------------------------------------
# filter extraction logic (extracted inline)
# ---------------------------------------------------------------------------


class _FakeArgDef:
    """Minimal stand-in for a GraphQL argument definition."""

    def __init__(self, description: str):
        self.description = description


def _extract_filter_args(
    filter_dict: dict,
    field_args: dict[str, _FakeArgDef],
) -> tuple[list[str], dict]:
    """Replicate the filter-extraction block from endpoint_grpc_proxy.grpc_proxy."""
    arg_parts: list[str] = []
    nf_api_args: dict = {}
    if isinstance(filter_dict, dict):
        for arg_name, arg_def in field_args.items():
            if arg_def.description and "Native API filter" in arg_def.description:
                bare = arg_name.lstrip("_")
                val = (
                    filter_dict.get(arg_name) if arg_name in filter_dict else filter_dict.get(bare)
                )
                if val is not None:
                    lit = (
                        f'"{val}"'
                        if isinstance(val, str)
                        else str(val).lower()
                        if isinstance(val, bool)
                        else str(val)
                    )
                    arg_parts.append(f"{arg_name}: {lit}")
                    nf_api_args[bare] = val
    return arg_parts, nf_api_args


def test_filter_extraction_string_value():
    args = {"status": _FakeArgDef("Native API filter: status")}
    parts, nf = _extract_filter_args({"status": "active"}, args)
    assert parts == ['status: "active"']
    assert nf == {"status": "active"}


def test_filter_extraction_integer_value():
    args = {"_id": _FakeArgDef("Native API filter: id")}
    parts, nf = _extract_filter_args({"id": 42}, args)
    assert parts == ["_id: 42"]
    assert nf == {"id": 42}


def test_filter_extraction_bool_value():
    args = {"active": _FakeArgDef("Native API filter: active")}
    parts, nf = _extract_filter_args({"active": True}, args)
    assert parts == ["active: true"]
    assert nf == {"active": True}


def test_filter_extraction_prefixed_arg_bare_key_in_filter():
    """GQL arg name is _id; filter dict supplies bare 'id'."""
    args = {"_id": _FakeArgDef("Native API filter")}
    parts, nf = _extract_filter_args({"id": 99}, args)
    assert parts == ["_id: 99"]
    assert nf == {"id": 99}


def test_filter_extraction_prefixed_arg_prefixed_key_in_filter():
    """GQL arg name is _id; filter dict supplies '_id' directly."""
    args = {"_id": _FakeArgDef("Native API filter")}
    parts, nf = _extract_filter_args({"_id": 7}, args)
    assert parts == ["_id: 7"]
    assert nf == {"id": 7}


def test_filter_extraction_non_native_arg_excluded():
    """Args without 'Native API filter' in description must be ignored."""
    args = {
        "limit": _FakeArgDef("Maximum rows to return"),
        "status": _FakeArgDef("Native API filter: status"),
    }
    parts, nf = _extract_filter_args({"limit": 10, "status": "ok"}, args)
    assert all("limit" not in p for p in parts)
    assert "limit" not in nf
    assert "status" in nf


def test_filter_extraction_missing_value_excluded():
    """If filter_dict does not contain the arg, it must not appear in output."""
    args = {"status": _FakeArgDef("Native API filter")}
    parts, nf = _extract_filter_args({}, args)
    assert parts == []
    assert nf == {}


def test_filter_extraction_empty_filter_dict():
    args = {"status": _FakeArgDef("Native API filter")}
    parts, nf = _extract_filter_args({}, args)
    assert parts == []
    assert nf == {}
