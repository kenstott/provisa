# Copyright (c) 2026 Kenneth Stott
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

import re


from provisa.api.data.endpoint_grpc_proxy import _pascal_to_snake, _proto_type_name
from provisa.grpc.proto_gen import _to_proto_type_name, _to_proto_field_name


# ---------------------------------------------------------------------------
# _pascal_to_snake
# ---------------------------------------------------------------------------


def test_pascal_to_snake_simple():
    assert _pascal_to_snake("Pets") == "pets"


def test_pascal_to_snake_multi_word():
    assert _pascal_to_snake("PetOwners") == "pet_owners"


def test_pascal_to_snake_already_snake():
    assert _pascal_to_snake("pet_owners") == "pet_owners"


def test_pascal_to_snake_all_caps_acronym():
    # The regex only inserts _ before an uppercase preceded by a lowercase/digit,
    # so consecutive caps like "API" are kept together as a run.
    assert _pascal_to_snake("MyAPIKey") == "my_apikey"


def test_pascal_to_snake_single_char():
    assert _pascal_to_snake("X") == "x"


def test_pascal_to_snake_lowercase_unchanged():
    assert _pascal_to_snake("pets") == "pets"


def test_pascal_to_snake_mixed_with_digits():
    # digit before uppercase triggers insertion
    assert _pascal_to_snake("Pet3Owners") == "pet3_owners"


def test_pascal_to_snake_multiple_words():
    assert _pascal_to_snake("UserProfileData") == "user_profile_data"


# ---------------------------------------------------------------------------
# _proto_type_name (endpoint_grpc_proxy) and _to_proto_type_name (proto_gen)
# They must agree: both convert GQL name → proto PascalCase
# ---------------------------------------------------------------------------


def test_proto_type_name_domain_prefixed():
    assert _proto_type_name("PS__Pets") == "PsPets"


def test_proto_type_name_no_prefix():
    assert _proto_type_name("Pets") == "Pets"


def test_proto_type_name_lowercase_prefix():
    # prefix.capitalize() lowercases the rest of the prefix, so "ps" → "Ps"; "pets" unchanged.
    assert _proto_type_name("ps__pets") == "Pspets"


def test_to_proto_type_name_agrees_with_proxy_helper():
    """Both modules must produce identical output for same input."""
    cases = ["PS__Pets", "Pets", "ps__pets", "my__Type"]
    for name in cases:
        assert _proto_type_name(name) == _to_proto_type_name(name), (
            f"Mismatch for {name!r}: proxy={_proto_type_name(name)!r} "
            f"proto_gen={_to_proto_type_name(name)!r}"
        )


def test_to_proto_field_name_replaces_double_underscore():
    assert _to_proto_field_name("ps__pets") == "ps_pets"


def test_to_proto_field_name_no_double_underscore():
    assert _to_proto_field_name("pets") == "pets"


# ---------------------------------------------------------------------------
# read_mask filtering logic (extracted inline — the logic lives in grpc_proxy
# but we test it directly as a pure function extracted here)
# ---------------------------------------------------------------------------


def _apply_read_mask(field_selections: list[str], mask_paths: list[str]) -> list[str]:
    """Replicate the read_mask filtering block from endpoint_grpc_proxy.grpc_proxy."""
    if not mask_paths:
        return field_selections

    top_level_map: dict[str, set[str] | None] = {}
    for p in mask_paths:
        parts = p.split(".", 1)
        top = parts[0]
        sub = parts[1] if len(parts) > 1 else None
        if top not in top_level_map:
            top_level_map[top] = set() if sub else None
        if sub and top_level_map[top] is not None:
            top_level_map[top].add(sub)  # type: ignore[union-attr]
        elif not sub:
            top_level_map[top] = None

    filtered: list[str] = []
    for sel in field_selections:
        sel_name = sel.split()[0]
        snake_name = _pascal_to_snake(sel_name)
        key = (
            snake_name
            if snake_name in top_level_map
            else (sel_name if sel_name in top_level_map else None)
        )
        if key is None:
            continue
        sub_filter = top_level_map[key]
        if sub_filter is None or "{" not in sel:
            filtered.append(sel)
        else:
            current_subs = re.findall(r"\b(\w+)\b", sel.split("{", 1)[1].rstrip("}").strip())
            restricted = [
                s for s in current_subs if s in sub_filter or _pascal_to_snake(s) in sub_filter
            ]
            if restricted:
                filtered.append(f"{sel_name} {{ {' '.join(restricted)} }}")
    return filtered


def test_read_mask_top_level_scalar():
    sels = ["id", "name", "status"]
    result = _apply_read_mask(sels, ["id", "status"])
    assert result == ["id", "status"]


def test_read_mask_excludes_unlisted_fields():
    sels = ["id", "name", "status"]
    result = _apply_read_mask(sels, ["name"])
    assert result == ["name"]
    assert "id" not in result
    assert "status" not in result


def test_read_mask_nested_all_subfields():
    """No dot → include nested field with all its sub-selections."""
    sels = ["id", "_meta { source_id created_at }"]
    result = _apply_read_mask(sels, ["_meta"])
    assert result == ["_meta { source_id created_at }"]
    assert "id" not in result


def test_read_mask_nested_specific_subfield():
    """dot-notation → restrict nested selection to named sub-field only."""
    sels = ["id", "_meta { source_id created_at }"]
    result = _apply_read_mask(sels, ["_meta.source_id"])
    assert len(result) == 1
    assert result[0] == "_meta { source_id }"
    assert "created_at" not in result[0]


def test_read_mask_nested_multiple_subfields():
    sels = ["_meta { source_id created_at updated_at }"]
    result = _apply_read_mask(sels, ["_meta.source_id", "_meta.created_at"])
    assert len(result) == 1
    assert "source_id" in result[0]
    assert "created_at" in result[0]
    assert "updated_at" not in result[0]


def test_read_mask_empty_paths_returns_all():
    sels = ["id", "name", "status"]
    result = _apply_read_mask(sels, [])
    assert result == sels


def test_read_mask_top_level_then_nested_dot_overrides_to_all():
    """If top-level path appears before dot path, result is None (all sub-fields)."""
    # "_meta" seen first → None (all). Then "_meta.source_id" should not narrow.
    sels = ["_meta { source_id created_at }"]
    result = _apply_read_mask(sels, ["_meta", "_meta.source_id"])
    # None means include all subs, so the full nested selection is kept
    assert result == ["_meta { source_id created_at }"]


def test_read_mask_unknown_field_excluded():
    sels = ["id", "name"]
    result = _apply_read_mask(sels, ["nonexistent"])
    assert result == []


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
