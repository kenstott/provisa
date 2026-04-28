# Copyright (c) 2026 Kenneth Stott
# Canary: bae0b12d-255c-47a8-916d-a46248b5d271
# Canary: PLACEHOLDER
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Unit tests for provisa/cypher/label_map.py — focus on _resolve_id_column."""

import pytest

from provisa.cypher.label_map import _resolve_id_column


# ---------------------------------------------------------------------------
# _resolve_id_column
# ---------------------------------------------------------------------------

def test_join_target_wins_over_all():
    # target_pk explicitly says "user_id" — must win even if "id" is present
    assert _resolve_id_column("User", ["id", "user_id", "name"], {"User": "user_id"}) == "user_id"


def test_exact_id_column():
    assert _resolve_id_column("Person", ["id", "name", "age"], {}) == "id"


def test_exact_underscore_id():
    assert _resolve_id_column("Event", ["_id", "title"], {}) == "_id"


def test_exact_pk():
    assert _resolve_id_column("Record", ["pk", "value"], {}) == "pk"


def test_exact_oid():
    assert _resolve_id_column("Doc", ["oid", "body"], {}) == "oid"


def test_exact_id_case_insensitive():
    assert _resolve_id_column("Table", ["ID", "name"], {}) == "ID"


def test_single_suffix_id():
    # Only one column ending in _id — unambiguous
    assert _resolve_id_column("Order", ["order_id", "amount", "created_at"], {}) == "order_id"


def test_ambiguous_suffix_falls_through_to_first_col():
    # Two _id columns — suffix heuristic is ambiguous; falls to first column
    result = _resolve_id_column("Link", ["source_id", "target_id", "weight"], {})
    assert result == "source_id"


def test_single_prefix_id():
    assert _resolve_id_column("Record", ["id_hash", "value"], {}) == "id_hash"


def test_first_column_fallback():
    assert _resolve_id_column("Thing", ["ref", "name", "code"], {}) == "ref"


def test_empty_columns_returns_hard_fallback():
    assert _resolve_id_column("Ghost", [], {}) == "id"


def test_type_not_in_target_pk_uses_column_heuristic():
    # target_pk has a different type — should not affect this one
    assert _resolve_id_column("Person", ["id", "name"], {"Company": "cid"}) == "id"


def test_join_target_overrides_exact_id():
    # Even if "id" is in columns, the join-declared PK wins
    assert _resolve_id_column("Company", ["id", "cid"], {"Company": "cid"}) == "cid"
