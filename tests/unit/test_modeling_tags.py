# Copyright (c) 2026 Kenneth Stott
# Canary: 8c0b8c93-2878-4d8a-8edc-4b3de4f1d1e9
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Unit tests for the shared modeling-role tag formatter (REQ-1320)."""

from __future__ import annotations

from provisa.core.modeling_tags import append_modeling_tag


def test_no_role_returns_description_unchanged():
    assert append_modeling_tag("Order records", None, None) == "Order records"


def test_no_role_no_description_returns_empty_string():
    assert append_modeling_tag(None, None, None) == ""


def test_role_only_appends_bracket():
    assert append_modeling_tag("Order records", "fact", None) == "Order records [fact]"


def test_role_and_history_appends_joined_bracket():
    assert (
        append_modeling_tag("Customer master", "dimension", "scd2")
        == "Customer master [dimension, scd2]"
    )


def test_role_with_no_description_returns_tag_alone():
    assert append_modeling_tag(None, "fact", None) == "[fact]"
    assert append_modeling_tag("", "dimension", "scd2") == "[dimension, scd2]"
