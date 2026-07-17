# Copyright (c) 2026 Kenneth Stott
# Canary: e2db644b-0333-43b3-8fb5-f4c0264f4465
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""REQ-415: Hasura V2 FK-derived relationship naming (inflection-based)."""

from __future__ import annotations

from provisa.discovery.fk_introspect import _m2o_alias, _o2m_alias


class TestDefaultStyleUnchanged:
    def test_m2o_alias_is_ref_table_verbatim(self):
        assert _m2o_alias("users") == "users"

    def test_o2m_alias_is_fk_table_verbatim(self):
        assert _o2m_alias("orders") == "orders"


class TestHasuraV2Style:
    def test_m2o_object_alias_singularized(self):
        # Object (many-to-one) relationship → singular ref table.
        assert _m2o_alias("users", hasura_v2_style=True) == "user"
        assert _m2o_alias("categories", hasura_v2_style=True) == "category"

    def test_o2m_array_alias_pluralized(self):
        # Array (one-to-many) relationship → plural FK table.
        assert _o2m_alias("order", hasura_v2_style=True) == "orders"
        assert _o2m_alias("category", hasura_v2_style=True) == "categories"

    def test_already_singular_m2o_unchanged(self):
        # inflect returns False for an already-singular noun; alias falls back to input.
        assert _m2o_alias("user", hasura_v2_style=True) == "user"

    def test_already_plural_o2m_unchanged(self):
        assert _o2m_alias("orders", hasura_v2_style=True) == "orders"
