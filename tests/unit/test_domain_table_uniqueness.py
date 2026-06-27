# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""Domain + table uniqueness.

The DB constraint is only UNIQUE(source_id, schema_name, table_name), but every query
layer addresses a table as domain.table. Two same-named tables in one domain make that
reference ambiguous, so registration rejects it and startup validates the whole registry.
"""

from unittest.mock import AsyncMock

import pytest

from provisa.api.admin.schema import _domain_table_conflict
from provisa.api.app import _assert_domain_table_unique


def _t(domain, table, source="s1", schema="public"):
    return {"domain_id": domain, "table_name": table, "source_id": source, "schema_name": schema}


class TestStartupValidation:
    def test_unique_registry_passes(self):
        result = _assert_domain_table_unique(
            [_t("d1", "pets"), _t("d1", "users"), _t("d2", "pets")]
        )
        assert result is None

    def test_same_name_different_domain_allowed(self):
        # Uniqueness is per-domain — the same table name in two domains is fine.
        result = _assert_domain_table_unique([_t("d1", "pets", "s1"), _t("d2", "pets", "s2")])
        assert result is None

    def test_same_domain_table_from_two_sources_rejected(self):
        with pytest.raises(RuntimeError, match="domain.+table"):
            _assert_domain_table_unique([_t("d1", "pets", "s1"), _t("d1", "pets", "s2")])

    def test_error_names_the_collision(self):
        with pytest.raises(RuntimeError) as ei:
            _assert_domain_table_unique([_t("d1", "pets", "s1"), _t("d1", "pets", "s2")])
        assert "d1.pets" in str(ei.value)


class TestRegistrationCheck:
    @pytest.mark.asyncio
    async def test_conflict_with_different_source_rejected(self):
        conn = AsyncMock()
        conn.fetchrow.return_value = {"source_id": "other", "schema_name": "public"}
        msg = await _domain_table_conflict(conn, "d1", "pets", "s1", "public")
        assert msg is not None
        assert "pets" in msg and "d1" in msg

    @pytest.mark.asyncio
    async def test_no_conflict_returns_none(self):
        conn = AsyncMock()
        conn.fetchrow.return_value = None
        msg = await _domain_table_conflict(conn, "d1", "pets", "s1", "public")
        assert msg is None

    @pytest.mark.asyncio
    async def test_re_registering_same_table_allowed(self):
        # The query excludes the same (source, schema) — so an update never self-conflicts.
        conn = AsyncMock()
        conn.fetchrow.return_value = None
        msg = await _domain_table_conflict(conn, "d1", "pets", "s1", "public")
        assert msg is None
        # Verify the exclusion is part of the query.
        sql = conn.fetchrow.call_args.args[0]
        assert "NOT (source_id" in sql
