# Copyright (c) 2026 Kenneth Stott
# Canary: 7843e533-bbb8-42f8-9b4e-42b5a1305674
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""REQ-433 (amended 2026-11-20): names are unique per domain. First-come ownership of a source
table is withdrawn: the name checked is (domain, effective table name), and the same name may be
used in another domain."""

# Requirements: REQ-433

from __future__ import annotations

import types

import pytest

from provisa.api.admin import schema_helpers

pytestmark = [pytest.mark.asyncio(loop_scope="session")]


async def test_first_come_ownership_is_gone():
    """REQ-433 amended 2026-11-20: no (source, table name) claim is checked."""
    assert not hasattr(schema_helpers, "_dataset_ownership_conflict")


# --- names are unique per domain ----------------------------------------------------------------


class _Rows:
    def __init__(self, row):
        self._row = row

    def fetchone(self):
        return self._row


class _NameConn:
    """Answers the (domain, effective name) query with the rows held in that domain."""

    def __init__(self, held: dict[tuple[str, str], tuple[str, str]]):
        self.held = held

    async def execute_core(self, stmt):
        params = stmt.compile().params
        domain = next(v for k, v in params.items() if k.startswith("domain_id"))
        name = next(v for k, v in params.items() if k.startswith("coalesce"))
        hit = self.held.get((domain, name))
        return _Rows(
            None if hit is None else types.SimpleNamespace(source_id=hit[0], schema_name=hit[1])
        )


async def test_a_second_table_of_the_same_name_in_one_domain_is_refused():
    conn = _NameConn({("pet-store", "beer"): ("pg", "public")})
    refused = await schema_helpers._domain_table_conflict(
        conn, "pet-store", "beer", "wikidata", "wd"
    )
    assert refused is not None and "already used in domain 'pet-store'" in refused


async def test_the_same_name_in_another_domain_is_not_refused():
    conn = _NameConn({("pet-store", "beer"): ("pg", "public")})
    assert (
        await schema_helpers._domain_table_conflict(conn, "brewery", "beer", "wikidata", "wd")
        is None
    )
