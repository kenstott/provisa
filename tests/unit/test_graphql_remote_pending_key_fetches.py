# Copyright (c) 2026 Kenneth Stott
# Canary: placeholder
# Canary: placeholder
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1865 with REQ-1929: a remote GraphQL table saved with row_materialize has its key fetch
checked while its registration is still in progress, before the transaction that stores it --
so the check reads the pending table, not only the tables this process already holds."""

# Requirements: REQ-1865, REQ-1929

from __future__ import annotations

import asyncio
from types import SimpleNamespace

import pytest

from provisa.api.admin import _graphql_table_registration as reg_mod
from provisa.graphql_remote.by_keys import BY_KEYS


def test_a_table_in_its_own_registration_is_checked_by_its_pending_key_fetches():
    reg = {"source_id": "hasura", "namespace": "", "tables": []}
    state = SimpleNamespace(graphql_remote_sources={"hasura": reg})
    fetches = {"id": {"field": "albums", "arg": "where"}}
    reg_mod.hold_pending(reg, "albums", {"field_name": "albums", BY_KEYS: fetches})
    try:
        got = asyncio.run(reg_mod.stored_key_fetches(state, "hasura", "albums"))
    finally:
        reg_mod.drop_pending("hasura", "albums")
    assert got == fetches


def test_a_table_neither_registered_nor_pending_is_refused_by_name():
    reg = {"source_id": "hasura", "namespace": "", "tables": []}
    state = SimpleNamespace(graphql_remote_sources={"hasura": reg})
    with pytest.raises(KeyError, match="albums"):
        asyncio.run(reg_mod.stored_key_fetches(state, "hasura", "albums"))
