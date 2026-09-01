# Copyright (c) 2026 Kenneth Stott
# Canary: 4457e9b2-be94-444d-9665-4ede19fc801a
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1491/REQ-1539: a write needs an established binding; permission is the roles' answer."""

from __future__ import annotations

import sqlglot
import pytest

from provisa.api.org_runtime import reset_current_env, set_current_env
from provisa.core.environments import PROD
from provisa.pgwire._pipeline import _reject_unbound_writes


class _State:
    org_id = "acme"
    admin_db = object()

    def __init__(self, *, tables=None, binding_env=None):
        self.tables = (
            tables if tables is not None else [{"table_name": "orders", "source_id": "s1"}]
        )
        self.source_binding_env = binding_env if binding_env is not None else {"s1": "base"}


def _parse(sql):
    return sqlglot.parse_one(sql, dialect="postgres")


class TestProdIsUntouched:
    @pytest.mark.asyncio
    async def test_prod_never_consults_a_binding(self):
        # prod branches from nothing, so every binding it has is its own — and every pre-environment
        # write must cost exactly what it always did.
        await _reject_unbound_writes(_parse("INSERT INTO orders VALUES (1)"), _State(tables=[]))


class TestBranchWrites:
    @pytest.mark.asyncio
    async def test_a_bound_source_is_written_through_without_a_permission_question(self):
        # REQ-1539: whether this person may write is what their roles answer, not the environment.
        token = set_current_env("feature")
        try:
            await _reject_unbound_writes(_parse("UPDATE orders SET x = 1"), _State())
        finally:
            reset_current_env(token)

    @pytest.mark.asyncio
    async def test_an_inherited_binding_is_writable(self):
        # The binding was resolved from 'base'; inheriting it is not itself a reason to refuse.
        token = set_current_env("feature")
        try:
            await _reject_unbound_writes(
                _parse("DELETE FROM orders WHERE x = 1"), _State(binding_env={"s1": "base"})
            )
        finally:
            reset_current_env(token)

    @pytest.mark.asyncio
    async def test_a_read_is_not_a_write(self):
        token = set_current_env("feature")
        try:
            await _reject_unbound_writes(_parse("SELECT * FROM orders"), _State(binding_env={}))
        finally:
            reset_current_env(token)

    @pytest.mark.asyncio
    async def test_unbound_source_is_refused(self):
        # Nothing in the lineage bound it, so there is no connection to write through — REQ-1491.
        token = set_current_env("feature")
        try:
            with pytest.raises(PermissionError, match="unbound"):
                await _reject_unbound_writes(
                    _parse("INSERT INTO orders VALUES (1)"), _State(binding_env={})
                )
        finally:
            reset_current_env(token)

    @pytest.mark.asyncio
    async def test_unregistered_target_is_refused_rather_than_skipped(self):
        # Which binding the write would travel cannot be established, and a write with no
        # established target is what REQ-1491 refuses.
        token = set_current_env("feature")
        try:
            with pytest.raises(PermissionError, match="not a registered table"):
                await _reject_unbound_writes(_parse("INSERT INTO whatever VALUES (1)"), _State())
        finally:
            reset_current_env(token)

    @pytest.mark.asyncio
    async def test_merge_is_a_write_too(self):
        token = set_current_env("feature")
        try:
            with pytest.raises(PermissionError, match="unbound"):
                await _reject_unbound_writes(
                    _parse(
                        "MERGE INTO orders USING src ON orders.id = src.id "
                        "WHEN MATCHED THEN UPDATE SET x = 1"
                    ),
                    _State(binding_env={}),
                )
        finally:
            reset_current_env(token)


def test_prod_constant_is_what_the_guard_compares_against():
    assert PROD == "prod"
