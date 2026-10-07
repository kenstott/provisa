# Copyright (c) 2026 Kenneth Stott
# Canary: 4457e9b2-be94-444d-9665-4ede19fc801a
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1491/REQ-1539/REQ-1942: a mutation outside prod is what the environment's mutation handling
says -- Refused, or Direct over a source bound to a connection of the environment's own, never an
inherited one; whether the person may write is their roles' answer."""

from __future__ import annotations

from types import SimpleNamespace

import sqlglot
import pytest

from provisa.core.request_context import reset_current_env, set_current_env
from provisa.core.environments import PROD
from provisa.pgwire._pipeline import _reject_unbound_writes


class _State:
    org_id = "acme"
    admin_db = object()

    def __init__(self, *, tables=None, binding_env=None, handling="direct"):
        self.tables = (
            tables if tables is not None else [{"table_name": "orders", "source_id": "s1"}]
        )
        # source_binding_env names only the environment's own bindings (provisa.api.app).
        self.source_binding_env = binding_env if binding_env is not None else {"s1": "feature"}
        self._runtime = SimpleNamespace(mutation_handling=handling)

    def _active_runtime(self):
        return self._runtime


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
    async def test_an_inherited_source_is_never_written_directly(self):
        # REQ-1942: its data is the parent's; Direct changes only data the environment owns.
        token = set_current_env("feature")
        try:
            with pytest.raises(PermissionError, match="Direct"):
                await _reject_unbound_writes(
                    _parse("DELETE FROM orders WHERE x = 1"), _State(binding_env={})
                )
        finally:
            reset_current_env(token)

    @pytest.mark.asyncio
    async def test_refused_handling_refuses_every_mutation_naming_the_environment(self):
        token = set_current_env("feature")
        try:
            with pytest.raises(PermissionError, match="'feature'.*Refused"):
                await _reject_unbound_writes(
                    _parse("UPDATE orders SET x = 1"), _State(handling="refused")
                )
        finally:
            reset_current_env(token)

    @pytest.mark.asyncio
    async def test_a_read_needs_no_mutation_handling(self):
        token = set_current_env("feature")
        try:
            await _reject_unbound_writes(_parse("SELECT * FROM orders"), _State(handling="refused"))
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
            with pytest.raises(PermissionError, match="not bound to a connection"):
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
            with pytest.raises(PermissionError, match="not bound to a connection"):
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
