# Copyright (c) 2026 Kenneth Stott
# Canary: 9a4c1e07-6b3d-4f82-8e15-c2d7a0b94f63
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The org secrets a stored value names are found in one statement per plane (REQ-2087, REQ-2006).

An org's redaction rules are loaded when its runtime is first used, and a demo sandbox's open is
that first use: the search read each text and JSON column of every table in its own statement,
hundreds of round trips, 1.5 s of the open. It reads them in one statement for the model and one
for the platform plane, and finds the same names: a reference in a text column, inside a JSON
value, with a field selector, and on the platform plane."""

# Requirements: REQ-2006, REQ-2087

from __future__ import annotations

import pytest
from sqlalchemy import insert

from provisa.core import schema_admin, schema_org, secret_references
from provisa.core.database import Connection

pytestmark = [pytest.mark.unit, pytest.mark.asyncio]


async def test_every_name_is_found_in_one_statement_per_plane(control_plane, monkeypatch):
    model, admin = control_plane.org(), control_plane.platform
    async with model.acquire() as conn:
        await conn.execute_core(
            insert(schema_org.metadata.tables["domains"]).values(
                id="zoo", description="reads ${secret:in_text#password}"
            )
        )
        await conn.execute_core(
            insert(schema_org.metadata.tables["sources"]).values(
                id="zoo-db",
                type="postgresql",
                password_ref="${secret:pg_pw}",
                mapping={"token": "${secret:in_json}"},
            )
        )
    async with admin.acquire() as conn:
        await conn.execute_core(
            insert(schema_admin.environments).values(
                org_id="default",
                name="dev",
                created_by="${secret:on_platform}",
                parent="prod",
                data_mode="inherit",
            )
        )
    statements: list[str] = []
    real = Connection.execute_core

    async def _counted(self, stmt):
        statements.append(str(stmt))
        return await real(self, stmt)

    monkeypatch.setattr(Connection, "execute_core", _counted)

    found = await secret_references.referenced_names(admin, "default", environments={"prod": model})

    assert found == {"in_text", "pg_pw", "in_json", "on_platform"}
    # One for the model's text and JSON columns, one for the platform plane's; each encrypted
    # column is opened in its own.
    searched = [s for s in statements if "${secret:" in s or "%" in s]
    assert len(searched) == 2, len(searched)
