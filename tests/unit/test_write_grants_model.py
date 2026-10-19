# Copyright (c) 2026 Kenneth Stott
# Canary: 9c3e182c-c8e4-4893-a850-1a061b2d0d00
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Write grants as a table declares and stores them, and the mutations they offer (REQ-2074)."""

# Requirements: REQ-2074, REQ-686

from __future__ import annotations

import pytest
from pydantic import ValidationError
from sqlalchemy import insert, select

from provisa.core import model_change
from provisa.core.models import Column, Table, WriteGrant
from provisa.core.schema_org import domains, registered_tables, roles, sources, table_write_grants


def _table(**extra) -> dict:
    return {
        "source_id": "pg",
        "domain_id": "sales",
        "table": "orders",
        "columns": [{"name": "id", "visible_to": []}, {"name": "status", "visible_to": []}],
        **extra,
    }


# --- what a configuration may say ----------------------------------------------------------


def test_writable_by_is_refused_by_name():
    with pytest.raises(ValidationError, match="'writable_by' is replaced by the table's"):
        Column(name="id", visible_to=[], writable_by=["admin"])  # type: ignore[call-arg]


def test_column_presets_are_refused_by_name():
    with pytest.raises(ValidationError, match="'column_presets' is replaced"):
        Table.model_validate(_table(column_presets=[{"column": "id", "source": "now"}]))


def test_one_grant_per_role_and_operation():
    with pytest.raises(ValidationError, match="two update grants for role 'r'"):
        Table.model_validate(
            _table(
                write_grants=[
                    {"role": "r", "operation": "update", "columns": ["id"]},
                    {"role": "r", "operation": "update", "columns": ["status"]},
                ]
            )
        )


def test_a_grant_names_only_the_tables_columns():
    with pytest.raises(ValidationError, match="does not have"):
        Table.model_validate(
            _table(write_grants=[{"role": "r", "operation": "insert", "columns": ["nope"]}])
        )


@pytest.mark.parametrize(
    ("grant", "match"),
    [
        ({"operation": "delete", "columns": ["id"]}, "a delete writes whole rows"),
        ({"operation": "insert", "rows": "TRUE"}, "an insert touches no existing rows"),
        (
            {
                "operation": "insert",
                "columns": ["status"],
                "presets": [{"column": "status", "source": "now"}],
            },
            "are preset",
        ),
    ],
)
def test_a_grant_fits_its_operation(grant, match):
    with pytest.raises(ValidationError, match=match):
        WriteGrant.model_validate({"role": "r", **grant})


# --- what a role is offered ----------------------------------------------------------------


def _mutations(grants: list[dict]) -> set[str]:
    from provisa.compiler.introspect import ColumnMetadata
    from provisa.compiler.schema_gen import SchemaInput, generate_schema

    table = {
        "id": 1,
        "source_id": "pg",
        "domain_id": "sales",
        "schema_name": "public",
        "table_name": "orders",
        "columns": [
            {"column_name": "id", "visible_to": ["r"]},
            {"column_name": "status", "visible_to": ["r"]},
        ],
        "write_ops": ["delete", "insert", "update"],
        "write_returns_rows": True,
        "write_refused_forms": [],
        "write_grants": [
            {"role": "r", "rows": None, "check": None, "presets": [], "columns": [], **g}
            for g in grants
        ],
    }
    schema = generate_schema(
        SchemaInput(
            tables=[table],
            relationships=[],
            column_types={
                1: [
                    ColumnMetadata(column_name="id", data_type="integer", is_nullable=False),
                    ColumnMetadata(column_name="status", data_type="varchar", is_nullable=True),
                ]
            },
            naming_rules=[],
            role={"id": "r", "capabilities": ["write"], "domain_access": ["*"]},
            domains=[{"id": "sales", "description": ""}],
        )
    )
    if schema.mutation_type is None:
        return set()
    out = set()
    for name, field in schema.mutation_type.fields.items():
        for arg_name, arg in field.args.items():
            inner = getattr(arg.type, "of_type", arg.type)
            if hasattr(inner, "fields") and arg_name in ("input", "set"):
                out.add(f"{name}({arg_name}: {','.join(sorted(inner.fields))})")
                break
        else:
            out.add(name)
    return out


def test_a_role_with_no_grant_is_offered_no_mutation():
    assert _mutations([]) == set()


def test_each_operation_is_offered_with_its_grants_columns():
    offered = _mutations(
        [
            {"operation": "insert", "columns": ["status"]},
            {"operation": "update", "columns": ["id", "status"]},
        ]
    )
    assert offered == {
        "insertOrders(input: status)",
        "updateOrders(set: id,status)",
        "upsertOrders(input: status)",
    }


def test_a_delete_grant_alone_offers_only_delete():
    assert _mutations([{"operation": "delete"}]) == {"deleteOrders"}


# --- how they are kept ---------------------------------------------------------------------


class _Reversing:
    """An encryption service whose ciphertext is the plaintext reversed: what was stored is
    seen to have passed through it."""

    def encrypt(self, data: bytes) -> bytes:
        return data[::-1]

    def decrypt(self, data: bytes) -> bytes:
        return data[::-1]


@pytest.mark.asyncio
async def test_grants_are_kept_with_their_predicates_encrypted(control_plane, monkeypatch):
    from provisa.core.repositories import write_grant as write_grant_repo

    monkeypatch.setattr(write_grant_repo, "encryption_service", _Reversing)
    db = control_plane.org()
    async with model_change.scope("grants"), db.acquire() as conn:
        await conn.execute_core(insert(sources).values(id="pg", type="postgresql"))
        await conn.execute_core(insert(domains).values(id="sales"))
        await conn.execute_core(insert(roles).values(id="support", capabilities=[]))
        table_id = (
            await conn.execute_core(
                insert(registered_tables)
                .values(source_id="pg", domain_id="sales", schema_name="public", table_name="o")
                .returning(registered_tables.c.id)
            )
        ).scalar_one()
        grant = WriteGrant(
            role="support",
            operation="update",
            columns=["status"],
            rows="status IN ('placed', 'packed')",
            check="TRUE",
            presets=[{"column": "updated", "source": "now"}],
        )
        await write_grant_repo.replace_for_table(conn, table_id, [grant], unheld=set())
        raw = (await conn.execute_core(select(table_write_grants.c.rows_expr))).scalar_one()
        assert bytes(raw) == b"status IN ('placed', 'packed')"[::-1]  # ciphertext at rest
        (kept,) = await write_grant_repo.for_table(conn, table_id)
    # REQ-2094: the grant comes back with its why, stamped with when it was decided.
    assert kept["why"]["decided_at"]
    assert WriteGrant.model_validate({**kept, "why": None}) == grant
