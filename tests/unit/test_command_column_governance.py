# Copyright (c) 2026 Kenneth Stott
# Canary: 9c4e2a71-5b3d-4f86-a1e0-7d2c6b8f3e19
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A command's output column carries a table column's tags, glossary term and classification,
in the same stores under a command-column kind named by the command and the column (REQ-1159,
amended 2026-11-25). Each is assigned, read back, and goes with its command.
"""

# Requirements: REQ-1159

from __future__ import annotations

import types
from typing import Any

import pytest
from sqlalchemy import delete, insert, select

import provisa.api.app as appmod
from provisa.api.admin import schema_mutation
from provisa.api.admin.types import TagAssignmentInput
from provisa.core.repositories import glossary as glossary_repo
from provisa.core.repositories import tag as tag_repo
from provisa.core.repositories.integrity import ObjectRef, remove_parts
from provisa.core.schema_org import (
    glossary_term_refs,
    glossary_terms,
    profiler_classifications,
    roles,
    tag_assignments,
    tags,
    tracked_functions,
)
from provisa.profiler import classifications
from provisa.profiler.classify import kind_of
from tests.unit.test_admin_deletes_need_the_domain import plane  # noqa: F401

ROLES = {
    "steward": {
        "capabilities": ["table_registration", "sensitive_data", "glossary_rw"],
        "domain_access": ["sales"],
    },
    "finance_steward": {
        "capabilities": ["table_registration", "sensitive_data"],
        "domain_access": ["finance"],
    },
}
OUTPUTS = [{"name": "ssn", "type": "varchar"}, {"name": "region", "type": "varchar"}]


def _info(role_id: str) -> Any:
    identity = types.SimpleNamespace(user_id="u1", roles=[role_id], acting_roles=[role_id])
    request = types.SimpleNamespace(
        state=types.SimpleNamespace(identity=identity, active_org_id="acme", role=role_id)
    )
    return types.SimpleNamespace(context={"request": request})


@pytest.fixture
async def governed(plane, monkeypatch):  # noqa: F811
    """The plane's commands, of_sales declaring two output columns; a tag applying to command
    columns; and the roles above."""

    async def _refresh() -> None:
        return None

    monkeypatch.setattr(schema_mutation, "_refresh_config_tags", _refresh)
    monkeypatch.setattr(
        appmod.state,
        "roles",
        {k: {"id": k, **v, "own_domain_access": v["domain_access"]} for k, v in ROLES.items()},
        raising=False,
    )
    async with plane.acquire() as conn:
        for role_id in ROLES:
            await conn.execute_core(insert(roles).values(id=role_id, description=role_id))
        await conn.execute_core(insert(tags).values(id="reviewed", applies_to=["command_column"]))
        await conn.execute_core(
            tracked_functions.update()
            .where(tracked_functions.c.name == "of_sales")
            .values(output_columns=OUTPUTS)
        )
    return plane


def _on(tag: str = "reviewed", column: str = "region", **extra: Any) -> TagAssignmentInput:
    return TagAssignmentInput(
        tag_id=tag,
        object_type="command_column",
        command_name="of_sales",
        column_name=column,
        **extra,
    )


# --- tags ----------------------------------------------------------------------------------------


async def test_a_tag_on_a_command_column_is_assigned_and_read_back(governed):
    result = await schema_mutation.Mutation().assign_tag(_info("steward"), _on())
    assert result.success, result.message
    async with governed.acquire() as conn:
        held = [
            a for a in await tag_repo.list_assignments(conn) if a["object_type"] == "command_column"
        ]
    assert [(a["tag_id"], a["command_name"], a["column_name"]) for a in held] == [
        ("reviewed", "of_sales", "region")
    ]
    async with governed.acquire() as conn:
        keys = (await conn.execute_core(select(tag_assignments.c.object_key))).scalars().all()
    assert keys == ["command_column:of_sales:region"]


async def test_a_sensitive_tag_on_a_command_column_needs_sensitive_data_in_its_domain(governed):
    with pytest.raises(PermissionError, match="sales"):
        await schema_mutation.Mutation().assign_tag(
            _info("finance_steward"), _on("pii", "ssn", reason="a person's tax id")
        )
    result = await schema_mutation.Mutation().assign_tag(
        _info("steward"), _on("pii", "ssn", reason="a person's tax id")
    )
    assert result.success, result.message


async def test_a_command_column_tag_names_its_command_and_column(governed):
    result = await schema_mutation.Mutation().assign_tag(
        _info("steward"),
        TagAssignmentInput(
            tag_id="reviewed", object_type="command_column", command_name="of_sales"
        ),
    )
    assert (result.success, result.code) == (False, "schema.tag_bad_target")


# --- glossary terms ------------------------------------------------------------------------------


async def _term(conn: Any, name: str) -> int:
    return (
        await conn.execute_core(
            insert(glossary_terms).values(name=name).returning(glossary_terms.c.id)
        )
    ).scalar_one()


async def test_a_term_on_a_command_column_is_grounded_and_read_back(governed):
    async with governed.acquire() as conn:
        term_id = await _term(conn, "sales region")
        await glossary_repo.ground_command_column(conn, term_id, "of_sales", "region")
        assert await glossary_repo.command_column_terms(conn, "of_sales") == {
            "region": {"id": term_id, "name": "sales region"}
        }
        # The column is in the command's domain, so the term is too.
        assert (await glossary_repo.term_domains(conn))[term_id] == {"sales"}
        assert await glossary_repo.unground_command_column(conn, "of_sales", "region")
        assert await glossary_repo.command_column_terms(conn, "of_sales") == {}


async def test_a_command_column_means_one_term(governed):
    async with governed.acquire() as conn:
        first = await _term(conn, "area")
        second = await _term(conn, "zone")
        await glossary_repo.ground_command_column(conn, first, "of_sales", "region")
        await glossary_repo.ground_command_column(conn, second, "of_sales", "region")
        assert (await glossary_repo.command_column_terms(conn, "of_sales"))["region"][
            "id"
        ] == second
        count = (
            await conn.execute_core(
                select(glossary_term_refs.c.id).where(
                    glossary_term_refs.c.command_name == "of_sales"
                )
            )
        ).fetchall()
    assert len(count) == 1


# --- classification ------------------------------------------------------------------------------


async def test_a_classification_of_a_command_column_is_recorded_and_read_back(governed):
    async with governed.acquire() as conn:
        await classifications.record_command_column(
            conn,
            "of_sales",
            "ssn",
            "us_ssn",
            status="accepted",
            evidence="the service returns a person's SSN",
            by="u1",
        )
        decided = await classifications.command_decisions(conn, "of_sales")
    (decision,) = decided.values()
    assert (decision["physical_column"], decision["kind"], decision["status"]) == (
        "ssn",
        "us_ssn",
        "accepted",
    )
    assert decision["table_id"] is None and decision["run_id"] is None


async def test_accepting_a_sensitive_kind_on_a_command_column_means_its_term_and_marks_pii(
    governed,
):
    kind = kind_of("us_ssn")
    async with governed.acquire() as conn:
        await classifications.accept_command_column_effects(
            conn, "of_sales", "sales", "ssn", kind, "the service returns a person's SSN"
        )
        terms = await glossary_repo.command_column_terms(conn, "of_sales")
        pii = [
            a
            for a in await tag_repo.list_assignments(conn)
            if a["object_type"] == "command_column" and a["tag_id"] == "pii"
        ]
    assert terms["ssn"]["name"] == kind.term.lower()
    assert [(a["command_name"], a["column_name"]) for a in pii] == [("of_sales", "ssn")]


# --- they go with the command --------------------------------------------------------------------


async def test_a_commands_column_governance_goes_with_it(governed):
    assert (await schema_mutation.Mutation().assign_tag(_info("steward"), _on())).success
    async with governed.acquire() as conn:
        term_id = await _term(conn, "sales region")
        await glossary_repo.ground_command_column(conn, term_id, "of_sales", "region")
        await classifications.record_command_column(
            conn, "of_sales", "ssn", "us_ssn", status="rejected", evidence="not a person", by=None
        )
        await remove_parts(conn, ObjectRef("command", "of_sales"))
        await conn.execute_core(
            delete(tracked_functions).where(tracked_functions.c.name == "of_sales")
        )
        for table in (tag_assignments, glossary_term_refs, profiler_classifications):
            left = (
                await conn.execute_core(select(table).where(table.c.command_name == "of_sales"))
            ).fetchall()
            assert left == [], table.name


# --- an environment's tree -----------------------------------------------------------------------


def test_a_command_column_tag_is_written_on_its_field_and_read_back():
    """In an environment's tree a command column's tag is on the command's file at ``#field:``,
    as a table column's is on its table's, and a deploy reads it back to the same assignment."""
    from provisa.core.env_deploy import _tags
    from provisa.core.env_project import _fragment

    row = {"object_type": "command_column", "command_name": "of_sales", "column_name": "region"}
    fragment = _fragment(row, {}, {})
    assert fragment == "#field:region"
    problems: list[str] = []
    (back,) = _tags(
        [{"tag_id": "reviewed", "on": fragment}],
        "commands/of_sales.yaml",
        problems,
        command_name="of_sales",
    )
    assert problems == []
    assert (back["object_type"], back["command_name"], back["column_name"], back["object_key"]) == (
        "command_column",
        "of_sales",
        "region",
        "command_column:of_sales:region",
    )
