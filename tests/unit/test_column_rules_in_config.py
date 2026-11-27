# Copyright (c) 2026 Kenneth Stott
# Canary: 3e9a51c7-2d84-4b6f-9a07-c15e8d2f4b93
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-2278: a column rule keyed on a tag or a classification is model, as a row rule on a tag is.

A configuration declares it, an apply writes it, the store writes it back out as it was declared,
and an environment's tree carries it: projected into a file of its own and deployed back. A change
of a column's term reference rebuilds the runtime when a rule is keyed on the tag the column carries
through its term (pii), so the column comes under the rule, or is released, at once.
"""

# Requirements: REQ-2278

from __future__ import annotations

import contextlib
from typing import Any

import pytest
from pydantic import ValidationError
from sqlalchemy import delete, insert, select

from provisa.core.schema_org import column_rules
from tests.unit.gate_identity import serve_every_column


@pytest.fixture(autouse=True)
def _served(monkeypatch):
    """Every column is served: which a caller may read is REQ-2152's own tests."""
    serve_every_column(monkeypatch)


PARTIAL = {"type": "regex", "pattern": "^(.).*$", "replace": "$1***"}

RULES: list[dict[str, Any]] = [
    {"tag_id": "pii", "role_id": "masked_reader", "action": "mask", "mask": PARTIAL},
    {"tag_id": "reviewed", "role_id": "seller", "action": "hide"},
    {"classification": "email_address", "role_id": "masked_reader", "action": "mask"},
]


def _raw(rules: list[dict[str, Any]]) -> dict[str, Any]:
    role = {"capabilities": [], "domain_access": ["sales"]}
    return {
        "sources": [
            {"id": "crm", "type": "postgresql", "host": "h", "port": 5432, "database": "d"}
        ],
        "domains": [{"id": "sales"}],
        "roles": [
            {"id": "masked_reader", "description": "Reads masked.", **role},
            {"id": "seller", "description": "Sells.", **role},
        ],
        "tags": [{"id": "reviewed", "applies_to": ["column"]}],
        "tables": [
            {
                "source_id": "crm",
                "domain_id": "sales",
                "schema": "public",
                "table": "orders",
                "columns": [
                    {"name": "id", "data_type": "integer", "visible_to": ["masked_reader"]}
                ],
            }
        ],
        "column_rules": rules,
    }


@contextlib.asynccontextmanager
async def _store(control_plane, monkeypatch):
    from provisa.core import secrets_providers, secrets_store

    monkeypatch.setattr(secrets_providers, "_many_organisations", lambda: False)

    @contextlib.asynccontextmanager
    async def _unbound():
        yield

    monkeypatch.setattr(secrets_store, "bound_to_request_org", _unbound)
    async with control_plane.org().acquire() as conn:
        yield conn


async def _held(conn) -> set[tuple[Any, ...]]:
    rows = (await conn.execute_core(select(column_rules))).fetchall()
    return {
        (r.tag_id, r.classification, r.role_id, r.action, None if r.mask is None else str(r.mask))
        for r in rows
    }


def _in_store_order(rules: list[dict[str, Any]]) -> list[dict[str, Any]]:
    from provisa.core.store_config import _key

    return sorted(rules, key=_key)


async def test_a_declared_rule_is_applied_and_written_back_as_declared(control_plane, monkeypatch):
    from provisa.core.config_loader import apply_config, parse_config_dict
    from provisa.core.store_config import store_model, with_store_model

    raw = _raw(RULES)
    async with _store(control_plane, monkeypatch) as conn:
        await apply_config(parse_config_dict(raw), conn)
        model = await store_model(conn)
        assert model["column_rules"] == _in_store_order(RULES)
        # The file the store writes, applied again, holds the same rules.
        held = await _held(conn)
        await apply_config(parse_config_dict(with_store_model(raw, model)), conn)
        assert await _held(conn) == held
        assert (await store_model(conn))["column_rules"] == model["column_rules"]


async def test_a_rule_naming_what_is_not_there_is_refused_by_name(control_plane, monkeypatch):
    from provisa.core.config_loader import apply_config, parse_config_dict

    for rule, named in (
        ({"tag_id": "nope", "role_id": "masked_reader", "action": "hide"}, "tag 'nope'"),
        ({"classification": "nope", "role_id": "masked_reader", "action": "hide"}, "'nope'"),
        ({"tag_id": "pii", "role_id": "nobody", "action": "hide"}, "role 'nobody'"),
    ):
        async with _store(control_plane, monkeypatch) as conn:
            with pytest.raises(ValueError, match=named):
                await apply_config(parse_config_dict(_raw([rule])), conn)


def test_a_rule_keyed_on_both_or_neither_is_refused_at_parse():
    from provisa.core.config_loader import parse_config_dict

    for rule in (
        {
            "tag_id": "pii",
            "classification": "email_address",
            "role_id": "masked_reader",
            "action": "hide",
        },
        {"role_id": "masked_reader", "action": "hide"},
    ):
        with pytest.raises(ValidationError, match="exactly one"):
            parse_config_dict(_raw([rule]))


async def test_an_environment_tree_carries_the_rules_and_a_deploy_writes_them_back(
    control_plane, monkeypatch
):
    from provisa.core.config_loader import apply_config, parse_config_dict
    from provisa.core.env_deploy import deploy_tree
    from provisa.core.env_project import project

    async with _store(control_plane, monkeypatch) as conn:
        await apply_config(parse_config_dict(_raw(RULES)), conn)
        held = await _held(conn)
        tree = await project(conn, "org_default")
    assert tree["column-rules/tag/pii/masked_reader.yaml"] == {
        "action": "mask",
        "mask": PARTIAL,
        "role_id": "masked_reader",
        "tag_id": "pii",
    }
    assert tree["column-rules/tag/reviewed/seller.yaml"]["action"] == "hide"
    assert tree["column-rules/classification/email_address/masked_reader.yaml"]["action"] == "mask"

    db = control_plane.org()
    async with db.acquire() as conn:
        await conn.execute_core(delete(column_rules))
    report = await deploy_tree(db, "default", None, tree, ref="test")
    assert sorted(p for p in report.delta.added if p.startswith("column-rules/")) == sorted(
        p for p in tree if p.startswith("column-rules/")
    )
    async with db.acquire() as conn:
        assert await _held(conn) == held


async def test_a_deploy_refuses_a_rule_for_a_role_the_environment_lacks(control_plane, monkeypatch):
    from provisa.core.config_loader import apply_config, parse_config_dict
    from provisa.core.env_deploy import DeployError, deploy_tree
    from provisa.core.env_project import project

    async with _store(control_plane, monkeypatch) as conn:
        await apply_config(parse_config_dict(_raw(RULES)), conn)
        tree = await project(conn, "org_default")
    tree["column-rules/tag/pii/nobody.yaml"] = {"action": "hide"}
    tree["column-rules/neither/pii/masked_reader.yaml"] = {"action": "hide"}
    with pytest.raises(DeployError) as refused:
        await deploy_tree(control_plane.org(), "default", None, tree, ref="test")
    problems = "\n".join(refused.value.problems)
    assert "role 'nobody'" in problems
    assert "column-rules/neither/pii/masked_reader.yaml" in problems


# --- a change of a column's term reference -------------------------------------------------------


@contextlib.asynccontextmanager
async def _ruled_model(control_plane, monkeypatch, *, ruled: bool):
    """test_pii_term_moves' model (customers.home_address meaning a pii term), a rule masking pii
    for analyst when ``ruled``, and the runtime rebuild counted."""
    import provisa.api.app as appmod
    from tests.unit.test_pii_term_moves import _model

    rebuilds: list[int] = []

    async def _rebuild() -> None:
        rebuilds.append(1)

    monkeypatch.setattr(appmod, "_rebuild_schemas", _rebuild)
    async with _model(control_plane, monkeypatch) as (db, ids):
        if ruled:
            async with db.acquire() as conn:
                await conn.execute_core(
                    insert(column_rules).values(tag_id="pii", role_id="analyst", action="mask")
                )
        yield db, ids, rebuilds


async def _move_off_pii(ids: dict[str, int]) -> None:
    from tests.unit.test_pii_term_moves import _move

    await _move(ids, confirm_tag_loss=["pii"])


async def test_moving_a_columns_term_rebuilds_when_a_rule_is_keyed_on_pii(
    control_plane, monkeypatch
):
    from provisa.core.repositories import column_rule as column_rule_repo

    async with _ruled_model(control_plane, monkeypatch, ruled=True) as (db, ids, rebuilds):
        target = ("column", ids["customers"], "home_address")
        async with db.acquire() as conn:
            assert target in await column_rule_repo.ruled_columns(conn)
        await _move_off_pii(ids)
        assert rebuilds == [1]
        async with db.acquire() as conn:  # the rebuild reads the column released
            assert target not in await column_rule_repo.ruled_columns(conn)


async def test_moving_a_columns_term_does_not_rebuild_when_no_rule_is_keyed_on_pii(
    control_plane, monkeypatch
):
    async with _ruled_model(control_plane, monkeypatch, ruled=False) as (_db, ids, rebuilds):
        await _move_off_pii(ids)
        assert rebuilds == []
