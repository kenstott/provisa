# Copyright (c) 2026 Kenneth Stott
# Canary: 3e8b5d20-7c4f-4a91-b6d2-e1f09a7c5b38
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A relationship the configuration declares is registered or the load fails by its name.

The loader logged a relationship it could not register and carried on: retail's second
ASKS_ABOUT edge was dropped that way and a view's join was then refused. The one deferral kept is
designed: a relationship naming a table the file does not declare -- one a remote source's
registration brings later in the boot -- waits for that registration's retry, and the boot's
retry after every registration fails by name."""

from __future__ import annotations

from typing import Any

import pytest
from pydantic import ValidationError

from provisa.core import config_loader
from provisa.core.models import ProvisaConfig
from provisa.core.repositories import relationship as rel_repo

_CONN: Any = object()  # the repository's upsert is patched; the connection is never touched


def _config(*relationships: dict) -> dict:
    return {
        "sources": [
            {"id": "crm", "type": "postgresql", "host": "h", "database": "d", "username": "u",
             "password": "p"}
        ],
        "domains": [{"id": "sales"}],
        "tables": [
            {"source_id": "crm", "domain_id": "sales", "schema": "public", "table": name,
             "columns": [{"name": "email", "visible_to": ["admin"]}]}
            for name in ("requests", "eu_customers", "us_customers")
        ],
        "roles": [{"id": "admin", "capabilities": [], "domain_access": ["*"]}],
        "relationships": list(relationships),
    }  # fmt: skip


def _rel(rel_id: str, target: str, alias: str) -> dict:
    return {
        "id": rel_id,
        "source_table_id": "requests",
        "target_table_id": target,
        "source_column": "email",
        "target_column": "email",
        "cardinality": "one-to-many",
        "alias": alias,
    }


def test_two_relationships_of_one_type_from_one_table_are_refused_naming_both():
    with pytest.raises(ValidationError) as refused:
        ProvisaConfig.model_validate(
            _config(
                _rel("to-eu", "eu_customers", "ASKS_ABOUT"),
                _rel("to-us", "us_customers", "ASKS_ABOUT"),
            )
        )
    said = str(refused.value)
    assert "'to-eu'" in said and "'to-us'" in said and "'ASKS_ABOUT'" in said


def test_one_type_per_edge_loads():
    config = ProvisaConfig.model_validate(
        _config(
            _rel("to-eu", "eu_customers", "ASKS_ABOUT_EU"),
            _rel("to-us", "us_customers", "ASKS_ABOUT_US"),
        )
    )
    assert [r.alias for r in config.relationships] == ["ASKS_ABOUT_EU", "ASKS_ABOUT_US"]


@pytest.fixture
def missing(monkeypatch: pytest.MonkeyPatch) -> dict[str, str]:
    """The repository's upsert, refusing a relationship whose target is in ``missing``."""
    tables: dict[str, str] = {}

    async def _upsert(_conn: Any, rel: Any) -> None:
        if rel.target_table_id in tables:
            raise rel_repo.TableNotRegistered("Target", rel.target_table_id)

    monkeypatch.setattr(rel_repo, "upsert", _upsert)
    return tables


async def test_a_table_the_file_declares_that_did_not_register_fails_the_load(missing):
    config = ProvisaConfig.model_validate(_config(_rel("to-eu", "eu_customers", "ASKS_ABOUT")))
    missing["eu_customers"] = "declared"
    with pytest.raises(ValueError, match="relationship 'to-eu': Target table not registered"):
        await config_loader._upsert_relationships(_CONN, config, defer_unregistered=True)


async def test_a_table_a_remote_source_registers_later_waits_for_its_retry(missing):
    config = ProvisaConfig.model_validate(_config(_rel("to-remote", "countries", "IN")))
    missing["countries"] = "remote"
    await config_loader._upsert_relationships(_CONN, config, defer_unregistered=True)
    declared = config_loader.declared_table_names(config)
    rel = config.relationships[0]
    assert await config_loader.upsert_declared_relationship(_CONN, rel, declared) == "countries"


async def test_an_apply_nothing_registers_after_refuses_a_relationship_still_waiting(missing):
    config = ProvisaConfig.model_validate(_config(_rel("to-remote", "countries", "IN")))
    missing["countries"] = "remote"
    with pytest.raises(ValueError, match="'to-remote' names 'countries', which no registration"):
        await config_loader._upsert_relationships(_CONN, config, defer_unregistered=False)


async def test_any_other_refusal_fails_the_load_by_name(monkeypatch):
    async def _upsert(_conn: Any, rel: Any) -> None:
        raise ValueError("Alias 'ASKS_ABOUT' already exists for source table 'requests'")

    monkeypatch.setattr(rel_repo, "upsert", _upsert)
    config = ProvisaConfig.model_validate(_config(_rel("to-eu", "eu_customers", "ASKS_ABOUT")))
    with pytest.raises(ValueError, match="relationship 'to-eu': Alias 'ASKS_ABOUT' already"):
        await config_loader._upsert_relationships(_CONN, config, defer_unregistered=True)
