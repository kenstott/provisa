# Copyright (c) 2026 Kenneth Stott
# Canary: e1a97c34-58d2-4b06-93f1-7c2d40b8e615
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""What a connector's standard file sets on its sources (REQ-2221): a default applied at create that
a steward can change, a fixed value no save can change."""

# Requirements: REQ-2221

from __future__ import annotations

import json

import pytest

from provisa.api.admin import _connector_values as values
from provisa.api.admin.types import SourceInput
from provisa.core.models import ConnectorConfig, ConnectorRequirement, ConnectorSetting
from provisa.core.repositories import connector as connector_repo

pytestmark = pytest.mark.unit


def _block() -> dict:
    return {
        "name": "crm",
        "requires": [
            {"name": "db_uri", "secret": True},
            {"name": "region", "default_value": "eu-west-1"},
            {"name": "script", "multiline": True, "fixed_value": "print(1)"},
            {"name": "free"},
        ],
        "settings": [
            {"name": "replicate", "default_value": 0},
            {"name": "cache_ttl", "fixed_value": 300},
        ],
    }


@pytest.fixture
def block(monkeypatch) -> dict:
    held = _block()

    async def get(conn, name):
        return held if name == "crm" else None

    async def in_block(name):
        return held if name == "crm" else None

    monkeypatch.setattr(connector_repo, "get", get)
    monkeypatch.setattr(values, "_block", in_block)
    return held


def _input(join: dict | None = None, settings: dict | None = None, **fields) -> SourceInput:
    mapping = {
        "connector_join": {"connector": "crm", **(join or {})},
        "settings": {"free": "x", **(settings or {})},
    }
    return SourceInput(id="crm_prod", type="python", mapping_json=json.dumps(mapping), **fields)


class TestCreate:
    async def test_defaults_and_fixed_values_are_applied(self, block):
        given = _input()
        assert await values.settle_on_create(given) is None
        mapping = json.loads(given.mapping_json)
        assert given.replicate == 0 and given.cache_ttl == 300
        assert mapping["settings"] == {"free": "x", "region": "eu-west-1", "script": "print(1)"}

    async def test_a_steward_can_change_a_default_when_creating(self, block):
        given = _input({"chose": ["replicate"]}, {"region": "us-east-1"}, replicate=-1)
        assert await values.settle_on_create(given) is None
        mapping = json.loads(given.mapping_json)
        assert given.replicate == -1 and mapping["settings"]["region"] == "us-east-1"

    async def test_chose_does_not_reach_the_join(self, block):
        given = _input({"chose": ["replicate"]})
        await values.settle_on_create(given)
        assert "chose" not in json.loads(given.mapping_json)["connector_join"]

    async def test_a_create_that_differs_from_a_fixed_setting_is_refused(self, block):
        refused = await values.settle_on_create(_input(cache_ttl=60))
        assert refused is not None and refused.code == "schema.connector_value_fixed"
        assert "cache_ttl" in refused.message and "crm" in refused.message

    async def test_a_create_that_differs_from_a_fixed_require_is_refused(self, block):
        refused = await values.settle_on_create(_input(settings={"script": "print(2)"}))
        assert refused is not None and "script" in refused.message

    async def test_a_source_that_joins_no_connector_is_untouched(self, block):
        given = SourceInput(id="s", type="python", mapping_json='{"runtime": "python"}')
        assert await values.settle_on_create(given) is None
        assert given.mapping_json == '{"runtime": "python"}' and given.replicate is None

    async def test_an_unknown_connector_is_left_for_the_join_to_refuse(self, block):
        given = SourceInput(
            id="s", type="python", mapping_json=json.dumps({"connector_join": {"connector": "no"}})
        )
        assert await values.settle_on_create(given) is None


STORED = {"connector": "crm", "settings": {"script": "print(1)", "region": "eu-west-1"}}


class TestSave:
    async def test_a_fixed_setting_kept_passes(self, block):
        mapping = dict(STORED)
        assert await values.fixed_refusal(None, "s", STORED, {"cache_ttl": 300}, mapping) is None

    async def test_a_default_can_be_changed(self, block):
        mapping = {"connector": "crm", "settings": {"script": "print(1)", "region": "ap"}}
        assert await values.fixed_refusal(None, "s", STORED, {"replicate": -1}, mapping) is None

    async def test_changing_a_fixed_setting_is_refused(self, block):
        refused = await values.fixed_refusal(None, "s", STORED, {"cache_ttl": 5}, dict(STORED))
        assert refused is not None and refused.code == "schema.connector_value_fixed"
        assert refused.params == {"source": "s", "connector": "crm", "name": "cache_ttl"}

    async def test_changing_a_fixed_require_is_refused(self, block):
        mapping = {"connector": "crm", "settings": {"script": "print(2)"}}
        refused = await values.fixed_refusal(None, "s", STORED, {}, mapping)
        assert refused is not None and "script" in refused.message

    async def test_leaving_a_fixed_require_out_is_refused(self, block):
        refused = await values.fixed_refusal(None, "s", STORED, {}, {"settings": {}})
        assert refused is not None and "script" in refused.message

    async def test_a_save_that_drops_the_connector_stays_held(self, block):
        mapping: dict = {"settings": {"script": "print(1)"}}
        assert await values.fixed_refusal(None, "s", STORED, {}, mapping) is None
        assert mapping["connector"] == "crm"

    async def test_a_source_of_no_connector_is_not_held(self, block):
        assert await values.fixed_refusal(None, "s", {}, {"cache_ttl": 5}, {}) is None

    async def test_a_connector_no_longer_offered_holds_nothing(self, block):
        stored = {"connector": "gone"}
        assert await values.fixed_refusal(None, "s", stored, {"cache_ttl": 5}, {}) is None


class TestTheBlockHoldsItsRules:
    def test_a_setting_has_one_value(self):
        with pytest.raises(ValueError, match="cache_ttl"):
            ConnectorSetting(name="cache_ttl", default_value=1, fixed_value=2)

    def test_a_secret_has_no_value(self):
        with pytest.raises(ValueError, match="secret"):
            ConnectorRequirement(name="db", secret=True, default_value="a")

    def test_an_icon_is_an_image_data_uri(self):
        with pytest.raises(ValueError, match="icon"):
            ConnectorConfig(
                name="crm",
                git="https://git.example/crm.git",
                ref="main",
                commit="a" * 40,
                language="python",
                image="registry.example/data/crm",
                digest="sha256:" + "b" * 64,
                code_hash="c" * 64,
                arch="amd64",
                icon="https://evil.example/x.svg",
            )


@pytest.mark.parametrize(
    ("mutation", "arguments", "changes"),
    [
        (
            "update_source_replicate",
            {"source_id": "s", "replicate": 0},
            {"replicate": 0},
        ),
        (
            "update_source_cache",
            {"source_id": "s", "cache_enabled": False, "cache_ttl": 9},
            {"cache_enabled": False, "cache_ttl": 9},
        ),
        (
            "update_source_load_protection",
            {"source_id": "s", "load_protected": True, "off_peak_window": "01:00-02:00"},
            {"load_protected": True, "off_peak_window": "01:00-02:00", "off_peak_tz": "UTC"},
        ),
    ],
)
async def test_a_mutation_of_one_setting_is_held_to_the_fixed_value(
    monkeypatch, mutation, arguments, changes
):
    from unittest.mock import AsyncMock, MagicMock

    import provisa.api.admin.schema_mutation as schema_mutation
    from provisa.api.admin.schema_mutation import Mutation
    from provisa.api.admin.types import MutationResult
    from tests.unit.gate_identity import grant
    import provisa.api.app as app_mod

    refusal = MutationResult(success=False, message="fixed", code="schema.connector_value_fixed")
    seen: list[dict] = []

    async def refused(conn, source_id, given):
        seen.append(given)
        return refusal

    conn = MagicMock(execute_core=AsyncMock())
    acquire = MagicMock()
    acquire.__aenter__ = AsyncMock(return_value=conn)
    acquire.__aexit__ = AsyncMock(return_value=None)
    monkeypatch.setattr(values, "fixed_refusal_by_id", refused)
    monkeypatch.setattr(
        schema_mutation, "_get_pool", AsyncMock(return_value=MagicMock(acquire=lambda: acquire))
    )
    info = grant(monkeypatch, "source_registration", state=app_mod.state)[0]

    result = await getattr(Mutation(), mutation)(info, **arguments)

    assert result is refusal and seen == [changes]
    conn.execute_core.assert_not_awaited()
