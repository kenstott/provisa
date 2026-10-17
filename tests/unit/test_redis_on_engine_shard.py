# Copyright (c) 2026 Kenneth Stott
# Canary: ac0e9d99-24c7-4c59-a2c4-a2397b9991d9
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Redis is not available on a hosted engine shard (REQ-2073).

Trino reads a Redis catalog's tables from description files in a directory of the catalog's own,
and a running shard cannot be given one. A Redis source is refused by name when it is saved for an
org whose engine is a shard; one stored before that rule gets no catalog at boot, named at ERROR,
without stopping the boot. Elasticsearch, whose Trino connector reads columns from the index
mapping and takes no description directory, has none written for it.
"""

# Requirements: REQ-2073

from __future__ import annotations

import json
from types import SimpleNamespace

import pytest

from provisa.core import catalog as catalog_module
from provisa.core import engine_shard as shard_module
from provisa.core.models import Source
from provisa.core.repositories import source as source_repo


def _source(source_type: str, **extra) -> Source:
    return Source.model_validate(
        {"id": "cache", "type": source_type, "host": "redis", "port": 6379, **extra}
    )


@pytest.fixture
def engine_shard(monkeypatch):
    # The app installs the real resolver at import; imported first, so the one patched in here is
    # the one restored after.
    import provisa.api.app  # noqa: F401

    holder = SimpleNamespace(shard="shared_1")
    monkeypatch.setattr(shard_module, "_resolver", lambda: holder.shard)
    return holder


async def test_saving_a_redis_source_on_a_shard_is_refused_by_name(engine_shard):
    for write in (source_repo.upsert, source_repo.create):
        with pytest.raises(source_repo.RedisSourceRefused, match="'cache'.*shard shared_1"):
            await write(None, _source("redis"))


def test_off_a_shard_a_redis_source_is_saved(engine_shard):
    engine_shard.shard = None
    source_repo.refuse_on_engine_shard(_source("redis"))


def test_another_source_type_is_saved_on_a_shard(engine_shard):
    source_repo.refuse_on_engine_shard(_source("mongodb", port=27017))


async def test_an_apply_that_saves_one_answers_422_naming_it(engine_shard):
    from provisa.api.app import create_app

    with pytest.raises(source_repo.RedisSourceRefused) as refused:
        source_repo.refuse_on_engine_shard(_source("redis"))
    handler = create_app().exception_handlers[shard_module.EngineShardRefused]
    response = await handler(None, refused.value)  # type: ignore[arg-type]
    assert response.status_code == 422
    body = json.loads(response.body)
    assert body["code"] == "sources.redis_needs_engine_files"
    assert body["params"] == {"source": "cache", "shard": "shared_1"}


class _Coordinator:
    def __init__(self) -> None:
        self.statements: list[str] = []

    def cursor(self):
        return self

    def execute(self, sql: str) -> None:
        self.statements.append(sql)

    def fetchall(self):
        return []


def _redis_with_tables() -> Source:
    return _source(
        "redis",
        mapping={
            "tables": [
                {
                    "name": "sessions",
                    "key_pattern": "sessions:*",
                    "columns": [{"name": "user", "type": "VARCHAR"}],
                }
            ]
        },
    )


def test_a_stored_redis_source_gets_no_catalog_at_boot_and_does_not_stop_it(
    engine_shard, monkeypatch, tmp_path, caplog
):
    monkeypatch.setenv("PROVISA_TRINO_ETC_DIR", str(tmp_path))
    coordinator = _Coordinator()
    catalog_module.create_catalog(coordinator, _redis_with_tables(), "", "org_default__cache")
    assert coordinator.statements == []
    assert not any(tmp_path.rglob("*.json"))
    assert "'cache'" in caplog.text and "shared_1" in caplog.text


def test_off_a_shard_the_redis_catalog_reads_its_own_description_directory(
    engine_shard, monkeypatch, tmp_path
):
    engine_shard.shard = None
    monkeypatch.setenv("PROVISA_TRINO_ETC_DIR", str(tmp_path))
    coordinator = _Coordinator()
    catalog_module.create_catalog(coordinator, _redis_with_tables(), "", "org_default__cache")
    assert (tmp_path / "redis" / "org_default__cache" / "sessions.json").exists()
    (create,) = [s for s in coordinator.statements if s.startswith("CREATE CATALOG")]
    assert "redis.table-description-dir" in create and "org_default__cache" in create


def test_no_description_files_are_written_for_elasticsearch(tmp_path):
    from provisa.core.trino_catalog_files import write_table_definitions

    source = _source(
        "elasticsearch",
        port=9200,
        mapping={"tables": [{"name": "logs", "index": "logs-*", "columns": []}]},
    )
    assert write_table_definitions(source, "", "org_default__es", tmp_path) == []
    assert not any(tmp_path.rglob("*"))
