# Copyright (c) 2025 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

import asyncio
import os

import asyncpg
import pytest
import trino


@pytest.fixture(scope="session")
def event_loop():
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()


@pytest.fixture(scope="session")
def pg_dsn() -> str:
    return (
        f"postgresql://{os.environ.get('PG_USER', 'provisa')}"
        f":{os.environ.get('PG_PASSWORD', 'provisa')}"
        f"@{os.environ.get('PG_HOST', 'localhost')}"
        f":{os.environ.get('PG_PORT', '5432')}"
        f"/{os.environ.get('PG_DATABASE', 'provisa')}"
    )


@pytest.fixture(scope="session")
def pg_pool(pg_dsn, event_loop):
    pool = event_loop.run_until_complete(
        asyncpg.create_pool(pg_dsn, min_size=1, max_size=3)
    )
    yield pool
    event_loop.run_until_complete(pool.close())


@pytest.fixture(scope="session")
def trino_conn():
    conn = trino.dbapi.connect(
        host=os.environ.get("TRINO_HOST", "localhost"),
        port=int(os.environ.get("TRINO_PORT", "8080")),
        user="test",
        catalog="postgresql",
        schema="public",
    )
    yield conn
    conn.close()


@pytest.fixture
def sample_config():
    import yaml
    from pathlib import Path

    config_path = Path(__file__).parent / "fixtures" / "sample_config.yaml"
    with open(config_path) as f:
        return yaml.safe_load(f)
