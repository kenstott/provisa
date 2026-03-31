# Copyright (c) 2025 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Config loader: YAML → validate → resolve secrets → upsert PG → create Trino catalogs."""

from pathlib import Path

import asyncpg
import trino
import yaml

from provisa.core.models import ProvisaConfig
from provisa.core.secrets import resolve_secrets
from provisa.core.repositories import (
    source as source_repo,
    domain as domain_repo,
    table as table_repo,
    relationship as rel_repo,
    role as role_repo,
    rls as rls_repo,
)
from provisa.core import catalog


def parse_config(path: str | Path) -> ProvisaConfig:
    """Parse and validate a YAML config file. Does NOT resolve secrets."""
    with open(Path(path), encoding="utf-8") as f:
        raw = yaml.safe_load(f)
    return ProvisaConfig.model_validate(raw)


def parse_config_dict(data: dict) -> ProvisaConfig:
    """Parse and validate a config dict."""
    return ProvisaConfig.model_validate(data)


async def _load_config_in_txn(
    config: ProvisaConfig,
    conn: asyncpg.Connection,
    trino_conn: trino.dbapi.Connection | None,
) -> None:
    """Upsert full config into PG within caller's transaction scope."""
    # 1. Sources
    for src in config.sources:
        await source_repo.upsert(conn, src)
        if trino_conn is not None:
            resolved_pw = resolve_secrets(src.password)
            catalog.create_catalog(trino_conn, src, resolved_pw)

    # 2. Domains
    for dom in config.domains:
        await domain_repo.upsert(conn, dom)

    # 3. Naming rules
    await conn.execute("DELETE FROM naming_rules")
    for rule in config.naming.rules:
        await conn.execute(
            "INSERT INTO naming_rules (pattern, replacement) VALUES ($1, $2)",
            rule.pattern,
            rule.replace,
        )

    # 4. Roles (before tables/RLS so FK refs exist)
    for role in config.roles:
        await role_repo.upsert(conn, role)

    # 5. Tables + columns
    for tbl in config.tables:
        await table_repo.upsert(conn, tbl)

    # 6. Relationships (tables must exist first)
    for rel in config.relationships:
        await rel_repo.upsert(conn, rel)

    # 7. RLS rules (tables + roles must exist first)
    for rule in config.rls_rules:
        await rls_repo.upsert(conn, rule)


async def load_config(
    config: ProvisaConfig,
    pg_conn: asyncpg.Connection,
    trino_conn: trino.dbapi.Connection | None = None,
) -> None:
    """Upsert full config into PG within a transaction. Idempotent."""
    async with pg_conn.transaction():
        await _load_config_in_txn(config, pg_conn, trino_conn)


async def load_config_from_yaml(
    path: str | Path,
    pg_conn: asyncpg.Connection,
    trino_conn: trino.dbapi.Connection | None = None,
) -> ProvisaConfig:
    """Parse YAML, resolve secrets in source passwords, load into PG."""
    config = parse_config(path)
    await load_config(config, pg_conn, trino_conn)
    return config
