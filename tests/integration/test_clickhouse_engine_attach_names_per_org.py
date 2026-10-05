# Copyright (c) 2026 Kenneth Stott
# Canary: 25cd3bd6-0c82-46eb-81bb-46f566aebfee
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Integration: what the ClickHouse engine keeps to reach a source live — the database a
relational source is exposed under and the live view over it — is named after the source's
catalog name, which carries its org and environment (REQ-1266, REQ-1529).

Two orgs keep one ClickHouse store (embedded chdb on one path), each with a SQLite source ``inv``
of its own. Each org's runtime attaches its source; each reads its own rows, through databases of
its own."""

# Requirements: REQ-1266, REQ-1529

from __future__ import annotations

import sqlite3
from pathlib import Path
from types import SimpleNamespace

import pytest

from provisa.compiler.naming import live_view_schema, org_prefixed_catalog, source_to_catalog

pytestmark = [pytest.mark.integration]

pytest.importorskip("chdb")

_BOOT = "acme"


def _sqlite(path: Path, ids: list[int]) -> Path:
    con = sqlite3.connect(path)
    con.execute("CREATE TABLE widget (id INTEGER)")
    con.executemany("INSERT INTO widget VALUES (?)", [(i,) for i in ids])
    con.commit()
    con.close()
    return path


def _source(path: Path, org: str, env: str | None) -> SimpleNamespace:
    return SimpleNamespace(
        id="inv",
        catalog=org_prefixed_catalog(org, source_to_catalog("inv"), default_org=_BOOT, env=env),
        type=SimpleNamespace(value="sqlite"),
        path=str(path),
        schema_name="main",
        table_name="widget",
        federation_hints={},
    )


def test_orgs_and_environments_on_one_clickhouse_store_each_read_their_own_source(tmp_path):
    from provisa.federation.clickhouse_runtime import ClickHouseFederationRuntime

    boot = _source(_sqlite(tmp_path / "acme.db", [1, 2, 3]), _BOOT, None)
    beta = _source(_sqlite(tmp_path / "beta.db", [900]), "beta", None)
    beta_qa = _source(_sqlite(tmp_path / "beta_qa.db", [950]), "beta", "qa")
    store = str(tmp_path / "chdb")
    runtimes = [ClickHouseFederationRuntime.embedded(path=store) for _ in range(3)]
    try:
        # The other org's attach first: the boot org's must not land on its databases.
        for runtime, source in zip((runtimes[1], runtimes[2], runtimes[0]), (beta, beta_qa, boot)):
            runtime.attach_source(source)

        def ids(runtime, source) -> list[int]:
            schema = live_view_schema(source.catalog, source.schema_name)
            sql = f'SELECT id FROM "{schema}"."widget" ORDER BY id'
            return [r[0] for r in runtime.run_sync(sql).rows]

        assert ids(runtimes[0], boot) == [1, 2, 3]
        assert ids(runtimes[1], beta) == [900]
        assert ids(runtimes[2], beta_qa) == [950]
        databases = {r[0] for r in runtimes[0].run_sync("SELECT name FROM system.databases").rows}
        assert {"ch_inv", "ch_org_beta__inv", "ch_org_beta_env_qa__inv"} <= databases
    finally:
        for runtime in runtimes:
            runtime.close()
