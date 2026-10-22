# Copyright (c) 2026 Kenneth Stott
# Canary: a1f6c284-3d70-4b9e-9c52-8e0a2f7b16d4
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""E2E: the config-driven Postgres FDW connector federates a LIVE MongoDB via mongo_fdw (REQ-1177).

mongo_fdw is REQ-1177's named PG conformance target. This proves the config-only path end to end — no
skip, no substitution: scripts/build_mongo_fdw.sh builds mongo_fdw + its bundled mongo-c-driver against
PG 16.11 into the test platform's private pginstall (tests/embedded_pg.py), an operator declares a `mongo_custom` source_type in config/custom_connectors.yaml (via
PROVISA_CUSTOM_CONNECTORS) on the GENERIC pg_fdw descriptor, and PgFederationRuntime drives the
no-IMPORT branch — CREATE SERVER … mongo_fdw + a bare USER MAPPING + CREATE FOREIGN TABLE with per-table
OPTIONS(database/collection) — to read the docker-seeded provisa.product_reviews collection. No engine
code change; a green run proves the connector federates MongoDB itself.
"""

from __future__ import annotations

import os
import textwrap
from types import SimpleNamespace

import pytest

pytestmark = [
    pytest.mark.integration,
    pytest.mark.requires_mongodb,
    pytest.mark.asyncio(loop_scope="session"),
]

asyncpg = pytest.importorskip("asyncpg")

import psycopg  # noqa: E402

from provisa.federation.pg_runtime import PgFederationRuntime  # noqa: E402
from tests.embedded_pg import start_local_postgres  # noqa: E402

_DESCRIPTOR = """
connectors:
  - engine: postgres
    source_type: mongo_custom
    kind: pg_fdw
    extension: mongo_fdw
    mechanism: attach_r
    supports_import: false
    server_options:
      address: "{host}"
      port: "{port}"
    user_mapping: {}
    table_options:
      database: "{database}"
      collection: "{table_name}"
"""


@pytest.fixture(scope="session")
def pg_with_mongo_fdw():
    pg = start_local_postgres(mongo_fdw=True)
    # Prove the module + driver libraries load before any federation is attempted.
    with psycopg.connect(pg.dsn(), autocommit=True) as c:
        c.execute("CREATE EXTENSION IF NOT EXISTS mongo_fdw")
    yield pg
    pg.stop()


async def test_config_driven_mongo_fdw_federates_live_mongodb(
    pg_with_mongo_fdw, tmp_path, monkeypatch
):
    server = pg_with_mongo_fdw
    mongo_port = int(os.environ["MONGO_PORT"])  # set by the requires_mongodb docker provisioning

    cfg = tmp_path / "custom_connectors.yaml"
    cfg.write_text(textwrap.dedent(_DESCRIPTOR))
    monkeypatch.setenv("PROVISA_CUSTOM_CONNECTORS", str(cfg))

    rt = PgFederationRuntime(engine_dsn=server.dsn())
    try:
        # The descriptor made a brand-new source_type reachable on the PG engine — no code change.
        assert rt._engine.reachable("mongo_custom")

        src = SimpleNamespace(
            id="reviews",
            catalog="reviews",
            type=SimpleNamespace(value="mongo_custom"),
            host="127.0.0.1",
            port=mongo_port,
            database="provisa",
            username=None,
            password=None,
            schema_name="mongo",
            table_name="product_reviews",
            columns=[
                ("product_id", "int4"),
                ("reviewer", "text"),
                ("rating", "int4"),
                ("comment", "text"),
            ],
            federation_hints={},
        )
        rt.attach_source(src)

        # REQ-1730: exposed under the folded "{catalog}_{schema}" schema the ENGINE route targets
        # (source_to_catalog("reviews") == "reviews").
        res = rt.run_sync(
            'SELECT "product_id", "reviewer", "rating" FROM "reviews_mongo"."product_reviews" '
            'ORDER BY "product_id", "reviewer"'
        )
        assert res.column_names == ["product_id", "reviewer", "rating"]
        assert len(res.rows) == 10  # db/mongo-init.js seeds 10 product_reviews docs
        assert res.rows[0] == (1, "alice", 5)
        assert res.rows[-1] == (7, "jack", 1)
        assert {r[0] for r in res.rows} == {1, 2, 3, 4, 5, 6, 7}
    finally:
        rt.close()
