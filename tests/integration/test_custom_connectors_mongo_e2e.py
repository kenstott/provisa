# Copyright (c) 2026 Kenneth Stott
# Canary: a1f6c284-3d70-4b9e-9c52-8e0a2f7b16d4
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""E2E: the config-driven Postgres FDW connector federates a LIVE MongoDB via mongo_fdw (REQ-1177).

mongo_fdw is REQ-1177's named PG conformance target. This proves the config-only path end to end — no
skip, no substitution: scripts/build_mongo_fdw.sh builds mongo_fdw + its bundled mongo-c-driver against
the embedded PG 16.2 (cached, restart-safe), we install the module + driver dylibs into the pgserver
embedded PG, an operator declares a `mongo_custom` source_type in config/custom_connectors.yaml (via
PROVISA_CUSTOM_CONNECTORS) on the GENERIC pg_fdw descriptor, and PgFederationRuntime drives the
no-IMPORT branch — CREATE SERVER … mongo_fdw + a bare USER MAPPING + CREATE FOREIGN TABLE with per-table
OPTIONS(database/collection) — to read the docker-seeded provisa.product_reviews collection. No engine
code change; a green run proves the connector federates MongoDB itself.
"""

from __future__ import annotations

import os
import shutil
import subprocess
import sys
import tempfile
import textwrap
from pathlib import Path
from types import SimpleNamespace

import pytest

pytestmark = [
    pytest.mark.integration,
    pytest.mark.requires_mongodb,
    pytest.mark.asyncio(loop_scope="session"),
]

asyncpg = pytest.importorskip("asyncpg")
pgserver = pytest.importorskip("pgserver")
psycopg2 = pytest.importorskip("psycopg2")

from provisa.federation.pg_runtime import PgFederationRuntime  # noqa: E402
from tests.integration.test_embedded_pg_fdw_engine_e2e import (  # noqa: E402
    _CACHE,
    _build_fdw_artifacts,
    _have_build_tools,
)

_REPO = Path(__file__).resolve().parents[2]
_DEPS = _CACHE / "mongo_fdw_deps" / "lib"
_BUILT_LIB = _CACHE / "pg162" / "lib" / "postgresql"
_BUILT_EXT = _CACHE / "pg162" / "share" / "postgresql" / "extension"

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


def _build_mongo_fdw() -> None:
    """Build mongo_fdw + its bundled driver against the cached PG 16.2 (restart-safe no-op if present)."""
    _build_fdw_artifacts()  # ensure the embedded PG 16.2 (pg_config the FDW builds against) exists
    subprocess.run(["bash", str(_REPO / "scripts" / "build_mongo_fdw.sh")], check=True)


def _install_mongo_fdw_into_pgserver() -> None:
    """Install mongo_fdw + colocate its driver dylibs into the pgserver install, with an @loader_path
    rpath so the @rpath references in mongo_fdw and libmongoc resolve to the colocated libs."""
    pginstall = Path(pgserver.__file__).parent / "pginstall"
    dst_lib = pginstall / "lib" / "postgresql"
    dst_ext = pginstall / "share" / "postgresql" / "extension"
    suffix = "dylib" if (dst_lib / "plpgsql.dylib").exists() else "so"

    # The two driver dylibs, under the versioned names mongo_fdw/libmongoc load via @rpath.
    for real, name in [
        (_DEPS / "libmongoc-1.0.0.0.0.dylib", "libmongoc-1.0.0.dylib"),
        (_DEPS / "libbson-1.0.0.0.0.dylib", "libbson-1.0.0.dylib"),
    ]:
        dst = dst_lib / name
        shutil.copy(real, dst)
        os.chmod(dst, 0o755)

    fdw_dst = dst_lib / f"mongo_fdw.{suffix}"
    shutil.copy(_BUILT_LIB / "mongo_fdw.dylib", fdw_dst)
    os.chmod(fdw_dst, 0o755)

    def _add_loader_rpath(p: Path) -> None:
        out = subprocess.run(["otool", "-l", str(p)], capture_output=True, text=True).stdout
        if "@loader_path" not in out:
            subprocess.run(["install_name_tool", "-add_rpath", "@loader_path", str(p)], check=True)

    _add_loader_rpath(fdw_dst)  # resolves @rpath/lib{mongoc,bson} to the colocated dylibs
    _add_loader_rpath(dst_lib / "libmongoc-1.0.0.dylib")  # resolves @rpath/libbson for the driver

    for f in _BUILT_EXT.glob("mongo_fdw*"):
        shutil.copy(f, dst_ext / f.name)


@pytest.fixture(scope="session")
def pg_with_mongo_fdw():
    if sys.platform != "darwin" or not _have_build_tools():
        pytest.skip("mongo_fdw build here is wired for macOS (dylib/install_name_tool) + a C toolchain")
    _build_mongo_fdw()
    _install_mongo_fdw_into_pgserver()
    base = tempfile.mkdtemp(prefix="provisa_mongo_fdw_")
    server = pgserver.get_server(base)
    # Prove the module + driver dylibs load before any federation is attempted.
    conn = psycopg2.connect(server.get_uri())
    conn.autocommit = True
    conn.cursor().execute("CREATE EXTENSION IF NOT EXISTS mongo_fdw")
    conn.close()
    yield server


async def test_config_driven_mongo_fdw_federates_live_mongodb(pg_with_mongo_fdw, tmp_path, monkeypatch):
    server = pg_with_mongo_fdw
    mongo_port = int(os.environ["MONGO_PORT"])  # set by the requires_mongodb docker provisioning

    cfg = tmp_path / "custom_connectors.yaml"
    cfg.write_text(textwrap.dedent(_DESCRIPTOR))
    monkeypatch.setenv("PROVISA_CUSTOM_CONNECTORS", str(cfg))

    rt = PgFederationRuntime(engine_dsn=server.get_uri())
    try:
        # The descriptor made a brand-new source_type reachable on the PG engine — no code change.
        assert rt._engine.reachable("mongo_custom")

        src = SimpleNamespace(
            id="reviews",
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

        res = rt.run_sync(
            'SELECT "product_id", "reviewer", "rating" FROM "mongo"."product_reviews" '
            'ORDER BY "product_id", "reviewer"'
        )
        assert res.column_names == ["product_id", "reviewer", "rating"]
        assert len(res.rows) == 10  # db/mongo-init.js seeds 10 product_reviews docs
        assert res.rows[0] == (1, "alice", 5)
        assert res.rows[-1] == (7, "jack", 1)
        assert {r[0] for r in res.rows} == {1, 2, 3, 4, 5, 6, 7}
    finally:
        rt.close()
