# Copyright (c) 2026 Kenneth Stott
# Canary: 6b1d9f2a-3c4e-5a6b-7c8d-9e0f1a2b3c4d
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""E2E: Accumulo as a source, materialized through the Provisa engine (REQ-1097).

Accumulo is NOT in the Trino-connector bucket that test_cassandra_source_e2e.py documents. Unlike
cassandra/exasol/redis, ``trino_connector_name("accumulo")`` is None
(provisa/federation/trino_connectors.py:419 — accumulo is absent from build_trino_connectors():387).
Its intended reach is the SOURCE-ADAPTER / MATERIALIZATION pipeline, the same family that lands
openapi/graphql_remote rows: fetch the source's current rows through an injected row-fetcher, land
them into the materialization store via the ONE write face, expose the landed replica as a
physical-named view, and query it through Provisa's single governed pipeline. An accumulo source is
defined with EXPLICIT column definitions (like redis): each column maps to an Accumulo column
family/qualifier via provisa.accumulo.source.AccumuloColumn (REQ-250/251/252).

The materialization seam this test WOULD drive once accumulo is reachable:
  1. provisa/accumulo/source.py AccumuloSourceConfig/AccumuloTableConfig/AccumuloColumn declare the
     source + explicit family/qualifier column mappings.
  2. provisa/federation/residency.py:110 run_prep -> resolve_landing_args (:53) -> the injected
     ResidencyLoader.load (:129, provisa/events/source_loader.py:60 SourceRowLoader) fetches the
     accumulo rows -> runtime.materialize_source (provisa/federation/duckdb_runtime.py:186) lands
     them via provisa/federation/store_writer.py:286 land() and _expose_landed() (:262) exposes the
     read view. This is the SAME governed pipeline every materialized source uses — not a parallel
     path.
  3. Query the exposed replica through the engine terminal (execute_engine), assert the seeded rows.

WHY THIS TEST IS A REACH-GATED SKIP (documented gap, not a dodge)
-----------------------------------------------------------------
Accumulo has NO working reach path in Provisa today. Verified against source:

  * NOT in provisa/federation/trino_connectors.py TRINO_CONNECTORS -> create_catalog
    (provisa/core/catalog.py:112-114) logs "No Trino connector for source type 'accumulo'" and
    returns WITHOUT creating a catalog. So there is no engine-scannable catalog for accumulo.
  * NOT in provisa/events/source_loader.py:33 _ADAPTER_FETCH_ONLY and there is no accumulo adapter
    loader wired in provisa/events/app_wiring.py. SourceRowLoader.load (:76) therefore falls through
    to the engine SQL terminal (:97-102) — SELECT * FROM "<catalog>"... — against the catalog that
    was never created. The fetch fails; nothing can be landed.
  * NOT in provisa/federation/pgwire_replica.py:43 PGWIRE_REPLICA_TYPES ({"files","sharepoint",
    "splunk"}) — no bundled Calcite pgwire replica to read it either.
  * provisa/accumulo/source.py exposes ONLY generate_catalog_properties()/generate_table_definitions()
    (catalog config) — no scan/fetch/client function — and the real catalog path builds properties
    from the connector classes, never from this adapter, so the module is dead for the live pipeline
    (only tests/unit/test_accumulo_source.py exercises it). There is no Accumulo client library in
    the venv (only `thrift`).

Making accumulo reachable is a FEATURE (wire an accumulo ResidencyLoader + real Accumulo client, or
register a Trino accumulo connector) — out of scope for wiring an e2e test, and the wider REQ-1097
design states the Trino connector is None by design. On this host the blocker is also physical: no
maintained upstream Accumulo image exists (apache/accumulo and bare `accumulo` are absent from
Docker Hub), and a Hadoop+ZooKeeper+Accumulo JVM stack cannot fit this host's ~12.5 GB Docker VM
alongside the Trino stack. See docker-compose.test.yml (the accumulo service is defined but the
harness unconditionally discards it — tests/conftest.py::_DockerServiceManager).

The skip flips to a live run automatically once a reach mechanism is wired: _accumulo_reachable()
below is derived from the same source-of-truth sets, so no edit to the gate is needed — only the
product wiring.
"""

from __future__ import annotations

import os
import subprocess
import time

import pytest

pytestmark = [pytest.mark.integration, pytest.mark.asyncio(loop_scope="session")]


def _accumulo_reachable() -> bool:
    """True only when Provisa has a real reach path for accumulo — derived from the source-of-truth
    registries, so the gate needs no manual edit when the feature lands. Reachable iff accumulo has a
    Trino connector (engine-scannable catalog) OR a wired non-engine row-fetch (adapter-fetch-only or
    a pgwire-replica bundle)."""
    from provisa.events.source_loader import _ADAPTER_FETCH_ONLY
    from provisa.federation.pgwire_replica import PGWIRE_REPLICA_TYPES
    from provisa.federation.trino_connectors import trino_connector_name

    return (
        trino_connector_name("accumulo") is not None
        or "accumulo" in _ADAPTER_FETCH_ONLY
        or "accumulo" in PGWIRE_REPLICA_TYPES
    )


_REACH_SKIP = pytest.mark.skipif(
    not _accumulo_reachable(),
    reason=(
        "accumulo has no Provisa reach path (REQ-1097): absent from TRINO_CONNECTORS "
        "(trino_connector_name is None), _ADAPTER_FETCH_ONLY, and PGWIRE_REPLICA_TYPES; "
        "provisa/accumulo/source.py is catalog-props-only with no row-fetch. Live e2e requires "
        "wiring an accumulo ResidencyLoader/client (a feature). Heavy Hadoop+ZK stack, no maintained "
        "image, and this host's Docker memory also block booting it — see module docstring."
    ),
)

_ITEST_PROJECT = os.environ.get("PROVISA_ITEST_PROJECT", "provisa-itest")

# The explicit column mapping the source configures (REQ-250/251/252): one Accumulo column
# family/qualifier per engine column. Seed writes rows into these exact family:qualifier cells.
_ACCUMULO_TABLE = "widgets"
_FAMILY = "cf"
_WIDGETS = [(1, "Widget A"), (2, "Widget B"), (3, "Widget C")]


def _accumulo_container_id() -> str:
    """The running `accumulo` service's container id, found by compose labels (no fixed name — the
    isolated stack never uses container_name, so it never collides with a parallel run)."""
    out = subprocess.run(
        [
            "docker", "ps", "-q",
            "--filter", f"label=com.docker.compose.project={_ITEST_PROJECT}",
            "--filter", "label=com.docker.compose.service=accumulo",
        ],  # fmt: skip
        capture_output=True,
        text=True,
        check=True,
    ).stdout.strip()
    ids = out.splitlines()
    if not ids:
        raise RuntimeError(f"No running accumulo container for project {_ITEST_PROJECT!r}")
    return ids[0]


def _accumulo_shell(container_id: str, script: str) -> None:
    """Run an accumulo shell script inside the container (no python driver installed; the image
    ships the `accumulo shell` CLI). Used to create the table and insert the seed cells."""
    subprocess.run(
        ["docker", "exec", "-i", container_id, "accumulo", "shell", "-u", "root", "-p", "secret"],
        input=script,
        capture_output=True,
        text=True,
        check=True,
    )


def _seed_accumulo() -> None:
    """Create the widgets table and insert 3 rows across the cf:id / cf:name cells the source maps."""
    container_id = _accumulo_container_id()
    lines = [f"createtable {_ACCUMULO_TABLE}", f"table {_ACCUMULO_TABLE}"]
    for wid, name in _WIDGETS:
        lines.append(f"insert row{wid} {_FAMILY} id {wid}")
        lines.append(f"insert row{wid} {_FAMILY} name {name}")
    lines.append("exit")
    _accumulo_shell(container_id, "\n".join(lines) + "\n")


@_REACH_SKIP
@pytest.mark.requires_accumulo
async def test_accumulo_source_materialized_and_queryable():
    """Configure an accumulo Source with explicit AccumuloColumn family/qualifier mappings,
    materialize its rows through the residency pipeline, and query the landed replica end-to-end.

    Drives the REAL governed materialization path (provisa/federation/residency.py::run_prep ->
    SourceRowLoader.load -> runtime.materialize_source -> store_writer.land -> engine read view) — the
    same seam every materialized source uses. Skipped today because accumulo has no wired row-fetch
    (see module docstring); the body is the flow it will run once that reach is added.
    """
    from provisa.accumulo.source import (
        AccumuloColumn,
        AccumuloSourceConfig,
        AccumuloTableConfig,
    )
    from provisa.core.models import Source, SourceType

    _seed_accumulo()

    # Explicit column definitions: each engine column maps to an Accumulo family/qualifier.
    cfg = AccumuloSourceConfig(
        id="accumulo-itest",
        instance="accumulo",
        zookeepers="accumulo:2181",
        username="root",
        password="secret",
        tables=[
            AccumuloTableConfig(
                name=_ACCUMULO_TABLE,
                accumulo_table=_ACCUMULO_TABLE,
                columns=[
                    AccumuloColumn(name="id", data_type="INTEGER", family=_FAMILY, qualifier="id"),
                    AccumuloColumn(
                        name="name", data_type="VARCHAR", family=_FAMILY, qualifier="name"
                    ),
                ],
            )
        ],
    )
    src = Source(id=cfg.id, type=SourceType.accumulo, host="accumulo", port=2181)

    # Materialize the accumulo rows through the engine's residency pipeline and query the landed
    # replica. The concrete loader + engine wiring is supplied by whatever mechanism makes accumulo
    # reachable; this asserts the seeded rows come back through the ONE governed query path.
    rows = await _materialize_and_query(src, cfg)
    assert sorted((int(r[0]), r[1]) for r in rows) == _WIDGETS


async def _materialize_and_query(src, cfg):  # pragma: no cover - runs only once accumulo is wired
    """Land the accumulo source via residency.run_prep and read the exposed replica back.

    Deliberately routes through provisa.federation.residency + the engine runtime (not a bespoke
    reader) so the test exercises Provisa's single materialization/query pipeline. Reachable only
    when _accumulo_reachable() is True; guarded by the module-level skip until then.
    """
    deadline = time.monotonic() + 60
    last_err: Exception | None = None
    while time.monotonic() < deadline:
        try:
            from provisa.federation.engine import build_engine
            from provisa.federation.runtime import EngineRuntime

            runtime = EngineRuntime(build_engine("duckdb"), None)
            columns = [(c.name, c.data_type) for c in cfg.tables[0].columns]
            rows = await _fetch_accumulo_rows(src, cfg)
            await runtime.materialize_source(src, columns, rows)
            catalog = _catalog_name(src.id)
            result = await runtime.execute_engine(
                f'SELECT id, name FROM "{catalog}"."default"."{_ACCUMULO_TABLE}" ORDER BY id'
            )
            return result.rows
        except Exception as e:  # connector may not be warm immediately after land
            last_err = e
            time.sleep(2)
    raise RuntimeError(f"accumulo materialize/query never succeeded: {last_err!r}")


async def _fetch_accumulo_rows(src, cfg):  # pragma: no cover - runs only once accumulo is wired
    """Fetch the accumulo source's current rows through its wired adapter row-fetcher.

    This is the piece REQ-1097 leaves unimplemented: an accumulo ResidencyLoader (the analogue of
    provisa/events/source_loader.py::make_openapi_loader) that scans the configured table's
    family:qualifier cells into row dicts. Until it exists, _accumulo_reachable() is False and this
    is never reached.
    """
    from provisa.events.source_loader import SourceRowLoader

    loader = SourceRowLoader(None)
    from types import SimpleNamespace

    table = SimpleNamespace(schema_name="default", table_name=_ACCUMULO_TABLE)
    return await loader.load(src, table)


def _catalog_name(source_id: str) -> str:
    from provisa.compiler.naming import source_to_catalog

    return source_to_catalog(source_id)
