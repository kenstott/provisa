# Copyright (c) 2026 Kenneth Stott
# Canary: 9a5c7e13-4d28-4f06-b3a1-7e0c2d9f6b85
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Live cloud inventory source (REQ-1947): read through its pgwire server.

A ``Source`` row (type=cloudops) carries one or more clouds in ``mapping`` (see
``provisa/federation/cloudops.py``). ``provisa.federation.pgwire_replica`` writes the server's
model from them and starts the bundled ``pgwire-cloudops`` server from the source's own state
directory; every engine but Trino attaches it.

Credentials
-----------
There is no cloud emulator behind the adapter: real accounts are required, so this runs in the
warehouse lane. It reads the CLOUDOPS_* variables of all three clouds (``CLOUDOPS_`` + the
source's mapping key, upper-cased). The prefix is the source's own: the plain AWS_* variables of
the lane belong to its object store, and one test asserts that a source naming one cloud reads no
other.

What the adapter returns
------------------------
The source's pgwire server is the pinned release's (engine-v0.106.3); the same assertions passed
against engine-v0.108.0's. They are what the adapter answers today, to be tightened when a later
release fills it in. The last two points were seen on engine-v0.108.0's server and are avoided
here on either release:

- the tables in ``_EMPTY_BY_ADAPTER`` come back with no rows for the cloud named there although
  the account holds such resources;
- ``compute_resources`` is asserted for no particular cloud: the adapter answers a cloud's
  failed listing with no rows for that cloud and no error (it logs the failure at debug), and a
  first read returned no rows, then AWS alone, where a later read of the same server returned
  all three clouds;
- only ``cloud_provider`` is read: a statement that projects a column holding a null string
  (``SELECT *`` on any table with rows) ends the connection, the server logging
  ``NullPointerException: Cannot invoke "String.getBytes(java.nio.charset.Charset)" because
  "value" is null``;
- no statement binds a text parameter: the server plans an untyped parameter as a number and
  answers ``NumberFormatException: For input string: "<the text>"``.
"""

from __future__ import annotations

import contextlib
import os

import pytest

pytestmark = [
    pytest.mark.integration,
    pytest.mark.requires_cloudops,
    pytest.mark.requires_warehouse,
]

_aio = pytest.mark.asyncio(loop_scope="session")

_SOURCE_ID = "cloudops-itest"
_SCHEMA = _SOURCE_ID.replace("-", "_")
_TABLES = {
    "compute_resources",
    "compute_security_groups",
    "storage_resources",
    "kubernetes_clusters",
    "container_registries",
    "network_resources",
    "iam_resources",
    "database_resources",
}


# (table, cloud) pairs the adapter answers with no rows; see the module docstring.
_EMPTY_BY_ADAPTER: frozenset[tuple[str, str]] = frozenset(
    {
        ("iam_resources", "gcp"),
        ("database_resources", "gcp"),
        ("container_registries", "gcp"),
    }
)
_CLOUDS = ("azure", "aws", "gcp")


def _mapping(clouds: tuple[str, ...]) -> dict[str, str]:
    from provisa.federation.cloudops import CLOUDOPS_CLOUDS

    return {
        key: os.environ[f"CLOUDOPS_{key.upper()}"]
        for cloud in clouds
        for key, _ in (*CLOUDOPS_CLOUDS[cloud]["required"], *CLOUDOPS_CLOUDS[cloud]["optional"])
    }


def _source(source_id: str = _SOURCE_ID, clouds: tuple[str, ...] = _CLOUDS):
    from provisa.core.models import Source, SourceType

    return Source(id=source_id, type=SourceType.cloudops, mapping=_mapping(clouds))


@contextlib.contextmanager
def _serving(source, data_dir):
    """The source's pgwire server, started from a state directory of this run's own."""
    from provisa.federation import pgwire_replica as pr

    previous = os.environ.get("PROVISA_DATA_DIR")
    os.environ["PROVISA_DATA_DIR"] = str(data_dir)
    try:
        yield pr.ensure_endpoint(source)
    finally:
        pr.stop_endpoint(source.id)
        if previous is None:
            del os.environ["PROVISA_DATA_DIR"]
        else:
            os.environ["PROVISA_DATA_DIR"] = previous


async def _providers(endpoint, schema: str, table: str) -> dict[str, int]:
    """Rows of ``table`` per cloud, as the server answers them."""
    from provisa.federation import pgwire_replica as pr

    conn = await pr._pg_connect(endpoint.calcite_child_host, endpoint.pgwire_port)
    try:
        rows = await conn.fetch(f'SELECT "cloud_provider" FROM {schema}."{table}"')
    finally:
        await conn.close()
    counts: dict[str, int] = {}
    for r in rows:
        cloud = str(r["cloud_provider"]).lower()
        counts[cloud] = counts.get(cloud, 0) + 1
    return counts


@pytest.fixture(scope="module")
def endpoint(tmp_path_factory):
    with _serving(_source(), tmp_path_factory.mktemp("cloudops-state")) as served:
        yield served


@_aio
async def test_the_inventory_tables_are_offered(endpoint):
    from provisa.federation import pgwire_replica as pr

    conn = await pr._pg_connect(endpoint.calcite_child_host, endpoint.pgwire_port)
    try:
        rows = await conn.fetch(
            f"SELECT table_name FROM information_schema.tables WHERE table_schema = '{_SCHEMA}'"  # noqa: S608 — a constant; see the module docstring
        )
    finally:
        await conn.close()
    assert _TABLES <= {r[0] for r in rows}


@_aio
@pytest.mark.parametrize("table", ["storage_resources", "network_resources"])
async def test_each_configured_cloud_has_rows(endpoint, table):
    counts = await _providers(endpoint, _SCHEMA, table)
    assert set(counts) == set(_CLOUDS), counts


@_aio
@pytest.mark.parametrize("table", sorted(_TABLES))
async def test_every_table_is_read_and_names_only_configured_clouds(endpoint, table):
    counts = await _providers(endpoint, _SCHEMA, table)
    assert set(counts) <= set(_CLOUDS), counts
    for empty_table, cloud in _EMPTY_BY_ADAPTER:
        if empty_table == table:
            assert cloud not in counts, (
                f"{table} now has {cloud} rows: drop it from _EMPTY_BY_ADAPTER and assert them"
            )


@_aio
async def test_a_source_naming_aws_reads_aws_and_no_other_cloud(tmp_path):
    source = _source("cloudops-itest-aws", ("aws",))
    with _serving(source, tmp_path) as served:
        counts = await _providers(served, "cloudops_itest_aws", "storage_resources")
    assert set(counts) == {"aws"}, counts
