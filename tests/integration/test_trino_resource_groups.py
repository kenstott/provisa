# Copyright (c) 2026 Kenneth Stott
# Canary: b69e3d56-3882-4d5a-9146-08cbebd46af6
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-056: on a real Trino, each org's engine statements run in that org's resource group.

The isolated stack's Trino mounts trino/etc/resource-groups.json. A statement is sent the way
the engine sends it (user ``provisa``, source ``provisa/<org>``) and its group is read back from
``system.runtime.queries``. This proves the named group in the selector's source pattern expands
into the group name, which no render or unit test can.
"""

from __future__ import annotations

import os
import uuid

import pytest
import trino

pytestmark = [pytest.mark.integration]


def _connect(user: str, source: str | None):
    kwargs = dict(
        host="localhost",
        port=int(os.environ["TRINO_PORT"]),
        user=user,
        catalog="system",
        schema="runtime",
        http_scheme="http",
    )
    if source is not None:
        kwargs["source"] = source
    return trino.dbapi.connect(**kwargs)


def _group_of_a_statement(user: str, source: str | None) -> str:
    """Run a tagged statement, then read the resource group Trino placed it in."""
    tag = f"rg_{uuid.uuid4().hex[:12]}"
    cur = _connect(user, source).cursor()
    cur.execute(f"SELECT '{tag}'")
    cur.fetchall()
    probe = _connect("itest", None).cursor()
    probe.execute(
        "SELECT resource_group_id FROM system.runtime.queries "
        f"WHERE query LIKE '%{tag}%' AND query NOT LIKE '%system.runtime.queries%'"
    )
    rows = probe.fetchall()
    assert len(rows) == 1, rows
    return ".".join(rows[0][0])


@pytest.mark.parametrize("org", ["acme", "beta"])
def test_an_orgs_statement_runs_in_that_orgs_group(org):
    assert _group_of_a_statement("provisa", f"provisa/{org}") == f"global.tenant-{org}"


def test_the_engine_connection_lands_in_its_orgs_group():
    from provisa.federation.trino_lifecycle import ENGINE_USER, engine_source

    assert _group_of_a_statement(ENGINE_USER, engine_source("acme")) == "global.tenant-acme"


def test_an_engine_statement_naming_no_org_is_rejected():
    cur = _connect("provisa", "trino-python-client").cursor()
    with pytest.raises(trino.exceptions.TrinoQueryError) as rejected:
        cur.execute("SELECT 1")
        cur.fetchall()
    assert "resource group" in str(rejected.value).lower() or "QUERY_REJECTED" in str(
        rejected.value
    )


def test_another_user_runs_in_the_adhoc_group():
    assert _group_of_a_statement("itest", None) == "global.adhoc"


def test_the_arrow_flight_path_runs_in_its_one_shared_group():
    """The Arrow Flight proxy opens its own Trino connection and cannot carry the org; its
    statements share one group (REQ-056 records this gap). They must run, not be rejected."""
    from provisa.executor.trino_flight import create_flight_connection, execute_trino_flight_arrow

    tag = f"rg_{uuid.uuid4().hex[:12]}"
    conn = create_flight_connection(
        host="localhost", port=int(os.environ["ZAYCHIK_PORT"]), user="provisa"
    )
    try:
        assert execute_trino_flight_arrow(conn, f"SELECT '{tag}' AS t", None).num_rows == 1
    finally:
        conn.close()
    probe = _connect("itest", None).cursor()
    probe.execute(
        "SELECT resource_group_id, source FROM system.runtime.queries "
        f"WHERE query LIKE '%{tag}%' AND query NOT LIKE '%system.runtime.queries%'"
    )
    rows = probe.fetchall()
    assert len(rows) == 1, rows
    assert ".".join(rows[0][0]) == "global.flight", rows
