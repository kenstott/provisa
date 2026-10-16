# Copyright (c) 2026 Kenneth Stott
# Canary: 542e3dcc-2bb1-48af-b08d-688e0eb612b2
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Live ServiceNow source (REQ-1954): listed and read through its pgwire server.

ServiceNow has no direct driver of its own. A ``Source`` row (type=servicenow) carries:

  - ``base_url`` (or ``host``) -> the instance's address
  - ``username`` / ``password`` -> the basic-auth credential

``provisa.federation.pgwire_replica`` writes the server's model from those fields and the tables
registered from the source, and starts the bundled ``pgwire-servicenow`` server. The server
serves the instance's table list and one table's columns at a time; a table is served as a
table once it is registered.

Credentials
-----------
There is no ServiceNow emulator: a real instance is required. This reads
SERVICENOW_INSTANCE_URL, SERVICENOW_USERNAME and SERVICENOW_PASSWORD from the lane's
environment. It only reads.
"""

from __future__ import annotations

import os

import pytest

from tests.conftest import as_deployment_org, deployment_org_on_this_node

pytestmark = [
    pytest.mark.integration,
    pytest.mark.requires_servicenow,
    pytest.mark.requires_warehouse,
]

_aio = pytest.mark.asyncio(loop_scope="session")

_SOURCE_ID = "servicenow-itest"
_SCHEMA = "servicenow_itest"
# The server's start and its first catalog query, which counts the rows of every table served.
_COLD_START_SECONDS = 600


def _source():
    from provisa.core.models import Source, SourceType

    return Source(
        id=_SOURCE_ID,
        type=SourceType.servicenow,
        base_url=os.environ["SERVICENOW_INSTANCE_URL"],
        username=os.environ["SERVICENOW_USERNAME"],
        password=os.environ["SERVICENOW_PASSWORD"],
    )


@pytest.fixture(scope="module")
def endpoint():
    """The source's pgwire server, with ``incident`` registered from it."""
    from provisa.federation import pgwire_replica as pr
    from provisa.federation import served_tables

    served_tables.serve_tables(_SOURCE_ID, ["incident"])
    try:
        # A module fixture runs before the per-test org binding: the endpoint is the
        # deployment org's, its folder ready, as its runtime's boot starts it (REQ-1266, REQ-1992).
        with deployment_org_on_this_node():
            served = pr.ensure_endpoint(_source(), timeout=_COLD_START_SECONDS)
        yield served
    finally:
        with as_deployment_org():
            pr.stop_endpoint(_SOURCE_ID)
        served_tables.serve_tables(_SOURCE_ID, [])


@_aio
async def test_every_table_is_listed_with_its_label_under_its_theme(endpoint):
    from provisa.federation import pgwire_replica as pr

    del endpoint
    listed = await pr.servicenow_table_list(_source(), _SCHEMA)
    by_name = {name: (label, theme) for name, label, theme in listed}
    assert len(listed) > 1000  # the whole instance, not the registered table alone
    assert by_name["incident"][0] == "Incident"
    assert by_name["change_request"][1]  # every table has a theme, "(no package)" at worst


@_aio
async def test_a_selected_tables_columns_are_read_for_it_alone(endpoint):
    from provisa.federation import pgwire_replica as pr

    del endpoint
    columns = dict(await pr.servicenow_columns(_source(), _SCHEMA, "incident"))
    assert "sys_id" in columns and "number" in columns
    # A reference field is two columns: the referenced record's sys_id and its display value.
    assert "caller_id" in columns and "caller_id__display" in columns


@_aio
async def test_a_registered_table_is_read_through_a_filter_and_a_limit(endpoint):
    from provisa.federation import pgwire_replica as pr

    conn = await pr._pg_connect(endpoint.pgwire_host, endpoint.pgwire_port)
    try:
        rows = await conn.fetch(
            f'SELECT "sys_id", "number", "active" FROM "{_SCHEMA}"."incident" '
            'WHERE "active" = TRUE LIMIT 5'
        )
    finally:
        await conn.close()
    assert 0 < len(rows) <= 5
    assert all(r["sys_id"] and r["number"].startswith("INC") and r["active"] for r in rows)
