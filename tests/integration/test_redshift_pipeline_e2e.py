# Copyright (c) 2026 Kenneth Stott
# Canary: 3f8b5c21-7a04-4e69-b2d1-9c60ae83f715
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""True E2E for Redshift as a connector-source: registration through to a served query.

``test_redshift_source_e2e.py`` proves Trino can reach Redshift over a raw ``trino.dbapi`` cursor
against a three-part physical ref. The settled pipeline rejects that shape outright, so that test
says nothing about whether Provisa can serve Redshift. This closes the gap — see
``connector_source_harness`` for the two steps in between.

Why the source id equals the database name
------------------------------------------
Redshift's JDBC URL carries the database (``models.py:275`` → ``jdbc:redshift://host:port/<db>``),
so ``database`` cannot be left empty the way the other connector pipeline tests leave it. But
``createSource`` records ``state.source_catalogs[id]`` from ``input.database or
source_to_catalog(input.id)`` (``schema_mutation.py:378-381``) while the catalog is physically
created from ``_to_catalog_name(source.id)`` alone (``catalog.py:116``). With a non-empty database
those two agree only when the database name *is* the source id, so the source id is taken from
``REDSHIFT_DATABASE``. Any other id registers cleanly and then fails every query at the
unknown-catalog gate.

Credential-gated, not docker-gated: Redshift is AWS-only and billable. The ``redshift_cluster``
fixture provisions an ephemeral Serverless workgroup at first test setup — after the compose stack
is healthy, so the cluster does not stand idle through the stack bring-up — and deletes it when the
session ends.
"""

from __future__ import annotations

import os

import pytest

from tests.integration.connector_source_harness import (
    assert_registration_and_query,
    connector_client,  # noqa: F401 — imported for pytest fixture discovery
)
from tests.integration.redshift_cluster import (
    have_aws_creds,
    redshift_cluster,  # noqa: F401 — imported for pytest fixture discovery
)
from tests.integration.test_redshift_source_e2e import (
    _SCHEMA,
    _TABLE,
    _WIDGETS,
    _seed_redshift,
)

pytestmark = [
    pytest.mark.e2e,
    pytest.mark.requires_redshift,
    pytest.mark.requires_warehouse,
    pytest.mark.asyncio(loop_scope="session"),
    pytest.mark.skipif(
        not have_aws_creds(),
        reason=(
            "No AWS credentials for the ephemeral Redshift lane (AWS-only, billable, not "
            "self-provisionable on this host); set REDSHIFT_AWS_ACCESS_KEY_ID / "
            "REDSHIFT_AWS_SECRET_ACCESS_KEY in .env"
        ),
    ),
]

_DOMAIN = "redshift_e2e"


async def test_redshift_registers_and_serves_semantic_query(  # noqa: F811
    redshift_cluster, connector_client
):
    """createSource → registerTable (types introspected) → rebuildSchemas → semantic SELECT.

    ``host``/``port`` are the real AWS endpoint rather than a compose service name — unlike every
    other source in this bucket, Redshift is not on the stack's private network, and the isolated
    Trino container reaches it out through the same NAT address the security-group rule was opened
    for.

    ``id`` is ``INTEGER`` and ``name`` ``VARCHAR(64)`` on the Redshift side; Redshift folds
    unquoted identifiers to lower case, so the registered column names are lowercase and the served
    ``id`` arrives as a JSON number that compares equal to the integers in ``_WIDGETS``.
    """
    _seed_redshift()

    await assert_registration_and_query(
        connector_client,
        source_id=os.environ["REDSHIFT_DATABASE"],
        source_type="redshift",
        host=os.environ["REDSHIFT_HOST"],
        port=int(os.environ["REDSHIFT_PORT"]),
        database=os.environ["REDSHIFT_DATABASE"],
        username=os.environ["REDSHIFT_USER"],
        password=os.environ["REDSHIFT_PASSWORD"],
        domain_id=_DOMAIN,
        schema_name=_SCHEMA,
        table_name=_TABLE,
        alias="widgets",
        columns=["id", "name"],
        order_by="id",
        expected_rows=[{"id": wid, "name": name} for wid, name in _WIDGETS],
    )
