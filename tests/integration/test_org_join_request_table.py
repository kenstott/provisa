# Copyright (c) 2026 Kenneth Stott
# Canary: 8c47e1d2-96fa-4b3e-a05c-2d1f7b9e6c43
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1250: org_join_requests and orgs.open_to_requests on a real PostgreSQL control plane.

The registry is created by ``metadata.create_all`` and reconciled by ``add_missing_columns`` (V1
ships no migrations), so both are exercised: a fresh registry has the table, its one-open-request
index and the invite_only default, and a registry created before the column gets it back with
that default.
"""

# Requirements: REQ-1250

from __future__ import annotations

import datetime
import os
from datetime import timezone

import pytest
from sqlalchemy import create_engine, insert, select, text
from sqlalchemy.exc import IntegrityError

from provisa.core.db import add_missing_columns
from provisa.core.schema_admin import REGISTRY_TABLES, org_join_requests, orgs
from provisa.core.schema_admin import metadata as admin_metadata

pytestmark = [pytest.mark.integration]

_PG_HOST = os.environ.get("PG_HOST", "localhost")
_PG_PORT = os.environ.get("PG_PORT", "5432")
_URL = f"postgresql+psycopg://provisa:provisa@{_PG_HOST}:{_PG_PORT}/provisa"
_SCHEMA = "test_req1250_registry"
_T0 = datetime.datetime(2026, 11, 20, tzinfo=timezone.utc)


@pytest.fixture
def registry():
    engine = create_engine(_URL, pool_pre_ping=True)
    with engine.begin() as conn:
        conn.execute(text(f"DROP SCHEMA IF EXISTS {_SCHEMA} CASCADE"))
        conn.execute(text(f"CREATE SCHEMA {_SCHEMA}"))
        conn.execute(text(f"SET search_path TO {_SCHEMA}"))
        admin_metadata.create_all(conn, tables=REGISTRY_TABLES)
    yield engine
    with engine.begin() as conn:
        conn.execute(text(f"DROP SCHEMA IF EXISTS {_SCHEMA} CASCADE"))
    engine.dispose()


def _request(request_id: str, status: str) -> dict:
    return {
        "id": request_id,
        "user_id": "fb-asker",
        "org_id": "open",
        "status": status,
        "requested_at": _T0,
        "expires_at": _T0 + datetime.timedelta(days=7),
    }


def test_the_table_has_the_requirement_columns(registry):
    with registry.connect() as conn:
        columns = {
            r[0]
            for r in conn.execute(
                text(
                    "SELECT column_name FROM information_schema.columns "
                    "WHERE table_schema = :s AND table_name = 'org_join_requests'"
                ),
                {"s": _SCHEMA},
            )
        }
    assert {
        "user_id",
        "org_id",
        "status",
        "requested_at",
        "decided_by",
        "decided_at",
        "expires_at",
    } <= columns


def test_an_org_is_invite_only_by_default(registry):
    with registry.begin() as conn:
        conn.execute(text(f"SET search_path TO {_SCHEMA}"))
        conn.execute(insert(orgs).values(id="quiet", name="Quiet"))
        flag = conn.execute(select(orgs.c.open_to_requests).where(orgs.c.id == "quiet")).scalar()
    assert flag is False


def test_one_open_request_per_user_and_org(registry):
    with registry.begin() as conn:
        conn.execute(text(f"SET search_path TO {_SCHEMA}"))
        conn.execute(insert(org_join_requests).values(**_request("a", "pending")))
        # Closed requests for the same pair are not limited.
        conn.execute(insert(org_join_requests).values(**_request("b", "rejected")))
        conn.execute(insert(org_join_requests).values(**_request("c", "expired")))
    with pytest.raises(IntegrityError, match="uq_org_join_requests_open"):
        with registry.begin() as conn:
            conn.execute(text(f"SET search_path TO {_SCHEMA}"))
            conn.execute(insert(org_join_requests).values(**_request("d", "pending")))


def test_an_unknown_status_is_refused(registry):
    with pytest.raises(IntegrityError, match="ck_org_join_requests_status"):
        with registry.begin() as conn:
            conn.execute(text(f"SET search_path TO {_SCHEMA}"))
            conn.execute(insert(org_join_requests).values(**_request("x", "maybe")))


def test_a_registry_from_before_the_column_gains_it(registry):
    with registry.begin() as conn:
        conn.execute(text(f"SET search_path TO {_SCHEMA}"))
        conn.execute(insert(orgs).values(id="older", name="Older"))
        conn.execute(text("ALTER TABLE orgs DROP COLUMN open_to_requests"))
        add_missing_columns(conn, REGISTRY_TABLES, _SCHEMA)
        flag = conn.execute(select(orgs.c.open_to_requests).where(orgs.c.id == "older")).scalar()
    assert flag is False
