# Copyright (c) 2026 Kenneth Stott
# Canary: a931d960-eaa1-4c8c-80fa-16e4e5f0fa2a
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A column is pii through its glossary term, on a real server (REQ-2143, REQ-1942, REQ-1943).

The configuration marks the term "home address" pii, not any column. Both columns that mean it
are then sensitive: the one declaring a fake is covered, and the one declaring none keeps a Test
(fake) environment from being made, named, because it would show real values."""

# Requirements: REQ-2143

from __future__ import annotations

import json
import os
import urllib.error
import urllib.request
from typing import Any

import pytest
import sqlalchemy as sa

from tests.integration.worker_boot_harness import WorkerBoot

pytestmark = [pytest.mark.integration]

_ROLES = ["org_admin"]


def _col(name: str, data_type: str, **extra) -> dict:
    return {"name": name, "data_type": data_type, "visible_to": _ROLES, **extra}


def _table(name: str, *columns: dict) -> dict:
    return {
        "source_id": "sales-pg",
        "domain_id": "sales",
        "schema": "public",
        "table": name,
        "columns": list(columns),
    }


@pytest.fixture(scope="module")
def boot():
    b = WorkerBoot(
        1,
        pg_host=os.environ.get("PG_HOST", "localhost"),
        pg_port=int(os.environ.get("PG_PORT", "5432")),
        env={"PROVISA_REDIRECT_ENABLED": "false"},
    )
    b._extra_config = {
        "tables": [
            _table(
                "customers",
                _col("id", "integer", is_primary_key=True),
                _col("home_address", "varchar", fake="street_address()"),
            ),
            _table(
                "accounts",
                _col("id", "integer", is_primary_key=True),
                _col("home_address", "varchar"),
            ),
        ],
        "glossary_terms": [
            {"name": "home address", "definition": "Where a person lives", "domains": ["sales"]}
        ],
        "tag_assignments": [
            {
                "tag_id": "pii",
                "object_type": "term",
                "term": "home address",
                "reason": "an address identifies a person",
            }
        ],
    }
    b.create_database()
    try:
        engine = sa.create_engine(b.url, isolation_level="AUTOCOMMIT")
        with engine.connect() as conn:
            for name in ("customers", "accounts"):
                conn.execute(
                    sa.text(
                        f"CREATE TABLE public.{name} (id integer PRIMARY KEY, home_address text)"
                    )
                )
                conn.execute(sa.text(f"INSERT INTO public.{name} VALUES (1, '1 Real Street')"))
        engine.dispose()
        b.start()
        b.wait_all_ready(timeout=300)
        yield b
    finally:
        b.cleanup()


def _call(b, method: str, path: str, body: Any = None) -> tuple[int, Any]:
    req = urllib.request.Request(
        f"http://127.0.0.1:{b.ports['http']}{path}",
        data=None if body is None else json.dumps(body).encode(),
        headers={"Content-Type": "application/json", "x-provisa-role": "org_admin"},
        method=method,
    )
    try:
        with urllib.request.urlopen(req, timeout=300) as resp:
            return resp.status, json.loads(resp.read().decode())
    except urllib.error.HTTPError as exc:
        return exc.code, {"error": exc.read().decode()}


def test_both_columns_meaning_the_term_are_sensitive_and_one_has_no_fake(boot):
    status, body = _call(
        boot,
        "POST",
        f"/admin/orgs/{boot.org_id}/environments",
        {"name": "detailed", "data_mode": "unbound"},
    )
    assert status == 200, body
    status, detail = _call(boot, "GET", f"/admin/orgs/{boot.org_id}/environments/detailed/detail")
    assert status == 200, detail
    assert detail["test_data"]["sensitive_without_fake"] == ["accounts.home_address"]


def test_test_fake_is_refused_naming_the_column_pii_through_its_term(boot):
    status, body = _call(
        boot,
        "POST",
        f"/admin/orgs/{boot.org_id}/environments",
        {"name": "faked", "data_mode": "test_fake"},
    )
    assert status == 422, body
    assert "accounts.home_address" in body["error"], body
    assert "customers.home_address" not in body["error"], body
