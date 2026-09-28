# Copyright (c) 2026 Kenneth Stott
# Canary: 9a7bc542-456d-467b-977f-451986f449a0
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1868: PgDuckdbMotherDuckConnector — live ATTACH_RW into a MotherDuck-hosted
DuckDB database via pg_duckdb's own FOREIGN DATA WRAPPER (TYPE 'motherduck').

Pure logic — the async ``probe(fetch)`` is driven by a fake fetch callable; no
live Postgres. The gate specific to MotherDuck is the FDW registration check
(pg_foreign_data_wrapper), distinct from the scan-connector probes that check
for a scanner function.
"""

from __future__ import annotations

import pytest

from provisa.core.models import Source, SourceType
from provisa.federation.connector import Mechanism
from provisa.federation.connector_duckdb import PgDuckdbMotherDuckConnector


def _src(sid: str, **kw) -> Source:
    fields = {"password": "md_token_abc", **kw}
    return Source(id=sid, type=SourceType.motherduck, **fields)


class _FakeFetch:
    """Async fetch keyed on distinctive substrings of the probe SQL."""

    def __init__(self, *, preloaded: bool, fdw_registered: bool):
        self._preloaded = preloaded
        self._fdw_registered = fdw_registered

    async def __call__(self, sql: str):
        if "shared_preload_libraries" in sql:
            return [{"v": "pg_duckdb" if self._preloaded else ""}]
        if "pg_foreign_data_wrapper" in sql:
            return [{"one": 1}] if self._fdw_registered else []
        return []


# ---- connector identity / mechanism (REQ-1868) ------------------------------


def test_motherduck_connector_identity():
    c = PgDuckdbMotherDuckConnector()
    assert c.engine == "postgres"
    assert c.source_type == "motherduck"
    assert c.key == "pg_duckdb_motherduck"
    assert c.mechanism is Mechanism.ATTACH_RW  # live in place, read + write
    assert c.reads_in_place is True


def test_motherduck_capability_full_pushdown_and_writable():
    cap = PgDuckdbMotherDuckConnector().capability()
    assert cap.predicate_pushdown is True
    assert cap.join_pushdown is True
    assert cap.aggregate_pushdown is True
    assert cap.write is True


# ---- attach DDL: CREATE SERVER + USER MAPPING (REQ-1868) --------------------


def test_attach_ddl_provisions_server_and_user_mapping_with_token():
    details = PgDuckdbMotherDuckConnector().details(_src("mdsrc", password="secrettoken"))
    ddl = details["attach_ddl"]
    assert ddl[0] == (
        "CREATE SERVER IF NOT EXISTS fdw_mdsrc TYPE 'motherduck' FOREIGN DATA WRAPPER duckdb"
    )
    assert ddl[1] == (
        "CREATE USER MAPPING IF NOT EXISTS FOR CURRENT_USER SERVER fdw_mdsrc "
        "OPTIONS (token 'secrettoken')"
    )
    assert details["server"] == "fdw_mdsrc"


def test_attach_ddl_includes_options_clause_when_federation_hints_set():
    details = PgDuckdbMotherDuckConnector().details(
        _src(
            "mdsrc2",
            federation_hints={
                "default_database": "analytics",
                "tables_owner_role": "app_role",
                "background_catalog_refresh_inactivity_timeout": "10m",
            },
        )
    )
    server_ddl = details["attach_ddl"][0]
    assert "OPTIONS (" in server_ddl
    assert "default_database 'analytics'" in server_ddl
    assert "tables_owner_role 'app_role'" in server_ddl
    assert "background_catalog_refresh_inactivity_timeout '10m'" in server_ddl


def test_attach_ddl_omits_options_clause_when_no_hints_set():
    details = PgDuckdbMotherDuckConnector().details(_src("mdsrc3"))
    server_ddl = details["attach_ddl"][0]
    assert "OPTIONS" not in server_ddl


def test_details_raises_when_token_missing():
    with pytest.raises(ValueError, match="password.*MotherDuck token"):
        PgDuckdbMotherDuckConnector().details(_src("mdsrc4", password=""))


# ---- functional-truth probe (REQ-904/1868) ----------------------------------


@pytest.mark.asyncio
async def test_probe_available_when_preloaded_and_fdw_registered():
    r = await PgDuckdbMotherDuckConnector().probe(_FakeFetch(preloaded=True, fdw_registered=True))
    assert r.available is True


@pytest.mark.asyncio
async def test_probe_unavailable_when_not_preloaded():
    r = await PgDuckdbMotherDuckConnector().probe(_FakeFetch(preloaded=False, fdw_registered=True))
    assert r.available is False
    assert "shared_preload_libraries" in r.reason


@pytest.mark.asyncio
async def test_probe_unavailable_when_fdw_not_registered():
    r = await PgDuckdbMotherDuckConnector().probe(_FakeFetch(preloaded=True, fdw_registered=False))
    assert r.available is False
    assert "foreign data wrapper" in r.reason
    assert r.remediation and "CREATE EXTENSION" in r.remediation


if __name__ == "__main__":
    pytest.main([__file__, "-q"])
