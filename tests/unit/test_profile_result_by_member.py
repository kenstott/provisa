# Copyright (c) 2026 Kenneth Stott
# Canary: c5bee0f8-d805-48b3-a335-d375b7d509c1
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-2052: a configuration names a profile result table by its member and kind.

Its name carries the member's id, <member>_<id>_profile_<kind>, which a configuration cannot
know: the ids a seed hands out depend on what was registered before it. The seed resolves it."""

# Requirements: REQ-2052

from __future__ import annotations

import contextlib

import pytest
from sqlalchemy import insert, select

from provisa.core import secrets_store
from provisa.core.config_loader import apply_config, parse_config_dict
from provisa.core.schema_org import domains, registered_tables, sources

_VIS = ["org_admin"]


def _config() -> dict:
    return {
        "sources": [
            {"id": "crm", "type": "postgresql", "host": "h", "port": 5432, "database": "d",
             "username": "u", "password": "p"},
            {"id": "prof", "type": "data_profiler", "mapping": {
                "cron": "0 2 * * *", "low_cardinality_max": 100, "drift_window": 7,
                "drift_season": "none", "drift_distance": 3.0, "drift_slope": 3.0,
                "drift_ks": 0.2, "drift_psi": 0.25, "correlation_max_columns": 20,
                "joint_max_distinct": 20, "category_max_columns": 10}},
        ],
        "domains": [{"id": "harbor"}],
        "roles": [],
        "tables": [
            {"source_id": "prof", "domain_id": "harbor", "profile_of": "customers",
             "kind": "columns", "alias": "customer_profile",
             "columns": [{"name": "run_id", "visible_to": _VIS},
                         {"name": "run_time", "visible_to": _VIS},
                         {"name": "column_name", "visible_to": _VIS},
                         {"name": "null_count", "visible_to": _VIS}]},
            {"source_id": "crm", "domain_id": "harbor", "schema": "public", "table": "customers",
             "profiler_source_id": "prof",
             "columns": [{"name": "id", "data_type": "integer", "is_primary_key": True,
                          "visible_to": _VIS}]},
        ],
    }  # fmt: skip


@pytest.fixture
def unbound(monkeypatch):
    @contextlib.asynccontextmanager
    async def _unbound():
        yield

    monkeypatch.setattr(secrets_store, "bound_to_request_org", _unbound)


async def _names(conn) -> dict[str, int]:
    rows = (
        await conn.execute_core(select(registered_tables.c.table_name, registered_tables.c.id))
    ).fetchall()
    return {r.table_name: r.id for r in rows}


@pytest.mark.parametrize("registered_before", [0, 7])
async def test_the_result_is_named_by_its_member_s_id_whatever_id_the_seed_gives(
    control_plane, unbound, registered_before
):
    db = control_plane.org()
    async with db.acquire() as conn:
        await conn.execute_core(insert(domains).values(id="other"))
        await conn.execute_core(insert(sources).values(id="elsewhere", type="postgresql"))
        for n in range(registered_before):  # what another release seeds before the demo's tables
            await conn.execute_core(
                insert(registered_tables).values(
                    source_id="elsewhere", domain_id="other", schema_name="s", table_name=f"t{n}"
                )
            )
        await apply_config(parse_config_dict(_config()), conn)
        names = await _names(conn)
        member = names["customers"]
        assert f"customers_{member}_profile_columns" in names
        assert not any(
            n.endswith("_profile_columns") and n != f"customers_{member}_profile_columns"
            for n in names
        )

        # Applied again, the member stored: resolved before anything is written, no second table.
        await apply_config(parse_config_dict(_config()), conn)
        assert await _names(conn) == names


def test_a_result_named_by_its_member_names_no_table_and_a_known_kind():
    from provisa.core.models import Table

    base = {"source_id": "prof", "domain_id": "harbor", "profile_of": "customers", "columns": []}
    with pytest.raises(ValueError, match="names no kind"):
        Table.model_validate(base)
    with pytest.raises(ValueError, match="names no table"):
        Table.model_validate({**base, "kind": "columns", "table": "customers_3_profile_columns"})
    config = _config()
    config["tables"][0]["kind"] = "colums"
    with pytest.raises(ValueError, match="unknown profile result kind 'colums'"):
        from provisa.profiler.registration import validate_config

        validate_config(parse_config_dict(config))
