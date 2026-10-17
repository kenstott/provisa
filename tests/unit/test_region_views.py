# Copyright (c) 2026 Kenneth Stott
# Canary: bd9a7354-9a02-4135-8fbf-d0a210362838
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""REQ-1921, A VIEW MAY NAME A REGION: a reader connected through another region is served from
the copy the view's region keeps, read in place, or refused naming that region."""

from __future__ import annotations

from datetime import UTC, datetime
from types import SimpleNamespace

import pytest
from sqlalchemy import insert

from provisa.core.region_stores import ForeignRegion, HomeRegionUnavailable
from provisa.core.schema_org import mv_build_state
from provisa.mv.models import MVDefinition
from provisa.mv.registry import MVRegistry

_REGIONS = {
    "regions": [
        {"id": "eu", "address": "https://eu.example.com"},
        {"id": "us", "address": "https://us.example.com"},
    ]
}


@pytest.fixture
def node_in_us():
    from provisa.core import process_region

    was = process_region._region
    process_region.bind_launch(_REGIONS, requested="us")
    yield
    process_region._region = was


class _Backend:
    """Records what it is asked to attach; reads another region's store under a name it gives."""

    def __init__(self, unreachable: bool = False):
        self.attached: list[tuple] = []
        self.unreachable = unreachable

    def region_read_address(self, state, region, schema, table):
        return (f"{region.id}:{region.reads}", schema, table)

    def attach_region_read(self, state, region, schema, table, build):
        from provisa.federation.backend import RegionStoreUnreachable

        if self.unreachable:
            raise RegionStoreUnreachable("connection refused")
        self.attached.append((region.id, region.reads, region.store_url(), schema, table, build))


@pytest.fixture
def eu_state_db(control_plane):
    return control_plane.org()


def _state(eu_state_db, backend, monkeypatch, *, region="eu"):
    from provisa.federation import registry_view

    registry = MVRegistry()
    registry.register(
        MVDefinition(
            id="view-eu_sales",
            source_tables=[],
            target_catalog="mat_store",
            target_schema="org_acme_mv_cache",
            target_table="mv_eu_sales",
            sql="SELECT 1",
            region=region,
        )
    )

    async def _tables(_state):
        return [SimpleNamespace(id=42, source_id="__derived__", table_name="eu_sales")]

    monkeypatch.setattr(registry_view, "registered_tables", _tables)
    return SimpleNamespace(
        role_build_inputs={"draft_tables": []},  # REQ-1921: none out of service
        tables=[],
        org_id="acme",
        mv_registry=registry,
        federation_engine=SimpleNamespace(engine=SimpleNamespace(backend=backend)),
        foreign_regions={
            "eu": ForeignRegion(
                "eu",
                "postgresql://eu/replicas",
                eu_state_db,
                "pg",
                "postgresql://eu/views",
                "replicas",
            )
        },
    )


async def _built(db, at):
    async with db.acquire() as conn:
        await conn.execute_core(
            insert(mv_build_state).values(
                mv_id="view-eu_sales", region="eu", status="fresh", last_refresh_at=at
            )
        )


@pytest.mark.asyncio
async def test_a_view_another_region_keeps_is_attached_from_its_views_store_once_built(
    eu_state_db, monkeypatch, node_in_us
):
    from provisa.federation.query_residency import read_home_views

    backend = _Backend()
    state = _state(eu_state_db, backend, monkeypatch)
    built_at = datetime(2026, 10, 5, tzinfo=UTC)
    await _built(eu_state_db, built_at)
    assert await read_home_views(state, backend, [42]) == {"eu"}
    [(region, reads, url, schema, table, build)] = backend.attached
    assert (region, reads, url, table) == ("eu", "views", "postgresql://eu/views", "mv_eu_sales")
    assert schema.endswith("_rg_eu_mv_cache")  # where eu keeps its views
    assert build == built_at


@pytest.mark.asyncio
async def test_a_view_whose_copy_is_not_built_or_cannot_be_reached_is_refused_naming_its_region(
    eu_state_db, monkeypatch, node_in_us
):
    from provisa.federation.query_residency import read_home_views

    state = _state(eu_state_db, _Backend(), monkeypatch)
    with pytest.raises(
        HomeRegionUnavailable, match="its copy there, which is not built"
    ) as refused:
        await read_home_views(state, _Backend(), [42])
    assert refused.value.params == {"table": "eu_sales", "region": "eu"}
    await _built(eu_state_db, datetime(2026, 10, 5, tzinfo=UTC))
    with pytest.raises(HomeRegionUnavailable, match="cannot be reached"):
        await read_home_views(state, _Backend(unreachable=True), [42])


@pytest.mark.asyncio
async def test_a_view_this_region_builds_is_not_read_from_another(
    eu_state_db, monkeypatch, node_in_us
):
    from provisa.federation.query_residency import read_home_views

    backend = _Backend()
    state = _state(eu_state_db, backend, monkeypatch, region="us")
    assert await read_home_views(state, backend, [42]) == set()
    assert backend.attached == []


def test_every_reader_is_served_from_its_regions_copy_never_its_inputs(
    eu_state_db, monkeypatch, node_in_us
):
    """The copy holds what an administrator in its region may see; a reader here reads it with
    the view's own rules on it — not the view's SQL over inputs read here."""
    from provisa.compiler.stage2 import GovernanceContext
    from provisa.mv.view_read import semantic_statement, unnarrowed_view_bodies

    state = _state(eu_state_db, _Backend(), monkeypatch)
    view_sql_map = {"eu_sales": "SELECT * FROM sales.orders WHERE home = 'eu'"}
    home = 'SELECT * FROM "eu:views"."org_default_rg_eu_mv_cache"."mv_eu_sales"'
    # Governance keeps the reference (the view's own rules apply to it); optimization reads the copy.
    governed, _ = semantic_statement(
        "SELECT * FROM eu_sales", view_sql_map, state, GovernanceContext()
    )
    assert governed == "SELECT * FROM eu_sales"
    assert unnarrowed_view_bodies("SELECT * FROM eu_sales", view_sql_map, state) == {
        "eu_sales": home
    }


def test_a_readers_rules_are_not_held_to_the_inputs_of_another_regions_view(
    eu_state_db, monkeypatch, node_in_us
):
    """The relationship guard and the column checks judge the statement a reader sends; a view
    another region keeps is read from its copy there, so the tables and joins of its SQL are not
    the reader's. Expanded into the statement those checks judge, a reader who may read the view
    but not its inputs -- an insider reviewer reading EU mail scored in the EU -- was refused the
    view's own join and every input column it reads."""
    from provisa.compiler.stage2 import GovernanceContext
    from provisa.mv.view_read import semantic_statement

    state = _state(eu_state_db, _Backend(), monkeypatch)
    view_sql_map = {
        "eu_sales": "SELECT o.id FROM sales.orders AS o JOIN sales.staff AS s ON s.id = o.staff_id"
    }
    governed, validated = semantic_statement(
        "SELECT * FROM eu_sales", view_sql_map, state, GovernanceContext()
    )
    assert governed == validated == "SELECT * FROM eu_sales"


@pytest.mark.asyncio
async def test_a_read_of_only_that_view_names_its_region_as_the_one_that_answered(
    eu_state_db, monkeypatch, node_in_us
):
    from provisa.federation.query_residency import ensure_resident

    backend = _Backend()
    state = _state(eu_state_db, backend, monkeypatch)
    await _built(eu_state_db, datetime(2026, 10, 5, tzinfo=UTC))
    residency = await ensure_resident(state, set(), reader_role="analyst", table_ids=[42])
    assert residency.answered_in == "eu"


def test_a_regions_views_store_is_read_under_a_name_of_its_own():
    from provisa.federation.replica_address import region_read_name

    region = ForeignRegion("eu", "postgresql://r", None, "pg", "postgresql://v", "replicas")  # type: ignore[arg-type]
    state = SimpleNamespace(org_id="acme", active_org_id="acme")
    replicas, views = region_read_name(state, region), region_read_name(state, region.views())
    assert views == f"{replicas}__views"


def test_a_statement_may_name_the_other_regions_stores_as_this_engine_reads_them(
    eu_state_db, monkeypatch, node_in_us
):
    """The pipeline refuses a statement naming an unknown catalog; the stores of the org's other
    regions — replicas and views, as this engine names them — are known ones."""
    from provisa.federation.query_residency import other_region_read_catalogs

    state = _state(eu_state_db, _Backend(), monkeypatch)
    assert other_region_read_catalogs(state) == {"eu:replicas", "eu:views"}
