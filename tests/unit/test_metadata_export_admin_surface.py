# Copyright (c) 2026 Kenneth Stott
# Canary: 9d2f7b81-4c56-4a03-b7e9-1f5a08c36d24
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1074: the admin surface that configures and operates metadata export.

The four things worth pinning here are the ones a wrong implementation gets wrong quietly:
credentials must never travel back out on the read, an edit that omits a credential must not
erase it, an org below the REQ-1073 tier must not reach the writer, and a target that refuses
the connection must be reported with the text it refused with rather than as a bare failure.
"""

# Requirements: REQ-1068, REQ-1072, REQ-1073, REQ-1074

from __future__ import annotations

import types


import pytest

from provisa.api.errors import ApiError
from provisa.core.models import ProvisaConfig
from tests.role_state import RoleState

ORG_ID = "acme"


def _request(org_id: str = ORG_ID):
    return types.SimpleNamespace(
        state=types.SimpleNamespace(
            identity=types.SimpleNamespace(user_id="alice", roles=[]), active_org_id=org_id
        )
    )


@pytest.fixture(autouse=True)
def _served(monkeypatch):
    """Every column is served here; what a target may be published as is the role tests below."""
    from tests.unit.gate_identity import serve_every_column

    serve_every_column(monkeypatch)


def _json_body(body: dict):
    async def _json():
        return body

    return _json


class _TenantDb:
    """The control plane's org database, holding the catalog_bindings table (REQ-1389).

    The publish path loads and persists vendor bindings around every publish, so the state
    stub has to hand it a working connection rather than a bare object.
    """

    def __init__(self, control_plane):
        self._db = control_plane.org()

    def acquire(self):
        return self._db.acquire()


@pytest.fixture
def surface(monkeypatch, control_plane):
    """Drive the handlers against recorded org settings and a registered premium org.

    Returns a namespace with ``stored`` (what the org has saved — mutate it to set up a case),
    ``written`` (what a handler persisted), and ``org`` (call it to re-register at a tier).
    """
    import provisa.api.admin.capabilities as capmod
    import provisa.core.org_settings as org_settings_mod

    monkeypatch.setattr(capmod, "_resolved_capabilities", lambda identity, state: {"org_settings"})

    stored: dict = {}
    written: dict = {}
    # REQ-2122: the org's catalogs, which each target names.
    catalogs: list = [
        {"id": "mz", "provider": "openlineage", "endpoint": "http://mz", "domains": ["sales"]},
        {"id": "dh", "provider": "datahub", "endpoint": "http://b", "domains": ["sales"]},
    ]

    async def _resolve(_db):
        return {"catalogs": list(catalogs), "metadata_export": dict(stored)}

    async def _write(_db, updates, *, updated_by):
        written.clear()
        written.update(updates)
        written["_updated_by"] = updated_by
        return list(updates)

    monkeypatch.setattr(org_settings_mod, "resolve_org_config", _resolve)
    monkeypatch.setattr(org_settings_mod, "write_org_overrides", _write)
    monkeypatch.setattr(
        "provisa.api.app.state",
        RoleState(
            model_db=(_one_db := _TenantDb(control_plane)),
            tenant_db=_one_db,
            # REQ-1659: the listing links back at the org's public origin, read off config.mail.
            config=types.SimpleNamespace(
                mail=types.SimpleNamespace(base_url="https://provisa.test")
            ),
            contexts={},
            multitenancy=False,
        ),
        raising=False,
    )

    # The tier stored with the org (REQ-1066), as provisa.core.org_tier reads it.
    tiers: dict[str, str] = {}

    async def _tier(org_id: str) -> str | None:
        return tiers.get(org_id)

    monkeypatch.setattr("provisa.core.org_tier.read_org_tier", _tier)
    # A commercial deployment, where the stored tier decides (REQ-1066).
    monkeypatch.setattr("provisa.core.commerce.enabled", lambda: True)

    def _org(tier: str = "premium", org_id: str = ORG_ID):
        tiers[org_id] = tier

    _org()
    return types.SimpleNamespace(stored=stored, written=written, org=_org, catalogs=catalogs)


async def _put(body: dict):
    from provisa.api.admin.metadata_export_router import set_metadata_export

    request = _request()
    request.json = _json_body(body)
    return await set_metadata_export(request)


# --- reading ---------------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_the_read_reports_each_target_by_its_catalog(surface):  # REQ-2122
    """A target names its catalog and carries the export's own settings, never a credential."""
    from provisa.api.admin.metadata_export_router import get_metadata_export

    surface.stored.update({"enabled": True, "targets": [{"catalog": "mz"}]})
    body = await get_metadata_export(_request())

    assert body["config"]["targets"] == [
        # REQ-2152: the role it publishes as, none until the export names one.
        {"catalog": "mz", "datazone_project_id": "", "delta_share": False, "role": None}
    ]


@pytest.mark.asyncio
async def test_the_read_carries_the_entitlement_flag_the_tab_gates_on(surface):
    from provisa.api.admin.metadata_export_router import get_metadata_export

    assert (await get_metadata_export(_request()))["entitled"] is True
    surface.org(tier="standard")
    body = await get_metadata_export(_request())
    assert body["entitled"] is False
    # The tab tells the admin what tier would open it, so the gate is explicable, not just shut.
    assert body["required_tier"] == "premium"


@pytest.mark.asyncio
async def test_the_read_lists_the_providers_the_registry_actually_has(surface):
    from provisa.api.admin.metadata_export_router import get_metadata_export

    providers = (await get_metadata_export(_request()))["providers"]
    assert {"openlineage", "openmetadata"} <= set(providers)


# --- writing ---------------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_a_target_left_out_of_the_list_is_removed(surface):  # REQ-2118
    surface.stored.update({"enabled": True, "targets": [{"catalog": "mz"}, {"catalog": "dh"}]})
    await _put({"targets": [{"catalog": "dh"}]})

    assert surface.written["metadata_export"]["targets"] == [{"catalog": "dh"}]


@pytest.mark.asyncio
async def test_a_target_naming_no_catalog_of_the_org_is_refused_at_the_write(surface):  # REQ-2122
    with pytest.raises(ApiError) as exc:
        await _put({"enabled": True, "targets": [{"catalog": "gone"}]})

    assert exc.value.status_code == 400
    assert "name no catalog of the org: gone" in str(exc.value.detail)
    assert surface.written == {}


@pytest.mark.asyncio
async def test_a_key_outside_the_export_settings_is_not_written(surface):
    # The body is an untrusted dict; only the enumerated settings may reach storage, or an admin
    # writes arbitrary config through the export door.
    await _put({"enabled": True, "tier": "x", "targets": [{"catalog": "mz", "tier": "x"}]})

    assert "tier" not in surface.written["metadata_export"]
    assert "tier" not in surface.written["metadata_export"]["targets"][0]


@pytest.mark.asyncio
async def test_an_enabled_export_with_no_target_is_refused_at_the_write(surface):
    with pytest.raises(ApiError) as exc:
        await _put({"enabled": True, "targets": []})

    assert exc.value.status_code == 400
    assert surface.written == {}


@pytest.mark.asyncio
async def test_the_write_is_attributed_to_the_caller(surface):
    await _put({"enabled": False})

    assert surface.written["_updated_by"] == "alice"


# --- the tier gate ---------------------------------------------------------------------------


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "handler_name", ["set_metadata_export", "check_metadata_export", "publish_metadata_export"]
)
async def test_every_operating_handler_refuses_an_unentitled_org(surface, handler_name):
    """REQ-1073: hiding the tab is not the gate — the endpoints are."""
    import provisa.api.admin.metadata_export_router as mod

    surface.org(tier="standard")
    surface.stored.update(
        {
            "enabled": True,
            "targets": [{"catalog": "mz"}],
        }
    )
    request = _request()
    request.json = _json_body({"enabled": True})

    with pytest.raises(ApiError) as exc:
        await getattr(mod, handler_name)(request)
    assert exc.value.status_code == 403


@pytest.mark.asyncio
async def test_an_org_the_control_plane_never_registered_is_refused(surface):
    request = _request(org_id="ghost")
    request.json = _json_body({"enabled": False})

    with pytest.raises(ApiError) as exc:
        await _unregistered(request)
    assert exc.value.status_code == 403


async def _unregistered(request):
    from provisa.api.admin.metadata_export_router import set_metadata_export

    return await set_metadata_export(request)


# --- health and publish ----------------------------------------------------------------------


@pytest.mark.asyncio
async def test_health_reports_the_refusal_text_rather_than_a_bare_failure(surface, monkeypatch):
    """A wrong URL and a rejected credential look identical without the message."""
    import provisa.api.admin.metadata_export_router as mod
    from provisa.api.admin.metadata_export_router import check_metadata_export

    surface.stored.update(
        {
            "enabled": True,
            "targets": [{"catalog": "mz"}],
        }
    )

    class _Failing:
        async def health(self):
            raise ConnectionError("Name or service not known")

    monkeypatch.setattr(mod, "metadata_export", lambda config: _Failing())
    body = await check_metadata_export(_request())

    assert body["ok"] is False
    (check,) = body["targets"]
    assert check["id"] == "mz"
    assert "Name or service not known" in check["error"]
    assert "ConnectionError" in check["error"]


@pytest.mark.asyncio
async def test_health_on_a_target_that_cannot_be_built_answers_instead_of_500ing(surface):
    """Pressing Test connection must say WHY, not fail with a bare 500 — adapter construction
    raises MetadataExportNotConfiguredError and that text is the answer."""
    from provisa.api.admin.metadata_export_router import check_metadata_export

    surface.catalogs.append(
        {"id": "x", "provider": "nope", "endpoint": "http://a", "domains": ["sales"]}
    )
    surface.stored.update({"enabled": False, "targets": [{"catalog": "x"}]})

    body = await check_metadata_export(_request())

    assert body["ok"] is False
    assert "Unknown metadata export provider 'nope'" in body["targets"][0]["error"]


@pytest.mark.asyncio
async def test_publish_returns_the_assets_the_target_rejected(surface, monkeypatch):
    """A partial publish is the case the tab exists to diagnose."""
    from provisa.api.admin.metadata_export_router import (
        get_metadata_export,
        publish_metadata_export,
    )
    from provisa.api.metadata_export.model import AssetRef, MetadataSnapshot
    from provisa.api.metadata_export.provider import AssetError, PublishResult

    surface.stored.update(
        {
            "enabled": True,
            "targets": [{"catalog": "mz", "role": "steward"}],
        }
    )
    result = PublishResult(
        provider_name="openlineage",
        published={"dataset": 2},
        errors=[
            AssetError(
                asset=AssetRef(kind="table", parts=("wh", "public.orders")),
                message="422 unknown field type",
            )
        ],
    )

    class _Partial:
        needs_export_views = False  # a remote catalog: it addresses nothing in the engine's store

        async def publish(self, snapshot):
            return result

    # The endpoint publishes through the REQ-1072 sync path, so the adapter and the builder are
    # stubbed where that path resolves them.
    import provisa.api.metadata_export.publishing as sync_mod

    monkeypatch.setattr(sync_mod, "metadata_export", lambda config: _Partial())
    monkeypatch.setattr(
        sync_mod,
        "build_snapshot",
        lambda config, *, org_id, dialect, contexts, glossary=None, dq_outcomes=None, uri_org_id=None, documentation_base_url=None, owner_emails=None: (
            MetadataSnapshot(org_id=ORG_ID)
        ),
    )

    async def _stub_model():
        # Empty rather than a bare object: the publish path reads each registered contract's
        # last scan off this config (REQ-1443), and an empty config registers none.
        return ProvisaConfig(sources=[], domains=[], tables=[], roles=[])

    # The model now assembles from the registration tables (DB truth, #97); stub the
    # assembly the same way the builder is stubbed — this test is about the publish report.
    monkeypatch.setattr(sync_mod, "_model_for_export", _stub_model)

    async def _reads_everything(state, role):
        return lambda table: None  # what a role is sent is test_metadata_export_scope

    monkeypatch.setattr(sync_mod, "role_reader", _reads_everything)
    monkeypatch.setattr(sync_mod, "role_reach", lambda state, role: lambda domain_id: True)
    body = await publish_metadata_export(_request())

    assert body["ok"] is False
    (target,) = body["targets"]
    assert (target["id"], target["total_published"]) == ("mz", 2)
    assert target["errors"] == [{"asset": "wh.public.orders", "message": "422 unknown field type"}]
    # The tab shows the last outcome without re-publishing to find out what it was.
    assert (await get_metadata_export(_request()))["last_publish"] == body


@pytest.mark.asyncio
async def test_publishing_a_disabled_target_is_refused_before_a_snapshot_is_built(surface):
    from provisa.api.admin.metadata_export_router import publish_metadata_export

    surface.stored.update(
        {
            "enabled": False,
            "targets": [{"catalog": "mz"}],
        }
    )

    with pytest.raises(ApiError) as exc:
        await publish_metadata_export(_request())
    assert exc.value.status_code == 400


@pytest.mark.asyncio
async def test_a_target_naming_no_role_is_sent_nothing_and_says_why(surface, monkeypatch):
    """REQ-2152 (Q1): a catalog is published as a role; a target naming none is refused by name
    and the other targets still publish."""
    from provisa.api.admin.metadata_export_router import publish_metadata_export
    from provisa.api.metadata_export.model import MetadataSnapshot

    import provisa.api.metadata_export.publishing as sync_mod

    surface.stored.update({"enabled": True, "targets": [{"catalog": "mz"}]})
    monkeypatch.setattr(
        sync_mod,
        "build_snapshot",
        lambda config, **kwargs: MetadataSnapshot(org_id=ORG_ID),
    )

    async def _stub_model():
        return ProvisaConfig(sources=[], domains=[], tables=[], roles=[])

    monkeypatch.setattr(sync_mod, "_model_for_export", _stub_model)
    monkeypatch.setattr(sync_mod, "metadata_export", lambda config: pytest.fail("published"))
    body = await publish_metadata_export(_request())
    (target,) = body["targets"]
    assert target["errors"] == [
        {
            "asset": "mz",
            "message": "catalog 'mz' names no role to publish as: set the role whose reads it is sent",
        }
    ]


# --- a target's role is within its saver's reads (REQ-2152, ruling Q8) ----------------------------


def _narrowed(monkeypatch):
    """A saver served orders.id alone; ``steward`` reads orders.id and orders.ssn, ``analyst``
    orders.id."""
    import provisa.api.app as appmod
    from provisa.api.admin.catalog_scope import CatalogScope

    scope = CatalogScope(
        whole=False,
        reach=None,
        served={1: {"id"}},
        governance=False,
        role="saver",
        compiled=frozenset({1}),
    )
    monkeypatch.setattr("provisa.api.admin.catalog_scope.catalog_scope", lambda info: scope)
    reads = {"steward": {1: {"id", "ssn"}}, "analyst": {1: {"id"}}}
    monkeypatch.setattr(
        "provisa.api.flight.catalog.role_visibility", lambda state, role: reads.get(role, {})
    )
    monkeypatch.setattr(appmod.state, "roles", {"steward": {}, "analyst": {}}, raising=False)


@pytest.mark.asyncio
async def test_a_target_cannot_be_published_as_a_role_reading_more_than_its_saver(
    surface, monkeypatch
):
    _narrowed(monkeypatch)
    with pytest.raises(ApiError) as refused:
        await _put({"enabled": True, "targets": [{"catalog": "mz", "role": "steward"}]})
    assert (refused.value.status_code, refused.value.code) == (
        403,
        "metadata_export.role_beyond_reads",
    )
    assert "ssn" not in str(refused.value)
    await _put({"enabled": True, "targets": [{"catalog": "mz", "role": "analyst"}]})
    assert surface.written["metadata_export"]["targets"] == [{"catalog": "mz", "role": "analyst"}]


@pytest.mark.asyncio
async def test_the_role_picker_offers_only_roles_within_the_callers_reads(surface, monkeypatch):
    from provisa.api.admin.metadata_export_router import get_metadata_export

    _narrowed(monkeypatch)
    surface.stored.update({"enabled": True, "targets": [{"catalog": "mz"}]})
    assert (await get_metadata_export(_request()))["publishable_roles"] == ["analyst"]
