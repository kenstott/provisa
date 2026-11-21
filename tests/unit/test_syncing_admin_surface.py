# Copyright (c) 2026 Kenneth Stott
# Canary: 3e7a9c51-6b28-4d04-a1f3-9c5d0e72b846
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-2247, REQ-2122, REQ-1074: the Syncing page's surface.

One list of entries replaces the Catalogs panel and the Metadata Export screen: each entry is
connected once, states its type and direction, and a data catalog carries its export settings,
Publish now and its last run. What is asserted is what a wrong surface gets wrong silently:
returning a stored credential, erasing one the admin did not retype, keeping an entry for a domain
the org does not have, publishing to an entry whose Export is off, publishing below the entitled
tier, and publishing as a role that reads more than its saver.
"""

# Requirements: REQ-2247, REQ-2122, REQ-1072, REQ-1073, REQ-1074, REQ-2152

from __future__ import annotations

import types
from typing import Any

import pytest

from provisa.api.errors import ApiError
from provisa.core.models import ProvisaConfig
from tests.role_state import RoleState

ORG_ID = "acme"


def _request(body: dict | None = None, org_id: str = ORG_ID) -> Any:
    request = types.SimpleNamespace(
        state=types.SimpleNamespace(
            identity=types.SimpleNamespace(user_id="alice", roles=[]), active_org_id=org_id
        )
    )

    async def _json():
        return body or {}

    request.json = _json
    return request


def _catalog(**over) -> dict:
    return {
        "id": "mz",
        "type": "data_catalog",
        "direction": "both",
        "provider": "openlineage",
        "endpoint": "http://mz",
        "domains": ["sales"],
        **over,
    }


@pytest.fixture(autouse=True)
def _served(monkeypatch):
    """Every column is served here; what an entry may be published as is the role tests below."""
    from tests.unit.gate_identity import serve_every_column

    serve_every_column(monkeypatch)


class _TenantDb:
    """The control plane's org database, holding the catalog_bindings table (REQ-1389): the
    publish path loads and persists vendor bindings around every publish."""

    def __init__(self, control_plane):
        self._db = control_plane.org()

    def acquire(self):
        return self._db.acquire()


@pytest.fixture
def surface(monkeypatch, control_plane):
    """The handlers against recorded org settings, the org's domains and a registered org.

    ``settings`` is what the org has saved (mutate it to set up a case), ``written`` what a handler
    persisted, and ``org`` re-registers the org at a tier.
    """
    import provisa.api.admin.capabilities as capmod
    import provisa.api.admin.syncing_router as mod
    import provisa.core.org_settings as org_settings_mod

    monkeypatch.setattr(capmod, "_resolved_capabilities", lambda identity, state: {"org_settings"})
    settings: dict = {"catalogs": [], "metadata_export": {}}
    written: dict = {}

    async def _resolve(_db):
        return {key: value for key, value in settings.items()}

    async def _write(_db, updates, *, updated_by):
        written.clear()
        written.update(updates)
        written["_updated_by"] = updated_by
        return list(updates)

    async def _domains():
        return {"sales", "finance"}

    monkeypatch.setattr(org_settings_mod, "resolve_org_config", _resolve)
    monkeypatch.setattr(org_settings_mod, "write_org_overrides", _write)
    monkeypatch.setattr(mod, "_domain_ids", _domains)
    monkeypatch.setattr(mod, "_LAST_RUN", {})
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
    return types.SimpleNamespace(settings=settings, written=written, org=_org)


async def _get() -> dict:
    from provisa.api.admin.syncing_router import get_syncing

    return await get_syncing(_request())


async def _put(entries: list[dict], **extra):
    from provisa.api.admin.syncing_router import set_syncing

    return await set_syncing(_request({"entries": entries, **extra}))


def _stub_publish(monkeypatch, exporter=None):
    """The publish path with the snapshot and the model stubbed, publishing through ``exporter``."""
    import provisa.api.metadata_export.publishing as sync_mod
    from provisa.api.metadata_export.model import MetadataSnapshot

    monkeypatch.setattr(
        sync_mod, "build_snapshot", lambda config, **kwargs: MetadataSnapshot(org_id=ORG_ID)
    )

    async def _stub_model():
        # Empty rather than a bare object: the publish path reads each registered contract's
        # last scan off this config (REQ-1443), and an empty config registers none.
        return ProvisaConfig(sources=[], domains=[], tables=[], roles=[])

    monkeypatch.setattr(sync_mod, "_model_for_export", _stub_model)

    async def _reads_everything(state, role):
        return lambda table: None  # what a role is sent is test_metadata_export_scope

    monkeypatch.setattr(sync_mod, "role_reader", _reads_everything)
    monkeypatch.setattr(sync_mod, "role_reach", lambda state, role: lambda domain_id: True)
    built: list = []

    def _export(target):
        built.append(target.id)
        if exporter is None:
            pytest.fail(f"published to {target.id}")
        return exporter

    monkeypatch.setattr(sync_mod, "metadata_export", _export)
    return built


class _Partial:
    needs_export_views = False  # a remote catalog: it addresses nothing in the engine's store
    binds_products = False  # OpenLineage keeps no product bindings

    def __init__(self, result):
        self._result = result

    async def publish(self, snapshot):
        return self._result


# --- reading ---------------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_the_read_reports_credentials_as_set_and_never_returns_them(surface):
    surface.settings["catalogs"] = [_catalog(api_key="s3cr3t")]
    body = await _get()

    (entry,) = body["entries"]
    assert (entry["api_key_set"], entry["token_set"]) == (True, False)
    assert "api_key" not in entry
    assert "s3cr3t" not in str(body)
    assert (entry["endpoint"], entry["domains"]) == ("http://mz", ["sales"])
    assert {"openlineage", "openmetadata"} <= set(body["providers"])


@pytest.mark.asyncio
async def test_the_read_reports_each_entry_with_its_type_direction_and_export_settings(surface):
    surface.settings["catalogs"] = [
        _catalog(direction="export", role="steward"),
        {"id": "terms", "type": "ontology", "direction": "import"},
    ]
    entries = {e["id"]: e for e in (await _get())["entries"]}

    assert (entries["mz"]["type"], entries["mz"]["direction"]) == ("data_catalog", "export")
    assert {k: entries["mz"][k] for k in ("role", "datazone_project_id", "delta_share")} == {
        "role": "steward",
        "datazone_project_id": "",
        "delta_share": False,
    }
    assert (entries["terms"]["type"], entries["terms"]["direction"]) == ("ontology", "import")
    # Nothing has run in this process yet.
    assert entries["mz"]["last_run"] is None


@pytest.mark.asyncio
async def test_the_read_names_the_providers_not_yet_proven_as_beta(surface):
    """The provider picker labels them "(beta)" from the adapter's own flag (AtlanExport.beta)."""
    from provisa.api.metadata_export.registry import beta_providers

    body = await _get()
    assert "atlan" in body["beta_providers"]
    assert body["beta_providers"] == beta_providers()
    assert set(body["beta_providers"]) <= set(body["providers"])


@pytest.mark.asyncio
async def test_the_read_carries_the_entitlement_flag_and_the_schedule(surface):
    surface.settings["metadata_export"] = {"reconcile_cron": "*/5 * * * *"}
    body = await _get()
    assert (body["entitled"], body["reconcile_cron"]) == (True, "*/5 * * * *")
    surface.org(tier="standard")
    body = await _get()
    assert body["entitled"] is False
    # The page tells the admin what tier would open publishing, so the gate is explicable.
    assert body["required_tier"] == "premium"


# --- writing ---------------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_editing_the_endpoint_does_not_erase_the_stored_credential(surface):
    surface.settings["catalogs"] = [_catalog(endpoint="http://old", api_key="keep-me")]
    await _put([_catalog(endpoint="http://new")])

    (kept,) = surface.written["catalogs"]
    assert (kept["endpoint"], kept["api_key"]) == ("http://new", "keep-me")
    assert surface.written["_updated_by"] == "alice"


@pytest.mark.asyncio
async def test_two_catalogs_of_one_provider_keep_their_credentials_apart(surface):
    om = {"provider": "openmetadata"}
    surface.settings["catalogs"] = [
        _catalog(id="om-a", endpoint="http://a", token="ta", **om),
        _catalog(id="om-b", endpoint="http://b", token="tb", domains=["finance"], **om),
    ]
    await _put(
        [
            _catalog(id="om-b", endpoint="http://b2", domains=["finance"], **om),
            _catalog(id="om-a", endpoint="http://a", **om),
        ]
    )

    kept = {c["id"]: c for c in surface.written["catalogs"]}
    assert (kept["om-a"]["token"], kept["om-b"]["token"]) == ("ta", "tb")
    assert kept["om-b"]["endpoint"] == "http://b2"


@pytest.mark.asyncio
async def test_an_empty_credential_clears_it(surface):
    surface.settings["catalogs"] = [_catalog(api_key="old")]
    await _put([_catalog(api_key="")])

    assert surface.written["catalogs"][0]["api_key"] == ""


@pytest.mark.asyncio
async def test_a_key_outside_the_entry_settings_is_not_written(surface):
    # The body is an untrusted dict; only the enumerated settings may reach storage.
    await _put([_catalog(tier="x")], tier="x")

    assert "tier" not in surface.written["catalogs"][0]
    assert "tier" not in surface.written


@pytest.mark.asyncio
async def test_an_entry_left_out_of_the_list_is_removed(surface):
    surface.settings["catalogs"] = [_catalog(), _catalog(id="dh", provider="datahub")]
    await _put([_catalog(id="dh", provider="datahub")])

    assert [c["id"] for c in surface.written["catalogs"]] == ["dh"]


@pytest.mark.asyncio
async def test_the_export_settings_are_kept_on_the_entry(surface):
    await _put([_catalog(direction="export", role="steward", delta_share=True)])

    (kept,) = surface.written["catalogs"]
    assert (kept["direction"], kept["role"], kept["delta_share"]) == ("export", "steward", True)


@pytest.mark.asyncio
@pytest.mark.parametrize("entry_type", ["ontology", "custom"])
async def test_an_ontology_or_custom_entry_is_created_and_listed(surface, entry_type):
    await _put([{"id": "terms", "type": entry_type, "direction": "both"}])

    (kept,) = surface.written["catalogs"]
    assert (kept["type"], kept["direction"]) == (entry_type, "both")
    surface.settings["catalogs"] = surface.written["catalogs"]
    (listed,) = (await _get())["entries"]
    assert (listed["id"], listed["type"]) == ("terms", entry_type)


@pytest.mark.asyncio
async def test_an_entry_with_no_direction_is_refused(surface):
    entry = _catalog()
    del entry["direction"]
    with pytest.raises(ApiError) as exc:
        await _put([entry])

    assert (exc.value.status_code, exc.value.code) == (400, "syncing.invalid")
    assert surface.written == {}


@pytest.mark.asyncio
async def test_a_catalog_with_no_domain_is_refused(surface):
    with pytest.raises(ApiError) as exc:
        await _put([_catalog(domains=[])])

    assert exc.value.status_code == 400
    assert "assigned to no domain" in str(exc.value.detail)
    assert surface.written == {}


@pytest.mark.asyncio
async def test_a_catalog_for_a_domain_the_org_does_not_have_is_refused(surface):
    with pytest.raises(ApiError) as exc:
        await _put([_catalog(domains=["sales", "hr"])])

    assert (exc.value.status_code, exc.value.code) == (400, "catalogs.unknown_domain")
    assert exc.value.params["domains"] == "hr"
    assert surface.written == {}


@pytest.mark.asyncio
async def test_a_catalog_with_no_endpoint_is_refused_naming_the_setting(surface):
    with pytest.raises(ApiError) as exc:
        await _put([_catalog(provider="openmetadata", endpoint="")])

    assert "catalog 'mz' has no endpoint" in str(exc.value.detail)


@pytest.mark.asyncio
async def test_the_schedule_is_kept_once_for_the_organisation(surface):
    await _put([], reconcile_cron="*/10 * * * *")

    assert surface.written["metadata_export"] == {"reconcile_cron": "*/10 * * * *"}


# --- the tier gate (REQ-1073) ----------------------------------------------------------------


@pytest.mark.asyncio
async def test_below_the_tier_an_entry_may_import_but_not_turn_export_on(surface):
    surface.org(tier="standard")
    await _put([_catalog(direction="import")])
    assert surface.written["catalogs"][0]["direction"] == "import"

    with pytest.raises(ApiError) as exc:
        await _put([_catalog(direction="both")])
    assert exc.value.status_code == 403


@pytest.mark.asyncio
async def test_below_the_tier_an_exporting_entry_kept_as_it_was_can_still_be_edited(surface):
    """A lapsed plan must not lock the admin out of an entry's address."""
    surface.settings["catalogs"] = [_catalog(direction="export", role="steward")]
    surface.org(tier="standard")
    await _put([_catalog(direction="export", role="steward", endpoint="http://moved")])

    assert surface.written["catalogs"][0]["endpoint"] == "http://moved"


@pytest.mark.asyncio
async def test_below_the_tier_the_schedule_cannot_be_changed(surface):
    surface.org(tier="standard")
    with pytest.raises(ApiError) as exc:
        await _put([], reconcile_cron="*/1 * * * *")
    assert exc.value.status_code == 403


@pytest.mark.asyncio
@pytest.mark.parametrize("handler_name", ["publish_all", "publish_entry"])
async def test_every_publish_refuses_an_unentitled_org(surface, handler_name):
    """REQ-1073: hiding the button is not the gate -- the endpoints are."""
    import provisa.api.admin.syncing_router as mod

    surface.settings["catalogs"] = [_catalog(role="steward")]
    surface.org(tier="standard")
    args = (_request(),) if handler_name == "publish_all" else (_request(), "mz")

    with pytest.raises(ApiError) as exc:
        await getattr(mod, handler_name)(*args)
    assert exc.value.status_code == 403


@pytest.mark.asyncio
async def test_an_org_the_control_plane_never_registered_is_refused(surface):
    from provisa.api.admin.syncing_router import publish_all

    with pytest.raises(ApiError) as exc:
        await publish_all(_request(org_id="ghost"))
    assert exc.value.status_code == 403


# --- the connection test ---------------------------------------------------------------------


@pytest.mark.asyncio
async def test_the_connection_test_reports_the_providers_refusal(surface, monkeypatch):
    """The test is the provider's own health check, the one the export runs; a wrong URL and a
    rejected credential look identical without the message."""
    import provisa.api.admin.syncing_router as mod

    surface.settings["catalogs"] = [_catalog()]
    built = []

    class _Failing:
        async def health(self):
            raise ConnectionError("Name or service not known")

    def _export(target):
        built.append(target)
        return _Failing()

    monkeypatch.setattr(mod, "metadata_export", _export)
    body = await mod.check_entry(_request(), "mz")

    assert body["ok"] is False
    assert "ConnectionError: Name or service not known" in body["error"]
    assert (built[0].id, built[0].endpoint) == ("mz", "http://mz")


@pytest.mark.asyncio
async def test_the_connection_test_carries_the_entrys_export_settings(surface, monkeypatch):
    """A DataZone catalog is tested with the project that owns its products."""
    import provisa.api.admin.syncing_router as mod

    surface.settings["catalogs"] = [
        _catalog(
            id="dz",
            provider="datazone",
            endpoint="",
            aws_region="us-east-1",
            datazone_domain_id="dzd_1",
            datazone_project_id="prj_1",
        )
    ]
    built = []

    class _Healthy:
        async def health(self):
            return None

    monkeypatch.setattr(mod, "metadata_export", lambda target: built.append(target) or _Healthy())
    body = await mod.check_entry(_request(), "dz")

    assert body["ok"] is True
    assert built[0].datazone_project_id == "prj_1"


@pytest.mark.asyncio
async def test_the_connection_test_on_an_unknown_provider_answers_instead_of_500ing(surface):
    """Adapter construction raises MetadataExportNotConfiguredError, and that text is the answer."""
    from provisa.api.admin.syncing_router import check_entry

    surface.settings["catalogs"] = [_catalog(id="x", provider="nope")]
    body = await check_entry(_request(), "x")

    assert body["ok"] is False
    assert "Unknown metadata export provider 'nope'" in body["error"]


@pytest.mark.asyncio
async def test_testing_an_entry_the_org_does_not_have_is_a_404(surface):
    from provisa.api.admin.syncing_router import check_entry

    with pytest.raises(ApiError) as exc:
        await check_entry(_request(), "nope")
    assert exc.value.status_code == 404


# --- Publish now and the last run --------------------------------------------------------------


@pytest.mark.asyncio
async def test_publish_now_returns_the_assets_the_entry_rejected_and_keeps_it_as_its_last_run(
    surface, monkeypatch
):
    """A partial publish is the case the page exists to diagnose."""
    from provisa.api.admin.syncing_router import publish_entry
    from provisa.api.metadata_export.model import AssetKind, AssetRef
    from provisa.api.metadata_export.provider import AssetError, PublishResult

    surface.settings["catalogs"] = [_catalog(role="steward")]
    result = PublishResult(
        provider_name="openlineage",
        published={"dataset": 2},
        errors=[
            AssetError(
                asset=AssetRef(kind=AssetKind.TABLE, parts=("wh", "public.orders")),
                message="422 unknown field type",
            )
        ],
    )
    _stub_publish(monkeypatch, _Partial(result))
    body = await publish_entry(_request(), "mz")

    assert body["ok"] is False
    (outcome,) = body["entries"]
    assert (outcome["id"], outcome["total_published"]) == ("mz", 2)
    assert outcome["errors"] == [{"asset": "wh.public.orders", "message": "422 unknown field type"}]
    # The page shows the last outcome on the entry without re-publishing to find out what it was.
    (entry,) = (await _get())["entries"]
    assert entry["last_run"] == outcome


@pytest.mark.asyncio
async def test_publish_now_on_an_entry_whose_export_is_off_is_refused_before_a_snapshot(
    surface, monkeypatch
):
    """REQ-2247: a direction that is off never runs, by hand as on a schedule."""
    from provisa.api.admin.syncing_router import publish_entry

    surface.settings["catalogs"] = [_catalog(direction="import", role="steward")]
    _stub_publish(monkeypatch)

    with pytest.raises(ApiError) as exc:
        await publish_entry(_request(), "mz")
    assert (exc.value.status_code, exc.value.code) == (400, "syncing.direction_off")


@pytest.mark.asyncio
async def test_an_ontology_entry_has_no_publish_to_run(surface, monkeypatch):
    from provisa.api.admin.syncing_router import publish_entry

    surface.settings["catalogs"] = [{"id": "terms", "type": "ontology", "direction": "export"}]
    _stub_publish(monkeypatch)

    with pytest.raises(ApiError) as exc:
        await publish_entry(_request(), "terms")
    assert (exc.value.status_code, exc.value.code) == (400, "syncing.no_publish")


@pytest.mark.asyncio
async def test_publish_all_reaches_only_the_entries_that_export(surface, monkeypatch):
    from provisa.api.admin.syncing_router import publish_all
    from provisa.api.metadata_export.provider import PublishResult

    surface.settings["catalogs"] = [
        _catalog(id="out", direction="export", role="steward"),
        _catalog(id="in", direction="import", role="steward"),
        {"id": "terms", "type": "ontology", "direction": "export"},
    ]
    built = _stub_publish(monkeypatch, _Partial(PublishResult(provider_name="openlineage")))
    body = await publish_all(_request())

    assert built == ["out"]
    assert [o["id"] for o in body["entries"]] == ["out"]


@pytest.mark.asyncio
async def test_publish_all_with_no_entry_that_exports_is_refused(surface, monkeypatch):
    from provisa.api.admin.syncing_router import publish_all

    surface.settings["catalogs"] = [_catalog(direction="import")]
    _stub_publish(monkeypatch)

    with pytest.raises(ApiError) as exc:
        await publish_all(_request())
    assert (exc.value.status_code, exc.value.code) == (400, "syncing.nothing_exports")


@pytest.mark.asyncio
async def test_an_entry_naming_no_role_is_sent_nothing_and_says_why(surface, monkeypatch):
    """REQ-2152 (Q1): a catalog is published as a role; an entry naming none is refused by name."""
    from provisa.api.admin.syncing_router import publish_entry

    surface.settings["catalogs"] = [_catalog()]
    _stub_publish(monkeypatch)
    body = await publish_entry(_request(), "mz")

    (outcome,) = body["entries"]
    assert outcome["errors"] == [
        {
            "asset": "mz",
            "message": "catalog 'mz' names no role to publish as: set the role whose reads it is sent",
        }
    ]


# --- an entry's role is within its saver's reads (REQ-2152, ruling Q8) ------------------------


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
async def test_an_entry_cannot_be_published_as_a_role_reading_more_than_its_saver(
    surface, monkeypatch
):
    _narrowed(monkeypatch)
    with pytest.raises(ApiError) as refused:
        await _put([_catalog(role="steward")])
    assert (refused.value.status_code, refused.value.code) == (
        403,
        "metadata_export.role_beyond_reads",
    )
    assert "ssn" not in str(refused.value)
    await _put([_catalog(role="analyst")])
    assert surface.written["catalogs"][0]["role"] == "analyst"


@pytest.mark.asyncio
async def test_a_role_someone_else_saved_does_not_block_editing_the_entry(surface, monkeypatch):
    _narrowed(monkeypatch)
    surface.settings["catalogs"] = [_catalog(role="steward")]
    await _put([_catalog(role="steward", endpoint="http://moved")])

    assert surface.written["catalogs"][0]["endpoint"] == "http://moved"


@pytest.mark.asyncio
async def test_the_role_picker_offers_only_roles_within_the_callers_reads(surface, monkeypatch):
    _narrowed(monkeypatch)
    assert (await _get())["publishable_roles"] == ["analyst"]
