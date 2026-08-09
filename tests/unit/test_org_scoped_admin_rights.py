# Copyright (c) 2026 Kenneth Stott
# Canary: 5c1e97a4-2b60-4f38-8d7a-e0946bb3f215
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1349: the org-scoped admin rights, and the per-org config layer they gate.

An org administrator in a multitenant deployment holds neither ``platform_settings`` nor
``cross_org``, which left every Admin surface invisible to them. The fix is vocabulary, not a role
check (REQ-1337): ``org_settings`` names the surfaces whose subject is the acting org — its AI/NL
provider, domains, scheduled tasks, approvals — and ``observability`` names the read-only
performance and health views. Asserted here:

* both rights exist as capabilities and are distinct from platform bypass;
* the gates admit a holder, admit platform bypass, reject everyone else, and allow dev mode;
* the config layer merges one level deep and refuses any key that is not the org's to own;
* the span buffer scopes traces by the org that produced them.
"""

from __future__ import annotations

import types

import pytest
from fastapi import HTTPException

from provisa.core.org_settings import ORG_OVERRIDABLE_KEYS, merge_org_overrides
from provisa.security.rights import Capability, has_platform_bypass

# --- the rights themselves ----------------------------------------------------------------------


def test_the_org_scoped_rights_exist_and_are_not_platform_bypass():
    assert Capability.ORG_SETTINGS.value == "org_settings"
    assert Capability.OBSERVABILITY.value == "observability"
    # Holding either must not open the deployment-wide surfaces. The whole point of minting them
    # was to stop needing the `admin` wildcard to make the Admin tab useful to an org.
    assert not has_platform_bypass({"org_settings", "observability"})


def test_org_settings_is_not_platform_settings():
    # Two different scopes: the org being acted in, versus the deployment. A gate must never treat
    # one as satisfying the other, or an org administrator reaches the federation engine.
    assert Capability.ORG_SETTINGS.value != Capability.PLATFORM_SETTINGS.value


# --- the gates ----------------------------------------------------------------------------------


def _request(caps: set[str] | None, *, user_id: str = "alice"):
    """A Request stand-in carrying an identity; ``caps`` None means no identity at all (dev mode)."""
    identity = None if caps is None else types.SimpleNamespace(user_id=user_id, roles=[])
    return types.SimpleNamespace(state=types.SimpleNamespace(identity=identity))


@pytest.fixture
def resolve_caps(monkeypatch):
    """Pin ``_resolved_capabilities`` so the gates are tested, not the claim-resolution path."""

    def _install(caps: set[str]):
        import provisa.api.admin.capabilities as capmod

        monkeypatch.setattr(capmod, "_resolved_capabilities", lambda identity, state: caps)

    return _install


@pytest.mark.parametrize(
    "gate_name,right",
    [("require_org_settings", "org_settings"), ("require_observability", "observability")],
)
class TestOrgScopedGates:
    def _gate(self, gate_name):
        from provisa.api.admin import _platform_guard

        return getattr(_platform_guard, gate_name)

    def test_the_named_right_admits(self, gate_name, right, resolve_caps):
        resolve_caps({right})
        self._gate(gate_name)(_request({right}))  # no raise

    def test_platform_bypass_admits(self, gate_name, right, resolve_caps):
        # platform_admin carries the wildcard and never needs the org-scoped right enumerated.
        resolve_caps({"admin", "superadmin"})
        self._gate(gate_name)(_request({"admin"}))

    def test_a_data_plane_right_is_rejected(self, gate_name, right, resolve_caps):
        # An analyst holds `usage` and a developer `query_development`; neither administers the org.
        resolve_caps({"usage", "query_development", "full_results"})
        with pytest.raises(HTTPException) as exc:
            self._gate(gate_name)(_request({"usage"}))
        assert exc.value.status_code == 403
        assert right in exc.value.detail

    def test_the_other_org_scoped_right_does_not_substitute(self, gate_name, right, resolve_caps):
        # Holding observability must not open the settings writer, and vice versa: read-only
        # performance access and the right to change the org's AI provider are separate grants.
        other = "observability" if right == "org_settings" else "org_settings"
        resolve_caps({other})
        with pytest.raises(HTTPException):
            self._gate(gate_name)(_request({other}))

    def test_dev_mode_is_allowed(self, gate_name, right, resolve_caps):
        # No auth configured — matches every other admin gate in the tree.
        resolve_caps(set())
        self._gate(gate_name)(_request(None))
        self._gate(gate_name)(_request(set(), user_id="anonymous"))


# --- the per-org config layer ---------------------------------------------------------------------


def test_overridable_keys_exclude_every_deployment_wide_concern():
    # The allow-list is the boundary: a key absent from it has no per-org representation at all.
    # metadata_export joined the set with REQ-1074: the catalog an org publishes to, and the
    # credentials it publishes with, are that org's, not the deployment's.
    assert ORG_OVERRIDABLE_KEYS == {"ai_models", "vector_models", "nl", "metadata_export"}
    for deployment_wide in (
        "federation",
        "cache",
        "encryption",
        "auth",
        "sources",
        "engine",
        "database",
    ):
        assert deployment_wide not in ORG_OVERRIDABLE_KEYS


def test_a_dict_override_merges_one_level_deep():
    # The org changes only the NL provider; every other operation keeps the deployment's model.
    base = {
        "ai_models": {
            "sql_generation": {"vendor": "anthropic", "model": "claude-opus-5"},
            "column_description": {"vendor": "openai", "model": "gpt-4o"},
        },
        "nl": {"rate_limit": 10},
    }
    merged = merge_org_overrides(
        base, {"ai_models": {"sql_generation": {"vendor": "openai", "model": "gpt-4o"}}}
    )

    assert merged["ai_models"]["sql_generation"] == {"vendor": "openai", "model": "gpt-4o"}
    assert merged["ai_models"]["column_description"] == {"vendor": "openai", "model": "gpt-4o"}
    assert merged["nl"] == {"rate_limit": 10}


def test_a_non_dict_override_replaces_outright():
    # vector_models is a list — an org owns the registry whole or not at all; element-wise merging
    # would produce a registry neither the deployment nor the org asked for.
    base = {"vector_models": [{"id": "a"}, {"id": "b"}]}
    assert merge_org_overrides(base, {"vector_models": [{"id": "c"}]}) == {
        "vector_models": [{"id": "c"}]
    }


def test_the_base_config_is_not_mutated():
    # resolve_org_config runs per request against the process-wide config dict; mutating it would
    # leak one org's provider into the next org's request.
    base = {"ai_models": {"sql_generation": {"vendor": "anthropic"}}}
    merge_org_overrides(base, {"ai_models": {"sql_generation": {"vendor": "openai"}}})
    assert base == {"ai_models": {"sql_generation": {"vendor": "anthropic"}}}


def test_no_overrides_yields_the_deployment_config():
    base = {"ai_models": {"sql_generation": {"vendor": "anthropic"}}}
    assert merge_org_overrides(base, {}) == base


@pytest.mark.asyncio
async def test_write_rejects_a_key_the_org_does_not_own():
    # The gate decides WHETHER you may write org settings; this decides WHAT is an org setting. A
    # caller holding org_settings must not reach the federation engine through the same endpoint.
    from provisa.core.org_settings import write_org_overrides

    with pytest.raises(ValueError, match="not org-overridable"):
        await write_org_overrides(
            object(), {"ai_models": {}, "federation": {"engine": "trino"}}, updated_by="alice"
        )


# --- the AI-models surface writes an org DELTA -----------------------------------------------------


@pytest.fixture
def ai_models_surface(monkeypatch, resolve_caps):
    """Drive the router against recorded org overrides instead of a database.

    Returns ``(call, written)``: ``call(body, existing)`` runs the PUT handler with ``existing``
    already stored for the org, and ``written`` collects what the handler persisted.
    """
    import provisa.core.org_settings as org_settings_mod

    resolve_caps({"org_settings"})
    written: dict = {}

    async def _read(_db):
        return _read.existing

    async def _write(_db, updates, *, updated_by):
        written.clear()
        written.update(updates)
        written["_updated_by"] = updated_by
        return list(updates)

    monkeypatch.setattr(org_settings_mod, "read_org_overrides", _read)
    monkeypatch.setattr(org_settings_mod, "write_org_overrides", _write)
    monkeypatch.setattr(
        "provisa.api.app.state", types.SimpleNamespace(tenant_db=object()), raising=False
    )

    async def call(body: dict, existing: dict | None = None):
        from provisa.api.admin.ai_models_router import set_ai_models

        _read.existing = existing or {}
        request = _request({"org_settings"})
        request.json = _json_body(body)
        return await set_ai_models(request)

    return call, written


def _json_body(body: dict):
    async def _json():
        return body

    return _json


@pytest.mark.asyncio
async def test_setting_the_nl_provider_writes_only_that_role(ai_models_surface):
    # "Change the LLM provider for NL" is exactly ai_models.sql_generation. The org's stored
    # override must carry that role alone — everything else stays the deployment's choice, so a
    # later deployment-wide model change still reaches this org.
    call, written = ai_models_surface
    result = await call({"ai_models": {"sql_generation": {"vendor": "openai", "model": "gpt-4o"}}})

    assert result["updated"] == ["ai_models.sql_generation"]
    assert result["restart_required"] is False
    assert written["ai_models"] == {"sql_generation": {"vendor": "openai", "model": "gpt-4o"}}


@pytest.mark.asyncio
async def test_a_blank_model_removes_the_org_override(ai_models_surface):
    # Reverting is "use the deployment's choice", not "use a value baked into the router".
    call, written = ai_models_surface
    await call(
        {"ai_models": {"sql_generation": ""}},
        existing={"ai_models": {"sql_generation": "gpt-4o", "table_selection": "gpt-4o-mini"}},
    )

    assert written["ai_models"] == {"table_selection": "gpt-4o-mini"}


@pytest.mark.asyncio
async def test_clearing_the_last_override_deletes_the_row(ai_models_surface):
    # An empty delta is not an empty override — it is NO override, so the row goes away and the
    # deployment config governs the org again with nothing shadowing it.
    call, written = ai_models_surface
    await call(
        {"ai_models": {"sql_generation": ""}}, existing={"ai_models": {"sql_generation": "x"}}
    )

    assert written["ai_models"] is None


@pytest.mark.asyncio
async def test_an_unknown_model_role_is_ignored(ai_models_surface):
    call, written = ai_models_surface
    await call({"ai_models": {"not_a_role": "gpt-4o"}})

    assert written["ai_models"] is None


@pytest.mark.asyncio
async def test_the_writer_is_attributed_to_the_caller(ai_models_surface):
    call, written = ai_models_surface
    await call({"nl": {"rate_limit": 5}})

    assert written["nl"] == {"rate_limit": 5}
    assert written["_updated_by"] == "alice"


@pytest.mark.asyncio
async def test_the_surface_rejects_a_caller_without_the_right(resolve_caps):
    # Before REQ-1349 this endpoint had no server-side gate at all: any authenticated user could
    # rewrite the deployment's AI config, with only the UI route hiding it.
    from provisa.api.admin.ai_models_router import get_ai_models, set_ai_models

    resolve_caps({"usage", "query_development"})
    for handler in (get_ai_models, set_ai_models):
        with pytest.raises(HTTPException) as exc:
            await handler(_request({"usage"}))
        assert exc.value.status_code == 403


# --- the NL query path uses the acting org's provider ----------------------------------------------


def test_the_llm_client_honours_a_passed_config():
    # The seam that makes an org override reach a query: the client resolves vendor/model from the
    # config it is HANDED, not from the process-global file it used to read at construction.
    from provisa.llm.client import ProvisaLLMClient

    client = ProvisaLLMClient(
        "sql_generation",
        config={"ai_models": {"sql_generation": {"vendor": "openai", "model": "gpt-4o"}}},
    )
    assert (client._vendor, client._model) == ("openai", "gpt-4o")


@pytest.mark.asyncio
async def test_nl_builds_its_client_from_the_orgs_resolved_config(monkeypatch):
    # _get_llm runs per request, so an org administrator's provider change takes effect on the next
    # NL query — there is no cached client and no restart between the write and the read.
    import provisa.core.org_secrets as org_secrets_mod
    import provisa.core.org_settings as org_settings_mod
    from provisa.api.rest.nl_router import _get_llm

    async def _resolve(_db):
        return {"ai_models": {"sql_generation": {"vendor": "openai", "model": "gpt-4o"}}}

    async def _api_keys(_db):
        return {}

    monkeypatch.setattr(org_settings_mod, "resolve_org_config", _resolve)
    monkeypatch.setattr(org_secrets_mod, "read_org_api_keys", _api_keys)
    client = await _get_llm(types.SimpleNamespace(tenant_db=object()))

    assert (client._vendor, client._model) == ("openai", "gpt-4o")


# --- org-scoped traces ----------------------------------------------------------------------------


class _FakeSpan:
    def __init__(self, name: str) -> None:
        self.name = name
        self.status = None
        self.end_time = None
        self.start_time = None
        self.attributes = {}

    def get_span_context(self):
        return types.SimpleNamespace(trace_id=1, span_id=2)


def _buffer_with(entries: list[tuple[str, str | None]]):
    """A SpanBuffer holding one span per (name, org) pair, pushed under that org's ContextVar."""
    from provisa.api.org_runtime import current_org
    from provisa.api.otel_setup import SpanBuffer

    buf = SpanBuffer()
    for name, org in entries:
        token = current_org.set(org)
        try:
            buf.push(_FakeSpan(name))
        finally:
            current_org.reset(token)
    return buf


def test_recent_without_a_scope_returns_every_span():
    # A cross_org holder — or a single-tenant deployment — reads the whole buffer.
    buf = _buffer_with([("a", "acme"), ("b", "beta"), ("c", None)])
    assert {e["name"] for e in buf.recent()} == {"a", "b", "c"}


def test_recent_scoped_to_an_org_excludes_another_tenants_spans():
    buf = _buffer_with([("a", "acme"), ("b", "beta")])
    assert [e["name"] for e in buf.recent(org_id="acme")] == ["a"]


def test_a_span_with_no_org_is_invisible_to_an_org_scoped_reader():
    # Startup and background spans belong to no org. Showing them to a tenant would be a claim
    # about that tenant's activity that is not true.
    buf = _buffer_with([("startup", None), ("query", "acme")])
    assert [e["name"] for e in buf.recent(org_id="acme")] == ["query"]
