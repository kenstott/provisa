# Copyright (c) 2026 Kenneth Stott
# Canary: 3b9d5c27-1f48-4a6e-b2d0-7c4e9a1f6d58
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The ways into a held organization that do not pass the write seam as a person's write
(REQ-1557): a configuration import, a tree brought from outside the organization's history, an
admin route's body, a key variable read at use. And what the environment gives a credential
field is kept out of error text and logs, while what it gives any other field stays readable."""

# Requirements: REQ-1557, REQ-2087

from __future__ import annotations

import json
from types import SimpleNamespace

import pytest
import yaml

from provisa.core.env_reference_guard import EnvReferenceEditRefused, EnvReferenceRefused
from provisa.core.request_context import current_acting_role
from provisa.core.secrets import (
    is_credential_field,
    resolve_api_key_field,
    resolve_secrets,
    resolve_secrets_in_dict,
)

pytestmark = pytest.mark.asyncio

_VARIABLE = "PROVISA_TEST_SERVER_ONLY_VARIABLE"
_VALUE = "the-servers-own-value-7f3a"
_REF = f"${{env:{_VARIABLE}}}"
_SANDBOX = "sbx0123456789ab"
_TENANT = "acme"
_DEPLOYMENT = "default"


@pytest.fixture
def server(monkeypatch):
    from provisa.core import secrets, secrets_providers
    from provisa.security import redaction, sandbox_egress

    # The API layer says what the deployment is when it is imported; it is imported first, so
    # what this test says after it is what holds.
    import provisa.api.app  # noqa: F401

    monkeypatch.setenv(_VARIABLE, _VALUE)
    read: list[str] = []
    resolve = secrets.EnvSecretsProvider.resolve

    def _recorded(self, reference: str) -> str:
        read.append(reference)
        return resolve(self, reference)

    monkeypatch.setattr(secrets.EnvSecretsProvider, "resolve", _recorded)
    redaction.reset()

    def _say(many: bool) -> None:
        monkeypatch.setattr(secrets_providers, "_many_organisations", lambda: many)
        monkeypatch.setattr(sandbox_egress, "_deployment_org", lambda: _DEPLOYMENT)

    _say.read = read  # type: ignore[attr-defined]
    yield _say
    redaction.reset()


@pytest.fixture
def member():
    """Bind an organization with a person acting in it, until the test ends."""
    from provisa.core.request_context import current_org

    before = (current_org.get(), current_acting_role.get())

    def _bind(org_id: str) -> None:
        current_org.set(org_id)
        current_acting_role.set("org_admin")

    yield _bind
    current_org.set(before[0])
    current_acting_role.set(before[1])


# --- the configuration import -----------------------------------------------------------------

_CONFIG = {
    "sources": [
        {"id": "pg1", "type": "postgresql", "host": "db.example.com", "port": 5432, "database": "d"}
    ]
}


def _request():
    return SimpleNamespace(
        state=SimpleNamespace(identity=SimpleNamespace(user_id="a@example.com", roles=[]))
    )


def _grant(monkeypatch, caps: set[str]) -> None:
    import provisa.api.admin.capabilities as capmod

    monkeypatch.setattr(capmod, "_resolved_capabilities", lambda _identity, _state: caps)


def _config_yaml(**source) -> str:
    return yaml.dump(
        {"sources": [{**_CONFIG["sources"][0], **source}], "domains": [], "tables": [], "roles": []}
    )


@pytest.mark.parametrize(
    "document",
    [
        _config_yaml(host=_REF),
        _config_yaml(password=_REF),
        _config_yaml(port=_REF),
        'sources: [{id: pg1, type: postgresql, host: "\\x24{env:%s}"}]\ndomains: []' % _VARIABLE,
    ],
    ids=["host", "password", "a-number-field", "yaml-escaped"],
)
@pytest.mark.parametrize("held_org", [_TENANT, _SANDBOX], ids=["tenant", "sandbox"])
async def test_a_members_import_into_a_held_organization_is_refused_unresolved(
    server, member, monkeypatch, document, held_org
):
    import provisa.api.admin.import_router as ir

    server(True)
    member(held_org)
    _grant(monkeypatch, {"source_registration"})
    with pytest.raises(EnvReferenceRefused) as refused:
        await ir.apply_import(ir.ImportApplyRequest(config_yaml=document), _request())
    assert refused.value.params == {"name": _VARIABLE}
    assert server.read == []


async def test_the_import_asks_the_callers_rights_before_it_resolves_anything(
    server, member, monkeypatch
):
    """In an organization that may reference the environment, a caller who holds one import
    right and not the declared section's is refused with nothing read."""
    import provisa.api.admin.import_router as ir
    from provisa.api.errors import ApiError

    server(False)
    member(_DEPLOYMENT)
    _grant(monkeypatch, {"glossary_rw"})
    with pytest.raises(ApiError) as refused:
        await ir.apply_import(
            ir.ImportApplyRequest(config_yaml=_config_yaml(host=_REF)), _request()
        )
    assert refused.value.status_code == 403
    assert refused.value.code == "import.rights_required"
    assert server.read == []


async def test_an_imports_validation_error_carries_no_resolved_value(server, member, monkeypatch):
    import provisa.api.admin.import_router as ir
    from provisa.api.errors import ApiError

    server(False)
    member(_DEPLOYMENT)
    _grant(monkeypatch, {"source_registration"})
    with pytest.raises(ApiError) as refused:
        await ir.apply_import(
            ir.ImportApplyRequest(config_yaml=_config_yaml(port=_REF)), _request()
        )
    assert refused.value.status_code == 400
    assert server.read  # this organization's reference was resolved, into a field it does not fit
    assert _VALUE not in str(refused.value.detail)
    assert "port" in str(refused.value.detail)


def test_a_refusal_that_quotes_a_resolved_value_names_its_variable_instead(server):
    from provisa.core.config_loader import invalid_text

    refusal = ValueError(f"source pg1: host {_VALUE!r} is not a host name")
    text = invalid_text(refusal, {"sources": [{"host": _REF}]})
    assert _VALUE not in text and f"(the value of {_VARIABLE})" in text


async def test_an_imports_preview_does_not_dial_a_held_organizations_uploaded_sources(
    server, member
):
    from provisa.api.admin.import_typing import type_imported_columns
    from provisa.core.models import ProvisaConfig
    from provisa.import_shared.warnings import WarningCollector

    server(True)
    config = ProvisaConfig.model_validate(
        {
            "sources": [{**_CONFIG["sources"][0], "password": _REF}],
            "domains": [{"id": "d"}],
            "roles": [],
            "tables": [
                {
                    "source_id": "pg1",
                    "domain_id": "d",
                    "schema": "public",
                    "table": "t",
                    "columns": [],
                }
            ],
        }
    )
    member(_SANDBOX)
    dialled: list = []

    class _Pool:
        async def add(self, *fields) -> None:
            dialled.append(fields)

        async def close_all(self) -> None:
            return None

    with pytest.raises(EnvReferenceRefused):
        await type_imported_columns(config, WarningCollector(), pool_factory=_Pool)
    assert dialled == [] and server.read == []


# --- an admin route's body --------------------------------------------------------------------


def _http(path: str, body: bytes, content_type: str = "application/json"):
    from starlette.requests import Request

    async def receive() -> dict:
        return {"type": "http.request", "body": body, "more_body": False}

    scope = {
        "type": "http",
        "method": "POST",
        "path": path,
        "query_string": b"",
        "headers": [(b"content-type", content_type.encode())],
    }
    return Request(scope, receive)


def _Request(path: str, document: object):
    return _http(path, json.dumps(document).encode())


async def test_an_admin_routes_body_is_checked_as_the_route_will_parse_it(server, member):
    from provisa.api.admin.env_reference_check import entered_body

    server(True)
    member(_TENANT)
    escaped = _http("/admin/x", b'{"token": "\\u0024{env:%s}"}' % _VARIABLE.encode())
    for request in (
        _Request("/admin/dbt/preview", {"spec": "cloud", "token": _REF}),
        _Request("/admin/ai-models", {"ai_endpoints": [{"id": "e", "api_key_env": _VARIABLE}]}),
        escaped,
    ):
        with pytest.raises(EnvReferenceRefused):
            await entered_body(request)
    # A query's own text is not an admin object: the data plane is not checked here.
    await entered_body(_Request("/data/sql", {"sql": f"SELECT '{_REF}'"}))
    # A WebSocket's connection has no body to check.
    from starlette.requests import HTTPConnection

    await entered_body(
        HTTPConnection({"type": "websocket", "path": "/admin/graphql", "headers": []})
    )
    assert server.read == []


async def test_the_refusals_answer_is_the_products_coded_error(server):
    from provisa.api.admin.env_reference_check import refused_response

    for refusal, code in (
        (EnvReferenceRefused(_VARIABLE), "secrets.env_reference_refused"),
        (EnvReferenceEditRefused("sources"), "secrets.env_reference_object_locked"),
    ):
        response = refused_response(refusal)
        body = json.loads(bytes(response.body))
        assert response.status_code == 403
        assert body["code"] == code and body["params"] == refusal.params
        assert _VALUE not in body["detail"] and "REQ-" not in body["detail"]


# --- a key variable, at use ---------------------------------------------------------------------


def _own_settings(monkeypatch, settings: dict) -> None:
    from provisa.core import request_context

    monkeypatch.setattr(request_context, "_org_overrides_provider", lambda: settings)


@pytest.mark.parametrize("written", [_VARIABLE, _REF], ids=["bare-name", "reference"])
async def test_a_key_variable_a_held_organizations_own_settings_name_is_not_read(
    server, member, monkeypatch, written
):
    from provisa.llm.client import ProvisaLLMClient

    server(True)
    member(_TENANT)
    endpoint = {
        "id": "mine",
        "style": "openai",
        "base_url": "https://c.example",
        "api_key_env": written,
    }
    _own_settings(monkeypatch, {"ai_endpoints": [endpoint]})
    with pytest.raises(EnvReferenceRefused) as refused:
        await resolve_api_key_field(written)
    assert refused.value.params == {"name": _VARIABLE}
    client = ProvisaLLMClient.__new__(ProvisaLLMClient)
    client._api_keys = {}
    client._endpoints = {"mine": endpoint}
    with pytest.raises(EnvReferenceRefused):
        client._vendor_key_problem("mine")
    with pytest.raises(EnvReferenceRefused):
        client._respond("mine", "m", "p", "", 16)
    assert server.read == []


async def test_the_operators_own_endpoint_is_read_in_a_held_organization(
    server, member, monkeypatch
):
    """The deployment's configuration names it; the organization's own settings do not."""
    server(True)
    member(_SANDBOX)
    _own_settings(monkeypatch, {})
    assert await resolve_api_key_field(_VARIABLE) == _VALUE
    assert await resolve_api_key_field(_REF) == _VALUE


async def test_an_organization_that_is_not_held_reads_its_own_key_variable(
    server, member, monkeypatch
):
    server(False)
    member(_DEPLOYMENT)
    _own_settings(monkeypatch, {"ai_endpoints": [{"id": "mine", "api_key_env": _VARIABLE}]})
    assert await resolve_api_key_field(_VARIABLE) == _VALUE


# --- (g) redaction: a credential field's value, and no other ------------------------------------


def _echo() -> str:
    from provisa.security import redaction

    return redaction.redact(f"connection to {_VALUE} failed for password {_VALUE!r}")


async def test_what_the_environment_gives_a_credential_field_is_kept_out_of_errors_and_logs(
    server, caplog
):
    import logging

    from provisa.security.redaction import redact

    server(False)
    assert resolve_secrets(_REF, credential=True) == _VALUE
    assert _VALUE not in _echo()
    assert f"[REDACTED:auto:env:{_VARIABLE}]" in _echo()
    # An error returned to a caller, and a log line, are redacted by the same redactor.
    with caplog.at_level(logging.ERROR):
        logging.getLogger("provisa.test").error(redact(f"password authentication failed: {_VALUE}"))
    assert _VALUE not in caplog.text


async def test_what_the_environment_gives_any_other_field_stays_readable(server):
    server(False)
    assert resolve_secrets(_REF) == _VALUE  # a host, a port, a database's name
    assert resolve_secrets_in_dict({"host": _REF, "mapping": {"region": _REF}}) == {
        "host": _VALUE,
        "mapping": {"region": _VALUE},
    }
    assert _echo().count(_VALUE) == 2


@pytest.mark.parametrize("field", ["password", "api_key", "clientSecret", "refresh_token"])
async def test_a_structure_resolved_whole_marks_its_credential_fields(server, field):
    server(False)
    assert resolve_secrets_in_dict({"host": "db", "auth": {field: _REF}})["auth"][field] == _VALUE
    assert _VALUE not in _echo()


async def test_an_api_key_field_is_a_credential(server):
    server(False)
    assert await resolve_api_key_field(_VARIABLE) == _VALUE
    assert _VALUE not in _echo()


async def test_a_written_default_is_the_references_own_text_and_is_not_redacted(server):
    from provisa.security import redaction

    server(False)
    written = "${env:PROVISA_TEST_UNSET_VARIABLE:-localhost}"
    assert resolve_secrets(written, credential=True) == "localhost"
    assert redaction.redact("connecting to localhost") == "connecting to localhost"


def test_which_fields_are_credentials():
    for name in ("password", "password_ref", "api_key", "clientSecret", "token", "private_key"):
        assert is_credential_field(name), name
    for name in ("host", "port", "database", "username", "keyspace", "monkey", "tokenizer"):
        assert not is_credential_field(name), name


# --- a tree brought from outside the organization's own history --------------------------------


@pytest.fixture
def repository(tmp_path, monkeypatch):
    from provisa.core.org_files import folder

    monkeypatch.setenv("PROVISA_DATA_DIR", str(tmp_path / "data"))
    for org_id in (_TENANT, _DEPLOYMENT):
        folder(org_id).mkdir(parents=True, exist_ok=True)
    return tmp_path


_STORED = {
    "sales/webhooks/demo.yaml": {"name": "demo", "url": f"https://hooks.example/{_REF}"},
    "sales/webhooks/mine.yaml": {"name": "mine", "url": "https://mine.example/hook"},
}


def _brought(**changed) -> dict:
    return {**_STORED, **changed}


@pytest.mark.parametrize(
    ("tree", "refusal"),
    [
        (
            _brought(
                **{"sales/webhooks/new.yaml": {"name": "new", "url": f"https://c.example/{_REF}"}}
            ),
            EnvReferenceRefused,
        ),
        (
            _brought(
                **{"sales/webhooks/mine.yaml": {"name": "mine", "url": f"https://c.example/{_REF}"}}
            ),
            EnvReferenceRefused,
        ),
        (
            _brought(**{"sales/webhooks/demo.yaml": {"name": "demo", "url": "https://c.example/"}}),
            EnvReferenceEditRefused,
        ),
    ],
    ids=["adds-one", "writes-one-into-theirs", "changes-the-operators"],
)
async def test_a_brought_tree_neither_writes_nor_edits_a_reference(
    server, repository, tree, refusal
):
    from provisa.core.env_deploy import _refuse_brought

    server(True)
    with pytest.raises(refusal):
        _refuse_brought(_TENANT, "0" * 40, tree, _STORED)
    with pytest.raises(refusal):
        _refuse_brought(_TENANT, "files a caller gave", tree, _STORED)
    assert server.read == []


async def test_a_brought_tree_carries_the_operators_objects_unchanged(server, repository):
    from provisa.core.env_deploy import _refuse_brought

    server(True)
    changed = _brought(
        **{"sales/webhooks/mine.yaml": {"name": "mine", "url": "https://o.example/"}}
    )
    _refuse_brought(_TENANT, "0" * 40, changed, _STORED)
    without = {k: v for k, v in _STORED.items() if "demo" not in k}
    _refuse_brought(_TENANT, "0" * 40, without, _STORED)  # deleting is not restricted


async def test_a_tree_of_the_organizations_own_history_loads_as_before(server, repository):
    """An undo, a redo, a deploy of its own commit: a model this control plane stored."""
    from provisa.core.env_deploy import _refuse_brought
    from provisa.core.env_repo import commit_files, ensure_repo, written_here

    server(True)
    repo = ensure_repo(_TENANT)
    first = commit_files(repo, "prod", {"a.yaml": "url: one\n"}, "seed", None)
    second = commit_files(repo, "prod", {"a.yaml": "url: two\n"}, "edit", None)
    assert first and second
    assert written_here(_TENANT, first) and written_here(_TENANT, second)
    assert written_here(_TENANT, "prod")
    assert not written_here(_TENANT, "0" * 40)
    assert not written_here(_TENANT, "origin/prod")
    restored = _brought(**{"sales/webhooks/new.yaml": {"name": "new", "url": _REF}})
    _refuse_brought(_TENANT, first, restored, _STORED)


async def test_an_organization_that_is_not_held_loads_a_brought_tree_as_before(server, repository):
    from provisa.core.env_deploy import _refuse_brought

    server(True)
    tree = _brought(**{"sales/webhooks/new.yaml": {"name": "new", "url": _REF}})
    _refuse_brought(_DEPLOYMENT, "0" * 40, tree, _STORED)
    server(False)
    _refuse_brought(_TENANT, "0" * 40, tree, _STORED)
