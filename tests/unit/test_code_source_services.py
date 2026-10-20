# Copyright (c) 2026 Kenneth Stott
# Canary: 3c9c4cfe-5490-4aec-88e1-297791c20a7b
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A code source's declared secrets, read through Provisa's services (REQ-2104): kept as
references through config load, scoped to the source's worker by a token issued as it starts and
revoked when it stops, answered by the egress proxy at ``$PROVISA_SERVICES/secrets/<name>``,
resolved in the org's vault at each read, audited, and refused by name. A value is never in a log
or a refusal."""

# Requirements: REQ-2104

from __future__ import annotations

import logging
import os
import urllib.error
import urllib.request
from pathlib import Path

import pytest

from provisa.codesource import services
from provisa.codesource.bundle import BundleRefused
from provisa.codesource.egress import EgressProxy
from provisa.core import secrets_store

pytestmark = pytest.mark.unit

_VALUE = "sk-or-v1-not-a-real-key"


@pytest.fixture
def vault(monkeypatch: pytest.MonkeyPatch) -> dict:
    """The org vault the grant's references resolve in, and the reads audited."""
    held: dict = {"values": {"openrouter": _VALUE}, "audited": []}

    async def decrypted(admin_db, org_id, owner_id):
        return dict(held["values"]) if org_id == "acme" else {}

    async def audit(grant, name, outcome):
        held["audited"].append((grant.source_id, name, outcome))

    monkeypatch.setattr(secrets_store, "_decrypted", decrypted)
    monkeypatch.setattr(services, "_audit", audit)
    return held


def _grant(**secrets: str) -> services.ServiceGrant:
    return services.ServiceGrant(org_id="acme", source_id="rates", secrets=secrets, admin_db=None)


@pytest.fixture
def token():
    issued = services.issue(_grant(openrouter="${secret:openrouter}", missing="${secret:nope}"))
    yield issued
    services.revoke(issued)


# -- declaring --------------------------------------------------------------------------------


def test_a_name_is_the_org_secret_of_that_name_and_a_mapping_names_its_references():
    assert services.declared(None) == {}
    assert services.declared(["openrouter"]) == {"openrouter": "${secret:openrouter}"}
    assert services.declared({"key": "${env:OPENROUTER_API_KEY}"}) == {
        "key": "${env:OPENROUTER_API_KEY}"
    }


@pytest.mark.parametrize("raw", [[""], [3], {"key": "sk-literal"}, {"key": "${user:x}"}, "name"])
def test_anything_but_a_name_or_a_reference_is_refused(raw):
    with pytest.raises(BundleRefused) as refused:
        services.declared(raw)
    assert refused.value.code == "codesource.secrets_invalid"
    assert "sk-literal" not in str(refused.value)


def test_a_config_naming_a_secret_nobody_has_stored_loads_with_the_references_as_written(
    monkeypatch,
):
    from provisa.core.config_loader import parse_config_dict

    monkeypatch.delenv("PROVISA_TEST_UNSET_KEY", raising=False)
    written = {"openrouter": "${secret:openrouter}", "other": "${env:PROVISA_TEST_UNSET_KEY}"}
    config = parse_config_dict(
        {
            "sources": [
                {
                    "id": "rates",
                    "type": "python",
                    "mapping": {
                        "code": {"kind": "pypi", "package": "acme-rates", "version": "1.0.0"},
                        "secrets": written,
                    },
                }
            ],
            "domains": [],
            "tables": [],
            "roles": [],
        }
    )
    assert config.sources[0].mapping["secrets"] == written
    assert config.written.sources[0].mapping["secrets"] == written


# -- reading ----------------------------------------------------------------------------------


def test_a_declared_secret_is_read_from_the_orgs_vault_and_audited(vault, token):
    assert services.answer("GET", f"/{token}/secrets/openrouter") == (200, _VALUE.encode())
    assert vault["audited"] == [("rates", "openrouter", "read")]


def test_a_rotated_secret_is_read_on_the_next_fetch(vault, token):
    services.answer("GET", f"/{token}/secrets/openrouter")
    vault["values"]["openrouter"] = "sk-or-v1-rotated"
    assert services.answer("GET", f"/{token}/secrets/openrouter") == (200, b"sk-or-v1-rotated")


def test_an_undeclared_name_is_refused_by_name_and_audited(vault, token):
    status, body = services.answer("GET", f"/{token}/secrets/stripe")
    assert status == 403
    assert "'stripe'" in body.decode() and "'rates'" in body.decode()
    assert vault["audited"] == [("rates", "stripe", "undeclared")]


def test_an_unresolvable_secret_is_refused_by_name_never_by_value(vault, token, caplog):
    caplog.set_level(logging.DEBUG)
    status, body = services.answer("GET", f"/{token}/secrets/missing")
    assert status == 404
    assert "'missing'" in body.decode() and "${secret:nope}" in body.decode()
    assert vault["audited"] == [("rates", "missing", "unresolved")]
    services.answer("GET", f"/{token}/secrets/openrouter")
    assert _VALUE not in caplog.text


def test_an_env_reference_resolves_from_the_deployments_environment(vault, monkeypatch):
    monkeypatch.setenv("PROVISA_TEST_CODE_KEY", "from-env")
    issued = services.issue(_grant(key="${env:PROVISA_TEST_CODE_KEY}"))
    try:
        assert services.answer("GET", f"/{issued}/secrets/key") == (200, b"from-env")
    finally:
        services.revoke(issued)


def test_a_revoked_or_unknown_token_reads_nothing(vault):
    issued = services.issue(_grant(openrouter="${secret:openrouter}"))
    services.revoke(issued)
    for path in (f"/{issued}/secrets/openrouter", "/not-a-token/secrets/openrouter"):
        status, body = services.answer("GET", path)
        assert status == 401 and _VALUE.encode() not in body
    assert vault["audited"] == []


def test_another_service_or_method_is_refused_by_name(vault, token):
    assert services.answer("GET", f"/{token}/cache/x")[0] == 404
    assert services.answer("POST", f"/{token}/secrets/openrouter")[0] == 405


# -- through the egress proxy -----------------------------------------------------------------


def _through(proxy: EgressProxy, url: str) -> tuple[int, str]:
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({"http": proxy.url}))
    try:
        with opener.open(url, timeout=10) as resp:
            return resp.status, resp.read().decode()
    except urllib.error.HTTPError as refused:
        return refused.code, refused.read().decode()


def test_the_egress_proxy_answers_the_services_address_itself(vault, token, caplog):
    caplog.set_level(logging.DEBUG)
    proxy = EgressProxy(allow=[])  # nothing on the allow-list: the services are not egress
    try:
        base = services.address(token)
        assert _through(proxy, f"{base}/secrets/openrouter") == (200, _VALUE)
        status, body = _through(proxy, f"{base}/secrets/stripe")
        assert status == 403 and "'stripe'" in body
        # Provisa's own ports are still never reached.
        assert _through(proxy, "http://127.0.0.1:1/")[0] == 403
    finally:
        proxy.stop()
    assert _VALUE not in caplog.text


# -- the worker's token -----------------------------------------------------------------------


def test_a_worker_gets_only_the_address_and_its_token_is_revoked_when_it_ends(
    vault, monkeypatch, tmp_path: Path
):
    from provisa.codesource import worker as worker_mod

    started: dict = {}

    class Popen:
        def __init__(self, argv, *, env, **_):
            started["env"] = env
            self.stderr = iter(())
            self.returncode = None

        def poll(self):
            return None

    monkeypatch.setattr(worker_mod.subprocess, "Popen", Popen)
    command = worker_mod.HostCommand(
        argv=lambda address: ["host", address],
        env={"PATH": os.environ.get("PATH", "")},
        cwd=tmp_path,
        services=_grant(openrouter="${secret:openrouter}"),
    )
    w = worker_mod.Worker("rates", "hash", command)
    w._spawn("fd:3", ())  # noqa: SLF001 -- the start's spawn alone: no host to say hello
    env = started["env"]
    base = env[services.SERVICES_ENV]
    assert base.startswith(f"http://{services.SERVICES_HOST}/")
    assert _VALUE not in "".join(env.values())  # nothing is resolved as the worker starts
    assert vault["audited"] == []
    token = base.rsplit("/", 1)[1]
    assert services.answer("GET", f"/{token}/secrets/openrouter")[0] == 200
    w._end("the worker was stopped")  # noqa: SLF001
    assert services.answer("GET", f"/{token}/secrets/openrouter")[0] == 401


def test_one_orgs_worker_never_serves_another_orgs_calls(monkeypatch, tmp_path: Path):
    """Each worker holds its org's grant, so the pool keeps one per org for the same code."""
    from provisa.codesource import worker as worker_mod

    monkeypatch.setattr(worker_mod.Worker, "start", lambda self, timeout=0: None)
    monkeypatch.setattr(worker_mod.Worker, "alive", lambda self: True)
    pool = worker_mod.WorkerPool()
    command = worker_mod.HostCommand(argv=lambda a: [a], env={}, cwd=tmp_path)
    a = pool.get("rates", "hash", lambda: command, org="acme")
    b = pool.get("rates", "hash", lambda: command, org="globex")
    assert a is not b
    assert pool.get("rates", "hash", lambda: command, org="acme") is a
