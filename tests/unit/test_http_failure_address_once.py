# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A message built through ``describe_http_failure`` names the request's address once.

``describe_http_failure`` already names the method and URL, so a call site that also names the
URL ("could not fetch {url}: ...") says it twice. The structure test reads every call site; the
sites that once named the address themselves are each driven to the failure here.
"""

from __future__ import annotations

import ast
import re
from pathlib import Path

import httpx
import pytest

_ROOT = Path(__file__).resolve().parents[2] / "provisa"
_ADDRESS = re.compile(r"\b(url|uri|host|port|endpoint|address|base_url)\b|_url\b")

#: Call sites whose result is handed to another function that builds the message; each is
#: driven to the failure below, so the message is checked whole.
_PASSED_ON = {
    ("provisa/kafka/avro_registry.py", "_get"): "test_avro_registry_names_the_address_once",
}


def _parents(tree: ast.AST) -> dict[ast.AST, ast.AST]:
    return {child: node for node in ast.walk(tree) for child in ast.iter_child_nodes(node)}


def _enclosing(
    node: ast.AST, parents: dict[ast.AST, ast.AST], kind: type | tuple[type, ...]
) -> ast.AST | None:
    current: ast.AST | None = parents.get(node)
    while current is not None and not isinstance(current, kind):
        current = parents.get(current)
    return current


def _other_parts(joined: ast.JoinedStr, exclude: ast.AST) -> list[str]:
    """The source of every interpolation in ``joined`` except the one holding ``exclude``."""
    parts = []
    for value in joined.values:
        if isinstance(value, ast.FormattedValue) and exclude not in ast.walk(value):
            parts.append(ast.unparse(value.value))
    return parts


def _call_sites() -> list[tuple[str, ast.Call, dict[ast.AST, ast.AST]]]:
    sites = []
    for path in sorted(_ROOT.rglob("*.py")):
        rel = path.relative_to(_ROOT.parent).as_posix()
        if rel == "provisa/core/http_failure.py":
            continue
        source = path.read_text()
        if "describe_http_failure(" not in source:
            continue
        tree = ast.parse(source)
        parents = _parents(tree)
        for node in ast.walk(tree):
            if (
                isinstance(node, ast.Call)
                and isinstance(node.func, ast.Name)
                and node.func.id == "describe_http_failure"
            ):
                sites.append((rel, node, parents))
    return sites


def _messages_holding(
    call: ast.Call, parents: dict[ast.AST, ast.AST]
) -> list[tuple[ast.JoinedStr, ast.AST]] | None:
    """Each f-string the call's text lands in, with the node standing for it there; None when
    the text is passed on to another function."""
    joined = _enclosing(call, parents, ast.JoinedStr)
    if isinstance(joined, ast.JoinedStr):
        return [(joined, call)]
    parent = parents[call]
    if isinstance(parent, ast.IfExp):
        parent = parents[parent]
    if isinstance(parent, ast.Assign) and isinstance(parent.targets[0], ast.Name):
        name = parent.targets[0].id
        func = _enclosing(call, parents, (ast.FunctionDef, ast.AsyncFunctionDef))
        assert func is not None
        found: list[tuple[ast.JoinedStr, ast.AST]] = []
        for node in ast.walk(func):
            if isinstance(node, ast.JoinedStr):
                for value in node.values:
                    if isinstance(value, ast.FormattedValue):
                        for ref in ast.walk(value):
                            if isinstance(ref, ast.Name) and ref.id == name:
                                found.append((node, ref))
        return found
    if isinstance(parent, ast.keyword) or (isinstance(parent, ast.Call) and call in parent.args):
        call_parent = parents[parent] if isinstance(parent, ast.keyword) else parent
        if (
            isinstance(call_parent, ast.Call)
            and isinstance(call_parent.func, ast.Name)
            and (call_parent.func.id in {"AssetError", "QueryPreviewType"})
        ):
            return []
        return None
    return None


def test_every_call_site_names_the_address_once():
    sites = _call_sites()
    assert len(sites) > 20, "the call sites moved; the walk no longer finds them"
    doubled = []
    for rel, call, parents in sites:
        func = _enclosing(call, parents, (ast.FunctionDef, ast.AsyncFunctionDef))
        func_name = getattr(func, "name", "<module>")
        messages = _messages_holding(call, parents)
        if messages is None:
            assert (rel, func_name) in _PASSED_ON, (
                f"{rel}:{call.lineno} ({func_name}) hands describe_http_failure's text to another "
                "function; drive that message to the failure in this file and list it in _PASSED_ON"
            )
            continue
        for joined, holder in messages:
            for part in _other_parts(joined, holder):
                if _ADDRESS.search(part):
                    doubled.append(f"{rel}:{joined.lineno} ({func_name}) also names {{{part}}}")
    assert doubled == []


# -- the sites that once named the address themselves, driven to the failure ----------------------

_HOOK = "https://hooks.example.com/requests"


def _occurrences(message: str, address: str) -> int:
    return message.count(address)


class _Failing(httpx.AsyncClient):
    """An AsyncClient whose every request fails as a refused connection does."""

    def __init__(self, *args, **kwargs):
        kwargs.pop("transport", None)

        def refuse(request: httpx.Request) -> httpx.Response:
            raise httpx.ConnectError("", request=request)

        super().__init__(*args, transport=httpx.MockTransport(refuse), **kwargs)


@pytest.mark.asyncio
async def test_request_hook_names_the_address_once(monkeypatch):
    from provisa.core import offer_hooks

    async def secret(_target):
        return "s3cret"

    monkeypatch.setattr(offer_hooks, "resolved_secret", secret)
    monkeypatch.setattr(httpx, "AsyncClient", _Failing)
    with pytest.raises(offer_hooks.HookFailed) as failed:
        await offer_hooks.deliver(offer_hooks.HookTarget(url=_HOOK, secret="ref"), {"id": 1})
    assert str(failed.value) == (
        f"the request hook could not be reached: could not connect to POST {_HOOK}"
    )


def _timed_out_get(url: str, **_kw) -> httpx.Response:
    raise httpx.ReadTimeout("", request=httpx.Request("GET", url))


def test_runtime_download_names_the_address_once(monkeypatch):
    from provisa.runtime_deps import code_runtimes

    url = "https://dl.example.com/runtime.tar.gz"
    monkeypatch.setattr(httpx, "get", _timed_out_get)
    with pytest.raises(code_runtimes.RuntimeUnavailable) as failed:
        code_runtimes.fetch_https(url)
    assert str(failed.value) == (
        f"failed to download: timed out waiting for a response from GET {url}"
    )


def test_bundle_fetch_names_the_address_once(monkeypatch):
    from provisa.codesource.bundle import BundleRefused
    from provisa.codesource.install import http_get

    url = "https://registry.example.com/pkg.tgz"
    monkeypatch.setattr(httpx, "get", _timed_out_get)
    with pytest.raises(BundleRefused) as refused:
        http_get(url, {})
    assert str(refused.value) == (
        f"could not fetch: timed out waiting for a response from GET {url}"
    )
    assert refused.value.code == "codesource.fetch_failed"


@pytest.mark.asyncio
async def test_reference_library_fetch_names_the_address_once(monkeypatch):
    from provisa.reference_library import library

    url = "https://docs.example.com/glossary.html"
    monkeypatch.setattr(httpx, "AsyncClient", _Failing)
    with pytest.raises(library.FetchFailed) as failed:
        await library.fetch(url, None)
    assert str(failed.value) == f"could not be fetched: could not connect to GET {url}"


@pytest.mark.asyncio
async def test_avro_registry_names_the_address_once():
    from provisa.kafka.avro_registry import RegistrySettings, SchemaRegistry, SchemaRegistryRefusal

    registry = SchemaRegistry(RegistrySettings(url="http://registry:8081"))
    registry._client = _Failing()  # noqa: SLF001
    with pytest.raises(SchemaRegistryRefusal) as refused:
        await registry.schema(1)
    message = str(refused.value)
    assert message == (
        "Schema registry cannot be reached: could not connect to GET "
        "http://registry:8081/schemas/ids/1"
    )
    assert _occurrences(message, "registry:8081") == 1
    assert refused.value.params == {
        "error": message.removeprefix("Schema registry cannot be reached: ")
    }


@pytest.mark.asyncio
async def test_avro_registry_os_error_names_the_address_once():
    from provisa.kafka.avro_registry import RegistrySettings, SchemaRegistry, SchemaRegistryRefusal

    class _OsFailing(httpx.AsyncClient):
        async def get(self, url, **_kw):
            raise OSError(65, "No route to host")

    registry = SchemaRegistry(RegistrySettings(url="http://registry:8081"))
    registry._client = _OsFailing()  # noqa: SLF001
    with pytest.raises(SchemaRegistryRefusal) as refused:
        await registry.schema(1)
    message = str(refused.value)
    assert message == (
        "Schema registry cannot be reached: OSError reaching GET "
        "http://registry:8081/schemas/ids/1: [Errno 65] No route to host"
    )


@pytest.mark.asyncio
async def test_coordinator_readiness_names_the_address_once(monkeypatch):
    from provisa.federation import isolated_provisioner as prov

    monkeypatch.setattr(httpx, "AsyncClient", _Failing)
    with pytest.raises(prov.IsolatedProvisioningError) as failed:
        await prov._wait_until_ready("provisa-trino-acme", 8080, timeout=0.01)  # noqa: SLF001
    message = str(failed.value)
    assert message == (
        "dedicated coordinator did not become ready within 0s "
        "(could not connect to GET http://provisa-trino-acme:8080/v1/info)"
    )


@pytest.mark.asyncio
async def test_coordinator_not_ready_answer_names_the_address(monkeypatch):
    from provisa.federation import isolated_provisioner as prov

    class _Starting(httpx.AsyncClient):
        def __init__(self, *args, **kwargs):
            kwargs.pop("transport", None)
            super().__init__(
                *args,
                transport=httpx.MockTransport(lambda _r: httpx.Response(503)),
                **kwargs,
            )

    monkeypatch.setattr(httpx, "AsyncClient", _Starting)
    with pytest.raises(prov.IsolatedProvisioningError) as failed:
        await prov._wait_until_ready("provisa-trino-acme", 8080, timeout=0.01)  # noqa: SLF001
    assert str(failed.value) == (
        "dedicated coordinator did not become ready within 0s "
        "(GET http://provisa-trino-acme:8080/v1/info answered HTTP 503)"
    )
