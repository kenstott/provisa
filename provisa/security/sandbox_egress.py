# Copyright (c) 2026 Kenneth Stott
# Canary: c3d2f542-7928-4406-80da-cf059f7e689c
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Where a sandbox's sources may point (REQ-2006).

A sandbox's visitor may register a source -- that is the point of a sandbox -- and Provisa then
connects to wherever the source points, from inside the deployment. A destination on a private,
loopback, link-local or otherwise non-public address would therefore be reached from the
deployment's own network: its control-plane database, its cache, a cloud metadata endpoint. A
source of a sandbox org (a demo sandbox, a demo template, or the hosted sandbox org of REQ-1598)
may not resolve to any such address.

One check, :func:`require_public_destination`, is asked at each point a sandbox's source gains or
uses a destination: when it is registered or changed, and when Provisa connects to it -- again,
because the name may resolve differently by then -- including each redirect an HTTP source
follows. It resolves the name and refuses, by name, when ANY address it resolves to is not public.

On a deployment that holds many organizations (``multitenancy: true``, the shared hosted platform),
every org's sources are held to the same check: the deployment's private network carries its
control-plane database, cache and object store, which no org's source may reach. Only a sandbox
org is further held to the address it was checked at (pinning, below); any other org's source is
checked when it is saved and each time Provisa connects. A deployment of one organization (a
dedicated platform) holds only its sandboxes to it: its own org may have private sources.
"""

# Requirements: REQ-2006, REQ-1598

from __future__ import annotations

import ipaddress
import socket
from typing import TYPE_CHECKING
from urllib.parse import urlsplit

from provisa.core.org_ids import is_sandbox_tier_org

if TYPE_CHECKING:
    import httpx

#: The REQ-1598 hosted sandbox org.
_HOSTED_SANDBOX_ORG = "sandbox"


class PrivateDestinationRefused(PermissionError):
    """A held org's source names a destination that is not on the public internet."""

    code = "sources.private_destination"

    def __init__(self, host: str, address: str) -> None:
        super().__init__(
            f"This org's sources may reach only public addresses; {host!r} is {address}, "
            "which is not one"
        )
        self.params = {"host": host, "address": address}


def confined(org_id: str | None) -> bool:
    """Whether ``org_id`` is a sandbox org: its sources are held to public destinations and to the
    address each was checked at, apart from what its demo declares."""
    return org_id is not None and (is_sandbox_tier_org(org_id) or org_id == _HOSTED_SANDBOX_ORG)


def held(org_id: str | None) -> bool:
    """Whether ``org_id``'s sources are held to public destinations: a sandbox org, or any org on a
    deployment that holds many organizations."""
    if org_id is None:
        return False
    if confined(org_id):
        return True
    from provisa.core.secrets_providers import many_organisations

    return many_organisations()


def _public(address: ipaddress.IPv4Address | ipaddress.IPv6Address) -> bool:
    if isinstance(address, ipaddress.IPv6Address) and address.ipv4_mapped is not None:
        address = address.ipv4_mapped
    return address.is_global and not address.is_multicast


def resolve(host: str, port: int | None = None) -> list[str]:
    """Every address ``host`` resolves to now (an IP literal is itself)."""
    try:
        return [str(ipaddress.ip_address(host.strip("[]")))]
    except ValueError:
        pass
    try:
        infos = socket.getaddrinfo(host, port, proto=socket.IPPROTO_TCP)
    except socket.gaierror as exc:
        # A name with no address now cannot be shown to be public, and may be given one later.
        raise PrivateDestinationRefused(host, "a name that does not resolve") from exc
    return sorted({str(info[4][0]) for info in infos})


def require_public_destination(host: str | None, port: int | None = None) -> list[str]:
    """The addresses ``host`` resolves to, refused by name when any is not public. A source with
    no network host (an uploaded file, an embedded store) has nothing to check."""
    if not host:
        return []
    addresses = resolve(host, port)
    for address in addresses:
        if not _public(ipaddress.ip_address(address)):
            raise PrivateDestinationRefused(host, address)
    return addresses


# --- what a sandbox org may reach -------------------------------------------------------------
#
# A demo's own sources are the operator's: its template's configuration names them (the demo's
# shared, read-only database is on the deployment's private network by design). Each sandbox org's
# runtime records them when it is built (app._build_org_runtime), and a destination among them is
# reached whatever its address; every other destination must be public.

_demo_destinations: dict[str, frozenset[tuple[str, int | None]]] = {}

#: A network-host source with no host given is reached at this host (schema_common._add_source_pool).
_DEFAULT_HOST = "localhost"
_URL_FIELDS = ("path", "base_url")


def _plain(value: object) -> str | None:
    """``value`` with any secret reference resolved -- a source may name its host as
    ``${secret:NAME}``, and the address it reaches is the resolved one."""
    if not isinstance(value, str) or not value:
        return None
    from provisa.core.secrets import resolve_secrets

    return str(resolve_secrets(value))


def _host_destination(host: str, port: int | None) -> tuple[str, int | None]:
    """Where a source's ``host`` points: the name and port a URL names (a Qdrant source's host
    is its instance's URL), else the host itself at ``port``."""
    url = _url_destination(host) if "://" in host else None
    return url if url is not None else (host.lower(), port)


def _url_destination(url: str | None) -> tuple[str, int | None] | None:
    if not url:
        return None
    parts = urlsplit(url)
    if parts.scheme.lower() not in _NETWORK_SCHEMES or not parts.hostname:
        return None
    return parts.hostname.lower(), parts.port


_NETWORK_SCHEMES = ("http", "https", "ftp", "sftp", "s3", "gs", "ws", "wss", "postgresql", "mysql")


def destinations(fields: dict) -> list[tuple[str, int | None]]:
    """Every network destination a source names, from its fields: ``host``/``port``, a URL in
    ``path`` or ``base_url``, and any URL among the values of ``federation_hints`` and ``mapping``
    (an object store's endpoint, a crawl's start). A network source given a port and no host is
    reached at localhost, so that is its destination."""
    found: list[tuple[str, int | None]] = []
    host = _plain(fields.get("host"))
    port = fields.get("port") or None
    if host:
        found.append(_host_destination(host, port))
    elif port:
        found.append((_DEFAULT_HOST, port))
    for name in _URL_FIELDS:
        url = _url_destination(_plain(fields.get(name)))
        if url is not None:
            found.append(url)
    for bag in ("federation_hints", "mapping"):
        for value in _strings(fields.get(bag)):
            url = _url_destination(_plain(value))
            if url is not None:
                found.append(url)
    return found


def _strings(node: object) -> list[str]:
    if isinstance(node, str):
        return [node]
    if isinstance(node, dict):
        return [s for v in node.values() for s in _strings(v)]
    if isinstance(node, (list, tuple)):
        return [s for v in node for s in _strings(v)]
    return []


def set_demo_destinations(org_id: str, sources: list[dict]) -> None:
    """Record what the demo of sandbox org ``org_id`` declares, from its configuration's sources."""
    _demo_destinations[org_id] = frozenset(d for s in sources for d in destinations(s))


def forget(org_id: str) -> None:
    _demo_destinations.pop(org_id, None)


def _org(org_id: str | None) -> str | None:
    if org_id is not None:
        return org_id
    from provisa.core.request_context import current_org

    return current_org.get(None)


def require_allowed(host: str, port: int | None, org_id: str | None = None) -> None:
    """Refuse, by name, ``host`` for a held org (:func:`held`) unless its demo declares it or every
    address it resolves to is public. Any other org reaches what it names."""
    org = _org(org_id)
    if not held(org):
        return
    assert org is not None
    if (host.lower(), port) in _demo_destinations.get(org, frozenset()):
        return
    require_public_destination(host, port)


def require_source_allowed(fields: dict, org_id: str | None = None) -> None:
    """:func:`require_allowed` for every destination a source names (:func:`destinations`)."""
    org = _org(org_id)
    if not held(org):
        return
    for host, port in destinations(fields):
        require_allowed(host, port, org)


def source_fields(source: object) -> dict:
    """The address-bearing fields of a registered :class:`provisa.core.models.Source`."""
    return {
        "host": source.host,  # type: ignore[attr-defined]
        "port": source.port,  # type: ignore[attr-defined]
        "path": source.path,  # type: ignore[attr-defined]
        "base_url": source.base_url,  # type: ignore[attr-defined]
        "federation_hints": source.federation_hints,  # type: ignore[attr-defined]
        "mapping": source.mapping,  # type: ignore[attr-defined]
    }


def http_event_hooks(org_id: str | None = None) -> dict:
    """httpx event hooks that check each request a held org's source makes -- every redirect
    is a request of its own -- and send it to the address checked (:func:`_pin_request`). Empty
    for any other org."""
    org = _org(org_id)
    if not held(org):
        return {}

    assert org is not None

    async def _acheck(request: object) -> None:
        _pin_request(request, org)

    return {"request": [_acheck]}


def http_sync_event_hooks(org_id: str | None = None) -> dict:
    """:func:`http_event_hooks` for a synchronous ``httpx.Client``."""
    org = _org(org_id)
    if not held(org):
        return {}

    assert org is not None

    def _check(request: object) -> None:
        _pin_request(request, org)

    return {"request": [_check]}


def checked_get(url: str, *, timeout: float) -> "httpx.Response":
    """``httpx.get(url, follow_redirects=True)``, with every hop of a sandbox org's request
    checked (:func:`http_sync_event_hooks`)."""
    import httpx

    with httpx.Client(
        timeout=timeout, follow_redirects=True, event_hooks=http_sync_event_hooks()
    ) as client:
        return client.get(url)


def pinned_address(server_address: str, org_id: str | None = None) -> tuple[str, str]:
    """A ``host:port`` target (a gRPC channel's) as a sandbox org dials it: ``(target, name)``,
    the target carrying the checked address and the name kept for TLS. Any other org's is
    itself."""
    host, _, port = server_address.rpartition(":")
    if not host:
        host, port = server_address, ""
    name = host.strip("[]")
    address = pin(name, int(port) if port.isdigit() else None, org_id)
    if address == name:
        return server_address, name
    literal = f"[{address}]" if ":" in address else address
    return (f"{literal}:{port}" if port else literal), name


# --- pinning: the address checked is the address connected to ----------------------------------
#
# A name checked and then resolved again by whoever connects could resolve elsewhere by then (DNS
# rebinding). For a sandbox org, every connection is therefore made to the address the check
# resolved: a source's host is replaced by that address before its connection is built, an HTTP
# request is sent to that address with its own name kept for the Host header and for TLS (each
# redirect hop the same), and a gRPC channel dials it with its name kept for TLS. A source whose
# connection reads its address from somewhere that cannot be pinned -- a URL handed to the engine's
# own readers, a service the engine reaches through a child process -- is refused, by name, unless
# its demo declares the destination.

#: Source types whose connection, on every engine, takes its network address from ``host`` alone.
#: (ClickHouse is not one: the DuckDB engine reaches it over HTTP, whose redirects it follows
#: itself.)
PINNABLE_TYPES = frozenset({"postgresql", "mysql", "mariadb", "sqlserver", "oracle", "firebird"})


class UnpinnableDestinationRefused(PermissionError):
    """A sandbox's source would be reached by a connection that resolves its own address."""

    code = "sources.unpinnable_destination"

    def __init__(self, source_type: str, host: str) -> None:
        super().__init__(
            f"A sandbox's {source_type} source cannot be held to the address it was checked "
            f"at, so it may not reach {host!r}"
        )
        self.params = {"type": source_type, "host": host}


def _declared(org: str, host: str, port: int | None) -> bool:
    return (host.lower(), port) in _demo_destinations.get(org, frozenset())


def pin(host: str, port: int | None, org_id: str | None = None) -> str:
    """The address to connect to for ``host``: for a held org, a checked address it resolves
    to (a demo's own destinations are connected to by name); for any other org, ``host``."""
    org = _org(org_id)
    if not held(org):
        return host
    assert org is not None
    if _declared(org, host, port):
        return host
    return require_public_destination(host, port)[0]


def pin_host(host: str, port: int | None, source_type: str, org_id: str | None = None) -> str:
    """The host a source's connection is made to (:func:`pin`), checked first
    (:func:`require_allowed`). A host given as a URL is checked by the name and port it names,
    and connected to by that URL, which cannot be held to the address checked: for a sandbox org
    it may reach only a destination its demo declares."""
    if "://" not in host:
        require_allowed(host, port, org_id)
        return pin(host, port, org_id)
    name, url_port = _host_destination(host, port)
    require_allowed(name, url_port, org_id)
    org = _org(org_id)
    if not confined(org):
        return host
    assert org is not None
    if not _declared(org, name, url_port):
        raise UnpinnableDestinationRefused(source_type, name)
    return host


def require_connectable(fields: dict, source_type: str, org_id: str | None = None) -> str | None:
    """Refuse, by name, a held org's source (:func:`held`) that names a destination its demo does
    not declare and that is not public, or a sandbox org's that cannot be held to the address it
    is checked at; the checked address its host is to be connected at, when there is one to pin.
    None for any other org's source, and for one whose destinations are all its demo's own."""
    org = _org(org_id)
    if not held(org):
        return None
    assert org is not None
    require_source_allowed(fields, org)
    undeclared = [d for d in destinations(fields) if not _declared(org, *d)]
    if not undeclared:
        return None
    host_only = destinations({"host": fields.get("host"), "port": fields.get("port")})
    if source_type not in PINNABLE_TYPES or undeclared != host_only:
        if not confined(org):
            # Not a sandbox: checked here and at each connection, not pinned (module docstring).
            return None
        raise UnpinnableDestinationRefused(source_type, undeclared[0][0])
    host, port = host_only[0]
    return pin(host, port, org)


def pinned_source(source: object, org_id: str | None = None) -> object:
    """``source`` as a sandbox org connects to it: its host replaced by its checked address
    (:func:`require_connectable`). Any other org's source is itself."""
    org = _org(org_id)
    if not held(org):
        return source
    source_type = str(getattr(source.type, "value", source.type))  # type: ignore[attr-defined]
    address = require_connectable(source_fields(source), source_type, org)
    if address is None:
        return source
    return source.model_copy(update={"host": address})  # type: ignore[attr-defined]


def _pin_request(request: object, org: str) -> None:
    """Send ``request`` to its host's checked address, keeping the name for Host and TLS."""
    url = request.url  # type: ignore[attr-defined]
    host, port = url.host, url.port
    address = pin(host, port, org)
    if address == host:
        return
    request.headers["Host"] = url.netloc.decode("ascii")  # type: ignore[attr-defined]
    request.extensions["sni_hostname"] = host  # type: ignore[attr-defined]
    request.url = url.copy_with(host=address)  # type: ignore[attr-defined]
