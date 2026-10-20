# Copyright (c) 2026 Kenneth Stott
# Canary: 8fa916a5-cdd1-4291-a104-1e423ce281e5
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The egress proxy a sandboxed code source reaches the network through (REQ-1984, REQ-1993).

An operating system's sandbox filters connections by address, never by host name, and the egress
allow-list (REQ-885, ``udf.egress_allowlist``) names hosts. So a sandboxed worker may connect only
to its socket and to this proxy, and the proxy admits a connection only to a host on the
allow-list: an HTTPS ``CONNECT host:port`` tunnel, or a plain HTTP request in absolute form.
Anything else is answered 403, naming the host. Loopback is never reached, whatever the list says
and whatever a name resolves to: Provisa's own ports are not a code source's to call.

One host is answered by the proxy itself, never connected to: Provisa's services to code
(``provisa-services``, REQ-2104, provisa/codesource/services.py), a plain HTTP request.

The runtime is pointed at the proxy with ``HTTP_PROXY`` and ``HTTPS_PROXY`` (and Node 24's
``NODE_USE_ENV_PROXY=1``, without which its built-in fetch ignores them).
"""

# Requirements: REQ-1984, REQ-1993, REQ-885, REQ-2104
from __future__ import annotations

import ipaddress
import logging
import socket
import socketserver
import threading
from collections.abc import Iterable

from provisa.codesource import services

log = logging.getLogger(__name__)

#: How long a proxied connection may wait to be accepted by its upstream.
CONNECT_TIMEOUT_S = 30.0

_MAX_HEAD = 64 * 1024


def _host_port(target: str, default_port: int) -> tuple[str, int]:
    if target.startswith("["):  # [v6]:port
        host, _, rest = target[1:].partition("]")
        return host, int(rest[1:]) if rest.startswith(":") else default_port
    host, sep, port = target.rpartition(":")
    if sep and port.isdigit():
        return host, int(port)
    return target, default_port


def _loopback(host: str) -> bool:
    if host.lower() in ("localhost", "localhost.localdomain"):
        return True
    try:
        return ipaddress.ip_address(host).is_loopback
    except ValueError:
        return False


class EgressProxy:
    """One proxy on 127.0.0.1, on a port of its own, for the process's sandboxed workers."""

    def __init__(self, allow: Iterable[str]) -> None:
        self.allow = frozenset(a.strip().lower() for a in allow if a.strip())
        proxy = self

        class _Handler(socketserver.BaseRequestHandler):
            def handle(self) -> None:
                proxy._serve(self.request)

        class _Server(socketserver.ThreadingTCPServer):
            daemon_threads = True
            allow_reuse_address = True

        self._server = _Server(("127.0.0.1", 0), _Handler)
        self.port: int = self._server.server_address[1]
        self._thread = threading.Thread(
            target=self._server.serve_forever, name="code-egress-proxy", daemon=True
        )
        self._thread.start()

    @property
    def url(self) -> str:
        return f"http://127.0.0.1:{self.port}"

    def environment(self) -> dict[str, str]:
        """What a runtime is given to send its requests through the proxy."""
        return {
            "HTTP_PROXY": self.url,
            "HTTPS_PROXY": self.url,
            "http_proxy": self.url,
            "https_proxy": self.url,
            "NODE_USE_ENV_PROXY": "1",
        }

    def stop(self) -> None:
        self._server.shutdown()
        self._server.server_close()

    def admits(self, host: str, port: int) -> bool:
        """Whether the allow-list names ``host`` (or ``host:port``), and it is not loopback."""
        host = host.lower()
        if _loopback(host):
            return False
        return host in self.allow or f"{host}:{port}" in self.allow

    # -- one connection -------------------------------------------------------------------------

    def _serve(self, client: socket.socket) -> None:
        head = self._read_head(client)
        if head is None:
            return
        request_line = head.split(b"\r\n", 1)[0].decode("latin-1")
        parts = request_line.split(" ")
        if len(parts) != 3:
            self._refuse(client, 400, "a request the proxy cannot read")
            return
        method, target, version = parts
        if method == "CONNECT":
            host, port = _host_port(target, 443)
            upstream = self._open(client, host, port)
            if upstream is None:
                return
            client.sendall(f"{version} 200 Connection Established\r\n\r\n".encode())
            self._pipe(client, upstream, b"")
            return
        if not target.startswith("http://"):
            self._refuse(client, 400, f"{target} is not an absolute http:// address")
            return
        authority, _, path = target[len("http://") :].partition("/")
        host, port = _host_port(authority, 80)
        if host.lower() == services.SERVICES_HOST:
            self._services(client, method, f"/{path}")
            return
        upstream = self._open(client, host, port)
        if upstream is None:
            return
        rest = head.split(b"\r\n", 1)[1] if b"\r\n" in head else b""
        first = f"{method} /{path} {version}\r\n".encode("latin-1")
        self._pipe(client, upstream, first + rest)

    def _read_head(self, client: socket.socket) -> bytes | None:
        """The request's head, through the blank line, and any body bytes read with it."""
        data = b""
        while b"\r\n\r\n" not in data:
            chunk = client.recv(4096)
            if not chunk:
                return None
            data += chunk
            if len(data) > _MAX_HEAD:
                self._refuse(client, 431, "a request head larger than the proxy reads")
                return None
        return data

    def _open(self, client: socket.socket, host: str, port: int) -> socket.socket | None:
        if not self.admits(host, port):
            self._refuse(client, 403, f"egress to {host}:{port} is not on the egress allow-list")
            return None
        try:
            infos = socket.getaddrinfo(host, port, type=socket.SOCK_STREAM)
        except OSError as exc:
            self._refuse(client, 502, f"{host} does not resolve: {exc}")
            return None
        for family, kind, proto, _, address in infos:
            if ipaddress.ip_address(address[0]).is_loopback:
                self._refuse(client, 403, f"{host} resolves to loopback, which is never reached")
                return None
            upstream = socket.socket(family, kind, proto)
            upstream.settimeout(CONNECT_TIMEOUT_S)
            try:
                upstream.connect(address)
            except OSError:
                upstream.close()
                continue
            upstream.settimeout(None)
            return upstream
        self._refuse(client, 502, f"{host}:{port} did not accept the connection")
        return None

    def _services(self, client: socket.socket, method: str, path: str) -> None:
        """Answer a request to Provisa's services: a value or a refusal, never forwarded."""
        status, body = services.answer(method, path)
        if status != 200:
            log.info("code-source services refused: %s", body.decode())
        try:
            client.sendall(
                f"HTTP/1.1 {status} {'OK' if status == 200 else 'Refused'}\r\n"
                f"Content-Type: text/plain; charset=utf-8\r\nContent-Length: {len(body)}\r\n"
                "Cache-Control: no-store\r\nConnection: close\r\n\r\n".encode()
                + body
            )
        except OSError:
            pass  # the client has gone: there is no one to answer

    def _refuse(self, client: socket.socket, status: int, reason: str) -> None:
        log.info("code-source egress refused: %s", reason)
        body = reason.encode()
        try:
            client.sendall(
                f"HTTP/1.1 {status} Refused\r\nContent-Type: text/plain\r\n"
                f"Content-Length: {len(body)}\r\nConnection: close\r\n\r\n".encode()
                + body
            )
        except OSError:
            pass  # the client has gone: there is no one to tell

    @staticmethod
    def _pipe(client: socket.socket, upstream: socket.socket, first: bytes) -> None:
        def _one_way(src: socket.socket, dst: socket.socket) -> None:
            try:
                while chunk := src.recv(65536):
                    dst.sendall(chunk)
            except OSError:
                pass  # either side closed: the tunnel is over
            finally:
                try:
                    dst.shutdown(socket.SHUT_WR)
                except OSError:
                    pass  # already closed

        if first:
            upstream.sendall(first)
        back = threading.Thread(target=_one_way, args=(upstream, client), daemon=True)
        back.start()
        _one_way(client, upstream)
        back.join()
        upstream.close()
