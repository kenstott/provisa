# Copyright (c) 2026 Kenneth Stott
# Canary: dbf25bad-9751-4b25-9522-7a87a8a4dcb3
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A registry of a test's own for demo bundles (REQ-2049).

The calls of the OCI distribution API a bundle's push and pull make, served in this process on a
leased loopback port: manifests by tag (GET, HEAD, PUT), monolithic blob uploads (POST then PUT
with the digest) and blobs by digest. It asks for no credentials; a push still passes the
publisher's (scripts/demo_bundle.py push requires them), which it does not read. Publishing to the
real registry, ghcr.io, is demo-bundles.yml's."""

from __future__ import annotations

import hashlib
import os
import subprocess
import sys
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse

_SCRIPT = Path(__file__).resolve().parents[2] / "scripts" / "demo_bundle.py"
_SYNC = Path(__file__).resolve().parents[2] / "scripts" / "demo_stores_sync.sh"
_STORES = Path(__file__).resolve().parents[2] / "scripts" / "demo_stores.py"
_SHARED = (
    Path(__file__).resolve().parents[2]
    / "packaging"
    / "linux"
    / "demo-stores"
    / "docker-compose.shared.yml"
)
_PREFIX = "/v2/test/provisa-demos/"


class _Store:
    def __init__(self) -> None:
        self.blobs: dict[str, bytes] = {}
        self.manifests: dict[str, bytes] = {}
        self.uploads = 0
        self.lock = threading.Lock()


def _handler(store: _Store) -> type[BaseHTTPRequestHandler]:
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, format: str, *args: object) -> None:  # noqa: A002 - the base name
            pass

        def _send(self, code: int, body: bytes = b"", headers: dict | None = None) -> None:
            self.send_response(code)
            for key, value in (headers or {}).items():
                self.send_header(key, value)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            if self.command != "HEAD":
                self.wfile.write(body)

        def _body(self) -> bytes:
            return self.rfile.read(int(self.headers.get("Content-Length", "0")))

        def _manifest(self, tag: str) -> None:
            body = store.manifests.get(tag)
            if body is None:
                self._send(404)
                return
            digest = "sha256:" + hashlib.sha256(body).hexdigest()
            self._send(200, body, {"Docker-Content-Digest": digest})

        def do_HEAD(self) -> None:  # noqa: N802 - http.server's name
            path = urlparse(self.path).path
            self._manifest(path[len(_PREFIX + "manifests/") :])

        def do_GET(self) -> None:  # noqa: N802
            path = urlparse(self.path).path
            if path == "/v2/":
                self._send(200, b"{}")
            elif path.startswith(_PREFIX + "manifests/"):
                self._manifest(path[len(_PREFIX + "manifests/") :])
            elif path.startswith(_PREFIX + "blobs/"):
                blob = store.blobs.get(path[len(_PREFIX + "blobs/") :])
                self._send(404) if blob is None else self._send(200, blob)
            else:
                self._send(404)

        def do_POST(self) -> None:  # noqa: N802
            if urlparse(self.path).path != _PREFIX + "blobs/uploads/":
                self._send(404)
                return
            with store.lock:
                store.uploads += 1
                number = store.uploads
            self._send(202, headers={"Location": f"/upload/{number}"})

        def do_PUT(self) -> None:  # noqa: N802
            url = urlparse(self.path)
            body = self._body()
            if url.path.startswith("/upload/"):
                digest = parse_qs(url.query)["digest"][0]
                if digest != "sha256:" + hashlib.sha256(body).hexdigest():
                    self._send(400)
                    return
                store.blobs[digest] = body
                self._send(201)
            elif url.path.startswith(_PREFIX + "manifests/"):
                store.manifests[url.path[len(_PREFIX + "manifests/") :]] = body
                self._send(201)
            else:
                self._send(404)

    return Handler


class DemoRegistry:
    def __init__(self) -> None:
        from tests.port_lease import lease_ports

        (self.port,) = lease_ports(1)
        self.url = f"http://127.0.0.1:{self.port}/test"
        self.store = _Store()
        self._server = ThreadingHTTPServer(("127.0.0.1", self.port), _handler(self.store))
        self._thread = threading.Thread(target=self._server.serve_forever, daemon=True)
        # The node's shared stores, a compose project of this registry's own (the node's is
        # provisa-demo-stores); taken down with their volumes at stop.
        self.stores_project = f"provisa-demo-stores-{self.port}"
        self._bundles: Path | None = None

    def start(self) -> None:
        self._thread.start()

    def stop(self) -> None:
        self._server.shutdown()
        self._server.server_close()
        if self._bundles is not None:
            subprocess.run(
                ["docker", "compose", "-p", self.stores_project, "-f", str(_SHARED)]
                + ["down", "-v", "--remove-orphans"],
                env={**os.environ, "DEMO_BUNDLES_DIR": str(self._bundles)},
                capture_output=True,
                timeout=300,
                check=False,
            )

    def push(self, bundle: Path) -> str:
        done = subprocess.run(
            [sys.executable, str(_SCRIPT), "push", str(bundle), "--registry", self.url],
            capture_output=True,
            text=True,
            timeout=300,
            env={
                **os.environ,
                "PROVISA_DEMO_REGISTRY_USER": "test",
                "PROVISA_DEMO_REGISTRY_TOKEN": "unused-by-this-registry",
            },
        )
        assert done.returncode == 0, (done.stdout, done.stderr)
        return done.stdout

    def pull(self, spec: str, into: Path) -> subprocess.CompletedProcess:
        return subprocess.run(
            [
                sys.executable,
                str(_SCRIPT),
                "pull",
                spec,
                "--into",
                str(into),
                "--registry",
                self.url,
            ],
            capture_output=True,
            text=True,
            timeout=300,
        )

    def sync(
        self, demos: str, bundles: Path, *, requested: bool = True
    ) -> subprocess.CompletedProcess:
        """The node's sync (scripts/demo_stores_sync.sh) for ``demos`` into ``bundles``, run as
        the node runs it, its stores named by 127.0.0.1 and the port docker picked: the tier is a
        process on this host. ``requested``: as after the tier has asked for each demo (its first
        open, demo_bundles.request), so its stores are brought up; else it is only pulled."""
        self._bundles = bundles
        if requested:
            for name in (n.strip() for n in demos.split(",") if n.strip()):
                (bundles / name).mkdir(parents=True, exist_ok=True)
                (bundles / name / "requested").write_text("requested\n")
        return subprocess.run(
            ["bash", str(_SYNC)],
            capture_output=True,
            text=True,
            timeout=900,
            env={
                **os.environ,
                "PROVISA_DEMO_BUNDLES": demos,
                "PROVISA_DEMO_REGISTRY": self.url,
                "DEMO_BUNDLES_DIR": str(bundles),
                "DEMO_PULL": f"{sys.executable} {_SCRIPT}",
                "DEMO_PULL_INTO": str(bundles),
                "DEMO_STORES_NETWORK": "",
                "DEMO_STORES": str(_STORES),
                "DEMO_STORES_SHARED": str(_SHARED),
                "DEMO_STORES_PROJECT": self.stores_project,
            },
        )
