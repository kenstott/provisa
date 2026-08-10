# Copyright (c) 2026 Kenneth Stott
# Canary: 76d8c1bd-7426-4405-9d41-9066883bbd41
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""SPA static file server with API reverse proxy.

Serves provisa-ui/dist/ on port 3000 and reverse-proxies API calls to the provisa
API container at http://provisa:8000, letting the React SPA use relative URLs.

API-vs-SPA routing is deterministic, driven by the browser's Sec-Fetch-Dest
signal rather than a maintained path allowlist: only a genuine top-level
navigation (Sec-Fetch-Dest: document) renders the SPA shell; every other request
is proxied to the API and its real status — including 404 — is surfaced. This
means an unrecognized API path can never silently masquerade as an HTML 200, and
SPA deep-link refreshes (e.g. /admin/overview) resolve correctly.
"""

# Requirements: REQ-057, REQ-058, REQ-559

import json
import os
from collections.abc import Mapping
from pathlib import Path

import httpx
from fastapi import FastAPI, Request
from fastapi.responses import FileResponse, HTMLResponse, Response
from fastapi.staticfiles import StaticFiles

# REQ-1127: the pip wheel embeds the precompiled React UI at provisa/_ui/ so the delivery is
# self-contained (no npm/Node at runtime). Prefer that packaged directory; fall back to the
# repo-root static/ used by the Docker image and the dev tree. This is resource location
# (packaged vs source layout), not error-hiding — the chosen dir is used unconditionally.
_PACKAGED_UI = Path(__file__).resolve().parent / "_ui"
_REPO_STATIC = Path(__file__).resolve().parent.parent / "static"
STATIC_DIR = _PACKAGED_UI if _PACKAGED_UI.is_dir() else _REPO_STATIC

# API container reachable via Docker network hostname; override via env var.
# REQ-1226: in a cluster deploy every endpoint serves TLS, so the API listens
# HTTPS on 8000. The node compose overlay (first-launch.sh) sets PROVISA_API_URL
# to the https:// upstream on the UI service whenever it enables TLS; the desktop
# default stays plaintext. A plaintext http:// hop to a TLS port fails with
# httpx.ReadError, so the scheme must track the API's actual listener.
API_BASE_URL = os.environ.get("PROVISA_API_URL", "http://provisa:8000")
# TLS verification for the internal proxy leg keys off the resolved scheme, not a
# cert env var (the UI container mounts the node cert at a fixed path, not via
# PROVISA_TLS_CERT). This is an internal same-node container hop: the presented
# cert is the node's public wildcard (e.g. *.provisa.dev) or a self-signed node
# cert, neither of which can match the Docker service name "provisa", so TLS
# hostname verification is structurally impossible here. The node itself is the
# trust boundary (REQ-1226); verify is disabled only for this proxy leg — the
# browser->UI leg is unaffected.
_API_VERIFY = not API_BASE_URL.startswith("https://")

# Paths that are always served from static files (never proxied).
# /docs-site/ holds the bundled MkDocs Material site (served same-origin so the
# in-app Docs reader works airgapped when the hosted site is unreachable). The
# SPA route /docs is NOT here — it renders the reader, which iframes /docs-site/.
_STATIC_PREFIXES = (
    "/assets/",
    "/monacoeditorwork/",
    "/docs-site/",
    "/favicon",
    "/icon.svg",
    "/icon-192.png",
    "/icon-512.png",
    "/apple-touch-icon.png",
    "/site.webmanifest",
    # REQ-1348: the cross-subdomain auth relay. It loads as an iframe subresource, not a
    # navigation, so without this it would be treated as an API call and proxied upstream
    # instead of being served as the built page.
    "/auth-relay.html",
    # The Schema Explorer's SDL view renders GraphQL Voyager inside a srcDoc iframe, which pulls
    # these vendored bundles as script/style subresources — not navigations. Without the prefix
    # they fall through to the API proxy and come back 401, and the iframe renders blank.
    "/voyager/",
)

# Disable this proxy app's own Swagger so /docs falls through to the SPA (the API's
# Swagger lives at /data/openapi/docs, reachable through the proxy).
app = FastAPI(docs_url=None, redoc_url=None, openapi_url=None)

assets_dir = STATIC_DIR / "assets"
if assets_dir.is_dir():
    app.mount("/assets", StaticFiles(directory=assets_dir), name="assets")

monaco_dir = STATIC_DIR / "monacoeditorwork"
if monaco_dir.is_dir():
    app.mount("/monacoeditorwork", StaticFiles(directory=monaco_dir), name="monacoeditorwork")


def is_spa_navigation(method: str, headers: Mapping[str, str]) -> bool:  # REQ-1006
    # Sec-Fetch-Dest: document identifies a real page load / deep-link refresh.
    # All browsers Provisa ships against emit it; when absent (legacy UA) fall
    # back to the Accept: text/html + GET pair, which carries the same meaning.
    # Anything else (fetch/XHR/EventSource: empty; iframe subresource; any
    # non-GET) is an API request and is proxied — never served index.html.
    dest = headers.get("sec-fetch-dest")
    return dest == "document" or (
        dest is None and method == "GET" and "text/html" in headers.get("accept", "")
    )


@app.get("/firebase-config.js")
async def firebase_config() -> Response:  # REQ-1266
    # The SPA's Firebase web config (public client keys — apiKey/authDomain/projectId)
    # is injected at runtime from the node's environment, so a single built image
    # serves any Firebase project: the deploy (terraform -> first-launch) sets the
    # VITE_FIREBASE_* env on this UI container. index.html loads this before the app
    # bundle, exposing window.__PROVISA_FIREBASE__ for lib/firebase.ts.
    #
    # When the deployment is not Firebase-backed (basic/none), the vars are absent and
    # this emits null — the login page never imports lib/firebase in that case, so null
    # is the configured-off state, not a hidden error. All three must be present
    # together; a partial set is a misconfiguration and yields null rather than a
    # broken initializeApp.
    api_key = os.environ.get("VITE_FIREBASE_API_KEY", "")
    auth_domain = os.environ.get("VITE_FIREBASE_AUTH_DOMAIN", "")
    project_id = os.environ.get("VITE_FIREBASE_PROJECT_ID", "")
    if api_key and auth_domain and project_id:
        cfg_dict = {"apiKey": api_key, "authDomain": auth_domain, "projectId": project_id}
        # Optional Microsoft (Azure AD/Entra) tenant restriction; injected at runtime so the
        # prebuilt image need not bake it. Absent => firebase.ts uses "common".
        azure_tenant = os.environ.get("VITE_AZURE_TENANT", "")
        if azure_tenant:
            cfg_dict["azureTenant"] = azure_tenant
        cfg = json.dumps(cfg_dict)
        body = f"window.__PROVISA_FIREBASE__ = {cfg};"
    else:
        body = "window.__PROVISA_FIREBASE__ = null;"
    return Response(
        content=body,
        media_type="application/javascript",
        headers={"Cache-Control": "no-store"},
    )


@app.api_route(
    "/{full_path:path}",
    methods=["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS", "HEAD"],
    response_model=None,
)
async def handler(request: Request, full_path: str) -> Response:  # REQ-057, REQ-058, REQ-559
    # ── Static asset — serve from disk ───────────────────────────────────────
    if any(request.url.path.startswith(p) for p in _STATIC_PREFIXES):
        candidate = STATIC_DIR / full_path
        if candidate.is_file():
            return FileResponse(candidate)
        # MkDocs uses directory URLs (/docs-site/security/ -> .../security/index.html);
        # serve the directory index so the offline site navigates correctly.
        if candidate.is_dir() and (candidate / "index.html").is_file():
            return FileResponse(candidate / "index.html")

    # ── SPA root — serve index.html ───────────────────────────────────────────
    index = STATIC_DIR / "index.html"
    if not index.is_file():
        return HTMLResponse(
            "<h1>Provisa UI not bundled</h1><p>Static files were not built into this image.</p>",
            status_code=503,
        )

    # ── SPA shell — only for genuine top-level navigations ───────────────────
    # Sec-Fetch-Dest: document identifies a real page load / deep-link refresh.
    # All browsers Provisa ships against emit it; when absent (legacy UA) fall
    # back to the Accept: text/html + GET pair, which carries the same meaning.
    # Anything else (fetch/XHR/EventSource: empty; iframe subresource; any
    # non-GET) is an API request and is proxied below — never served index.html.
    if is_spa_navigation(request.method, request.headers):
        return FileResponse(index)

    # ── API proxy — forward to the provisa API, surfacing its real status ─────
    target = f"{API_BASE_URL}/{full_path}"
    if request.url.query:
        target = f"{target}?{request.url.query}"

    body = await request.body()
    headers = {
        k: v for k, v in request.headers.items() if k.lower() not in ("host", "content-length")
    }

    async with httpx.AsyncClient(timeout=120, verify=_API_VERIFY) as client:
        try:
            upstream = await client.request(
                method=request.method,
                url=target,
                headers=headers,
                content=body,
            )
        except httpx.ConnectError:
            return HTMLResponse("API unavailable", status_code=502)

    # The API is reachable to the browser only through this proxy's own origin.
    # FastAPI's trailing-slash and other redirects emit an absolute Location on the
    # internal host (http://provisa:8000/...), which the browser cannot resolve —
    # the fetch dies as "Failed to fetch". Rewrite it back to a path on our origin.
    resp_headers = dict(upstream.headers)
    location = resp_headers.get("location")
    if location and location.startswith(API_BASE_URL):
        resp_headers["location"] = location[len(API_BASE_URL) :] or "/"

    return Response(
        content=upstream.content,
        status_code=upstream.status_code,
        headers=resp_headers,
    )
