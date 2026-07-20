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
API_BASE_URL = os.environ.get("PROVISA_API_URL", "http://provisa:8000")

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

    async with httpx.AsyncClient(timeout=120) as client:
        try:
            upstream = await client.request(
                method=request.method,
                url=target,
                headers=headers,
                content=body,
            )
        except httpx.ConnectError:
            return HTMLResponse("API unavailable", status_code=502)

    return Response(
        content=upstream.content,
        status_code=upstream.status_code,
        headers=dict(upstream.headers),
    )
