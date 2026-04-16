"""SPA static file server with API reverse proxy.

Serves provisa-ui/dist/ on port 3000 and proxies all API calls
(anything that is not a static asset) to the provisa API container
at http://provisa:8000.  This lets the React SPA use relative URLs
for all API requests regardless of the environment.
"""
import os
from pathlib import Path

import httpx
from fastapi import FastAPI, Request
from fastapi.responses import FileResponse, HTMLResponse, Response, StreamingResponse
from fastapi.staticfiles import StaticFiles

STATIC_DIR = Path(__file__).parent.parent / "static"

# API container reachable via Docker network hostname; override via env var.
API_BASE_URL = os.environ.get("PROVISA_API_URL", "http://provisa:8000")

# Paths that are always served from static files (never proxied).
_STATIC_PREFIXES = ("/assets/", "/monacoeditorwork/", "/favicon")

app = FastAPI()

assets_dir = STATIC_DIR / "assets"
if assets_dir.is_dir():
    app.mount("/assets", StaticFiles(directory=assets_dir), name="assets")

monaco_dir = STATIC_DIR / "monacoeditorwork"
if monaco_dir.is_dir():
    app.mount("/monacoeditorwork", StaticFiles(directory=monaco_dir), name="monacoeditorwork")


@app.api_route("/{full_path:path}", methods=["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS", "HEAD"], response_model=None)
async def handler(request: Request, full_path: str) -> Response:
    # ── Static asset — serve from disk ───────────────────────────────────────
    if any(request.url.path.startswith(p) for p in _STATIC_PREFIXES):
        candidate = STATIC_DIR / full_path
        if candidate.is_file():
            return FileResponse(candidate)

    # ── SPA root — serve index.html ───────────────────────────────────────────
    index = STATIC_DIR / "index.html"
    if not index.is_file():
        return HTMLResponse(
            "<h1>Provisa UI not bundled</h1><p>Static files were not built into this image.</p>",
            status_code=503,
        )

    # ── API proxy — forward to provisa container ──────────────────────────────
    # Heuristic: paths with no extension that match known API prefixes are proxied.
    api_prefixes = (
        "/admin/", "/data/", "/sources", "/tables", "/views",
        "/domains", "/roles", "/health", "/metrics", "/auth",
    )
    is_api = any(request.url.path == p.rstrip("/") or request.url.path.startswith(p) for p in api_prefixes)

    if is_api:
        target = f"{API_BASE_URL}/{full_path}"
        if request.url.query:
            target = f"{target}?{request.url.query}"

        body = await request.body()
        headers = {
            k: v for k, v in request.headers.items()
            if k.lower() not in ("host", "content-length")
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
                return HTMLResponse("API unavailable", status_code=503)

        return Response(
            content=upstream.content,
            status_code=upstream.status_code,
            headers=dict(upstream.headers),
        )

    # ── SPA fallback — all other paths render index.html ─────────────────────
    return FileResponse(index)
