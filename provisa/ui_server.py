"""SPA static file server — serves provisa-ui/dist/ with index.html fallback."""
from pathlib import Path

from fastapi import FastAPI
from fastapi.responses import FileResponse, HTMLResponse
from fastapi.staticfiles import StaticFiles

STATIC_DIR = Path(__file__).parent.parent / "static"

app = FastAPI()

assets_dir = STATIC_DIR / "assets"
if assets_dir.is_dir():
    app.mount("/assets", StaticFiles(directory=assets_dir), name="assets")


@app.get("/{full_path:path}", response_model=None)
async def spa_fallback(full_path: str) -> FileResponse | HTMLResponse:
    index = STATIC_DIR / "index.html"
    if not index.is_file():
        return HTMLResponse("<h1>Provisa UI not bundled</h1><p>Static files were not built into this image.</p>", status_code=503)
    candidate = STATIC_DIR / full_path
    if candidate.is_file():
        return FileResponse(candidate)
    return FileResponse(index)
