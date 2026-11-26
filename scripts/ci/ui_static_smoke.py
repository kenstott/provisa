# Copyright (c) 2026 Kenneth Stott
# Canary: 0f3b8e27-6a4d-4c19-9e52-b7d1a0c4f836
#
# This source code is licensed under the Business Source License 1.1
"""Inside the built image: the UI server answers GET / with the React SPA the Dockerfile's
ui-build stage built into static/, not 503 "UI not bundled". Run as
`docker run -i <image> python - < this`."""

from __future__ import annotations

from fastapi.testclient import TestClient

from provisa.ui_server import STATIC_DIR, app

index = STATIC_DIR / "index.html"
assert index.is_file(), f"{index} is missing: the image carries no built UI"
assets = sorted(p.name for p in (STATIC_DIR / "assets").iterdir())
assert assets, f"{STATIC_DIR / 'assets'} is empty"
docs = STATIC_DIR / "docs-site" / "index.html"
assert docs.is_file(), f"{docs} is missing: the offline docs site was not built into the UI"

response = TestClient(app).get("/", headers={"Sec-Fetch-Dest": "document", "Accept": "text/html"})
assert response.status_code == 200, (response.status_code, response.text[:300])
assert "/assets/" in response.text, response.text[:300]
print(f"GET / -> 200 from {STATIC_DIR} ({len(assets)} assets)")
