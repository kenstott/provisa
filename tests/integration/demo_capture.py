# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A named demo's answers, as its test reads them, kept for its card (the UI render harness's
``<demo>-capture/``).

A demo whose answers need data only CI lands (govdata) has its card rendered from them: with
``PROVISA_DEMO_CAPTURE_DIR`` set, its integration test writes each response it reads into
``<that directory>/<demo>-capture/``, under the names the harness reads (``sql-<step>.json``,
``gql-<example>.json``, ``rest.json``, ``introspection.json``, ``data-graph-schema``...). Unset, as in
every other run, nothing is written.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

import httpx

CAPTURE_ENV = "PROVISA_DEMO_CAPTURE_DIR"


def _directory(demo: str) -> Path | None:
    root = os.environ.get(CAPTURE_ENV)
    if not root:
        return None
    path = Path(root) / f"{demo}-capture"
    path.mkdir(parents=True, exist_ok=True)
    return path


def keep(demo: str, name: str, body: str) -> None:
    """Write ``body`` as ``name`` of ``demo``'s capture, when captures are kept."""
    directory = _directory(demo)
    if directory is not None:
        (directory / name).write_text(body)


def keep_json(demo: str, name: str, value: Any) -> None:
    keep(demo, name, json.dumps(value))


def keep_served(demo: str, base_url: str, role: str) -> None:
    """Read the endpoints the demo's pages read beside its answers, as ``role``, and keep each:
    every one must answer, captured or not."""

    def get(path: str) -> httpx.Response:
        resp = httpx.get(f"{base_url}{path}", headers={"X-Provisa-Role": role}, timeout=300)
        assert resp.status_code == 200, f"{path}: HTTP {resp.status_code}: {resp.text[:500]}"
        return resp

    keep_json(demo, "introspection.json", get("/data/introspection").json()["data"])
    keep(demo, "data-graph-schema", get("/data/graph-schema").text)
    keep(demo, "data-graph-counts", get("/data/graph-counts").text)
    terms = get("/admin/glossary/terms")
    keep(demo, "admin-glossary-terms", terms.text)
    keep_json(
        demo,
        "glossary-detail.json",
        {str(t["id"]): get(f"/admin/glossary/terms/{t['id']}").json() for t in terms.json()},
    )
    keep(demo, "data-rest-docs", get("/data/rest/docs").text)
    keep(demo, "data-rest-openapi.json", get("/data/rest/openapi.json").text)
