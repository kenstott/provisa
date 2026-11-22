# Copyright (c) 2026 Kenneth Stott
# Canary: e88fb425-d4ef-4bb3-b553-d9905491e43a
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""E2E: render_graph through the real renderer -- the MCP tool runs Node on provisa-ui/graph-render's
bundle, which draws with the UI's own Cytoscape stylesheet and layouts, and answers a PNG on the
graph canvas colour for the theme.

The renderer is built here first (npm ci, npm run build), as the image's graph-render stage builds
it; the run needs node and npm on PATH."""

from __future__ import annotations

import shutil
import struct
import subprocess
import zlib
from pathlib import Path

import pytest

from provisa.api.mcp import tools

pytestmark = [pytest.mark.integration]

_PACKAGE = Path(__file__).resolve().parents[2] / "provisa-ui" / "graph-render"


@pytest.fixture(scope="module")
def built() -> Path:
    for tool in ("node", "npm"):
        assert shutil.which(tool), f"{tool} is required to build the graph renderer"
    subprocess.run(["npm", "ci", "--no-audit", "--no-fund"], cwd=_PACKAGE, check=True)
    subprocess.run(["npm", "run", "build"], cwd=_PACKAGE, check=True)
    bundle = _PACKAGE / "dist" / "render.mjs"
    assert bundle.is_file()
    return bundle


def _first_pixel(png: bytes) -> str:
    """The top-left pixel of an 8-bit RGBA PNG, as #rrggbb (the renderer's own output format)."""
    assert png[:8] == b"\x89PNG\r\n\x1a\n"
    pos, idat, width = 8, b"", 0
    while pos < len(png):
        (length,) = struct.unpack(">I", png[pos : pos + 4])
        kind = png[pos + 4 : pos + 8]
        body = png[pos + 8 : pos + 8 + length]
        if kind == b"IHDR":
            width = struct.unpack(">I", body[:4])[0]
            assert body[8:10] == b"\x08\x06", "8-bit RGBA"
        elif kind == b"IDAT":
            idat += body
        pos += 12 + length
    row = zlib.decompress(idat)[: 1 + 4 * width]
    assert row[0] in (0, 1, 2, 3, 4)
    # Every PNG filter predicts the first row's first pixel from zeros, so its bytes are raw.
    return "#" + row[1:4].hex()


# The Cypher graph's canvas colour (graphTokens canvas), the colour every label box is drawn in.
@pytest.mark.parametrize(("theme", "background"), [("light", "#f8f9fa"), ("dark", "#242424")])
def test_render_graph_draws_a_png_on_the_graph_canvas_colour(built, monkeypatch, theme, background):
    monkeypatch.setattr(tools, "GRAPH_RENDERER", built)
    ada = {
        "id": 1,
        "label": "Sales:Customer",
        "tableLabel": "Customer",
        "properties": {"name": "Ada"},
    }
    order = {"id": 7, "label": "Sales:Order", "tableLabel": "Order", "properties": {"total": 10.5}}
    placed = {
        "identity": 70,
        "start": 1,
        "end": 7,
        "type": "PLACED",
        "properties": {},
        "startNode": ada,
        "endNode": order,
    }
    elements = tools.cytoscape_elements([{"r": placed}])
    png = tools.render_graph(elements, "force", theme, 800, 600)
    assert len(png) > 1000
    assert _first_pixel(png) == background
