# Copyright (c) 2026 Kenneth Stott
# Canary: 0fd5f8aa-f691-432f-b8ea-5004efde3330
#
# This source code is licensed under the Business Source License 1.1
"""Inside the built image: render_graph draws a graph through the image's own node, renderer and
fonts, on the graph canvas colour for the light and dark themes. Run as `docker run -i <image> python - < this`."""

from __future__ import annotations

import struct
import zlib

from provisa.api.mcp import tools


def first_pixel(png: bytes) -> str:
    assert png[:8] == b"\x89PNG\r\n\x1a\n", "not a PNG"
    pos, idat, width = 8, b"", 0
    while pos < len(png):
        (length,) = struct.unpack(">I", png[pos : pos + 4])
        kind, body = png[pos + 4 : pos + 8], png[pos + 8 : pos + 8 + length]
        if kind == b"IHDR":
            width = struct.unpack(">I", body[:4])[0]
        elif kind == b"IDAT":
            idat += body
        pos += 12 + length
    # Every PNG filter predicts the first row's first pixel from zeros, so its bytes are raw.
    return "#" + zlib.decompress(idat)[1 : 1 + 4 * width][:3].hex()


ada = {"id": 1, "label": "Sales:Customer", "tableLabel": "Customer", "properties": {"name": "Ada"}}
order = {"id": 7, "label": "Sales:Order", "tableLabel": "Order", "properties": {}}
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
# The graph canvas colour (graphTokens canvas), from provisa-ui/src/theme/palette.ts: SCALES.gray[0]
# light, SCALES.dark[7] dark. The image carries no UI source, so the two values are literals here.
for layout in tools.GRAPH_LAYOUTS:
    for theme, background in (("light", "#f8f9fa"), ("dark", "#242424")):
        png = tools.render_graph(elements, layout, theme)
        assert first_pixel(png) == background, (layout, theme, first_pixel(png))
        print(f"render_graph {layout} {theme}: {len(png)} bytes on {background}")
