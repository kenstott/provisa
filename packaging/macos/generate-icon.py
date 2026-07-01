#!/usr/bin/env python3
# Copyright (c) 2026 Kenneth Stott
# Canary: dc1a39a3-3106-4421-ad4d-2bbfd2031758
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Render the Provisa brand mark (graphite tile + emerald "P") to the icon
assets every installer needs. Single source of truth for the mark, mirroring
provisa-ui/public/icon.svg.

Self-installs Pillow if not present. Outputs into <output_dir>:
  Provisa.iconset/  + Provisa.icns   — macOS app/volume icon (needs iconutil)
  provisa.ico                        — Windows installer/shortcut icon
  provisa-mark.png (256)             — Windows installer header logo
  Provisa.png (512)                  — Linux AppImage icon

Run: python3 generate-icon.py [output_dir]
"""

import subprocess
import sys
from pathlib import Path


def ensure_pillow():
    try:
        from PIL import Image, ImageDraw  # noqa: F401
    except ImportError:
        print("[icon] Installing Pillow...", flush=True)
        subprocess.check_call([sys.executable, "-m", "pip", "install", "pillow", "--quiet"])


ensure_pillow()

from PIL import Image, ImageDraw  # noqa: E402


# ── Brand palette (matches provisa-ui/public/icon.svg) ────────────────────────
GRAPHITE = (31, 41, 51)  # #1F2933 — tile + inner cutout
WHITE = (255, 255, 255)  # P stem + bowl
EMERALD = (16, 185, 129)  # #10B981 — accent dot

# Mark geometry in the 512×512 viewBox (icon.svg, group translate(48,66) scale 4)
_TILE_RADIUS = 112
_STEM = (168, 138, 228, 394, 28)  # x0, y0, x1, y1, corner-radius
_BOWL = (256, 206, 88)  # cx, cy, r  (white)
_CUTOUT = (256, 202, 42)  # cx, cy, r  (graphite)
_DOT = (256, 202, 18)  # cx, cy, r  (emerald)

_SS = 4  # supersample factor for smooth edges


def draw_mark(size: int) -> "Image.Image":
    """Render the brand mark at <size>px, supersampled then downscaled."""
    s = size * _SS
    k = s / 512.0
    img = Image.new("RGBA", (s, s), (0, 0, 0, 0))
    d = ImageDraw.Draw(img)

    d.rounded_rectangle([0, 0, s - 1, s - 1], radius=_TILE_RADIUS * k, fill=GRAPHITE)

    x0, y0, x1, y1, r = _STEM
    d.rounded_rectangle([x0 * k, y0 * k, x1 * k, y1 * k], radius=r * k, fill=WHITE)

    def circle(cx, cy, rad, fill):
        d.ellipse([(cx - rad) * k, (cy - rad) * k, (cx + rad) * k, (cy + rad) * k], fill=fill)

    circle(*_BOWL, WHITE)
    circle(*_CUTOUT, GRAPHITE)
    circle(*_DOT, EMERALD)

    return img.resize((size, size), Image.LANCZOS)


_ICONSET_SIZES = [
    ("icon_16x16.png", 16),
    ("icon_16x16@2x.png", 32),
    ("icon_32x32.png", 32),
    ("icon_32x32@2x.png", 64),
    ("icon_128x128.png", 128),
    ("icon_128x128@2x.png", 256),
    ("icon_256x256.png", 256),
    ("icon_256x256@2x.png", 512),
    ("icon_512x512.png", 512),
    ("icon_512x512@2x.png", 1024),
]


def main():
    out_dir = Path(sys.argv[1]) if len(sys.argv) > 1 else Path(__file__).parent
    out_dir.mkdir(parents=True, exist_ok=True)

    # macOS iconset — draw each size directly for crisp small icons.
    iconset_dir = out_dir / "Provisa.iconset"
    iconset_dir.mkdir(parents=True, exist_ok=True)
    for filename, px in _ICONSET_SIZES:
        draw_mark(px).save(iconset_dir / filename, "PNG")
    print("[icon] iconset rendered", flush=True)

    # Windows header logo + Linux AppImage icon.
    draw_mark(256).save(out_dir / "provisa-mark.png", "PNG")
    draw_mark(512).save(out_dir / "Provisa.png", "PNG")

    # Windows multi-resolution .ico.
    ico_sizes = [16, 32, 48, 64, 128, 256]
    draw_mark(256).save(
        out_dir / "provisa.ico",
        format="ICO",
        sizes=[(n, n) for n in ico_sizes],
    )
    print("[icon] provisa.ico / provisa-mark.png / Provisa.png rendered", flush=True)

    # Compile .icns (macOS only — iconutil not present elsewhere).
    icns_path = out_dir / "Provisa.icns"
    result = subprocess.run(
        ["iconutil", "-c", "icns", str(iconset_dir), "-o", str(icns_path)],
        capture_output=True,
    )
    if result.returncode == 0:
        print(f"[icon] Compiled: {icns_path}", flush=True)
    elif sys.platform == "darwin":
        print(f"[icon] iconutil failed: {result.stderr.decode()}", flush=True)
        sys.exit(1)
    else:
        print("[icon] iconutil unavailable (non-macOS) — skipped .icns", flush=True)


if __name__ == "__main__":
    main()
