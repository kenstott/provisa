#!/usr/bin/env python3
# Copyright (c) 2026 Kenneth Stott
# Canary: 7acb5d34-b1a0-411c-97a2-b6656f65c204
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Generate the DMG installer background image (660×400 @1x).

Layout:
  Top:     Provisa branding + tagline
  Left:    App icon position (x=165, y=155)
  Right:   Applications folder drop target (x=495, y=155)
  Centre:  Drag arrow between them
  Bottom:  "Drag Provisa to the Applications folder to install" instruction

Rendered at 1× so Finder renders it at the correct size without Retina scaling
confusion.
"""
import math
import os
import subprocess
import sys
from pathlib import Path


def ensure_pillow():
    try:
        from PIL import Image, ImageDraw, ImageFont  # noqa: F401
    except ImportError:
        subprocess.check_call(
            [sys.executable, "-m", "pip", "install", "pillow", "--quiet"]
        )


ensure_pillow()

from PIL import Image, ImageDraw, ImageFont  # noqa: E402


# ── Constants ─────────────────────────────────────────────────────────────────
W, H = 660, 400

GRAD_TL = (15,  40, 100)   # deep navy
GRAD_BR = (80,  10, 140)   # rich purple

TEXT_MAIN  = (255, 255, 255, 255)
TEXT_DIM   = (200, 210, 240, 200)
ARROW_COL  = (180, 190, 255, 180)

SCALE = 1   # 1× — Finder renders DMG backgrounds without Retina scaling


def lerp(a, b, t):
    return a + (b - a) * t


def make_background(scale: int = SCALE) -> Image.Image:
    sw, sh = W * scale, H * scale

    img  = Image.new("RGBA", (sw, sh), (0, 0, 0, 255))
    draw = ImageDraw.Draw(img)

    # ── gradient background ───────────────────────────────────────────────
    for y in range(sh):
        ty = y / (sh - 1)
        for x in range(sw):
            tx  = x / (sw - 1)
            t   = (tx * 0.4 + ty * 0.6)
            r   = int(lerp(GRAD_TL[0], GRAD_BR[0], t))
            g   = int(lerp(GRAD_TL[1], GRAD_BR[1], t))
            b   = int(lerp(GRAD_TL[2], GRAD_BR[2], t))
            draw.point((x, y), fill=(r, g, b, 255))

    # ── branding text (left side) ─────────────────────────────────────────
    brand_x = int(30 * scale)
    brand_y = int(35 * scale)

    logo_font = _font(int(56 * scale))
    tag_font  = _font(int(15 * scale))

    draw.text((brand_x, brand_y), "Provisa",
              font=logo_font, fill=TEXT_MAIN)

    tag_y = brand_y + int(66 * scale)
    draw.text((brand_x, tag_y), "Data Virtualization Platform",
              font=tag_font, fill=TEXT_DIM)

    # ── instruction text ──────────────────────────────────────────────────
    inst_font = _font(int(16 * scale))
    inst_y    = int(H * scale * 0.88)
    inst_text = "Drag Provisa to the Applications folder to install"
    bbox      = draw.textbbox((0, 0), inst_text, font=inst_font)
    inst_x    = (sw - (bbox[2] - bbox[0])) // 2
    draw.text((inst_x, inst_y), inst_text, font=inst_font, fill=TEXT_DIM)

    # ── horizontal drag arrow (Provisa → Applications) ────────────────────
    app_cx  = int(165 * scale)   # Provisa.app icon centre
    apps_cx = int(495 * scale)   # Applications drop target centre
    icon_r  = int(55 * scale)    # approximate icon half-width
    arr_y   = int(230 * scale)   # vertical centre of the icon row
    ah = int(14 * scale)         # arrowhead half-size
    lw = max(2, int(3 * scale))

    x1 = app_cx  + icon_r + int(10 * scale)  # start just right of Provisa icon
    x2 = apps_cx - icon_r - int(10 * scale)  # end just left of Applications icon

    draw.line([(x1, arr_y), (x2, arr_y)], fill=ARROW_COL, width=lw)
    draw.polygon(
        [(x2,      arr_y),
         (x2 - ah, arr_y - ah),
         (x2 - ah, arr_y + ah)],
        fill=ARROW_COL,
    )

    return img


_FONT_CANDIDATES = [
    "/System/Library/Fonts/SFCompact-Bold.otf",
    "/System/Library/Fonts/SFCompactText-Bold.otf",
    "/System/Library/Fonts/SF Pro/SF-Pro-Display-Bold.otf",
    "/Library/Fonts/Arial Bold.ttf",
    "/System/Library/Fonts/Helvetica.ttc",
    "/System/Library/Fonts/Arial.ttf",
]


def _font(size: int):
    for path in _FONT_CANDIDATES:
        if os.path.exists(path):
            try:
                return ImageFont.truetype(path, size)
            except Exception:
                continue
    return ImageFont.load_default(size=size) if hasattr(ImageFont, "load_default") else ImageFont.load_default()


def main():
    out_dir = Path(sys.argv[1]) if len(sys.argv) > 1 else Path(__file__).parent
    out_path = out_dir / "dmg-background.png"

    print("[bg] Generating DMG background...", flush=True)
    img = make_background(scale=SCALE)
    img = img.convert("RGB")  # DMG backgrounds don't need alpha
    img.save(out_path, "PNG")
    print(f"[bg] Saved: {out_path} ({img.width}×{img.height})", flush=True)


if __name__ == "__main__":
    main()
