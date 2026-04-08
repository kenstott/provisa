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

"""Generate Provisa.app icon in all required macOS iconset sizes.

Self-installs Pillow if not present. Outputs:
  Provisa.iconset/icon_NxN[@2x].png  — all required sizes
  Provisa.icns                        — compiled icon (requires iconutil)

Run: python3 generate-icon.py [output_dir]
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
        print("[icon] Installing Pillow...", flush=True)
        subprocess.check_call(
            [sys.executable, "-m", "pip", "install", "pillow", "--quiet"]
        )


ensure_pillow()

from PIL import Image, ImageDraw, ImageFont  # noqa: E402


# ── Design constants ──────────────────────────────────────────────────────────

# Gradient: deep navy top-left → rich purple bottom-right
GRAD_TL = (15, 40, 100)    # #0F2864 — deep navy
GRAD_BR = (100, 20, 180)   # #6414B4 — rich purple

# Letter colour and shadow
LETTER     = (255, 255, 255, 255)
LETTER_SHD = (0,   0,   0,    60)

CORNER_RADIUS_RATIO = 0.225   # ~23% of size — matches macOS icon spec


def lerp(a, b, t):
    return a + (b - a) * t


def make_icon(size: int) -> Image.Image:
    img   = Image.new("RGBA", (size, size), (0, 0, 0, 0))
    draw  = ImageDraw.Draw(img)

    # ── gradient fill ──────────────────────────────────────────────────────
    grad = Image.new("RGB", (size, size))
    gd   = ImageDraw.Draw(grad)
    for y in range(size):
        ty = y / (size - 1)
        for x in range(size):
            tx = x / (size - 1)
            t  = (tx + ty) / 2          # diagonal blend
            r  = int(lerp(GRAD_TL[0], GRAD_BR[0], t))
            g  = int(lerp(GRAD_TL[1], GRAD_BR[1], t))
            b  = int(lerp(GRAD_TL[2], GRAD_BR[2], t))
            gd.point((x, y), fill=(r, g, b))

    # ── rounded-rectangle mask ─────────────────────────────────────────────
    radius = int(size * CORNER_RADIUS_RATIO)
    mask   = Image.new("L", (size, size), 0)
    md     = ImageDraw.Draw(mask)
    md.rounded_rectangle([(0, 0), (size - 1, size - 1)], radius=radius, fill=255)

    # ── subtle radial glow (lighter centre) ───────────────────────────────
    cx, cy = size // 2, size // 2
    glow   = Image.new("RGBA", (size, size), (0, 0, 0, 0))
    for y in range(0, size, max(1, size // 64)):
        for x in range(0, size, max(1, size // 64)):
            dist = math.hypot(x - cx, y - cy) / (size * 0.7)
            alpha = int(max(0, 40 * (1 - dist)))
            glow.paste((255, 255, 255, alpha), (x, y, min(x + size // 64 + 1, size),
                                                min(y + size // 64 + 1, size)))

    img.paste(grad, mask=mask)
    img = Image.alpha_composite(img, glow)
    # re-apply mask to clip glow
    img.putalpha(mask)

    # ── "P" letterform ────────────────────────────────────────────────────
    font = _load_font(size)
    draw = ImageDraw.Draw(img)
    letter = "P"

    # measure
    bbox = draw.textbbox((0, 0), letter, font=font)
    tw   = bbox[2] - bbox[0]
    th   = bbox[3] - bbox[1]
    tx   = (size - tw) // 2 - bbox[0]
    ty   = (size - th) // 2 - bbox[1] - int(size * 0.02)

    # drop shadow
    sdx = max(1, int(size * 0.005))
    sdy = max(1, int(size * 0.008))
    draw.text((tx + sdx, ty + sdy), letter, font=font, fill=LETTER_SHD)

    # letter
    draw.text((tx, ty), letter, font=font, fill=LETTER)

    return img


_FONT_CANDIDATES = [
    # macOS system fonts (bold)
    "/System/Library/Fonts/SFCompact-Bold.otf",
    "/System/Library/Fonts/SFCompactText-Bold.otf",
    "/System/Library/Fonts/SF Pro/SF-Pro-Display-Bold.otf",
    "/Library/Fonts/Arial Bold.ttf",
    "/System/Library/Fonts/Helvetica.ttc",
    "/System/Library/Fonts/Arial.ttf",
]


def _load_font(size: int):
    font_size = int(size * 0.60)
    for path in _FONT_CANDIDATES:
        if os.path.exists(path):
            try:
                return ImageFont.truetype(path, font_size)
            except Exception:
                continue
    # fallback — PIL built-in (low-res but always present)
    return ImageFont.load_default(size=font_size) if hasattr(ImageFont, "load_default") else ImageFont.load_default()


# ── Iconset sizes ─────────────────────────────────────────────────────────────

ICONSET_SIZES = [
    ("icon_16x16.png",       16),
    ("icon_16x16@2x.png",    32),
    ("icon_32x32.png",       32),
    ("icon_32x32@2x.png",    64),
    ("icon_128x128.png",    128),
    ("icon_128x128@2x.png", 256),
    ("icon_256x256.png",    256),
    ("icon_256x256@2x.png", 512),
    ("icon_512x512.png",    512),
    ("icon_512x512@2x.png", 1024),
]


def main():
    out_dir = Path(sys.argv[1]) if len(sys.argv) > 1 else Path(__file__).parent

    iconset_dir = out_dir / "Provisa.iconset"
    iconset_dir.mkdir(parents=True, exist_ok=True)

    print("[icon] Generating icon at 1024×1024...", flush=True)
    master = make_icon(1024)

    for filename, px in ICONSET_SIZES:
        dest = iconset_dir / filename
        if px == 1024:
            img = master.copy()
        else:
            img = master.resize((px, px), Image.LANCZOS)
        img.save(dest, "PNG")
        print(f"[icon]   {filename} ({px}×{px})", flush=True)

    # compile with iconutil (macOS only)
    icns_path = out_dir / "Provisa.icns"
    result = subprocess.run(
        ["iconutil", "-c", "icns", str(iconset_dir), "-o", str(icns_path)],
        capture_output=True,
    )
    if result.returncode == 0:
        print(f"[icon] Compiled: {icns_path}", flush=True)
    else:
        print(f"[icon] iconutil failed: {result.stderr.decode()}", flush=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
