# Copyright (c) 2026 Kenneth Stott
# Canary: f7d208c8-64b7-4c93-a4dc-c884585b1884
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.
"""Raster brand tiles for services that upload an image instead of linking a URL.

The geometry is `provisa-ui/public/icon.svg` transcribed: the same rect, the same three concentric
circles, the same `translate(48,66) scale(4)`. It is redrawn rather than rasterised because the repo
carries no SVG rasteriser, and a mark that drifts from the app's own favicon is a second logo.

One tile per slot, and the slots differ in what crops them:

* ``provisa-product-image-1000.png`` is the Lemon Squeezy product image, which is where Provisa's
  mark belongs -- the store logo is one image shared by SimpleIsHard's three products, so it cannot
  be any one of their marks. 1000px because that is the width the store's own CDN serves the large
  thumbnail at, and it is scaled ``fit=contain``, so the tile is square and full-bleed.
* ``provisa-favicon-512.png`` keeps ``icon.svg``'s ``rx=112`` corner, because a favicon is shown as
  the square it was uploaded as -- the same tile the app already serves in its own tab. It has no
  slot on a store selling three brands; it is here for a store that sells only Provisa.

The tile is opaque ``#1F2933`` inside the corner: it has to read on the light checkout (``#ffffff``)
and the dark one (``#1a1d27``), and a transparent square would leave a white mark on white.
"""

from PIL import Image, ImageDraw

SS = 4  # supersample; the store serves a 100px thumbnail too, and the edges have to survive it.
INK = "#1F2933"
MARK = "#ffffff"
ACCENT = "#10B981"


def _draw(size: int, corner_radius: int) -> Image.Image:
    # The geometry below is written in icon.svg's 512 viewBox; `k` carries it to the tile's size.
    k = size * SS / 512
    img = Image.new("RGBA", (size * SS, size * SS), INK)
    d = ImageDraw.Draw(img)
    d.rounded_rectangle((168 * k, 138 * k, 228 * k, 394 * k), radius=28 * k, fill=MARK)
    for radius, fill in ((88, MARK), (42, INK), (18, ACCENT)):
        d.ellipse(
            (
                (256 - radius) * k,
                (206 - radius) * k,
                (256 + radius) * k,
                (206 + radius) * k,
            ),
            fill=fill,
        )
    if corner_radius:
        # Cut the corner to transparency rather than to a colour, the same as ``icon.svg``: a tab
        # strip or a settings row is whatever colour it is, and a baked-in corner is a visible notch
        # on every one that is not the colour that was baked in.
        alpha = Image.new("L", (size * SS, size * SS), 0)
        ImageDraw.Draw(alpha).rounded_rectangle(
            (0, 0, size * SS - 1, size * SS - 1), radius=corner_radius * k, fill=255
        )
        img.putalpha(alpha)
    return img.resize((size, size), Image.LANCZOS)


if __name__ == "__main__":
    _draw(1000, 0).save("brand/provisa-product-image-1000.png")
    _draw(512, 112).save("brand/provisa-favicon-512.png")
