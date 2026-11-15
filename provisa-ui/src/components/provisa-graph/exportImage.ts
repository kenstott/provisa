// Copyright (c) 2026 Kenneth Stott
// Canary: d17a5c3e-8b42-4f69-a0d1-5c9e3b7f2a84
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2239: a graph's downloads: the whole drawing as SVG, PNG or JPEG on the canvas colour, with
// a margin round it.

import { downloadBlob } from "../graph/graph-export";
import type { CyInstance } from "../graph/cytoscape-types";

const PAD = 10;

/** ``blob``'s image on ``bg`` with a margin, saved as ``filename``. */
function padRaster(blob: Blob, bg: string, mimeType: string, quality: number, filename: string) {
  const url = URL.createObjectURL(blob);
  const img = new Image();
  img.onload = () => {
    const canvas = document.createElement("canvas");
    canvas.width = img.width + PAD * 2;
    canvas.height = img.height + PAD * 2;
    const ctx = canvas.getContext("2d")!;
    ctx.fillStyle = bg;
    ctx.fillRect(0, 0, canvas.width, canvas.height);
    ctx.drawImage(img, PAD, PAD);
    URL.revokeObjectURL(url);
    canvas.toBlob((b) => b && downloadBlob(b, filename), mimeType, quality);
  };
  img.src = url;
}

export function exportSvg(cy: CyInstance, bg: string, name: string): void {
  const raw = cy.svg({ full: true, bg }) as string;
  // The margin: the image widened and heightened, its content moved in by it.
  const padded = raw
    .replace(
      /(<svg[^>]*\swidth="(\d+(?:\.\d+)?)"[^>]*\sheight="(\d+(?:\.\d+)?)"[^>]*>)/,
      (_match: string, tag: string, w: string, h: string) => {
        const nw = parseFloat(w) + PAD * 2;
        const nh = parseFloat(h) + PAD * 2;
        return (
          tag.replace(/width="[^"]*"/, `width="${nw}"`).replace(/height="[^"]*"/, `height="${nh}"`) +
          `<rect width="${nw}" height="${nh}" fill="${bg}"/><g transform="translate(${PAD},${PAD})">`
        );
      },
    )
    .replace("</svg>", "</g></svg>");
  downloadBlob(new Blob([padded], { type: "image/svg+xml" }), `${name}.svg`);
}

export function exportPng(cy: CyInstance, bg: string, name: string): void {
  padRaster(
    cy.png({ output: "blob", full: true, bg }) as unknown as Blob,
    bg,
    "image/png",
    1,
    `${name}.png`,
  );
}

export function exportJpeg(cy: CyInstance, bg: string, name: string): void {
  padRaster(
    cy.jpg({ output: "blob", full: true, bg, quality: 0.92 }) as unknown as Blob,
    bg,
    "image/jpeg",
    0.92,
    `${name}.jpg`,
  );
}

export function exportJson(body: unknown, name: string): void {
  downloadBlob(
    new Blob([JSON.stringify(body, null, 2)], { type: "application/json" }),
    `${name}.json`,
  );
}
