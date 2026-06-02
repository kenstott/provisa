// Copyright (c) 2026 Kenneth Stott
// Canary: 80ba4c56-0c4e-4949-a66a-731b71319aab
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import type { CyInstance } from "./cytoscape-types";

export function downloadBlob(blob: Blob, filename: string) {
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = filename;
  a.click();
  URL.revokeObjectURL(url);
}

export function compositeGraphDownload(
  cy: CyInstance,
  hullSvg: SVGSVGElement | null,
  filename: string,
  format: "png" | "jpg",
) {
  const bg = format === "jpg" ? "white" : "transparent";
  const mimeType = format === "jpg" ? "image/jpeg" : "image/png";
  const hasHulls = hullSvg && hullSvg.children.length > 0;

  if (!hasHulls) {
    const blob = (format === "jpg"
      ? cy.jpg({ output: "blob", bg, full: true })
      : cy.png({ output: "blob", bg, full: true })) as unknown as Blob;
    downloadBlob(blob, filename);
    return;
  }

  // Hull coords are renderedPosition()-based (viewport pixels). Capture viewport only.
  const dataUrl = (
    format === "jpg" ? cy.jpg({ output: "dataURL", bg }) : cy.png({ output: "dataURL", bg })
  ) as string;

  const img = new Image();
  img.onload = () => {
    const canvas = document.createElement("canvas");
    canvas.width = img.width;
    canvas.height = img.height;
    const ctx = canvas.getContext("2d")!;
    ctx.drawImage(img, 0, 0);

    // viewBox maps hull CSS-pixel coords → PNG physical pixels (handles devicePixelRatio)
    const container = cy.container();
    const cssW = container.offsetWidth;
    const cssH = container.offsetHeight;
    const svgClone = hullSvg!.cloneNode(true) as SVGSVGElement;
    svgClone.setAttribute("viewBox", `0 0 ${cssW} ${cssH}`);
    svgClone.setAttribute("width", String(img.width));
    svgClone.setAttribute("height", String(img.height));

    const svgData = new XMLSerializer().serializeToString(svgClone);
    const svgBlob = new Blob([svgData], { type: "image/svg+xml;charset=utf-8" });
    const svgUrl = URL.createObjectURL(svgBlob);
    const svgImg = new Image();
    const finish = () => {
      URL.revokeObjectURL(svgUrl);
      canvas.toBlob((b) => {
        if (b) downloadBlob(b, filename);
      }, mimeType);
    };
    svgImg.onload = () => {
      ctx.drawImage(svgImg, 0, 0, img.width, img.height);
      finish();
    };
    svgImg.onerror = finish;
    svgImg.src = svgUrl;
  };
  img.src = dataUrl;
}

export function downloadGraphSvg(cy: CyInstance, hullSvg: SVGSVGElement | null) {
  const hasHulls = hullSvg && hullSvg.children.length > 0;
  // Viewport SVG shares coord system with renderedPosition(); full=true would differ.
  const svgBase = cy.svg({ full: !hasHulls, bg: "transparent" });
  if (!hasHulls) {
    downloadBlob(new Blob([svgBase], { type: "image/svg+xml" }), "graph.svg");
    return;
  }
  const hullContent = Array.from(hullSvg!.children)
    .map((c) => new XMLSerializer().serializeToString(c))
    .join("");
  const composite = svgBase.replace("</svg>", `<g class="hulls">${hullContent}</g></svg>`);
  downloadBlob(new Blob([composite], { type: "image/svg+xml" }), "graph.svg");
}

export function toCSV(columns: string[], rows: Record<string, unknown>[]): string {
  const esc = (v: unknown) => {
    const s =
      v === null || v === undefined ? "" : typeof v === "object" ? JSON.stringify(v) : String(v);
    return s.includes(",") || s.includes('"') || s.includes("\n")
      ? `"${s.replace(/"/g, '""')}"`
      : s;
  };
  return [
    columns.map(esc).join(","),
    ...rows.map((r) => columns.map((c) => esc(r[c])).join(",")),
  ].join("\n");
}
