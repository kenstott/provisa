// Copyright (c) 2026 Kenneth Stott
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// The renderer as the MCP server runs it: a request on stdin, a PNG on stdout, one process per
// call. Run after `npm run build`.

import { spawnSync } from "node:child_process";
import { fileURLToPath } from "node:url";
import { before, test } from "node:test";
import assert from "node:assert/strict";
import { createCanvas, loadImage } from "canvas";
import { build } from "esbuild";

const RENDER = fileURLToPath(new URL("../dist/render.mjs", import.meta.url));
// The renderer built as `npm run build` builds it, but with an elkjs whose every answer is a
// failure (test/elk-fails.mjs): the ELK layout's failure, which no request can cause on its own.
const RENDER_ELK_FAILS = fileURLToPath(new URL("../dist/render-elk-fails.mjs", import.meta.url));
const PACKAGE = fileURLToPath(new URL("..", import.meta.url));
const PNG_SIGNATURE = Buffer.from([0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a]);

const elements = {
  nodes: [
    { data: { id: "Sales:Customer:1", label: "Sales:Customer", properties: { name: "Ada" } } },
    { data: { id: "Sales:Order:7", label: "Sales:Order", properties: { total: 10.5 } } },
    { data: { id: "Sales:Order:8", label: "Sales:Order", properties: { total: 3 } } },
  ],
  edges: [
    {
      data: {
        id: "70",
        source: "Sales:Customer:1",
        target: "Sales:Order:7",
        label: "PLACED",
        properties: {},
      },
    },
    {
      data: {
        id: "71",
        source: "Sales:Customer:1",
        target: "Sales:Order:8",
        label: "PLACED",
        properties: {},
      },
    },
  ],
};

function render(request, renderer = RENDER) {
  return spawnSync(process.execPath, [renderer], {
    input: JSON.stringify(request),
    timeout: 60_000,
  });
}

before(async () => {
  await build({
    absWorkingDir: PACKAGE,
    entryPoints: ["src/main.ts"],
    bundle: true,
    platform: "node",
    format: "esm",
    target: "node22",
    outfile: RENDER_ELK_FAILS,
    external: ["canvas", "jsdom"],
    plugins: [
      {
        name: "elk-fails",
        setup(b) {
          b.onResolve({ filter: /^elkjs(\/|$)/ }, () => ({
            path: fileURLToPath(new URL("./elk-fails.mjs", import.meta.url)),
          }));
        },
      },
    ],
    logLevel: "silent",
  });
});

async function cornerPixel(png) {
  const image = await loadImage(png);
  const canvas = createCanvas(image.width, image.height);
  const ctx = canvas.getContext("2d");
  ctx.drawImage(image, 0, 0);
  const [r, g, b] = ctx.getImageData(0, 0, 1, 1).data;
  return `#${[r, g, b].map((v) => v.toString(16).padStart(2, "0")).join("")}`;
}

for (const layout of ["force", "hierarchy", "elk"]) {
  for (const [theme, background] of [
    ["light", "#ffffff"],
    ["dark", "#0f1117"],
  ]) {
    test(`${layout} on ${theme}: a PNG on the page's background for the theme`, async () => {
      const out = render({ elements, layout, theme, width: 800, height: 600 });
      assert.equal(out.status, 0, out.stderr.toString());
      assert.deepEqual(out.stdout.subarray(0, 8), PNG_SIGNATURE);
      assert.equal(await cornerPixel(out.stdout), background);
    });
  }
}

test("an unknown layout or theme fails, saying which", () => {
  for (const [request, says] of [
    [
      { elements, layout: "circle", theme: "light", width: 100, height: 100 },
      /unknown layout "circle"/,
    ],
    [
      { elements, layout: "force", theme: "sepia", width: 100, height: 100 },
      /unknown theme "sepia"/,
    ],
  ]) {
    const out = render(request);
    assert.notEqual(out.status, 0);
    assert.match(out.stderr.toString(), says);
  }
});

test("a node whose id is not <label>:<id> fails, naming it", () => {
  const bad = {
    nodes: [{ data: { id: "x", label: "Sales:Customer", properties: {} } }],
    edges: [],
  };
  const out = render({ elements: bad, layout: "force", theme: "light", width: 100, height: 100 });
  assert.notEqual(out.status, 0);
  assert.match(out.stderr.toString(), /node "x" is not <label>:<id>/);
});

test("an ELK layout that fails ends the drawing with ELK's error, not a PNG or a wait", () => {
  const out = render(
    { elements, layout: "elk", theme: "light", width: 800, height: 600 },
    RENDER_ELK_FAILS,
  );
  // A layout whose failure is dropped never stops: the renderer would wait out the timeout.
  assert.notEqual(out.signal, "SIGTERM", "the renderer waited on a layout that failed");
  assert.notEqual(out.status, 0);
  assert.equal(out.stdout.length, 0);
  assert.match(
    out.stderr.toString(),
    /the elk layout failed: UnsupportedGraphException: held to fail by the test/,
  );
});
