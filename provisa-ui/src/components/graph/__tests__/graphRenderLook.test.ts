// Copyright (c) 2026 Kenneth Stott
// Canary: 31234032-153d-48a2-89d3-f54108242ed6
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// @vitest-environment node

// The MCP server's render_graph draws with the graph page's own stylesheet and layouts
// (graph-render/src/draw.ts imports them). What it adds is the background each theme draws on,
// which must be the page's own: the --bg the transparent graph canvas sits on.

import { readFileSync } from "node:fs";
import { resolve } from "node:path";
import { describe, expect, it } from "vitest";
import { BACKGROUND, LAYOUTS } from "../../../../graph-render/src/look";
import { LAYOUT_OPTIONS } from "../canvas/canvas-types";
import { ERD_ELK_LAYOUT } from "../../provisa-graph/presets";

const tokens = readFileSync(resolve(__dirname, "../../../theme/tokens.css"), "utf8");

function pageBackground(scheme: "light" | "dark"): string {
  const at = tokens.indexOf(`:root[data-mantine-color-scheme="${scheme}"] {`);
  expect(at, scheme).toBeGreaterThan(-1);
  const block = tokens.slice(at, tokens.indexOf("}", at));
  const bg = /--bg:\s*(#[0-9a-fA-F]{6});/.exec(block);
  expect(bg, `${scheme} --bg`).not.toBeNull();
  return bg![1].toLowerCase();
}

describe("what render_graph draws on and with", () => {
  it("draws each theme on the page's own background", () => {
    expect(BACKGROUND.light).toBe(pageBackground("light"));
    expect(BACKGROUND.dark).toBe(pageBackground("dark"));
  });

  it("lays out with the graph page's force and hierarchy, and the ERD's ELK", () => {
    expect(LAYOUTS.force).toBe(LAYOUT_OPTIONS.force);
    expect(LAYOUTS.hierarchy).toBe(LAYOUT_OPTIONS.hierarchy);
    expect(LAYOUTS.elk).toBe(ERD_ELK_LAYOUT);
  });
});
