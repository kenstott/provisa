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
// which must be the page's own: the graph tokens' canvas the Cypher graph is painted in.

import { describe, expect, it } from "vitest";
import { BACKGROUND, LAYOUTS } from "../../../../graph-render/src/look";
import { LAYOUT_OPTIONS } from "../canvas/canvas-types";
import { ERD_ELK_LAYOUT } from "../../provisa-graph/presets";
import { graphTokens } from "../../provisa-graph/tokens";

describe("what render_graph draws on and with", () => {
  it("draws each theme on the page's own background", () => {
    expect(BACKGROUND.light).toBe(graphTokens(false).canvas);
    expect(BACKGROUND.dark).toBe(graphTokens(true).canvas);
  });

  it("lays out with the graph page's force and hierarchy, and the ERD's ELK", () => {
    expect(LAYOUTS.force).toBe(LAYOUT_OPTIONS.force);
    expect(LAYOUTS.hierarchy).toBe(LAYOUT_OPTIONS.hierarchy);
    expect(LAYOUTS.elk).toBe(ERD_ELK_LAYOUT);
  });
});
