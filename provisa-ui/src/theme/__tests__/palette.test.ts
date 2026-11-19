// Copyright (c) 2026 Kenneth Stott
// Canary: a5cb79b9-8de5-431c-9ba1-bff864510846
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// The palette is the one colour source: Mantine's theme takes its scales from it, and the graph
// tokens (which the graph-render bundle builds without Mantine) read it directly. Its scales are
// Mantine's defaults, so a component and the graph draw the same gray, dark and hues.

import { readFileSync } from "node:fs";
import { resolve } from "node:path";
import { DEFAULT_THEME } from "@mantine/core";
import { describe, expect, it } from "vitest";
import { BRAND, SCALES } from "../palette";
import { theme } from "../theme";

describe("the palette", () => {
  it("holds Mantine's default scales, value for value", () => {
    for (const [name, scale] of Object.entries(SCALES)) {
      expect(scale, name).toEqual(DEFAULT_THEME.colors[name]);
    }
    expect(Object.keys(SCALES).sort()).toEqual(
      Object.keys(DEFAULT_THEME.colors).sort(),
    );
  });

  it("is what the Mantine theme is built from", () => {
    expect(theme.colors!.brand).toEqual(BRAND);
    for (const [name, scale] of Object.entries(SCALES)) {
      expect(theme.colors![name], name).toEqual(scale);
    }
  });

  it("imports no Mantine or React, so graph-render bundles it alone", () => {
    const src = readFileSync(resolve(__dirname, "../palette.ts"), "utf8");
    expect(src).not.toMatch(/^\s*import\s/m);
  });
});
