// Copyright (c) 2026 Kenneth Stott
// Canary: b4e92a07-35d1-4c68-8f1e-0a7c6d2e5b19
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// The label pills of the graph explorers read in the light theme as in the dark: a pill with no
// label colour (a SPARQL property) takes its ground and text from the theme tokens, so the pill
// rules hold no literal colour, and the pill carries no inline colour.

import { describe, it, expect } from "vitest";
import { readFileSync } from "node:fs";
import { resolve } from "node:path";

const css = readFileSync(resolve(__dirname, "../../../pages/GraphPage.css"), "utf8");

function rule(selector: string): string {
  const at = css.indexOf(`\n${selector} {`);
  if (at < 0) throw new Error(`no rule ${selector}`);
  return css.slice(at, css.indexOf("}", at));
}

describe("graph pill rules", () => {
  for (const sel of [
    ".graph-label-pill",
    ".graph-label-pill--highlight",
    ".graph-rel-badge",
    ".graph-rel-badge:hover",
  ]) {
    it(`${sel} holds no literal colour`, () => {
      expect(rule(sel)).not.toMatch(/#[0-9a-fA-F]{3,8}\b|rgba?\(/);
    });
  }

  it("an uncoloured pill takes ground and text from tokens", () => {
    const r = rule(".graph-label-pill");
    expect(r).toMatch(/color:\s*var\(--text\)/);
    expect(r).toMatch(/background:\s*var\(--surface-alt\)/);
  });
});

