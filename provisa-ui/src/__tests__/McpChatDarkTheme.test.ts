// Copyright (c) 2026 Kenneth Stott
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { readFileSync } from "node:fs";
import { resolve } from "node:path";
import { describe, expect, it } from "vitest";

const css = readFileSync(resolve(__dirname, "../index.css"), "utf8");

/** The rule of the dark theme for chatscope's `surface`, inside the MCP explorer's chat. */
function darkRule(surface: string): string {
  const prefix = ':root[data-mantine-color-scheme="dark"] .mcp-chat-wrap ';
  const start = css.indexOf(`${prefix}${surface}`);
  expect(start, `no dark rule for ${surface}`).toBeGreaterThanOrEqual(0);
  return css.slice(css.indexOf("{", start), css.indexOf("}", start));
}

describe("the MCP explorer's chat in the dark theme", () => {
  // chatscope's own stylesheet paints each of these in a fixed light colour.
  it.each([
    ".cs-main-container",
    ".cs-chat-container",
    ".cs-message-list",
    ".cs-message-input",
    ".cs-message-input__content-editor-wrapper",
    ".cs-message-input__content-editor",
    ".cs-message--incoming .cs-message__content",
    ".cs-message--outgoing .cs-message__content",
  ])("%s takes the theme's background", (surface) => {
    expect(darkRule(surface)).toMatch(/background-color: var\(--/);
  });
});
