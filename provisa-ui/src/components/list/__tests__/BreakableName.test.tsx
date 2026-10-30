// Copyright (c) 2026 Kenneth Stott
// Canary: c83b6e17-4d2a-4f95-a1e8-0b7d59f2c364
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// A listed name wraps at its whole width, breaking only after "_", "|" and ".", never cut with an
// ellipsis (the list rule: words wrap).

import { readFileSync } from "node:fs";
import { join } from "node:path";
import { describe, expect, it } from "vitest";
import { render } from "@testing-library/react";
import { BreakableName } from "../BreakableName";

const css = (file: string) => readFileSync(join(__dirname, "..", file), "utf8");
/** The declarations of ``selector``'s rule in ``text``. */
const rule = (text: string, selector: string) => {
  const at = text.indexOf(`${selector} {`);
  expect(at).toBeGreaterThanOrEqual(0);
  return text.slice(at, text.indexOf("}", at));
};

describe("a breakable name", () => {
  it("splits after each _, | and . and nowhere else", () => {
    const html = (name: string) =>
      (render(<BreakableName name={name} />).container.querySelector(".name-breakable") as HTMLElement)
        .innerHTML;
    expect(html("a|b.c_d")).toBe("a|<wbr>b.<wbr>c_<wbr>d");
    expect(html("sales-orders 2024")).toBe("sales-orders 2024");
  });

  it("renders a break opportunity after each _, | and ., the whole name kept", () => {
    const { container } = render(<BreakableName name="find_pets_by_status.status|v2" />);
    const span = container.querySelector(".name-breakable") as HTMLElement;
    expect(span.innerHTML).toBe(
      "find_<wbr>pets_<wbr>by_<wbr>status.<wbr>status|<wbr>v2",
    );
    expect(span.textContent).toBe("find_pets_by_status.status|v2");
  });

  it("puts no break opportunity in a name without _, | or .", () => {
    const { container } = render(<BreakableName name="customers" />);
    expect(container.querySelectorAll("wbr")).toHaveLength(0);
  });

  it("wraps and is never cut with an ellipsis", () => {
    const own = rule(css("BreakableName.css"), ".name-breakable");
    expect(own).toContain("white-space: normal");
    // Breaks only at its break points: no break inside a part.
    expect(own).toContain("overflow-wrap: normal");
    expect(own).toContain("word-break: normal");
    expect(own).not.toContain("ellipsis");
    const panel = rule(css("../graph3d/graph3d.css"), ".p3d-row-name.is-wrap");
    expect(panel).toContain("white-space: normal");
    expect(panel).toContain("text-overflow: clip");
  });
});
