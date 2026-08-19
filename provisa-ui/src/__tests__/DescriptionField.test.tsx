// Copyright (c) 2026 Kenneth Stott
// Canary: 39d495f3-b737-40b0-9d11-cab151560282
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { describe, it, expect, vi } from "vitest";
import { render, screen, fireEvent } from "../test-utils/render";
import { DescriptionField } from "../pages/tables/DescriptionField";

/** jsdom reports every rect as zero; pin the collapsed field's box so the
 *  anchor decision has real geometry to read. */
function stubRect(el: Element, top: number, height: number) {
  vi.spyOn(el, "getBoundingClientRect").mockReturnValue({
    top,
    bottom: top + height,
    left: 0,
    right: 400,
    width: 400,
    height,
    x: 0,
    y: top,
    toJSON: () => ({}),
  } as DOMRect);
}

describe("DescriptionField expansion anchor", () => {
  it("grows upward when 300px fits above the field", () => {
    render(<DescriptionField value="" onChange={() => {}} />);
    const ta = screen.getByRole("textbox");
    stubRect(ta, 600, 60);
    fireEvent.focus(ta);
    const root = ta.closest(".mantine-Textarea-root") as HTMLElement;
    expect(root.style.bottom).toBe("0px");
    expect(root.style.top).toBe("");
  });

  it("grows downward when the field is too near the top to fit above it", () => {
    render(<DescriptionField value="" onChange={() => {}} />);
    const ta = screen.getByRole("textbox");
    stubRect(ta, 40, 60);
    fireEvent.focus(ta);
    const root = ta.closest(".mantine-Textarea-root") as HTMLElement;
    expect(root.style.top).toBe("0px");
    expect(root.style.bottom).toBe("");
  });
});
