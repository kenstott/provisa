// Copyright (c) 2026 Kenneth Stott
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// A Related Tables entry's detail: three lines with an ellipsis, opened in place and closed again by
// a button that says whether it is open; detail that fits is shown with nothing to toggle. jsdom
// lays nothing out, so the detail's height is set here: 20px a line, the clamp showing three.

import { afterEach, describe, expect, it, vi } from "vitest";
import { fireEvent, render, screen } from "../../../test-utils/render";
import { ClampedDetail } from "../ClampedDetail";

function laidOut(lines: number) {
  vi.spyOn(HTMLElement.prototype, "scrollHeight", "get").mockReturnValue(lines * 20);
  vi.spyOn(HTMLElement.prototype, "clientHeight", "get").mockReturnValue(Math.min(lines, 3) * 20);
}

afterEach(() => vi.restoreAllMocks());

describe("a clamped detail", () => {
  it("opens in place and closes again, saying which it is", () => {
    laidOut(6);
    render(<ClampedDetail testId="related-1">A long description</ClampedDetail>);
    const toggle = screen.getByTestId("related-1");
    expect(toggle.tagName).toBe("BUTTON");
    expect(toggle).toHaveAttribute("aria-expanded", "false");
    expect(toggle).toHaveAccessibleName("Show the whole entry");
    expect(screen.getByTestId("related-1-text")).toHaveStyle({ overflow: "hidden" });
    fireEvent.click(toggle);
    expect(toggle).toHaveAttribute("aria-expanded", "true");
    expect(toggle).toHaveAccessibleName("Show less");
    expect(screen.getByTestId("related-1-text")).not.toHaveStyle({ overflow: "hidden" });
    fireEvent.click(toggle);
    expect(toggle).toHaveAttribute("aria-expanded", "false");
  });

  it("is reached by the keyboard", () => {
    laidOut(6);
    render(<ClampedDetail testId="related-1">A long description</ClampedDetail>);
    const toggle = screen.getByTestId("related-1");
    toggle.focus();
    expect(toggle).toHaveFocus();
  });

  it("shows detail that fits in three lines with nothing to toggle", () => {
    laidOut(2);
    render(<ClampedDetail testId="related-1">Short</ClampedDetail>);
    expect(screen.queryByTestId("related-1")).toBeNull();
    expect(screen.queryByRole("button")).toBeNull();
    expect(screen.getByTestId("related-1-text")).toHaveTextContent("Short");
  });
});
