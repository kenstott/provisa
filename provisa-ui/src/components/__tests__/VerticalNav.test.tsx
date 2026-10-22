// Copyright (c) 2026 Kenneth Stott
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// The one vertical menu: named, its items buttons the keyboard reaches, the chosen one current.

import { describe, expect, it, vi } from "vitest";
import { fireEvent, render, screen } from "../../test-utils/render";
import { VerticalNav } from "../VerticalNav";

const ITEMS = [
  { value: "a", label: "Alpha", testId: "nav-a" },
  { value: "b", label: "Beta", testId: "nav-b" },
];

describe("the vertical menu", () => {
  it("names itself, marks the chosen item and reports a pick", () => {
    const onChange = vi.fn();
    render(<VerticalNav items={ITEMS} value="a" onChange={onChange} label="Results" />);
    const nav = screen.getByRole("navigation", { name: "Results" });
    expect(nav).toHaveClass("vertical-nav");
    expect(screen.getByTestId("nav-a")).toHaveAttribute("aria-current", "true");
    expect(screen.getByTestId("nav-a")).toHaveClass("active");
    expect(screen.getByTestId("nav-b")).not.toHaveAttribute("aria-current");
    fireEvent.click(screen.getByRole("button", { name: "Beta" }));
    expect(onChange).toHaveBeenCalledWith("b");
  });

  it("is reached by the keyboard", () => {
    render(<VerticalNav items={ITEMS} value="a" onChange={vi.fn()} label="Results" />);
    const beta = screen.getByRole("button", { name: "Beta" });
    beta.focus();
    expect(beta).toHaveFocus();
  });
});
