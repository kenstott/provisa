// Copyright (c) 2026 Kenneth Stott
// Canary: abb0846a-3470-4435-aff4-a3bc3d67c432
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2239: a view's side panel is hidden and shown again by the handle on its edge, and the
// choice is kept per view for the viewer.

import { beforeEach, describe, expect, it, vi } from "vitest";
import { fireEvent, render, screen } from "../../../test-utils/render";
import { SidePanel } from "../SidePanel";
import { readCollapsibleStates } from "../collapsibleState";

const panel = (view: string, onToggle?: (open: boolean) => void) => (
  <SidePanel view={view} onToggle={onToggle} testId="side">
    <div data-testid="side-content">detail</div>
  </SidePanel>
);

beforeEach(() => {
  window.localStorage.clear();
});

describe("SidePanel", () => {
  it("is shown until the viewer hides it", () => {
    render(panel("view-a"));
    expect(screen.getByTestId("side-content")).toBeInTheDocument();
    expect(screen.getByTestId("side-toggle")).toHaveAttribute("aria-expanded", "true");
    expect(screen.getByTestId("side")).not.toHaveClass("is-hidden");
  });

  it("hides and shows again by the handle on its edge, telling the view each time", () => {
    const onToggle = vi.fn();
    render(panel("view-a", onToggle));
    fireEvent.click(screen.getByTestId("side-toggle"));
    expect(screen.queryByTestId("side-content")).not.toBeInTheDocument();
    expect(screen.getByTestId("side")).toHaveClass("is-hidden");
    expect(screen.getByTestId("side-toggle")).toHaveAttribute("aria-expanded", "false");
    expect(screen.getByTestId("side-toggle")).toHaveAccessibleName("Show the panel");
    fireEvent.click(screen.getByTestId("side-toggle"));
    expect(screen.getByTestId("side-content")).toBeInTheDocument();
    expect(screen.getByTestId("side-toggle")).toHaveAccessibleName("Hide the panel");
    expect(onToggle.mock.calls).toEqual([[false], [true]]);
  });

  it("keeps the choice per view: hidden in one view, shown in another", () => {
    const first = render(panel("view-a"));
    fireEvent.click(screen.getByTestId("side-toggle"));
    first.unmount();
    expect(readCollapsibleStates()["side-panels"]).toEqual({ "view-a": false });

    const again = render(panel("view-a"));
    expect(screen.queryByTestId("side-content")).not.toBeInTheDocument();
    again.unmount();

    render(panel("view-b"));
    expect(screen.getByTestId("side-content")).toBeInTheDocument();
  });
});
