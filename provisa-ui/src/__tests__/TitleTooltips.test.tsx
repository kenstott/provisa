// Copyright (c) 2026 Kenneth Stott
// Canary: f70244ec-531b-46c2-a065-fc37605da28b
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.

import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";
import { render, screen, fireEvent, act } from "@testing-library/react";
import { TitleTooltips } from "../components/TitleTooltips";

describe("REQ-1520 native titles are painted by the app", () => {
  beforeEach(() => vi.useFakeTimers());
  afterEach(() => vi.useRealTimers());

  function mount() {
    return render(
      <>
        <TitleTooltips />
        <button title="Register a table">add</button>
      </>,
    );
  }

  it("replaces the browser tooltip with a themed one while hovering", () => {
    mount();
    const btn = screen.getByText("add");
    fireEvent.mouseOver(btn);
    expect(btn.hasAttribute("title")).toBe(false);
    act(() => void vi.advanceTimersByTime(400));
    expect(screen.getByTestId("app-tooltip").textContent).toBe("Register a table");
  });

  it("puts the accessible title back when the pointer leaves", () => {
    mount();
    const btn = screen.getByText("add");
    fireEvent.mouseOver(btn);
    act(() => void vi.advanceTimersByTime(400));
    fireEvent.mouseOut(btn, { relatedTarget: document.body });
    expect(btn.getAttribute("title")).toBe("Register a table");
    expect(screen.queryByTestId("app-tooltip")).toBeNull();
  });

  it("ignores elements with no title text", () => {
    render(
      <>
        <TitleTooltips />
        <button title="  ">bare</button>
      </>,
    );
    fireEvent.mouseOver(screen.getByText("bare"));
    act(() => void vi.advanceTimersByTime(400));
    expect(screen.queryByTestId("app-tooltip")).toBeNull();
  });
});
