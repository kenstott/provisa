// Copyright (c) 2026 Kenneth Stott
// Canary: 2f6a8c13-9e4b-4d70-b25a-7c1e3d9f6a82
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2277: the Explore shell (its toolbar strip above its panes), a side panel whose right edge is
// dragged to resize it, an editor above its results whose divider is dragged to share the height,
// each kept per viewer; and the result panel: its strip (title, meta, views, actions) above the
// result edge to edge.

import { describe, it, expect, beforeEach, vi } from "vitest";
import { fireEvent, render, screen, within } from "../../../test-utils/render";
import { storageKey as scopedKey } from "../../../lib/browserStorage";
import { ExploreMain, ExploreShell, ExploreSide, ExploreStack, ResultPanel } from "..";

beforeEach(() => {
  localStorage.clear();
});

describe("ExploreShell", () => {
  it("puts the toolbar in a strip above the body and marks itself as the shell", () => {
    render(
      <ExploreShell testId="surface" className="own" toolbar={<span>tools</span>} answered>
        <ExploreMain>work</ExploreMain>
      </ExploreShell>,
    );
    const shell = screen.getByTestId("surface");
    expect(shell).toHaveAttribute("data-explore-shell");
    expect(shell).toHaveAttribute("data-answered");
    expect(shell).toHaveClass("explore-shell", "own");
    const toolbar = screen.getByTestId("surface-toolbar");
    expect(toolbar).toHaveTextContent("tools");
    expect(toolbar.nextElementSibling).toHaveClass("explore-shell__body");
    expect(toolbar.nextElementSibling).toHaveTextContent("work");
  });

  it("draws no toolbar strip when the surface has none", () => {
    render(<ExploreShell testId="surface">work</ExploreShell>);
    expect(screen.queryByTestId("surface-toolbar")).toBeNull();
  });
});

describe("ExploreSide", () => {
  const KEY = "provisa.test.sideWidth";
  const side = () =>
    render(
      <ExploreShell testId="surface">
        <ExploreSide storageKey={KEY} initial={300} min={200} max={600} testId="side">
          tree
        </ExploreSide>
        <ExploreMain>work</ExploreMain>
      </ExploreShell>,
    );

  it("starts at its initial width with the splitter at its right edge", () => {
    side();
    expect(screen.getByTestId("side").style.width).toBe("300px");
    const handle = screen.getByTestId("side-handle");
    expect(handle).toHaveAttribute("role", "separator");
    expect(handle).toHaveAttribute("aria-orientation", "vertical");
    expect(handle.previousElementSibling).toBe(screen.getByTestId("side"));
  });

  it("follows the pointer within its bounds and keeps the width", () => {
    side();
    const pane = screen.getByTestId("side");
    pane.getBoundingClientRect = vi.fn(() => ({ left: 100 }) as DOMRect);
    fireEvent.pointerDown(screen.getByTestId("side-handle"), { clientX: 400 });
    fireEvent.pointerMove(window, { clientX: 550 });
    expect(pane.style.width).toBe("450px");
    fireEvent.pointerMove(window, { clientX: 5000 });
    expect(pane.style.width).toBe("600px");
    fireEvent.pointerMove(window, { clientX: 0 });
    expect(pane.style.width).toBe("200px");
    fireEvent.pointerUp(window);
    expect(localStorage.getItem(scopedKey(KEY))).toBe("200");
  });

  it("is moved from the keyboard: right widens, left narrows", () => {
    side();
    const pane = screen.getByTestId("side");
    Object.defineProperty(pane, "offsetWidth", { configurable: true, value: 300 });
    fireEvent.keyDown(screen.getByTestId("side-handle"), { key: "ArrowRight" });
    expect(pane.style.width).toBe("332px");
    fireEvent.keyDown(screen.getByTestId("side-handle"), { key: "ArrowLeft" });
    expect(pane.style.width).toBe("268px");
  });

  it("opens at the width this viewer left it", () => {
    localStorage.setItem(scopedKey(KEY), "420");
    side();
    expect(screen.getByTestId("side").style.width).toBe("420px");
  });
});

describe("ExploreStack", () => {
  const KEY = "provisa.test.editorHeight";
  /** The results' least, ExploreShell.tsx's RESULTS_MIN. */
  const RESULTS_MIN = 240;
  it("shares the height between the editor and the results by dragging the divider", () => {
    render(
      <ExploreStack
        storageKey={KEY}
        initial={300}
        min={100}
        testId="stack"
        editor={<span>editor</span>}
        results={<span>results</span>}
      />,
    );
    const stack = screen.getByTestId("stack");
    const handle = screen.getByTestId("stack-handle");
    expect(handle).toHaveAttribute("aria-orientation", "horizontal");
    const editor = handle.previousElementSibling as HTMLElement;
    expect(editor).toHaveTextContent("editor");
    expect(handle.nextElementSibling).toHaveTextContent("results");
    expect(editor.style.height).toBe("300px");
    Object.defineProperty(stack, "clientHeight", { configurable: true, value: 700 });
    editor.getBoundingClientRect = vi.fn(() => ({ top: 50 }) as DOMRect);
    fireEvent.pointerDown(handle, { clientY: 350 });
    fireEvent.pointerMove(window, { clientY: 450 });
    expect(editor.style.height).toBe("400px");
    // The results keep their least (RESULTS_MIN) below.
    fireEvent.pointerMove(window, { clientY: 2000 });
    expect(editor.style.height).toBe(`${700 - RESULTS_MIN}px`);
    fireEvent.pointerUp(window);
    expect(localStorage.getItem(scopedKey(KEY))).toBe(String(700 - RESULTS_MIN));
  });

  // A stack shorter than the editor's kept height and the results' least: the editor gives way
  // (it shrinks to its own least), so the results still show their grid's header and rows.
  it("keeps the results' least on screen, not only on a drag", () => {
    render(
      <ExploreStack
        storageKey={KEY}
        initial={380}
        min={200}
        testId="stack"
        editor={<span>editor</span>}
        results={<span>results</span>}
      />,
    );
    const handle = screen.getByTestId("stack-handle");
    const editor = handle.previousElementSibling as HTMLElement;
    const results = handle.nextElementSibling as HTMLElement;
    expect(editor.style.height).toBe("380px");
    expect(editor.style.minHeight).toBe("200px");
    expect(results.style.minHeight).toBe(`${RESULTS_MIN}px`);
  });
});

describe("ResultPanel", () => {
  it("marks itself as the result panel and puts its body below the strip", () => {
    render(
      <ResultPanel testId="results" title="Response" meta="3 rows" actions={<button>copy</button>}>
        <span>grid</span>
      </ResultPanel>,
    );
    const panel = screen.getByTestId("results");
    expect(panel).toHaveAttribute("data-result-panel");
    const strip = panel.querySelector(".result-panel__strip")!;
    expect(strip).toHaveTextContent("Response");
    expect(strip).toHaveTextContent("3 rows");
    expect(within(strip as HTMLElement).getByRole("button", { name: "copy" })).toBeInTheDocument();
    expect(panel.querySelector(".result-panel__body")).toHaveTextContent("grid");
  });

  it("switches its views, with their counts", () => {
    const onViewChange = vi.fn();
    render(
      <ResultPanel
        testId="results"
        views={[
          { value: "table", label: "Table", count: 4, testId: "view-table" },
          { value: "raw", label: "Raw", testId: "view-raw" },
          { value: "errors", label: "Errors", count: 0, testId: "view-errors" },
        ]}
        view="table"
        onViewChange={onViewChange}
      >
        <span>grid</span>
      </ResultPanel>,
    );
    expect(screen.getByTestId("view-table")).toHaveAttribute("aria-selected", "true");
    expect(screen.getByTestId("view-table")).toHaveTextContent("4");
    expect(screen.getByTestId("view-errors")).not.toHaveTextContent("0");
    fireEvent.click(screen.getByTestId("view-raw"));
    expect(onViewChange).toHaveBeenCalledWith("raw");
  });

  it("draws no strip when it has nothing to put there", () => {
    render(
      <ResultPanel testId="results">
        <span>frames</span>
      </ResultPanel>,
    );
    expect(screen.getByTestId("results").querySelector(".result-panel__strip")).toBeNull();
  });
});
