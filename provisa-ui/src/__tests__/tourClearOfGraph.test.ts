// Copyright (c) 2026 Kenneth Stott
// Canary: 41c7e8b2-d6a3-49f1-8b70-2c9e5f1a7d34
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// A tour step on a page with a graph keeps its text off the graph: beside it first, then above or
// below it, and again whenever driver.js puts the popover back.

import { afterEach, describe, expect, it, vi } from "vitest";
import { clearPlace, keepClearOf } from "../tour/clearOfGraph";
import { demoTourSteps } from "../tour/demoTour";

const overlaps = (
  a: { left: number; top: number; width: number; height: number },
  b: { left: number; top: number; width: number; height: number },
) =>
  a.left < b.left + b.width &&
  b.left < a.left + a.width &&
  a.top < b.top + b.height &&
  b.top < a.top + a.height;

describe("where a popover goes around a graph", () => {
  const graph = { left: 376, top: 525, width: 880, height: 320 };
  const view = { width: 1280, height: 900 };

  it("sits beside the graph, narrowed to the room there, never over it", () => {
    const at = clearPlace({ width: 460, height: 190 }, graph, view);
    expect(at).not.toBeNull();
    expect(at?.width).toBe(344);
    expect(overlaps({ left: at!.left, top: at!.top, width: at!.width!, height: 190 }, graph)).toBe(false);
  });

  it("goes above where there is no room beside", () => {
    const wide = { left: 20, top: 500, width: 1240, height: 300 };
    const at = clearPlace({ width: 460, height: 190 }, wide, view);
    expect(at).not.toBeNull();
    expect(overlaps({ left: at!.left, top: at!.top, width: 460, height: 190 }, wide)).toBe(false);
  });

  it("says so when there is no room anywhere", () => {
    const all = { left: 0, top: 0, width: 1280, height: 900 };
    expect(clearPlace({ width: 460, height: 190 }, all, view)).toBeNull();
  });
});

describe("a step with a graph on its page", () => {
  afterEach(() => {
    document.body.innerHTML = "";
    vi.restoreAllMocks();
  });

  it("moves the popover clear of the graph, and again after a scroll puts it back", async () => {
    vi.spyOn(window, "requestAnimationFrame").mockImplementation((f) => {
      f(0);
      return 1;
    });
    document.body.innerHTML = '<div class="driver-popover"></div><div class="graph"></div>';
    const popover = document.querySelector<HTMLElement>(".driver-popover") as HTMLElement;
    const graph = document.querySelector<HTMLElement>(".graph") as HTMLElement;
    graph.scrollIntoView = vi.fn();
    Object.defineProperty(popover, "offsetWidth", { value: 300 });
    Object.defineProperty(popover, "offsetHeight", { value: 190 });
    graph.getBoundingClientRect = () => ({ left: 500, top: 400, width: 500, height: 300 }) as DOMRect;
    const abort = new AbortController();
    keepClearOf({ selector: ".graph", reveal: true }, abort.signal);
    expect(popover.style.left).toBe("184px");
    expect(popover.style.top).toBe("400px");
    expect(graph.scrollIntoView).toHaveBeenCalledOnce();
    popover.style.left = "600px";
    window.dispatchEvent(new Event("scroll"));
    expect(popover.style.left).toBe("184px");
    abort.abort();
    popover.style.left = "600px";
    window.dispatchEvent(new Event("scroll"));
    expect(popover.style.left).toBe("600px");
  });

  it("is asked of a named demo's term step and product step", () => {
    const steps = demoTourSteps([
      { key: "a", route: "/admin/ontology", term: "loss ratio" },
      { key: "b", route: "/data-products", product: "c360" },
    ] as never);
    expect(steps.map((s) => s.clearOf)).toEqual([
      { selector: ".g3d-panel-view", reveal: true },
      { selector: ".g3d-panel-view", reveal: false },
    ]);
  });
});
