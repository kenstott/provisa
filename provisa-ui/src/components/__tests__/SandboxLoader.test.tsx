// Copyright (c) 2026 Kenneth Stott
// Canary: 54d3b112-7ab2-4e4e-a1f3-bcf84d3f8ab1
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.
import { render, screen, waitFor } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";
import { SandboxLoader } from "../SandboxLoader";
import { loaderTitleKey, pickLoader, SANDBOX_LOADERS } from "../sandboxLoaderKind";
import { demoOpenPage } from "../../i18n/locales/en/demoOpenPage.json";

const prefersReduced = (reduce: boolean) =>
  vi.spyOn(window, "matchMedia").mockImplementation(
    (query: string) =>
      ({
        matches: reduce && query.includes("prefers-reduced-motion: reduce"),
        media: query,
        onchange: null,
        addListener: () => {},
        removeListener: () => {},
        addEventListener: () => {},
        removeEventListener: () => {},
        dispatchEvent: () => false,
      }) as MediaQueryList,
  );

const kindOf = (c: HTMLElement) => c.querySelector("[data-loader]")!.getAttribute("data-loader");

describe("the sandbox loader's random choice", () => {
  afterEach(() => vi.restoreAllMocks());

  it("maps an injected random source onto the six loaders", () => {
    expect(SANDBOX_LOADERS).toEqual(["cube", "box", "gift", "pot", "plant", "score"]);
    expect(pickLoader(() => 0)).toBe("cube");
    expect(pickLoader(() => 0.17)).toBe("box");
    expect(pickLoader(() => 0.34)).toBe("gift");
    expect(pickLoader(() => 0.5)).toBe("pot");
    expect(pickLoader(() => 0.67)).toBe("plant");
    expect(pickLoader(() => 0.84)).toBe("score");
    expect(pickLoader(() => 0.999999)).toBe("score");
    expect(pickLoader(() => 1)).toBe("score");
  });

  it("names each animation's own title, and the unboxings keep the page's", () => {
    expect(SANDBOX_LOADERS.map(loaderTitleKey)).toEqual([
      "demoOpenPage.titleMaking",
      "demoOpenPage.title",
      "demoOpenPage.title",
      "demoOpenPage.titleCooking",
      "demoOpenPage.titleGrowing",
      "demoOpenPage.titleComposing",
    ]);
    expect(demoOpenPage.titleMaking).toBe("Making your demo sandbox");
    expect(demoOpenPage.titleCooking).toBe("Cooking up your demo sandbox");
    expect(demoOpenPage.titleGrowing).toBe("Growing your demo sandbox");
    expect(demoOpenPage.titleComposing).toBe("Composing your demo sandbox");
  });

  it("covers every loader across a seeded run", () => {
    let s = 7;
    const seeded = () => (s = (s * 48271) % 2147483647) / 2147483647;
    const seen = new Set(Array.from({ length: 60 }, () => pickLoader(seeded)));
    expect(seen).toEqual(new Set(SANDBOX_LOADERS));
  });

  it.each([
    [0, 27, ".ac-cube"],
    [0.2, 4, ".ub-flap"],
    [0.4, 4, ".ub-rib"],
    [0.55, 3, ".wl-drop"],
    [0.7, 1, ".wl-stem"],
    [0.9, 4, ".wl-note"],
  ])("with random %s it shows the matching loader", (r, count, selector) => {
    prefersReduced(false);
    const { container } = render(
      <SandboxLoader label="Opening your demo" testId="loader" random={() => r} />,
    );
    expect(screen.getByRole("status", { name: "Opening your demo" })).toBe(
      screen.getByTestId("loader"),
    );
    expect(container.querySelectorAll(selector)).toHaveLength(count);
    expect(container.querySelector("svg")).toHaveAttribute("aria-hidden", "true");
  });

  it("keeps its choice across re-renders and draws again on a new mount", () => {
    prefersReduced(false);
    const random = vi.fn().mockReturnValueOnce(0.2).mockReturnValueOnce(0.4);
    const { container, rerender, unmount } = render(<SandboxLoader label="x" random={random} />);
    expect(kindOf(container)).toBe("box");
    rerender(<SandboxLoader label="x" random={random} />);
    expect(kindOf(container)).toBe("box");
    expect(random).toHaveBeenCalledTimes(1);
    unmount();
    const again = render(<SandboxLoader label="x" random={random} />);
    expect(kindOf(again.container)).toBe("gift");
  });

  it("can be forced to one loader", () => {
    prefersReduced(false);
    const { container } = render(<SandboxLoader label="x" kind="gift" random={() => 0} />);
    expect(kindOf(container)).toBe("gift");
  });

  it.each(["box", "gift"] as const)(
    "the %s stands still and open when motion is reduced",
    async (kind) => {
      prefersReduced(true);
      const { container } = render(<SandboxLoader label="Opening" kind={kind} />);
      await waitFor(() =>
        expect(screen.getByRole("status", { name: "Opening" })).toHaveAttribute(
          "data-motion",
          "reduced",
        ),
      );
      expect(container.querySelector("svg")).toHaveClass("ub--still");
      expect(container.querySelector("svg")).not.toHaveClass("ub--moving");
    },
  );

  it.each(["box", "gift"] as const)("the %s moves when motion is allowed", (kind) => {
    prefersReduced(false);
    const { container } = render(<SandboxLoader label="Opening" kind={kind} />);
    expect(container.querySelector("svg")).toHaveClass("ub--moving");
  });

  it.each(["pot", "plant", "score"] as const)(
    "the %s stands still when motion is reduced",
    async (kind) => {
      prefersReduced(true);
      const { container } = render(<SandboxLoader label="Opening" kind={kind} />);
      await waitFor(() =>
        expect(screen.getByRole("status", { name: "Opening" })).toHaveAttribute(
          "data-motion",
          "reduced",
        ),
      );
      expect(container.querySelector("svg")).toHaveClass("wl--still");
      expect(container.querySelector("svg")).not.toHaveClass("wl--moving");
    },
  );

  it.each(["pot", "plant", "score"] as const)("the %s moves when motion is allowed", (kind) => {
    prefersReduced(false);
    const { container } = render(<SandboxLoader label="Opening" kind={kind} />);
    expect(container.querySelector("svg")).toHaveClass("wl--moving");
  });

  it("grows the plant with the progress it is given, and still does when motion is reduced", () => {
    const grown = (c: HTMLElement) =>
      c.querySelector('[data-testid="plant-growth"]')!.getAttribute("data-grown");
    const stem = (c: HTMLElement) => c.querySelector(".wl-stem")!.getAttribute("d");
    prefersReduced(false);
    const { container, rerender } = render(<SandboxLoader label="x" kind="plant" progress={0} />);
    expect(grown(container)).toBe("0.00");
    expect(container.querySelector(".wl-seed")).toBeInTheDocument();
    expect(container.querySelector(".wl-can")).toBeInTheDocument();
    const seedling = stem(container);
    rerender(<SandboxLoader label="x" kind="plant" progress={40} />);
    expect(grown(container)).toBe("0.40");
    expect(stem(container)).not.toBe(seedling);
    expect(container.querySelector(".wl-seed")).toBeNull();
    rerender(<SandboxLoader label="x" kind="plant" progress={95} />);
    expect(grown(container)).toBe("0.95");
    expect(container.querySelector(".wl-can")).toBeNull();
    expect(container.querySelector(".wl-flower")).toBeInTheDocument();
    rerender(<SandboxLoader label="x" kind="plant" progress={140} />);
    expect(grown(container)).toBe("1.00");
  });

  it("is as wide as asked", () => {
    prefersReduced(false);
    const { container } = render(<SandboxLoader label="x" kind="box" size={120} />);
    expect(container.querySelector("svg")).toHaveAttribute("width", "120");
  });
});
