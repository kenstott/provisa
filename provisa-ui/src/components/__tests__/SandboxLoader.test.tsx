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
import { pickLoader, SANDBOX_LOADERS } from "../sandboxLoaderKind";

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

  it("maps an injected random source onto the three loaders", () => {
    expect(SANDBOX_LOADERS).toEqual(["cube", "box", "gift"]);
    expect(pickLoader(() => 0)).toBe("cube");
    expect(pickLoader(() => 0.34)).toBe("box");
    expect(pickLoader(() => 0.67)).toBe("gift");
    expect(pickLoader(() => 0.999999)).toBe("gift");
    expect(pickLoader(() => 1)).toBe("gift");
  });

  it("covers every loader across a seeded run", () => {
    let s = 7;
    const seeded = () => ((s = (s * 48271) % 2147483647) / 2147483647);
    const seen = new Set(Array.from({ length: 60 }, () => pickLoader(seeded)));
    expect(seen).toEqual(new Set(SANDBOX_LOADERS));
  });

  it.each([
    [0, 27, ".ac-cube"],
    [0.5, 4, ".ub-flap"],
    [0.9, 4, ".ub-rib"],
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
    const random = vi.fn().mockReturnValueOnce(0.5).mockReturnValueOnce(0.9);
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

  it.each(["box", "gift"] as const)("the %s stands still and open when motion is reduced", async (kind) => {
    prefersReduced(true);
    const { container } = render(<SandboxLoader label="Opening" kind={kind} />);
    await waitFor(() =>
      expect(screen.getByRole("status", { name: "Opening" })).toHaveAttribute("data-motion", "reduced"),
    );
    expect(container.querySelector("svg")).toHaveClass("ub--still");
    expect(container.querySelector("svg")).not.toHaveClass("ub--moving");
  });

  it.each(["box", "gift"] as const)("the %s moves when motion is allowed", (kind) => {
    prefersReduced(false);
    const { container } = render(<SandboxLoader label="Opening" kind={kind} />);
    expect(container.querySelector("svg")).toHaveClass("ub--moving");
  });

  it("is as wide as asked", () => {
    prefersReduced(false);
    const { container } = render(<SandboxLoader label="x" kind="box" size={120} />);
    expect(container.querySelector("svg")).toHaveAttribute("width", "120");
  });
});
