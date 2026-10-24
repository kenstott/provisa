// Copyright (c) 2026 Kenneth Stott
// Canary: 332a44b7-17e2-40af-b54a-f7bf97c486c0
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { render, screen, waitFor } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";
import { AssemblingCube } from "../AssemblingCube";

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

describe("the assembling cube", () => {
  afterEach(() => vi.restoreAllMocks());

  it("is a status named by its label, its 27 small cubes hidden from screen readers", () => {
    prefersReduced(false);
    const { container } = render(<AssemblingCube label="Opening your demo" testId="cube" />);
    const status = screen.getByRole("status", { name: "Opening your demo" });
    expect(status).toBe(screen.getByTestId("cube"));
    expect(container.querySelector("svg")).toHaveAttribute("aria-hidden", "true");
    expect(container.querySelectorAll(".ac-cube")).toHaveLength(27);
    expect(status).toHaveAttribute("data-motion", "full");
    expect(container.querySelector("svg")).toHaveClass("ac--moving");
  });

  it("stands assembled and only pulses when motion is reduced", async () => {
    prefersReduced(true);
    const { container } = render(<AssemblingCube label="Opening your demo" />);
    await waitFor(() =>
      expect(screen.getByRole("status", { name: "Opening your demo" })).toHaveAttribute(
        "data-motion",
        "reduced",
      ),
    );
    expect(container.querySelector("svg")).toHaveClass("ac--still");
    expect(container.querySelector("svg")).not.toHaveClass("ac--moving");
  });

  it("is as wide as asked, its height in proportion", () => {
    prefersReduced(false);
    const { container } = render(<AssemblingCube label="x" size={110} />);
    const svg = container.querySelector("svg")!;
    expect(svg).toHaveAttribute("width", "110");
    expect(Number(svg.getAttribute("height"))).toBeCloseTo((110 * 6.4) / 5.5);
  });
});
