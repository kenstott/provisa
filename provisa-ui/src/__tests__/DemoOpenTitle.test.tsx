// Copyright (c) 2026 Kenneth Stott
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { screen } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";
import { Route, Routes } from "react-router-dom";
import { render } from "../test-utils/render";
import { DemoOpenPage } from "../pages/demos/DemoOpenPage";

vi.mock("../api/demoSandbox", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../api/demoSandbox")>()),
  // Never answers: the page stays on its loader.
  openDemoSandbox: vi.fn(() => new Promise(() => {})),
}));

function openOn(random: number) {
  vi.spyOn(Math, "random").mockReturnValue(random);
  const { container } = render(
    <Routes>
      <Route path="/demos/:name" element={<DemoOpenPage />} />
    </Routes>,
    { initialEntries: ["/demos/weblogs"] },
  );
  return container.querySelector("[data-loader]")!.getAttribute("data-loader");
}

describe("the demo sandbox screen's title", () => {
  afterEach(() => vi.restoreAllMocks());

  it.each([
    [0, "cube", "Making your demo sandbox"],
    [0.2, "box", "Opening your demo sandbox"],
    [0.4, "gift", "Opening your demo sandbox"],
    [0.55, "pot", "Cooking up your demo sandbox"],
    [0.7, "plant", "Growing your demo sandbox"],
    [0.9, "score", "Composing your demo sandbox"],
  ])("with random %s, the %s loader goes with its own title", (random, kind, title) => {
    expect(openOn(random)).toBe(kind);
    expect(screen.getByRole("heading", { level: 2 })).toHaveTextContent(title);
  });
});
