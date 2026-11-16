// Copyright (c) 2026 Kenneth Stott
// Canary: 9a3f21c7-6f0b-4f4e-9a7a-2d1c5b8e4f30
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

/**
 * Resuming the tour enters at the saved index directly, so every index must resolve to a page to
 * navigate to. Steps that continue on their predecessor's page omit `route`; entering one of those
 * without resolving the inherited route leaves the visitor on whatever page they were on, its
 * anchor never appears, and the launch button looks dead.
 */

import { describe, it, expect } from "vitest";
import { TOUR_STEPS, showsRoute, stepRoute } from "../tour/tourSteps";

describe("tour step route resolution", () => {
  it("resolves a route for every step index", () => {
    for (let i = 0; i < TOUR_STEPS.length; i++) {
      expect(stepRoute(i), `step ${i} (${TOUR_STEPS[i].key})`).toBeTruthy();
    }
  });

  it("uses a step's own route when it declares one", () => {
    for (let i = 0; i < TOUR_STEPS.length; i++) {
      const own = TOUR_STEPS[i].route;
      if (own) expect(stepRoute(i)).toBe(own);
    }
  });

  it("inherits the nearest preceding route for a step that omits one", () => {
    const inherited = TOUR_STEPS.map((_, i) => i).filter((i) => !TOUR_STEPS[i].route);
    expect(inherited.length).toBeGreaterThan(0);
    for (const i of inherited) {
      expect(stepRoute(i)).toBe(stepRoute(i - 1));
    }
  });
});

// REQ-2241: a page's open panel is in its URL. The page a route names is still the one shown with a
// panel open, and a step continuing on it must not navigate to the bare route, closing the panel.
describe("the page a step's route names", () => {
  it("is shown with the parameters of a panel open beside the route's own", () => {
    expect(
      showsRoute(
        "/tables?source=pet-store-sqlite",
        "/tables",
        "?source=pet-store-sqlite&table=296&section=quality",
      ),
    ).toBe(true);
    expect(
      showsRoute("/tables?source=pet-store-sqlite", "/tables", "?source=pet-store-sqlite"),
    ).toBe(true);
    expect(showsRoute("/sources", "/sources", "?expanded=3")).toBe(true);
  });

  it("is not shown on another path, or where a parameter the route names differs or is missing", () => {
    expect(
      showsRoute("/tables?source=pet-store-sqlite", "/views", "?source=pet-store-sqlite"),
    ).toBe(false);
    expect(
      showsRoute("/tables?source=pet-store-sqlite", "/tables", "?source=dq-checker&table=302"),
    ).toBe(false);
    expect(showsRoute("/tables?source=pet-store-sqlite", "/tables", "?table=296")).toBe(false);
  });
});
