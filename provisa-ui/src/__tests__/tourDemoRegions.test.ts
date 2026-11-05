// Copyright (c) 2026 Kenneth Stott
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1922, REQ-2035: a demo's steps on the pages where regions are defined -- the Platform page,
// Admin > Regions, Federation's tab per region and a table's region -- and the Platform page's step
// kept for a demo sandbox's visitor, who may read it without its right.
import { describe, it, expect } from "vitest";
import { demoTourSteps } from "../tour/demoTour";
import { tourItinerary } from "../tour/tourSteps";
import { viewerMeets } from "../tour/viewerMeets";

const EXAMPLES = [{ name: "1-group-customers", query: "query GroupCustomers { a }" }];

describe("a demo's steps where regions are defined", () => {
  it("opens a table's Identity on one field, its region, with no column tab", () => {
    const [field] = demoTourSteps(
      [
        {
          key: "h",
          route: "/tables",
          capability: "table_registration",
          table: "eu-shop.customers",
          open: "identity.region",
        },
      ],
      EXAMPLES,
    );
    expect(field.route).toBe("/tables?open=eu-shop.customers&section=identity");
    expect(field.element).toBe('[data-testid="table-identity-region"]');
  });

  it("points a region at its row or its Federation tab, and the purposes at their legend", () => {
    const [platform, legend, org, engines] = demoTourSteps(
      [
        { key: "p", route: "/admin/platform", capability: "platform_settings", region: "eu" },
        { key: "l", purposes: true },
        { key: "o", route: "/admin/regions", capability: "org_settings", region: "eu" },
        { key: "e", route: "/admin/org-engine", capability: "org_settings", region: "eu" },
      ],
      EXAMPLES,
    );
    expect(platform).toEqual({
      key: "p",
      route: "/admin/platform",
      capability: "platform_settings",
      element: '[data-testid="platform-region-row-eu"]',
    });
    expect(legend).toEqual({ key: "l", element: '[data-testid="region-purposes-legend"]' });
    expect(org.element).toBe('[data-testid="org-region-row-eu"]');
    // Federation's tab for the region is picked, so the page shows that region's engine.
    expect(engines.clickBefore).toBe('[data-testid="federation-region-tab-eu"]');
    expect(engines.element).toBe('[data-testid="federation-region-tab-eu"]');
  });

  it("refuses a region or the purposes off a page that lists regions", () => {
    const sources = { route: "/sources", capability: "source_registration" as const };
    expect(() => demoTourSteps([{ key: "r", ...sources, region: "eu" }], EXAMPLES)).toThrow(
      /shows region "eu" off a regions page/,
    );
    expect(() =>
      demoTourSteps(
        [{ key: "f", route: "/admin/org-engine", capability: "org_settings", purposes: true }],
        EXAMPLES,
      ),
    ).toThrow(/shows the region purposes off a regions page/);
  });

  it("keeps the Platform page's step for a demo sandbox's visitor, without its right", () => {
    const steps = demoTourSteps(
      [
        { key: "p", route: "/admin/platform", capability: "platform_settings", region: "eu" },
        { key: "l", purposes: true },
        { key: "o", route: "/admin/regions", capability: "org_settings", region: "eu" },
      ],
      EXAMPLES,
    );
    const rights = ["org_settings"];
    // A sandbox reads the Platform page; anywhere else its right gates it, with its legend.
    expect(tourItinerary(viewerMeets(rights, "sbx0123abcd"), undefined, steps)).toEqual([0, 1, 2]);
    expect(tourItinerary(viewerMeets(rights, "acme"), undefined, steps)).toEqual([2]);
    // A demo's template org is not a visitor's sandbox.
    expect(tourItinerary(viewerMeets(rights, "sbxtplretail"), undefined, steps)).toEqual([2]);
  });
});
