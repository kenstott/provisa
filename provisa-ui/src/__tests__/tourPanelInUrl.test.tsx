// Copyright (c) 2026 Kenneth Stott
// Canary: 5c2e8f17-3b9d-4a60-9e41-7d0b6a2f8c53
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

/**
 * REQ-2241: a list page's open panel is its URL (`/tables?source=…&table=…&section=…`). A tour card
 * that continues on its predecessor's page (no `route` of its own) inherits that page's route,
 * which names no panel. The runner took the page with a panel open for another page and navigated
 * to the bare route, which closed the panel the card explains: the Data quality topic's "Checks on
 * profile results" card came up over a closed table, its profiler panel gone
 * (tour-profiler-step.spec.ts, run 43725827456).
 */

import { describe, it, expect, vi, beforeEach } from "vitest";
import { useEffect } from "react";
import { useLocation, useSearchParams } from "react-router-dom";
import { render, screen, fireEvent, waitFor } from "../test-utils/render";

vi.mock("../hooks/useOrgHas", async () => {
  const { STANDARD_DEMO_ORG } = await import("../test-utils/tourOrg");
  const read = () => Promise.resolve(STANDARD_DEMO_ORG);
  return { useOrgHas: () => read };
});
vi.mock("../pageChunks", () => ({ prefetchAllPageChunks: () => Promise.resolve() }));
vi.mock("../hooks/useAdminQueries", () => ({ useTourPrefetch: () => () => Promise.resolve() }));
// The Tables steps warm /admin/settings; there is no server here.
vi.mock("../api/admin", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../api/admin")>()),
  prefetchSettings: () => {},
}));
vi.mock("../context/AuthContext", async () => {
  const { TOUR_STEPS } = await import("../tour/tourSteps");
  const capabilities = [
    ...new Set(TOUR_STEPS.flatMap((s) => (s.capability ? [s.capability] : []))),
  ];
  return { useAuth: () => ({ loading: false, capabilities }) };
});

const { TourProvider, useTour } = await import("../tour/useTour");

function Compass() {
  const { startTour } = useTour();
  return (
    <button type="button" onClick={() => startTour()}>
      launch
    </button>
  );
}

const visited: string[] = [];

/**
 * The Tables page as the quality topic reads it: a row opens its table's panel by writing `table`
 * into the URL, as useDetailParam does, and clicking the open row closes it, removing `table` and
 * `section`. While a table is open its read view, editor and Quality section's panels are shown.
 */
function TablesStandIn() {
  const [params, setParams] = useSearchParams();
  const location = useLocation();
  useEffect(() => {
    visited.push(`${location.pathname}${location.search}`);
  }, [location.pathname, location.search]);
  const toggle = (id: string) => {
    const next = new URLSearchParams(params);
    if (next.get("table") === id) {
      next.delete("table");
      next.delete("section");
    } else {
      next.set("table", id);
    }
    setParams(next);
  };
  const open = params.get("table") !== null;
  return (
    <div>
      <button
        type="button"
        data-table-row="dq-checker.pets_quality_dq_checker_dq"
        onClick={() => toggle("302")}
      />
      <button type="button" data-table-row="pet-store-sqlite.pets" onClick={() => toggle("296")} />
      {open && (
        <>
          <button type="button" data-testid="table-read-view-edit" />
          <button type="button" data-testid="table-edit-cancel" />
          <button type="button" data-testid="table-section-quality" />
          <div data-tour="dq-panel" />
          <div data-tour="profiler-panel" />
        </>
      )}
    </div>
  );
}

const title = () => document.querySelector(".driver-popover-title")?.textContent;
const location = () => visited[visited.length - 1];

describe("a tour card on its predecessor's page, with a panel open in the URL", () => {
  beforeEach(() => {
    localStorage.clear();
    sessionStorage.clear();
    visited.length = 0;
  });

  it("stays on the page with the panel open, and a card with its own route still goes there", async () => {
    localStorage.setItem("provisa_tour_seen", "true");
    const nav = document.createElement("div");
    nav.innerHTML = '<button class="navbar-tour-btn"></button>';
    document.body.appendChild(nav);
    try {
      render(
        <TourProvider>
          <Compass />
          <TablesStandIn />
        </TourProvider>,
        { initialEntries: ["/sources"] },
      );
      fireEvent.click(screen.getByText("launch"));
      fireEvent.click(await screen.findByTestId("tour-topic-quality"));

      // The first card opens the checker's table: the page writes it into the URL.
      await waitFor(() => expect(title()).toBe("A quality checker is just a source"), {
        timeout: 5000,
      });
      expect(location()).toBe("/tables?source=dq-checker&table=302");

      // The next card has no route of its own: it explains the panel that is open.
      const opened = visited.length;
      fireEvent.click(document.querySelector(".driver-popover-next-btn")!);
      await waitFor(() => expect(title()).toBe("Build a quality contract"), { timeout: 5000 });
      expect(visited.slice(opened)).toEqual([]);
      expect(location()).toBe("/tables?source=dq-checker&table=302");

      // "Profile a table" names another source: the tour goes there and opens the pets table.
      fireEvent.click(document.querySelector(".driver-popover-next-btn")!);
      await waitFor(() => expect(title()).toBe("Profile a table"), { timeout: 5000 });
      expect(visited).toContain("/tables?source=pet-store-sqlite");
      expect(location()).toBe("/tables?source=pet-store-sqlite&table=296");

      // "Checks on profile results" continues on that page, over the same open panel.
      const before = visited.length;
      fireEvent.click(document.querySelector(".driver-popover-next-btn")!);
      await waitFor(() => expect(title()).toBe("Checks on profile results"), { timeout: 5000 });
      expect(visited.slice(before)).toEqual([]);
      expect(location()).toBe("/tables?source=pet-store-sqlite&table=296");
      expect(document.querySelector('[data-tour="profiler-panel"]')).not.toBeNull();
    } finally {
      nav.remove();
    }
  });
});
