// Copyright (c) 2026 Kenneth Stott
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2052: a tour step whose object does not exist stops when the page says so. A profile graph
// step in an org whose table has never been profiled: View Profile Runs says so, and the step is
// stuck at once, that line its reason, instead of waiting out an anchor no run can draw.

import { describe, it, expect, vi } from "vitest";
import { render, screen, act, fireEvent } from "@testing-library/react";
import { MemoryRouter } from "react-router-dom";
import { MantineProvider } from "@mantine/core";
import { NamedDemoContext, type NamedDemo } from "../context/NamedDemoContext";

const t = (key: string) => key;
vi.mock("../hooks/useOrgHas", () => {
  const has = {
    sources: new Set(["crm"]),
    tables: new Set(["crm.customers"]),
    dataProducts: 0,
    pollyConfigured: false,
  };
  const read = () => Promise.resolve(has);
  return { useOrgHas: () => read };
});
vi.mock("react-i18next", () => ({ useTranslation: () => ({ t }) }));
vi.mock("../pageChunks", () => ({ prefetchAllPageChunks: () => Promise.resolve() }));
vi.mock("../hooks/useAdminQueries", () => ({ useTourPrefetch: () => () => Promise.resolve() }));
vi.mock("../context/pollyState", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../context/pollyState")>()),
  usePolly: () => ({
    open: false,
    checkingConfig: false,
    unconfiguredReason: null,
    setUnconfiguredReason: () => {},
    openPolly: () => Promise.resolve(),
    closePolly: () => {},
    showExchange: () => {},
    shownExchange: null,
  }),
}));
vi.mock("../context/AuthContext", () => ({
  useAuth: () => ({ loading: false, capabilities: ["source_registration", "table_registration"] }),
}));

const { TourProvider, useTour } = await import("../tour/useTour");

const PROFILING: NamedDemo = {
  name: "profiling",
  exampleQueries: [],
  tour: [
    { key: "profilingScenario", route: "/sources", capability: "source_registration", source: "crm" },
    {
      key: "profilingTerrain",
      route: "/tables",
      capability: "table_registration",
      table: "crm.customers",
      open: "quality",
      graph: "terrain",
    },
  ],
  landing: null,
  notebook: [],
  orgFiles: [],
};

function Launcher() {
  const { startTour } = useTour();
  return (
    <button type="button" onClick={() => startTour()}>
      launch
    </button>
  );
}

/** The table's View Profile Runs, as it opens on a table with no run. */
function NeverProfiled() {
  return (
    <button
      type="button"
      data-testid="table-read-view-profile-runs"
      onClick={(e) => {
        const none = document.createElement("p");
        none.dataset.testid = "profile-runs-none";
        none.textContent = "This table has not been profiled yet.";
        e.currentTarget.after(none);
      }}
    />
  );
}

describe("a profile graph step in an org with no profile run", () => {
  it("stops at once, naming why, offering Skip", async () => {
    vi.useFakeTimers({ shouldAdvanceTime: true });
    render(
      <MantineProvider>
        <MemoryRouter>
          <NamedDemoContext.Provider value={PROFILING}>
            <TourProvider>
              <Launcher />
              <table>
                <tbody>
                  <tr data-testid="sources-row-crm" />
                </tbody>
              </table>
              <NeverProfiled />
            </TourProvider>
          </NamedDemoContext.Provider>
        </MemoryRouter>
      </MantineProvider>,
    );
    fireEvent.click(screen.getByText("launch"));
    expect(await screen.findByText("tour.steps.profilingScenario.title")).toBeInTheDocument();
    fireEvent.click(document.querySelector<HTMLElement>(".driver-popover-next-btn")!);
    // Well inside the anchor's window (ANCHOR_WAIT_MS): the step did not wait it out.
    await act(async () => {
      await vi.advanceTimersByTimeAsync(3000);
    });
    expect(
      screen.getByText("This table has not been profiled yet.", {
        selector: ".tour-status-message",
      }),
    ).toBeInTheDocument();
    expect(screen.getByText("tour.status.skip")).toBeInTheDocument();
    vi.useRealTimers();
  });
});
