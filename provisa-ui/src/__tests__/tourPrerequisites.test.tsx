// Copyright (c) 2026 Kenneth Stott
// Canary: 43ccfeb5-4858-4ccf-a913-323eb6e633a7
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2013: a tour is offered only where the org has what its steps show. The Deep Dives built on
// the standard demo's objects are not in the menu of an org that lacks them (a named demo's), so
// none of them can be started there and wait for a row that is not on the page.

import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";
import { render, screen, act, fireEvent } from "@testing-library/react";
import { MemoryRouter } from "react-router-dom";
import { MantineProvider } from "@mantine/core";
import { prerequisitesMet, type OrgHas } from "../tour/tourPrerequisites";
import { TOUR_PREREQUISITES } from "../tour/tourSteps";
import { STANDARD_DEMO_ORG } from "../test-utils/tourOrg";

const t = (key: string) => key;
vi.mock("react-i18next", () => ({ useTranslation: () => ({ t }) }));

const org = vi.hoisted(() => ({ current: null as unknown }));
vi.mock("../hooks/useOrgHas", () => {
  // One function, as the hook's useCallback gives: a new one each render re-runs the tour's step.
  const read = () => Promise.resolve(org.current);
  return { useOrgHas: () => read };
});
let chunksResolve: () => void = () => {};
vi.mock("../pageChunks", () => ({
  prefetchAllPageChunks: () =>
    new Promise<void>((resolve) => {
      chunksResolve = resolve;
    }),
}));
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
  }),
}));
vi.mock("../context/AuthContext", async () => {
  const { TOUR_STEPS } = await import("../tour/tourSteps");
  const capabilities = [
    ...new Set(TOUR_STEPS.flatMap((s) => (s.capability ? [s.capability] : []))),
  ];
  return { useAuth: () => ({ loading: false, capabilities }) };
});

const { TourProvider, useTour } = await import("../tour/useTour");

/** An org like the weblogs demo's: its own shop, none of the standard demo's objects. */
const WEBLOGS_ORG: OrgHas = {
  sources: new Set(["shop-db", "shop_access", "shop_db_log"]),
  tables: new Set(["shop-db.customers", "shop_access.shop_access"]),
  dataProducts: 0,
  pollyConfigured: true,
};

describe("a tour's prerequisites", () => {
  it("are met by an org that has every source, table and data product named", () => {
    expect(prerequisitesMet(TOUR_PREREQUISITES.quality, STANDARD_DEMO_ORG)).toBe(true);
    expect(prerequisitesMet(undefined, WEBLOGS_ORG)).toBe(true); // a tour that needs nothing
  });

  it("are unmet by an org missing any one of them", () => {
    expect(prerequisitesMet(TOUR_PREREQUISITES.quality, WEBLOGS_ORG)).toBe(false);
    const noChecker = {
      ...STANDARD_DEMO_ORG,
      tables: new Set([...STANDARD_DEMO_ORG.tables].filter((x) => !x.startsWith("dq-checker."))),
    };
    expect(prerequisitesMet(TOUR_PREREQUISITES.quality, noChecker)).toBe(false);
    expect(prerequisitesMet({ sources: ["dq-checker"] }, WEBLOGS_ORG)).toBe(false);
    expect(prerequisitesMet({ dataProduct: true }, { ...STANDARD_DEMO_ORG, dataProducts: 0 })).toBe(
      false,
    );
  });
});

function Launcher() {
  const { startTour } = useTour();
  return (
    <button type="button" onClick={() => startTour()}>
      launch
    </button>
  );
}

function renderTour(children?: React.ReactNode) {
  return render(
    <MantineProvider>
      <MemoryRouter>
        <TourProvider>
          <Launcher />
          {children}
        </TourProvider>
      </MemoryRouter>
    </MantineProvider>,
  );
}

describe("the Deep Dives menu", () => {
  beforeEach(() => {
    localStorage.clear();
    sessionStorage.clear();
    localStorage.setItem("provisa_tour_seen", "true"); // the tour button opens the menu
  });

  it("offers every deep dive where the org has what they show", async () => {
    org.current = STANDARD_DEMO_ORG;
    renderTour();
    fireEvent.click(screen.getByText("launch"));
    expect(await screen.findByTestId("tour-topic-quality")).toBeInTheDocument();
    expect(screen.getByTestId("tour-topic-testdata")).toBeInTheDocument();
    expect(screen.getByTestId("tour-menu-core-tour")).toBeInTheDocument();
  });

  it("leaves out a deep dive whose objects the org lacks", async () => {
    org.current = WEBLOGS_ORG;
    renderTour();
    fireEvent.click(screen.getByText("launch"));
    await screen.findByTestId("tour-menu");
    // The quality checker, the pet store and the products are the standard demo's.
    for (const id of ["quality", "testdata", "query", "model", "publish"]) {
      expect(screen.queryByTestId(`tour-topic-${id}`)).toBeNull();
    }
    expect(screen.queryByTestId("tour-menu-core-tour")).toBeNull();
    // Those that need nothing but their pages stay.
    for (const id of ["relationships", "govern", "operate"]) {
      expect(screen.getByTestId(`tour-topic-${id}`)).toBeInTheDocument();
    }
  });
});

describe("a step whose opener swaps the page under it", () => {
  beforeEach(() => {
    localStorage.clear();
    sessionStorage.clear();
    org.current = STANDARD_DEMO_ORG;
    vi.useFakeTimers({ shouldAdvanceTime: true });
  });
  afterEach(() => {
    vi.useRealTimers();
    document.getElementById("tables-stand-in")?.remove();
  });

  it("clicks the editor's tab, not the read view's that is going away", async () => {
    // A stand-in for the pets table's panel: Edit swaps the read view for the editor a moment
    // later, and only the editor's Quality tab shows the profiler.
    const root = document.createElement("div");
    root.id = "tables-stand-in";
    document.body.appendChild(root);
    const row = document.createElement("div");
    row.setAttribute("data-table-row", "pet-store-sqlite.pets");
    root.appendChild(row);
    const read = document.createElement("div");
    const edit = document.createElement("button");
    edit.setAttribute("data-testid", "table-read-view-edit");
    const readTab = document.createElement("button");
    readTab.setAttribute("data-testid", "table-section-quality");
    read.append(edit, readTab);
    root.appendChild(read);
    edit.addEventListener("click", () =>
      setTimeout(() => {
        read.remove();
        const editor = document.createElement("div");
        const cancel = document.createElement("button");
        cancel.setAttribute("data-testid", "table-edit-cancel");
        const tab = document.createElement("button");
        tab.setAttribute("data-testid", "table-section-quality");
        tab.addEventListener("click", () => {
          const panel = document.createElement("div");
          panel.setAttribute("data-tour", "profiler-panel");
          editor.appendChild(panel);
        });
        editor.append(cancel, tab);
        root.appendChild(editor);
      }, 50),
    );

    localStorage.setItem("provisa_tour_seen", "true");
    renderTour();
    fireEvent.click(screen.getByText("launch"));
    fireEvent.click(await screen.findByTestId("tour-topic-testdata"));
    await act(async () => chunksResolve());
    expect(await screen.findByText("tour.steps.stepTestDataMap.title")).toBeInTheDocument();

    fireEvent.click(document.querySelector<HTMLElement>(".driver-popover-next-btn")!);
    for (let i = 0; i < 10; i++) {
      await act(async () => {
        await vi.advanceTimersByTimeAsync(100);
      });
    }
    expect(root.querySelector('[data-tour="profiler-panel"]')).not.toBeNull();
    expect(await screen.findByText("tour.steps.stepTestDataProfiler.title")).toBeInTheDocument();
  });
});
