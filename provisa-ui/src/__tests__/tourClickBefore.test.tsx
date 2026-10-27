// Copyright (c) 2026 Kenneth Stott
// Canary: 7d1e4b2a-9c36-4f85-b0a7-3e6f2d8c5a19
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

/**
 * A step's `clickBefore` opens what the step shows, once. The step runs again whenever the runner
 * effect's dependencies change -- `t` when a translation namespace arrives, `navigate` -- and a
 * second click on a toggle ("+ Source" opens and closes the type picker) closed what the first had
 * opened: the step then waited out its anchor window on a picker it had shut itself, and the
 * popover never came (tour-page-preload.spec.ts, run 40177970935, stuck at "Next (4/11)").
 */

import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";
import { render, screen, act, fireEvent } from "@testing-library/react";
import { MemoryRouter } from "react-router-dom";
import { MantineProvider } from "@mantine/core";
import { TOUR_STEPS } from "../tour/tourSteps";

// The runner effect lists `t`; a new one is what a namespace's arrival hands it.
const i18n = vi.hoisted(() => ({ t: (key: string) => key }));
vi.mock("react-i18next", () => ({
  useTranslation: () => ({ t: i18n.t }),
}));
vi.mock("../hooks/useOrgHas", async () => {
  const { STANDARD_DEMO_ORG } = await import("../test-utils/tourOrg");
  const read = () => Promise.resolve(STANDARD_DEMO_ORG);
  return { useOrgHas: () => read };
});
let chunksResolve: () => void;
vi.mock("../pageChunks", () => ({
  prefetchAllPageChunks: () =>
    new Promise<void>((resolve) => {
      chunksResolve = resolve;
    }),
}));
vi.mock("../hooks/useAdminQueries", () => ({
  useTourPrefetch: () => () => Promise.resolve(),
}));
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

function Launcher() {
  const { startTour } = useTour();
  return (
    <button type="button" onClick={() => startTour()}>
      launch
    </button>
  );
}

const tree = () => (
  <MantineProvider>
    <MemoryRouter>
      <TourProvider>
        <Launcher />
      </TourProvider>
    </MemoryRouter>
  </MantineProvider>
);

// step3: "+ Source" (clickBefore) opens the type picker, whose search box is the anchor.
const PICKER_STEP = TOUR_STEPS.findIndex((s) => s.key === "step3");
const SEARCH = "source-type-picker-search";

/** The Sources page's "+ Source": a toggle, as SourcesPage's is. */
function standIn() {
  const root = document.createElement("div");
  root.id = "click-before-stand-in";
  const add = document.createElement("button");
  add.setAttribute("data-tour", "sources-add");
  add.addEventListener("click", () => {
    const open = document.querySelector(`[data-testid="${SEARCH}"]`);
    if (open) {
      open.remove();
      return;
    }
    const search = document.createElement("input");
    search.setAttribute("data-testid", SEARCH);
    root.appendChild(search);
  });
  root.appendChild(add);
  document.body.appendChild(root);
  return add;
}

describe("a step's clickBefore", () => {
  beforeEach(() => {
    localStorage.clear();
    sessionStorage.clear();
    vi.useFakeTimers({ shouldAdvanceTime: true });
  });
  afterEach(() => {
    vi.useRealTimers();
    document.getElementById("click-before-stand-in")?.remove();
  });

  it("is not clicked again when the step runs again over what it opened", async () => {
    expect(TOUR_STEPS[PICKER_STEP].clickBefore).toBe('[data-tour="sources-add"]');
    const add = standIn();
    const clicks = vi.fn();
    add.addEventListener("click", clicks);
    localStorage.setItem("provisa_tour_progress", String(PICKER_STEP));
    const { rerender } = render(tree());
    fireEvent.click(screen.getByText("launch"));
    await act(async () => {
      chunksResolve();
    });
    await act(async () => {
      vi.advanceTimersByTime(1000);
    });
    expect(screen.getByTestId(SEARCH)).toBeInTheDocument();
    expect(clicks).toHaveBeenCalledTimes(1);

    // A namespace arrives: a new `t`, and the step runs again.
    i18n.t = (key: string) => key;
    rerender(tree());
    await act(async () => {
      vi.advanceTimersByTime(1000);
    });

    expect(clicks).toHaveBeenCalledTimes(1);
    expect(screen.getByTestId(SEARCH)).toBeInTheDocument();
  });
});
