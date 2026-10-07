// Copyright (c) 2026 Kenneth Stott
// Canary: 42dc279a-c211-4cb4-a54a-cf26a91e9aa0
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

/**
 * The tour on a machine under load: every wait is visible, and no wait destroys the position.
 *
 * Three failures made "Resume tour" look broken. The launch click sat silent through the start-up
 * prefetch; a step whose page had not finished loading showed nothing at all; and when the anchor
 * wait finally expired the tour ended *and* deleted the saved step, so the next Resume silently
 * restarted from the beginning. These tests pin the replacements: a status while preparing, a
 * status while waiting, and a stuck step that offers Retry / Skip / Exit with progress intact.
 */

import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";
import { render, screen, act, fireEvent } from "@testing-library/react";
import { MemoryRouter } from "react-router-dom";
import { MantineProvider } from "@mantine/core";
import { TOUR_STEPS } from "../tour/tourSteps";

// One `t` for the whole suite, not one per render. The runner effect lists `t` in its
// dependencies, so a fresh identity on every render re-runs the step — which is unbounded when the
// step ends by setting a status (a re-render), as the failing-prep case below does.
const t = (key: string) => key;
vi.mock("react-i18next", () => ({
  useTranslation: () => ({ t }),
}));

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

// TourProvider now reads the signed-in rights to decide which steps this viewer is shown. These
// suites are about the offer and the recovery behaviour, not about gating, so the viewer holds
// every right the tour's steps name — the whole tour is on the itinerary and nothing is dropped.
vi.mock("../context/AuthContext", async () => {
  const { TOUR_STEPS } = await import("../tour/tourSteps");
  const capabilities = [...new Set(TOUR_STEPS.flatMap((s) => (s.capability ? [s.capability] : [])))];
  return { useAuth: () => ({ loading: false, capabilities }) };
});

const { TourProvider, useTour, resetTourStateForDemoSession } = await import("../tour/useTour");

function Launcher() {
  const { startTour } = useTour();
  return (
    <button type="button" onClick={() => startTour()}>
      launch
    </button>
  );
}

// Only the core tour resumes (REQ-1945), so the saved position these tests use is a core step whose
// anchor an unmounted app cannot supply: the Sources navigation step.
const CORE_STEP = TOUR_STEPS.findIndex((s) => s.key === "step1");

function renderTour() {
  return render(
    <MantineProvider>
      <MemoryRouter>
        <TourProvider>
          <Launcher />
        </TourProvider>
      </MemoryRouter>
    </MantineProvider>,
  );
}

describe("tour resilience under load", () => {
  beforeEach(() => {
    localStorage.clear();
    sessionStorage.clear();
    vi.useFakeTimers({ shouldAdvanceTime: true });
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  it("says it is resuming while the launch prefetch runs", async () => {
    localStorage.setItem("provisa_tour_progress", String(CORE_STEP));
    renderTour();

    fireEvent.click(screen.getByText("launch"));

    // The prefetch is still pending: the click has to be visibly acknowledged.
    expect(screen.getByText("tour.status.resuming")).toBeInTheDocument();

    await act(async () => {
      chunksResolve();
    });
  });

  it("keeps the saved step when an anchor never arrives, and offers a way on", async () => {
    localStorage.setItem("provisa_tour_progress", String(CORE_STEP));
    renderTour();

    fireEvent.click(screen.getByText("launch"));
    await act(async () => {
      chunksResolve();
    });

    // Nothing of the app is mounted here, so the core step's anchor cannot appear — exactly the shape of
    // a page that never finishes loading.
    await act(async () => {
      vi.advanceTimersByTime(2000);
    });
    expect(screen.getByText("tour.status.waiting")).toBeInTheDocument();

    await act(async () => {
      vi.advanceTimersByTime(60000);
    });
    expect(screen.getByText("tour.status.stuck")).toBeInTheDocument();
    expect(screen.getByText("tour.status.retry")).toBeInTheDocument();
    expect(screen.getByText("tour.status.skip")).toBeInTheDocument();

    // The position survives the failed step — this is what the old endTour("failed") threw away.
    expect(localStorage.getItem("provisa_tour_progress")).toBe(String(CORE_STEP));
  });

  it("stays stuck when the step fails before the waiting hint, instead of spinning forever", async () => {
    // The step's prep writes the visitor's NL state to localStorage, and an origin whose quota is
    // exhausted refuses it — a failure raised in the first few milliseconds of the step, long
    // before WAITING_HINT_MS. The waiting timer used to fire afterwards and overwrite "stuck" with
    // "waiting", stranding the visitor on a spinner whose only button is Cancel: the anchor
    // timeout that offers Retry / Skip / Exit had already come and gone.
    const nlStep = TOUR_STEPS.findIndex((s) => s.prep === "seedNl");
    expect(nlStep).toBeGreaterThan(-1);
    localStorage.setItem("provisa_tour_progress", String(nlStep));
    const realSetItem = Storage.prototype.setItem;
    const setItem = vi
      .spyOn(Storage.prototype, "setItem")
      .mockImplementation(function (this: Storage, key: string, value: string) {
        if (key.startsWith("nl-") || key === "provisa_tour_nl_backup") {
          throw new DOMException("quota", "QuotaExceededError");
        }
        realSetItem.call(this, key, value);
      });

    renderTour();
    fireEvent.click(screen.getByText("launch"));
    await act(async () => {
      chunksResolve();
    });

    await act(async () => {
      vi.advanceTimersByTime(100);
    });
    expect(screen.getByText("tour.status.stuck")).toBeInTheDocument();

    // Past WAITING_HINT_MS and past the anchor window: the failed step is still the failed step.
    await act(async () => {
      vi.advanceTimersByTime(60000);
    });
    expect(screen.getByText("tour.status.stuck")).toBeInTheDocument();
    expect(screen.queryByText("tour.status.waiting")).not.toBeInTheDocument();
    expect(screen.getByText("tour.status.retry")).toBeInTheDocument();

    setItem.mockRestore();
  });

  it("exits a stuck step with the position saved", async () => {
    localStorage.setItem("provisa_tour_progress", String(CORE_STEP));
    renderTour();

    fireEvent.click(screen.getByText("launch"));
    await act(async () => {
      chunksResolve();
    });
    await act(async () => {
      vi.advanceTimersByTime(62000);
    });

    fireEvent.click(screen.getByText("tour.status.exit"));
    expect(screen.queryByText("tour.status.stuck")).not.toBeInTheDocument();
    expect(localStorage.getItem("provisa_tour_progress")).toBe(String(CORE_STEP));
  });
});

describe("demo session reset", () => {
  beforeEach(() => {
    localStorage.clear();
    sessionStorage.clear();
  });

  it("drops the previous visitor's tour state once per session, and nothing else", () => {
    localStorage.setItem("provisa_tour_seen", "true");
    localStorage.setItem("provisa_tour_progress", "9");
    localStorage.setItem("provisa_token", "bearer-abc");

    resetTourStateForDemoSession();

    expect(localStorage.getItem("provisa_tour_seen")).toBeNull();
    expect(localStorage.getItem("provisa_tour_progress")).toBeNull();
    // The session's bearer is not the tour's to discard.
    expect(localStorage.getItem("provisa_token")).toBe("bearer-abc");

    // A second call in the same session leaves a tour started since the reset alone.
    localStorage.setItem("provisa_tour_progress", "3");
    resetTourStateForDemoSession();
    expect(localStorage.getItem("provisa_tour_progress")).toBe("3");
  });
});

describe("a step that opens its own starting state", () => {
  beforeEach(() => {
    localStorage.clear();
    sessionStorage.clear();
    vi.useFakeTimers({ shouldAdvanceTime: true });
  });
  afterEach(() => {
    vi.useRealTimers();
    document.getElementById("sources-stand-in")?.remove();
  });

  it("resumes at the source-types step with the add-source form closed, and reaches the picker", async () => {
    // A stand-in for /sources: the Add button toggles the form; the Type field opens the picker.
    const root = document.createElement("div");
    root.id = "sources-stand-in";
    document.body.appendChild(root);
    let formOpen = false;
    const draw = () => {
      root.innerHTML = "";
      const add = document.createElement("button");
      add.setAttribute("data-tour", "sources-add");
      add.addEventListener("click", () => {
        formOpen = !formOpen;
        draw();
      });
      root.appendChild(add);
      if (formOpen) {
        const type = document.createElement("select");
        type.setAttribute("data-tour", "sources-type");
        type.addEventListener("click", () => {
          const search = document.createElement("input");
          search.setAttribute("data-testid", "source-type-picker-search");
          root.appendChild(search);
        });
        root.appendChild(type);
      }
    };
    draw();

    // step3 belongs to the Connect topic: the core tour has been seen, so the tour button opens the
    // Deep Dives menu, and picking the topic runs it from its first step.
    localStorage.setItem("provisa_tour_seen", "true");
    renderTour();
    fireEvent.click(screen.getByText("launch"));
    fireEvent.click(await screen.findByTestId("tour-topic-connect"));
    await act(async () => {
      chunksResolve();
    });
    for (let i = 0; i < 10; i++) {
      await act(async () => {
        await vi.advanceTimersByTimeAsync(200);
      });
    }

    expect(formOpen).toBe(true);
    expect(root.querySelector('[data-testid="source-type-picker-search"]')).not.toBeNull();
  });
});
