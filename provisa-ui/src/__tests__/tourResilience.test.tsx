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

vi.mock("react-i18next", () => ({
  useTranslation: () => ({ t: (key: string) => key }),
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

const { TourProvider, useTour, resetTourStateForDemoSession } = await import("../tour/useTour");

function Launcher() {
  const { startTour } = useTour();
  return (
    <button type="button" onClick={() => startTour()}>
      launch
    </button>
  );
}

function renderTour() {
  return render(
    <MemoryRouter>
      <TourProvider>
        <Launcher />
      </TourProvider>
    </MemoryRouter>,
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
    localStorage.setItem("provisa_tour_progress", "5");
    renderTour();

    fireEvent.click(screen.getByText("launch"));

    // The prefetch is still pending: the click has to be visibly acknowledged.
    expect(screen.getByText("tour.status.resuming")).toBeInTheDocument();

    await act(async () => {
      chunksResolve();
    });
  });

  it("keeps the saved step when an anchor never arrives, and offers a way on", async () => {
    localStorage.setItem("provisa_tour_progress", "5");
    renderTour();

    fireEvent.click(screen.getByText("launch"));
    await act(async () => {
      chunksResolve();
    });

    // Nothing of the app is mounted here, so step 5's anchor cannot appear — exactly the shape of
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
    expect(localStorage.getItem("provisa_tour_progress")).toBe("5");
  });

  it("exits a stuck step with the position saved", async () => {
    localStorage.setItem("provisa_tour_progress", "5");
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
    expect(localStorage.getItem("provisa_tour_progress")).toBe("5");
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
