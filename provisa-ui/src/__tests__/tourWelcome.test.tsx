// Copyright (c) 2026 Kenneth Stott
// Canary: ca980554-0fbe-47f6-8db0-cfe35507f225
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

/**
 * The tour is offered, not launched. These pin who gets asked and who stops being asked: once per
 * browser session, never after it has been taken, never again once the checkbox is ticked — and the
 * toolbar button still starts it either way, because that click is a request, not an offer.
 */

import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, fireEvent, waitFor } from "../test-utils/render";
import { MemoryRouter } from "react-router-dom";

vi.mock("../pageChunks", () => ({ prefetchAllPageChunks: () => Promise.resolve() }));
vi.mock("../hooks/useAdminQueries", () => ({ useTourPrefetch: () => () => Promise.resolve() }));

const {
  TourProvider,
  useTour,
  claimTourOffer,
  tourDeclined,
  declineTour,
  resetTourStateForDemoSession,
} = await import("../tour/useTour");
const { TourWelcomeModal } = await import("../tour/TourWelcomeModal");
const { TOUR_SEEN_KEY } = await import("../tour/tourKeys");

/** Stands in for the navbar compass: a plain launch click, no offer involved. */
function Compass() {
  const { startTour, status } = useTour();
  return (
    <>
      <button type="button" onClick={() => startTour()}>
        launch
      </button>
      <div data-testid="status">{status ? status.kind : "idle"}</div>
    </>
  );
}

function renderModal(onClose = () => {}) {
  return render(
    <MemoryRouter>
      <TourProvider>
        <TourWelcomeModal onClose={onClose} />
      </TourProvider>
    </MemoryRouter>,
  );
}

describe("tour welcome offer", () => {
  beforeEach(() => {
    localStorage.clear();
    sessionStorage.clear();
  });

  it("offers once per browser session", () => {
    expect(claimTourOffer()).toBe(true);
    expect(claimTourOffer()).toBe(false);
  });

  it("does not offer once the tour has been taken", () => {
    localStorage.setItem(TOUR_SEEN_KEY, "true");
    expect(claimTourOffer()).toBe(false);
  });

  it("stops offering for good once the checkbox is ticked", () => {
    const onClose = vi.fn();
    renderModal(onClose);
    fireEvent.click(screen.getByLabelText("Don't show this again"));
    fireEvent.click(screen.getByRole("button", { name: "Maybe later" }));

    expect(onClose).toHaveBeenCalled();
    expect(tourDeclined()).toBe(true);
    sessionStorage.clear(); // a fresh session, i.e. the next sign-in
    expect(claimTourOffer()).toBe(false);
  });

  it("keeps offering when the modal is dismissed without the checkbox", () => {
    renderModal();
    fireEvent.click(screen.getByRole("button", { name: "Maybe later" }));

    expect(tourDeclined()).toBe(false);
    expect(claimTourOffer()).toBe(true);
  });

  // The toolbar compass is an explicit request; the offer's off-switch has no say over it.
  it("still starts the tour from a declined state", async () => {
    declineTour();
    render(
      <MemoryRouter>
        <TourProvider>
          <Compass />
        </TourProvider>
      </MemoryRouter>,
    );
    fireEvent.click(screen.getByRole("button", { name: "launch" }));

    // The launch takes effect: the tour leaves idle and starts preparing.
    await waitFor(() => expect(screen.getByTestId("status")).not.toHaveTextContent("idle"));
    expect(tourDeclined()).toBe(true);
  });

  // A demo box is walked by a new visitor each session; the last one's "never again" is not theirs.
  it("clears the declined flag for a new demo visitor", () => {
    localStorage.setItem("provisa_tour_declined", "true");
    resetTourStateForDemoSession();
    expect(tourDeclined()).toBe(false);
  });
});
