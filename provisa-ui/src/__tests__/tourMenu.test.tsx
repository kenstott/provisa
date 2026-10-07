// Copyright (c) 2026 Kenneth Stott
// Canary: 0f93b6c8-47a1-4e2d-b5d9-3a8c71e60f24
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

/**
 * REQ-1945: the Deep Dives menu lists one card per topic (title, sentence, step count, completed
 * mark), and the tour button opens it directly once the core tour has been seen. Picking a card
 * runs that topic; the core tour's steps carry a Deep Dives button that leaves for the menu.
 */

import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, fireEvent, waitFor, act } from "../test-utils/render";

vi.mock("../pageChunks", () => ({ prefetchAllPageChunks: () => Promise.resolve() }));
vi.mock("../hooks/useAdminQueries", () => ({ useTourPrefetch: () => () => Promise.resolve() }));
vi.mock("../context/AuthContext", async () => {
  const { TOUR_STEPS } = await import("../tour/tourSteps");
  const capabilities = [...new Set(TOUR_STEPS.flatMap((s) => (s.capability ? [s.capability] : [])))];
  return { useAuth: () => ({ loading: false, capabilities }) };
});

const { TourProvider, useTour } = await import("../tour/useTour");
const { TourMenu } = await import("../tour/TourMenu");
const { TOPIC_IDS, TOUR_SCOPES } = await import("../tour/tourSteps");

describe("TourMenu", () => {
  it("renders a card per topic with title, sentence and step count, and marks completed ones", () => {
    const onPick = vi.fn();
    render(
      <TourMenu
        opened
        topics={TOPIC_IDS.map((id) => ({ id, steps: TOUR_SCOPES[id].length }))}
        completed={["govern"]}
        onPick={onPick}
        onClose={() => {}}
      />,
    );
    expect(screen.getByText("Deep Dives")).toBeInTheDocument();
    for (const id of TOPIC_IDS) {
      expect(screen.getByTestId(`tour-topic-${id}`)).toBeInTheDocument();
    }
    expect(screen.getByText("Connect your data")).toBeInTheDocument();
    expect(screen.getByText("Model it")).toBeInTheDocument();
    expect(screen.getByTestId("tour-topic-done-govern")).toBeInTheDocument();
    expect(screen.queryByTestId("tour-topic-done-query")).not.toBeInTheDocument();
    expect(screen.getAllByText(/Steps: \d+/)).toHaveLength(TOPIC_IDS.length);

    fireEvent.click(screen.getByTestId("tour-topic-query"));
    expect(onPick).toHaveBeenCalledWith("query");
  });
});

function Compass() {
  const { startTour } = useTour();
  return (
    <button type="button" onClick={() => startTour()}>
      launch
    </button>
  );
}

describe("tour button routing", () => {
  beforeEach(() => {
    localStorage.clear();
    sessionStorage.clear();
  });

  it("opens the Deep Dives menu directly once the core tour has been seen", async () => {
    localStorage.setItem("provisa_tour_seen", "true");
    render(
      <TourProvider>
        <Compass />
      </TourProvider>,
    );
    fireEvent.click(screen.getByText("launch"));
    expect(await screen.findByTestId("tour-menu")).toBeInTheDocument();
    for (const id of TOPIC_IDS) expect(screen.getByTestId(`tour-topic-${id}`)).toBeInTheDocument();
  });

  it("starts the core tour, not the menu, for a viewer who has not seen it", async () => {
    render(
      <TourProvider>
        <Compass />
      </TourProvider>,
    );
    fireEvent.click(screen.getByText("launch"));
    await act(async () => {});
    expect(screen.queryByTestId("tour-topic-connect")).not.toBeInTheDocument();
  });

  it("leaves the core tour for the menu from its Deep Dives button", async () => {
    // Stand-ins for the first core step's route content and anchor.
    const root = document.createElement("div");
    root.innerHTML = '<button class="navbar-tour-btn"></button><button data-tour="sources-add"></button>';
    document.body.appendChild(root);
    try {
      render(
        <TourProvider>
          <Compass />
        </TourProvider>,
      );
      fireEvent.click(screen.getByText("launch"));
      const deep = await waitFor(
        () => {
          const el = document.querySelector<HTMLButtonElement>(".driver-popover-deepdives-btn");
          expect(el).not.toBeNull();
          return el!;
        },
        { timeout: 5000 },
      );
      expect(deep.textContent).toBe("Deep Dives");
      fireEvent.click(deep);
      expect(await screen.findByTestId("tour-menu")).toBeInTheDocument();
      expect(localStorage.getItem("provisa_tour_seen")).toBe("true");
    } finally {
      root.remove();
    }
  });
});
