// Copyright (c) 2026 Kenneth Stott
// Canary: 3a8f5d61-0b2e-4c97-8d14-e6a72b90c5f3
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

/**
 * REQ-1945: a tour step that refers to Polly opens the Polly panel through the launcher's own handler,
 * and the tour closes it again on the first step that does not refer to Polly.
 */

import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, act, fireEvent, waitFor } from "@testing-library/react";
import { MemoryRouter } from "react-router-dom";
import { MantineProvider } from "@mantine/core";
import { TOUR_STEPS } from "../tour/tourSteps";

const t = (key: string) => key;
vi.mock("react-i18next", () => ({ useTranslation: () => ({ t }) }));
vi.mock("../pageChunks", () => ({ prefetchAllPageChunks: () => Promise.resolve() }));
vi.mock("../hooks/useAdminQueries", () => ({ useTourPrefetch: () => () => Promise.resolve() }));
const fetchMcpChatStatus = vi.fn();
vi.mock("../api/mcpChat", () => ({ fetchMcpChatStatus: () => fetchMcpChatStatus() }));
vi.mock("../context/AuthContext", async () => {
  const { TOUR_STEPS } = await import("../tour/tourSteps");
  const capabilities = [...new Set(TOUR_STEPS.flatMap((s) => (s.capability ? [s.capability] : [])))];
  return { useAuth: () => ({ loading: false, capabilities }) };
});

const { TourProvider, useTour } = await import("../tour/useTour");
const { PollyProvider } = await import("../context/PollyContext");
const { usePolly } = await import("../context/pollyState");

const POLLY_STEP = TOUR_STEPS.findIndex((s) => s.key === "stepPolly");

/** Stands in for the app shell: the launcher, and the panel only while Polly is open. */
function Shell() {
  const { open, openPolly } = usePolly();
  const { startTour } = useTour();
  return (
    <>
      <button type="button" onClick={() => startTour()}>
        launch
      </button>
      <button type="button" data-tour="polly-toggle" onClick={() => void openPolly()}>
        polly-launcher
      </button>
      {open && <div data-testid="chat-panel">polly-panel</div>}
    </>
  );
}

function renderShell() {
  return render(
    <MantineProvider>
      <MemoryRouter>
        <PollyProvider>
          <TourProvider>
            <Shell />
          </TourProvider>
        </PollyProvider>
      </MemoryRouter>
    </MantineProvider>,
  );
}

describe("tour opens Polly", () => {
  beforeEach(() => {
    localStorage.clear();
    sessionStorage.clear();
    fetchMcpChatStatus.mockReset();
  });

  it("marks exactly the steps whose text refers to Polly", async () => {
    const { default: en } = await import("../i18n/locales/en/tour.json");
    const steps = en.tour.steps as unknown as Record<string, { title: string; description: string }>;
    for (const step of TOUR_STEPS) {
      const refers = /\bPolly\b/.test(`${steps[step.key].title} ${steps[step.key].description}`);
      expect(step.pollyOpen === true, `step ${step.key}`).toBe(refers);
    }
  });

  it("has Polly open when the Polly step is shown", async () => {
    fetchMcpChatStatus.mockResolvedValue({ configured: true });
    localStorage.setItem("provisa_tour_progress", String(POLLY_STEP));
    renderShell();
    expect(screen.queryByTestId("chat-panel")).toBeNull();

    fireEvent.click(screen.getByText("launch"));

    await waitFor(() => expect(screen.getByTestId("chat-panel")).toBeInTheDocument());
    await waitFor(() =>
      expect(document.querySelector(".driver-popover-title")?.textContent).toBe(
        "tour.steps.stepPolly.title",
      ),
    );
    expect(fetchMcpChatStatus).toHaveBeenCalledTimes(1);
  });

  it("closes Polly on moving to a step that does not refer to Polly, when the tour opened it", async () => {
    fetchMcpChatStatus.mockResolvedValue({ configured: true });
    localStorage.setItem("provisa_tour_progress", String(POLLY_STEP));
    renderShell();
    fireEvent.click(screen.getByText("launch"));
    await waitFor(() => expect(screen.getByTestId("chat-panel")).toBeInTheDocument());
    await waitFor(() => expect(document.querySelector(".driver-popover-next-btn")).not.toBeNull());

    fireEvent.click(document.querySelector(".driver-popover-next-btn")!);

    await waitFor(() => expect(screen.queryByTestId("chat-panel")).toBeNull());
  });

  it("leaves a Polly the viewer already opened open", async () => {
    fetchMcpChatStatus.mockResolvedValue({ configured: true });
    localStorage.setItem("provisa_tour_progress", String(POLLY_STEP));
    renderShell();
    fireEvent.click(screen.getByText("polly-launcher"));
    await waitFor(() => expect(screen.getByTestId("chat-panel")).toBeInTheDocument());
    fireEvent.click(screen.getByText("launch"));
    await waitFor(() => expect(document.querySelector(".driver-popover-next-btn")).not.toBeNull());

    fireEvent.click(document.querySelector(".driver-popover-next-btn")!);
    await act(async () => {
      await Promise.resolve();
    });

    expect(screen.getByTestId("chat-panel")).toBeInTheDocument();
    expect(fetchMcpChatStatus).toHaveBeenCalledTimes(1);
  });

  it("does not open Polly when the launcher would refuse, and the step fails loudly", async () => {
    vi.useFakeTimers({ shouldAdvanceTime: true });
    fetchMcpChatStatus.mockResolvedValue({ configured: false, reason: "no vendor" });
    localStorage.setItem("provisa_tour_progress", String(POLLY_STEP));
    renderShell();

    fireEvent.click(screen.getByText("launch"));
    await act(async () => {
      await Promise.resolve();
    });
    await act(async () => {
      vi.advanceTimersByTime(2000);
    });
    await act(async () => {
      vi.advanceTimersByTime(60000);
    });

    expect(screen.queryByTestId("chat-panel")).toBeNull();
    expect(screen.getByText("tour.status.stuck")).toBeInTheDocument();
    vi.useRealTimers();
  });
});
