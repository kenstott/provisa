// Copyright (c) 2026 Kenneth Stott
// Canary: 1cd51d16-e23d-44c1-9156-f1cb22ee2b76
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2115: a tour step acts, then explains. Entering it, its actions (Polly's own UI actions) run
// on the screen with no popover, under the step's caption, and the visitor's tour presses a Save
// with nothing more asked; then the popover anchors on what they built. Entered again, the step
// finds what it built and only explains. An action that cannot be done leaves the step stuck.

import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";
import { render, screen, act, fireEvent, waitFor } from "@testing-library/react";
import { MemoryRouter } from "react-router-dom";
import { MantineProvider } from "@mantine/core";
import { TOUR_STEPS, SUPPLIES_PARQUET_URL } from "../tour/tourSteps";

const t = (key: string) => key;
vi.mock("../hooks/useOrgHas", async () => {
  const { STANDARD_DEMO_ORG } = await import("../test-utils/tourOrg");
  const read = () => Promise.resolve(STANDARD_DEMO_ORG);
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

const BUILD_STEP = TOUR_STEPS.findIndex((s) => s.key === "stepBuildSource");

function Launcher() {
  const { startTour } = useTour();
  return (
    <button type="button" onClick={() => startTour()}>
      launch
    </button>
  );
}

/** The Sources screen as far as the step's actions reach it: + Source opens the picker, a type
 * opens its form, whose Save and Cancel the test records. */
function sourcesScreen(formAlready: boolean) {
  const page = document.createElement("div");
  page.id = "sources-stand-in";
  const pressed: string[] = [];
  const el = (tag: string, attrs: Record<string, string>, text = "") => {
    const e = document.createElement(tag);
    for (const [k, v] of Object.entries(attrs)) e.setAttribute(k, v);
    e.textContent = text;
    return e;
  };
  const sourceForm = () => {
    const form = page.appendChild(el("form", { "data-tour": "sources-form" }));
    form.appendChild(el("input", { "data-testid": "sources-id-input" }));
    form.appendChild(el("input", { "data-testid": "source-file-location" }));
    const footer = form.appendChild(el("div", { class: "form-footer" }));
    footer
      .appendChild(el("button", { type: "button", "data-testid": "sources-submit" }, "Save"))
      .addEventListener("click", () => pressed.push("save"));
    footer
      .appendChild(el("button", { type: "button", "data-testid": "sources-cancel" }, "Cancel"))
      .addEventListener("click", () => {
        pressed.push("cancel");
        form.remove();
      });
  };
  const add = page.appendChild(
    el("button", { type: "button", "data-testid": "sources-add-toggle" }, "+ Source"),
  );
  add.addEventListener("click", () => {
    pressed.push("add");
    const body = page.appendChild(el("div", { "data-testid": "source-type-picker-body" }));
    const parquet = body.appendChild(
      el("button", { type: "button", "data-testid": "source-type-option-parquet" }, "Parquet File"),
    );
    parquet.addEventListener("click", () => {
      pressed.push("parquet");
      sourceForm();
    });
  });
  if (formAlready) sourceForm();
  document.body.appendChild(page);
  return pressed;
}

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

const title = () => document.querySelector(".driver-popover-title")?.textContent;

describe("a tour step that acts, then explains", () => {
  beforeEach(() => {
    localStorage.clear();
    sessionStorage.clear();
    localStorage.setItem("provisa_tour_progress", String(BUILD_STEP));
  });
  afterEach(() => {
    vi.useRealTimers();
    document.getElementById("sources-stand-in")?.remove();
  });

  it("fills the source form under its caption, explains it, and Next cancels it", async () => {
    const pressed = sourcesScreen(false);
    renderTour();
    fireEvent.click(screen.getByText("launch"));

    expect(await screen.findByText("tour.steps.stepBuildSource.acting")).toBeInTheDocument();
    expect(document.body.classList.contains("tour-acting")).toBe(true);
    expect(title()).toBeUndefined();

    await waitFor(() => expect(title()).toBe("tour.steps.stepBuildSource.title"), {
      timeout: 20000,
    });
    expect(pressed).toEqual(["add", "parquet"]);
    expect((screen.getByTestId("sources-id-input") as HTMLInputElement).value).toBe("supplies");
    expect((screen.getByTestId("source-file-location") as HTMLInputElement).value).toBe(
      SUPPLIES_PARQUET_URL,
    );
    expect(screen.queryByText("tour.steps.stepBuildSource.acting")).toBeNull();
    expect(document.body.classList.contains("tour-acting")).toBe(false);

    // Next cancels the form: nothing was saved, and the form is gone.
    fireEvent.click(document.querySelector(".driver-popover-next-btn")!);
    await waitFor(() => expect(pressed).toEqual(["add", "parquet", "cancel"]));
    expect(document.querySelector('[data-tour="sources-form"]')).toBeNull();
    expect(document.querySelector('[data-testid^="sources-row-"]')).toBeNull();
  }, 30000);

  it("entered with the form open, only explains", async () => {
    const pressed = sourcesScreen(true);
    renderTour();
    fireEvent.click(screen.getByText("launch"));
    await waitFor(() => expect(title()).toBe("tour.steps.stepBuildSource.title"), {
      timeout: 10000,
    });
    expect(pressed).toEqual([]);
  });

  it("is stuck, offering Retry and Skip, when an action cannot be done", async () => {
    // No Sources screen: the step's first action finds no + Source.
    renderTour();
    fireEvent.click(screen.getByText("launch"));
    expect(await screen.findByText("tour.steps.stepBuildSource.acting")).toBeInTheDocument();
    // Past the action's element wait (ANCHOR_WAIT_MS), a second at a time: each wait polls, and
    // a poll set before the clock was faked resolves on the real one.
    const realPause = globalThis.setTimeout.bind(globalThis);
    vi.useFakeTimers({ shouldAdvanceTime: true });
    for (let s = 0; s < 60 && !screen.queryByText("tour.status.stuck"); s++) {
      await act(async () => {
        await vi.advanceTimersByTimeAsync(1000);
        await new Promise((r) => realPause(r, 5));
      });
    }
    expect(screen.getByText("tour.status.stuck")).toBeInTheDocument();
    expect(screen.getByText("tour.status.skip")).toBeInTheDocument();
    expect(document.body.classList.contains("tour-acting")).toBe(false);
  });
});
