// Copyright (c) 2026 Kenneth Stott
// Canary: a98c096b-249c-47ef-8770-723f63cb5fae
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.

// REQ-2084: the notebook a named demo ships, on the Notebook tab. The embedded JupyterLite app is
// a double: what is asserted is which cells the tab hands it, whether it runs them, and that the
// tab says when they have answered, and points a tour step at the last cell's outputs.

import { describe, it, expect, vi, beforeEach } from "vitest";
import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import { MantineProvider } from "@mantine/core";
import { MemoryRouter, Route, Routes } from "react-router-dom";
import "../i18n";

vi.mock("../dataScience/bridge", () => ({ bridgeAuth: vi.fn() }));
vi.mock("../hooks/useCapability", () => ({ useCapability: () => true }));
vi.mock("../dataScience/liteBridge", async (real) => ({
  ...(await real<typeof import("../dataScience/liteBridge")>()),
  runCells: vi.fn(async () => ({ stdout: "trained\n", value: "", error: null })),
  lastOutputArea: vi.fn(() => null),
  startLiteBridge: vi.fn(() => ({ stop: () => {}, handOff: async () => {} })),
  seedNewNotebooks: vi.fn(() => () => {}),
  followScheme: vi.fn(async () => undefined),
}));

import { lastOutputArea, runCells } from "../dataScience/liteBridge";
import { DataScienceHost, type MakeWorker } from "../pages/DataSciencePage";
import { NamedDemoContext, NO_NAMED_DEMO } from "../context/NamedDemoContext";

const CELLS = ["import provisa_client\nprovisa_client.sign_in()", "model.score(X, y)"];
const APP = { restored: Promise.resolve(), shell: {}, commands: {} };

function page(notebook: string[], state?: Record<string, unknown>) {
  const make = (() => ({ postMessage: vi.fn(), terminate: vi.fn() })) as unknown as MakeWorker;
  return render(
    <MantineProvider>
      <NamedDemoContext.Provider value={{ ...NO_NAMED_DEMO, name: "churn", notebook }}>
        <MemoryRouter initialEntries={[{ pathname: "/notebook", state }]}>
          <DataScienceHost makeWorker={make} />
          <Routes>
            <Route path="/notebook" element={null} />
          </Routes>
        </MemoryRouter>
      </NamedDemoContext.Provider>
    </MantineProvider>,
  );
}

/** The embedded app comes up in the frame, as JupyterLite's does once it has loaded. */
async function appStarts() {
  const frame = (await screen.findByTestId("data-science-notebook")) as HTMLIFrameElement;
  Object.defineProperty(frame.contentWindow, "jupyterapp", { value: APP, configurable: true });
  fireEvent.load(frame);
}

beforeEach(() => {
  vi.clearAllMocks();
  window.fetch = vi.fn(
    async () => new Response("{}", { headers: { "content-type": "application/json" } }),
  ) as typeof window.fetch;
});

describe("The demo's notebook on the Notebook tab", () => {
  it("is offered, and adds its cells to the open notebook without running them", async () => {
    page(CELLS);
    await appStarts();
    const open = screen.getByTestId("notebook-open-demo");
    await waitFor(() => expect(open).toBeEnabled());
    fireEvent.click(open);
    await waitFor(() =>
      expect(runCells).toHaveBeenCalledWith(APP, CELLS, false, expect.any(Function)),
    );
  });

  it("is not offered when the demo ships none", async () => {
    page([]);
    await appStarts();
    expect(screen.queryByTestId("notebook-open-demo")).not.toBeInTheDocument();
  });

  it("runs the cells a tour step hands over, and says once they have answered", async () => {
    page(CELLS, { cells: CELLS, autoRun: true });
    expect(screen.queryByTestId("notebook-answered")).not.toBeInTheDocument();
    await appStarts();
    await waitFor(() =>
      expect(runCells).toHaveBeenCalledWith(APP, CELLS, true, expect.any(Function)),
    );
    expect(await screen.findByTestId("notebook-answered")).toBeInTheDocument();
  });

  it("scrolls the last cell's outputs into view and marks where they are for the tour", async () => {
    // The figure's outputs in the frame, 300 pixels tall from its top.
    const area = document.createElement("div");
    area.scrollIntoView = vi.fn();
    area.getBoundingClientRect = () =>
      ({ top: 10, bottom: 310, left: 20, right: 520, width: 500, height: 300 }) as DOMRect;
    vi.mocked(lastOutputArea).mockReturnValue(area);
    page(CELLS, { cells: CELLS, autoRun: true });
    // The frame, 800 pixels tall: the whole figure inside it.
    const frame = await screen.findByTestId("data-science-notebook");
    frame.getBoundingClientRect = () =>
      ({ top: 0, bottom: 800, left: 0, right: 1000, width: 1000, height: 800 }) as DOMRect;
    await appStarts();
    const answer = await screen.findByTestId("notebook-answer");
    expect(area.scrollIntoView).toHaveBeenCalledWith({ block: "start" });
    expect(answer.style.height).toBe("300px");
    expect(answer.style.width).toBe("500px");
  });

  it("marks nothing for the tour when the cells are only added", async () => {
    vi.mocked(lastOutputArea).mockReturnValue(document.createElement("div"));
    page(CELLS);
    await appStarts();
    const open = screen.getByTestId("notebook-open-demo");
    await waitFor(() => expect(open).toBeEnabled());
    fireEvent.click(open);
    await waitFor(() => expect(runCells).toHaveBeenCalled());
    expect(screen.queryByTestId("notebook-answer")).not.toBeInTheDocument();
  });
});
