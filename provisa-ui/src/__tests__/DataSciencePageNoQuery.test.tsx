// Copyright (c) 2026 Kenneth Stott
// Canary: b7d20f4a-8e15-4c93-a6d1-39c5e08b7f21
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.

// REQ-2071: the Data Science area's Python tab. The Python worker is a double: what is asserted is
// what the page tells it and how it shows what comes back.

// A Python editor run with no code in it (blank, or only comments) says so in the output pane,
// before anything is sent to the worker. Real code still runs.

import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, waitFor, fireEvent } from "@testing-library/react";
import { MantineProvider } from "@mantine/core";
import { Link, MemoryRouter, Route, Routes } from "react-router-dom";
import "../i18n";

vi.mock("../dataScience/bridge", () => ({ bridgeAuth: vi.fn() }));
vi.mock("../hooks/useCapability", () => ({ useCapability: () => true }));

import { bridgeAuth } from "../dataScience/bridge";
import { DataScienceHost, type MakeWorker } from "../pages/DataSciencePage";
import type { FromWorker, ToWorker } from "../dataScience/protocol";

const MANIFEST = {
  pyodide: "0.27.7",
  packages: ["pandas"],
  client_wheel: "wheels/provisa_client-0.1.0-py3-none-any.whl",
};
const AUTH = { baseUrl: "", role: "analyst_eu", bearer: "tok", org: "acme", env: null };

function workerDouble() {
  const sent: ToWorker[] = [];
  const double = {
    onmessage: null as ((e: MessageEvent<FromWorker>) => void) | null,
    postMessage: vi.fn((m: ToWorker) => sent.push(m)),
    terminate: vi.fn(),
  };
  const reply = (m: FromWorker) => double.onmessage?.({ data: m } as MessageEvent<FromWorker>);
  const make = (() => double) as unknown as MakeWorker;
  return { sent, double, reply, make };
}

/** The host at a route, as the app shows it: /python by default. */
function page(make: MakeWorker, state?: Record<string, unknown>, pathname = "/python") {
  return render(
    <MantineProvider>
      <MemoryRouter initialEntries={[{ pathname, state }]}>
        <DataScienceHost makeWorker={make} />
        <Routes>
          <Route path="/python" element={<Link to="/sql">to sql</Link>} />
          <Route path="/notebook" element={<Link to="/python">to python</Link>} />
          <Route path="/sql" element={<Link to="/python">back to python</Link>} />
        </Routes>
      </MemoryRouter>
    </MantineProvider>,
  );
}

function serve(manifest: Response) {
  window.fetch = vi.fn(async () => manifest) as typeof window.fetch;
}


beforeEach(() => {
  vi.clearAllMocks();
  window.localStorage.clear();
  vi.mocked(bridgeAuth).mockResolvedValue(AUTH);
});

describe("Python editor with no code", () => {
  it.each([
    ["blank", "  \n"],
    ["comment-only", "# nothing here\n"],
  ])("%s: says there is no query and sends nothing to run", async (_n, code) => {
    serve(
      new Response(JSON.stringify(MANIFEST), { headers: { "content-type": "application/json" } }),
    );
    const { make, sent, reply } = workerDouble();
    page(make, { code, autoRun: false });
    await waitFor(() => expect(sent[0]).toEqual({ type: "init", auth: AUTH }));
    reply({ type: "ready" });
    const run = screen.getByTestId("data-science-run");
    await waitFor(() => expect(run).toBeEnabled());
    fireEvent.click(run);
    expect(await screen.findByTestId("data-science-no-query")).toHaveTextContent(
      "There is no query to run.",
    );
    expect(sent.filter((m) => m.type === "run")).toHaveLength(0);
  });

  it("real code still runs", async () => {
    serve(
      new Response(JSON.stringify(MANIFEST), { headers: { "content-type": "application/json" } }),
    );
    const { make, sent, reply } = workerDouble();
    page(make, { code: "# c\nprint(1)", autoRun: false });
    await waitFor(() => expect(sent[0]).toEqual({ type: "init", auth: AUTH }));
    reply({ type: "ready" });
    const run = screen.getByTestId("data-science-run");
    await waitFor(() => expect(run).toBeEnabled());
    fireEvent.click(run);
    await waitFor(() => expect(sent.some((m) => m.type === "run")).toBe(true));
    expect(screen.queryByTestId("data-science-no-query")).toBeNull();
  });
});
