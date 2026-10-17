// Copyright (c) 2026 Kenneth Stott
// Canary: 85b7128f-b37f-4255-9c3a-7562d1be2380
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.

// REQ-2071: the Data Science area's Python tab. The Python worker is a double: what is asserted is
// what the page tells it and how it shows what comes back.

import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, waitFor, fireEvent } from "@testing-library/react";
import { MantineProvider } from "@mantine/core";
import "../i18n";

vi.mock("../dataScience/bridge", () => ({ bridgeAuth: vi.fn() }));

import { bridgeAuth } from "../dataScience/bridge";
import { DataSciencePage, STARTER_CODE, type MakeWorker } from "../pages/DataSciencePage";
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

function page(make: MakeWorker) {
  return render(
    <MantineProvider>
      <DataSciencePage makeWorker={make} />
    </MantineProvider>,
  );
}

function serve(manifest: Response) {
  window.fetch = vi.fn(async () => manifest) as typeof window.fetch;
}

beforeEach(() => {
  vi.clearAllMocks();
  vi.mocked(bridgeAuth).mockResolvedValue(AUTH);
});

describe("DataSciencePage", () => {
  it("says the server's notebook assets are missing when /pyodide is not served", async () => {
    serve(new Response('{"detail":"Not Found"}', { status: 404 }));
    const { make, double } = workerDouble();
    page(make);
    expect(await screen.findByTestId("data-science-missing")).toBeInTheDocument();
    expect(screen.getByText("This server's notebook assets are missing")).toBeInTheDocument();
    expect(screen.getByText(/Reinstall Provisa on the server/)).toBeInTheDocument();
    expect(double.postMessage).not.toHaveBeenCalled();
  });

  it("starts Python as the page's user and runs the starter cell with the identity of the moment", async () => {
    serve(
      new Response(JSON.stringify(MANIFEST), { headers: { "content-type": "application/json" } }),
    );
    const { make, sent, reply } = workerDouble();
    page(make);
    await waitFor(() => expect(sent[0]).toEqual({ type: "init", manifest: MANIFEST, auth: AUTH }));
    const runButton = screen.getByTestId("data-science-run");
    expect(runButton).toBeDisabled();
    reply({ type: "ready" });
    await waitFor(() => expect(runButton).toBeEnabled());
    vi.mocked(bridgeAuth).mockResolvedValue({ ...AUTH, role: "org_admin", env: "staging" });
    fireEvent.click(runButton);
    await waitFor(() => expect(sent).toHaveLength(3));
    expect(sent[1]).toEqual({
      type: "auth",
      auth: { ...AUTH, role: "org_admin", env: "staging" },
    });
    expect(sent[2]).toEqual({ type: "run", id: 1, code: STARTER_CODE });
  });

  it("runs on Ctrl+Enter in the editor", async () => {
    serve(
      new Response(JSON.stringify(MANIFEST), { headers: { "content-type": "application/json" } }),
    );
    const { make, sent, reply } = workerDouble();
    page(make);
    await waitFor(() => expect(sent).toHaveLength(1));
    reply({ type: "ready" });
    await waitFor(() => expect(screen.getByTestId("data-science-run")).toBeEnabled());
    const editor = screen.getByTestId("data-science-editor");
    fireEvent.keyDown(editor, { key: "Enter", ctrlKey: true });
    await waitFor(() => expect(sent.at(-1)).toEqual({ type: "run", id: 1, code: STARTER_CODE }));
  });

  it("shows a run's printed output, its table, its figures and its error", async () => {
    serve(
      new Response(JSON.stringify(MANIFEST), { headers: { "content-type": "application/json" } }),
    );
    const { make, sent, reply } = workerDouble();
    page(make);
    await waitFor(() => expect(sent).toHaveLength(1));
    reply({ type: "ready" });
    reply({
      type: "result",
      id: 1,
      stdout: "rows 2\n",
      value: { kind: "table", columns: ["region", "n"], rows: [["EU", 3], ["US", null]], total: 9 },
      images: ["iVBORw0KGgo="],
      error: null,
    });
    expect(await screen.findByTestId("data-science-stdout")).toHaveTextContent("rows 2");
    const table = screen.getByTestId("data-science-table");
    expect(table).toHaveTextContent("region");
    expect(table).toHaveTextContent("EU");
    expect(screen.getByText("Showing 2 of 9 rows")).toBeInTheDocument();
    expect(screen.getByTestId("data-science-figure")).toHaveAttribute(
      "src",
      "data:image/png;base64,iVBORw0KGgo=",
    );
    reply({
      type: "result",
      id: 2,
      stdout: "",
      value: { kind: "none" },
      images: [],
      error: "NameError: name 'x' is not defined",
    });
    expect(await screen.findByTestId("data-science-error")).toHaveTextContent("NameError");
  });

  it("splits the area between editor and output, the rule moved from the keyboard within bounds", async () => {
    serve(
      new Response(JSON.stringify(MANIFEST), { headers: { "content-type": "application/json" } }),
    );
    const { make, sent, reply } = workerDouble();
    page(make);
    await waitFor(() => expect(sent).toHaveLength(1));
    reply({ type: "ready" });
    const rule = await screen.findByTestId("data-science-split");
    expect(rule).toHaveAttribute("aria-valuenow", "45");
    fireEvent.keyDown(rule, { key: "ArrowDown" });
    expect(rule).toHaveAttribute("aria-valuenow", "50");
    for (let i = 0; i < 20; i++) fireEvent.keyDown(rule, { key: "ArrowUp" });
    expect(rule).toHaveAttribute("aria-valuenow", "15");
    expect(screen.queryByRole("heading", { name: "Data Science" })).not.toBeInTheDocument();
  });

  it("says plainly when Python cannot start", async () => {
    serve(
      new Response(JSON.stringify(MANIFEST), { headers: { "content-type": "application/json" } }),
    );
    const { make, sent, reply } = workerDouble();
    page(make);
    await waitFor(() => expect(sent).toHaveLength(1));
    reply({ type: "failed", error: "pyodide.asm.wasm: 404" });
    expect(await screen.findByTestId("data-science-failure")).toHaveTextContent("404");
    expect(screen.getByTestId("data-science-run")).toBeDisabled();
  });
});
