// Copyright (c) 2026 Kenneth Stott
// Canary: 85b7128f-b37f-4255-9c3a-7562d1be2380
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.

// REQ-2071: the Data Science area's Python tab. The Python worker is a double: what is asserted is
// what the page tells it and how it shows what comes back.

import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, waitFor, fireEvent, within } from "@testing-library/react";
import { MantineProvider } from "@mantine/core";
import { Link, MemoryRouter, Route, Routes } from "react-router-dom";
import "../i18n";

vi.mock("../dataScience/bridge", () => ({ bridgeAuth: vi.fn() }));
vi.mock("../hooks/useCapability", () => ({ useCapability: () => true }));
vi.mock("../dataScience/runReports", async (real) => ({
  ...(await real<typeof import("../dataScience/runReports")>()),
  reportRun: vi.fn(),
}));

import { bridgeAuth } from "../dataScience/bridge";
import { reportRun } from "../dataScience/runReports";
import { DataScienceHost, STARTER_CODE, type MakeWorker } from "../pages/DataSciencePage";
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
  // The scripts are kept in the browser: each test starts with none kept.
  window.localStorage.clear();
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
    await waitFor(() => expect(sent[0]).toEqual({ type: "init", auth: AUTH }));
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
    // A result is the reply to a run: of the script that ran.
    await waitFor(() => expect(screen.getByTestId("data-science-run")).toBeEnabled());
    fireEvent.click(screen.getByTestId("data-science-run"));
    await waitFor(() => expect(sent.at(-1)).toMatchObject({ type: "run", id: 1 }));
    reply({
      type: "result",
      id: 1,
      stdout: "rows 2\n",
      value: {
        kind: "table",
        columns: ["region", "n"],
        rows: [
          ["EU", 3],
          ["US", null],
        ],
        total: 9,
      },
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
    await waitFor(() => expect(screen.getByTestId("data-science-run")).toBeEnabled());
    fireEvent.click(screen.getByTestId("data-science-run"));
    await waitFor(() => expect(sent.at(-1)).toMatchObject({ type: "run", id: 2 }));
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

  it("splits editor (left) and output (right), the rule moved from the keyboard within bounds", async () => {
    serve(
      new Response(JSON.stringify(MANIFEST), { headers: { "content-type": "application/json" } }),
    );
    const { make, sent, reply } = workerDouble();
    page(make);
    await waitFor(() => expect(sent).toHaveLength(1));
    reply({ type: "ready" });
    const rule = await screen.findByTestId("data-science-split");
    expect(rule).toHaveAttribute("aria-orientation", "vertical");
    expect(rule).toHaveAttribute("aria-valuenow", "45");
    fireEvent.keyDown(rule, { key: "ArrowRight" });
    expect(rule).toHaveAttribute("aria-valuenow", "50");
    for (let i = 0; i < 20; i++) fireEvent.keyDown(rule, { key: "ArrowLeft" });
    expect(rule).toHaveAttribute("aria-valuenow", "15");
  });

  it("runs code it is handed once Python is ready, and reports the run under its id", async () => {
    serve(
      new Response(JSON.stringify(MANIFEST), { headers: { "content-type": "application/json" } }),
    );
    const { make, sent, reply } = workerDouble();
    page(make, { code: "print(2)", autoRun: true, runId: "polly-7" });
    await waitFor(() => expect(sent).toHaveLength(1));
    expect(sent.find((m) => m.type === "run")).toBeUndefined();
    reply({ type: "ready" });
    await waitFor(() => expect(sent.at(-1)).toEqual({ type: "run", id: 1, code: "print(2)" }));
    reply({
      type: "result",
      id: 1,
      stdout: "2\n",
      value: { kind: "none" },
      images: [],
      error: null,
    });
    await waitFor(() =>
      expect(reportRun).toHaveBeenCalledWith("polly-7", { stdout: "2\n", value: "", error: null }),
    );
  });

  it("reports at once that a handed run cannot run when the notebook assets are missing", async () => {
    serve(new Response('{"detail":"Not Found"}', { status: 404 }));
    const { make } = workerDouble();
    page(make, { code: "1", autoRun: true, runId: "polly-8" });
    await waitFor(() =>
      expect(reportRun).toHaveBeenCalledWith("polly-8", {
        stdout: "",
        value: "",
        error: "This server's notebook assets are missing",
      }),
    );
  });

  it("opens the Notebook tab on the starter notebook, mounting Python only when it is visited", async () => {
    window.fetch = vi.fn(async (url: RequestInfo | URL) =>
      String(url).includes("jupyter-lite.json")
        ? new Response("{}", { headers: { "content-type": "application/json" } })
        : new Response('{"detail":"Not Found"}', { status: 404 }),
    ) as typeof window.fetch;
    const { make } = workerDouble();
    page(make, undefined, "/notebook");
    const frame = await screen.findByTestId("data-science-notebook");
    expect(frame).toHaveAttribute("src", "/lite/notebooks/index.html?path=Provisa.ipynb");
    expect(screen.queryByTestId("python-panel")).not.toBeInTheDocument();
  });

  it("says the notebook's assets are missing when /lite is not served", async () => {
    serve(new Response('{"detail":"Not Found"}', { status: 404 }));
    const { make } = workerDouble();
    page(make, undefined, "/notebook");
    expect(await screen.findByTestId("data-science-notebook-missing")).toBeInTheDocument();
  });

  it("hands cells to the Notebook tab and code to the Python tab, each by its route", async () => {
    window.fetch = vi.fn(async (url: RequestInfo | URL) =>
      String(url).includes("jupyter-lite.json")
        ? new Response("{}", { headers: { "content-type": "application/json" } })
        : new Response(JSON.stringify(MANIFEST), {
            headers: { "content-type": "application/json" },
          }),
    ) as typeof window.fetch;
    const { make } = workerDouble();
    page(make, { code: "print('handed')", autoRun: false });
    expect(await screen.findByText("print('handed')")).toBeInTheDocument();
  });

  it("keeps a visited tab alive, only hidden, while the user is on another page", async () => {
    serve(
      new Response(JSON.stringify(MANIFEST), { headers: { "content-type": "application/json" } }),
    );
    const { make, double } = workerDouble();
    page(make);
    const panel = await screen.findByTestId("python-panel");
    expect(panel).toBeVisible();
    fireEvent.click(screen.getByText("to sql"));
    await waitFor(() => expect(screen.getByTestId("python-panel")).not.toBeVisible());
    expect(double.terminate).not.toHaveBeenCalled();
    fireEvent.click(screen.getByText("back to python"));
    await waitFor(() => expect(screen.getByTestId("python-panel")).toBeVisible());
    expect(screen.getByTestId("python-panel")).toBe(panel);
  });

  it("maximizes the tab in place and restores it with its button or Escape", async () => {
    serve(
      new Response(JSON.stringify(MANIFEST), { headers: { "content-type": "application/json" } }),
    );
    const { make, double } = workerDouble();
    page(make);
    const panel = await screen.findByTestId("python-panel");
    fireEvent.click(screen.getByTestId("python-panel-maximize"));
    expect(panel).toHaveClass("ds-max");
    expect(screen.getByTestId("data-science-backdrop")).toBeInTheDocument();
    fireEvent.keyDown(window, { key: "Escape" });
    await waitFor(() => expect(panel).not.toHaveClass("ds-max"));
    fireEvent.click(screen.getByTestId("python-panel-maximize"));
    fireEvent.click(screen.getByTestId("python-panel-maximize"));
    expect(panel).not.toHaveClass("ds-max");
    expect(screen.getByTestId("python-panel")).toBe(panel);
    expect(double.terminate).not.toHaveBeenCalled();
  });

  it("copies the editor's code and clears it from its hover controls", async () => {
    serve(
      new Response(JSON.stringify(MANIFEST), { headers: { "content-type": "application/json" } }),
    );
    const writeText = vi.fn(async () => undefined);
    Object.assign(navigator, { clipboard: { writeText } });
    const { make, sent, reply } = workerDouble();
    page(make);
    await waitFor(() => expect(sent).toHaveLength(1));
    reply({ type: "ready" });
    const controls = await screen.findByTestId("data-science-editor-controls");
    fireEvent.click(within(controls).getByTestId("copy-symbol-button"));
    await waitFor(() => expect(writeText).toHaveBeenCalledWith(STARTER_CODE));
    fireEvent.click(screen.getByTestId("data-science-clear"));
    await waitFor(() =>
      expect(screen.queryByText("provisa_client.sign_in()")).not.toBeInTheDocument(),
    );
  });

  it("copies a run's output: what it printed and its table as tab-separated values", async () => {
    serve(
      new Response(JSON.stringify(MANIFEST), { headers: { "content-type": "application/json" } }),
    );
    const writeText = vi.fn(async () => undefined);
    Object.assign(navigator, { clipboard: { writeText } });
    const { make, sent, reply } = workerDouble();
    page(make);
    await waitFor(() => expect(sent).toHaveLength(1));
    reply({ type: "ready" });
    // A result is the reply to a run: of the script that ran.
    await waitFor(() => expect(screen.getByTestId("data-science-run")).toBeEnabled());
    fireEvent.click(screen.getByTestId("data-science-run"));
    await waitFor(() => expect(sent.at(-1)).toMatchObject({ type: "run", id: 1 }));
    reply({
      type: "result",
      id: 1,
      stdout: "rows 2\n",
      value: {
        kind: "table",
        columns: ["region", "n"],
        rows: [
          ["EU", 3],
          ["US", null],
        ],
        total: 2,
      },
      images: [],
      error: null,
    });
    const controls = await screen.findByTestId("data-science-output-controls");
    fireEvent.click(within(controls).getByTestId("copy-symbol-button"));
    await waitFor(() => expect(writeText).toHaveBeenCalledWith("rows 2\n\nregion\tn\nEU\t3\nUS\t"));
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

describe("Python script tabs", () => {
  const ready = async (sent: ToWorker[], reply: (m: FromWorker) => void) => {
    await waitFor(() => expect(sent).toHaveLength(1));
    reply({ type: "ready" });
    await waitFor(() => expect(screen.getByTestId("data-science-run")).toBeEnabled());
  };
  const tabs = () => screen.getAllByTestId(/^python-tab-/);
  const titles = () => tabs().map((t) => t.textContent);
  const result = (id: number, stdout: string): FromWorker => ({
    type: "result",
    id,
    stdout,
    value: { kind: "none" },
    images: [],
    error: null,
  });
  const manifest = () =>
    serve(
      new Response(JSON.stringify(MANIFEST), { headers: { "content-type": "application/json" } }),
    );

  it("keeps each script's own code and last output; a run is the active script's", async () => {
    manifest();
    const { make, sent, reply } = workerDouble();
    page(make);
    await ready(sent, reply);
    expect(titles()).toEqual(["Script 1"]);
    fireEvent.click(screen.getByTestId("python-new-tab"));
    expect(titles()).toEqual(["Script 1", "Script 2"]);
    fireEvent.click(screen.getByTestId("data-science-run"));
    await waitFor(() => expect(sent.at(-1)).toEqual({ type: "run", id: 1, code: STARTER_CODE }));
    // The output lands on the script that ran, though another is shown when it arrives.
    fireEvent.click(tabs()[0]);
    reply(result(1, "from script 2"));
    await waitFor(() => expect(screen.getByTestId("data-science-run")).toBeEnabled());
    expect(screen.queryByTestId("data-science-stdout")).not.toBeInTheDocument();
    fireEvent.click(tabs()[1]);
    expect(await screen.findByTestId("data-science-stdout")).toHaveTextContent("from script 2");
  });

  it("renames, moves and closes scripts, and finds them as they were after a reload", async () => {
    manifest();
    const first = workerDouble();
    const view = page(first.make);
    await ready(first.sent, first.reply);
    fireEvent.click(screen.getByTestId("python-new-tab"));
    fireEvent.click(screen.getByTestId("python-new-tab"));
    expect(titles()).toEqual(["Script 1", "Script 2", "Script 3"]);
    fireEvent.doubleClick(tabs()[1]);
    const input = within(tabs()[1]).getByRole("textbox");
    fireEvent.change(input, { target: { value: "cleanup" } });
    fireEvent.keyDown(input, { key: "Enter" });
    expect(titles()).toEqual(["Script 1", "cleanup", "Script 3"]);
    // Alt+ArrowLeft moves the focused tab one place left.
    fireEvent.keyDown(tabs()[2], { key: "ArrowLeft", altKey: true });
    expect(titles()).toEqual(["Script 1", "Script 3", "cleanup"]);
    fireEvent.click(within(tabs()[0]).getByRole("button", { name: "Close Script 1" }));
    expect(titles()).toEqual(["Script 3", "cleanup"]);
    fireEvent.click(tabs()[1]);
    view.unmount();

    manifest();
    const second = workerDouble();
    page(second.make);
    await ready(second.sent, second.reply);
    expect(titles()).toEqual(["Script 3", "cleanup"]);
    expect(tabs()[1]).toHaveStyle({ fontWeight: "600" }); // still the active one
  });

  it("shows each script's own code when switched to", async () => {
    manifest();
    const { make, sent, reply } = workerDouble();
    page(make, { code: "print('first')", autoRun: false });
    await ready(sent, reply);
    expect(screen.getByText("print('first')")).toBeInTheDocument();
    fireEvent.click(screen.getByTestId("python-new-tab"));
    await waitFor(() => expect(screen.queryByText("print('first')")).not.toBeInTheDocument());
    expect(screen.getByText("provisa_client.sign_in()")).toBeInTheDocument();
    fireEvent.click(tabs()[0]);
    expect(await screen.findByText("print('first')")).toBeInTheDocument();
  });

  it("closing the last script leaves a fresh one", async () => {
    manifest();
    const { make, sent, reply } = workerDouble();
    page(make);
    await ready(sent, reply);
    fireEvent.click(within(tabs()[0]).getByRole("button", { name: "Close Script 1" }));
    expect(titles()).toEqual(["Script 1"]);
    expect(screen.getByText("provisa_client.sign_in()")).toBeInTheDocument();
  });

  it("writes Polly's code into a new script tab when asked, and runs it there", async () => {
    manifest();
    const { make, sent, reply } = workerDouble();
    page(make, { code: "print('in its own tab')", newTab: true, title: "churn", autoRun: true });
    await waitFor(() => expect(sent).toHaveLength(1));
    expect(titles()).toEqual(["Script 1", "churn"]);
    reply({ type: "ready" });
    await waitFor(() =>
      expect(sent.at(-1)).toEqual({ type: "run", id: 1, code: "print('in its own tab')" }),
    );
    reply(result(1, "ran in churn"));
    expect(await screen.findByTestId("data-science-stdout")).toHaveTextContent("ran in churn");
    fireEvent.click(tabs()[0]);
    expect(screen.queryByTestId("data-science-stdout")).not.toBeInTheDocument();
  });

  it("works for the session when the browser refuses its storage", async () => {
    manifest();
    const refuse = () => {
      throw new DOMException("blocked", "SecurityError");
    };
    const get = vi.spyOn(Storage.prototype, "getItem").mockImplementation(refuse);
    const set = vi.spyOn(Storage.prototype, "setItem").mockImplementation(refuse);
    try {
      const { make, sent, reply } = workerDouble();
      page(make);
      await ready(sent, reply);
      fireEvent.click(screen.getByTestId("python-new-tab"));
      expect(titles()).toEqual(["Script 1", "Script 2"]);
    } finally {
      get.mockRestore();
      set.mockRestore();
    }
  });
});
