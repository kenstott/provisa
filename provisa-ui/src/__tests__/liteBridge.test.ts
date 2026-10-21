// Copyright (c) 2026 Kenneth Stott
// Canary: 3713314a-aa9f-47e9-94e1-7038058800df
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.

// REQ-2071: each notebook kernel is handed the page's user before the first code it runs, again
// when the user's identity changes and after a restart; a new, empty notebook opens on the
// starter cell. The JupyterLite app is a double.

import { describe, it, expect, vi, afterEach } from "vitest";
import {
  FONT_CACHE_CODE,
  LITE_SPLASH_ID,
  LITE_THEMES,
  NOTEBOOK_STARTER,
  followScheme,
  runCells,
  bridgeCode,
  seedNewNotebooks,
  startLiteBridge,
  type KernelConnection,
  type LiteApp,
} from "../dataScience/liteBridge";
import type { ProvisaAuth } from "../dataScience/protocol";

const AUTH: ProvisaAuth = { baseUrl: "", role: "analyst_eu", bearer: "tok", org: null, env: null };

function kernel() {
  const sent: Array<{ code: string; silent?: boolean }> = [];
  let onStatus: (sender: unknown, status: string) => void = () => {};
  const k: KernelConnection = {
    requestExecute: (content) => {
      sent.push(content);
      return undefined;
    },
    statusChanged: { connect: (slot) => (onStatus = slot) },
  };
  return { k, sent, status: (s: string) => onStatus(undefined, s) };
}

function app(kernels: KernelConnection[], current: unknown = null) {
  const slots: Array<() => void> = [];
  const lite: LiteApp = {
    restored: Promise.resolve(),
    commands: { execute: vi.fn(async () => true) },
    shell: {
      widgets: () => kernels.map((k) => ({ sessionContext: { session: { kernel: k } } })),
      currentWidget: current,
      currentChanged: {
        connect: (slot) => slots.push(slot),
        disconnect: vi.fn(),
      },
    },
  };
  return { lite, fire: () => slots.forEach((s) => s()) };
}

afterEach(() => vi.useRealTimers());

describe("startLiteBridge", () => {
  it("hands a kernel the user once, before the first code it runs", async () => {
    vi.useFakeTimers();
    const { k, sent } = kernel();
    const { lite } = app([k]);
    const { stop } = startLiteBridge(
      () => lite,
      async () => AUTH,
    );
    await vi.advanceTimersByTimeAsync(0);
    expect(sent).toEqual([{ code: bridgeCode(AUTH), silent: true, store_history: false }]);
    k.requestExecute({ code: "provisa_client.sign_in()" });
    await vi.advanceTimersByTimeAsync(2000);
    expect(sent.map((m) => m.code)).toEqual([bridgeCode(AUTH), "provisa_client.sign_in()"]);
    stop();
  });

  it("hands a restarted kernel the user again before its next code", async () => {
    vi.useFakeTimers();
    const { k, sent, status } = kernel();
    const { lite } = app([k]);
    const { stop } = startLiteBridge(
      () => lite,
      async () => AUTH,
      60_000,
    );
    await vi.advanceTimersByTimeAsync(0);
    status("restarting");
    k.requestExecute({ code: "1 + 1" });
    expect(sent.map((m) => m.code)).toEqual([bridgeCode(AUTH), bridgeCode(AUTH), "1 + 1"]);
    stop();
  });

  it("hands the kernel the new identity when the page's user changes", async () => {
    vi.useFakeTimers();
    const { k, sent } = kernel();
    const { lite } = app([k]);
    let auth = AUTH;
    const { stop } = startLiteBridge(
      () => lite,
      async () => auth,
    );
    await vi.advanceTimersByTimeAsync(0);
    auth = { ...AUTH, role: "org_admin", env: "staging" };
    await vi.advanceTimersByTimeAsync(2000);
    expect(sent.map((m) => m.code)).toEqual([bridgeCode(AUTH), bridgeCode(auth)]);
    stop();
  });

  it("sets provisaAuth from the identity as JSON Python can parse", () => {
    const code = bridgeCode({ ...AUTH, bearer: 'quote"and\\back' });
    expect(code).toContain("_provisa_js.self.provisaAuth = _provisa_js.JSON.parse(");
    const literal = code.split("JSON.parse(")[1].split(")\n")[0];
    expect(JSON.parse(JSON.parse(literal))).toEqual({ ...AUTH, bearer: 'quote"and\\back' });
  });
});

describe("seedNewNotebooks", () => {
  function notebook(sources: string[]) {
    const cells = sources.map((source) => {
      let text = source;
      return { getSource: () => text, setSource: (s: string) => (text = s) };
    });
    return { widget: { content: { model: { sharedModel: { cells } } } }, cells };
  }

  it("opens a new, empty notebook on the starter cell and leaves others as they are", async () => {
    const fresh = notebook([""]);
    const { lite, fire } = app([], fresh.widget);
    seedNewNotebooks(lite);
    await vi.waitFor(() => expect(fresh.cells[0].getSource()).toBe(NOTEBOOK_STARTER));
    const written = notebook(["print(1)"]);
    lite.shell.currentWidget = written.widget;
    fire();
    await Promise.resolve();
    expect(written.cells[0].getSource()).toBe("print(1)");
  });

  it("seeds a notebook made current before its document has loaded, once it has", async () => {
    let load = () => {};
    const ready = new Promise<void>((resolve) => (load = resolve));
    const cells: Array<{ getSource(): string; setSource(s: string): void }> = [];
    const widget = { context: { ready }, content: { model: { sharedModel: { cells } } } };
    const { lite } = app([], widget);
    seedNewNotebooks(lite);
    let text = "";
    cells.push({ getSource: () => text, setSource: (s: string) => (text = s) });
    load();
    await vi.waitFor(() => expect(text).toBe(NOTEBOOK_STARTER));
  });
});

describe("followScheme", () => {
  it("waits for the app's startup splash to be gone before asking for the theme", async () => {
    // JupyterLab removes its splash 200 ms after it is restored; a theme loaded in between shows
    // the splash again and has it removed twice, which throws inside the app.
    const { lite } = app([]);
    const splash = document.createElement("div");
    splash.id = LITE_SPLASH_ID;
    document.body.appendChild(splash);
    const following = followScheme(lite, "light", document);
    await new Promise((resolve) => setTimeout(resolve, 0));
    expect(lite.commands.execute).not.toHaveBeenCalled();
    splash.remove();
    await following;
    expect(lite.commands.execute).toHaveBeenCalledWith("apputils:change-theme", {
      theme: LITE_THEMES.light,
    });
  });

  it("waits for the app to be restored before asking for the theme", async () => {
    const { lite } = app([]);
    let restore = () => {};
    lite.restored = new Promise<void>((resolve) => (restore = resolve));
    const following = followScheme(lite, "dark", document);
    await Promise.resolve();
    expect(lite.commands.execute).not.toHaveBeenCalled();
    restore();
    await following;
    expect(lite.commands.execute).toHaveBeenCalledWith("apputils:change-theme", {
      theme: LITE_THEMES.dark,
    });
  });

  it("puts the embedded app in the app's colour scheme with JupyterLab's theme command", async () => {
    const { lite } = app([]);
    await followScheme(lite, "dark", document);
    await followScheme(lite, "light", document);
    expect(vi.mocked(lite.commands.execute).mock.calls).toEqual([
      ["apputils:change-theme", { theme: LITE_THEMES.dark }],
      ["apputils:change-theme", { theme: LITE_THEMES.light }],
    ]);
  });
});

describe("runCells", () => {
  function notebookApp(outputsFor: (source: string) => unknown[]) {
    const sources: string[] = [];
    const widgets: Array<{ model: { outputs: { toJSON(): unknown[] } } }> = [];
    const panel = {
      content: {
        activeCellIndex: 0,
        model: {
          sharedModel: {
            cells: sources,
            insertCell: (index: number, cell: { source: string }) => {
              sources.splice(index, 0, cell.source);
              widgets.splice(index, 0, {
                model: { outputs: { toJSON: () => outputsFor(cell.source) } },
              });
            },
          },
        },
        widgets,
      },
    };
    const execute = vi.fn(async () => true);
    const lite = {
      commands: { execute },
      shell: {
        widgets: () => [panel],
        currentWidget: panel,
        currentChanged: { connect: vi.fn(), disconnect: vi.fn() },
      },
    } as unknown as LiteApp;
    return { lite, sources, execute, panel };
  }

  it("appends the cells, runs each in turn and reports what they printed and showed", async () => {
    const { lite, sources, execute } = notebookApp((source) =>
      source === "print(1)"
        ? [{ output_type: "stream", text: ["1\n"] }]
        : [
            { output_type: "execute_result", data: { "text/plain": "   n\n0  1" } },
            { output_type: "display_data", data: { "image/png": "x", "text/plain": "<Figure>" } },
          ],
    );
    const report = await runCells(lite, ["print(1)", "df"], true);
    expect(sources).toEqual(["print(1)", "df"]);
    expect(execute).toHaveBeenCalledTimes(2);
    expect(execute).toHaveBeenCalledWith("notebook:run-cell");
    expect(report).toEqual({ stdout: "1\n", value: "<Figure>\n(1 figure(s) drawn)", error: null });
  });

  it("stops at the first error and reports it", async () => {
    const { lite, execute } = notebookApp((source) =>
      source === "boom"
        ? [{ output_type: "error", ename: "NameError", evalue: "name 'boom' is not defined" }]
        : [],
    );
    const report = await runCells(lite, ["boom", "never"], true);
    expect(execute).toHaveBeenCalledTimes(1);
    expect(report.error).toBe("NameError: name 'boom' is not defined");
  });

  it("says no notebook is open once its wait is over", async () => {
    const lite = {
      commands: { execute: vi.fn() },
      shell: {
        widgets: () => [],
        currentWidget: null,
        currentChanged: { connect: vi.fn(), disconnect: vi.fn() },
      },
    } as unknown as LiteApp;
    const report = await runCells(lite, ["1"], true, async () => {}, 0);
    expect(report.error).toBe("No notebook is open.");
  });

  it("hands the kernel the user before the first cell runs", async () => {
    const order: string[] = [];
    const { lite, execute } = notebookApp(() => []);
    execute.mockImplementation(async () => {
      order.push("run");
      return true;
    });
    await runCells(lite, ["1"], true, async () => {
      order.push("hand-off");
    });
    expect(order).toEqual(["hand-off", "run"]);
  });

  it("only adds the cells when it is not asked to run them", async () => {
    const { lite, sources, execute } = notebookApp(() => []);
    await runCells(lite, ["a = 1"], false);
    expect(sources).toEqual(["a = 1"]);
    expect(execute).not.toHaveBeenCalled();
  });

  it("builds matplotlib's font cache silently before cells that draw, and not otherwise", async () => {
    const { lite, panel, execute } = notebookApp(() => []);
    const order: string[] = [];
    execute.mockImplementation(async () => {
      order.push("run-cell");
      return true;
    });
    const requestExecute = vi.fn(() => {
      order.push("font-cache");
      return { done: Promise.resolve() };
    });
    Object.assign(panel, {
      sessionContext: { ready: Promise.resolve(), session: { kernel: { requestExecute } } },
    });
    await runCells(lite, ["import matplotlib.pyplot as plt", "plt.show()"], true);
    expect(requestExecute).toHaveBeenCalledTimes(1);
    expect(requestExecute).toHaveBeenCalledWith({
      code: FONT_CACHE_CODE,
      silent: true,
      store_history: false,
    });
    expect(order).toEqual(["font-cache", "run-cell", "run-cell"]);
    requestExecute.mockClear();
    await runCells(lite, ["print(1)"], true);
    await runCells(lite, ["import matplotlib"], false);
    expect(requestExecute).not.toHaveBeenCalled();
  });
});
