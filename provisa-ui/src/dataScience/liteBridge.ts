// Copyright (c) 2026 Kenneth Stott
// Canary: ce24ef7a-c975-4981-b7b8-4ae439cef534
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.

// REQ-2071: the Jupyter Notebook tab's kernels read Provisa as the page's user. A notebook kernel
// is a Web Worker the page cannot reach, so the page reaches it through the embedded JupyterLite
// app (same origin, exposed by its exposeAppInBrowser setting): each running kernel is handed the
// signed-in user as silent code setting `provisaAuth`, again whenever that user's identity
// changes, and a new, empty notebook opens on the starter cell.
import type { ProvisaAuth } from "./protocol";
import { clipped, type RunReport } from "./runReports";

/** The first cell of every new notebook: load the client and say who it reads as. The same cell
 * runs unchanged in a desktop Jupyter once the notebook is downloaded. */
export const NOTEBOOK_STARTER =
  "%pip install provisa-client\n%load_ext provisa_client\nimport provisa_client\n\nprovisa_client.sign_in()";

interface ExecuteContent {
  code: string;
  silent?: boolean;
  store_history?: boolean;
}

/** A notebook's or console's own connection to its kernel. */
export interface KernelConnection {
  requestExecute(content: ExecuteContent, ...rest: unknown[]): unknown;
  statusChanged: { connect(slot: (sender: unknown, status: string) => void): void };
}

interface SharedCell {
  getSource(): string;
  setSource(source: string): void;
}

/** The parts of the JupyterLite app the page uses. */
export interface LiteApp {
  /** Resolves once every plugin is active and the layout restored: before it, a plugin's command
   * (the theme manager's apputils:change-theme) may not be registered yet. */
  restored: Promise<void>;
  commands: { execute(id: string, args?: Record<string, unknown>): Promise<unknown> };
  shell: {
    widgets(area: "main"): Iterable<unknown>;
    currentWidget: unknown;
    currentChanged: { connect(slot: () => void): void; disconnect(slot: () => void): void };
  };
}

/** Python that sets the kernel's `provisaAuth` (provisa_client/browser.py reads it). */
export function bridgeCode(auth: ProvisaAuth): string {
  const json = JSON.stringify(auth);
  return `import js as _provisa_js\n_provisa_js.self.provisaAuth = _provisa_js.JSON.parse(${JSON.stringify(json)})\ndel _provisa_js`;
}

const RESTARTED = new Set(["starting", "restarting", "autorestarting"]);

/** Hand every open notebook's and console's kernel the current identity: before the first code
 * it runs (the hand-off rides the same connection, so it runs first), again whenever the
 * identity changes (read every `everyMs`), and again after a restart. Returns the stop. */
/** A running bridge: `handOff` guards and hands every open kernel the identity now (cells about
 * to run call it first); `stop` ends it. */
export interface LiteBridge {
  handOff(): Promise<void>;
  stop(): void;
}

export function startLiteBridge(
  app: () => LiteApp | undefined,
  identity: () => Promise<ProvisaAuth>,
  everyMs = 2000,
): LiteBridge {
  let code: string | null = null;
  const handed = new WeakMap<KernelConnection, string>();
  const direct = new WeakMap<KernelConnection, KernelConnection["requestExecute"]>();
  let stopped = false;

  const hand = (kernel: KernelConnection) => {
    const send = direct.get(kernel);
    if (code === null || send === undefined || handed.get(kernel) === code) return;
    handed.set(kernel, code);
    send({ code, silent: true, store_history: false });
  };
  const guard = (kernel: KernelConnection) => {
    if (direct.has(kernel)) return;
    const send = kernel.requestExecute.bind(kernel);
    direct.set(kernel, send);
    kernel.requestExecute = (content, ...rest) => {
      if (!content.silent) hand(kernel);
      return send(content, ...rest);
    };
    kernel.statusChanged.connect((_sender, status) => {
      if (RESTARTED.has(status)) handed.delete(kernel);
    });
  };
  const tick = async () => {
    const lite = app();
    if (lite === undefined || stopped) return;
    code = bridgeCode(await identity());
    for (const widget of lite.shell.widgets("main")) {
      const kernel = (widget as { sessionContext?: { session?: { kernel?: KernelConnection } } })
        ?.sessionContext?.session?.kernel;
      if (kernel === undefined || kernel === null) continue;
      guard(kernel);
      hand(kernel);
    }
  };
  void tick();
  const timer = setInterval(() => void tick(), everyMs);
  return {
    handOff: tick,
    stop: () => {
      stopped = true;
      clearInterval(timer);
    },
  };
}

/** JupyterLab's themes for the app's colour schemes. */
export const LITE_THEMES = { light: "JupyterLab Light", dark: "JupyterLab Dark" } as const;

/** JupyterLab's splash screen, in the embedded app's document while it starts or loads a theme. */
export const LITE_SPLASH_ID = "jupyterlab-splash";

/** Resolves once `doc` holds no splash screen. JupyterLab removes its startup splash 200 ms after
 * the app is restored; a theme loaded meanwhile shows the same splash again and schedules a second
 * removal of it, which throws (removeChild of a node no longer a child). */
function splashGone(doc: Document): Promise<void> {
  return new Promise((resolve) => {
    if (doc.getElementById(LITE_SPLASH_ID) === null) return resolve();
    const watch = new MutationObserver(() => {
      if (doc.getElementById(LITE_SPLASH_ID) !== null) return;
      watch.disconnect();
      resolve();
    });
    watch.observe(doc.body, { childList: true });
  });
}

/** The embedded app in the app's colour scheme (JupyterLab's own theme command), once the app is
 * restored -- the page has the app as soon as its frame exposes it, before its plugins are active
 * -- and its startup splash is gone from `doc`, the app's document. */
export async function followScheme(
  app: LiteApp,
  scheme: keyof typeof LITE_THEMES,
  doc: Document,
): Promise<unknown> {
  await app.restored;
  await splashGone(doc);
  return app.commands.execute("apputils:change-theme", { theme: LITE_THEMES[scheme] });
}

/** The cells of the notebook a widget shows, when it shows one. */
function notebookCells(widget: unknown): SharedCell[] | null {
  const model = (widget as { content?: { model?: { sharedModel?: { cells?: SharedCell[] } } } })
    ?.content?.model?.sharedModel;
  return model?.cells ?? null;
}

/** A new notebook (one empty cell) opens on the starter cell; returns the stop. JupyterLab makes
 * a new notebook current before its document has loaded, so its cells are read once its
 * document context is ready. */
export function seedNewNotebooks(app: LiteApp): () => void {
  const seed = async () => {
    const widget = app.shell.currentWidget;
    await (widget as { context?: { ready?: Promise<void> } } | null)?.context?.ready;
    const cells = notebookCells(widget);
    if (cells !== null && cells.length === 1 && cells[0].getSource() === "") {
      cells[0].setSource(NOTEBOOK_STARTER);
    }
  };
  const onChange = () => void seed();
  app.shell.currentChanged.connect(onChange);
  onChange();
  return () => app.shell.currentChanged.disconnect(onChange);
}

/** An nbformat output, as a cell's output model gives it. */
interface CellOutput {
  output_type: string;
  text?: string | string[];
  data?: Record<string, string | string[]>;
  ename?: string;
  evalue?: string;
}

interface NotebookPanel {
  sessionContext?: {
    ready: Promise<void>;
    session?: { kernel?: { requestExecute(content: ExecuteContent): { done: Promise<unknown> } } };
  };
  content: {
    model: {
      sharedModel: {
        cells: unknown[];
        insertCell(index: number, cell: { cell_type: "code"; source: string }): void;
      };
    };
    activeCellIndex: number;
    widgets: ReadonlyArray<{ model: { outputs: { toJSON(): CellOutput[] } }; node?: HTMLElement }>;
  };
}

const joined = (text: string | string[] | undefined) =>
  Array.isArray(text) ? text.join("") : (text ?? "");

/** How long cells handed over wait for the embedded app to open a notebook. */
const NOTEBOOK_WAIT_MS = 60_000;

/** Matplotlib's first import in a kernel builds its font cache and says so on stderr, above the
 * first figure drawn. Built silently before cells that draw, it says nothing in them. */
export const FONT_CACHE_CODE = "import matplotlib.font_manager";
const DRAWS = /\bmatplotlib\b/;

/** The notebook the user is looking at, or else the first one open. */
function openNotebook(app: LiteApp): NotebookPanel | null {
  const isNotebook = (w: unknown): w is NotebookPanel =>
    Array.isArray((w as NotebookPanel)?.content?.model?.sharedModel?.cells);
  if (isNotebook(app.shell.currentWidget)) return app.shell.currentWidget;
  for (const widget of app.shell.widgets("main")) if (isNotebook(widget)) return widget;
  return null;
}

/** Append `cells` to the open notebook and, with `run`, run them in turn as the user would (so
 * the kernel is handed the user first), stopping at the first error; the report is what they
 * printed, the last value they showed and the error. */
export async function runCells(
  app: LiteApp,
  cells: string[],
  run: boolean,
  handOff: () => Promise<void> = async () => {},
  waitMs = NOTEBOOK_WAIT_MS,
): Promise<RunReport> {
  // Cells may arrive as the app starts, before its notebook has opened.
  const started = Date.now();
  let notebook = openNotebook(app);
  while (notebook === null && Date.now() - started < waitMs) {
    await new Promise((resolve) => setTimeout(resolve, 250));
    notebook = openNotebook(app);
  }
  if (notebook === null) return { stdout: "", value: "", error: "No notebook is open." };
  await notebook.sessionContext?.ready;
  // The notebook's kernel now exists: it is handed the user before the first cell runs.
  if (run) await handOff();
  const kernel = notebook.sessionContext?.session?.kernel;
  if (run && kernel && cells.some((cell) => DRAWS.test(cell))) {
    await kernel.requestExecute({ code: FONT_CACHE_CODE, silent: true, store_history: false }).done;
  }
  const shared = notebook.content.model.sharedModel;
  let stdout = "";
  let value = "";
  let figures = 0;
  for (const source of cells) {
    const index = shared.cells.length;
    shared.insertCell(index, { cell_type: "code", source });
    if (!run) continue;
    notebook.content.activeCellIndex = index;
    await app.commands.execute("notebook:run-cell");
    for (const output of notebook.content.widgets[index].model.outputs.toJSON()) {
      if (output.output_type === "stream") stdout += joined(output.text);
      if (output.output_type === "error") {
        return clipped({ stdout, value, error: `${output.ename}: ${output.evalue}` });
      }
      if (output.data !== undefined) {
        if (output.data["image/png"] !== undefined) figures += 1;
        const text = joined(output.data["text/plain"]);
        if (text !== "") value = text;
      }
    }
  }
  if (figures > 0) value += `${value ? "\n" : ""}(${figures} figure(s) drawn)`;
  return clipped({ stdout, value, error: null });
}

/** The open notebook's last cell's outputs as the notebook draws them (a figure, a table, what it
 * printed): what a tour step points at once its cells have run. Null while no notebook is open or
 * its last cell has drawn nothing. */
export function lastOutputArea(app: LiteApp): HTMLElement | null {
  const notebook = openNotebook(app);
  if (notebook === null) return null;
  const { widgets } = notebook.content;
  const area = widgets[widgets.length - 1]?.node?.querySelector<HTMLElement>(".jp-OutputArea");
  return area != null && area.childElementCount > 0 ? area : null;
}
