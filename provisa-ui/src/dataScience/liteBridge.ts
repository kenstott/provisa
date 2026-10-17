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
export function startLiteBridge(
  app: () => LiteApp | undefined,
  identity: () => Promise<ProvisaAuth>,
  everyMs = 2000,
): () => void {
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
  return () => {
    stopped = true;
    clearInterval(timer);
  };
}

/** JupyterLab's themes for the app's colour schemes. */
export const LITE_THEMES = { light: "JupyterLab Light", dark: "JupyterLab Dark" } as const;

/** The embedded app in the app's colour scheme (JupyterLab's own theme command). */
export function followScheme(app: LiteApp, scheme: keyof typeof LITE_THEMES): Promise<unknown> {
  return app.commands.execute("apputils:change-theme", { theme: LITE_THEMES[scheme] });
}

/** The cells of the notebook a widget shows, when it shows one. */
function notebookCells(widget: unknown): SharedCell[] | null {
  const model = (widget as { content?: { model?: { sharedModel?: { cells?: SharedCell[] } } } })
    ?.content?.model?.sharedModel;
  return model?.cells ?? null;
}

/** A new notebook (one empty cell) opens on the starter cell; returns the stop. */
export function seedNewNotebooks(app: LiteApp): () => void {
  const seed = () => {
    const cells = notebookCells(app.shell.currentWidget);
    if (cells !== null && cells.length === 1 && cells[0].getSource() === "") {
      cells[0].setSource(NOTEBOOK_STARTER);
    }
  };
  app.shell.currentChanged.connect(seed);
  seed();
  return () => app.shell.currentChanged.disconnect(seed);
}
