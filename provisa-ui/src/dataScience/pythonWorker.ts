// Copyright (c) 2026 Kenneth Stott
// Canary: a8a02955-6681-4b44-8272-4e4206b2ffbf
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.

// REQ-2071: the Data Science area's Python, off the page's main thread. Pyodide and its packages
// come from /pyodide (the provisa-notebook package); provisa_client reads as the page's user from
// self.provisaAuth, which the page keeps current.
import type { FromWorker, ProvisaAuth, PyodideManifest, ToWorker } from "./protocol";

// Pyodide reports each package it loads; that is not the code's output, so it is not shown.
interface LoadOptions {
  messageCallback: (message: string) => void;
}
const QUIET: LoadOptions = { messageCallback: () => {} };

interface Pyodide {
  loadPackage(names: string | string[], options?: LoadOptions): Promise<unknown>;
  loadPackagesFromImports(code: string, options?: LoadOptions): Promise<unknown>;
  runPythonAsync(code: string): Promise<unknown>;
  setStdout(options: { batched: (text: string) => void }): void;
  setStderr(options: { batched: (text: string) => void }): void;
  globals: { get(name: string): (...args: unknown[]) => unknown };
}

const scope = self as unknown as {
  provisaAuth?: ProvisaAuth;
  postMessage(message: FromWorker): void;
  onmessage: ((event: MessageEvent<ToWorker>) => void) | null;
};

// Runs once after the packages load: the agg backend (no display in a worker), and the helper that
// turns a run's last value and its figures into what the page shows.
const PRELUDE = `
import os, io, base64, json
os.environ["MPLBACKEND"] = "agg"

def _provisa_show(value):
    import sys
    figures = []
    if "matplotlib.pyplot" in sys.modules:
        import matplotlib.pyplot as plt
        for num in plt.get_fignums():
            buf = io.BytesIO()
            plt.figure(num).savefig(buf, format="png", bbox_inches="tight")
            figures.append(base64.b64encode(buf.getvalue()).decode())
        plt.close("all")
    if value is None:
        shown = {"kind": "none"}
    elif type(value).__name__ == "DataFrame" and hasattr(value, "to_dict"):
        head = value.head(200)
        shown = {
            "kind": "table",
            "columns": [str(c) for c in head.columns],
            "rows": json.loads(head.to_json(orient="values", date_format="iso")),
            "total": len(value),
        }
    else:
        shown = {"kind": "text", "text": repr(value)}
    return json.dumps({"value": shown, "images": figures})
`;

// A URL the server answers at run time, not a module of this build.
const PYODIDE_MODULE = "/pyodide/pyodide.mjs";

let pyodide: Pyodide | null = null;
let stdout = "";

async function init(manifest: PyodideManifest, auth: ProvisaAuth): Promise<void> {
  scope.provisaAuth = auth;
  // The Pyodide served beside this page: never a CDN.
  const { loadPyodide } = (await import(/* @vite-ignore */ PYODIDE_MODULE)) as {
    loadPyodide(options: { indexURL: string }): Promise<Pyodide>;
  };
  const py = await loadPyodide({ indexURL: "/pyodide/" });
  py.setStdout({ batched: (text) => (stdout += text + "\n") });
  py.setStderr({ batched: (text) => (stdout += text + "\n") });
  await py.loadPackage(["pandas", "pyarrow", `/pyodide/${manifest.client_wheel}`], QUIET);
  await py.runPythonAsync(PRELUDE);
  pyodide = py;
}

async function run(id: number, code: string): Promise<FromWorker> {
  const py = pyodide;
  if (py === null) throw new Error("Python is not loaded");
  stdout = "";
  try {
    // A package the code imports (scikit-learn, matplotlib) is loaded from /pyodide first.
    await py.loadPackagesFromImports(code, QUIET);
    const value = await py.runPythonAsync(code);
    const shown = JSON.parse(String(py.globals.get("_provisa_show")(value))) as {
      value: Extract<FromWorker, { type: "result" }>["value"];
      images: string[];
    };
    (value as { destroy?: () => void } | undefined)?.destroy?.();
    return { type: "result", id, stdout, value: shown.value, images: shown.images, error: null };
  } catch (err) {
    return {
      type: "result",
      id,
      stdout,
      value: { kind: "none" },
      images: [],
      error: String((err as Error)?.message ?? err),
    };
  }
}

scope.onmessage = (event) => {
  const message = event.data;
  if (message.type === "auth") {
    scope.provisaAuth = message.auth;
  } else if (message.type === "init") {
    init(message.manifest, message.auth).then(
      () => scope.postMessage({ type: "ready" }),
      (err: unknown) => scope.postMessage({ type: "failed", error: String(err) }),
    );
  } else {
    void run(message.id, message.code).then((result) => scope.postMessage(result));
  }
};
