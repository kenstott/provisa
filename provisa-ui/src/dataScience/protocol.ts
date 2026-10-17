// Copyright (c) 2026 Kenneth Stott
// Canary: 2acc2516-9a77-40c4-961e-b2b2a9844cfc
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.

// REQ-2071: the messages between the Data Science area and its Python worker.

/** The page's signed-in user as provisa_client reads it (provisa_client/browser.py). */
export interface ProvisaAuth {
  baseUrl: string;
  role: string | null;
  bearer: string | null;
  org: string | null;
  env: string | null;
}

/** The files the provisa-notebook package serves at /pyodide (its manifest.json). */
export interface PyodideManifest {
  pyodide: string;
  packages: string[];
  client_wheel: string;
}

export type ToWorker =
  | { type: "init"; auth: ProvisaAuth }
  | { type: "auth"; auth: ProvisaAuth }
  | { type: "run"; id: number; code: string };

/** A value a run ended on, as the page shows it. */
export type RunValue =
  | { kind: "none" }
  | { kind: "text"; text: string }
  | { kind: "table"; columns: string[]; rows: unknown[][]; total: number };

export type FromWorker =
  | { type: "ready" }
  | { type: "failed"; error: string }
  | {
      type: "result";
      id: number;
      stdout: string;
      value: RunValue;
      images: string[];
      error: string | null;
    };
