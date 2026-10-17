// Copyright (c) 2026 Kenneth Stott
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.

import type { FromWorker } from "./protocol";

// REQ-2071: what a run Polly asked for came to. Polly's navigate tool hands the Data Science area
// code to run under a run id (router state, as every explorer takes its payload); the area reports
// the run's outcome here under that id, and the tool, waiting on it, returns it to the model.

/** A run's outcome as the model reads it. */
export interface RunReport {
  stdout: string;
  /** The run's last value: text, or a table's columns, first rows and row count. */
  value: string;
  error: string | null;
}

type RunResult = Pick<
  Extract<FromWorker, { type: "result" }>,
  "stdout" | "value" | "images" | "error"
>;

/** How many rows of a table result the model is shown. */
const REPORT_ROWS = 20;
/** The longest printed output or value text the model is shown. */
const REPORT_CHARS = 8000;

const clip = (text: string) =>
  text.length > REPORT_CHARS ? `${text.slice(0, REPORT_CHARS)}… (${text.length} characters)` : text;

/** A run's outcome as the model reads it: a table as its columns, first rows and row count. */
export function summarise(output: RunResult): RunReport {
  const { value } = output;
  let shown = "";
  if (value.kind === "text") shown = value.text;
  if (value.kind === "table") {
    const rows = value.rows.slice(0, REPORT_ROWS).map((row) => JSON.stringify(row));
    shown = [
      `columns: ${JSON.stringify(value.columns)}`,
      ...rows,
      `(${value.total} rows; first ${rows.length} shown)`,
    ].join("\n");
  }
  if (output.images.length > 0)
    shown += `${shown ? "\n" : ""}(${output.images.length} figure(s) drawn)`;
  return { stdout: clip(output.stdout), value: clip(shown), error: output.error };
}

const waiting = new Map<string, (report: RunReport) => void>();

/** Called by the area when the run with this id has finished (or could not start). */
export function reportRun(runId: string, report: RunReport): void {
  const resolve = waiting.get(runId);
  waiting.delete(runId);
  resolve?.(report);
}

/** The report of the run with this id, or a timeout error naming how long it waited. */
export function awaitRun(runId: string, timeoutMs: number): Promise<RunReport> {
  return new Promise((resolve) => {
    const timer = setTimeout(() => {
      waiting.delete(runId);
      resolve({ stdout: "", value: "", error: `No result within ${timeoutMs / 1000} s.` });
    }, timeoutMs);
    waiting.set(runId, (report) => {
      clearTimeout(timer);
      resolve(report);
    });
  });
}
