// Copyright (c) 2026 Kenneth Stott
// Canary: 5e2c8a17-4b9d-4f63-a0e1-c37d6b2f9a48
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-413 (amended 2026-11-16): a discovery run's first line, the one its collapsible block in the
// dialog opens and closes by and the one the Relationships page shows. Going: how far it has got
// and its suggestions so far. Ended: how it ended and how many of its suggestions wait.

import type { TFunction } from "i18next";
import { isReading, type DiscoveryRun } from "../../api/admin";

/** How the run stands: how it ended, or how far it has got. */
export function runStatus(run: DiscoveryRun, t: TFunction): string {
  if (run.status === "done") return t("discoverDialog.done");
  if (run.status === "stopped") return t("discoverDialog.stopped");
  if (run.status === "failed") return t("discoverDialog.failed", { error: run.error ?? "" });
  if (run.stop_requested) return t("discoverDialog.stopping");
  if (isReading(run.report)) {
    return t("discoverDialog.reading", {
      done: run.report.tables_read,
      total: run.report.tables_total,
    });
  }
  if (run.requests_total > 0) {
    return t("discoverDialog.requests", { done: run.requests_done, total: run.requests_total });
  }
  return t("discoverDialog.starting");
}

/** How far a run going has got, from 0 to 1: the tables read while reading, then the requests. */
export function runFraction(run: DiscoveryRun): number {
  if (isReading(run.report)) {
    return run.report.tables_total > 0 ? run.report.tables_read / run.report.tables_total : 0;
  }
  return run.requests_total > 0 ? run.requests_done / run.requests_total : 0;
}

/** The run's first line. ``waiting``: how many of an ended run's suggestions wait for a
 *  decision, null where not counted. */
export function runLine(run: DiscoveryRun, waiting: number | null, t: TFunction): string {
  const status = runStatus(run, t);
  if (run.status === "running") {
    return `${status} · ${t("discoverDialog.suggestionsSoFar", { count: run.suggestions })}`;
  }
  return waiting !== null && waiting > 0
    ? `${status} ${t("discoverDialog.waiting", { count: waiting })}`
    : status;
}
