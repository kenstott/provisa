// Copyright (c) 2026 Kenneth Stott
// Canary: d8b04392-fb16-45e6-9ffb-583060ba95ca
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2180 (amended 2026-11-11, QUALITY IS STATUS AND COUNTS, NOT A SCORE): a product's quality is
// never one weighted number. It is the worst outcome of the checks promised, how many passed,
// failed and have not run, the failing checks named, and when they last ran; per offer, of the
// checks the offer's contracts promise. The one sortable figure is the plain count passing out of
// those promised.

import { useTranslation } from "react-i18next";
import type { ProductDqEntry } from "../../api/lineage";
import type { Offer } from "../../api/dataProductOffers";

/** The worst outcome among the checks: one failing, else one not yet run, else all passing. */
export type Worst = "fail" | "notRun" | "pass";

export interface QualitySummary {
  /** Null when nothing is promised. */
  worst: Worst | null;
  total: number;
  passing: number;
  failing: number;
  notRun: number;
  /** The checks that did not pass, each by its column and name ("customer_id unique"). */
  failingNames: string[];
  /** The most recent of the checks' latest runs; null when none has run. */
  lastRun: string | null;
}

/** The summary of ``promises``: each checker's checks are its rules; a run checker's failing
 *  checks are those its latest run did not pass, the rest of its rules passing; a checker never
 *  run has every rule not yet run. */
export function qualitySummary(promises: ProductDqEntry[]): QualitySummary {
  let total = 0;
  let passing = 0;
  let failing = 0;
  let notRun = 0;
  const failingNames: string[] = [];
  let lastRun: string | null = null;
  for (const c of promises) {
    const checks = Math.max(c.rules.length, c.failing_checks.length);
    total += checks;
    if (c.latest_run_at === null) {
      notRun += checks;
      continue;
    }
    failing += c.failing_checks.length;
    passing += checks - c.failing_checks.length;
    failingNames.push(
      ...c.failing_checks.map((f) => [f.column, f.name].filter(Boolean).join(" ")),
    );
    if (lastRun === null || Date.parse(c.latest_run_at) > Date.parse(lastRun)) {
      lastRun = c.latest_run_at;
    }
  }
  const worst: Worst | null =
    total === 0 ? null : failing > 0 ? "fail" : notRun > 0 ? "notRun" : "pass";
  return { worst, total, passing, failing, notRun, failingNames, lastRun };
}

/** The product's promises an offer gives: those on the datasets its contracts cover. */
export function offerPromises(offer: Offer, promises: ProductDqEntry[]): ProductDqEntry[] {
  const covered = new Set(offer.contracts.map((c) => c.table_id));
  return promises.filter((c) => covered.has(c.checked_table_id));
}

const UNITS: [Intl.RelativeTimeFormatUnit, number][] = [
  ["day", 86_400_000],
  ["hour", 3_600_000],
  ["minute", 60_000],
];

/** How long ago ``iso`` was, in ``language``'s words ("2 hours ago"). */
export function since(iso: string, language: string, now: number = Date.now()): string {
  const elapsed = Date.parse(iso) - now;
  const format = new Intl.RelativeTimeFormat(language, { numeric: "auto" });
  for (const [unit, ms] of UNITS) {
    if (Math.abs(elapsed) >= ms) return format.format(Math.round(elapsed / ms), unit);
  }
  return format.format(0, "minute");
}

/** The summary in words: its counts, the failing checks named, and when they last ran. */
export function useQualityWords(): (q: QualitySummary) => string {
  const { t, i18n } = useTranslation();
  return (q) => {
    if (q.worst === null) return t("dataProductOffers.qualityNone");
    const parts = [t("dataProductOffers.qualityChecks", { count: q.total })];
    if (q.failing > 0) {
      parts.push(
        t("dataProductOffers.qualityFailing", {
          count: q.failing,
          checks: q.failingNames.join(", "),
        }),
      );
    }
    if (q.notRun > 0) parts.push(t("dataProductOffers.qualityNotRun", { count: q.notRun }));
    const counts = parts.join(", ");
    return q.lastRun
      ? `${counts} · ${t("dataProductOffers.qualityChecked", { when: since(q.lastRun, i18n.language) })}`
      : counts;
  };
}
