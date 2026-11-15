// Copyright (c) 2026 Kenneth Stott
// Canary: 9d4b2f6e-71a3-4c05-b8e2-5f0a3c7d1e64
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-413: what a discovery run found, in plain words: one line per kind it ran.

import type { TFunction } from "i18next";
import type { DiscoveryResult, SemanticsReport } from "../../api/admin";

function semanticsLine(s: SemanticsReport, t: TFunction): string {
  if (s.empty_reason === "cut_short") return t("discoverResult.semanticsCutShort");
  const line = semanticsFound(s, t);
  // An answer cut off at the model's length limit says so, whatever was kept from it.
  return s.cut_short ? `${line} ${t("discoverResult.cutShortNote")}` : line;
}

function semanticsFound(s: SemanticsReport, t: TFunction): string {
  if (s.suggested > 0) return t("discoverResult.semanticsFound", { count: s.suggested });
  if (s.empty_reason === "unreadable") return t("discoverResult.semanticsUnreadable");
  if (s.empty_reason === "none_proposed") return t("discoverResult.semanticsNoneProposed");
  const why = [
    s.below_threshold > 0 &&
      t("discoverResult.belowThreshold", {
        count: s.below_threshold,
        threshold: Math.round(s.threshold * 100),
      }),
    s.already_known > 0 && t("discoverResult.alreadyKnown", { count: s.already_known }),
    s.invalid > 0 && t("discoverResult.invalid", { count: s.invalid }),
  ].filter((part): part is string => typeof part === "string");
  return t("discoverResult.semanticsFiltered", { count: s.proposed, why: why.join("; ") });
}

export function discoveryResultLines(result: DiscoveryResult, t: TFunction): string[] {
  const lines: string[] = [];
  if (result.foreign_keys) {
    lines.push(t("discoverResult.foreignKeys", { count: result.foreign_keys.found }));
  }
  if (result.semantics) lines.push(semanticsLine(result.semantics, t));
  return lines;
}
