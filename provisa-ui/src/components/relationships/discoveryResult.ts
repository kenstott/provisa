// Copyright (c) 2026 Kenneth Stott
// Canary: 9d4b2f6e-71a3-4c05-b8e2-5f0a3c7d1e64
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-413: what a discovery run found, in plain words: one line per kind it ran, and when
// semantics found none, why; the requests split, the answers cut short and the requests that
// failed; then what was left out, counted by kind.

import type { TFunction } from "i18next";
import type { DiscoveryReport, RemovedKind, SemanticsReport, TypeGroup } from "../../api/admin";

/** The order the kinds left out are listed in. */
const REMOVED_ORDER: RemovedKind[] = [
  "same_column",
  "primary_key_to_primary_key",
  "both_repeating",
  "existing",
  "pending",
  "rejected",
  "matches_foreign_key",
  "out_of_scope",
];

export function typeGroupLabel(group: TypeGroup, t: TFunction): string {
  return t(`discoverDialog.group.${group}`);
}

function semanticsLines(s: SemanticsReport, t: TFunction): string[] {
  const found =
    s.suggested > 0
      ? t("discoverResult.semanticsFound", { count: s.suggested })
      : s.empty_reason === "unreadable"
        ? t("discoverResult.semanticsUnreadable")
        : s.empty_reason === "none_proposed"
          ? t("discoverResult.semanticsNoneProposed")
          : s.empty_reason === "below_threshold"
            ? t("discoverResult.semanticsBelowThreshold", {
                threshold: Math.round(s.threshold * 100),
              })
            : t("discoverResult.semanticsFiltered");
  return [
    found,
    ...(s.split > 0 ? [t("discoverResult.split", { count: s.split })] : []),
    ...(s.cut_short > 0 ? [t("discoverResult.cutShort", { count: s.cut_short })] : []),
    ...s.failed.map((f) =>
      t("discoverResult.failedRequest", { group: typeGroupLabel(f.group, t), error: f.error }),
    ),
  ];
}

function leftOut(report: DiscoveryReport, t: TFunction): string | null {
  const item = (label: string, count: number) => t("discoverResult.leftOutItem", { label, count });
  const s = report.semantics;
  const parts = [
    ...(s && s.below_threshold > 0
      ? [
          item(
            t("discoverResult.removed.below_threshold", {
              threshold: Math.round(s.threshold * 100),
            }),
            s.below_threshold,
          ),
        ]
      : []),
    ...(s && s.invalid > 0 ? [item(t("discoverResult.removed.invalid"), s.invalid)] : []),
    ...REMOVED_ORDER.flatMap((kind) => {
      const count = report.removed[kind] ?? 0;
      return count > 0 ? [item(t(`discoverResult.removed.${kind}`), count)] : [];
    }),
  ];
  return parts.length === 0 ? null : t("discoverResult.leftOut", { list: parts.join(", ") });
}

export function discoveryResultLines(report: DiscoveryReport, t: TFunction): string[] {
  const lines: string[] = [];
  if (report.foreign_keys) {
    lines.push(
      t("discoverResult.foreignKeys", {
        found: report.foreign_keys.found,
        count: report.foreign_keys.suggested,
      }),
    );
  }
  if (report.semantics) lines.push(...semanticsLines(report.semantics, t));
  const out = leftOut(report, t);
  if (out) lines.push(out);
  return lines;
}
