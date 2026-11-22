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
// semantics found none, why (every request failing is said as that, never as the model proposing
// none); how many requests failed of those made, the requests split, the answers cut short and
// each failed request in the product's own words; then what is not among the suggestions, counted
// by kind. First of all, the scope the run ran with: its kinds, its model, and how many tables of
// which domains its source and target sets held, so a table outside them is plain.

import type { TFunction } from "i18next";
import type { DiscoveryReport, RemovedKind, SemanticsReport, TypeGroup } from "../../api/admin";
import { serverMessage } from "../../i18n/serverMessage";

/** The order the kinds left out are listed in. */
const REMOVED_ORDER: RemovedKind[] = ["same_column", "existing", "rejected", "out_of_scope"];

/** The compared type groups, as the server orders them (provisa/discovery/columns.py GROUPS).
 *  A date or timestamp is never a key candidate, so neither is a group (REQ-413). */
export const GROUP_ORDER: readonly TypeGroup[] = ["whole_number", "decimal", "text", "uuid"];

export function typeGroupLabel(group: TypeGroup, t: TFunction): string {
  return t(`discoverDialog.group.${group}`);
}

function emptyLine(s: SemanticsReport, t: TFunction): string {
  switch (s.empty_reason) {
    case "requests_failed":
      return t("discoverResult.semanticsRequestsFailed", { count: s.failed.length });
    case "unreadable":
      return t("discoverResult.semanticsUnreadable");
    case "none_proposed":
      return t("discoverResult.semanticsNoneProposed");
    case "below_threshold":
      return t("discoverResult.semanticsBelowThreshold", {
        threshold: Math.round(s.threshold * 100),
      });
    default:
      return t("discoverResult.semanticsFiltered");
  }
}

function semanticsLines(s: SemanticsReport, t: TFunction): string[] {
  const failed = s.failed.length;
  const found =
    s.suggested > 0 ? t("discoverResult.semanticsFound", { count: s.suggested }) : emptyLine(s, t);
  return [
    found,
    ...(failed > 0 && failed < s.requests_done
      ? [t("discoverResult.partial", { failed, count: s.requests_done })]
      : []),
    ...(s.split > 0 ? [t("discoverResult.split", { count: s.split })] : []),
    ...(s.cut_short > 0 ? [t("discoverResult.cutShort", { count: s.cut_short })] : []),
    // Jev's score is shown beside the model's and removes none (REQ-1789): its count is said.
    ...(s.scored_below_threshold !== undefined && s.scored_below_threshold > 0
      ? [
          t("discoverResult.jevBelow", {
            count: s.scored_below_threshold,
            threshold: Math.round(s.threshold * 100),
          }),
        ]
      : []),
    ...(s.scoring_failed !== undefined && s.scoring_failed > 0
      ? [t("discoverResult.scoringFailed", { count: s.scoring_failed })]
      : []),
    ...s.failed.map((f) =>
      t("discoverResult.failedRequest", {
        group: typeGroupLabel(f.group, t),
        message: serverMessage(f, f.message),
      }),
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
    ...(s && s.not_comparable !== undefined && s.not_comparable > 0
      ? [item(t("discoverResult.removed.not_comparable"), s.not_comparable)]
      : []),
    ...REMOVED_ORDER.flatMap((kind) => {
      const count = report.removed[kind] ?? 0;
      return count > 0 ? [item(t(`discoverResult.removed.${kind}`), count)] : [];
    }),
  ];
  return parts.length === 0 ? null : t("discoverResult.leftOut", { list: parts.join(", ") });
}

/** The scope the run ran with: the kinds, the model, and the tables of its source and target
 *  sets with their domains. A run recorded before a run said so gives only what its report holds. */
function scopeLines(report: DiscoveryReport, t: TFunction): string[] {
  const kinds = [
    ...(report.foreign_keys ? [t("discoverDialog.foreignKeys")] : []),
    ...(report.key_names ? [t("discoverDialog.keyNames")] : []),
    ...(report.semantics ? [t("discoverDialog.semantics")] : []),
    ...(report.key_values ? [t("discoverDialog.keyValues")] : []),
  ];
  const lines = [t("discoverResult.scopeKinds", { kinds: kinds.join(", ") })];
  // REQ-413: Key value overlap's cutoff and sample size, and what it measured and admitted.
  if (report.key_values && report.semantics) {
    lines.push(
      t("discoverResult.keyValues", {
        cutoff: Math.round(report.key_values.cutoff * 100),
        threshold: Math.round(report.semantics.threshold * 100),
        size: report.key_values.sample_size.toLocaleString(),
        measured: report.semantics.measured,
        admitted: report.semantics.admitted,
      }),
    );
  }
  if (report.model) {
    lines.push(
      t("discoverDialog.modelRan", { model: `${report.model.vendor}:${report.model.model}` }),
    );
  }
  const { source_domains: from, target_domains: to } = report;
  if (from === undefined || to === undefined) return lines;
  // A set of no table has no domain to name: its count alone.
  const set = (key: "Source" | "Target", count: number | undefined, domains: string[]) =>
    domains.length > 0
      ? t(`discoverResult.scope${key}`, { count, domains: domains.join(", ") })
      : t(`discoverResult.scope${key}Count`, { count });
  lines.push(
    set("Source", report.source_tables, from),
    report.target_same_as_source
      ? t("discoverResult.scopeTargetSame")
      : set("Target", report.target_tables, to),
  );
  if (report.source_left_out) {
    lines.push(t("discoverResult.sourceLeftOut", { count: report.source_left_out }));
  }
  if (report.target_left_out) {
    lines.push(t("discoverResult.targetLeftOut", { count: report.target_left_out }));
  }
  return lines;
}

export function discoveryResultLines(report: DiscoveryReport, t: TFunction): string[] {
  const lines: string[] = scopeLines(report, t);
  if (report.foreign_keys) {
    lines.push(
      t("discoverResult.foreignKeys", {
        found: report.foreign_keys.found,
        count: report.foreign_keys.suggested,
      }),
    );
  }
  if (report.key_names) {
    lines.push(
      t("discoverResult.keyNames", {
        found: report.key_names.found,
        count: report.key_names.suggested,
      }),
    );
  }
  if (report.semantics) lines.push(...semanticsLines(report.semantics, t));
  const out = leftOut(report, t);
  if (out) lines.push(out);
  return lines;
}
