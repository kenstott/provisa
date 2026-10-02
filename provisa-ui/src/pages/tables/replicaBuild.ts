// Copyright (c) 2026 Kenneth Stott
// Canary: f9686907-0444-45f7-b418-f8c91c279f5b
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1915: what the table form says about a table's replica build, as one line.

import type { ReplicaBuild } from "../../api/admin";

type Translate = (key: string, options?: Record<string, unknown>) => string;

/** How a build copies, in words: "streamed in batches, row copy". */
function how(build: ReplicaBuild, t: Translate): string {
  const parts: string[] = [];
  if (build.method) parts.push(t(`replicaBuild.method.${build.method}`));
  if (build.loadKind) parts.push(t(`replicaBuild.loadKind.${build.loadKind}`));
  return parts.join(", ");
}

/** The line for one replica's build; `build` is undefined for a table with no replica record. */
export function replicaBuildLine(
  build: ReplicaBuild | undefined,
  t: Translate,
  formatNumber: (n: number) => string,
  formatWhen: (iso: string) => string,
): string {
  if (!build) return t("replicaBuild.none");
  const rows = formatNumber(build.rowsCopied ?? 0);
  switch (build.state) {
    case "retired":
      return t("replicaBuild.retired");
    case "failed":
      return t("replicaBuild.failed", { error: build.lastError ?? "" });
    case "requested":
      return build.waitingOn
        ? t("replicaBuild.waiting", { waitingOn: build.waitingOn })
        : t("replicaBuild.requested");
    case "building":
      return build.rowsPerSecond !== null
        ? t("replicaBuild.building", {
            rows,
            rate: formatNumber(Math.round(build.rowsPerSecond)),
            how: how(build, t),
          })
        : t("replicaBuild.buildingNoRate", { how: how(build, t) });
    default:
      return build.completedAt
        ? t("replicaBuild.built", {
            when: formatWhen(build.completedAt),
            rows,
            how: how(build, t),
          })
        : t("replicaBuild.none");
  }
}
