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
import type { ServerMessageShape } from "../../i18n/serverMessage";

type Translate = (key: string, options?: Record<string, unknown>) => string;
/** A server message in the reader's language: its code when the catalog has it, else its English text. */
type ServerText = (body: ServerMessageShape, fallback: string) => string;

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
  serverText: ServerText,
): string {
  if (!build) return t("replicaBuild.none");
  const rows = formatNumber(build.rowsCopied ?? 0);
  switch (build.state) {
    case "retired":
      return t("replicaBuild.retired");
    case "failed": {
      const attempts = formatNumber(build.failedAttempts);
      const error = serverText(
        { code: build.lastErrorCode, params: build.lastErrorParams, message: build.lastError },
        "",
      );
      // The wait before the next attempt grows with each failure in a row, so "it is tried
      // again" says when.
      return build.nextAttemptAt
        ? t("replicaBuild.failedNextAt", {
            attempts,
            when: formatWhen(build.nextAttemptAt),
            error,
          })
        : t("replicaBuild.failed", { attempts, error });
    }
    case "requested":
      return build.waitingOn
        ? t("replicaBuild.waiting", {
            waitingOn: serverText({ code: build.waitingOnCode, message: build.waitingOn }, ""),
          })
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

/** REQ-1861: the line for a change-feed table whose listener is down; null while it is
 * watching (or the table follows no change feed). */
export function replicaFeedLine(
  build: ReplicaBuild | undefined,
  t: Translate,
  formatWhen: (iso: string) => string,
): string | null {
  if (!build?.feedDownSince) return null;
  return t("replicaBuild.feedDown", {
    since: formatWhen(build.feedDownSince),
    error: build.feedError ?? "",
  });
}

/** REQ-874: how a delta table's replica was last refreshed, or null for a non-delta table (no
 * delta_skipped and not a delta build). A whole rebuild names its reason (`deltaSkipped`, a
 * delta.SKIP_* code); an applied delta shows the cursor it advanced to. */
/** What the last completed build had to say of the copy it made: a line for each note, none
 * when it had nothing to say. A note this build of the UI has no wording for is shown by its
 * code, never dropped. */
export function replicaNoteLines(build: ReplicaBuild | undefined, t: Translate): string[] {
  return (build?.buildNotes ?? []).map(({ code, params: given }) => {
    const params = given ?? {};
    if (code === "replication.unreadable_messages") {
      const named = ((params.ids as string[] | undefined) ?? []).join(", ");
      const more = Number(params.more ?? 0);
      return t("replicaBuild.note.unreadable_messages", {
        total: Number(params.count ?? 0),
        ids: more > 0 ? t("replicaBuild.note.andMore", { ids: named, more }) : named,
      });
    }
    if (code === "replication.mailboxes_left_out") {
      const named = ((params.accounts as string[] | undefined) ?? []).join(", ");
      const more = Number(params.more ?? 0);
      return t("replicaBuild.note.mailboxes_left_out", {
        total: Number(params.count ?? 0),
        accounts: more > 0 ? t("replicaBuild.note.andMore", { ids: named, more }) : named,
      });
    }
    return t("replicaBuild.note.other", { code });
  });
}

export function replicaDeltaLine(build: ReplicaBuild | undefined, t: Translate): string | null {
  if (!build) return null;
  if (build.deltaSkipped) {
    return t("replicaBuild.delta.wholeRebuild", {
      reason: t(`replicaBuild.delta.skip.${build.deltaSkipped}`),
    });
  }
  if (build.method === "delta") {
    return t("replicaBuild.delta.applied", {
      cursor:
        build.deltaCursor === null || build.deltaCursor === undefined
          ? ""
          : String(build.deltaCursor),
    });
  }
  return null;
}
