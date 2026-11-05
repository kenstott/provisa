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

/** The build of one table among those the server reported; undefined when it has no replica
 * record, or builds could not be read. */
export function buildOf(
  builds: ReplicaBuild[] | undefined,
  table: { sourceId: string; schemaName: string; tableName: string },
): ReplicaBuild | undefined {
  return builds?.find(
    (b) =>
      b.sourceId === table.sourceId &&
      b.schemaName === table.schemaName &&
      b.tableName === table.tableName,
  );
}

/** The most a running build is shown as: it is not complete until it says so. */
export const MOST_SHOWN_PERCENT = 95;

/** How far a running build has got, for the mark that shows it. THE one place this is worked
 * out: a build that comes to know a total of another kind (mailboxes read of mailboxes listed,
 * files read of files matched) is given its measure here, and the mark draws it unchanged. */
export interface ReplicaProgress {
  /** How full the ring is, 0 to MOST_SHOWN_PERCENT; null when there is nothing to measure the
   * build against, and the ring turns without a measure. Never made up. */
  percent: number | null;
  /** Whether the measure is a total the build knows, or an estimate of one. */
  exact: boolean;
  /** What the mark says of it: the wording's key under `replicaBuild`, and the numbers in it
   * (each is formatted for the reader's language where it is shown). */
  said: { key: string; numbers: Record<string, number>; words?: Record<string, string> };
}

/** How far `build`, which is running, has got. Today the one measure is the rows of the last
 * build that copied the whole table: an estimate, since the table may have grown or shrunk. */
export function replicaProgress(build: ReplicaBuild): ReplicaProgress {
  const measured = measuredProgress(build);
  // Its source's service asked the read to wait: the ring keeps what it had measured, and
  // says that -- how far a build has got matters less than why it is standing still.
  if (build.wait) {
    return {
      ...measured,
      said: {
        key: "progressWaiting",
        numbers: { seconds: Math.round(build.wait.seconds) },
        words: { by: build.wait.by },
      },
    };
  }
  return measured;
}

/** What a build that lands a part at a time counts as it reads them to their end: a mail
 * source's mailboxes, a Logs source's files (REQ-1996). */
const PARTS_SAID: Record<string, { progress: string; partial: string; parts: string }> = {
  mailboxes: {
    progress: "progressMailboxes",
    partial: "buildingPartial",
    parts: "buildingParts",
  },
  files: {
    progress: "progressFiles",
    partial: "buildingPartialFiles",
    parts: "buildingPartsFiles",
  },
};

function partsSaid(unit: string) {
  const said = PARTS_SAID[unit];
  if (!said) throw new Error(`a build reports parts of an unknown kind: ${unit}`);
  return said;
}

function measuredProgress(build: ReplicaBuild): ReplicaProgress {
  // A build that lands a mailbox (or a file) at a time knows its total: the parts it reads.
  // That is a count, not an estimate -- and still never shown as complete while the build runs.
  const parts = build.progress;
  if (parts && parts.total > 0) {
    return {
      // Until its first mailbox is read to its end the ring turns: a still, empty ring would
      // look stuck for the whole first read of a one-mailbox source. It says the count.
      percent:
        parts.done > 0
          ? Math.min(MOST_SHOWN_PERCENT, Math.floor((parts.done / parts.total) * 100))
          : null,
      exact: true,
      said: {
        key: partsSaid(parts.unit).progress,
        numbers: { done: parts.done, total: parts.total },
      },
    };
  }
  const rows = build.rowsCopied ?? 0;
  const total = build.completedRows ?? 0;
  // A delta applies changes only: its rows are not a share of the table's.
  if (build.method === "delta" || total <= 0 || rows <= 0) {
    return {
      percent: null,
      exact: false,
      said:
        rows > 0 ? { key: "progressRows", numbers: { rows } } : { key: "replicating", numbers: {} },
    };
  }
  const percent = Math.min(MOST_SHOWN_PERCENT, Math.floor((rows / total) * 100));
  return {
    percent,
    exact: false,
    said:
      rows > total
        ? { key: "progressPast", numbers: { rows, total } }
        : { key: "progressAbout", numbers: { rows, total } },
  };
}

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
      // Its source's service asked the read to wait: say so, in place of a rate that stands
      // still.
      if (build.wait) {
        return t("replicaBuild.buildingWaiting", {
          seconds: formatNumber(Math.round(build.wait.seconds)),
          by: build.wait.by,
          how: how(build, t),
        });
      }
      // A replica written a part at a time is read meanwhile: it says how many of its source's
      // parts (a mail source's mailboxes, a Logs source's files) have been read to their end,
      // not a rate of rows.
      if (build.progress) {
        const said = partsSaid(build.progress.unit);
        return t(`replicaBuild.${build.partial ? said.partial : said.parts}`, {
          done: formatNumber(build.progress.done),
          total: formatNumber(build.progress.total),
          how: how(build, t),
        });
      }
      return build.rowsPerSecond !== null
        ? t("replicaBuild.building", {
            rows,
            rate: formatNumber(Math.round(build.rowsPerSecond)),
            how: how(build, t),
          })
        : t("replicaBuild.buildingNoRate", { how: how(build, t) });
    default:
      if (!build.completedAt) return t("replicaBuild.none");
      // REQ-855: a copy its probe has since found unchanged says when it was last checked.
      return build.verifiedAt
        ? t("replicaBuild.builtChecked", {
            when: formatWhen(build.completedAt),
            checked: formatWhen(build.verifiedAt),
            rows,
            how: how(build, t),
          })
        : t("replicaBuild.built", {
            when: formatWhen(build.completedAt),
            rows,
            how: how(build, t),
          });
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
/** REQ-2015: the replica's indexes -- how many of those its landing asked for are built, and a
 * line for each that failed, its cause worded from its code. Empty when it asked for none. */
export function replicaIndexLines(
  build: ReplicaBuild | undefined,
  t: Translate,
): { summary: string | null; failed: string[] } {
  const indexes = build ? build.indexes : [];
  if (indexes.length === 0) return { summary: null, failed: [] };
  const built = indexes.filter((i) => i.state === "built").length;
  const pending = indexes.filter((i) => i.state === "pending").length;
  const summary = t(
    pending > 0 ? "replicaBuild.index.summaryPending" : "replicaBuild.index.summary",
    {
      built,
      total: indexes.length,
      pending,
    },
  );
  const failed = indexes
    .filter((i) => i.state === "failed")
    .map((i) =>
      i.errorCode === "replica.index_duplicate_key"
        ? t("replicaBuild.index.duplicateKey", { columns: i.columns.join(", ") })
        : t("replicaBuild.index.failed", {
            columns: i.columns.join(", "),
            error: i.error ?? "",
          }),
    );
  return { summary, failed };
}

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
    if (code === "replication.mailboxes_at_quota") {
      const named = ((params.accounts as string[] | undefined) ?? []).join(", ");
      const more = Number(params.more ?? 0);
      return t("replicaBuild.note.mailboxes_at_quota", {
        total: Number(params.count ?? 0),
        accounts: more > 0 ? t("replicaBuild.note.andMore", { ids: named, more }) : named,
      });
    }
    if (code === "replication.mailboxes_failed") {
      const named = ((params.accounts as string[] | undefined) ?? []).join(", ");
      const more = Number(params.more ?? 0);
      return t("replicaBuild.note.mailboxes_failed", {
        total: Number(params.count ?? 0),
        accounts: more > 0 ? t("replicaBuild.note.andMore", { ids: named, more }) : named,
      });
    }
    if (code === "replication.place_not_kept") {
      return t("replicaBuild.note.place_not_kept", { reason: String(params.reason ?? "") });
    }
    if (code === "replication.indexes_not_planned") {
      return t("replicaBuild.note.indexes_not_planned", { reason: String(params.reason ?? "") });
    }
    // REQ-2171: a subtyped column left out of the copy, which a read of it is refused for.
    if (code === "replication.column_not_landed") {
      return t("replicaBuild.note.column_not_landed", {
        column: String(params.column ?? ""),
        subtype: String(params.subtype ?? ""),
        reader: String(params.reader ?? ""),
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
