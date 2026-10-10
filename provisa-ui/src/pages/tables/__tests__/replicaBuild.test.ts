// Copyright (c) 2026 Kenneth Stott
// Canary: 2d623d24-026d-437b-bb4f-fb082a76b5ee
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1915: the table form's one line about a table's replica build.

import { describe, it, expect } from "vitest";
import type { ReplicaBuild } from "../../../api/admin";
import type { ServerMessageShape } from "../../../i18n/serverMessage";
import {
  replicaBuildLine,
  replicaDeltaLine,
  replicaFeedLine,
  replicaNoteLines,
} from "../replicaBuild";

const t = (key: string, options?: Record<string, unknown>) =>
  options ? `${key} ${JSON.stringify(options)}` : key;
const num = (n: number) => `#${n}`;
const when = (iso: string) => `@${iso}`;
// As serverMessage: the catalog's text for a code it has, else the server's English text.
const catalog: Record<string, string> = {
  "replication.spool_full": "spool full for {{table}}",
  "replication.waiting_engine_jobs": "engine at its cap",
};
const msg = (body: ServerMessageShape, fallback: string) =>
  body.code && catalog[body.code]
    ? `${catalog[body.code]} ${JSON.stringify(body.params ?? {})}`
    : (body.message ?? fallback);

function build(over: Partial<ReplicaBuild>): ReplicaBuild {
  return {
    sourceId: "src",
    schemaName: "public",
    tableName: "orders",
    region: null,
    state: "idle",
    requestedReason: null,
    method: null,
    loadKind: null,
    startedAt: null,
    rowsCopied: null,
    rowsPerSecond: null,
    completedAt: null,
    nextRefreshAt: null,
    lastError: null,
    lastErrorCode: null,
    lastErrorParams: null,
    failedAttempts: 0,
    nextAttemptAt: null,
    waitingOn: null,
    waitingOnCode: null,
    feedDownSince: null,
    feedError: null,
    deltaSkipped: null,
    deltaCursor: null,
    buildNotes: [],
    ...over,
  };
}

describe("replicaBuildLine", () => {
  it("says there is no replica for a table with no record or no completed build", () => {
    expect(replicaBuildLine(undefined, t, num, when, msg)).toBe("replicaBuild.none");
    expect(replicaBuildLine(build({}), t, num, when, msg)).toBe("replicaBuild.none");
  });

  it("shows a running build's rows, rate and how it copies", () => {
    const line = replicaBuildLine(
      build({
        state: "building",
        method: "stream_batches",
        loadKind: "row_copy",
        rowsCopied: 3800000,
        rowsPerSecond: 38000.4,
      }),
      t,
      num,
      when,
      msg,
    );
    expect(line).toBe(
      'replicaBuild.building {"rows":"#3800000","rate":"#38000","how":"replicaBuild.method.stream_batches, replicaBuild.loadKind.row_copy"}',
    );
  });

  it("shows a build that has not reported progress yet without a rate", () => {
    const line = replicaBuildLine(
      build({ state: "building", method: "engine_statement", loadKind: "bulk_stream" }),
      t,
      num,
      when,
      msg,
    );
    expect(line).toBe(
      'replicaBuild.buildingNoRate {"how":"replicaBuild.method.engine_statement, replicaBuild.loadKind.bulk_stream"}',
    );
  });

  it("says why a requested build is waiting, when it is", () => {
    expect(replicaBuildLine(build({ state: "requested" }), t, num, when, msg)).toBe(
      "replicaBuild.requested",
    );
    expect(
      replicaBuildLine(
        build({ state: "requested", waitingOn: "the engine is busy" }),
        t,
        num,
        when,
        msg,
      ),
    ).toBe('replicaBuild.waiting {"waitingOn":"the engine is busy"}');
  });

  it("says when a failed build is tried next", () => {
    expect(
      replicaBuildLine(
        build({
          state: "failed",
          failedAttempts: 3,
          lastError: "source down",
          nextAttemptAt: "2026-10-09T12:04:00+00:00",
        }),
        t,
        num,
        when,
        msg,
      ),
    ).toBe(
      `replicaBuild.failedNextAt {"attempts":"#3","when":"${when("2026-10-09T12:04:00+00:00")}","error":"source down"}`,
    );
  });

  it("shows a failed build's error and a retired replica", () => {
    expect(
      replicaBuildLine(build({ state: "failed", lastError: "source down" }), t, num, when, msg),
    ).toBe('replicaBuild.failed {"attempts":"#0","error":"source down"}');
    // A cause Provisa names is shown from the catalog, with how often the build has failed.
    expect(
      replicaBuildLine(
        build({
          state: "failed",
          failedAttempts: 4,
          lastError: "the replica build of src.orders was refused: ...",
          lastErrorCode: "replication.spool_full",
          lastErrorParams: { table: "src.orders" },
        }),
        t,
        num,
        when,
        msg,
      ),
    ).toBe(
      'replicaBuild.failed {"attempts":"#4","error":"spool full for {{table}} {\\"table\\":\\"src.orders\\"}"}',
    );
    expect(
      replicaBuildLine(
        build({
          state: "requested",
          waitingOn: "the engine is at its background job cap",
          waitingOnCode: "replication.waiting_engine_jobs",
        }),
        t,
        num,
        when,
        msg,
      ),
    ).toBe('replicaBuild.waiting {"waitingOn":"engine at its cap {}"}');
    expect(replicaBuildLine(build({ state: "retired" }), t, num, when, msg)).toBe(
      "replicaBuild.retired",
    );
  });

  it("shows when a replica was built and how many rows it has", () => {
    const line = replicaBuildLine(
      build({
        completedAt: "2026-10-02T12:00:00+00:00",
        rowsCopied: 500,
        method: "engine_statement",
        loadKind: "bulk_stream",
      }),
      t,
      num,
      when,
      msg,
    );
    expect(line).toBe(
      'replicaBuild.built {"when":"@2026-10-02T12:00:00+00:00","rows":"#500","how":"replicaBuild.method.engine_statement, replicaBuild.loadKind.bulk_stream"}',
    );
  });
});

describe("replicaFeedLine", () => {
  it("says nothing while the change feed is watching, or for a table with no record", () => {
    expect(replicaFeedLine(undefined, t, when)).toBeNull();
    expect(replicaFeedLine(build({}), t, when)).toBeNull();
  });

  it("says since when the change feed is down and the server's reason", () => {
    const line = replicaFeedLine(
      build({ feedDownSince: "2026-10-03T08:00:00+00:00", feedError: "connection refused" }),
      t,
      when,
    );
    expect(line).toBe(
      'replicaBuild.feedDown {"since":"@2026-10-03T08:00:00+00:00","error":"connection refused"}',
    );
  });
});

describe("replicaDeltaLine (REQ-874)", () => {
  it("is null for a table that is not a delta table", () => {
    expect(replicaDeltaLine(undefined, t)).toBeNull();
    expect(replicaDeltaLine(build({ method: "stream_batches" }), t)).toBeNull();
  });

  it("names the reason when a delta table whole-rebuilt instead of applying a delta", () => {
    expect(replicaDeltaLine(build({ deltaSkipped: "first_build" }), t)).toBe(
      'replicaBuild.delta.wholeRebuild {"reason":"replicaBuild.delta.skip.first_build"}',
    );
  });

  it("shows the advanced cursor when a delta was applied", () => {
    expect(replicaDeltaLine(build({ method: "delta", deltaCursor: 42 }), t)).toBe(
      'replicaBuild.delta.applied {"cursor":"42"}',
    );
  });

  it("renders an applied delta with no cursor without throwing", () => {
    expect(replicaDeltaLine(build({ method: "delta", deltaCursor: null }), t)).toBe(
      'replicaBuild.delta.applied {"cursor":""}',
    );
  });
});

describe("replicaNoteLines", () => {
  const UNREADABLE = "replication.unreadable_messages";

  it("says nothing for a build that had nothing to say", () => {
    expect(replicaNoteLines(undefined, t)).toEqual([]);
    expect(replicaNoteLines(build({}), t)).toEqual([]);
  });

  it("counts and names the messages kept without their text", () => {
    const noted = build({
      buildNotes: [{ code: UNREADABLE, params: { count: 2, ids: ["m1", "m9"], more: 0 } }],
    });
    expect(replicaNoteLines(noted, t)).toEqual([
      'replicaBuild.note.unreadable_messages {"total":2,"ids":"m1, m9"}',
    ]);
  });

  it("says how many more there are than it names", () => {
    const noted = build({
      buildNotes: [{ code: UNREADABLE, params: { count: 150, ids: ["m1", "m2"], more: 148 } }],
    });
    expect(replicaNoteLines(noted, t)).toEqual([
      'replicaBuild.note.unreadable_messages {"total":150,"ids":"replicaBuild.note.andMore {\\"ids\\":\\"m1, m2\\",\\"more\\":148}"}',
    ]);
  });

  it("gives a line for each thing the build had to say, in the order it said them", () => {
    const noted = build({
      buildNotes: [
        { code: UNREADABLE, params: { count: 1, ids: ["m1"], more: 0 } },
        { code: "replication.something_new", params: null },
      ],
    });
    expect(replicaNoteLines(noted, t)).toEqual([
      'replicaBuild.note.unreadable_messages {"total":1,"ids":"m1"}',
      'replicaBuild.note.other {"code":"replication.something_new"}',
    ]);
  });
});
