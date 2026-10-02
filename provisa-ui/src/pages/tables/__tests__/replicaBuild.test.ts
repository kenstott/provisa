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
import { replicaBuildLine } from "../replicaBuild";

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
    waitingOn: null,
    waitingOnCode: null,
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
      replicaBuildLine(build({ state: "requested", waitingOn: "the engine is busy" }), t, num, when, msg),
    ).toBe('replicaBuild.waiting {"waitingOn":"the engine is busy"}');
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
