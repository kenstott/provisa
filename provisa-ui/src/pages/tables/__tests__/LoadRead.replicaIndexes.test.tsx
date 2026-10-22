// Copyright (c) 2026 Kenneth Stott
// Canary: 5f59bd3a-94fd-46fa-8c4a-0e51ec758c1b
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2015: the table's detail panel, read, says where its replica stands and its indexes with
// it -- the Load section shows what its form shows -- and a live-only source's table, which has
// no replica, shows neither.

import { describe, it, expect, vi } from "vitest";
import { render, screen } from "../../../test-utils/render";
import type { ReplicaBuild, ReplicaBuilds } from "../../../api/admin";
import type { RegisteredTable, Source } from "../../../types/admin";
import { ReplicaBuildsContext } from "../replicaBuildsContext";
import { LoadRead } from "../TableSectionsRead";

const kinds = vi.hoisted(() => ({ liveOnly: false }));
vi.mock("../../sources/sourceKind", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../sources/sourceKind")>()),
  useSourceKind: () => ({ live_only: kinds.liveOnly }),
}));

const TABLE = {
  id: 7,
  sourceId: "pg",
  schemaName: "public",
  tableName: "orders",
  viewSql: null,
  changeSignal: null,
  cacheTtl: 60,
  replicate: 0,
  loadProtected: null,
  roleTtl: [],
  delta: null,
  live: null,
  businessKey: [],
  validTime: null,
  recordedTime: null,
} as unknown as RegisteredTable;

const BUILD = {
  sourceId: "pg",
  schemaName: "public",
  tableName: "orders",
  state: "idle",
  completedAt: "2026-10-15T10:00:00+00:00",
  rowsCopied: 12,
  failedAttempts: 0,
  buildNotes: [],
  indexes: [
    { name: "rx_pk", columns: ["id"], primary: true, state: "built", error: null, errorCode: null },
    {
      name: "rx_cust",
      columns: ["customer_id"],
      primary: false,
      state: "failed",
      error: "disk full",
      errorCode: "replica.index_failed",
    },
  ],
} as unknown as ReplicaBuild;

function renderLoad() {
  const builds = { builds: [BUILD], convergenceError: null } as unknown as ReplicaBuilds;
  render(
    <ReplicaBuildsContext.Provider value={builds}>
      <LoadRead table={TABLE} source={{ id: "pg", type: "postgresql" } as Source} />
    </ReplicaBuildsContext.Provider>,
  );
}

describe("LoadRead: the replica and its indexes", () => {
  it("shows the replica's indexes, built and failed, in the read panel", () => {
    kinds.liveOnly = false;
    renderLoad();
    expect(screen.getByTestId("replica-build-line")).toBeInTheDocument();
    expect(screen.getByTestId("replica-index-line")).toHaveTextContent("1 of 2");
    expect(screen.getByTestId("replica-index-failed")).toHaveTextContent("disk full");
  });

  it("shows no replica for a live-only source's table", () => {
    kinds.liveOnly = true;
    renderLoad();
    expect(screen.queryByTestId("replica-build-line")).toBeNull();
    expect(screen.queryByTestId("replica-index-line")).toBeNull();
  });
});
