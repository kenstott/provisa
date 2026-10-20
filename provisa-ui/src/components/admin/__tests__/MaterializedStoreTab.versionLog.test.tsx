// Copyright (c) 2026 Kenneth Stott
// Canary: e132d292-41e3-48b8-9b5c-2af9a7d24cfb
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2109: a bitemporal table updated in place keeps its versions in a log Provisa captures. The
// materialized store lists it read-only, named for its table, with a full capture that records
// the rows the table deleted.

import { describe, it, expect, vi } from "vitest";
import userEvent from "@testing-library/user-event";
import { render, screen, within } from "../../../test-utils/render";
import type { MVInfo } from "../../../api/admin";

const mv = (over: Partial<MVInfo>): MVInfo => ({
  id: "view-daily_totals",
  sourceTables: ["payments"],
  targetTable: "mv_daily_totals",
  refreshInterval: 300,
  enabled: true,
  status: "fresh",
  lastRefreshAt: null,
  rowCount: 4,
  lastError: null,
  versionLogOf: null,
  ...over,
});
const rows = [
  mv({}),
  mv({ id: "versions-7", targetTable: "payments__versions_7", versionLogOf: "payments" }),
];
const refreshMV = vi.fn(async () => ({ success: true, message: "" }));
const toggleMV = vi.fn();

vi.mock("../../../hooks/useAdminOpsQueries", async (orig) => ({
  ...(await orig<typeof import("../../../hooks/useAdminOpsQueries")>()),
  useMVList: () => ({ mvList: rows, loading: false, error: undefined, refetch: vi.fn() }),
  useRefreshMV: () => ({ refreshMV, loading: false }),
  useToggleMV: () => ({ toggleMV, loading: false }),
  useMaterializeStoreInfo: () => ({ materializeStoreInfo: null }),
}));

import { MaterializedStoreTab } from "../CacheManager";

describe("a table's version log in the materialized store", () => {
  it("is named for its table and offers a full capture, not a disable", async () => {
    render(<MaterializedStoreTab platform={false} />);
    const log = screen.getByText("Version log of payments").closest("tr") as HTMLElement;
    expect(within(log).queryByTestId("mv-toggle-btn-versions-7")).toBeNull();
    await userEvent.click(within(log).getByTestId("mv-full-capture-btn-versions-7"));
    expect(refreshMV).toHaveBeenCalledWith("versions-7", true);
    // its ordinary capture is the refresh every view has
    await userEvent.click(within(log).getByTestId("mv-refresh-btn-versions-7"));
    expect(refreshMV).toHaveBeenLastCalledWith("versions-7", false);
  });

  it("leaves every other view as it was", () => {
    render(<MaterializedStoreTab platform={false} />);
    const view = screen.getByText("daily_totals").closest("tr") as HTMLElement;
    expect(within(view).getByTestId("mv-toggle-btn-view-daily_totals")).toBeInTheDocument();
    expect(within(view).queryByTestId("mv-full-capture-btn-view-daily_totals")).toBeNull();
  });
});
