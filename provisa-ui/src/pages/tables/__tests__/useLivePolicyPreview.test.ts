// Copyright (c) 2026 Kenneth Stott
// Canary: 9b60db9f-5248-447c-a030-946ab745050a
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1143/REQ-826: the policy preview follows the draft's Replicate value, and is not asked for a
// draft the server refuses (Never together with load protection on the table itself).

import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";
import { renderHook, act } from "@testing-library/react";
import type { RegisteredTable } from "../../../types/admin";

const preview = vi.fn();
vi.mock("../../../hooks/useAdminQueries", () => ({
  useRefreshPolicyPreview: () => preview,
}));

import { useLivePolicyPreview } from "../useLivePolicyPreview";

const TABLE = {
  id: 7,
  sourceId: "sales-pg",
  domainId: "sales",
  schemaName: "public",
  tableName: "orders",
  cacheTtl: 60,
  replicate: null,
  loadProtected: null,
  offPeakWindow: null,
  offPeakTz: null,
  changeSignal: null,
  refreshPolicySummary: null,
} as unknown as RegisteredTable;

describe("useLivePolicyPreview", () => {
  beforeEach(() => {
    vi.useFakeTimers();
    preview.mockReset();
    preview.mockResolvedValue(null);
  });
  afterEach(() => {
    vi.useRealTimers();
  });

  it("previews the draft's Replicate value", async () => {
    renderHook(() => useLivePolicyPreview({ ...TABLE, replicate: 500 }, undefined));
    await act(async () => {
      await vi.advanceTimersByTimeAsync(300);
    });
    expect(preview).toHaveBeenCalledTimes(1);
    expect(preview).toHaveBeenCalledWith(
      expect.objectContaining({ replicate: 500, loadProtected: null, cacheTtl: 60 }),
    );
  });

  it("does not ask for a preview of Never with load protection", async () => {
    renderHook(() =>
      useLivePolicyPreview({ ...TABLE, replicate: -1, loadProtected: true }, undefined),
    );
    await act(async () => {
      await vi.advanceTimersByTimeAsync(1000);
    });
    expect(preview).not.toHaveBeenCalled();
  });
});
