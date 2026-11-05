// Copyright (c) 2026 Kenneth Stott
// Canary: 3926a5dd-3ca0-4977-b980-e205e4267706
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1620: a trace's spans are read as the selected roles, the "Role: All" set included, never
// as a role the picker did not select.

import { describe, it, expect, vi } from "vitest";
import { render, waitFor } from "../../../test-utils/render";

vi.mock("../../../context/AuthContext", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../../context/AuthContext")>()),
  useAuth: () => ({ actingRoles: "analyst,steward" }),
}));

const runSql = vi.fn();
vi.mock("../../../api/admin", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../../api/admin")>()),
  runSql: (...a: unknown[]) => runSql(...a),
}));

import { TraceDetailsModal } from "../TraceDetailsModal";

describe("TraceDetailsModal", () => {
  it("reads the spans as the selected set of roles", async () => {
    runSql.mockResolvedValue({ columns: [], rows: [] });
    render(
      <TraceDetailsModal kind="trace" id="0af7651916cd43dd8448eb211c80319c" onClose={() => {}} />,
    );
    await waitFor(() => expect(runSql).toHaveBeenCalledWith(expect.anything(), "analyst,steward"));
  });
});
