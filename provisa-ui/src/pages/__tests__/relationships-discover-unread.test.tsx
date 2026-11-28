// Copyright (c) 2026 Kenneth Stott
// Canary: 2b7e94c1-5a3d-4f86-9c0e-6d1a8f3b5e27
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-413: a table of a run's sets that could not be read takes no part in the run, which goes on
// over the rest; the dialog lists each such table with why, beneath the run's block. A run over
// every table of the model failed whole on one GraphQL table whose read failed.
import { describe, it, expect, vi, beforeEach } from "vitest";
import { screen, fireEvent } from "@testing-library/react";
import {
  REPORT,
  SLOW,
  openDialog,
  resetDiscoveryMocks,
  runButton,
  runMock,
  runOf,
} from "./discoverDialog.fixtures";

vi.mock("../../hooks/useAdminQueries", async () =>
  (await import("./discoverDialog.state")).adminQueries(),
);
vi.mock("../../context/AuthContext", async () => {
  const { AUTH } = await import("./discoverDialog.state");
  return { useAuth: () => AUTH };
});
vi.mock("../../context/DomainFilterContext", async () => {
  const { FILTER } = await import("./discoverDialog.state");
  return { useDomainFilter: () => FILTER };
});
vi.mock("../../api/admin", async (importOriginal) =>
  (await import("./discoverDialog.state")).adminApi(
    await importOriginal<typeof import("../../api/admin")>(),
  ),
);
vi.mock("../../api/actions", () => ({
  fetchActions: vi.fn().mockResolvedValue({ functions: [], webhooks: [] }),
}));
vi.mock("../../components/TagControl", () => ({ TagControl: () => null }));

beforeEach(resetDiscoveryMocks);

describe("Discovery dialog: tables that could not be read", () => {
  it("lists each table that could not be read, with why, and still shows what the run found", async () => {
    runMock.mockResolvedValue(
      runOf({
        status: "done",
        report: {
          ...REPORT,
          tables_unread: [
            {
              table_id: 41,
              table: "shelter.schedules",
              reason: 'Binder Error: Values list "t" does not have a column named "employee_id"',
            },
            { table_id: 42, table: "shop.ledger", reason: "the source has no connection" },
          ],
        },
      }),
    );
    await openDialog();
    fireEvent.click(runButton());
    const unread = await screen.findByTestId("discover-unread", {}, SLOW);
    expect(unread).toHaveTextContent("2 tables could not be read and took no part in the run");
    expect(screen.getByTestId("discover-unread-41")).toHaveTextContent(
      'shelter.schedules: Binder Error: Values list "t" does not have a column named "employee_id"',
    );
    expect(screen.getByTestId("discover-unread-42")).toHaveTextContent(
      "shop.ledger: the source has no connection",
    );
    expect(screen.getByTestId("discover-run-line")).not.toHaveTextContent("failed");
  });

  it("names no unread table when every table was read", async () => {
    runMock.mockResolvedValue(runOf({ status: "done", report: { ...REPORT, tables_unread: [] } }));
    await openDialog();
    fireEvent.click(runButton());
    await screen.findByTestId("discover-run-line", {}, SLOW);
    expect(screen.queryByTestId("discover-unread")).toBeNull();
  });
});
