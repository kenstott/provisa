// Copyright (c) 2026 Kenneth Stott
// Canary: d9ae650d-986b-4042-8b11-69a72073569e
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// Creating a trigger: "+ Trigger" opens an empty form whatever trigger was selected or edited
// before it, and Duplicate (the panel head, beside Edit) opens the same create form filled from
// the selected trigger under a new id and name (rule: components/list/__tests__/panelDuplicateGuard.test.ts).

import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, fireEvent, waitFor } from "../test-utils/render";
import { expectHoldsNoneOf } from "../test-utils/createForm";
import { ScheduledTasks } from "../components/admin/ScheduledTasks";

const createSpy = vi.fn(async (_input: Record<string, unknown>) => ({
  success: true,
  message: "ok",
}));
const updateSpy = vi.fn(async () => ({ success: true, message: "ok" }));

const TASKS = [
  {
    id: "nightly",
    name: "Nightly Rollup",
    firesOn: "schedule",
    cronExpression: "0 2 * * *",
    onTable: null,
    onEvent: null,
    webhookUrl: null,
    kind: "sql",
    sql: "INSERT INTO audit.d SELECT 1",
    command: null,
    role: "ops",
    enabled: false,
    lastRunAt: null,
    lastOutcome: null,
    lastError: null,
    lastFiredBy: null,
    nextRunAt: null,
  },
];

vi.mock("../hooks/useAdminOpsQueries", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../hooks/useAdminOpsQueries")>()),
  useScheduledTasks: () => ({ scheduledTasks: TASKS, loading: false, refetch: vi.fn() }),
  useToggleScheduledTask: () => ({ toggleScheduledTask: vi.fn() }),
  useCreateScheduledTask: () => ({ createScheduledTask: createSpy }),
  useUpdateScheduledTask: () => ({ updateScheduledTask: updateSpy }),
  useDeleteScheduledTask: () => ({ deleteScheduledTask: vi.fn() }),
}));
vi.mock("../hooks/useAdminQueries", () => ({
  useRoles: () => ({ roles: [{ id: "ops" }, { id: "analyst" }], loading: false }),
  useTables: () => ({
    tables: [{ sourceId: "shop", schemaName: "public", tableName: "orders" }],
    loading: false,
  }),
}));
vi.mock("../api/actions", () => ({
  fetchActions: vi.fn(async () => ({ functions: [], webhooks: [] })),
}));

const VALUES = [
  "nightly",
  "Nightly Rollup",
  "0 2 * * *",
  "INSERT INTO audit.d SELECT 1",
  "nightly-copy",
];
const openRow = () => fireEvent.click(screen.getByTestId("scheduled-task-row-nightly"));
const panel = () => screen.getByTestId("scheduled-tasks-split-panel");

beforeEach(() => vi.clearAllMocks());

describe("+ Trigger opens an empty form", () => {
  it("holds none of the values of the trigger selected before it", () => {
    render(<ScheduledTasks />);
    openRow();
    fireEvent.click(screen.getByTestId("scheduled-tasks-toggle-form"));
    expectHoldsNoneOf(panel(), VALUES);
  });

  it("holds none of them after the trigger was edited and the edit cancelled", async () => {
    render(<ScheduledTasks />);
    openRow();
    fireEvent.click(screen.getByTestId("scheduled-task-edit"));
    fireEvent.click(await screen.findByText("Cancel"));
    fireEvent.click(screen.getByTestId("scheduled-tasks-toggle-form"));
    expectHoldsNoneOf(panel(), VALUES);
  });
});

describe("Duplicate, in a trigger's panel head", () => {
  it("is beside Edit and Delete, named Duplicate", () => {
    render(<ScheduledTasks />);
    openRow();
    expect(screen.getByTestId("scheduled-task-duplicate")).toHaveAccessibleName("Duplicate");
  });

  it("opens the create form filled from the trigger, under a new id and name", () => {
    render(<ScheduledTasks />);
    openRow();
    fireEvent.click(screen.getByTestId("scheduled-task-duplicate"));
    expect(screen.getByDisplayValue("nightly-copy")).toBeInTheDocument();
    expect(screen.getByDisplayValue("Nightly Rollup-copy")).toBeInTheDocument();
    expect(screen.getByDisplayValue("0 2 * * *")).toBeInTheDocument();
    expect(screen.getByDisplayValue("INSERT INTO audit.d SELECT 1")).toBeInTheDocument();
  });

  it("creates the copy as a create, with what the trigger runs, never as an update", async () => {
    render(<ScheduledTasks />);
    openRow();
    fireEvent.click(screen.getByTestId("scheduled-task-duplicate"));
    fireEvent.click(screen.getByTestId("scheduled-tasks-submit"));
    await waitFor(() => expect(createSpy).toHaveBeenCalledTimes(1));
    expect(updateSpy).not.toHaveBeenCalled();
    expect(createSpy.mock.calls[0][0]).toMatchObject({
      id: "nightly-copy",
      name: "Nightly Rollup-copy",
      cron: "0 2 * * *",
      kind: "sql",
      sql: "INSERT INTO audit.d SELECT 1",
      role: "ops",
    });
  });
});
