// Copyright (c) 2026 Kenneth Stott
// Canary: b1f4d2a7-9c3e-4a51-8d0b-7e2f6a1c4d90
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, fireEvent, waitFor, within } from "../test-utils/render";
import i18n from "../i18n";
import { ScheduledTasks } from "../components/admin/ScheduledTasks";

const t = i18n.getFixedT("en");

const createSpy = vi.fn(async () => ({ success: true, message: "ok" }));
const deleteSpy = vi.fn(async () => ({ success: true, message: "ok" }));
const toggleSpy = vi.fn(async () => ({ success: true, message: "ok" }));
const updateSpy = vi.fn(async () => ({ success: true, message: "ok" }));

let mockTasks: unknown[] = [];
const refetchSpy = vi.fn(async () => ({}));

// Spread the real module: vmThreads + fileParallelism:false share one module registry, so a
// replace-everything factory here leaks into other files. In particular a stubbed
// useMaterializeStoreInfo would win over the real Apollo hook that
// TableEditForm.consistency drives through MockedProvider.
vi.mock("../hooks/useAdminOpsQueries", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../hooks/useAdminOpsQueries")>()),
  useScheduledTasks: () => ({ scheduledTasks: mockTasks, loading: false, refetch: refetchSpy }),
  useToggleScheduledTask: () => ({ toggleScheduledTask: toggleSpy }),
  useCreateScheduledTask: () => ({ createScheduledTask: createSpy }),
  useUpdateScheduledTask: () => ({ updateScheduledTask: updateSpy }),
  useDeleteScheduledTask: () => ({ deleteScheduledTask: deleteSpy }),
}));

vi.mock("../hooks/useAdminQueries", () => ({
  useRoles: () => ({ roles: [{ id: "ops" }, { id: "analyst" }], loading: false }),
  useTables: () => ({
    tables: [{ sourceId: "shop", schemaName: "public", tableName: "orders" }],
    loading: false,
  }),
}));

vi.mock("../api/actions", () => ({
  fetchActions: vi.fn(async () => ({
    functions: [{ name: "reconcile_orders", arguments: [] }],
    webhooks: [],
  })),
}));

/** A trigger as the list reads it; each test overrides what it is about. */
function task(over: Record<string, unknown> = {}) {
  return {
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
    enabled: true,
    lastRunAt: null,
    lastOutcome: null,
    lastError: null,
    lastFiredBy: null,
    nextRunAt: null,
    ...over,
  };
}

const pick = async (label: string, option: string) => {
  fireEvent.click(screen.getByRole("textbox", { name: label }));
  fireEvent.click(await screen.findByRole("option", { name: option, hidden: true }));
};

/** Open a trigger's detail: its row is selected and the right-hand panel shows it. */
const openRow = (id: string) => fireEvent.click(screen.getByTestId(`scheduled-task-row-${id}`));

describe("ScheduledTasks — SQL trigger", () => {
  beforeEach(() => {
    createSpy.mockClear();
    deleteSpy.mockClear();
    toggleSpy.mockClear();
    updateSpy.mockClear();
    mockTasks = [];
  });

  const selectKind = async (label: string) => {
    fireEvent.click(screen.getByRole("textbox", { name: t("scheduledTasks.kindLabel") }));
    const listbox = await screen.findByRole("listbox");
    fireEvent.click(within(listbox).getByText(label));
  };

  it("shows the SQL statement field only when kind is SQL", async () => {
    render(<ScheduledTasks />);
    fireEvent.click(screen.getByTestId("scheduled-tasks-toggle-form"));

    // Webhook kind by default: no SQL field.
    expect(screen.queryByLabelText(t("scheduledTasks.sqlLabel"))).toBeNull();

    await selectKind(t("scheduledTasks.kindSql"));
    expect(await screen.findByLabelText(t("scheduledTasks.sqlLabel"))).toBeTruthy();
    // Date-token hint is visible.
    expect(screen.getByText(/\{\{YYYY-MM-DD\}\}/)).toBeTruthy();
  });

  it("creates a SQL trigger with the entered statement + cron", async () => {
    render(<ScheduledTasks />);
    fireEvent.click(screen.getByTestId("scheduled-tasks-toggle-form"));
    await selectKind(t("scheduledTasks.kindSql"));
    fireEvent.change(screen.getByLabelText(t("scheduledTasks.idLabel")), {
      target: { value: "nightly" },
    });
    fireEvent.change(screen.getByLabelText(t("scheduledTasks.nameLabel")), {
      target: { value: "Nightly Rollup" },
    });
    fireEvent.change(screen.getByLabelText(t("scheduledTasks.cronLabel")), {
      target: { value: "0 2 * * *" },
    });
    fireEvent.change(await screen.findByLabelText(t("scheduledTasks.sqlLabel")), {
      target: { value: "INSERT INTO audit.d SELECT '{{YYYY-MM-DD}}'" },
    });

    // A SQL task runs as a role: without one it is not sent.
    fireEvent.click(screen.getByTestId("scheduled-tasks-submit"));
    expect(await screen.findByText(t("scheduledTasks.validationRoleRequired"))).toBeTruthy();
    expect(createSpy).not.toHaveBeenCalled();

    fireEvent.click(screen.getByRole("textbox", { name: t("scheduledTasks.roleLabel") }));
    fireEvent.click(await screen.findByRole("option", { name: "ops", hidden: true }));
    fireEvent.click(screen.getByTestId("scheduled-tasks-submit"));

    await waitFor(() => expect(createSpy).toHaveBeenCalledTimes(1));
    expect(createSpy).toHaveBeenCalledWith({
      id: "nightly",
      name: "Nightly Rollup",
      cron: "0 2 * * *",
      kind: "sql",
      sql: "INSERT INTO audit.d SELECT '{{YYYY-MM-DD}}'",
      role: "ops",
    });
  });

  it("lists an existing SQL trigger and can delete it from its panel", async () => {
    mockTasks = [task()];
    render(<ScheduledTasks />);
    expect(screen.getByText("INSERT INTO audit.d SELECT 1")).toBeTruthy();
    openRow("nightly");
    // The read view says what it runs and as which role.
    const read = await screen.findByTestId("scheduled-task-read");
    expect(within(read).getByText("ops")).toBeTruthy();

    // Delete asks first, naming the trigger; nothing is deleted until it is confirmed.
    fireEvent.click(screen.getByTestId("scheduled-task-delete"));
    const dialog = await screen.findByRole("dialog");
    expect(dialog.textContent).toContain(
      t("scheduledTasks.deleteTitle", { name: "Nightly Rollup" }),
    );
    expect(deleteSpy).not.toHaveBeenCalled();
    fireEvent.click(screen.getByRole("button", { name: t("common.delete") }));
    await waitFor(() => expect(deleteSpy).toHaveBeenCalledWith("nightly"));
    // REQ-2198: the list is read again, and the panel closes on it.
    await waitFor(() => expect(screen.queryByTestId("scheduled-task-read")).toBeNull());
    expect(refetchSpy).toHaveBeenCalled();
  });
});

// A mutation wrapper throws when the server answers with GraphQL errors: the action's busy state
// ends, and what went wrong is said, never a button left spinning.
describe("ScheduledTasks — a failed action", () => {
  beforeEach(() => {
    mockTasks = [task({ sql: "SELECT 1" })];
  });

  const confirmDelete = async () => {
    openRow("nightly");
    fireEvent.click(await screen.findByTestId("scheduled-task-delete"));
    await screen.findByRole("dialog");
    fireEvent.click(screen.getByRole("button", { name: t("common.delete") }));
  };

  // REQ-2198: a failed delete is said in the panel it was pressed in, which it unlocks.
  it("says why a delete raised, and frees its button", async () => {
    deleteSpy.mockImplementationOnce(async () => {
      throw new Error("scheduled task nightly is in use");
    });
    render(<ScheduledTasks />);
    await confirmDelete();
    expect((await screen.findByTestId("scheduled-task-delete-problem")).textContent).toBe(
      t("common.deleteFailed", { reason: "scheduled task nightly is in use" }),
    );
    expect(screen.getByTestId("scheduled-task-delete")).not.toBeDisabled();
    expect(screen.getByTestId("scheduled-task-read")).toBeTruthy();
  });

  it("says a refused delete, rather than taking it for done", async () => {
    deleteSpy.mockImplementationOnce(async () => ({ success: false, message: "refused: in use" }));
    render(<ScheduledTasks />);
    await confirmDelete();
    expect((await screen.findByTestId("scheduled-task-delete-problem")).textContent).toBe(
      t("common.deleteFailed", { reason: "refused: in use" }),
    );
    expect(screen.getByTestId("scheduled-task-read")).toBeTruthy();
  });

  it("says why a create raised, and frees the form's save", async () => {
    createSpy.mockImplementationOnce(async () => {
      throw new Error("id nightly is taken");
    });
    mockTasks = [];
    render(<ScheduledTasks />);
    fireEvent.click(screen.getByTestId("scheduled-tasks-toggle-form"));
    fireEvent.change(screen.getByLabelText(t("scheduledTasks.idLabel")), {
      target: { value: "nightly" },
    });
    fireEvent.change(screen.getByLabelText(t("scheduledTasks.nameLabel")), {
      target: { value: "Nightly" },
    });
    fireEvent.change(screen.getByLabelText(t("scheduledTasks.cronLabel")), {
      target: { value: "0 2 * * *" },
    });
    fireEvent.click(screen.getByRole("textbox", { name: t("scheduledTasks.kindLabel") }));
    fireEvent.click(
      within(await screen.findByRole("listbox")).getByText(t("scheduledTasks.kindSql")),
    );
    fireEvent.change(await screen.findByLabelText(t("scheduledTasks.sqlLabel")), {
      target: { value: "SELECT 1" },
    });
    fireEvent.click(screen.getByRole("textbox", { name: t("scheduledTasks.roleLabel") }));
    fireEvent.click(await screen.findByRole("option", { name: "ops", hidden: true }));
    fireEvent.click(screen.getByTestId("scheduled-tasks-submit"));

    await waitFor(() => expect(createSpy).toHaveBeenCalledTimes(1));
    expect(await screen.findByText("id nightly is taken")).toBeTruthy();
    // The form stays open to correct, its save free again.
    expect(screen.getByTestId("scheduled-tasks-submit")).not.toHaveAttribute("data-loading");
  });
});

describe("Triggers — on a table, edited, and how the last run ended", () => {
  beforeEach(() => {
    createSpy.mockClear();
    updateSpy.mockClear();
    mockTasks = [];
  });

  it("creates a command trigger fired when a table changes", async () => {
    render(<ScheduledTasks />);
    fireEvent.click(screen.getByTestId("scheduled-tasks-toggle-form"));
    fireEvent.change(screen.getByLabelText(t("scheduledTasks.idLabel")), {
      target: { value: "recon" },
    });
    fireEvent.change(screen.getByLabelText(t("scheduledTasks.nameLabel")), {
      target: { value: "Reconcile orders" },
    });
    await pick(t("scheduledTasks.firesOnLabel"), t("scheduledTasks.firesChange"));
    // A trigger on a table has no cron expression; it names the table instead.
    expect(screen.queryByLabelText(t("scheduledTasks.cronLabel"))).toBeNull();
    await pick(t("scheduledTasks.tableLabel"), "shop.public.orders");
    await pick(t("scheduledTasks.kindLabel"), t("scheduledTasks.kindCommand"));
    await pick(t("scheduledTasks.commandLabel"), "reconcile_orders");
    await pick(t("scheduledTasks.roleLabel"), "ops");
    fireEvent.click(screen.getByTestId("scheduled-tasks-submit"));
    await waitFor(() => expect(createSpy).toHaveBeenCalledTimes(1));
    expect(createSpy).toHaveBeenCalledWith({
      id: "recon",
      name: "Reconcile orders",
      onTable: "shop.public.orders",
      onEvent: "change",
      kind: "command",
      command: "reconcile_orders",
      role: "ops",
    });
  });

  it("edits a trigger in its panel, sending its new definition under the same id", async () => {
    mockTasks = [task()];
    render(<ScheduledTasks />);
    openRow("nightly");
    fireEvent.click(await screen.findByTestId("scheduled-task-edit"));
    // The id names the trigger and is not changed by an edit.
    expect(screen.getByLabelText(t("scheduledTasks.idLabel"))).toBeDisabled();
    fireEvent.change(screen.getByLabelText(t("scheduledTasks.cronLabel")), {
      target: { value: "30 4 * * 1" },
    });
    fireEvent.click(screen.getByTestId("scheduled-tasks-submit"));
    await waitFor(() => expect(updateSpy).toHaveBeenCalledTimes(1));
    expect(updateSpy).toHaveBeenCalledWith({
      id: "nightly",
      name: "Nightly Rollup",
      cron: "30 4 * * 1",
      kind: "sql",
      sql: "INSERT INTO audit.d SELECT 1",
      role: "ops",
    });
    expect(createSpy).not.toHaveBeenCalled();
  });

  it("shows how a table trigger's last run ended, its error and what fired it", async () => {
    mockTasks = [
      task({
        id: "on-error",
        name: "Page on error",
        firesOn: "event",
        cronExpression: null,
        onTable: "shop.public.orders",
        onEvent: "error",
        kind: "command",
        sql: null,
        command: "reconcile_orders",
        lastRunAt: "2026-10-15T12:00:00Z",
        lastOutcome: "failed",
        lastError: "the command refused the batch",
        lastFiredBy: "event:shop.public.orders",
      }),
    ];
    render(<ScheduledTasks />);
    expect(
      screen.getByText(
        t("scheduledTasks.firesOnTable", {
          table: "shop.public.orders",
          event: t("scheduledTasks.eventError"),
        }),
      ),
    ).toBeTruthy();
    openRow("on-error");
    const read = await screen.findByTestId("scheduled-task-read");
    expect(within(read).getByText(t("scheduledTasks.outcomeFailed"))).toBeTruthy();
    expect(within(read).getByText("the command refused the batch")).toBeTruthy();
    expect(within(read).getByText("event:shop.public.orders")).toBeTruthy();
  });
});
