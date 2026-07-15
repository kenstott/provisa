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

let mockTasks: unknown[] = [];

vi.mock("../hooks/useAdminQueries", () => ({
  useScheduledTasks: () => ({ scheduledTasks: mockTasks, loading: false }),
  useToggleScheduledTask: () => ({ toggleScheduledTask: toggleSpy }),
  useCreateScheduledTask: () => ({ createScheduledTask: createSpy }),
  useDeleteScheduledTask: () => ({ deleteScheduledTask: deleteSpy }),
}));

vi.mock("../api/actions", () => ({
  fetchActions: vi.fn(async () => ({ functions: [], webhooks: [] })),
}));

describe("ScheduledTasks — SQL trigger", () => {
  beforeEach(() => {
    createSpy.mockClear();
    deleteSpy.mockClear();
    toggleSpy.mockClear();
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

    fireEvent.click(screen.getByTestId("scheduled-tasks-submit"));

    await waitFor(() => expect(createSpy).toHaveBeenCalledTimes(1));
    expect(createSpy).toHaveBeenCalledWith({
      id: "nightly",
      name: "Nightly Rollup",
      cron: "0 2 * * *",
      kind: "sql",
      sql: "INSERT INTO audit.d SELECT '{{YYYY-MM-DD}}'",
    });
  });

  it("lists an existing SQL trigger and can delete it", async () => {
    mockTasks = [
      {
        id: "nightly",
        name: "Nightly Rollup",
        cronExpression: "0 2 * * *",
        webhookUrl: null,
        kind: "sql",
        sql: "INSERT INTO audit.d SELECT 1",
        enabled: true,
        lastRunAt: null,
        nextRunAt: null,
      },
    ];
    render(<ScheduledTasks />);
    expect(screen.getByText("INSERT INTO audit.d SELECT 1")).toBeTruthy();

    fireEvent.click(screen.getByRole("button", { name: t("scheduledTasks.delete") }));
    await waitFor(() => expect(deleteSpy).toHaveBeenCalledWith("nightly"));
  });
});
