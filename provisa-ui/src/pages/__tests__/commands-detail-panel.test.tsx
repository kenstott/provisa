// Copyright (c) 2026 Kenneth Stott
// Canary: e7d4ac3f-7121-4c2f-8770-db14e7a4b8ad
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// The Commands page shows a command's detail in a panel at the right of its two lists, one at
// a time. A command's actions are in the panel's head. A test run is started from the panel,
// as the role chosen there, and its result is shown in the panel with a way back. "+ Command"
// asks only what a command needs in order to exist; saved, it is selected and opens in edit.

import { beforeEach, describe, it, expect, vi } from "vitest";
import { fireEvent, render, screen, waitFor, within } from "../../test-utils/render";
import userEvent from "@testing-library/user-event";
import type { TrackedFunction, TrackedWebhook } from "../../api/actions";

const fn = (name: string): TrackedFunction =>
  ({
    name,
    sourceId: "sales-pg",
    schemaName: "public",
    functionName: name,
    returns: "public.orders",
    arguments: [{ name: "order_id", type: "Int" }],
    visibleTo: ["admin"],
    domainId: "sales",
    description: "Refunds one order",
    kind: "mutation",
    implKind: "source_procedure",
    binding: {},
  }) as unknown as TrackedFunction;
const wh = (name: string): TrackedWebhook =>
  ({
    name,
    url: "https://hooks.example.com/ops",
    method: "POST",
    timeoutMs: 5000,
    returns: null,
    inlineReturnType: [],
    arguments: [],
    visibleTo: [],
    domainId: "",
    description: "",
    kind: "mutation",
    approved: true,
  }) as unknown as TrackedWebhook;

const NO_SOURCES = vi.hoisted(() => ({ sources: [], loading: false, refetch: () => undefined }));
const NO_DOMAINS = vi.hoisted(() => ({ domains: [], loading: false, refetch: () => undefined }));
let ACTIONS: { functions: TrackedFunction[]; webhooks: TrackedWebhook[] };
const saveWebhook = vi.fn();
const testAction = vi.fn();

vi.mock("../../api/actions", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../api/actions")>()),
  fetchActions: vi.fn(async () => ACTIONS),
  saveFunction: vi.fn().mockResolvedValue(undefined),
  saveWebhook: (...args: unknown[]) => saveWebhook(...args),
  testAction: (...args: unknown[]) => testAction(...args),
}));

vi.mock("../../api/admin", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../api/admin")>()),
  fetchOrgRoles: vi.fn().mockResolvedValue([{ id: "admin" }, { id: "analyst" }]),
}));

vi.mock("../../context/AuthContext", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../context/AuthContext")>()),
  useAuth: () => ({
    role: "admin",
    selectedRoles: ["admin"],
    capabilities: ["admin"],
    domainAccess: ["*"],
    activeOrgId: "acme",
  }),
}));

vi.mock("../../hooks/useAdminQueries", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../hooks/useAdminQueries")>()),
  // The same lists every render: the page reacts to a change of them.
  useSources: () => NO_SOURCES,
  useDomains: () => NO_DOMAINS,
}));

import { CommandsPage } from "../CommandsPage";

const panel = () => screen.queryByTestId("commands-split-panel");

beforeEach(() => {
  ACTIONS = { functions: [fn("refund_order"), fn("close_period")], webhooks: [wh("notify_ops")] };
  saveWebhook.mockReset().mockResolvedValue(undefined);
  testAction.mockReset().mockResolvedValue({ rows: [{ refunded: true }] });
});

describe("the Commands lists' search", () => {
  it("lists the commands matching any word of the search, in both lists", async () => {
    render(<CommandsPage />);
    await screen.findByTestId("commands-fn-row-refund_order");
    fireEvent.change(screen.getByTestId("filter-input"), { target: { value: "refund notify" } });
    expect(screen.getByTestId("commands-fn-row-refund_order")).toBeInTheDocument();
    expect(screen.getByTestId("commands-wh-row-notify_ops")).toBeInTheDocument();
    expect(screen.queryByTestId("commands-fn-row-close_period")).toBeNull();
  });
});

describe("a command's detail is a panel at the right of the lists", () => {
  it("shows nothing until a row is chosen, then that command, one at a time across both lists", async () => {
    render(<CommandsPage />);
    const row = await screen.findByTestId("commands-fn-row-refund_order");
    expect(panel()).toBeNull();
    await userEvent.click(row);
    expect(screen.getByTestId("commands-split-title")).toHaveTextContent("refund_order");
    expect(row).toHaveClass("row-selected");

    // A webhook is in the other list; choosing it swaps the one panel.
    await userEvent.click(screen.getByTestId("commands-wh-row-notify_ops"));
    expect(screen.getByTestId("commands-split-title")).toHaveTextContent("notify_ops");
    expect(row).not.toHaveClass("row-selected");
    expect(screen.getByTestId("commands-wh-row-notify_ops")).toHaveClass("row-selected");
  });

  it("closes by its own close, by Escape, and by the row again", async () => {
    render(<CommandsPage />);
    const row = await screen.findByTestId("commands-fn-row-refund_order");
    await userEvent.click(row);
    await userEvent.click(screen.getByTestId("commands-split-close"));
    expect(panel()).toBeNull();
    await userEvent.click(row);
    await userEvent.keyboard("{Escape}");
    expect(panel()).toBeNull();
    await userEvent.click(row);
    await userEvent.click(row);
    expect(panel()).toBeNull();
  });

  it("puts the command's actions in the panel's head", async () => {
    render(<CommandsPage />);
    await userEvent.click(await screen.findByTestId("commands-fn-row-refund_order"));
    const head = within(screen.getByTestId("commands-split-panel"));
    expect(head.getByRole("button", { name: "Edit refund_order" })).toBeInTheDocument();
    expect(head.getByRole("button", { name: "Test" })).toBeInTheDocument();
    expect(head.getByRole("button", { name: "Delete function refund_order" })).toBeInTheDocument();
  });

  it("runs a test from the panel and shows its result there, with a way back", async () => {
    render(<CommandsPage />);
    await userEvent.click(await screen.findByTestId("commands-fn-row-refund_order"));
    // Whom the run is made as is chosen in the command's own panel.
    expect(within(panel()!).getByText("Test as role:")).toBeInTheDocument();
    await userEvent.click(screen.getByRole("button", { name: "Test" }));
    await waitFor(() => expect(testAction).toHaveBeenCalledWith("function", "refund_order", ""));
    const result = await screen.findByTestId("commands-test-result");
    expect(within(panel()!).getByText(/"refunded": true/)).toBeInTheDocument();
    expect(result).toHaveTextContent("refund_order");
    expect(document.querySelector(".mantine-Modal-root")).toBeNull(); // not a dialog over the page
    await userEvent.click(screen.getByTestId("commands-test-back"));
    expect(screen.queryByTestId("commands-test-result")).toBeNull();
    expect(screen.getByRole("button", { name: "Edit refund_order" })).toBeInTheDocument();
  });

  it("does not close on Escape while a command is being edited", async () => {
    render(<CommandsPage />);
    await userEvent.click(await screen.findByTestId("commands-fn-row-refund_order"));
    await userEvent.click(screen.getByRole("button", { name: "Edit refund_order" }));
    await userEvent.keyboard("{Escape}");
    expect(screen.getByTestId("commands-split-panel")).toBeInTheDocument();
    expect(within(panel()!).getByText("Save")).toBeInTheDocument();
  });
});

describe("+ Command asks only what a command needs in order to exist", () => {
  it("asks a function for its name, what it calls and its domain, and nothing a command can exist without", async () => {
    render(<CommandsPage />);
    await userEvent.click(await screen.findByTestId("commands-toggle-form"));
    const step = within(panel()!);
    expect(screen.getByTestId("commands-split-title")).toHaveTextContent("New command");
    expect(step.getByLabelText(/^Name/)).toBeInTheDocument();
    expect(step.getByLabelText(/^Source/)).toBeInTheDocument();
    expect(step.getByLabelText(/^Function Name/)).toBeInTheDocument();
    // The server refuses a command that names no domain, so its domain is asked here too.
    expect(step.getByLabelText(/^Domain/)).toBeInTheDocument();
    for (const later of ["Description", "Visible To", "Requires approval"]) {
      expect(step.queryByLabelText(new RegExp(`^${later}`))).toBeNull();
    }
    expect(step.queryByText("Arguments")).toBeNull();
  });

  it("selects the command it created and opens its detail in edit, the rest at defaults", async () => {
    render(<CommandsPage />);
    await userEvent.click(await screen.findByTestId("commands-toggle-form"));
    // Choosing the kind starts the form afresh, so the name is typed after it.
    await userEvent.click(within(panel()!).getByLabelText(/^Type/));
    await userEvent.click(await screen.findByRole("option", { name: "Webhook", hidden: true }));
    await userEvent.type(within(panel()!).getByLabelText(/^Name/), "page_ops");
    await userEvent.type(screen.getByTestId("command-url-input"), "https://hooks.example.com/p");
    saveWebhook.mockImplementation(async () => {
      ACTIONS = { ...ACTIONS, webhooks: [...ACTIONS.webhooks, wh("page_ops")] };
    });
    await userEvent.click(screen.getByTestId("commands-save"));
    await waitFor(() => expect(saveWebhook).toHaveBeenCalled());
    expect(saveWebhook.mock.calls[0][0]).toMatchObject({ name: "page_ops" });
    expect(saveWebhook.mock.calls[0][1]).toBeNull(); // a new one: created, never saved over
    // Selected in its list, and its detail open in edit: the whole form now.
    await waitFor(() =>
      expect(screen.getByTestId("commands-split-title")).toHaveTextContent("page_ops"),
    );
    expect(screen.getByTestId("commands-wh-row-page_ops")).toHaveClass("row-selected");
    expect(within(panel()!).getByLabelText(/^Description/)).toBeInTheDocument();

    // Saved again from its open detail, it is updated by its name.
    saveWebhook.mockClear();
    await userEvent.click(screen.getByRole("button", { name: "Save" }));
    await waitFor(() => expect(saveWebhook).toHaveBeenCalled());
    expect(saveWebhook.mock.calls[0][1]).toBe("page_ops");
  });
});
