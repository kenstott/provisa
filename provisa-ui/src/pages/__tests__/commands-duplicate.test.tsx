// Copyright (c) 2026 Kenneth Stott
// Canary: 4dbce8cf-b196-4667-84b0-e781c7750fc1
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// Creating a command: "+ Command" opens an empty form whatever command was selected or edited
// before it, and Duplicate (the panel head, beside Edit) opens the same create form filled from
// the selected command under a new name: its definition and language bindings, not its grants
// (rule: components/list/__tests__/panelDuplicateGuard.test.ts).

import { beforeEach, describe, it, expect, vi } from "vitest";
import { fireEvent, render, screen, within } from "../../test-utils/render";
import userEvent from "@testing-library/user-event";
import { expectHoldsNoneOf } from "../../test-utils/createForm";
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
    languageBindings: { python: { name: "refund_order_py" } },
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
import { saveFunction } from "../../api/actions";

const panel = () => screen.getByTestId("commands-split-panel");
const COMMAND_VALUES = ["refund_order", "sales-pg", "Refunds one order"];

beforeEach(() => {
  ACTIONS = { functions: [fn("refund_order"), fn("close_period")], webhooks: [wh("notify_ops")] };
  saveWebhook.mockReset().mockResolvedValue(undefined);
  vi.mocked(saveFunction).mockClear();
});

describe("+ Command opens an empty form", () => {
  it("holds none of the values of the command selected before it", async () => {
    render(<CommandsPage />);
    await userEvent.click(await screen.findByTestId("commands-fn-row-refund_order"));
    await userEvent.click(screen.getByTestId("commands-toggle-form"));
    expectHoldsNoneOf(panel(), COMMAND_VALUES);
  });

  it("holds none of them after the command was edited and its row clicked", async () => {
    render(<CommandsPage />);
    const row = await screen.findByTestId("commands-fn-row-refund_order");
    await userEvent.click(row);
    await userEvent.click(screen.getByRole("button", { name: "Edit refund_order" }));
    // The row of the command being edited closes its panel: the edit ends with it.
    await userEvent.click(row);
    await userEvent.click(await screen.findByTestId("commands-toggle-form"));
    expectHoldsNoneOf(panel(), COMMAND_VALUES);
  });

  it("holds none of a webhook's values after it was edited and its row clicked", async () => {
    render(<CommandsPage />);
    const row = await screen.findByTestId("commands-wh-row-notify_ops");
    await userEvent.click(row);
    await userEvent.click(screen.getByRole("button", { name: "Edit notify_ops" }));
    await userEvent.click(row);
    await userEvent.click(await screen.findByTestId("commands-toggle-form"));
    expectHoldsNoneOf(panel(), ["notify_ops", "https://hooks.example.com/ops"]);
  });
});

describe("Duplicate, in a command's panel head", () => {
  it("is beside Edit and Delete, named Duplicate", async () => {
    render(<CommandsPage />);
    await userEvent.click(await screen.findByTestId("commands-fn-row-refund_order"));
    expect(within(panel()).getByRole("button", { name: "Duplicate" })).toBeInTheDocument();
  });

  it("opens the create form filled from the command, under a new name", async () => {
    render(<CommandsPage />);
    await userEvent.click(await screen.findByTestId("commands-fn-row-refund_order"));
    await userEvent.click(within(panel()).getByRole("button", { name: "Duplicate" }));
    expect(screen.getByTestId("commands-split-title")).toHaveTextContent("New command");
    expect(within(panel()).getByLabelText(/^Name/)).toHaveValue("refund_order-copy");
    expect(within(panel()).getByLabelText(/^Function Name/)).toHaveValue("refund_order");
  });

  it("creates the copy with the definition and language bindings, and not the grants", async () => {
    render(<CommandsPage />);
    await userEvent.click(await screen.findByTestId("commands-fn-row-refund_order"));
    await userEvent.click(within(panel()).getByRole("button", { name: "Duplicate" }));
    fireEvent.submit(panel().querySelector("form")!);
    await vi.waitFor(() => expect(saveFunction).toHaveBeenCalledTimes(1));
    const [saved, existing] = vi.mocked(saveFunction).mock.calls[0];
    expect(existing).toBeNull(); // a create: the server refuses a name that exists
    expect(saved).toMatchObject({
      name: "refund_order-copy",
      sourceId: "sales-pg",
      functionName: "refund_order",
      description: "Refunds one order",
      languageBindings: { python: { name: "refund_order_py" } },
      visibleTo: [],
    });
  });

  it("copies a webhook the same way", async () => {
    render(<CommandsPage />);
    await userEvent.click(await screen.findByTestId("commands-wh-row-notify_ops"));
    await userEvent.click(within(panel()).getByRole("button", { name: "Duplicate" }));
    fireEvent.submit(panel().querySelector("form")!);
    await vi.waitFor(() => expect(saveWebhook).toHaveBeenCalledTimes(1));
    const [saved, existing] = saveWebhook.mock.calls[0];
    expect(existing).toBeNull();
    expect(saved).toMatchObject({ name: "notify_ops-copy", url: "https://hooks.example.com/ops" });
  });
});
