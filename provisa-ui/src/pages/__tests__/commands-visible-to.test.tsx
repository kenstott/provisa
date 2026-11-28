// Copyright (c) 2026 Kenneth Stott
// Canary: e7ccb460-c7aa-4bc1-938b-7088e803b237
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2277: a field that takes a set of roles is the standard multi-select of the org's roles. A
// command's Visible To and an inline return field's Visible to offer the org's roles, save the
// picked role ids as the list the API takes, and reopen with them picked.

import { beforeEach, describe, it, expect, vi } from "vitest";
import { fireEvent, render, screen, within } from "../../test-utils/render";
import userEvent from "@testing-library/user-event";
import type { TrackedFunction, TrackedWebhook } from "../../api/actions";

const fn = (name: string): TrackedFunction =>
  ({
    name,
    sourceId: "sales-pg",
    schemaName: "public",
    functionName: name,
    returns: "public.orders",
    arguments: [],
    visibleTo: ["admin"],
    domainId: "sales",
    description: "",
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
    inlineReturnType: [{ name: "id", type: "Int", visible_to: ["analyst"] }],
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

vi.mock("../../api/actions", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../api/actions")>()),
  fetchActions: vi.fn(async () => ACTIONS),
  saveFunction: vi.fn().mockResolvedValue(undefined),
  saveWebhook: vi.fn().mockResolvedValue(undefined),
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
    capabilities: ["admin", "table_registration"],
    domainAccess: ["*"],
    activeOrgId: "acme",
  }),
}));

vi.mock("../../hooks/useAdminQueries", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../hooks/useAdminQueries")>()),
  useSources: () => NO_SOURCES,
  useDomains: () => NO_DOMAINS,
}));

import { CommandsPage } from "../CommandsPage";
import { saveFunction, saveWebhook } from "../../api/actions";

const panel = () => screen.getByTestId("commands-split-panel");

/** The field's own list, opened: the form holds other selects, each with a list of its own. */
async function openList(field: HTMLElement): Promise<HTMLElement> {
  await userEvent.click(field);
  return vi.waitFor(() => {
    const id = field.getAttribute("aria-controls");
    const list = id === null ? null : document.getElementById(id);
    if (list === null) throw new Error("the field's list is not open");
    return list;
  });
}

/** The options a role field offers, and which of them are picked. */
async function rolesOffered(field: HTMLElement): Promise<Record<string, boolean>> {
  const list = await openList(field);
  return Object.fromEntries(
    within(list)
      .getAllByRole("option", { hidden: true })
      .map((o) => [o.textContent ?? "", o.getAttribute("aria-selected") === "true"]),
  );
}

async function pick(field: HTMLElement, role: string) {
  const list = await openList(field);
  await userEvent.click(within(list).getByRole("option", { name: role, hidden: true }));
}

async function openEdit(rowTestId: string, name: string) {
  render(<CommandsPage />);
  await userEvent.click(await screen.findByTestId(rowTestId));
  await userEvent.click(screen.getByRole("button", { name: `Edit ${name}` }));
}

beforeEach(() => {
  ACTIONS = { functions: [fn("refund_order")], webhooks: [wh("notify_ops")] };
  vi.mocked(saveFunction).mockClear();
  vi.mocked(saveWebhook).mockClear();
});

describe("a command's Visible To", () => {
  it("is a multi-select of the org's roles, reopened with the command's roles picked", async () => {
    await openEdit("commands-fn-row-refund_order", "refund_order");
    const field = within(panel()).getByRole("textbox", { name: "Visible To" });
    expect(await rolesOffered(field)).toEqual({ admin: true, analyst: false });
  });

  it("saves the picked role ids as the list the API takes", async () => {
    await openEdit("commands-fn-row-refund_order", "refund_order");
    const field = within(panel()).getByRole("textbox", { name: "Visible To" });
    await pick(field, "analyst");
    fireEvent.submit(panel().querySelector("form")!);
    await vi.waitFor(() => expect(saveFunction).toHaveBeenCalledTimes(1));
    expect(vi.mocked(saveFunction).mock.calls[0][0]).toMatchObject({
      visibleTo: ["admin", "analyst"],
    });
  });

  it("is the same multi-select on a webhook", async () => {
    await openEdit("commands-wh-row-notify_ops", "notify_ops");
    const field = within(panel()).getByRole("textbox", { name: "Visible To" });
    expect(await rolesOffered(field)).toEqual({ admin: false, analyst: false });
    await pick(field, "admin");
    fireEvent.submit(panel().querySelector("form")!);
    await vi.waitFor(() => expect(saveWebhook).toHaveBeenCalledTimes(1));
    expect(vi.mocked(saveWebhook).mock.calls[0][0]).toMatchObject({ visibleTo: ["admin"] });
  });
});

describe("an inline return field's Visible to", () => {
  it("offers the org's roles, reopens with the field's roles picked and saves the ids", async () => {
    await openEdit("commands-wh-row-notify_ops", "notify_ops");
    const field = within(screen.getByTestId("inline-field-governance-0")).getByRole("textbox", {
      name: "Visible to",
    });
    expect(await rolesOffered(field)).toEqual({ admin: false, analyst: true });
    await pick(field, "admin");
    fireEvent.submit(panel().querySelector("form")!);
    await vi.waitFor(() => expect(saveWebhook).toHaveBeenCalledTimes(1));
    expect(vi.mocked(saveWebhook).mock.calls[0][0]).toMatchObject({
      inlineReturnType: [{ name: "id", type: "Int", visible_to: ["analyst", "admin"] }],
    });
  });

  it("stores no grant list as null, the command's public shape", async () => {
    await openEdit("commands-wh-row-notify_ops", "notify_ops");
    const field = within(screen.getByTestId("inline-field-governance-0")).getByRole("textbox", {
      name: "Visible to",
    });
    await pick(field, "analyst");
    fireEvent.submit(panel().querySelector("form")!);
    await vi.waitFor(() => expect(saveWebhook).toHaveBeenCalledTimes(1));
    expect(vi.mocked(saveWebhook).mock.calls[0][0]).toMatchObject({
      inlineReturnType: [{ name: "id", type: "Int", visible_to: null }],
    });
  });
});
