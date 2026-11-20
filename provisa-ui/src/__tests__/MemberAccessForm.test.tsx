// Copyright (c) 2026 Kenneth Stott
// Canary: a52fe4ad-7453-4670-badd-4ba5a96e8656
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2155: adding a member to an environment takes a list of roles and entitlements in one step.

import { describe, it, expect, vi, beforeEach } from "vitest";
import userEvent from "@testing-library/user-event";
import { render, screen, fireEvent, waitFor, within } from "../test-utils/render";
import i18n from "../i18n";
import { MemberAccessForm } from "../components/admin/MemberAccessForm";

const t = i18n.getFixedT("en");

const grantSpy = vi.fn(async (..._a: unknown[]) => ({ assignments: [] }));
let assigns = true;

vi.mock("../api/admin", () => ({
  grantMemberAccess: (...a: unknown[]) => grantSpy(...a),
}));

vi.mock("../hooks/useCapability", () => ({
  useCapability: (cap: string) => cap === "assign_entitlements" && assigns,
}));

const TABLES = { kind: "tables", semantic: true, depth: 2 };
vi.mock("../components/entitlements/kinds", () => ({
  useEntitlementKinds: () => ({ kinds: [TABLES], names: null }),
  kindInfo: (_kinds: unknown, kind: string) => (kind === "tables" ? TABLES : undefined),
}));

vi.mock("../components/entitlements/EntitlementPicker", () => ({
  EntitlementPicker: ({
    onChange,
    testId,
  }: {
    onChange: (v: string[]) => void;
    testId: string;
  }) => (
    <button type="button" data-testid={testId} onClick={() => onChange(["finance.revenue"])}>
      pick
    </button>
  ),
}));

const DOMAINS = [
  { value: "*", label: "* (all domains)" },
  { value: "sales", label: "sales" },
];

// Mantine renders each dropdown detached and floating-ui gives its options a zero rect in jsdom:
// an option is found in the listbox the opened input names (aria-controls), visible or not.
async function pick(input: HTMLElement, option: string) {
  await userEvent.click(input);
  const listbox = await waitFor(() => {
    const id = input.getAttribute("aria-controls");
    const found = id === null ? null : document.getElementById(id);
    if (found === null) throw new Error("dropdown not open");
    return found;
  });
  await userEvent.click(within(listbox).getByRole("option", { name: option, hidden: true }));
}

async function choose(label: string, option: string) {
  await pick(screen.getByRole("textbox", { name: label }), option);
}

function renderForm(onSaved = vi.fn()) {
  render(
    <MemberAccessForm
      userId="u1"
      allRoles={["analyst", "data_writer", "viewer"]}
      domainOptions={DOMAINS}
      onSaved={onSaved}
      onError={vi.fn()}
    />,
  );
  return onSaved;
}

describe("MemberAccessForm", () => {
  beforeEach(() => {
    grantSpy.mockClear();
    assigns = true;
  });

  it("grants several roles and an entitlement list in one save", async () => {
    const onSaved = renderForm();
    await choose(t("localUsers.rolesLabel"), "analyst");
    await choose(t("localUsers.rolesLabel"), "data_writer");
    fireEvent.click(screen.getByTestId("member-access-add-list"));
    await pick(screen.getByTestId("member-access-list-0-role"), "data_writer");
    await pick(screen.getByTestId("member-access-list-0-kind"), t("entitlements.kind.tables"));
    fireEvent.click(await screen.findByTestId("member-access-list-0-picker"));
    // A list records a reason: Save waits for one.
    expect(screen.getByTestId("member-access-save")).toBeDisabled();
    await userEvent.type(screen.getByTestId("member-access-reason"), "joining finance");
    fireEvent.click(screen.getByTestId("member-access-save"));

    await waitFor(() =>
      expect(grantSpy).toHaveBeenCalledWith("u1", {
        role_ids: ["analyst", "data_writer"],
        domain_id: "*",
        expires_at: null,
        entitlements: [
          { role_id: "data_writer", kind: "tables", identifiers: ["finance.revenue"] },
        ],
        reason: "joining finance",
      }),
    );
    expect(grantSpy).toHaveBeenCalledTimes(1);
    await waitFor(() => expect(onSaved).toHaveBeenCalled());
  });

  it("saves roles alone without asking for a reason", async () => {
    renderForm();
    expect(screen.getByTestId("member-access-save")).toBeDisabled();
    await choose(t("localUsers.rolesLabel"), "viewer");
    expect(screen.queryByTestId("member-access-reason")).not.toBeInTheDocument();
    fireEvent.click(screen.getByTestId("member-access-save"));
    await waitFor(() =>
      expect(grantSpy).toHaveBeenCalledWith("u1", {
        role_ids: ["viewer"],
        domain_id: "*",
        expires_at: null,
        entitlements: [],
        reason: "",
      }),
    );
  });

  it("offers no entitlement lists to a caller without assign_entitlements", async () => {
    assigns = false;
    renderForm();
    await choose(t("localUsers.rolesLabel"), "viewer");
    expect(screen.queryByTestId("member-access-add-list")).not.toBeInTheDocument();
  });
});
