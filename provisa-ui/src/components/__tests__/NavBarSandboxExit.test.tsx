// Copyright (c) 2026 Kenneth Stott
// Canary: 5e81c0d3-9a27-4b6f-8c14-2fd07a6b93e1
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1931: leaving the Try it Out sandbox. Logging out asks first, and both of the dialog's
// buttons delete the sandbox account and sign out. "Create Organization" does the same as Log Out:
// an organization is created by a sign-in with no membership, which lands on organization setup,
// and the sandbox account must be gone for the next sign-in to have none.

import { beforeEach, describe, expect, it, vi } from "vitest";
import userEvent from "@testing-library/user-event";
import { Route, Routes, useLocation } from "react-router-dom";
import { render, screen, waitFor } from "../../test-utils/render";

const calls = vi.hoisted(() => ({ order: [] as string[] }));
const auth = vi.hoisted(() => ({ activeOrgId: "sandbox", capabilities: ["query_development"] }));

vi.mock("../../lib/session", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../lib/session")>()),
  signOut: vi.fn(async () => {
    calls.order.push("signOut");
  }),
}));
vi.mock("../../api/admin", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../api/admin")>()),
  deleteSandboxAccount: vi.fn(async () => {
    calls.order.push("deleteSandboxAccount");
  }),
}));
vi.mock("../../context/AuthContext", () => ({
  useAuth: () => ({
    displayName: "Visitor",
    email: "visitor@example.com",
    devMode: false,
    authEnabled: true,
    capabilities: auth.capabilities,
    demonstrated: [],
    loading: false,
    billing: false,
    activeOrgId: auth.activeOrgId,
  }),
}));
vi.mock("../../context/DomainFilterContext", () => ({
  useDomainFilter: () => ({
    domains: [],
    checkedDomains: new Set(),
    toggleDomain: vi.fn(),
    domainsEnabled: false,
  }),
}));
vi.mock("../../tour/useTour", () => ({
  useTour: () => ({
    startTour: vi.fn(),
    canResume: false,
    coreSeen: false,
    status: null,
    available: false,
  }),
}));
vi.mock("../../context/subnavExtraSlot", () => ({
  useSubnavExtraSlot: () => ({ setNode: vi.fn() }),
}));
vi.mock("../OrgSwitcher", () => ({ OrgSwitcher: () => null }));
vi.mock("../EnvSwitcher", () => ({ EnvSwitcher: () => null }));
vi.mock("../RoleSelector", () => ({ RoleSelector: () => null }));
vi.mock("../DataAddressSwitcher", () => ({ DataAddressSwitcher: () => null }));

import { NavBar } from "../NavBar";

function Where() {
  return <div data-testid="where">{useLocation().pathname}</div>;
}

function renderBar() {
  render(
    <>
      <NavBar />
      <Routes>
        <Route path="*" element={<Where />} />
      </Routes>
    </>,
    { initialEntries: ["/admin/ontology"] },
  );
}

async function logOut() {
  await userEvent.click(screen.getByTestId("navbar-user-trigger"));
  await userEvent.click(await screen.findByRole("menuitem", { name: "Logout", hidden: true }));
}

describe("leaving the sandbox", () => {
  beforeEach(() => {
    calls.order = [];
    auth.activeOrgId = "sandbox";
  });

  it("Create Organization deletes the sandbox account and signs out, as Log Out does", async () => {
    renderBar();
    await logOut();
    await userEvent.click(await screen.findByTestId("sandbox-exit-create-org"));

    await waitFor(() => expect(calls.order).toEqual(["deleteSandboxAccount", "signOut"]));
    // It no longer opens the organizations administration page, which a visitor cannot open.
    expect(screen.getByTestId("where")).toHaveTextContent("/admin/ontology");
  });

  it("Log Out deletes the sandbox account and signs out", async () => {
    renderBar();
    await logOut();
    await userEvent.click(await screen.findByTestId("sandbox-exit-logout"));

    await waitFor(() => expect(calls.order).toEqual(["deleteSandboxAccount", "signOut"]));
  });

  it("signs a member of any other org out without the dialog or a deletion", async () => {
    auth.activeOrgId = "acme";
    renderBar();
    await logOut();

    await waitFor(() => expect(calls.order).toEqual(["signOut"]));
    expect(screen.queryByTestId("sandbox-exit-create-org")).toBeNull();
  });
});
