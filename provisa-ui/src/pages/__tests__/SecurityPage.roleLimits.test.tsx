// Copyright (c) 2026 Kenneth Stott
// Canary: ffae9eeb-1b20-4ad6-aefb-82f3562a875c
// Canary: placeholder
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1174: the role form surfaces per-role rate + query-complexity limits and stages them through
// the upsertRole save path (Hasura api_limits parity, editable in the UI).

import { describe, it, expect, vi } from "vitest";
import { render, screen, fireEvent, waitFor, within } from "../../test-utils/render";
import { openNewRole } from "../../test-utils/securityRoles";

const upsertRoleSpy = vi.fn(async () => ({ success: true, message: "" }));

vi.mock("../../context/DomainFilterContext", () => ({
  useDomainFilter: () => ({
    setDomains: vi.fn(),
    setSelectedDomain: vi.fn(),
    checkedDomains: new Set<string>(),
  }),
}));

// Stable: a hook that answered a new array each render would re-run the page's effects.
const LIMIT_ROLES = [
  {
    id: "analyst",
    description: "What analyst is for.",
    capabilities: [],
    demonstrated: [],
    domain_access: ["*"],
    parentRoleId: null,
  },
];

// Spread the real module: vmThreads + fileParallelism:false share one module registry, so a
// replace-everything factory here leaks into other files. In particular a stubbed
// useMaterializeStoreInfo would win over the real Apollo hook that
// TableEditForm.consistency drives through MockedProvider.
vi.mock("../../hooks/useAdminQueries", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../hooks/useAdminQueries")>()),
  useRoles: () => ({ roles: LIMIT_ROLES, loading: false, refetch: vi.fn() }),
  useTables: () => ({ tables: [], loading: false, refetch: vi.fn() }),
  useDomains: () => ({ domains: [], loading: false, refetch: vi.fn() }),
}));

vi.mock("../../hooks/useSecurityQueries", () => ({
  useRLSRules: () => ({ rlsRules: [], loading: false, refetch: vi.fn() }),
  useUpsertRole: () => ({ upsertRole: upsertRoleSpy, loading: false }),
  useDeleteRole: () => ({ deleteRole: vi.fn(), loading: false }),
  useUpsertRlsRule: () => ({ upsertRlsRule: vi.fn(), loading: false }),
  useDeleteRlsRule: () => ({ deleteRlsRule: vi.fn(), loading: false }),
  useRevokeRoleGrants: () => ({ revokeFromTable: vi.fn(), revokeFromObject: vi.fn() }),
}));

vi.mock("../../api/admin", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../api/admin")>()),
  fetchDerivedRoles: vi.fn(async () => ({ roles: [], source: "deployment" })),
}));

import { SecurityPage } from "../SecurityPage";

// A role must list a domain before it can be saved (the server refuses an empty list), so a test
// that saves a new role chooses "All Domains" in the picker first.
// Mantine MultiSelect in jsdom: floating-ui hides the detached dropdown (all rects are 0), so
// the options are found through the input's aria-controls listbox with hidden: true.
async function chooseAllDomains() {
  const picker = screen
    .getAllByLabelText("Domain Access")
    .find((el) => el.tagName === "INPUT") as HTMLElement;
  fireEvent.click(picker);
  await waitFor(() => {
    if (!picker.getAttribute("aria-controls")) throw new Error("dropdown not open");
  });
  const listbox = document.getElementById(picker.getAttribute("aria-controls") as string);
  fireEvent.click(
    within(listbox as HTMLElement).getByRole("option", { name: "All Domains", hidden: true }),
  );
}

function renderPage() {
  return render(<SecurityPage />);
}

// A role's limits are set in its detail: a new role is first created with what it needs to exist.
function editRole(id: string) {
  fireEvent.click(within(screen.getByTestId("roles-list")).getByText(id));
  fireEvent.click(screen.getByTestId(`edit-role-${id}`));
}

// Mantine spreads data-testid onto the underlying <input>, so the testid IS the input element.
const input = (testid: string) => screen.getByTestId(testid) as HTMLInputElement;

describe("SecurityPage — per-role rate & query-complexity limits (REQ-1174)", () => {
  it("asks a new role for no limits: they are set once it exists", async () => {
    renderPage();
    await openNewRole();
    expect(screen.getByTestId("role-id-input")).toBeInTheDocument();
    expect(screen.queryByTestId("role-req-per-sec")).not.toBeInTheDocument();
    expect(screen.queryByTestId("role-max-complexity")).not.toBeInTheDocument();
    expect(screen.queryByTestId("role-max-time-ms")).not.toBeInTheDocument();
    expect(screen.queryByTestId("role-inactivity-days")).not.toBeInTheDocument();
  });

  it("renders the limit fields when a role is edited", () => {
    renderPage();
    editRole("analyst");
    expect(screen.getByTestId("role-req-per-sec")).toBeInTheDocument();
    expect(screen.getByTestId("role-max-complexity")).toBeInTheDocument();
    // REQ-1174: the depth and node limits measured a GraphQL document; one complexity limit
    // measured on the statement replaced them.
    expect(screen.queryByTestId("role-max-depth")).not.toBeInTheDocument();
    expect(screen.queryByTestId("role-max-nodes")).not.toBeInTheDocument();
    expect(screen.getByTestId("role-max-time-ms")).toBeInTheDocument();
  });

  it("stages the entered limits through upsertRole", async () => {
    upsertRoleSpy.mockClear();
    renderPage();
    editRole("analyst");

    fireEvent.change(input("role-req-per-sec"), { target: { value: "5" } });
    fireEvent.change(input("role-max-complexity"), { target: { value: "200" } });
    fireEvent.change(input("role-max-time-ms"), { target: { value: "3000" } });

    fireEvent.click(screen.getByTestId("save-role-analyst"));

    await waitFor(() => expect(upsertRoleSpy).toHaveBeenCalledTimes(1));
    expect(upsertRoleSpy).toHaveBeenCalledWith(
      expect.objectContaining({
        id: "analyst",
        rateLimit: {
          requestsPerSecond: 5,
          maxQueryComplexity: 200,
          maxQueryTimeMs: 3000,
        },
      }),
      false, // a role that exists is updated
    );
  });

  // REQ-2077: a role removes its grants once unused this many days; blank is never.
  it("stages the days a grant may go unused, and blank as never", async () => {
    upsertRoleSpy.mockClear();
    renderPage();
    editRole("analyst");
    expect(input("role-inactivity-days").value).toBe("");
    fireEvent.change(input("role-inactivity-days"), { target: { value: "30" } });
    fireEvent.click(screen.getByTestId("save-role-analyst"));
    await waitFor(() => expect(upsertRoleSpy).toHaveBeenCalledTimes(1));
    expect(upsertRoleSpy).toHaveBeenCalledWith(
      expect.objectContaining({ id: "analyst", inactivityDays: 30 }),
      false,
    );
  });

  it("sends inactivityDays=null when no day count is entered", async () => {
    upsertRoleSpy.mockClear();
    renderPage();
    editRole("analyst");
    fireEvent.click(screen.getByTestId("save-role-analyst"));
    await waitFor(() => expect(upsertRoleSpy).toHaveBeenCalledTimes(1));
    expect(upsertRoleSpy).toHaveBeenCalledWith(
      expect.objectContaining({ inactivityDays: null }),
      false,
    );
  });

  it("sends rateLimit=null when no limit is entered", async () => {
    upsertRoleSpy.mockClear();
    renderPage();
    await openNewRole();
    fireEvent.change(input("role-id-input"), { target: { value: "guest" } });
    await chooseAllDomains();
    fireEvent.click(screen.getByTestId("save-role"));
    await waitFor(() => expect(upsertRoleSpy).toHaveBeenCalledTimes(1));
    expect(upsertRoleSpy).toHaveBeenCalledWith(
      expect.objectContaining({ rateLimit: null }),
      true, // a new role is created
    );
  });
});
