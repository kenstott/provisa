// Copyright (c) 2026 Kenneth Stott
// Canary: bf28e0c6-9112-4136-8fde-a8660ae07383
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// Tour anchors (test-utils/tourAnchor): the Govern tour's steps on the Roles and RLS pages: RBAC and
// row rules point at the section navigation (tourAnchors.nav) once their page is ready. The
// role-inheritance step's openers (ensureOpen, then clickBefore) reach the role form and its parent
// select, and Next (clickAfterNext) closes it. "+ Role" became a menu (a granted role, or one derived from an
// attribute) and the step, still clicking "+ Role" for the form, waited forever.

import { describe, it, expect, vi } from "vitest";
import { render, fireEvent, waitFor } from "../test-utils/render";
import { stepPageReady, tourStep, walkTourStep } from "../test-utils/tourAnchor";

vi.mock("../context/DomainFilterContext", () => ({
  useDomainFilter: () => ({
    setDomains: vi.fn(),
    setSelectedDomain: vi.fn(),
    checkedDomains: new Set<string>(),
  }),
}));
vi.mock("../hooks/useAdminQueries", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../hooks/useAdminQueries")>()),
  useRoles: () => ({
    roles: [
      {
        id: "analyst",
        description: "What analyst is for.",
        capabilities: [],
        demonstrated: [],
        domain_access: ["sales"],
        parentRoleId: null,
      },
    ],
    loading: false,
    refetch: vi.fn(),
  }),
  useTables: () => ({ tables: [], loading: false, refetch: vi.fn() }),
  useDomains: () => ({ domains: [], loading: false, refetch: vi.fn() }),
}));
vi.mock("../hooks/useSecurityQueries", () => ({
  useRLSRules: () => ({ rlsRules: [], loading: false, refetch: vi.fn() }),
  useUpsertRole: () => ({ upsertRole: vi.fn(), loading: false }),
  useDeleteRole: () => ({ deleteRole: vi.fn(), loading: false }),
  useUpsertRlsRule: () => ({ upsertRlsRule: vi.fn(), loading: false }),
  useDeleteRlsRule: () => ({ deleteRlsRule: vi.fn(), loading: false }),
  useRevokeRoleGrants: () => ({ revokeFromTable: vi.fn(), revokeFromObject: vi.fn() }),
}));
vi.mock("../api/admin", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../api/admin")>()),
  fetchDerivedRoles: vi.fn(async () => ({ roles: [], source: "deployment" })),
}));

import { SecurityPage } from "../pages/SecurityPage";
import { SecurityRlsPage } from "../pages/security/SecurityRlsPage";

describe("the role-inheritance tour step on the Roles page", () => {
  it("reaches the role form's parent select through its openers, and Next closes the form", async () => {
    const step = tourStep("stepRoleInherit");
    render(<SecurityPage />, { initialEntries: ["/security/roles"] });
    expect(await walkTourStep("stepRoleInherit")).toBeInTheDocument();
    fireEvent.click(document.querySelector<HTMLElement>(step.clickAfterNext!)!);
    await waitFor(() => expect(document.querySelector(step.element)).toBeNull());
  });
});

describe("the Govern tour's navigation steps wait on their pages", () => {
  it("is ready for step18 on the Roles page", async () => {
    render(<SecurityPage />, { initialEntries: [tourStep("step18").route!] });
    expect(await stepPageReady("step18")).toBeInTheDocument();
  });
  it("is ready for step19 on the RLS page", async () => {
    render(<SecurityRlsPage />, { initialEntries: [tourStep("step19").route!] });
    expect(await stepPageReady("step19")).toBeInTheDocument();
  });
});
