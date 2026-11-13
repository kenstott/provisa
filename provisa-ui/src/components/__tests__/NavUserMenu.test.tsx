// Copyright (c) 2026 Kenneth Stott
// Canary: 3f6b1c52-8d0e-4a47-b9a1-7c2e5d94f1a8
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// Admin and Docs are in the user menu, below a divider after Profile and What's new, and
// not in the top navigation. Admin is shown only to a caller who holds an admin right.

import { beforeEach, describe, expect, it, vi } from "vitest";
import userEvent from "@testing-library/user-event";
import { render, screen } from "../../test-utils/render";

const auth = vi.hoisted(() => ({ capabilities: [] as string[] }));
vi.mock("../../lib/releaseNotes", () => ({
  RELEASE_NOTES: {
    version: "v0.1.0",
    previous: "v0.0.9",
    date: "2026-11-08",
    features: [],
    changes: [],
  },
}));
vi.mock("../../context/AuthContext", () => ({
  useAuth: () => ({
    displayName: "Ada",
    email: "ada@example.com",
    devMode: false,
    authEnabled: true,
    capabilities: auth.capabilities,
    demonstrated: [],
    loading: false,
    billing: false,
    activeOrgId: "acme",
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

import { NavBar } from "../NavBar";

async function openUserMenu() {
  await userEvent.click(screen.getByTestId("navbar-user-trigger"));
  await screen.findAllByRole("menuitem", { hidden: true });
}

function menuLabels(): string[] {
  return screen.getAllByRole("menuitem", { hidden: true }).map((el) => el.textContent ?? "");
}

function topNavLabels(): string[] {
  const links = document.querySelector(".navbar-links")!;
  return Array.from(links.querySelectorAll("a, button")).map((el) => el.textContent ?? "");
}

describe("Admin and Docs in the user menu", () => {
  beforeEach(() => {
    auth.capabilities = ["org_settings", "observability", "query_development"];
  });

  it("are not in the top navigation", () => {
    render(<NavBar />);
    const labels = topNavLabels();
    expect(labels).not.toContain("Admin");
    expect(labels).not.toContain("Docs");
    expect(document.querySelector('[data-tour="nav-admin"]')).toBeNull();
    expect(document.querySelector('[data-tour="nav-docs"]')).toBeNull();
    expect(screen.queryByTestId("nav-group-admin")).toBeNull();
  });

  it("are in the user menu below a divider after What's new", async () => {
    render(<NavBar />);
    await openUserMenu();
    expect(menuLabels()).toEqual(["Profile", "What's new", "Admin", "Docs", "Logout"]);
    // REQ-1327: Admin is the one way into administration, so there is no Settings entry.
    expect(menuLabels()).not.toContain("Settings");
    const divider = document.querySelector('[data-testid="navbar-menu-divider"]');
    expect(divider).not.toBeNull();
    const whatsNew = screen.getByTestId("navbar-whats-new");
    const admin = screen.getByTestId("navbar-admin");
    expect(
      whatsNew.compareDocumentPosition(divider!) & Node.DOCUMENT_POSITION_FOLLOWING,
    ).toBeTruthy();
    expect(
      divider!.compareDocumentPosition(admin) & Node.DOCUMENT_POSITION_FOLLOWING,
    ).toBeTruthy();
  });

  it("opens the Docs page from the menu", async () => {
    render(<NavBar />);
    await openUserMenu();
    expect(screen.getByTestId("navbar-docs").closest("a")).toHaveAttribute("href", "/docs");
  });

  it("offers Docs but not Admin to a caller who holds no admin right", async () => {
    auth.capabilities = ["query_development"];
    render(<NavBar />);
    await openUserMenu();
    expect(menuLabels()).toEqual(["Profile", "What's new", "Docs", "Logout"]);
    expect(screen.queryByTestId("navbar-admin")).toBeNull();
  });
});
