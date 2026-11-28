// Copyright (c) 2026 Kenneth Stott
// Canary: 6d1f3a85-2c94-4e70-b7a3-8e5c0d9b4f12
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// The navbar's domain selector is on every page the selection narrows, the SPARQL page among them
// (its vocabulary is narrowed by it, as the Cypher page's labels are).

import { describe, expect, it, vi } from "vitest";
import { screen, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { renderWithProviders } from "../../test-utils/render";
import { NavBar } from "../NavBar";

// REQ-2277: unordered, business and internal domains mixed, as /data/domains answers them.
const DOMAINS = ["ops", "pet-store", "meta", "hr", "ontology", "finance"];
vi.mock("../../context/DomainFilterContext", () => ({
  useDomainFilter: () => ({
    domains: DOMAINS,
    checkedDomains: new Set(["hr", "ops"]),
    toggleDomain: () => {},
    domainsEnabled: true,
  }),
}));
vi.mock("../../context/AuthContext", () => ({
  useAuth: () => ({
    displayName: "t",
    email: "t@example.com",
    devMode: true,
    authEnabled: false,
    capabilities: [] as string[],
    demonstrated: [] as string[],
    billing: null,
    activeOrgId: null,
    role: { id: "admin", capabilities: [] },
    selectedRoles: [{ id: "admin", capabilities: [] }],
    loading: false,
  }),
}));

vi.mock("../../context/subnavExtraSlot", () => ({
  useSubnavExtraSlot: () => ({ setNode: () => {} }),
}));
// What the bar embeds is not under test: only its domain selector is.
vi.mock("../RoleSelector", () => ({ RoleSelector: () => null }));
vi.mock("../OrgSwitcher", () => ({ OrgSwitcher: () => null }));
vi.mock("../EnvSwitcher", () => ({ EnvSwitcher: () => null }));
vi.mock("../UserProfileModal", () => ({ UserProfileModal: () => null }));
vi.mock("../WhatsNewModal", () => ({ WhatsNewModal: () => null }));
vi.mock("../CapabilityGate", () => ({ CapabilityGate: () => null }));
vi.mock("../../tour/useTour", () => ({
  useTour: () => ({
    startTour: () => {},
    canResume: false,
    coreSeen: false,
    status: "idle",
    available: false,
  }),
}));

describe("the navbar's domain selector", () => {
  it.each(["/graph", "/sparql"])("is offered on %s", (path) => {
    renderWithProviders(<NavBar />, { initialEntries: [path] });
    expect(screen.getByTestId("navbar-domain-trigger")).toBeInTheDocument();
  });

  // Every Explore tab whose listing, schema or spec the selection narrows offers it: the OData
  // explorer's entity sets among them, which it once narrowed with no selector to undo it.
  it.each(["/schema", "/query", "/sql", "/grpc", "/jsonapi", "/odata", "/openapi"])(
    "is offered on the Explore tab %s",
    (path) => {
      renderWithProviders(<NavBar />, { initialEntries: [path] });
      expect(screen.getByTestId("navbar-domain-trigger")).toBeInTheDocument();
    },
  );

  it("is not offered where the selection narrows nothing", () => {
    renderWithProviders(<NavBar />, { initialEntries: ["/docs"] });
    expect(screen.queryByTestId("navbar-domain-trigger")).toBeNull();
  });

  // REQ-2277 (amended 2026-11-27): business domains first, alphabetically, then a divider and a
  // System label over the internal domains; the count on the trigger is every domain.
  it("lists the business domains alphabetically, then the system domains under a System label", async () => {
    renderWithProviders(<NavBar />, { initialEntries: ["/graph"] });
    const trigger = screen.getByTestId("navbar-domain-trigger");
    expect(trigger).toHaveTextContent("Domains (2/6)");

    await userEvent.click(trigger);
    // Mantine keeps the dropdown display:none in jsdom (no layout), so find it by its contents.
    const systemLabel = await screen.findByTestId("navbar-domain-system-label");
    const menu = systemLabel.closest<HTMLElement>('[role="menu"]');
    if (!menu) throw new Error("the System label is not inside the domains menu");
    const sequence = Array.from(
      menu.querySelectorAll<HTMLElement>(
        '[data-testid^="navbar-domain-item-"], [data-testid^="navbar-domain-system-"]',
      ),
    )
      .map((el) => el.dataset.testid)
      // Mantine repeats the test id on a checkbox's root and its input.
      .filter((id, i, all) => all.indexOf(id) === i);
    expect(sequence).toEqual([
      "navbar-domain-item-finance",
      "navbar-domain-item-hr",
      "navbar-domain-item-pet-store",
      "navbar-domain-system-divider",
      "navbar-domain-system-label",
      "navbar-domain-item-meta",
      "navbar-domain-item-ontology",
      "navbar-domain-item-ops",
    ]);
    expect(within(menu).getByTestId("navbar-domain-system-label")).toHaveTextContent("System");
    expect(trigger).toHaveTextContent("Domains (2/6)");
  });
});
