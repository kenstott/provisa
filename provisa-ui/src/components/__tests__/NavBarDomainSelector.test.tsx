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
import { screen } from "@testing-library/react";
import { renderWithProviders } from "../../test-utils/render";
import { NavBar } from "../NavBar";

vi.mock("../../context/DomainFilterContext", () => ({
  useDomainFilter: () => ({
    domains: ["hr", "ops"],
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

  it("is not offered where the selection narrows nothing", () => {
    renderWithProviders(<NavBar />, { initialEntries: ["/docs"] });
    expect(screen.queryByTestId("navbar-domain-trigger")).toBeNull();
  });
});
