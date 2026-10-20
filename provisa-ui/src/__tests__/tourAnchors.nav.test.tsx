// Copyright (c) 2026 Kenneth Stott
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// Tour anchors (test-utils/tourAnchor): the steps that point at the navigation, which the app's
// shell paints on every page (NavBar), found there on each step's own route for a viewer holding
// the step's right.

import { describe, it, expect, vi } from "vitest";
import { render } from "../test-utils/render";
import { tourStep } from "../test-utils/tourAnchor";

vi.mock("../context/AuthContext", () => ({
  useAuth: () => ({
    displayName: "Ada",
    email: "ada@example.com",
    devMode: false,
    authEnabled: true,
    capabilities: [
      "source_registration",
      "table_registration",
      "access_config",
      "query_development",
    ],
    demonstrated: [],
    loading: false,
    billing: false,
    activeOrgId: "acme",
  }),
}));
vi.mock("../context/DomainFilterContext", () => ({
  useDomainFilter: () => ({
    domains: [],
    checkedDomains: new Set(),
    toggleDomain: vi.fn(),
    domainsEnabled: false,
  }),
}));
vi.mock("../tour/useTour", () => ({
  useTour: () => ({
    startTour: vi.fn(),
    canResume: false,
    coreSeen: false,
    status: null,
    available: false,
  }),
}));
vi.mock("../context/subnavExtraSlot", () => ({
  useSubnavExtraSlot: () => ({ setNode: vi.fn() }),
}));
vi.mock("../components/OrgSwitcher", () => ({ OrgSwitcher: () => null }));
vi.mock("../components/EnvSwitcher", () => ({ EnvSwitcher: () => null }));
vi.mock("../components/RoleSelector", () => ({ RoleSelector: () => null }));

import { NavBar } from "../components/NavBar";

describe("tour steps that point at the navigation", () => {
  for (const key of ["step1", "step4", "step18"]) {
    it(`finds ${key}'s anchor on its route`, async () => {
      const step = tourStep(key);
      render(<NavBar />, { initialEntries: [step.route!] });
      expect(document.querySelector(step.element)).toBeInTheDocument();
    });
  }
});
