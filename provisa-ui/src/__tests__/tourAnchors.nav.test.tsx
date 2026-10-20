// Copyright (c) 2026 Kenneth Stott
// Canary: d9d20bd7-eae5-4a5d-9f99-5b43f5f429ba
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// Tour anchors (test-utils/tourAnchor): the steps that point at the navigation, which the app's
// shell paints on every page (NavBar), found there on the page each step runs on for a viewer
// holding the step's right.

import { describe, it, expect, vi } from "vitest";
import { render } from "../test-utils/render";
import { stepRoute, tourStep } from "../test-utils/tourAnchor";

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
      "observability",
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
    // The tour button (step0, step14) is drawn only while a tour can run.
    available: true,
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
  for (const key of [
    "step0",
    "step1",
    "step4",
    "stepQuery",
    "step6",
    "step7",
    "step8",
    "step9",
    "step10",
    "step11",
    "step12",
    "stepPython",
    "stepNotebook",
    "step14",
    "step18",
    "step19",
    "step20",
    "step21",
    "step23",
  ]) {
    it(`finds ${key}'s anchor on its route`, async () => {
      const step = tourStep(key);
      render(<NavBar />, { initialEntries: [stepRoute(key)] });
      expect(document.querySelector(step.element)).toBeInTheDocument();
    });
  }
});
