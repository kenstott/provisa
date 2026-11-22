// Copyright (c) 2026 Kenneth Stott
// Canary: 7c2e94a1-3b5d-4f08-a6e2-d18f0b9c4e57
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

/**
 * REQ-1327, REQ-1634, REQ-1560: what each standard role is shown in the navigation bar, and the
 * page the app opens on for it.
 *
 * The roles' rights come from `provisa/core/standard_roles.yaml` (own and every ancestor's, as the
 * DEFAULT_ADMIN_ROLE test resolves them), so the table below states the outcome and the yaml stays
 * the one source of what each role holds. The bar is the rendered NavBar, not a copy of its gates.
 *
 * - REQ-1634: Data Products is shown to every holder of data_product_read, whether or not the
 *   Model group is drawn.
 * - REQ-1327 (amended 2026-11-14): `/` opens the first navigation page the acting role holds the
 *   right for, as the Admin entry chooses; no role lands on a refusal. Requests and My Secrets
 *   open to every member, so every role with an org membership is offered Admin.
 */

import { readFileSync } from "node:fs";
import { join } from "node:path";

import { load } from "js-yaml";
import userEvent from "@testing-library/user-event";
import { Route, Routes, useLocation } from "react-router-dom";
import { beforeEach, describe, expect, it, vi } from "vitest";

import { render, screen, waitFor } from "../test-utils/render";

type StandardRole = { parents: string[]; capabilities: string[] };

function standardRoles(): Record<string, StandardRole> {
  const text = readFileSync(join(__dirname, "../../../provisa/core/standard_roles.yaml"), "utf8");
  return load(text) as Record<string, StandardRole>;
}

/** A role's own capabilities and every ancestor's (REQ-2155). */
function effective(id: string): string[] {
  const roles = standardRoles();
  const held = new Set<string>();
  const walk = (r: string) => {
    roles[r].capabilities.forEach((c) => held.add(c));
    roles[r].parents.forEach(walk);
  };
  walk(id);
  return [...held];
}

/** REQ-1602: the sandbox is shown what org_admin holds and it does not, but user_management
 * (provisa.core.db.sandbox_demonstrated). */
function demonstrated(id: string): string[] {
  if (id !== "sandbox") return [];
  const sandbox = new Set(effective("sandbox"));
  return effective("org_admin").filter((c) => !sandbox.has(c) && c !== "user_management");
}

const auth = vi.hoisted(() => ({ capabilities: [] as string[], demonstrated: [] as string[] }));
vi.mock("../context/AuthContext", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../context/AuthContext")>()),
  useAuth: () => ({
    displayName: "Ada",
    email: "ada@example.com",
    devMode: false,
    authEnabled: true,
    capabilities: auth.capabilities,
    demonstrated: auth.demonstrated,
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
vi.mock("../components/DataAddressSwitcher", () => ({ DataAddressSwitcher: () => null }));

import { NavBar } from "../components/NavBar";
import { HomeEntry } from "../App";

// The bar's labels, in English.
const SOURCES = "Sources";
const TABLES = "Tables";
const RELATIONSHIPS = "Relationships";
// REQ-2259: the Context group stands where the Ontology and Library links stood.
const CONTEXT = "Context";
const MODEL = "Derived";
const PRODUCTS = "Products";
const SECURITY = "Security";
const EXPLORE = "Explore";

const EVERY_ENTRY = [
  SOURCES,
  TABLES,
  RELATIONSHIPS,
  CONTEXT,
  MODEL,
  PRODUCTS,
  SECURITY,
  EXPLORE,
];
const READER = [CONTEXT, PRODUCTS];
// REQ-1327 (amended 2026-11-14): a holder of command_execution or create_view is shown Derived.
const COMMAND_READER = [CONTEXT, MODEL, PRODUCTS, EXPLORE];
const BUILDER = [RELATIONSHIPS, CONTEXT, MODEL, PRODUCTS, EXPLORE];
const GOVERNOR = [TABLES, CONTEXT, MODEL, PRODUCTS, SECURITY];

// role → [the bar's entries in order, whether the user menu offers Admin, the landing page]
const EXPECTED: Record<string, [string[], boolean, string]> = {
  viewer: [READER, true, "/context/health"],
  analyst: [COMMAND_READER, true, "/schema"],
  data_writer: [COMMAND_READER, true, "/schema"],
  developer: [BUILDER, true, "/schema"],
  modeler: [BUILDER, true, "/schema"],
  data_product_owner: [READER, true, "/context/health"],
  security_admin: [GOVERNOR, true, "/tables"],
  data_steward: [
    [TABLES, RELATIONSHIPS, CONTEXT, MODEL, PRODUCTS, SECURITY, EXPLORE],
    true,
    "/schema",
  ],
  dq_steward: [READER, true, "/context/health"],
  source_admin: [[SOURCES, TABLES, CONTEXT, MODEL, PRODUCTS], true, "/sources"],
  member_admin: [[CONTEXT, PRODUCTS, SECURITY], true, "/context/health"],
  org_operator: [READER, true, "/context/health"],
  org_admin: [EVERY_ENTRY, true, "/schema"],
  sandbox: [EVERY_ENTRY, true, "/schema"],
  platform_admin: [[], true, "/admin/orgs"],
};

function topNavLabels(): string[] {
  const links = document.querySelector(".navbar-links")!;
  return Array.from(links.querySelectorAll("a, button")).map((el) => el.textContent ?? "");
}

function Landed() {
  return <div data-testid="landed">{useLocation().pathname}</div>;
}

describe("the standard roles' navigation", () => {
  beforeEach(() => {
    // The remembered subnav item is a browser preference; each role starts without one.
    localStorage.clear();
  });

  it("covers every standard role", () => {
    expect(Object.keys(EXPECTED).sort()).toEqual(Object.keys(standardRoles()).sort());
  });

  describe.each(Object.entries(EXPECTED))("%s", (role, [entries, admin, landing]) => {
    beforeEach(() => {
      auth.capabilities = effective(role);
      auth.demonstrated = demonstrated(role);
    });

    it("is shown exactly the entries it holds the right for, in bar order", async () => {
      render(<NavBar />);
      expect(topNavLabels()).toEqual(entries);
      await userEvent.click(screen.getByTestId("navbar-user-trigger"));
      await screen.findAllByRole("menuitem", { hidden: true });
      if (admin) expect(screen.getByTestId("navbar-admin")).toBeInTheDocument();
      else expect(screen.queryByTestId("navbar-admin")).toBeNull();
    });

    it(`opens the app on ${landing}`, async () => {
      render(
        <Routes>
          <Route path="/" element={<HomeEntry />} />
          <Route path="*" element={<Landed />} />
        </Routes>,
        { initialEntries: ["/"] },
      );
      await waitFor(() => expect(screen.getByTestId("landed")).toHaveTextContent(landing));
    });

    it("lands on Explore when the bar shows it, else on the first entry the bar shows it", () => {
      // The landing page is Explore for a role that has it, else the bar's first entry, or Admin's
      // first page where the bar has none.
      const bar: Record<string, string> = {
        [SOURCES]: "/sources",
        [TABLES]: "/tables",
        [RELATIONSHIPS]: "/relationships",
        [CONTEXT]: "/context/health",
      };
      if (entries.includes(EXPLORE)) expect(landing).toBe("/schema");
      else if (entries.length > 0) expect(landing).toBe(bar[entries[0]]);
      else expect(admin).toBe(true);
    });
  });
});
