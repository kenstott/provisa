// Copyright (c) 2026 Kenneth Stott
// Canary: fb0b1c19-dfde-4811-b574-95179f5dc351
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2277 (amended 2026-11-27): the top bar is one row where it fits; below that width it is two
// rows — the logo, pickers and icon actions, then the page links — with the sub-navigation row
// beneath. The document order follows the rows, so reading and tab order match what is seen.

import { afterEach, describe, expect, it, vi } from "vitest";
import { screen, within } from "@testing-library/react";
import type { ReactNode } from "react";
import { renderWithProviders } from "../../test-utils/render";
import { NavBar, NAVBAR_ROWS_QUERY } from "../NavBar";

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
    devMode: false,
    authEnabled: false,
    capabilities: [] as string[],
    demonstrated: [] as string[],
    billing: null,
    activeOrgId: null,
    loading: false,
  }),
}));
vi.mock("../../context/subnavExtraSlot", () => ({
  useSubnavExtraSlot: () => ({ setNode: () => {} }),
}));
// The pickers stand in as plain buttons: where they sit is under test, not what they do.
vi.mock("../RoleSelector", () => ({
  RoleSelector: () => <button type="button">Role picker</button>,
}));
vi.mock("../EnvSwitcher", () => ({
  EnvSwitcher: () => <button type="button">Env picker</button>,
}));
vi.mock("../OrgSwitcher", () => ({ OrgSwitcher: () => null }));
vi.mock("../DataAddressSwitcher", () => ({ DataAddressSwitcher: () => null }));
vi.mock("../UserProfileModal", () => ({ UserProfileModal: () => null }));
vi.mock("../WhatsNewModal", () => ({ WhatsNewModal: () => null }));
vi.mock("../CapabilityGate", () => ({
  CapabilityGate: ({ children }: { children: ReactNode }) => <>{children}</>,
}));
vi.mock("../navGroups", async (importOriginal) => {
  const real = await importOriginal<typeof import("../navGroups")>();
  // Every group is offered: the rights that open each are not under test.
  return { ...real, barEntryTarget: () => "/" };
});
vi.mock("../../tour/useTour", () => ({
  useTour: () => ({
    startTour: () => {},
    canResume: false,
    coreSeen: false,
    status: { kind: "idle" },
    available: false,
  }),
}));

function windowIs(narrow: boolean) {
  vi.spyOn(window, "matchMedia").mockImplementation(
    (query: string) =>
      ({
        matches: narrow && query === NAVBAR_ROWS_QUERY,
        media: query,
        onchange: null,
        addListener: () => {},
        removeListener: () => {},
        addEventListener: () => {},
        removeEventListener: () => {},
        dispatchEvent: () => false,
      }) as MediaQueryList,
  );
}

/** The top bar's landmark: the first navigation, the sub-navigation being the second. */
function topBar(): HTMLElement {
  const [bar, subnav] = screen.getAllByRole("navigation");
  expect(subnav).toHaveClass("subnav");
  // The sub-navigation row follows the top bar directly.
  expect(bar.nextElementSibling).toBe(subnav);
  return bar;
}

function precedes(a: Element, b: Element): boolean {
  return (a.compareDocumentPosition(b) & Node.DOCUMENT_POSITION_FOLLOWING) !== 0;
}

afterEach(() => {
  vi.restoreAllMocks();
});

describe("the top bar's rows", () => {
  it("is one row in a window wide enough: logo, page links, then the pickers", () => {
    windowIs(false);
    renderWithProviders(<NavBar />, { initialEntries: ["/sparql"] });
    const bar = topBar();
    expect(bar).not.toHaveClass("navbar-rows");
    expect(within(bar).queryByTestId("navbar-row-context")).toBeNull();
    const home = within(bar).getByRole("link", { name: "Provisa home" });
    const sources = within(bar).getByRole("link", { name: "Sources" });
    const explore = within(bar).getByRole("button", { name: "Explore" });
    const env = within(bar).getByRole("button", { name: "Env picker" });
    const role = within(bar).getByRole("button", { name: "Role picker" });
    expect(precedes(home, sources)).toBe(true);
    expect(precedes(explore, env)).toBe(true);
    expect(precedes(env, role)).toBe(true);
    // The logo, the links and the pickers are the bar's own children: one row.
    expect([...bar.children].map((c) => c.className)).toEqual([
      "navbar-brand",
      "navbar-links",
      "navbar-role",
    ]);
  });

  it("is two rows below that width: logo, pickers and icon actions, then the page links", () => {
    windowIs(true);
    renderWithProviders(<NavBar />, { initialEntries: ["/sparql"] });
    const bar = topBar();
    expect(bar).toHaveClass("navbar-rows");
    const [first, second] = [...bar.children];
    expect(bar.children).toHaveLength(2);

    // Row 1: the logo, the context pickers (Domains, Env, Role) and the icon actions.
    expect(first).toBe(screen.getByTestId("navbar-row-context"));
    const row1 = within(first as HTMLElement);
    const home = row1.getByRole("link", { name: "Provisa home" });
    const domains = row1.getByTestId("navbar-domain-trigger");
    const env = row1.getByRole("button", { name: "Env picker" });
    const role = row1.getByRole("button", { name: "Role picker" });
    const user = row1.getByTestId("navbar-user-trigger");
    expect(precedes(home, domains)).toBe(true);
    expect(precedes(domains, env)).toBe(true);
    expect(precedes(env, role)).toBe(true);
    expect(precedes(role, user)).toBe(true);
    expect(row1.queryByRole("link", { name: "Sources" })).toBeNull();

    // Row 2: every page link, and nothing else.
    expect(second).toHaveClass("navbar-links");
    const row2 = within(second as HTMLElement);
    expect(row2.getAllByRole("link").map((a) => a.textContent)).toEqual([
      "Sources",
      "Tables",
      "Relationships",
      "Products",
    ]);
    expect(row2.getAllByRole("button").map((b) => b.textContent)).toEqual([
      "Derived",
      "Context",
      "Security",
      "Explore",
    ]);
    expect(row2.queryByRole("button", { name: "Env picker" })).toBeNull();
  });
});
