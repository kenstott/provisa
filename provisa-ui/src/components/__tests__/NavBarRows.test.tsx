// Copyright (c) 2026 Kenneth Stott
// Canary: fb0b1c19-dfde-4811-b574-95179f5dc351
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2277 (amended 2026-11-27): the top bar is one row where that row fits, measured live; where
// it does not, two rows — the logo, pickers and icon actions, then the page links — with the
// sub-navigation row beneath. Switching rows only re-classes the same elements, so no picker is
// remounted.

import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { act, screen, within } from "@testing-library/react";
import type { ReactNode } from "react";
import { renderWithProviders } from "../../test-utils/render";
import { NavBar } from "../NavBar";
import { NAVBAR_ROWS_HYSTERESIS_PX, nextNavbarRows } from "../navbarRows";

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
// The Env picker counts its mounts: a remount would fetch the environments again.
const envMounts = vi.hoisted(() => ({ count: 0 }));
vi.mock("../EnvSwitcher", async () => {
  const { useEffect } = await import("react");
  return {
    EnvSwitcher: () => {
      useEffect(() => {
        envMounts.count += 1;
      }, []);
      return <button type="button">Env picker</button>;
    },
  };
});
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

// The layout jsdom does not have: the bar is `available` wide, and its single row laid out at its
// natural size (the `navbar-measure` read) is `required` wide.
const widths = { required: 0, available: 0 };
const observers: ResizeObserverCallback[] = [];

class ResizeObserverMock {
  constructor(callback: ResizeObserverCallback) {
    observers.push(callback);
  }
  observe() {}
  unobserve() {}
  disconnect() {}
}

function rect(width: number): DOMRect {
  return { width, height: 50, top: 0, left: 0, right: width, bottom: 50, x: 0, y: 0, toJSON() {} };
}

beforeEach(() => {
  envMounts.count = 0;
  observers.length = 0;
  vi.stubGlobal("ResizeObserver", ResizeObserverMock);
  const real = HTMLElement.prototype.getBoundingClientRect;
  vi.spyOn(HTMLElement.prototype, "getBoundingClientRect").mockImplementation(function (
    this: HTMLElement,
  ) {
    if (!this.classList.contains("navbar")) return real.call(this);
    return rect(this.classList.contains("navbar-measure") ? widths.required : widths.available);
  });
});

afterEach(() => {
  vi.restoreAllMocks();
  vi.unstubAllGlobals();
});

/** The window or a picker changed size: every live observer reports. */
function resized(required: number, available: number) {
  widths.required = required;
  widths.available = available;
  act(() => {
    for (const callback of [...observers]) callback([], {} as ResizeObserver);
  });
}

function renderBar(required: number, available: number) {
  widths.required = required;
  widths.available = available;
  renderWithProviders(<NavBar />, { initialEntries: ["/sparql"] });
  return topBar();
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

describe("the choice of rows", () => {
  it("is one row while the single row fits, and two once it does not", () => {
    expect(nextNavbarRows(false, 1500, 1500)).toBe(false);
    expect(nextNavbarRows(false, 1501, 1500)).toBe(true);
  });

  it("goes back to one row only with the margin to spare", () => {
    expect(nextNavbarRows(true, 1500, 1500)).toBe(true);
    expect(nextNavbarRows(true, 1500, 1500 + NAVBAR_ROWS_HYSTERESIS_PX - 1)).toBe(true);
    expect(nextNavbarRows(true, 1500, 1500 + NAVBAR_ROWS_HYSTERESIS_PX)).toBe(false);
  });
});

describe("the top bar's rows", () => {
  it("is one row where the single row fits: logo, page links, then the pickers", () => {
    const bar = renderBar(1500, 1900);
    expect(bar).not.toHaveClass("navbar-rows");
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

  it("is two rows where it does not fit, from the first render", () => {
    const bar = renderBar(1700, 1280);
    expect(bar).toHaveClass("navbar-rows");
    // App.css places the same three children: the logo and the pickers on row 1, the links on 2.
    const [brand, links, controls] = [...bar.children];
    expect(brand).toHaveClass("navbar-brand");
    expect(controls).toHaveClass("navbar-role");
    const row1 = within(controls as HTMLElement);
    const domains = row1.getByTestId("navbar-domain-trigger");
    const env = row1.getByRole("button", { name: "Env picker" });
    const role = row1.getByRole("button", { name: "Role picker" });
    const user = row1.getByTestId("navbar-user-trigger");
    expect(precedes(domains, env)).toBe(true);
    expect(precedes(env, role)).toBe(true);
    expect(precedes(role, user)).toBe(true);

    // Row 2: every page link, and nothing else.
    expect(links).toHaveClass("navbar-links");
    const row2 = within(links as HTMLElement);
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

  it("follows the measured widths as they change, without remounting a picker", () => {
    const bar = renderBar(1500, 1900);
    expect(bar).not.toHaveClass("navbar-rows");
    const env = within(bar).getByRole("button", { name: "Env picker" });
    const links = bar.querySelector(".navbar-links");
    expect(envMounts.count).toBe(1);

    // The window narrows past the single row's width.
    resized(1500, 1400);
    expect(bar).toHaveClass("navbar-rows");
    // A longer label (another locale, the data-address picker) needs more than the window has.
    resized(2000, 1900);
    expect(bar).toHaveClass("navbar-rows");
    // Within the margin it stays two rows, so it does not flip at the edge.
    resized(1500, 1500 + NAVBAR_ROWS_HYSTERESIS_PX - 1);
    expect(bar).toHaveClass("navbar-rows");
    resized(1500, 1500 + NAVBAR_ROWS_HYSTERESIS_PX);
    expect(bar).not.toHaveClass("navbar-rows");
    // Once one row, it is two again only when the row no longer fits.
    resized(1500, 1500);
    expect(bar).not.toHaveClass("navbar-rows");
    resized(1501, 1500);
    expect(bar).toHaveClass("navbar-rows");

    // The same elements throughout: the Env picker mounted once, so it fetched once.
    expect(within(bar).getByRole("button", { name: "Env picker" })).toBe(env);
    expect(bar.querySelector(".navbar-links")).toBe(links);
    expect(envMounts.count).toBe(1);
  });
});
