// Copyright (c) 2026 Kenneth Stott
// Canary: 906cecc3-0a61-48a6-8d6f-7bf5de5a3bc7
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// "What's new" in the user menu shows the notes of the release the UI was built with, from the
// notes bundled with it: no request is made to show them, and a release with none offers nothing.

import { afterEach, describe, expect, it, vi } from "vitest";
import userEvent from "@testing-library/user-event";
import { render, screen, within } from "../../test-utils/render";
import type { ReleaseNotes } from "../../lib/releaseNotes";

const NOTES: ReleaseNotes = {
  version: "v0.1.0-alpha.479",
  previous: "v0.1.0-alpha.478",
  date: "2026-10-09",
  features: [
    {
      category: "Read Refusals",
      entries: [
        {
          text: "A read that needs a table whose replica could not be built is refused.",
          changes: ["A table whose replica cannot be built refuses the read by name"],
        },
      ],
    },
  ],
  changes: [
    { scope: "ui", entries: ["The table editor opens for a registrar"] },
    { scope: "", entries: ["strawberry-graphql is upgraded"] },
  ],
};

const bundle = vi.hoisted(() => ({ notes: null as ReleaseNotes | null }));
vi.mock("../../lib/releaseNotes", () => ({
  get RELEASE_NOTES() {
    return bundle.notes;
  },
}));
vi.mock("../../context/AuthContext", () => ({
  useAuth: () => ({
    displayName: "Ada",
    email: "ada@example.com",
    devMode: false,
    authEnabled: true,
    capabilities: [],
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

afterEach(() => {
  bundle.notes = null;
  vi.restoreAllMocks();
});

async function openUserMenu() {
  await userEvent.click(screen.getByTestId("navbar-user-trigger"));
}

describe("What's new", () => {
  it("opens the bundled notes of the running release from the user menu, asking the server nothing", async () => {
    bundle.notes = NOTES;
    const fetchSpy = vi.spyOn(globalThis, "fetch");
    render(<NavBar />);
    await openUserMenu();
    const items = (await screen.findAllByRole("menuitem", { hidden: true })).map(
      (el) => el.textContent,
    );
    // Above the divider that sets off Docs, above Logout, the last of the menu.
    expect(items.slice(-3)).toEqual(["What's new", "Docs", "Logout"]);
    const before = fetchSpy.mock.calls.length;
    await userEvent.click(screen.getByTestId("navbar-whats-new"));

    const dialog = await screen.findByRole("dialog");
    expect(dialog).toHaveTextContent("What's new in v0.1.0-alpha.479");
    expect(dialog).toHaveTextContent("Released 2026-10-09");
    const features = within(screen.getByTestId("whats-new-features"));
    expect(features.getByText("Read Refusals")).toBeInTheDocument();
    // A change that cites a completed feature is shown under it.
    expect(
      features.getByText("A table whose replica cannot be built refuses the read by name"),
    ).toBeInTheDocument();
    const changes = within(screen.getByTestId("whats-new-changes"));
    expect(changes.getByText("ui")).toBeInTheDocument();
    // A change with no scope is under the general heading.
    expect(changes.getByText("General")).toBeInTheDocument();
    expect(changes.getByText("strawberry-graphql is upgraded")).toBeInTheDocument();
    expect(fetchSpy.mock.calls.length).toBe(before);
  });

  it("offers nothing for a release the notes leave out", async () => {
    bundle.notes = null;
    render(<NavBar />);
    await openUserMenu();
    expect(screen.queryByTestId("navbar-whats-new")).toBeNull();
    expect(
      (await screen.findAllByRole("menuitem", { hidden: true })).map((el) => el.textContent),
    ).toContain("Logout");
  });
});
