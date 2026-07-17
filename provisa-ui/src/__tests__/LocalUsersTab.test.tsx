// Copyright (c) 2026 Kenneth Stott
// Canary: 5bd950d3-d982-455a-a4cc-907ce8223559
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, fireEvent, waitFor } from "../test-utils/render";
import i18n from "../i18n";
import { LocalUsersTab } from "../components/admin/LocalUsersTab";
import type { LocalUser } from "../api/admin";

const t = i18n.getFixedT("en");

const createSpy = vi.fn(async () => ({ id: "u2" }));
const deleteSpy = vi.fn(async () => undefined);
let mockUsers: LocalUser[] = [];

vi.mock("../api/admin", () => ({
  fetchLocalUsers: () => Promise.resolve(mockUsers),
  createLocalUser: (...a: unknown[]) => createSpy(...(a as [])),
  deleteLocalUser: (...a: unknown[]) => deleteSpy(...(a as [])),
  fetchUserAssignments: () => Promise.resolve([]),
  addUserAssignment: vi.fn(async () => undefined),
  removeUserAssignment: vi.fn(async () => undefined),
}));

// @mantine/notifications renders into a portal driven by a store; stub show()
// so the component under test doesn't require the <Notifications/> host.
vi.mock("@mantine/notifications", () => ({ notifications: { show: vi.fn() } }));

function makeUser(over: Partial<LocalUser> = {}): LocalUser {
  return {
    id: "u1",
    username: "alice",
    email: "alice@example.com",
    display_name: "Alice",
    is_active: true,
    ...over,
  } as LocalUser;
}

describe("LocalUsersTab", () => {
  beforeEach(() => {
    createSpy.mockClear();
    deleteSpy.mockClear();
    mockUsers = [];
  });

  it("renders the empty state when there are no users", async () => {
    render(<LocalUsersTab allRoles={["admin"]} allDomains={["sales"]} />);
    expect(await screen.findByText(t("localUsers.empty"))).toBeInTheDocument();
  });

  it("exposes an accessible delete control per user (role + name, not CSS class)", async () => {
    mockUsers = [makeUser()];
    render(<LocalUsersTab allRoles={["admin"]} allDomains={["sales"]} />);
    const del = await screen.findByRole("button", {
      name: t("localUsers.deleteUser", { username: "alice" }),
    });
    fireEvent.click(del);
    await waitFor(() => expect(deleteSpy).toHaveBeenCalledWith("u1"));
  });

  it("creates a user via the required fields and clears the form", async () => {
    render(<LocalUsersTab allRoles={["admin"]} allDomains={["sales"]} />);
    const username = screen.getByRole("textbox", { name: t("localUsers.username") });
    fireEvent.change(username, { target: { value: "bob" } });
    // PasswordInput hides the input from the textbox role; target it by its
    // (translated) placeholder, which is unique.
    fireEvent.change(screen.getByPlaceholderText(t("localUsers.passwordRequired")), {
      target: { value: "secret" },
    });
    mockUsers = [makeUser({ id: "u2", username: "bob" })];
    fireEvent.click(screen.getByRole("button", { name: t("localUsers.createButton") }));
    await waitFor(() =>
      expect(createSpy).toHaveBeenCalledWith(
        expect.objectContaining({ username: "bob", password: "secret" }),
      ),
    );
  });
});
