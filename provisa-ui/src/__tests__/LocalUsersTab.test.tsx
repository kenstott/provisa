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
const deleteSpy = vi.fn(async (): Promise<"org" | "account"> => "org");
let mockUsers: LocalUser[] = [];
// Whether the caller holds the cross-org right: it deletes accounts; an org administrator
// removes a person from their org.
let holdsCrossOrg = false;

vi.mock("../hooks/useCapability", () => ({
  useCapability: (cap: string) => cap === "cross_org" && holdsCrossOrg,
}));

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
const showSpy = vi.fn();
vi.mock("@mantine/notifications", () => ({
  notifications: { show: (...a: unknown[]) => showSpy(...a) },
}));

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
    deleteSpy.mockReset();
    deleteSpy.mockResolvedValue("org");
    showSpy.mockClear();
    mockUsers = [];
    holdsCrossOrg = false;
  });

  it("renders the empty state when there are no users", async () => {
    render(<LocalUsersTab allRoles={["admin"]} allDomains={["sales"]} />);
    expect(await screen.findByText(t("localUsers.empty"))).toBeInTheDocument();
  });

  it("an org administrator removes a user from the organization, and is told so first", async () => {
    mockUsers = [makeUser()];
    render(<LocalUsersTab allRoles={["admin"]} allDomains={["sales"]} />);
    // Accessible control: role + name, not a CSS class.
    const control = await screen.findByRole("button", {
      name: t("localUsers.removeFromOrg", { username: "alice" }),
    });
    fireEvent.click(control);

    // Nothing happens until the person confirms what the page says it will do.
    expect(deleteSpy).not.toHaveBeenCalled();
    expect(await screen.findByText(t("localUsers.confirmRemoveTitle"))).toBeInTheDocument();
    expect(
      screen.getByText(t("localUsers.confirmRemoveBody", { username: "alice" })),
    ).toBeInTheDocument();

    fireEvent.click(screen.getByRole("button", { name: t("localUsers.confirmRemoveButton") }));
    await waitFor(() => expect(deleteSpy).toHaveBeenCalledWith("u1"));
    await waitFor(() =>
      expect(showSpy).toHaveBeenCalledWith({
        message: t("localUsers.removedFromOrg", { username: "alice" }),
      }),
    );
  });

  it("the holder of the cross-org right deletes the account, and is told so first", async () => {
    holdsCrossOrg = true;
    deleteSpy.mockResolvedValue("account");
    mockUsers = [makeUser()];
    render(<LocalUsersTab allRoles={["admin"]} allDomains={["sales"]} />);
    fireEvent.click(
      await screen.findByRole("button", {
        name: t("localUsers.deleteUser", { username: "alice" }),
      }),
    );

    expect(await screen.findByText(t("localUsers.confirmDeleteTitle"))).toBeInTheDocument();
    expect(
      screen.getByText(t("localUsers.confirmDeleteBody", { username: "alice" })),
    ).toBeInTheDocument();

    fireEvent.click(screen.getByRole("button", { name: t("localUsers.confirmDeleteButton") }));
    await waitFor(() => expect(deleteSpy).toHaveBeenCalledWith("u1"));
    await waitFor(() =>
      expect(showSpy).toHaveBeenCalledWith({
        message: t("localUsers.deleted", { username: "alice" }),
      }),
    );
  });

  it("shows the refusal and keeps the user when the removal is refused", async () => {
    const refusal =
      "alice is the last org admin of: acme. Promote another org admin in each, or delete the organization, first.";
    deleteSpy.mockRejectedValue(new Error(refusal));
    mockUsers = [makeUser()];
    render(<LocalUsersTab allRoles={["admin"]} allDomains={["sales"]} />);
    fireEvent.click(
      await screen.findByRole("button", {
        name: t("localUsers.removeFromOrg", { username: "alice" }),
      }),
    );
    fireEvent.click(
      await screen.findByRole("button", { name: t("localUsers.confirmRemoveButton") }),
    );

    await waitFor(() =>
      expect(showSpy).toHaveBeenCalledWith({ color: "red", autoClose: false, message: refusal }),
    );
    expect(screen.getByText("alice")).toBeInTheDocument();
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
