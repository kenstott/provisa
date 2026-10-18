// Copyright (c) 2026 Kenneth Stott
// Canary: 5bd950d3-d982-455a-a4cc-907ce8223559
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";
import userEvent from "@testing-library/user-event";
import { render, screen, fireEvent, waitFor } from "../test-utils/render";
import i18n from "../i18n";
import { LocalUsersTab } from "../components/admin/LocalUsersTab";
import type { LocalUser, UserAssignment } from "../api/admin";
import { untilEndOf } from "../lib/grantUntil";
import { LIST_COLUMN_CAP_PX } from "../components/list/listCellLimits";

const t = i18n.getFixedT("en");

const createSpy = vi.fn(async () => ({ id: "u2" }));
const deleteSpy = vi.fn(async (): Promise<"org" | "account"> => "org");
let mockUsers: LocalUser[] = [];
let mockAssignments: UserAssignment[] = [];
const addSpy = vi.fn(async (..._a: unknown[]) => undefined);
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
  fetchUserAssignments: () => Promise.resolve(mockAssignments),
  addUserAssignment: (...a: unknown[]) => addSpy(...a),
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
    mockAssignments = [];
    addSpy.mockClear();
    holdsCrossOrg = false;
  });

  // REQ-2077: a grant's end and its last use are on the list; a grant can be given until a day.
  it("lists each grant with when it ends and when it was last used", async () => {
    mockUsers = [makeUser()];
    mockAssignments = [
      {
        id: 1,
        role_id: "analyst",
        domain_id: "sales",
        created_at: "2026-10-01T00:00:00Z",
        expires_at: "2026-11-30T23:59:59Z",
        last_used_at: "2026-10-17T09:00:00Z",
      },
      {
        id: 2,
        role_id: "steward",
        domain_id: "*",
        created_at: "2026-10-01T00:00:00Z",
        expires_at: null,
        last_used_at: null,
      },
    ];
    render(<LocalUsersTab allRoles={["analyst"]} allDomains={["sales"]} />);
    fireEvent.click(await screen.findByTestId("expand-user-alice"));
    const timed = await screen.findByTestId("assignment-analyst-sales");
    const day = (iso: string) => new Date(iso).toLocaleDateString("en");
    expect(timed).toHaveTextContent(day("2026-11-30T23:59:59Z"));
    expect(timed).toHaveTextContent(day("2026-10-17T09:00:00Z"));
    const open = screen.getByTestId("assignment-steward-*");
    expect(open).toHaveTextContent(t("localUsers.noExpiry"));
    expect(open).toHaveTextContent(t("localUsers.neverUsed"));
  });

  it("gives a grant until the close of the day picked", async () => {
    mockUsers = [makeUser()];
    render(<LocalUsersTab allRoles={["analyst"]} allDomains={["sales"]} />);
    fireEvent.click(await screen.findByTestId("expand-user-alice"));
    const until = await screen.findByTestId("assignment-until");
    const next = new Date();
    next.setDate(next.getDate() + 10);
    const picked = [
      next.getFullYear(),
      String(next.getMonth() + 1).padStart(2, "0"),
      String(next.getDate()).padStart(2, "0"),
    ].join("-");
    await userEvent.type(until, picked);
    fireEvent.blur(until);
    fireEvent.click(screen.getByRole("button", { name: t("localUsers.add") }));
    await waitFor(() =>
      expect(addSpy).toHaveBeenCalledWith("u1", "analyst", "*", untilEndOf(picked)),
    );
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
    // The form opens from the button above the list, which is hidden while the form is open.
    fireEvent.click(screen.getByTestId("local-user-add"));
    expect(screen.queryByTestId("local-user-add")).toBeNull();
    const username = screen.getByRole("textbox", { name: t("localUsers.username") });
    fireEvent.change(username, { target: { value: "bob" } });
    // PasswordInput hides the input from the textbox role; target it by its
    // (translated) placeholder, which is unique.
    fireEvent.change(screen.getByPlaceholderText(t("localUsers.passwordRequired")), {
      target: { value: "secret" },
    });
    mockUsers = [makeUser({ id: "u2", username: "bob" })];
    fireEvent.click(screen.getByTestId("local-user-save"));
    await waitFor(() =>
      expect(createSpy).toHaveBeenCalledWith(
        expect.objectContaining({ username: "bob", password: "secret" }),
      ),
    );
    // Created: the form closes and the opener is back.
    expect(await screen.findByTestId("local-user-add")).toBeInTheDocument();
  });

  describe("a user's name on its expand button", () => {
    const LONG =
      "svc-ingest-warehouse-eu-west-1-nightly-replication-account-0042@corp.example.internal";
    const pendingReports = new Set<{ disconnect(): void }>();
    afterEach(() => {
      for (const observer of pendingReports) observer.disconnect();
      vi.restoreAllMocks();
      vi.unstubAllGlobals();
    });

    it("asks for no more than a list column's cap, and a cut name is whole in the hint", async () => {
      // jsdom lays nothing out: the name's line reports it holds more than it shows while it is
      // on the page, and nothing once it has left it, as a browser measures.
      vi.spyOn(HTMLElement.prototype, "scrollWidth", "get").mockImplementation(function (
        this: HTMLElement,
      ) {
        return this.isConnected && this.textContent === LONG && this.tagName === "SPAN" ? 900 : 0;
      });
      // As in a browser, an observer reports once on observe, with an entry for its element, and
      // never after it is disconnected or the test has ended. The hint wraps the button anew when
      // the name is cut; the measure must follow the new element, not report the one that left.
      vi.stubGlobal(
        "ResizeObserver",
        class {
          report: ResizeObserverCallback;
          timers = new Set<ReturnType<typeof setTimeout>>();
          constructor(report: ResizeObserverCallback) {
            this.report = report;
          }
          observe(target: Element) {
            const timer = setTimeout(() => {
              this.timers.delete(timer);
              const contentRect = target.getBoundingClientRect();
              this.report(
                [{ target, contentRect } as ResizeObserverEntry],
                this as unknown as ResizeObserver,
              );
            }, 0);
            this.timers.add(timer);
            pendingReports.add(this);
          }
          unobserve() {}
          disconnect() {
            for (const timer of this.timers) clearTimeout(timer);
            this.timers.clear();
            pendingReports.delete(this);
          }
        },
      );
      mockUsers = [makeUser({ id: "u9", username: LONG })];
      render(<LocalUsersTab allRoles={["admin"]} allDomains={["sales"]} />);
      const button = await screen.findByTestId(`expand-user-${LONG}`);
      // Mantine writes a size in rem (16px each).
      expect(button.style.maxWidth).toContain(`${LIST_COLUMN_CAP_PX / 16}rem`);
      expect(button).toHaveAttribute("data-cut", "true");
      expect(screen.getByText(LONG)).toHaveStyle({
        textOverflow: "ellipsis",
        whiteSpace: "nowrap",
      });
      await new Promise((r) => setTimeout(r, 50));
      const shown = screen.getByTestId(`expand-user-${LONG}`);
      expect(shown).toHaveAttribute("data-cut", "true");
      await userEvent.hover(shown);
      expect(await screen.findByRole("tooltip")).toHaveTextContent(LONG);
    });

    it("a name that fits has no hint", async () => {
      mockUsers = [makeUser()];
      render(<LocalUsersTab allRoles={["admin"]} allDomains={["sales"]} />);
      const button = await screen.findByTestId("expand-user-alice");
      expect(button).not.toHaveAttribute("data-cut");
      await userEvent.hover(button);
      await new Promise((r) => setTimeout(r, 300));
      expect(screen.queryByRole("tooltip")).toBeNull();
    });
  });
});
