// Copyright (c) 2026 Kenneth Stott
// Canary: 3e7c1a95-6b2d-4f80-9c14-a5d8e0f3b672
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1948: a relationship request is decided by the domains it touches. The page shows which
// domains each request still waits on, offers the decision only to a user who may make it, and
// says in the reader's language why an approval was refused.

import { describe, it, expect, vi, beforeEach } from "vitest";
import { fireEvent, render, screen, within } from "@testing-library/react";
import { MemoryRouter } from "react-router-dom";
import { MantineProvider } from "@mantine/core";
import i18n from "../i18n";
import { expectFilterFindsEveryCell } from "../test-utils/listFilter";

// REQ-2216: the page offers the org's delivery setting to a holder of org_settings.
const auth = vi.hoisted(() => ({ capabilities: [] as string[] }));
vi.mock("../context/AuthContext", () => ({ useAuth: () => auth }));

import { RequestsPage } from "../pages/RequestsPage";

const base = {
  request_type: "relationship",
  capability: "approve_relationship",
  payload: { id: "orders_invoices" },
  status: "pending",
  rejection_reason: null,
  resolved_by: null,
  created_at: "2026-10-07T10:00:00Z",
  resolved_at: null,
  required_approvals: 2,
};

const DECIDABLE = {
  ...base,
  id: 1,
  requested_by: "asker",
  approvals: [{ approver: "sam", approved_at: "now", domains: ["sales"] }],
  domains: ["finance", "sales"],
  waiting_on: ["finance"],
  can_decide: true,
  can_withdraw: false,
  approve_refusal: null,
};

const OWN_REFUSAL = {
  code: "requests.own_request",
  params: {},
  detail: "server English",
};

// Two approvals from the sales side: the count is met, finance has not been heard from.
const ONE_SIDED = {
  ...base,
  id: 3,
  requested_by: "asker",
  approvals: [
    { approver: "sam", approved_at: "now", domains: ["sales"] },
    { approver: "me", approved_at: "now", domains: ["sales"] },
  ],
  domains: ["finance", "sales"],
  waiting_on: ["finance"],
  can_decide: true,
  can_withdraw: false,
  approve_refusal: { code: "requests.already_approved", params: {}, detail: "server English" },
};

const MINE = {
  ...base,
  id: 2,
  requested_by: "me",
  approvals: [],
  domains: ["finance", "sales"],
  waiting_on: ["finance", "sales"],
  can_decide: false,
  can_withdraw: false,
  approve_refusal: OWN_REFUSAL,
};

// A view request: one approval, which creates the view. A second one whose creation failed after
// that approval is the only place Execute is offered.
const VIEW = {
  ...base,
  id: 4,
  request_type: "view",
  capability: "approve_view",
  required_approvals: 1,
  requested_by: "asker",
  approvals: [],
  domains: [],
  waiting_on: [],
  can_decide: true,
  can_withdraw: false,
  approve_refusal: null,
};
const VIEW_TO_RETRY = {
  ...VIEW,
  id: 5,
  approvals: [{ approver: "me", approved_at: "now", domains: [] }],
  approve_refusal: { code: "requests.already_approved", params: {}, detail: "server English" },
};

// The user's own view request: not theirs to approve, theirs to take back.
const MY_VIEW = {
  ...VIEW,
  id: 6,
  requested_by: "me",
  can_decide: false,
  can_withdraw: true,
  approve_refusal: OWN_REFUSAL,
};

function json(body: unknown, status = 200) {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "Content-Type": "application/json" },
  });
}

function serve(approve: () => Response) {
  vi.stubGlobal(
    "fetch",
    vi.fn(async (url: string) => {
      if (url.includes("/rejection-reasons")) return json({ relationship: ["duplicate"] });
      if (url.includes("/request-delivery")) return json({ delivery: "events" });
      if (url.endsWith("/approve")) return approve();
      return json([DECIDABLE, MINE, ONE_SIDED, VIEW, VIEW_TO_RETRY, MY_VIEW]);
    }),
  );
}

function page() {
  return render(
    <MantineProvider>
      <MemoryRouter>
        <RequestsPage />
      </MemoryRouter>
    </MantineProvider>,
  );
}

async function row(id: number) {
  const cell = await screen.findByTestId(`requests-waiting-${id}`);
  return cell.closest("tr") as HTMLElement;
}

/** REQ-2277: a request's decision is in its panel; the row opens it. */
async function panel(id: number) {
  fireEvent.click(await screen.findByTestId(`request-row-${id}`));
  const shown = await screen.findByTestId(`request-detail-${id}`);
  return shown.closest('[data-testid="requests-split-panel"]') as HTMLElement;
}

describe("RequestsPage — a request is decided by the domains it touches", () => {
  beforeEach(async () => {
    vi.unstubAllGlobals();
    localStorage.clear();
    await i18n.changeLanguage("en");
  });

  it("finds the requests matching every word of the search, or either side of or", async () => {
    serve(() => json({}));
    page();
    await row(1);
    // "3" is request 3's id, "me" the requester of 2 (and 6): request 1 and 4 match neither.
    fireEvent.change(screen.getByTestId("filter-input"), { target: { value: "3 or me" } });
    expect(screen.getByTestId("requests-waiting-2")).toBeInTheDocument();
    expect(screen.getByTestId("requests-waiting-3")).toBeInTheDocument();
    expect(screen.queryByTestId("requests-waiting-1")).toBeNull();
    expect(screen.queryByTestId("requests-waiting-4")).toBeNull();
  });

  it("finds each request by every text its row shows", async () => {
    serve(() => json({}));
    page();
    await row(1);
    await expectFilterFindsEveryCell(screen.getByTestId("filter-input"), () =>
      screen.getByTestId("requests-list"),
    );
  });

  it("says nothing matches when the search empties the list", async () => {
    serve(() => json({}));
    page();
    await row(1);
    fireEvent.change(screen.getByTestId("filter-input"), { target: { value: "no-such-request" } });
    expect(screen.getByTestId("requests-empty")).toHaveTextContent(
      "No pending requests matching filter.",
    );
  });

  it("shows which domains each request still waits on", async () => {
    serve(() => json(DECIDABLE));
    page();
    expect(await screen.findByTestId("requests-waiting-1")).toHaveTextContent("finance");
    expect(screen.getByTestId("requests-waiting-1")).not.toHaveTextContent("sales");
    expect(screen.getByTestId("requests-waiting-2")).toHaveTextContent("finance, sales");
  });

  it("offers the decision only on a request the user may decide", async () => {
    serve(() => json(DECIDABLE));
    page();
    const decidable = await panel(1);
    expect(await within(decidable).findByTestId("requests-approve-1")).toBeInTheDocument();
    expect(within(decidable).getByTestId("requests-reject-1")).toBeInTheDocument();
    // The user's own request is listed, with nothing to press.
    const mine = await panel(2);
    expect(within(mine).queryByTestId("requests-approve-2")).toBeNull();
    expect(within(mine).queryByTestId("requests-reject-2")).toBeNull();
  });

  it("says why, where the user cannot decide", async () => {
    serve(() => json(DECIDABLE));
    page();
    await panel(2);
    expect(await screen.findByTestId("requests-cannot-2")).toHaveTextContent(
      "You cannot decide a request you made",
    );
    expect(screen.queryByTestId("requests-cannot-1")).toBeNull();
  });

  it("shows the approvals so far and the domains waited on, never a count past the total", async () => {
    serve(() => json(DECIDABLE));
    page();
    const oneSided = await row(3);
    expect(oneSided).toHaveTextContent("2 / 2");
    expect(within(oneSided).getByTestId("requests-waiting-3")).toHaveTextContent("finance");
    expect(oneSided).not.toHaveTextContent("3/2");
    expect(await within(await panel(1)).findByTestId("requests-approve-1")).toHaveTextContent(
      /^Approve$/,
    );
  });

  it("disables approve for a user whose approval adds nothing, with the reason", async () => {
    serve(() => json(DECIDABLE));
    page();
    const approve = await within(await panel(3)).findByTestId("requests-approve-3");
    expect(approve).toBeDisabled();
    // Said to a screen reader as the button's description, and shown as the hint on hover.
    expect(approve).toHaveAccessibleDescription("You have already approved this request");
    expect(approve).not.toHaveAttribute("title");
    // Rejecting is still theirs to do.
    expect(screen.getByTestId("requests-reject-3")).toBeEnabled();
    expect(await within(await panel(1)).findByTestId("requests-approve-1")).toBeEnabled();
  });

  it("does not offer execute while a domain is still waited on", async () => {
    serve(() => json(DECIDABLE));
    page();
    await within(await panel(1)).findByTestId("requests-approve-1");
    expect(screen.queryByTestId("requests-execute-1")).toBeNull();
  });

  it("offers execute only to retry a request that has had its approvals", async () => {
    serve(() => json(DECIDABLE));
    page();
    const fresh = await panel(4);
    expect(await within(fresh).findByTestId("requests-approve-4")).toBeEnabled();
    expect(within(fresh).queryByTestId("requests-execute-4")).toBeNull();
    const retry = await panel(5);
    expect(await within(retry).findByTestId("requests-execute-5")).toBeEnabled();
    expect(within(retry).getByTestId("requests-approve-5")).toBeDisabled();
  });

  it("lets the author take back their own request, where that is allowed", async () => {
    serve(() => json(DECIDABLE));
    page();
    const mine = await panel(6);
    expect(await within(mine).findByTestId("requests-withdraw-6")).toBeEnabled();
    expect(within(mine).queryByTestId("requests-approve-6")).toBeNull();
    // A relationship request is not its author's to reject.
    expect(within(await panel(2)).queryByTestId("requests-withdraw-2")).toBeNull();
  });

  it.each([
    ["en", "does not reach a domain this request touches (finance, sales)"],
    ["fr", "finance, sales"],
  ])("says why an approval was refused, in %s", async (lng, expected) => {
    await i18n.changeLanguage(lng);
    serve(() =>
      json(
        {
          detail: "server English",
          code: "requests.approver_outside_domains",
          params: { domains: "finance, sales" },
        },
        403,
      ),
    );
    page();
    await panel(1);
    fireEvent.click(await screen.findByTestId("requests-approve-1"));
    const alert = await screen.findByTestId("requests-error");
    expect(alert).toHaveTextContent(expected);
    expect(alert).not.toHaveTextContent("server English");
  });
});

describe("RequestsPage's remembered filters", () => {
  beforeEach(async () => {
    vi.unstubAllGlobals();
    localStorage.clear();
    await i18n.changeLanguage("en");
    serve(() => new Response("{}"));
  });

  it("reopens on the tab and the search it was left on", async () => {
    const first = page();
    fireEvent.click(await screen.findByTestId("requests-tab-resolved"));
    fireEvent.change(screen.getByTestId("filter-input"), { target: { value: "orders" } });
    first.unmount();
    page();
    expect(await screen.findByTestId("requests-tab-resolved")).toHaveAttribute(
      "aria-selected",
      "true",
    );
    expect(screen.getByTestId("filter-input")).toHaveValue("orders");
  });

  it("offers the delivery setting only to a holder of org_settings", async () => {
    serve(() => json({}));
    auth.capabilities = [];
    const { unmount } = page();
    await row(1);
    expect(screen.queryByTestId("request-delivery")).toBeNull();
    unmount();
    auth.capabilities = ["org_settings"];
    page();
    const setting = await screen.findByTestId("request-delivery");
    expect(setting).toHaveTextContent("Notify by");
    auth.capabilities = [];
  });
});
