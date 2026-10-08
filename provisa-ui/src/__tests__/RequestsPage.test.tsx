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
import { MantineProvider } from "@mantine/core";
import i18n from "../i18n";
import { RequestsPage } from "../pages/RequestsPage";

const base = {
  request_type: "relationship",
  capability: "create_relationship",
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
};

const MINE = {
  ...base,
  id: 2,
  requested_by: "me",
  approvals: [],
  domains: ["finance", "sales"],
  waiting_on: ["finance", "sales"],
  can_decide: false,
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
      if (url.endsWith("/approve")) return approve();
      return json([DECIDABLE, MINE]);
    }),
  );
}

function page() {
  return render(
    <MantineProvider>
      <RequestsPage />
    </MantineProvider>,
  );
}

async function row(id: number) {
  const cell = await screen.findByTestId(`requests-waiting-${id}`);
  return cell.closest("tr") as HTMLElement;
}

describe("RequestsPage — a request is decided by the domains it touches", () => {
  beforeEach(async () => {
    vi.unstubAllGlobals();
    localStorage.clear();
    await i18n.changeLanguage("en");
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
    const decidable = await row(1);
    expect(within(decidable).getByTestId("requests-approve-1")).toBeInTheDocument();
    expect(within(decidable).getByTestId("requests-reject-1")).toBeInTheDocument();
    // The user's own request is listed, with nothing to press.
    const mine = await row(2);
    expect(within(mine).queryByTestId("requests-approve-2")).toBeNull();
    expect(within(mine).queryByTestId("requests-reject-2")).toBeNull();
  });

  it("does not offer execute while a domain is still waited on", async () => {
    serve(() => json(DECIDABLE));
    page();
    await row(1);
    expect(screen.queryByTestId("requests-execute-1")).toBeNull();
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
    fireEvent.click(await screen.findByTestId("requests-approve-1"));
    const alert = await screen.findByTestId("requests-error");
    expect(alert).toHaveTextContent(expected);
    expect(alert).not.toHaveTextContent("server English");
  });
});
