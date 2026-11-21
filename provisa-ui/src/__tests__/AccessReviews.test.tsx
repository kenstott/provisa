// Copyright (c) 2026 Kenneth Stott
// Canary: 5d1f8a39-2b74-4e06-9c85-a7e3b0d6f192
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2266: the access review section lists a review's rows grouped by domain, lets the owner of a
// row's domain confirm or revoke it with a reason, starts a review on demand, exports it, and sets
// the schedule.

import { describe, it, expect, vi, beforeEach } from "vitest";
import { fireEvent, render, screen, waitFor, within } from "../test-utils/render";

const api = vi.hoisted(() => ({
  SCHEDULE_STARTER: "schedule",
  IDENTITY_PROVIDER: "identity_provider",
  fetchAccessReviews: vi.fn(),
  fetchAccessReview: vi.fn(),
  startAccessReview: vi.fn(),
  decideAccessReviewRow: vi.fn(),
  accessReviewExportUrl: (id: number) => `/admin/access-reviews/${id}/export`,
  fetchAccessReviewSchedule: vi.fn(),
  saveAccessReviewSchedule: vi.fn(),
}));
vi.mock("../api/accessReview", () => api);

const rights = vi.hoisted(() => ({ held: new Set<string>() }));
vi.mock("../hooks/useCapability", () => ({
  useCapability: (cap: string) => rights.held.has(cap),
}));

import { AccessReviews } from "../components/admin/AccessReviews";

const ROW = {
  entitlements: [],
  expires_at: null,
  last_used_at: null,
  decision: null,
  decided_by: null,
  decided_at: null,
  reason: null,
  display_name: null,
  email: null,
  source: "provisa",
};

const ROWS = [
  {
    ...ROW,
    id: 1,
    user_id: "bo",
    display_name: "Bo",
    role_id: "reader",
    domain_id: "sales",
    entitlements: [{ kind: "tables", capability: null, identifiers: ["sales.orders"] }],
    can_decide: true,
  },
  {
    ...ROW,
    id: 2,
    user_id: "cy",
    role_id: "clerk",
    domain_id: "hr",
    decision: "confirmed",
    decided_by: "ana",
    decided_at: "2026-11-20T10:00:00Z",
    reason: "still on the team",
    can_decide: false,
  },
  {
    ...ROW,
    id: 3,
    user_id: "di",
    role_id: "auditor",
    domain_id: "hr",
    source: "identity_provider",
    can_decide: false,
  },
];

beforeEach(() => {
  vi.clearAllMocks();
  rights.held = new Set(["user_management", "org_settings", "domain_steward"]);
  api.fetchAccessReviews.mockResolvedValue([
    {
      id: 7,
      started_by: "schedule",
      started_at: "2026-11-20T06:00:00Z",
      rows: 2,
      decided: 1,
      webhook_status: null,
      webhook_error: null,
    },
  ]);
  api.fetchAccessReview.mockResolvedValue({ id: 7, rows: ROWS });
  api.fetchAccessReviewSchedule.mockResolvedValue({
    cron: "0 6 1 * *",
    webhook_url: "https://hook.example.test/r",
    webhook_secret_set: true,
  });
  api.saveAccessReviewSchedule.mockImplementation(
    async (cron: string | null, url: string | null) => ({
      cron,
      webhook_url: url,
      webhook_secret_set: true,
    }),
  );
  api.startAccessReview.mockResolvedValue({ id: 8, rows: 3 });
  api.decideAccessReviewRow.mockResolvedValue({ ...ROWS[0], decision: "revoked" });
});

describe("AccessReviews", () => {
  it("lists the latest review's rows grouped by domain, with each decision", async () => {
    render(<AccessReviews />);
    const table = await screen.findByTestId("access-review-rows");
    await within(table).findByText("Bo");
    expect(within(table).getByText(/Domain: hr/)).toBeInTheDocument();
    expect(within(table).getByText(/Domain: sales/)).toBeInTheDocument();
    expect(within(table).getByText("sales.orders")).toBeInTheDocument();
    expect(within(table).getByText("still on the team")).toBeInTheDocument();
    expect(api.fetchAccessReview).toHaveBeenCalledWith(7);
    // Only a row the caller may decide offers the decision.
    expect(screen.getByTestId("access-review-revoke-1")).toBeInTheDocument();
    expect(screen.queryByTestId("access-review-revoke-2")).toBeNull();
  });

  it("revokes a row only with a reason", async () => {
    render(<AccessReviews />);
    fireEvent.click(await screen.findByTestId("access-review-revoke-1"));
    const submit = screen.getByTestId("access-review-decide-submit");
    expect(submit).toBeDisabled();
    fireEvent.change(screen.getByTestId("access-review-reason"), {
      target: { value: "left the team" },
    });
    fireEvent.click(screen.getByTestId("access-review-decide-submit"));
    await waitFor(() =>
      expect(api.decideAccessReviewRow).toHaveBeenCalledWith(1, "revoked", "left the team"),
    );
  });

  it("starts a review on demand and offers its export", async () => {
    render(<AccessReviews />);
    fireEvent.click(await screen.findByTestId("access-review-start"));
    await waitFor(() => expect(api.startAccessReview).toHaveBeenCalled());
    await waitFor(() => expect(api.fetchAccessReview).toHaveBeenCalledWith(8));
    expect(screen.getByTestId("access-review-export")).toHaveAttribute(
      "href",
      "/admin/access-reviews/8/export",
    );
  });

  it("saves the schedule, and clears it when emptied", async () => {
    render(<AccessReviews />);
    const cron = await screen.findByTestId("access-review-schedule-cron");
    await waitFor(() => expect(cron).toHaveValue("0 6 1 * *"));
    fireEvent.change(cron, { target: { value: "" } });
    fireEvent.click(screen.getByTestId("access-review-schedule-save"));
    await waitFor(() =>
      expect(api.saveAccessReviewSchedule).toHaveBeenCalledWith(
        null,
        "https://hook.example.test/r",
        null,
      ),
    );
  });

  it("saves the webhook URL and a new secret; the held secret is never shown", async () => {
    render(<AccessReviews />);
    const url = await screen.findByTestId("access-review-webhook-url");
    await waitFor(() => expect(url).toHaveValue("https://hook.example.test/r"));
    const secret = screen.getByTestId("access-review-webhook-secret");
    expect(secret).toHaveValue("");
    expect(secret).toHaveAttribute("type", "password");
    fireEvent.change(secret, { target: { value: "s3cret" } });
    fireEvent.click(screen.getByTestId("access-review-schedule-save"));
    await waitFor(() =>
      expect(api.saveAccessReviewSchedule).toHaveBeenCalledWith(
        "0 6 1 * *",
        "https://hook.example.test/r",
        "s3cret",
      ),
    );
    await waitFor(() => expect(secret).toHaveValue(""));
  });

  it("says when the scheduled review was not posted to the webhook, with why", async () => {
    api.fetchAccessReviews.mockResolvedValue([
      {
        id: 7,
        started_by: "schedule",
        started_at: "2026-11-20T06:00:00Z",
        rows: 2,
        decided: 1,
        webhook_status: "failed",
        webhook_error: "the request hook at https://h answered 500",
      },
    ]);
    render(<AccessReviews />);
    expect(await screen.findByTestId("access-review-webhook-failed")).toHaveTextContent(
      "answered 500",
    );
  });

  it("lists an identity provider role read-only with its source, offering no decision", async () => {
    render(<AccessReviews />);
    const row = await screen.findByTestId("access-review-row-3");
    expect(within(row).getByTestId("access-review-source-3")).toBeInTheDocument();
    expect(screen.queryByTestId("access-review-confirm-3")).toBeNull();
    expect(screen.queryByTestId("access-review-revoke-3")).toBeNull();
    expect(screen.getByTestId("access-review-confirm-1")).toBeInTheDocument();
  });

  it("shows an owner without user management neither start, export nor schedule", async () => {
    rights.held = new Set(["domain_steward"]);
    render(<AccessReviews />);
    await screen.findByTestId("access-review-revoke-1");
    expect(screen.queryByTestId("access-review-start")).toBeNull();
    expect(screen.queryByTestId("access-review-export")).toBeNull();
    expect(screen.queryByTestId("access-review-schedule")).toBeNull();
    expect(api.fetchAccessReviewSchedule).not.toHaveBeenCalled();
  });
});
