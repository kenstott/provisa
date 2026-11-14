// Copyright (c) 2026 Kenneth Stott
// Canary: 6f2a8c41-b7d9-4e05-93c2-e8a1d4f6b072
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2180, REQUESTS GO THROUGH HOOKS: the owner's Requests tab lists the bids for the product's
// offers, each accepted as bid or declined with a reason, and who reads each dataset now.

import { describe, it, expect, vi, beforeEach } from "vitest";
import { fireEvent, render, screen, waitFor, within } from "../../../../test-utils/render";
import type { Bid } from "../../../../api/dataProductOffers";

const api = vi.hoisted(() => ({
  bids: vi.fn<() => Promise<Bid[]>>(),
  accept: vi.fn<(id: string, bid: number) => Promise<unknown>>(),
  decline: vi.fn<(id: string, bid: number, reason: string) => Promise<unknown>>(),
}));
vi.mock("../../../../api/dataProductOffers", () => ({
  fetchBids: () => api.bids(),
  acceptBid: (id: string, bid: number) => api.accept(id, bid),
  declineBid: (id: string, bid: number, reason: string) => api.decline(id, bid, reason),
}));
vi.mock("../../../tables/TableAccessPanel", () => ({
  TableAccessPanel: ({ tableId }: { tableId: number }) => (
    <div data-testid={`access-${tableId}`} />
  ),
}));

import { RequestsTab } from "../RequestsTab";

const bid = (over: Partial<Bid>): Bid => ({
  id: 1,
  request_id: "req-1",
  kind: "bid",
  product_id: "c360",
  offer_id: 3,
  offer_version: 1,
  terms: { role_id: "analyst" },
  requester: "ada@acme.example",
  reason: "Churn model",
  consumer_kind: "person",
  approver: "owner",
  agreed_at: "2026-11-11T00:00:00Z",
  status: "pending",
  decided_by: null,
  decided_at: null,
  decline_reason: null,
  created_at: "2026-11-11T09:00:00Z",
  ...over,
});

beforeEach(() => {
  window.localStorage.clear();
  api.bids.mockReset();
  api.accept.mockReset().mockResolvedValue({});
  api.decline.mockReset().mockResolvedValue({});
});

function tab() {
  render(<RequestsTab productId="c360" datasets={[{ id: 7, name: "sales.customers" }]} />);
}

describe("a product's Requests tab", () => {
  it("lists each bid: who, for which role, why, and where it stands", async () => {
    api.bids.mockResolvedValue([
      bid({}),
      bid({ id: 2, status: "declined", decline_reason: "role too broad" }),
    ]);
    tab();
    const row = await screen.findByTestId("bid-1");
    expect(row).toHaveTextContent("ada@acme.example");
    expect(row).toHaveTextContent("analyst");
    expect(row).toHaveTextContent("Churn model");
    expect(row).toHaveTextContent("Pending");
    expect(screen.getByTestId("bid-2")).toHaveTextContent("Declined: role too broad");
    expect(screen.queryByTestId("bid-2-accept")).toBeNull();
  });

  it("accepts a bid as bid", async () => {
    api.bids.mockResolvedValue([bid({})]);
    tab();
    fireEvent.click(await screen.findByTestId("bid-1-accept"));
    await waitFor(() => expect(api.accept).toHaveBeenCalledWith("c360", 1));
  });

  it("declines a bid only with a reason", async () => {
    api.bids.mockResolvedValue([bid({})]);
    tab();
    fireEvent.click(await screen.findByTestId("bid-1-decline"));
    const send = screen.getByTestId("bid-1-decline-send");
    expect(send).toBeDisabled();
    fireEvent.change(screen.getByTestId("bid-1-decline-reason"), {
      target: { value: "Use the sample offer" },
    });
    fireEvent.click(send);
    await waitFor(() =>
      expect(api.decline).toHaveBeenCalledWith("c360", 1, "Use the sample offer"),
    );
  });

  it("says when there are no bids, and shows who reads each dataset", async () => {
    api.bids.mockResolvedValue([]);
    tab();
    expect(await screen.findByTestId("data-product-requests-empty")).toBeInTheDocument();
    fireEvent.click(screen.getByTestId("data-product-holders-7-toggle"));
    const holders = await screen.findByTestId("access-7");
    expect(within(holders.parentElement as HTMLElement).getByTestId("access-7")).toBeInTheDocument();
  });
});
