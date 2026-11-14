// Copyright (c) 2026 Kenneth Stott
// Canary: d8c3b5e2-7a14-4f69-a0e8-5b2f9c6d1e47
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2180 OFFERS, THE UI SHOWS CONTRACTS PER OFFER: a product's offers as cards. Each says its role,
// data level, duration and terms, with a quality line that links to Quality; it opens to the
// contract it gives on each dataset, the columns as its role sees them (the tables page's columns
// table, a masked one marked), the offer's terms of use and its row rules in words. A requester bids
// -- the form naming who approves it, asking the kind of consumer and the purpose (prefilled from
// the offer's usage), showing the terms of use and sending only once they are agreed to -- or
// queries an open offer now; the owner removes one, confirming, and adds one.

import type { ReactNode } from "react";
import { describe, it, expect, vi, beforeEach } from "vitest";
import { fireEvent, render, screen, waitFor, within } from "../../../../test-utils/render";
import type { Approver, BidInput, Offer } from "../../../../api/dataProductOffers";

const api = vi.hoisted(() => ({
  offers: vi.fn<() => Promise<Offer[]>>(),
  bid: vi.fn<(id: string, offer: number, bid: BidInput) => Promise<unknown>>(),
  approver: vi.fn<(id: string) => Promise<Approver>>(),
  remove: vi.fn<(id: string, offer: number) => Promise<unknown>>(),
}));
vi.mock("../../../../api/dataProductOffers", () => ({
  fetchOffers: () => api.offers(),
  bidForOffer: (id: string, offer: number, bid: BidInput) => api.bid(id, offer, bid),
  fetchApprover: (id: string) => api.approver(id),
  removeOffer: (id: string, offer: number) => api.remove(id, offer),
  fetchGrantableRoles: () => Promise.resolve([]),
  addOffer: vi.fn(),
}));
vi.mock("../../../../api/roleRights", () => ({ fetchTableAccess: vi.fn() }));
// A column's glossary hover reads the glossary: not here.
vi.mock("../../../tables/ColumnGlossaryHover", () => ({
  ColumnGlossaryHover: ({ children }: { children: ReactNode }) => <>{children}</>,
}));

import { OffersTab } from "../OffersTab";

const offer = (over: Partial<Offer>): Offer => ({
  id: 1,
  product_id: "c360",
  name: "default",
  role_id: "viewer",
  data_level: "full",
  sample_views: [],
  dq_results: true,
  profiles: false,
  open: false,
  duration_days: null,
  purpose: "",
  usage: "",
  limitations: "",
  version: 1,
  published_by: "owner",
  review: null,
  created_at: "2026-11-11T00:00:00Z",
  updated_at: "2026-11-11T00:00:00Z",
  summary: { extension: null },
  query_now: false,
  contracts: [
    {
      table_id: 7,
      table: "sales.customers",
      contract: {
        version: "1.0.0",
        created_at: "2026-11-11T00:00:00Z",
        terms: {
          fields: [
            { name: "id", type: "integer", description: "The customer", masked: null },
            { name: "email", type: "varchar", description: null, masked: "regex" },
          ],
          row_rules: ["only rows in the EU"],
          data_level: "full",
          sample_views: [],
          checks: [{ name: "unique id", column: "id", severity: "fail", definition: "" }],
          dq_results: true,
          profiles: [],
          duration_days: null,
          purpose: "Churn analysis",
          usage: "",
          limitations: "No re-identification",
        },
      },
    },
  ],
  ...over,
});

const handlers = () => ({
  onShowQuality: vi.fn(),
  onPreview: vi.fn(),
  onQueryNow: vi.fn(),
  onBid: vi.fn(),
});

function tab(owner: boolean, canEdit = owner) {
  const h = handlers();
  render(
    <OffersTab
      productId="c360"
      owner={owner}
      canEdit={canEdit}
      datasets={[{ id: 7, name: "sales.customers" }]}
      views={[]}
      {...h}
    />,
  );
  return h;
}

beforeEach(() => {
  window.localStorage.clear();
  api.offers.mockReset();
  api.bid.mockReset().mockResolvedValue({});
  api.approver.mockReset().mockResolvedValue({ via: "owner", name: "data_owner" });
  api.remove.mockReset().mockResolvedValue({ deleted: 1 });
});

describe("a product's offers", () => {
  it("says when there is none", async () => {
    api.offers.mockResolvedValue([]);
    tab(false);
    expect(await screen.findByTestId("data-product-offers-empty")).toBeInTheDocument();
  });

  it("are cards saying role, data level, duration and terms", async () => {
    api.offers.mockResolvedValue([
      offer({}),
      offer({
        id: 2,
        name: "trial",
        role_id: "analyst",
        data_level: "synthetic",
        duration_days: 30,
      }),
    ]);
    tab(false);
    const first = await screen.findByTestId("offer-card-1");
    expect(within(first).getByTestId("offer-card-1-role")).toHaveTextContent("viewer");
    expect(within(first).getByTestId("offer-card-1-level")).toHaveTextContent("Full data");
    expect(within(first).getByTestId("offer-card-1-duration")).toHaveTextContent("No end date");
    expect(within(first).getByTestId("offer-card-1-access")).toHaveTextContent(
      "1 masked columns · 1 row rules",
    );
    const second = screen.getByTestId("offer-card-2");
    expect(within(second).getByTestId("offer-card-2-level")).toHaveTextContent("Synthetic");
    expect(within(second).getByTestId("offer-card-2-duration")).toHaveTextContent("For 30 days");
    expect(within(second).getByTestId("offer-card-2-summary")).toHaveTextContent(
      "analyst gets synthetic data (no real records) of 1 dataset",
    );
  });

  it("carry a quality line that opens Quality", async () => {
    api.offers.mockResolvedValue([offer({})]);
    const h = tab(false);
    const line = await screen.findByTestId("offer-card-1-quality");
    expect(line).toHaveTextContent("1 quality check promised; results included.");
    fireEvent.click(within(line).getByTestId("offer-card-1-quality-link"));
    expect(h.onShowQuality).toHaveBeenCalled();
  });

  it("open to the contract each dataset gets: its columns as the role sees them, its row rules", async () => {
    api.offers.mockResolvedValue([offer({})]);
    const h = tab(false);
    fireEvent.click(await screen.findByTestId("offer-card-1-toggle"));
    fireEvent.click(await screen.findByTestId("offer-1-contract-7-toggle"));
    const contract = await screen.findByTestId("offer-1-contract-7");
    expect(screen.getByTestId("offer-1-contract-7-version")).toHaveTextContent("1.0.0");
    const email = within(contract).getByTestId("offer-1-contract-7-columns-col-email");
    expect(email).toHaveTextContent("Regex");
    expect(within(contract).getByTestId("offer-1-contract-7-columns-col-id")).not.toHaveTextContent(
      "Regex",
    );
    expect(within(contract).getByTestId("offer-1-contract-7-row-rules")).toHaveTextContent(
      "only rows in the EU",
    );
    // The contract states the offer's terms of use.
    expect(within(contract).getByTestId("offer-1-contract-7-terms-purpose")).toHaveTextContent(
      "Purpose: Churn analysis",
    );
    expect(within(contract).getByTestId("offer-1-contract-7-terms-limitations")).toHaveTextContent(
      "Limitations: No re-identification",
    );
    fireEvent.click(within(contract).getByTestId("offer-1-contract-7-preview"));
    expect(h.onPreview).toHaveBeenCalledWith(7);
  });

  it("are bid for by a requester who names the consumer, states the purpose and agrees to the terms", async () => {
    api.offers.mockResolvedValue([
      offer({
        summary: { extension: "extends your viewer role" },
        purpose: "Churn analysis",
        usage: "Monthly churn models",
        limitations: "No re-identification",
      }),
    ]);
    const h = tab(false);
    expect(await screen.findByTestId("offer-card-1-extension")).toHaveTextContent(
      "extends your viewer role",
    );
    fireEvent.click(screen.getByTestId("offer-card-1-bid"));
    // Who approves it is named.
    expect(await screen.findByTestId("offer-1-bid-approver")).toHaveTextContent(
      "The product's owner role, data_owner",
    );
    expect(api.approver).toHaveBeenCalledWith("c360");
    // The purpose starts from the offer's usage, and is editable.
    const purpose = screen.getByTestId("offer-1-bid-purpose");
    expect(purpose).toHaveValue("Monthly churn models");
    // The terms of use are shown.
    expect(screen.getByTestId("offer-1-bid-term-purpose")).toHaveTextContent("Churn analysis");
    expect(screen.getByTestId("offer-1-bid-term-usage")).toHaveTextContent("Monthly churn models");
    expect(screen.getByTestId("offer-1-bid-term-limitations")).toHaveTextContent(
      "No re-identification",
    );
    const send = screen.getByTestId("offer-1-bid-send");
    expect(send).toBeDisabled();
    fireEvent.click(screen.getByTestId("offer-1-bid-consumer-kind"));
    fireEvent.click(await screen.findByRole("option", { name: "A team", hidden: true }));
    fireEvent.change(purpose, { target: { value: "Quarterly churn model" } });
    // Not sent until the terms are agreed to.
    expect(send).toBeDisabled();
    fireEvent.click(screen.getByTestId("offer-1-bid-agree"));
    expect(send).toBeEnabled();
    fireEvent.click(send);
    await waitFor(() =>
      expect(api.bid).toHaveBeenCalledWith("c360", 1, {
        reason: "Quarterly churn model",
        consumer_kind: "team",
        agreed: true,
      }),
    );
    await waitFor(() => expect(h.onBid).toHaveBeenCalled());
  });

  it("asks for a person, a team, another data product or an agent", async () => {
    api.offers.mockResolvedValue([offer({})]);
    tab(false);
    fireEvent.click(await screen.findByTestId("offer-card-1-bid"));
    fireEvent.click(await screen.findByTestId("offer-1-bid-consumer-kind"));
    const options = (await screen.findAllByRole("option", { hidden: true })).map(
      (o) => o.textContent,
    );
    expect(options).toEqual(["A person", "A team", "Another data product", "An agent"]);
  });

  it("names the authority the request hook reaches as the approver", async () => {
    api.approver.mockResolvedValue({ via: "hook", name: "access.example.com" });
    api.offers.mockResolvedValue([offer({})]);
    tab(false);
    fireEvent.click(await screen.findByTestId("offer-card-1-bid"));
    expect(await screen.findByTestId("offer-1-bid-approver")).toHaveTextContent(
      "access.example.com, through the org's request hook",
    );
  });

  it("says no one approves a request for a product naming no owner role", async () => {
    api.approver.mockResolvedValue({ via: "owner", name: null });
    api.offers.mockResolvedValue([offer({})]);
    tab(false);
    fireEvent.click(await screen.findByTestId("offer-card-1-bid"));
    expect(await screen.findByTestId("offer-1-bid-approver")).toHaveTextContent(
      "No one: the product names no owner role",
    );
  });

  it("show each offer's terms of use on its card", async () => {
    api.offers.mockResolvedValue([
      offer({ purpose: "Churn analysis", limitations: "No re-identification" }),
      offer({ id: 2, name: "bare" }),
    ]);
    tab(false);
    const terms = await screen.findByTestId("offer-card-1-terms");
    expect(within(terms).getByTestId("offer-card-1-terms-purpose")).toHaveTextContent(
      "Purpose: Churn analysis",
    );
    expect(within(terms).getByTestId("offer-card-1-terms-limitations")).toHaveTextContent(
      "Limitations: No re-identification",
    );
    // A term it does not state is not shown; an offer stating none says so.
    expect(within(terms).queryByTestId("offer-card-1-terms-usage")).toBeNull();
    expect(screen.getByTestId("offer-card-2-terms-none")).toHaveTextContent(
      "No terms of use stated.",
    );
  });

  it("queried now when open, with no bid", async () => {
    api.offers.mockResolvedValue([offer({ open: true, query_now: true })]);
    const h = tab(false);
    fireEvent.click(await screen.findByTestId("offer-card-1-query-now"));
    expect(h.onQueryNow).toHaveBeenCalled();
    expect(screen.queryByTestId("offer-card-1-bid")).toBeNull();
    expect(screen.getByTestId("offer-card-1-open")).toBeInTheDocument();
  });

  it("are not bid for by the owner, who removes one, confirming", async () => {
    api.offers.mockResolvedValue([offer({})]);
    tab(true);
    await screen.findByTestId("offer-card-1");
    expect(screen.queryByTestId("offer-card-1-bid")).toBeNull();
    fireEvent.click(screen.getByTestId("offer-card-1-remove"));
    fireEvent.click(await screen.findByTestId("offer-card-1-remove-dialog-confirm"));
    await waitFor(() => expect(api.remove).toHaveBeenCalledWith("c360", 1));
  });

  it("are added by whoever may edit the product; the opener is hidden while the form is open", async () => {
    api.offers.mockResolvedValue([]);
    tab(true);
    fireEvent.click(await screen.findByTestId("add-offer-open-button"));
    expect(screen.getByTestId("add-offer-panel")).toBeInTheDocument();
    expect(screen.queryByTestId("add-offer-open-button")).toBeNull();
  });

  it("offer no Add to a reader who may not edit", async () => {
    api.offers.mockResolvedValue([]);
    tab(false);
    await screen.findByTestId("data-product-offers-empty");
    expect(screen.queryByTestId("add-offer-open-button")).toBeNull();
  });
});
