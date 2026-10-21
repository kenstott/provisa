// Copyright (c) 2026 Kenneth Stott
// Canary: 7c0a4e35-2b96-4d18-9f53-1e8d6b2a7c40
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2122: From catalog and Refresh from catalog. What is asserted is what a wrong pick list gets
// wrong silently: not preselecting the asset for the same physical table, applying what the
// steward did not accept, mapping a column the steward re-mapped to the wrong catalog column,
// offering a tag Provisa does not have, and hiding one catalog's failure behind another's list.

import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, waitFor, fireEvent, within } from "../test-utils/render";
import { CatalogPickModal } from "../pages/tables/CatalogPickModal";
import type { CatalogCandidates, CatalogOffer } from "../api/catalogLinks";

vi.mock("../api/catalogLinks", () => ({
  fetchCatalogCandidates: vi.fn(),
  fetchCatalogOffer: vi.fn(),
  applyCatalogOffer: vi.fn(),
}));

import { applyCatalogOffer, fetchCatalogCandidates, fetchCatalogOffer } from "../api/catalogLinks";

const mockCandidates = vi.mocked(fetchCatalogCandidates);
const mockOffer = vi.mocked(fetchCatalogOffer);
const mockApply = vi.mocked(applyCatalogOffer);

const asset = (ref: string, name: string, description = "") => ({
  ref,
  name,
  physical: name.split("."),
  description,
  url: null,
});

const LISTS: CatalogCandidates[] = [
  {
    catalog: "om",
    provider: "openmetadata",
    linked: null,
    preselected: "o",
    assets: [
      asset("o", "pg.sales.public.orders", "Every order"),
      asset("ol", "pg.sales.public.order_lines"),
    ],
  },
  { catalog: "collibra", provider: "collibra", error: "ConnectError: refused" },
];

const OFFER: CatalogOffer = {
  catalog: "om",
  provider: "openmetadata",
  asset: asset("o", "pg.sales.public.orders", "Every order"),
  description: { current: "", catalog: "Every order placed" },
  columns: [
    { column: "id", current: "", catalog_column: "id" },
    { column: "total", current: "Old total", catalog_column: null },
  ],
  catalog_columns: [
    {
      name: "id",
      description: "The order id",
      tags: [{ name: "PII.Identifier", tag: null }],
      terms: [],
    },
    {
      name: "amount",
      description: "The order total",
      tags: [{ name: "Finance.Revenue", tag: "revenue" }],
      terms: [{ name: "Glossary.Order Total", term: "order total" }],
    },
  ],
  owners: ["Ann Steward"],
  tags: [{ name: "Tier.Gold", tag: "gold" }],
  terms: [{ name: "Glossary.Sales", term: null }],
  current_tags: [],
  current_terms: {},
  imported_at: null,
  locked: false,
};

describe("CatalogPickModal", () => {
  beforeEach(() => {
    mockCandidates.mockReset();
    mockOffer.mockReset();
    mockApply.mockReset();
  });

  it("preselects the same physical table and applies only what the steward accepted", async () => {
    mockCandidates.mockResolvedValue(LISTS);
    mockOffer.mockResolvedValue(OFFER);
    mockApply.mockResolvedValue({ description: "Every order placed" });
    const onApplied = vi.fn();
    render(<CatalogPickModal tableId={7} opened onClose={vi.fn()} onApplied={onApplied} />);

    expect(await screen.findByTestId("catalog-pick-asset-o")).toBeChecked();
    fireEvent.click(screen.getByTestId("catalog-pick-next"));

    await screen.findByTestId("catalog-offer");
    expect(mockOffer).toHaveBeenCalledWith(7, "om", "o");
    expect(screen.getByTestId("catalog-offer-owners")).toHaveTextContent("Ann Steward");
    expect(screen.getByTestId("catalog-offer-description")).toBeChecked();
    // A tag Provisa has is offered; one it does not have is listed, not offered.
    fireEvent.click(screen.getByTestId("catalog-offer-tag--gold"));
    expect(screen.queryByTestId("catalog-offer-tag-id-PII.Identifier")).toBeNull();
    expect(screen.getByText("No tag of this name in Provisa")).toBeInTheDocument();
    expect(screen.getByText("Provisa links glossary terms to columns")).toBeInTheDocument();
    fireEvent.click(screen.getByTestId("catalog-offer-apply"));

    await waitFor(() => expect(mockApply).toHaveBeenCalledTimes(1));
    expect(mockApply.mock.calls[0]).toEqual([
      7,
      "om",
      {
        ref: "o",
        description: true,
        columns: { id: "id" },
        tags: [{ column: null, tag: "gold" }],
        terms: {},
      },
    ]);
    expect(onApplied).toHaveBeenCalledWith("om", { description: "Every order placed" });
  });

  it("maps a column to the catalog column the steward chooses, with its tags and terms", async () => {
    mockCandidates.mockResolvedValue(LISTS);
    mockOffer.mockResolvedValue(OFFER);
    mockApply.mockResolvedValue({});
    render(<CatalogPickModal tableId={7} opened onClose={vi.fn()} onApplied={vi.fn()} />);

    fireEvent.click(await screen.findByTestId("catalog-pick-next"));
    await screen.findByTestId("catalog-offer");
    fireEvent.click(screen.getByTestId("catalog-offer-description"));
    fireEvent.click(screen.getByTestId("catalog-offer-take-id"));
    const mapTotal = screen.getByTestId("catalog-offer-map-total");
    fireEvent.click(mapTotal);
    // Each column's Select has its own list; the one this input controls is the one chosen from.
    const list = await waitFor(() => {
      const found = document.getElementById(mapTotal.getAttribute("aria-controls") ?? "");
      expect(found).not.toBeNull();
      return found as HTMLElement;
    });
    fireEvent.click(within(list).getByRole("option", { name: "amount", hidden: true }));
    fireEvent.click(screen.getByTestId("catalog-offer-take-total"));
    fireEvent.click(screen.getByTestId("catalog-offer-tag-total-revenue"));
    fireEvent.click(screen.getByTestId("catalog-offer-term-total-order total"));
    fireEvent.click(screen.getByTestId("catalog-offer-apply"));

    await waitFor(() => expect(mockApply).toHaveBeenCalledTimes(1));
    expect(mockApply.mock.calls[0][2]).toEqual({
      ref: "o",
      description: false,
      columns: { total: "amount" },
      tags: [{ column: "total", tag: "revenue" }],
      terms: { total: "order total" },
    });
  });

  it("shows a catalog that could not be read beside the others", async () => {
    mockCandidates.mockResolvedValue(LISTS);
    render(<CatalogPickModal tableId={7} opened onClose={vi.fn()} onApplied={vi.fn()} />);

    fireEvent.click(await screen.findByTestId("catalog-pick-tab-collibra"));
    expect(await screen.findByTestId("catalog-pick-list-error")).toHaveTextContent("refused");
    expect(screen.getByTestId("catalog-pick-next")).toBeDisabled();
  });

  it("picks none: nothing is read and nothing applied", async () => {
    mockCandidates.mockResolvedValue(LISTS);
    render(<CatalogPickModal tableId={7} opened onClose={vi.fn()} onApplied={vi.fn()} />);

    fireEvent.click(await screen.findByTestId("catalog-pick-asset-none"));
    expect(screen.getByTestId("catalog-pick-next")).toBeDisabled();
    expect(mockOffer).not.toHaveBeenCalled();
  });

  it("refreshes from the linked asset, with no pick list", async () => {
    mockOffer.mockResolvedValue({
      ...OFFER,
      description: { current: "Every order placed", catalog: "Every order placed" },
    });
    render(
      <CatalogPickModal tableId={7} opened refresh="om" onClose={vi.fn()} onApplied={vi.fn()} />,
    );

    await screen.findByTestId("catalog-offer");
    expect(mockCandidates).not.toHaveBeenCalled();
    expect(mockOffer).toHaveBeenCalledWith(7, "om", undefined);
    // An unchanged description is not offered as a change.
    expect(screen.getByTestId("catalog-offer-description")).not.toBeChecked();
    expect(screen.queryByTestId("catalog-offer-back")).toBeNull();
  });
});
