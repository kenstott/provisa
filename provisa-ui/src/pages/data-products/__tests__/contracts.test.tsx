// Copyright (c) 2026 Kenneth Stott
// Canary: 42e7b83f-eb72-44d1-ab71-6ca34f1b1d84
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2158: a contract's ODCS document, as the panel shows it: each section a card, every element
// declared, derived or not declared. REQ-2180 CONTRACTS ARE DERIVED FROM THE OFFERS: the contracts
// are each offer's on each dataset, read from the offer's contract route when opened; none is
// written by hand, so there is no contract editor.

import { describe, it, expect, vi, beforeEach } from "vitest";
import { fireEvent, render, screen } from "../../../test-utils/render";
import { DataProductDetailPanel } from "../DataProductDetailPanel";
import type { ProductCore } from "../productCore";
import { lifecycleProps } from "./lifecycleProps";
import type { DataProduct } from "../../../types/admin";
import type { OdcsContract } from "../../../api/dataProductContracts";
import type { Offer } from "../../../api/dataProductOffers";

vi.mock("../../../components/OwnerResolution", () => ({ OwnerResolutionInline: () => null }));
vi.mock("../../../components/graph3d/OntologyGraph", () => ({
  OntologySnippet: () => null,
}));
vi.mock("../../tables/ColumnGlossaryHover", () => ({
  ColumnGlossaryHover: ({ children }: { children: unknown }) => children,
}));
// REQ-2180: a contract's document is read from its offer's route when it opens.
const fetchProductContract =
  vi.fn<(id: string, offer: string, member: string) => Promise<OdcsContract>>();
vi.mock("../../../api/dataProductContracts", () => ({
  fetchProductContract: (id: string, offer: string, member: string) =>
    fetchProductContract(id, offer, member),
  fetchContractVersions: () => Promise.resolve([]),
}));
// The product's one offer, giving a contract on the daily revenue and none on the checker.
const OFFER = {
  id: 1,
  product_id: "revenue",
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
  published_by: null,
  review: null,
  created_at: "2026-10-01T00:00:00Z",
  updated_at: "2026-10-01T00:00:00Z",
  summary: { extension: null },
  query_now: false,
  contracts: [
    {
      table_id: 7,
      table: "sales.daily_revenue",
      contract: {
        version: "2.0.0",
        created_at: "2026-10-01T00:00:00Z",
        versions: [],
        terms: {
          fields: [{ name: "day", type: "date", description: null, masked: null }],
          row_rules: [],
          data_level: "full",
          sample_views: [],
          checks: [],
          dq_results: true,
          profiles: [],
          duration_days: null,
          purpose: "",
          usage: "",
          limitations: "",
        },
      },
    },
  ],
} as Offer;
// Every section is mounted (Lineage and Related only once expanded): the reads the others make
// answer nothing here.
vi.mock("../../../api/dataProductOffers", () => ({
  fetchOffers: () => Promise.resolve([OFFER]),
  fetchBids: () => Promise.resolve([]),
  fetchProductProfiles: () => Promise.resolve([]),
}));

const PRODUCT = {
  id: "revenue",
  domainId: "sales",
  name: "Revenue",
  purpose: "",
  limitations: "",
  usage: "",
  customProperties: {},
} as unknown as DataProduct;

function core(): ProductCore {
  return {
    product: PRODUCT,
    queries: { graphql: "", sql: "", jsonapi: "" },
    tables: [
      {
        id: 7,
        sourceId: "wh",
        domainId: "sales",
        schemaName: "public",
        tableName: "daily_revenue",
        description: null,
        dqContract: null,
      },
      // A checker the product owns: no contract, no output port.
      {
        id: 8,
        sourceId: "dq",
        domainId: "sales",
        schemaName: "public",
        tableName: "revenue_checks",
        description: null,
        dqContract: "checks: []",
      },
    ],
  } as unknown as ProductCore;
}

describe("a product's contracts", () => {
  it("open to the product's card panels, each section a card of its own", async () => {
    await opened(ODCS);
    expect(screen.getByTestId("offer-1-contract-7-version")).toHaveTextContent("v2.0.0");
    const cards = ["details", "fields", "sla", "quality", "versions"];
    for (const card of [...cards, "support", "howToUse", "pricing", "custom"]) {
      expect(screen.getByTestId(`data-contract-7-${card}`)).toBeInTheDocument();
    }
    // A record is a table row, never a line of its fields.
    const support = screen.getByTestId("data-contract-support-7");
    expect(support).toHaveTextContent("channel");
    expect(support).toHaveTextContent("mailto:revenue@example.com");
    expect(support).not.toHaveTextContent("channel: support");
    const roles = rowTexts("data-contract-roles-7");
    expect(roles).toEqual([
      ["analyst", "read"],
      ["finance", "write"],
    ]);
    expect(rowTexts("data-contract-how-to-use-7").map((row) => row[0])).toEqual([
      "PostgreSQL",
      "REST",
    ]);
    // Nothing declared in a panel: its one line, alone.
    expect(screen.getByTestId("data-contract-7-pricing-not-declared")).toHaveTextContent(
      "Not declared: priceAmount, priceCurrency, priceUnit",
    );
    expect(screen.getByTestId("data-contract-7-custom-not-declared")).toHaveTextContent(
      "Not declared: customProperties",
    );
    expect(screen.getByTestId("data-contract-7-custom")).toHaveTextContent("tagsfinance");
    // The checker has no contract, so no row.
    expect(screen.queryByTestId("offer-1-contract-8-toggle")).toBeNull();
    expect(screen.queryByTestId("data-contract-8")).toBeNull();
    // The Columns card is one row per property; the SLA card each service level.
    expect(screen.getByTestId("data-contract-7-fields")).toHaveTextContent(
      "physicalNamewh.public.daily_revenue",
    );
    expect(rowTexts("data-contract-sla-7")).toEqual([["availability", "99.9", "%"]]);
    // The Promises card is a table of the product's promises, never a record of raw rules; this
    // port has none, so it says so.
    const promises = screen.getByTestId("data-contract-7-quality");
    expect(promises).toHaveTextContent("Not declared");
    expect(promises).not.toHaveTextContent("nullValues");
  });

  it("open to every element, declared, derived or not declared", async () => {
    await opened(ODCS);
    // One row a property, every element it declares a column of that row.
    expect(screen.getByTestId("data-contract-columns-7-day")).toHaveTextContent(
      "required, unique, primaryKey",
    );
    const amount = screen.getByTestId("data-contract-columns-7-amount");
    for (const shown of [
      "NUMERIC(12, 2)",
      "restricted",
      "The day's total",
      "type: library · metric: nullValues · mustBe: 0",
      "Total revenue",
    ]) {
      expect(amount).toHaveTextContent(shown);
    }
    // An element no property declares folds into the panel's line; it is not a column.
    expect(screen.getByTestId("data-contract-7-fields-not-declared")).toHaveTextContent("examples");
    const heads = screen.getByTestId("data-contract-columns-7").querySelectorAll("thead th");
    expect(Array.from(heads).map((h) => h.textContent)).not.toContain("examples");
    const details = screen.getByTestId("data-contract-7-details");
    expect(details).toHaveTextContent("dataProductRevenue");
    // tenant is declared on the contract (REQ-2158), and this one declares none.
    expect(screen.getByTestId("data-contract-7-details-not-declared")).toHaveTextContent(
      "Not declared: tenant, purpose, limitations, usage",
    );
  });

  it("say why one could not be read, in its row", async () => {
    fetchProductContract.mockRejectedValue(new Error("no such product"));
    await openDocument();
    expect(await screen.findByTestId("data-contract-error-7")).toHaveTextContent("no such product");
  });
});

/** The member's ODCS contract, as the contracts endpoint serves it. */
const ODCS: OdcsContract = {
  apiVersion: "v3.1.0",
  kind: "DataContract",
  id: "provisa://acme/sales/tables/daily_revenue",
  name: "daily_revenue",
  version: "2.0.0",
  status: "active",
  domain: "sales",
  dataProduct: "Revenue",
  schema: [
    {
      name: "daily_revenue",
      physicalName: "wh.public.daily_revenue",
      physicalType: "table",
      properties: [
        {
          name: "day",
          logicalType: "date",
          physicalType: "DATE",
          required: true,
          unique: true,
          primaryKey: true,
        },
        {
          name: "amount",
          logicalType: "number",
          physicalType: "NUMERIC(12, 2)",
          classification: "restricted",
          description: "The day's total",
          businessName: "Total revenue",
          quality: [{ type: "library", metric: "nullValues", mustBe: 0 }],
        },
      ],
    },
  ],
  slaProperties: [{ property: "availability", value: 99.9, unit: "%" }],
  support: [{ channel: "support", url: "mailto:revenue@example.com" }],
  roles: [
    { role: "analyst", access: "read" },
    { role: "finance", access: "write" },
  ],
  tags: ["finance"],
  servers: [
    { server: "pgwire", type: "postgres" },
    { server: "rest", type: "api" },
  ],
};

beforeEach(() => {
  localStorage.clear();
  fetchProductContract.mockReset();
});

/** A table's body rows, each as its cells' text. */
const rowTexts = (testId: string) =>
  Array.from(screen.getByTestId(testId).querySelectorAll("tbody tr")).map((row) =>
    Array.from(row.querySelectorAll("td")).map((cell) => cell.textContent),
  );

/** The panel with its offer's contract on the daily revenue opened to its document. */
async function openDocument() {
  panel();
  fireEvent.click(await screen.findByTestId("offer-card-1-toggle"));
  fireEvent.click(await screen.findByTestId("offer-1-contract-7-toggle"));
  fireEvent.click(await screen.findByTestId("offer-1-contract-7-document-toggle"));
}

/** The same, the contract served as ``odcs``. */
async function opened(odcs: OdcsContract) {
  fetchProductContract.mockResolvedValue(odcs);
  await openDocument();
  await screen.findByTestId("data-contract-7");
  expect(fetchProductContract).toHaveBeenCalledWith(
    "revenue",
    "default",
    "wh.public.daily_revenue",
  );
}

function panel() {
  render(
    <DataProductDetailPanel
      p={PRODUCT}
      core={core()}
      coreError={null}
      reader={null}
      canEdit={false}
      canDelete={false}
      owner
      seesConsumers
      lineageLoading={false}
      lineageError=""
      lineageGraph={null}
      onEdit={vi.fn()}
      onDelete={vi.fn()}
      {...lifecycleProps()}
    />,
  );
}
