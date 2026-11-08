// Copyright (c) 2026 Kenneth Stott
// Canary: 42e7b83f-eb72-44d1-ab71-6ca34f1b1d84
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2158: each member's contract, its output port, read with the product's panel core: the panel
// shows it, and the form edits it with the same fields. Only a changed contract is saved.

import { describe, it, expect, vi, beforeEach } from "vitest";
import { useState } from "react";
import { fireEvent, render, screen } from "../../../test-utils/render";
import { DataProductDetailPanel } from "../DataProductDetailPanel";
import { coreContracts, type ProductCore } from "../productCore";
import { ContractEditor } from "../ContractEditor";
import { contractChanged, contractDraft } from "../contractDraft";
import { lifecycleProps } from "./lifecycleProps";
import type { DataProduct } from "../../../types/admin";
import type { DataContractInput } from "../../../hooks/useDataProductMutations";
import type { OdcsContract } from "../../../api/dataProductContracts";

vi.mock("../../../components/OwnerResolution", () => ({ OwnerResolutionInline: () => null }));
vi.mock("../../../components/graph3d/OntologyGraph", () => ({
  OntologySnippet: () => null,
}));
// REQ-2158: a contract row reads its port's contract when it opens.
const fetchProductContract = vi.fn<(id: string, member: string) => Promise<OdcsContract>>();
vi.mock("../../../api/dataProductContracts", () => ({
  fetchProductContract: (id: string, member: string) => fetchProductContract(id, member),
  fetchContractVersions: () => Promise.resolve([]),
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

const STORED = {
  version: "2.0.0",
  status: "active",
  description: "Revenue by day",
  terms: { usage: "Join on day", limitations: null, notice_period: "P3M" },
  sla: [{ property: "availability", value: 99.9, unit: "%", description: null }],
  columns: [
    { name: "day", type: "DATE", description: null, required: true },
    { name: "amount", type: "NUMERIC(12, 2)", description: null, required: false },
  ],
};

function core(contract: unknown): ProductCore {
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
        contract,
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
        contract: null,
      },
    ],
  } as unknown as ProductCore;
}

describe("a product's contracts", () => {
  it("are read from each member that has one, stored JSON or text", () => {
    for (const stored of [STORED, JSON.stringify(STORED)]) {
      const [c, ...rest] = coreContracts(core(stored));
      expect(rest).toEqual([]);
      expect(c.contractId).toBe("revenue/public.daily_revenue");
      expect([c.version, c.status, c.terms.noticePeriod]).toEqual(["2.0.0", "active", "P3M"]);
      expect(c.columns.map((x) => [x.name, x.required])).toEqual([
        ["day", true],
        ["amount", false],
      ]);
    }
  });

  it("open to the product's card panels, each section a card of its own", async () => {
    await opened(ODCS);
    expect(screen.getByTestId("data-contract-version-7")).toHaveTextContent("v2.0.0");
    expect(screen.getByTestId("data-contract-status-7")).toHaveTextContent("Active");
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
    expect(screen.queryByTestId("data-contract-item-8")).toBeNull();
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
    panel();
    fireEvent.click(screen.getByTestId("data-contract-item-7-toggle"));
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

/** The panel with its contract row opened, the contract served as ``odcs``. */
async function opened(odcs: OdcsContract) {
  fetchProductContract.mockResolvedValue(odcs);
  panel();
  fireEvent.click(screen.getByTestId("data-contract-item-7-toggle"));
  await screen.findByTestId("data-contract-7");
  expect(fetchProductContract).toHaveBeenCalledWith("revenue", "wh.public.daily_revenue");
}

function panel() {
  render(
    <DataProductDetailPanel
      p={PRODUCT}
      core={core(STORED)}
      coreError={null}
      reader={null}
      canEdit={false}
      canDelete={false}
      owner
      seesConsumers
      lineageLoading={false}
      lineageError=""
      lineageGraph={null}
      contractsVersion={0}
      onEdit={vi.fn()}
      onDelete={vi.fn()}
      {...lifecycleProps()}
    />,
  );
}

function Editing({ onDraft }: { onDraft: (d: DataContractInput) => void }) {
  const [contract] = coreContracts(core(STORED));
  const [draft, setDraft] = useState(contractDraft(contract));
  return (
    <ContractEditor
      contract={contract}
      draft={draft}
      memberColumns={[
        { name: "day", dataType: "DATE" },
        { name: "amount", dataType: "NUMERIC(12, 2)" },
        { name: "region", dataType: "TEXT" },
      ]}
      onChange={(d) => {
        setDraft(d);
        onDraft(d);
      }}
    />
  );
}

describe("a contract's editor", () => {
  it("leaves an unedited contract unchanged, so it is not saved", () => {
    const [contract] = coreContracts(core(STORED));
    expect(contractChanged(contract, contractDraft(contract))).toBe(false);
    expect(contractChanged(contract, { ...contractDraft(contract), version: "3.0.0" })).toBe(true);
  });

  it("edits the version and keeps each promise's type", () => {
    const drafts: DataContractInput[] = [];
    render(<Editing onDraft={(d) => drafts.push(d)} />);
    fireEvent.change(screen.getByTestId("data-contract-version-input-7"), {
      target: { value: "3.0.0" },
    });
    const last = drafts[drafts.length - 1];
    expect(last.version).toBe("3.0.0");
    expect(last.columns.map((c) => [c.name, c.type])).toEqual([
      ["day", "DATE"],
      ["amount", "NUMERIC(12, 2)"],
    ]);
  });

  it("adds and removes a service level", () => {
    const drafts: DataContractInput[] = [];
    render(<Editing onDraft={(d) => drafts.push(d)} />);
    fireEvent.click(screen.getByTestId("data-contract-sla-add-7"));
    expect(drafts[drafts.length - 1].sla).toHaveLength(2);
    fireEvent.click(screen.getByTestId("data-contract-sla-remove-7-0"));
    expect(drafts[drafts.length - 1].sla.map((s) => s.property)).toEqual([""]);
  });
});
