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

import { describe, it, expect, vi } from "vitest";
import { useState } from "react";
import { fireEvent, render, screen, within } from "../../../test-utils/render";
import { DataProductDetailPanel } from "../DataProductDetailPanel";
import { coreContracts, type ProductCore } from "../productCore";
import { ContractEditor } from "../ContractEditor";
import { contractChanged, contractDraft } from "../contractDraft";
import { lifecycleProps } from "./lifecycleProps";
import type { DataProduct } from "../../../types/admin";
import type { DataContractInput } from "../../../hooks/useDataProductMutations";
import type { OdcsContract } from "../../../api/dataProductContracts";

vi.mock("../../../components/OwnerResolution", () => ({ OwnerResolutionInline: () => null }));
vi.mock("../../../components/graph3d/GraphPanel", () => ({
  GraphPanel: () => null,
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
        source_id: "wh",
        domain_id: "sales",
        schema_name: "public",
        table_name: "daily_revenue",
        description: null,
        dq_contract: null,
        contract,
      },
      // A checker the product owns: no contract, no output port.
      {
        id: 8,
        source_id: "dq",
        domain_id: "sales",
        schema_name: "public",
        table_name: "revenue_checks",
        description: null,
        dq_contract: "checks: []",
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

  it("show every ODCS section as a collapsed row with a one-line summary", () => {
    panel(ODCS);
    expect(screen.getByTestId("data-contract-version-7")).toHaveTextContent("v2.0.0");
    expect(screen.getByTestId("data-contract-status-7")).toHaveTextContent("Active");
    const sections = [
      "fundamentals",
      "schema",
      "quality",
      "sla",
      "support",
      "servers",
      "pricing",
      "custom",
    ];
    for (const section of sections) {
      expect(screen.getByTestId(`data-contract-7-${section}`)).toBeInTheDocument();
    }
    expect(screen.getByTestId("data-contract-7-schema-summary")).toHaveTextContent(
      "wh.public.daily_revenue · 2 properties",
    );
    expect(screen.getByTestId("data-contract-7-quality-summary")).toHaveTextContent("1 rules");
    expect(screen.getByTestId("data-contract-7-servers-summary")).toHaveTextContent("pgwire, rest");
    expect(screen.getByTestId("data-contract-7-pricing-summary")).toHaveTextContent("Not declared");
    // Collapsed until opened: nothing of a section's body is mounted.
    expect(screen.queryByTestId("data-contract-columns-7")).toBeNull();
    expect(screen.queryByTestId("data-contract-8")).toBeNull();
  });

  it("open to every element, declared, derived or not declared", async () => {
    panel(ODCS);
    fireEvent.click(screen.getByTestId("data-contract-7-schema-toggle"));
    const rows = within(await screen.findByTestId("data-contract-columns-7")).getAllByRole("row", {
      hidden: true, // the panel is still opening: Mantine marks it hidden until its transition ends
    });
    expect(rows.map((r) => r.textContent)).toEqual([
      "namelogicalTypephysicalTyperequireduniqueprimaryKeyclassificationdescriptionexamples",
      "daydateDATEYesYesYes",
      "amountnumberNUMERIC(12, 2)restrictedThe day's total",
    ]);
    fireEvent.click(screen.getByTestId("data-contract-7-fundamentals-toggle"));
    const fundamentals = await screen.findByTestId("data-contract-7-fundamentals");
    expect(fundamentals).toHaveTextContent("dataProductRevenue");
    // tenant is declared on the contract (REQ-2158), and this one declares none.
    expect(fundamentals).toHaveTextContent("tenantNot declared");
    fireEvent.click(screen.getByTestId("data-contract-7-sla-toggle"));
    expect(await screen.findByTestId("data-contract-sla-7")).toHaveTextContent(
      "property: availability · value: 99.9 · unit: %",
    );
  });

  it("say why they could not be read", () => {
    panel(null, "no such product");
    expect(screen.getByTestId("data-product-contracts-error-revenue")).toHaveTextContent(
      "no such product",
    );
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
          quality: [{ type: "library", metric: "nullValues", mustBe: 0 }],
        },
      ],
    },
  ],
  slaProperties: [{ property: "availability", value: 99.9, unit: "%" }],
  servers: [
    { server: "pgwire", type: "postgres" },
    { server: "rest", type: "api" },
  ],
};

function panel(odcs: OdcsContract | null, odcsError = "") {
  render(
    <DataProductDetailPanel
      p={PRODUCT}
      core={core(STORED)}
      coreError={null}
      reader={null}
      canEdit={false}
      canDelete={false}
      owner
      lineageLoading={false}
      lineageError=""
      lineageGraph={null}
      odcs={odcs === null ? null : { "wh.public.daily_revenue": odcs }}
      odcsError={odcsError}
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
