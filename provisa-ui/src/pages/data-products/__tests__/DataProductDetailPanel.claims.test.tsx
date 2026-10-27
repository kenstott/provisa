// Copyright (c) 2026 Kenneth Stott
// Canary: 8e2d4b17-5c93-4f60-a7e1-d9b3c6f8a245
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2132, as its scenario sets it: a product with two checker tables on its output ports (one
// whose latest run passed, one whose latest run failed, each with its reason), one on an input it
// is assigned, one on another input assigned to no product, and one assigned to it on a table it
// neither reads nor serves.

import { describe, expect, it, vi } from "vitest";
import { fireEvent, render, screen, within } from "../../../test-utils/render";
import { DataProductDetailPanel } from "../DataProductDetailPanel";
import { dqStatus } from "../dataProductClaims";
import type { ProductDq, ProductDqEntry, ProductInput, ProductLineage } from "../../../api/lineage";
import type { CoreEdge, CoreTable, ProductCore } from "../productCore";
import type { DataProduct } from "../../../types/admin";

vi.mock("../../../components/graph3d/GraphPanel", () => ({
  GraphPanel: () => null,
}));

const P = {
  id: "c360",
  domainId: "sales",
  name: "Customer 360",
  purpose: "",
  limitations: "",
  usage: "",
  customProperties: {},
} as DataProduct;

const table = (id: number, name: string, over: Partial<CoreTable> = {}): CoreTable => ({
  id,
  source_id: "pg",
  domain_id: "sales",
  schema_name: "public",
  table_name: name,
  alias: null,
  description: null,
  dq_contract: null,
  ...over,
});

const check = (
  checker: number,
  checked: [number, string],
  over: Partial<ProductDqEntry> = {},
  owner: string | null = "c360",
): ProductDqEntry => ({
  checker_table_id: checker,
  domain_id: "sales",
  table_name: `check_${checker}`,
  alias: null,
  description: `Claim ${checker}`,
  owner_product_id: owner,
  checked_table_id: checked[0],
  checked_table: { id: checked[0], domain_id: "sales", table_name: checked[1], alias: null },
  latest_outcome: "pass",
  latest_run_at: "2026-10-20T10:00:00Z",
  ...over,
});

// REQ-2157: the product lineage endpoint's lists.
const DQ: ProductDq = {
  promises: [
    check(11, [1, "customers"], { description: "Customers are unique so counts reconcile" }),
    check(12, [2, "orders"], {
      description: "Orders are complete so revenue reconciles",
      latest_outcome: "fail",
    }),
  ],
  expectations: [check(13, [30, "raw_orders"], { latest_outcome: null, latest_run_at: null })],
  related: [check(14, [31, "raw_customers"], {}, null)],
  unattached: [check(15, [99, "fx_rates"])],
};

// REQ-2126: the product's nearest physical inputs, as the product lineage endpoint finds them.
const input = (id: number, relation: string): ProductInput => ({
  relation,
  kind: "source_table",
  table_id: id,
  domain_id: relation.split(".")[0],
  table_name: relation.split(".")[1],
  alias: null,
  product_id: null,
  source_id: null,
  source_type: null,
  source_schema: null,
  source_table: null,
});
const lineage = (dq: ProductDq): ProductLineage => ({
  nodes: [],
  edges: [],
  outputs: [],
  members: [],
  // REQ-2160: one hop downstream, a view of the product's and another product's member.
  consumers: [
    {
      relation: "sales.revenue_daily",
      kind: "view",
      table_id: 70,
      domain_id: "sales",
      table_name: "revenue_daily",
      alias: null,
      product_id: null,
    },
    {
      relation: "finance.ledger",
      kind: "materialized_view",
      table_id: 71,
      domain_id: "finance",
      table_name: "ledger",
      alias: "gl",
      product_id: "billing",
    },
  ],
  inputs: [input(30, "raw.raw_orders"), input(31, "raw.raw_customers")],
  dq,
});
const LINEAGE = lineage(DQ);

const CORE: ProductCore = {
  product: P,
  tables: [
    table(1, "customers"),
    table(2, "orders"),
    table(11, "check_11", {
      dq_contract: "dataset: x",
      description: "Customers are unique so counts reconcile",
    }),
  ],
  queries: { graphql: "", sql: "", jsonapi: "" },
};

function panel() {
  render(
    <DataProductDetailPanel
      p={P}
      core={CORE}
      coreError={null}
      reader={null}
      lineageLoading={false}
      lineageError=""
      lineageGraph={LINEAGE}
      canEdit={false}
      canDelete={false}
      owner
      onEdit={vi.fn()}
      onDelete={vi.fn()}
    />,
  );
}

describe("a data product's quality claims (REQ-2132)", () => {
  it("Promises on Output Ports: 2 claims, some failing; the list says each one", async () => {
    panel();
    const promises = screen.getByTestId("data-product-detail-promises-c360");
    expect(promises).toHaveTextContent("2 promises");
    expect(promises).toHaveTextContent("some failing");
    fireEvent.click(promises);
    const list = await screen.findByTestId("data-product-detail-promises-c360-list");
    const row = within(list).getByTestId("data-product-detail-promises-c360-list-row-12");
    expect(row).toHaveTextContent("Orders are complete so revenue reconciles");
    expect(row).toHaveTextContent("sales.orders");
    expect(row).toHaveTextContent("failed");
    expect(within(list).getAllByRole("row")).toHaveLength(3); // the header and the two claims
  });

  it("Expectations on Inputs: 1 claim, not yet run", () => {
    panel();
    const expectations = screen.getByTestId("data-product-detail-expectations-c360");
    expect(expectations).toHaveTextContent("1 expectation");
    expect(expectations).toHaveTextContent("not yet run");
    expect(screen.getByTestId("data-product-detail-inputs-c360")).toHaveTextContent(
      "raw.raw_orders",
    );
  });

  it("each dataset with checks shows a DQ icon coloured by their latest result", () => {
    panel();
    expect(screen.getByTestId("data-product-detail-port-dq-1")).toHaveAttribute(
      "data-status",
      "passing",
    );
    expect(screen.getByTestId("data-product-detail-port-dq-2")).toHaveAttribute(
      "data-status",
      "failing",
    );
    expect(screen.getByTestId("data-product-detail-inputs-c360-dq-30")).toHaveAttribute(
      "data-status",
      "notRun",
    );
    // A partner's check is not the product's expectation.
    expect(screen.queryByTestId("data-product-detail-inputs-c360-dq-31")).toBeNull();
  });

  it("lists related checks in their own row and warns of an unattached one, naming its table", () => {
    panel();
    expect(screen.getByTestId("data-product-detail-dq-related-c360")).toHaveTextContent(
      "assigned to no product",
    );
    expect(screen.getByTestId("data-product-detail-dq-c360-orphan-15")).toHaveTextContent(
      "sales.fx_rates",
    );
  });

  it("shows an unassigned checker scanning a member as related, not as a member", () => {
    const partner = check(16, [1, "customers"], {}, null);
    render(
      <DataProductDetailPanel
        p={P}
        core={CORE}
        coreError={null}
        reader={null}
        lineageLoading={false}
        lineageError=""
        lineageGraph={lineage({ ...DQ, related: [...DQ.related, partner] })}
        canEdit={false}
        canDelete={false}
        owner
        onEdit={vi.fn()}
        onDelete={vi.fn()}
      />,
    );
    expect(screen.getByTestId("data-product-detail-dq-related-c360")).toHaveTextContent("Claim 16");
    expect(screen.getByTestId("data-product-detail-dq-promises-c360")).not.toHaveTextContent(
      "check_16",
    );
    expect(screen.getByTestId("data-product-detail-tables-c360")).not.toHaveTextContent("check_16");
  });

  it("REQ-2157: the data quality section is three rows, Expectations, Promises and Related", () => {
    panel();
    const row = (name: string) => screen.getByTestId(`data-product-detail-dq-${name}-c360`);
    const toggles = ["expectations", "promises", "related"].map((name) =>
      screen.getByTestId(`data-product-detail-dq-${name}-c360-toggle`),
    );
    expect(toggles.map((t) => t.textContent)).toEqual([
      expect.stringMatching(/^Expectations1not yet run/),
      expect.stringMatching(/^Promises2some failing/),
      expect.stringMatching(/^Related1all passing/),
    ]);
    // The product's check on its input, its checks on its members, another's (or no one's).
    expect(row("expectations")).toHaveTextContent("scans sales.raw_orders");
    expect(row("promises")).toHaveTextContent("Customers are unique so counts reconcile");
    expect(row("promises")).toHaveTextContent("Orders are complete so revenue reconciles");
    expect(row("related")).toHaveTextContent("scans sales.raw_customers");
    expect(row("expectations")).not.toHaveTextContent("Claim 14");
  });

  it("headlines a checker's claim in the Data Quality section", () => {
    panel();
    expect(screen.getByTestId("data-product-detail-dq-claim-11")).toHaveTextContent(
      "Customers are unique so counts reconcile",
    );
  });
});

describe("the owner's view and any other reader's (REQ-2160)", () => {
  const view = (owner: boolean, canEdit = true) =>
    render(
      <DataProductDetailPanel
        p={P}
        core={CORE}
        coreError={null}
        reader={null}
        lineageLoading={false}
        lineageError=""
        lineageGraph={LINEAGE}
        canEdit={canEdit}
        canDelete={canEdit}
        owner={owner}
        onEdit={vi.fn()}
        onDelete={vi.fn()}
      />,
    );

  it("shows the owner its expectations, related and unattached checks and its inputs", () => {
    view(true);
    for (const row of ["expectations", "promises", "related"])
      expect(screen.getByTestId(`data-product-detail-dq-${row}-c360`)).toBeInTheDocument();
    expect(screen.getByTestId("data-product-detail-dq-c360-orphan-15")).toBeInTheDocument();
    expect(screen.getByTestId("data-product-card-inputPorts-c360")).toBeInTheDocument();
    // Its consumers one hop downstream, each by its model name, what it is and its product.
    const consumers = screen.getByTestId("data-product-detail-consumers-c360");
    expect(consumers).toHaveTextContent("sales.revenue_daily (view)");
    expect(consumers).toHaveTextContent("finance.gl (materialized view · in billing)");
  });

  it("shows any other reader the product's promises, and none of the owner's part", () => {
    view(false);
    expect(screen.getByTestId("data-product-detail-dq-promises-c360")).toBeInTheDocument();
    expect(screen.queryByTestId("data-product-detail-dq-expectations-c360")).toBeNull();
    expect(screen.queryByTestId("data-product-detail-dq-related-c360")).toBeNull();
    expect(screen.queryByTestId("data-product-detail-dq-c360-orphan-15")).toBeNull();
    expect(screen.queryByTestId("data-product-card-inputPorts-c360")).toBeNull();
    expect(screen.queryByTestId("data-product-card-consumers-c360")).toBeNull();
    // Its promises on its output ports, as the owner sees them.
    expect(screen.getByTestId("data-product-detail-promises-c360")).toHaveTextContent("2 promises");
  });
});

describe("editing a data product follows the right to edit it, not the view (REQ-2160)", () => {
  const view = (owner: boolean, canEdit: boolean) =>
    render(
      <DataProductDetailPanel
        p={P}
        core={CORE}
        coreError={null}
        reader={null}
        lineageLoading={false}
        lineageError=""
        lineageGraph={LINEAGE}
        canEdit={canEdit}
        canDelete={canEdit}
        owner={owner}
        onEdit={vi.fn()}
        onDelete={vi.fn()}
      />,
    );

  it("offers Edit and Delete to a steward holding data_product_rw who does not own the product", () => {
    view(false, true);
    expect(screen.getByTestId("data-product-detail-edit-c360")).toBeInTheDocument();
    expect(screen.getByTestId("data-product-detail-delete-c360")).toBeInTheDocument();
    // The view is still the reader's, not the owner's.
    expect(screen.queryByTestId("data-product-detail-dq-expectations-c360")).toBeNull();
  });

  it("offers neither to an owner without the right", () => {
    view(true, false);
    expect(screen.queryByTestId("data-product-detail-edit-c360")).toBeNull();
    expect(screen.queryByTestId("data-product-detail-delete-c360")).toBeNull();
  });
});

describe("a set of checks' summary status", () => {
  it("is some failing, else not yet run, else all passing; none when there are no checks", () => {
    expect(dqStatus(DQ.promises)).toBe("failing");
    expect(dqStatus([DQ.promises[0], DQ.expectations[0]])).toBe("notRun");
    expect(dqStatus([DQ.promises[0]])).toBe("passing");
    expect(dqStatus([])).toBeNull();
  });
});

describe("Related Tables (REQ-2126)", () => {
  it("lists the directly related tables only; the indirect joins are the graph's", () => {
    const edge = (over: Partial<CoreEdge>): CoreEdge => ({
      id: `c360:related:${over.target_table_id}`,
      kind: "related",
      target_product_id: null,
      target_table_id: null,
      direct: true,
      hops: 1,
      path_count: 1,
      aliases: null,
      supporting_tables: null,
      ...over,
    });
    render(
      <DataProductDetailPanel
        p={P}
        core={{
          ...CORE,
          edges: [
            edge({
              target_table_id: 61,
              target_product_id: "billing",
              path_count: 2,
              aliases: "BILLED_TO, PAID_BY",
              target_table: {
                id: 61,
                domain_id: "finance",
                table_name: "invoices",
                alias: null,
                description: null,
              },
              target_product: { id: "billing", name: "Billing", domain_id: "finance" },
            }),
            edge({
              kind: "relationship",
              direct: false,
              hops: 2,
              target_table_id: 51,
              supporting_tables: "finance.currencies",
              target_table: {
                id: 51,
                domain_id: "finance",
                table_name: "fx_rates",
                alias: null,
                description: null,
              },
            }),
          ],
        }}
        coreError={null}
        reader={null}
        lineageLoading={false}
        lineageError=""
        lineageGraph={null}
        canEdit={false}
        canDelete={false}
        owner
        onEdit={vi.fn()}
        onDelete={vi.fn()}
      />,
    );
    const related = screen.getByTestId("data-product-detail-related-tables-c360");
    expect(related).toHaveTextContent("finance.invoices");
    expect(related).toHaveTextContent("in Billing");
    expect(related).toHaveTextContent("BILLED_TO, PAID_BY · 2 paths");
    expect(related).not.toHaveTextContent("fx_rates");
  });
});
