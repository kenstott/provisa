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
import type { CoreCheck, CoreEdge, CoreTable, ProductCore } from "../productCore";
import type { DataProduct } from "../../../types/admin";

vi.mock("../../../components/graph3d/KnowledgeGraphPanel", () => ({
  KnowledgeGraphPanel: () => null,
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
  description: null,
  dq_contract: null,
  ...over,
});

const check = (
  checker: number,
  state: CoreCheck["state"],
  checked: [number, string],
  over: Partial<NonNullable<CoreCheck["checker"]>> = {},
  assigned: string | null = "c360",
): CoreCheck => ({
  id: `c360:${checker}`,
  state,
  checker_table_id: checker,
  checked_table_id: checked[0],
  assigned_product_id: assigned,
  checker: {
    id: checker,
    source_id: "soda",
    domain_id: "sales",
    table_name: `check_${checker}`,
    description: `Claim ${checker}`,
    dq_contract: "dataset: x/y/z",
    latest_outcome: "pass",
    latest_run_at: "2026-10-20T10:00:00Z",
    ...over,
  },
  checked_table: { id: checked[0], domain_id: "sales", table_name: checked[1] },
});

const CHECKS: CoreCheck[] = [
  check(11, "promise", [1, "customers"], {
    description: "Customers are unique so counts reconcile",
  }),
  check(12, "promise", [2, "orders"], {
    description: "Orders are complete so revenue reconciles",
    latest_outcome: "fail",
  }),
  check(13, "expectation", [30, "raw_orders"], { latest_outcome: null, latest_run_at: null }),
  check(14, "partner_dq", [31, "raw_customers"], {}, null),
  check(15, "orphan", [99, "fx_rates"]),
];

const input = (id: number, relation: string): CoreEdge => ({
  kind: "lineage",
  direction: "upstream",
  target_product_id: null,
  target_table_id: id,
  target_relation: relation,
  direct: true,
  hops: 1,
  path_count: 1,
  aliases: null,
  supporting_tables: null,
  vias: null,
  nearest_input: true,
});

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
  checks: CHECKS,
  edges: [input(30, "raw.raw_orders"), input(31, "raw.raw_customers")],
  queries: { graphql: "", sql: "", jsonapi: "" },
};

function panel() {
  render(
    <DataProductDetailPanel
      p={P}
      core={CORE}
      coreError={null}
      reader={null}
      sources={[]}
      internalRelationships={[]}
      canEdit={false}
      canSeeLineage={true}
      inputPorts={[]}
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

  it("lists partner DQ apart and warns of an orphan, naming the table it scans", () => {
    panel();
    expect(screen.getByTestId("data-product-detail-dq-c360-partners")).toHaveTextContent(
      "assigned to no product",
    );
    expect(screen.getByTestId("data-product-detail-dq-c360-orphan-15")).toHaveTextContent(
      "sales.fx_rates",
    );
  });

  it("headlines a checker's claim in the Data Quality section", () => {
    panel();
    expect(screen.getByTestId("data-product-detail-dq-claim-11")).toHaveTextContent(
      "Customers are unique so counts reconcile",
    );
  });
});

describe("a set of checks' summary status", () => {
  it("is some failing, else not yet run, else all passing; unknown when the runs are not read", () => {
    expect(dqStatus(CHECKS.slice(0, 2))).toBe("failing");
    expect(dqStatus([CHECKS[0], CHECKS[2]])).toBe("notRun");
    expect(dqStatus([CHECKS[0]])).toBe("passing");
    const unread = { ...CHECKS[0], checker: { ...CHECKS[0].checker!, latest_outcome: undefined } };
    expect(dqStatus([unread])).toBeNull();
  });
});
