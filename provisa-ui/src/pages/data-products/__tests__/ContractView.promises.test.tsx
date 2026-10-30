// Copyright (c) 2026 Kenneth Stott
// Canary: 5e0a7f31-c2d8-4b96-a413-8d7b62e9f0c4
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2158: a contract's Promises are a table -- Applies to (the table, its rows or a column), Check
// (plain words) and Details (thresholds in plain words) -- the table's checks first, then its
// rows', then each column's. A checker's own check names and argument records never reach the DOM.

import { describe, expect, it } from "vitest";
import { render, screen, within } from "../../../test-utils/render";
import { ContractView } from "../ContractView";
import type { OdcsContract } from "../../../api/dataProductContracts";
import type { ProductDqEntry, ProductDqRule } from "../../../api/lineage";

const CONTRACT = { id: "c1", version: "1.0.0", status: "active" } as unknown as OdcsContract;

const promise = (id: number, rules: ProductDqRule[]): ProductDqEntry => ({
  checker_table_id: id,
  domain_id: "pet-store",
  table_name: `pets_scan_${id}`,
  alias: null,
  description: `Scan ${id}`,
  owner_product_id: "pet_health",
  checked_table_id: 100,
  checked_table: { id: 100, domain_id: "pet-store", table_name: "pets", alias: null },
  rules,
  latest_outcome: "pass",
  latest_run_at: null,
});
const rule = (check_type: string, column_name: string, args: Record<string, unknown> = {}) => ({
  check_type,
  column_name,
  args,
});

// Authored with the columns first, so the table's order is the view's, not the data's.
const PROMISES = [
  promise(1, [
    rule("expect_column_values_to_not_be_null", "owner_email"),
    rule("expect_column_values_to_be_between", "price", {
      min_value: 0,
      max_value: 500,
      mostly: 0.95,
    }),
    rule("expect_column_values_to_be_in_set", "species", { value_set: ["dog", "cat"] }),
  ]),
  promise(2, [
    rule("expect_column_pair_values_a_to_be_greater_than_b", "", {
      column_A: "adopted_at",
      column_B: "born_at",
    }),
    rule("expect_table_row_count_to_be_between", "", { min_value: 1 }),
  ]),
  promise(3, [
    rule("missing", "name", { threshold: { must_be: 0 } }),
    rule("row_count", "", { threshold: { must_be_greater_than: 10 } }),
    rule("duplicate", "id", { threshold: { must_be_less_than: 2, metric: "percent" } }),
  ]),
];

const rows = () =>
  Array.from(screen.getByTestId("data-contract-promises-table-7").querySelectorAll("tbody tr")).map(
    (tr) => Array.from(tr.querySelectorAll("td")).map((td) => td.textContent),
  );

function renderView() {
  render(
    <ContractView
      contract={CONTRACT}
      versions={[]}
      tableId={7}
      promises={PROMISES}
      consumers={[]}
    />,
  );
}

describe("a contract's promises table", () => {
  it("heads Applies to, Check and Details", () => {
    renderView();
    const heads = Array.from(
      screen.getByTestId("data-contract-promises-table-7").querySelectorAll("thead th"),
    ).map((th) => th.textContent);
    expect(heads.slice(0, 3)).toEqual(["Applies to", "Check", "Details"]);
  });

  it("applies to the table first, then the rows, then each column by name", () => {
    renderView();
    expect(rows().map((r) => r[0])).toEqual([
      "Table",
      "Table",
      "Row",
      "owner_email",
      "price",
      "species",
      "name",
      "id",
    ]);
  });

  it("names each check and its thresholds in plain words", () => {
    renderView();
    expect(rows().map((r) => [r[1], r[2]])).toEqual([
      ["Row count", "At least 1"],
      ["Row count", "Greater than 10"],
      ["Column order", "adopted_at greater than born_at"],
      ["Not null", ""],
      ["Value range", "From 0 to 500; At least 95% of rows"],
      ["Allowed values", "One of dog, cat"],
      ["No missing values", "Must be 0"],
      ["No duplicates", "Less than 2%"],
    ]);
  });

  it("shows no checker name and no JSON", () => {
    renderView();
    const text = screen.getByTestId("data-contract-promises-7").textContent ?? "";
    expect(text).not.toMatch(/expect_|value_set|min_value|must_be|row_count|[{}[\]"]/);
  });

  it("keeps each row's link to its promise's results", () => {
    renderView();
    const row = screen.getByTestId("data-contract-promises-table-7-item-1-0");
    expect(within(row).getByRole("button")).toBeInTheDocument();
  });

  it("refuses a check type it has no plain name for", () => {
    expect(() =>
      render(
        <ContractView
          contract={CONTRACT}
          versions={[]}
          tableId={7}
          promises={[promise(9, [rule("expect_something_new", "")])]}
          consumers={[]}
        />,
      ),
    ).toThrow(/expect_something_new/);
  });
});
