// Copyright (c) 2026 Kenneth Stott
// Canary: ef4205df-7275-48a9-a10c-c2cdeb39240d
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2219: the entries of a list as picked in cascading pickers. A pick with nothing chosen
// beneath it stands for all of it; the identifiers stored are those the server matches.

import { describe, it, expect } from "vitest";
import type { NamesNode } from "../api/entitlements";
import {
  choicesAt,
  filterChoices,
  indexNames,
  levelOfUnknown,
  selectionOf,
  toggle,
  valueOf,
} from "../components/entitlements/cascade";

const n = (id: string, children: NamesNode[] = []): NamesNode => ({
  id,
  label: id.split(".").pop() ?? id,
  children,
});
const TREE: NamesNode[] = [
  n("finance", [
    n("finance.costs", [n("finance.costs.amount")]),
    n("finance.revenue", [n("finance.revenue.amount")]),
  ]),
  n("sales", [n("sales.orders", [n("sales.orders.total")])]),
];
const index = indexNames(TREE);

describe("cascading picks", () => {
  it("round-trips a stored list unchanged", () => {
    for (const value of [
      ["finance"],
      ["finance.revenue", "sales"],
      ["finance.costs.amount", "sales.orders"],
      ["*"].slice(1),
    ]) {
      expect(valueOf(selectionOf(value, index, 3), index).sort()).toEqual([...value].sort());
    }
  });

  it("shows a listed table's domain chosen, and stores only the table", () => {
    const sel = selectionOf(["finance.revenue"], index, 2);
    expect(sel.levels).toEqual([["finance"], ["finance.revenue"]]);
    expect(valueOf(sel, index)).toEqual(["finance.revenue"]);
  });

  it("keeps an entry another listed entry covers from narrowing it", () => {
    const sel = selectionOf(["finance.revenue", "finance"], index, 2);
    expect(valueOf(sel, index)).toEqual(["finance"]);
  });

  it("keeps values the tree lacks, and those deeper than the kind, as they are", () => {
    const sel = selectionOf(["legacy.*", "finance.revenue.amount", "*"], index, 2);
    expect(sel.unknown).toEqual(["legacy.*", "finance.revenue.amount"]);
    expect(levelOfUnknown("legacy.*", 2)).toBe(1);
    expect(levelOfUnknown("finance.revenue.amount", 2)).toBe(1);
    expect(levelOfUnknown("gone", 2)).toBe(0);
  });

  it("un-choosing removes what is chosen beneath, and nothing above", () => {
    let sel = selectionOf(["finance.revenue.amount", "finance.costs"], index, 3);
    sel = toggle(sel, "finance.revenue", index);
    expect(valueOf(sel, index)).toEqual(["finance.costs"]);
    sel = toggle(sel, "FINANCE", index);
    expect(valueOf(sel, index)).toEqual([]);
  });

  it("offers the tables of the chosen domains, every table when none is, and columns only of chosen tables", () => {
    const none = selectionOf([], index, 3);
    expect(choicesAt(1, TREE, none, index).map((g) => g.parent)).toEqual(["finance", "sales"]);
    expect(choicesAt(2, TREE, none, index)).toEqual([]);
    const sales = selectionOf(["sales"], index, 3);
    expect(choicesAt(1, TREE, sales, index).map((g) => g.parent)).toEqual(["sales"]);
  });

  it("filters by the dotted name and bounds what is drawn", () => {
    const groups = choicesAt(1, TREE, selectionOf([], index, 2), index);
    expect(
      filterChoices(groups, "REV", 10).groups.flatMap((g) => g.options.map((o) => o.id)),
    ).toEqual(["finance.revenue"]);
    const finance = filterChoices(groups, "finance", 10);
    expect(finance.total).toBe(2);
    const bounded = filterChoices(groups, "", 2);
    expect([bounded.shown, bounded.total]).toEqual([2, 3]);
  });
});
