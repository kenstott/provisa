// Copyright (c) 2026 Kenneth Stott
// Canary: b7d0a160-7640-41cc-8d1e-5fb7e20b7573
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2240: a graph's link to a thing is the link its page writes for that thing's panel -- built
// by detailHref from DETAIL_VIEWS, the one place a detail URL is made.

import { describe, expect, it } from "vitest";
import { detailHref } from "../../components/list/useDetailParam";
import { entityHref } from "../entityHref";

describe("entityHref is detailHref for each thing with a panel", () => {
  it.each([
    ["term", entityHref("term", 7), detailHref("ontology", 7)],
    ["product", entityHref("product", "dp"), detailHref("products", "dp")],
    ["command", entityHref("command", "refund"), detailHref("commands", "refund")],
    ["table", entityHref("table", 3), detailHref("tables", 3)],
    [
      "column",
      entityHref("column", "amount", 3),
      detailHref("tables", 3, "columns", { column: "amount" }),
    ],
    ["checker", entityHref("checker", 3), detailHref("tables", 3, "quality")],
    ["profile", entityHref("profile", "p1", 3), detailHref("tables", 3, "quality")],
    ["domain", entityHref("domain", "sales"), detailHref("domains", "sales")],
    ["source", entityHref("source", "pg"), detailHref("sources", "pg")],
    ["relationship", entityHref("relationship", "a-b"), detailHref("relationships", "a-b")],
    ["tag", entityHref("tag", "pii"), detailHref("tags", "pii")],
  ])("%s", (_, built, expected) => {
    expect(built).toBe(expected);
  });

  it("marks a column on its table's Columns tab", () => {
    expect(entityHref("column", "amount", 3)).toBe("/tables?table=3&section=columns&column=amount");
  });
});
