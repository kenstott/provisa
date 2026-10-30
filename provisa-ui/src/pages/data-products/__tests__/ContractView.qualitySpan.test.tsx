// Copyright (c) 2026 Kenneth Stott
// Canary: 8c1f4a37-5d29-4e60-9b7a-2e3d6f08b415
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2158: in a contract's card grid the Quality promises card spans two columns, and only where the
// grid has three or more. jsdom lays nothing out, so this proves the card carries the span class and
// that the stylesheet applies it inside the three-column container query, to that class alone.

import { readFileSync } from "node:fs";
import { resolve } from "node:path";
import { describe, expect, it } from "vitest";
import { render, screen } from "../../../test-utils/render";
import { ContractView } from "../ContractView";
import type { OdcsContract } from "../../../api/dataProductContracts";

const CONTRACT = { id: "c1", version: "1.0.0", status: "active" } as unknown as OdcsContract;

describe("a contract's Quality promises card", () => {
  it("carries the two-column span, and no other card does", () => {
    render(
      <ContractView contract={CONTRACT} versions={[]} tableId={7} promises={null} consumers={[]} />,
    );
    expect(screen.getByTestId("data-contract-7-quality")).toHaveClass("detail-card--span-2");
    for (const key of ["details", "fields", "sla", "consumers", "versions"]) {
      expect(screen.getByTestId(`data-contract-7-${key}`)).not.toHaveClass("detail-card--span-2");
    }
  });

  it("keeps the cards in order inside the grid", () => {
    render(
      <ContractView contract={CONTRACT} versions={[]} tableId={7} promises={null} consumers={[]} />,
    );
    const grid = document.querySelector(".contract-cards__grid") as HTMLElement;
    expect(Array.from(grid.children).map((c) => c.getAttribute("data-testid"))).toEqual(
      ["details", "fields", "sla", "quality", "consumers", "versions"].map(
        (k) => `data-contract-7-${k}`,
      ),
    );
  });

  it("spans only where the grid has three columns", () => {
    const css = readFileSync(resolve(__dirname, "../ContractView.css"), "utf8");
    expect(css).toMatch(
      /@container \(min-width: 984px\)\s*\{\s*\.contract-cards__grid > \.detail-card--span-2\s*\{\s*grid-column: span 2;/,
    );
    expect(css.match(/grid-column/g)).toHaveLength(1);
  });
});
