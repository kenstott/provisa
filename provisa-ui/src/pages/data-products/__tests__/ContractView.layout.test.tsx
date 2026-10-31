// Copyright (c) 2026 Kenneth Stott
// Canary: 8c1f4a37-5d29-4e60-9b7a-2e3d6f08b415
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2158: a contract's panels in three tiers -- key-value panels in a grid, tables in two flowing
// columns, then Servers, Columns and Promises across the full width. Who consumes a port is the
// owner's, not the contract's (REQ-2160). jsdom lays nothing out, so this proves the structure and
// the stylesheet's rules that make the layout.

import { readFileSync } from "node:fs";
import { resolve } from "node:path";
import { describe, expect, it } from "vitest";
import { render, screen } from "../../../test-utils/render";
import { ContractView } from "../ContractView";
import type { OdcsContract } from "../../../api/dataProductContracts";
import type { ProductDqEntry } from "../../../api/lineage";

const CONTRACT = { id: "c1", version: "1.0.0", status: "active" } as unknown as OdcsContract;

const promise = (id: number, table: string, outcome: "pass" | "fail" | null): ProductDqEntry => ({
  checker_table_id: id,
  domain_id: "pet-store",
  table_name: `${table}_quality`,
  alias: null,
  description: `Quality scan of ${table}`,
  owner_product_id: "pet_health",
  checked_table_id: id + 100,
  checked_table: { id: id + 100, domain_id: "pet-store", table_name: table, alias: null },
  rules: [{ check_type: "expect_column_values_to_not_be_null", column_name: "id", args: {} }],
  latest_outcome: outcome,
  latest_run_at: null,
});
const PROMISES = [
  promise(3, "pets", "pass"),
  promise(4, "vets", "fail"),
  promise(5, "visits", null),
  promise(6, "owners", "pass"),
];

function renderView() {
  render(
    <ContractView
      contract={CONTRACT}
      versions={[]}
      tableId={7}
      promises={PROMISES}
    />,
  );
}

describe("a contract's card layout", () => {
  it("lays the panels out in their tiers, top to bottom", () => {
    renderView();
    const root = screen.getByTestId("data-contract-7");
    const [small, medium, servers, columns, promises] = Array.from(root.children) as HTMLElement[];
    const keys = (tier: HTMLElement) =>
      Array.from(tier.children).map((c) => c.getAttribute("data-testid"));
    expect(small).toHaveClass("contract-tier--small");
    expect(keys(small)).toEqual(
      ["details", "versions", "pricing", "custom"].map((k) => `data-contract-7-${k}`),
    );
    expect(medium).toHaveClass("contract-tier--medium");
    expect(keys(medium)).toEqual(
      ["roles", "sla", "support", "history"].map((k) => `data-contract-7-${k}`),
    );
    expect(servers).toBe(screen.getByTestId("data-contract-7-servers"));
    expect(columns).toBe(screen.getByTestId("data-contract-7-fields"));
    expect(promises).toBe(screen.getByTestId("data-contract-7-quality"));
    for (const wide of [servers, columns, promises]) {
      expect(wide.style.marginTop).toBe("var(--mantine-spacing-sm)");
    }
  });

  it("grids the small tier and flows the medium tier in two columns", () => {
    const css = readFileSync(resolve(__dirname, "../ContractLayout.css"), "utf8");
    expect(css).toMatch(/\.contract-tier--small \{[^}]*display: grid/);
    expect(css).toMatch(/\.contract-tier--medium \{[^}]*columns: 2 /);
  });

  it("labels fields on the shared grid, in no code font", () => {
    renderView();
    const details = screen.getByTestId("data-contract-7-details");
    expect(details.querySelector("dl.detail-fields")).not.toBeNull();
    const label = details.querySelector("dt") as HTMLElement;
    expect(label).toHaveTextContent("id");
    expect(label.getAttribute("style") ?? "").not.toMatch(/monospace/);
  });

  it("folds what is not declared into one line; an empty panel is that line alone", () => {
    renderView();
    expect(screen.getByTestId("data-contract-7-details-not-declared")).toHaveTextContent(
      "Not declared: name, domain, dataProduct, tenant, purpose, limitations, usage",
    );
    const pricing = screen.getByTestId("data-contract-7-pricing");
    expect(pricing.querySelector("dl, table")).toBeNull();
    expect(screen.getByTestId("data-contract-7-pricing-not-declared")).toHaveTextContent(
      "Not declared: priceAmount, priceCurrency, priceUnit",
    );
    expect(screen.getByTestId("data-contract-7-servers-not-declared")).toHaveTextContent(
      "Not declared: servers",
    );
  });

  it("draws servers as rows, each located by host and port or by URL", () => {
    render(
      <ContractView
        contract={{
          ...CONTRACT,
          servers: [
            {
              server: "pgwire",
              type: "postgres",
              description: "PostgreSQL wire protocol",
              host: "localhost",
              port: 5439,
              database: "provisa",
              schema: "pet_store",
            },
            {
              server: "rest",
              type: "api",
              description: "REST",
              location: "https://data.example.com/data/rest/pet-store/pets",
            },
          ],
        }}
        versions={[]}
        tableId={7}
        promises={[]}
      />,
    );
    const table = screen.getByTestId("data-contract-servers-7");
    expect(Array.from(table.querySelectorAll("th")).map((h) => h.textContent)).toEqual([
      "server",
      "type",
      "location",
      "database",
      "schema",
      "description",
    ]);
    expect(
      Array.from(table.querySelectorAll("tbody tr")).map((row) =>
        Array.from(row.querySelectorAll("td")).map((cell) => cell.textContent),
      ),
    ).toEqual([
      ["pgwire", "postgres", "localhost:5439", "provisa", "pet_store", "PostgreSQL wire protocol"],
      ["rest", "api", "https://data.example.com/data/rest/pet-store/pets", "—", "—", "REST"],
    ]);
  });

  it("has no Consumers card", () => {
    renderView();
    expect(screen.queryByTestId("data-contract-7-consumers")).toBeNull();
    expect(screen.queryByTestId("data-contract-consumers-7")).toBeNull();
    expect(screen.getByTestId("data-contract-7")).not.toHaveTextContent("Consumers");
  });

  it("heads each further section as a card of its own, none of them inside Details", () => {
    renderView();
    const details = screen.getByTestId("data-contract-7-details");
    const labels = {
      support: "Support and team",
      servers: "Servers",
      pricing: "Pricing",
      custom: "Custom properties and tags",
    };
    for (const [key, label] of Object.entries(labels)) {
      const card = screen.getByTestId(`data-contract-7-${key}`);
      expect(details.contains(card)).toBe(false);
      expect(screen.getByTestId(`data-contract-7-${key}-toggle`)).toHaveTextContent(label);
    }
  });
});
