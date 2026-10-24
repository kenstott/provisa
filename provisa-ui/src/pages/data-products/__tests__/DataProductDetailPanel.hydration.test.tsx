// Copyright (c) 2026 Kenneth Stott
// Canary: b4554bc5-e79a-4a4f-ba87-be4089e491e0
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// A data product's panel hydrates itself from the meta domain: one read of the product
// (productCore.loadProductCore) is every card's content. That is the test of how well the meta
// schema is defined. Lineage and Input Ports are the decided exceptions (the lineage API, scoped to
// the product). Any other part that reads elsewhere is named below as PENDING, waiting on the
// maintainer's decision (an exception, or the gap closed in the meta schema), never hidden.

import { readFileSync } from "node:fs";
import { resolve } from "node:path";
import { describe, expect, it, vi } from "vitest";
import { render, screen, within } from "../../../test-utils/render";
import { DataProductDetailPanel } from "../DataProductDetailPanel";
import type { ProductCore } from "../productCore";
import type { DataProduct } from "../../../types/admin";

vi.mock("../../../components/OwnerResolution", () => ({ OwnerResolutionInline: () => null }));
vi.mock("../../../components/graph3d/KnowledgeGraphPanel", () => ({
  KnowledgeGraphPanel: () => <div data-testid="kg-panel" />,
}));

/** Each panel input, by where it comes from. */
const META = ["p", "core", "coreError", "reader"]; // the meta domain: the product, its core, its schema
const NOT_DATA = ["canEdit", "canSeeLineage", "onEdit", "onDelete"]; // capabilities and callbacks
/** Decided: lineage and Input Ports read the lineage API, scoped to the product. */
const LINEAGE = [
  "lineageLoading",
  "lineageError",
  "lineageGraph",
  "lineageMembers",
  "describeColumn",
  "relationLabel",
  "inputPorts",
];
/** Pending the maintainer's decision: read outside the meta query today. */
const PENDING: Record<string, string> = {
  sources:
    "the admin API's sources (useSources): the checker type a DQ table's rules are parsed with",
  internalRelationships:
    "the admin API's relationships (useRelationships): the Relationships card and Access Patterns' Cypher rows",
};

/** Read inside the panel's own parts, not through its inputs. */
const INSIDE: Record<string, string> = {
  "../../components/OwnerResolution":
    "PENDING: the admin API's resolveOwners: the people who hold the owner and team roles",
  "./DqRulesModal": "PENDING: the admin API's dqContractParse: a DQ table's rules, parsed on open",
  "../../components/graph3d/KnowledgeGraphPanel":
    "the meta domain, in a read of its own (useProductGraph): the knowledge graph card",
  "../../components/lineage/LineageDag": "draws the lineage it is given (the decided exception)",
};

const PRODUCT = {
  id: "pet_health",
  domainId: "pet-store",
  name: "Pet Health",
  version: "2.1.0",
  status: "published",
  purpose: "One governed view of each pet.",
  customProperties: { tier: "gold" },
} as unknown as DataProduct;

const table = (id: number, name: string, over: Record<string, unknown> = {}) => ({
  id,
  source_id: "pet-store-sqlite",
  domain_id: "pet-store",
  schema_name: "pet_store",
  table_name: name,
  description: `The ${name} table`,
  dq_contract: null,
  table_columns: [],
  profile_results: [],
  ...over,
});

/** One read of the meta domain: everything the panel's cards show. */
const CORE: ProductCore = {
  product: PRODUCT,
  tables: [
    table(1, "pets", {
      profile_results: [{ id: 9, domain_id: "pet-store", table_name: "pets_profile" }],
    }),
    table(2, "vets"),
    table(3, "pets_quality", {
      dq_contract: "name: pets_quality",
      checks: { id: 1, domain_id: "pet-store", table_name: "pets" },
    }),
  ],
  commands: [{ name: "add_pet", description: "Add a pet", domain_id: "pet-store" }] as never,
  tags: ["pii"],
  terms: [
    { id: 7, name: "Customer", definition: "Someone who buys.", is_abstract: false, live: true },
  ] as never,
  edges: [
    {
      kind: "relationship",
      direction: null,
      target_product_id: null,
      target_table_id: 12,
      target_relation: null,
      direct: true,
      hops: 1,
      path_count: 1,
      aliases: "HAS_OWNER",
      supporting_tables: null,
      vias: null,
      target_table: { id: 12, domain_id: "pet-store", table_name: "owners", description: null },
    },
  ],
  queries: { graphql: "", sql: "", jsonapi: "" },
};

function panelFromMetaAlone() {
  render(
    <DataProductDetailPanel
      p={PRODUCT}
      core={CORE}
      coreError={null}
      reader={null}
      // Every input that is not the meta read is empty: what the cards show is the core's.
      sources={[]}
      internalRelationships={[]}
      canEdit={false}
      canSeeLineage={false}
      lineageLoading={false}
      lineageError=""
      lineageGraph={null}
      inputPorts={[]}
      onEdit={vi.fn()}
      onDelete={vi.fn()}
    />,
  );
}

const part = (id: string) => screen.getByTestId(`${id}-pet_health`);

describe("a data product's panel hydrates from the meta query", () => {
  it("shows every card's content from the one meta read, nothing else given", () => {
    panelFromMetaAlone();
    expect(part("data-product-read")).toHaveTextContent("Pet Health");
    expect(part("data-product-read")).toHaveTextContent("2.1.0");
    expect(part("data-product-detail-custom-properties")).toHaveTextContent("tier");
    expect(part("data-product-detail-tags")).toHaveTextContent("pii");
    const outputs = part("data-product-detail-tables");
    expect(outputs).toHaveTextContent("pet-store.pets");
    expect(outputs).toHaveTextContent("pet-store.vets");
    expect(within(outputs).queryByText(/pets_quality/)).toBeNull();
    expect(part("data-product-detail-commands")).toHaveTextContent("add_pet");
    expect(part("data-product-detail-dq-tables")).toHaveTextContent("pets_quality");
    expect(part("data-product-detail-profiles")).toHaveTextContent("pets");
    expect(part("data-product-detail-related-terms")).toHaveTextContent("Customer");
    expect(part("data-product-detail-related-tables")).toHaveTextContent("pet-store.owners");
    expect(part("data-product-detail-graphql-queries")).toHaveTextContent("pet-store.pets");
  });

  it("takes no input but the meta read, the decided lineage exception and the named pending gaps", () => {
    const source = readFileSync(resolve(__dirname, "../DataProductDetailPanel.tsx"), "utf8");
    const props = source.slice(
      source.indexOf("interface DataProductDetailPanelProps"),
      source.indexOf("\n}\n", source.indexOf("interface DataProductDetailPanelProps")),
    );
    const names = [...props.matchAll(/^ {2}(\w+)\??:/gm)].map((m) => m[1]).sort();
    expect(names).toEqual([...META, ...NOT_DATA, ...LINEAGE, ...Object.keys(PENDING)].sort());
  });

  it("reads inside its own parts only where named", () => {
    const source = readFileSync(resolve(__dirname, "../DataProductDetailPanel.tsx"), "utf8");
    const readers = [...source.matchAll(/^import .* from "([^"]+)";$/gm)]
      .map((m) => m[1])
      .filter((from) =>
        /OwnerResolution|DqRulesModal|graph3d|lineage\/LineageDag|api\/|hooks\//.test(from),
      )
      .filter(
        (from) => !from.startsWith("../../api/lineage") && !from.startsWith("../../api/metaGraph"),
      );
    expect(readers.sort()).toEqual(Object.keys(INSIDE).sort());
  });
});
