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
// schema is defined. The decided exceptions are named below, never hidden: lineage, Input Ports and
// Data Quality (the product lineage endpoint, REQ-2156 and REQ-2157) and the owner and team roles'
// people (resolveOwners).

import { readFileSync } from "node:fs";
import { resolve } from "node:path";
import { describe, expect, it, vi } from "vitest";
import { render, screen, within } from "../../../test-utils/render";
import { DataProductDetailPanel } from "../DataProductDetailPanel";
import type { ProductCore } from "../productCore";
import type { DataProduct } from "../../../types/admin";

vi.mock("../../../components/OwnerResolution", () => ({ OwnerResolutionInline: () => null }));
vi.mock("../../../components/graph3d/GraphPanel", () => ({
  GraphPanel: () => <div data-testid="graph-panel" />,
}));

/** Each panel input, by where it comes from. */
const META = ["p", "core", "coreError", "reader"]; // the meta domain: the product, its core, its schema
// Capabilities, the reader's standing (REQ-2160: whether it acts as the owner), and callbacks.
const NOT_DATA = ["canEdit", "canDelete", "owner", "onEdit", "onDelete"];
/** Decided: the lineage DAG, Input Ports and Data Quality read the product lineage endpoint. */
const LINEAGE = [
  "lineageLoading",
  "lineageError",
  "lineageGraph",
  "lineageMembers",
  "describeColumn",
  "relationLabel",
];
/** Read inside the panel's own parts, not through its inputs. */
const INSIDE: Record<string, string> = {
  "../../components/OwnerResolution":
    "the admin API's resolveOwners: the people who hold the owner and team roles (the decided exception)",
  "./DqRulesModal": "lists the rules it is given",
  "../../components/graph3d/GraphPanel":
    "the meta domain, in a read of its own (useProductGraph): the graph card",
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
  alias: null,
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
      profile_results: [{ id: 9, domain_id: "pet-store", table_name: "pets_profile", alias: null }],
      // A relationship between two members, with its effective names as meta stores them.
      source_relationships: [
        {
          id: "pets-to-vets",
          alias: null,
          graphql_field: "vet",
          cypher_type: "BELONGS_TO_VET",
          cardinality: "many-to-one",
          source_column: "vet_id",
          target_column: "id",
          disable_cypher: false,
          target_table: { id: 2, domain_id: "pet-store", table_name: "vets" },
        },
      ],
    }),
    table(2, "vets"),
    table(3, "pets_quality", {
      dq_contract: "name: pets_quality",
      checks: { id: 1, domain_id: "pet-store", table_name: "pets", alias: null },
      dq_rules: [{ position: 1, column_name: "name", check_type: "missing", definition: "" }],
    }),
  ],
  commands: [{ name: "add_pet", description: "Add a pet", domain_id: "pet-store" }] as never,
  tags: ["pii"],
  terms: [
    { id: 7, name: "Customer", definition: "Someone who buys.", is_abstract: false, live: true },
  ] as never,
  edges: [
    {
      id: "pet_health:related:12",
      kind: "related",
      target_product_id: null,
      target_table_id: 12,
      direct: true,
      hops: 1,
      path_count: 1,
      aliases: "HAS_OWNER",
      supporting_tables: null,
      target_table: {
        id: 12,
        domain_id: "pet-store",
        table_name: "owners",
        alias: null,
        description: null,
      },
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
      canEdit={false}
      canDelete={false}
      owner
      lineageLoading={false}
      lineageError=""
      lineageGraph={null}
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
    // Data Quality is the product lineage endpoint's (REQ-2157): nothing given, no card.
    expect(screen.queryByTestId("data-product-card-dataQuality-pet_health")).toBeNull();
    expect(part("data-product-detail-profiles")).toHaveTextContent("pets");
    expect(part("data-product-detail-related-terms")).toHaveTextContent("Customer");
    expect(part("data-product-detail-related-tables")).toHaveTextContent("pet-store.owners");
    expect(part("data-product-detail-graphql-queries")).toHaveTextContent("pet-store.pets");
    // The relationships between members, and their Cypher click-through, from the core too.
    const rels = part("data-product-detail-relationships");
    expect(rels).toHaveTextContent("BELONGS_TO_VET");
    expect(rels).toHaveTextContent("pets.vet_id → vets.id (many-to-one)");
    expect(part("data-product-detail-graphql-queries")).toHaveTextContent("BELONGS_TO_VET");
  });

  it("takes no input but the meta read and the decided lineage exception", () => {
    const source = readFileSync(resolve(__dirname, "../DataProductDetailPanel.tsx"), "utf8");
    const props = source.slice(
      source.indexOf("interface DataProductDetailPanelProps"),
      source.indexOf("\n}\n", source.indexOf("interface DataProductDetailPanelProps")),
    );
    const names = [...props.matchAll(/^ {2}(\w+)\??:/gm)].map((m) => m[1]).sort();
    expect(names).toEqual([...META, ...NOT_DATA, ...LINEAGE].sort());
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
