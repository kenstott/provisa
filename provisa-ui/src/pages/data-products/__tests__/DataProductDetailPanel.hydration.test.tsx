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
// (productCore.loadProductCore) is its header's and its Related tab's content. That is the test of
// how well the meta schema is defined. The decided exceptions are named below, never hidden:
// lineage, Input Ports and Data Quality (the product lineage endpoint, REQ-2156 and REQ-2157), the
// owner and team roles' people (resolveOwners), and, since REQ-2180 OFFERS, the offers endpoint
// (each offer with its contracts), the product's sanitized profiles, its bids and who reads each
// dataset -- each read by the tab that shows it, when it is opened.

import { readFileSync } from "node:fs";
import { resolve } from "node:path";
import { describe, expect, it, vi } from "vitest";
import { render, screen, within } from "../../../test-utils/render";
import { DataProductDetailPanel } from "../DataProductDetailPanel";
import { lifecycleProps } from "./lifecycleProps";
import { openSection } from "./panelSections";
import type { ProductCore } from "../productCore";
import type { DataProduct } from "../../../types/admin";

vi.mock("../../../components/OwnerResolution", () => ({ OwnerResolutionInline: () => null }));
vi.mock("../../../components/graph3d/OntologyGraph", () => ({
  OntologySnippet: () => <div data-testid="graph-panel" />,
}));

/** Each panel input, by where it comes from. */
const META = ["p", "core", "coreError", "reader"]; // the meta domain: the product, its core, its schema
// Capabilities, the reader's standing (REQ-2160: whether it acts as the owner; REQ-2158: the roles
// it holds, which a contract's How to use reads), and callbacks.
const NOT_DATA = [
  "canEdit",
  "canDelete",
  "owner",
  // REQ-2174 (amended 2026-11-07), REQ-2219: whether the reader owns the product (its Consumers).
  "seesConsumers",
  "viewer",
  "onEdit",
  "onDelete",
  "onDuplicate", // the page opens its create form prefilled from this product; no data
  "onStatusChanged",
  "onStatusFailed",
  "openProduct",
  "statusActions",
  "onTagsChanged", // REQ-1634: the tag chip changed the product's tags; the page reads its core again
];
/** Its lifecycle: the products a successor is chosen from, the members a retirement moves, and
 * its findings, read by the page for the list's own To review. */
const LIFECYCLE = ["products", "members", "findings"];
/** Decided: the lineage DAG, Input Ports and Data Quality read the product lineage endpoint. */
const LINEAGE = ["lineageLoading", "lineageError", "lineageGraph", "relationLabel"];
/** The page's own reads the panel's parts are composed from (REQ-2239): the tables a dataset is
 *  previewed through, the products sharing its datasets, the model's relationships (each dataset's
 *  associations), the role it reads as, and the edit form that takes the header's place. */
const PAGE = ["tables", "sharing", "relationships", "actingRole", "editForm"];
/** Decided (REQ-2180): the offers and their contracts are read by the Offers tab; the page says only
 * when to read them again. */
const CONTRACTS = ["contractsVersion"];
/** Read inside the panel's own parts, not through its inputs. */
const INSIDE: Record<string, string> = {
  "../../components/OwnerResolution":
    "the admin API's resolveOwners: the people who hold the owner and team roles (the decided exception)",
  "./offers/OffersTab":
    "REQ-2180 OFFERS: the offers endpoint, each offer with its contracts; the roles its owner may grant; each contract's ODCS document, from its offer's contract route, as it opens",
  "./tabs/QualityTab":
    "the checks bar's profile checks, read when the owner opens it (REQ-1934); a checker's rules",
  "./tabs/ProfileTab": "REQ-2180: the product's profiles, sanitized on the server for the reader",
  "./tabs/RelatedTab":
    "the ontology in a read of its own (useOntology), focused on the product; the tag chip; each dataset's glossary terms and class term",
  "./tabs/RequestsTab": "REQ-2180: the product's bids, and who reads each dataset (REQ-2162)",
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
  sourceId: "pet-store-sqlite",
  domainId: "pet-store",
  schemaName: "pet_store",
  tableName: name,
  alias: null,
  description: `The ${name} table`,
  dqContract: null,
  tableColumns: [],
  profileResults: [],
  ...over,
});

/** One read of the meta domain: everything the panel's cards show. */
const CORE: ProductCore = {
  product: PRODUCT,
  tables: [
    table(1, "pets", {
      profileResults: [{ id: 9, domainId: "pet-store", tableName: "pets_profile", alias: null }],
      // A relationship between two members, with its effective names as meta stores them.
      sourceRelationships: [
        {
          id: "pets-to-vets",
          alias: null,
          graphqlField: "vet",
          cypherType: "BELONGS_TO_VET",
          cardinality: "many-to-one",
          sourceColumn: "vet_id",
          targetColumn: "id",
          disableCypher: false,
          targetTable: { id: 2, domainId: "pet-store", tableName: "vets" },
        },
      ],
    }),
    table(2, "vets"),
    table(3, "pets_quality", {
      dqContract: "name: pets_quality",
      checks: { id: 1, domainId: "pet-store", tableName: "pets", alias: null },
      dqRules: [{ position: 1, columnName: "name", checkType: "missing", definition: "" }],
    }),
  ],
  commands: [{ name: "add_pet", description: "Add a pet", domainId: "pet-store" }] as never,
  tags: ["pii"],
  terms: [
    { id: 7, name: "Customer", definition: "Someone who buys.", isAbstract: false, live: true },
  ] as never,
  edges: [
    {
      id: "pet_health:related:12",
      kind: "related",
      targetProductId: null,
      targetTableId: 12,
      direct: true,
      hops: 1,
      pathCount: 1,
      aliases: "HAS_OWNER",
      supportingTables: null,
      targetTable: {
        id: 12,
        domainId: "pet-store",
        tableName: "owners",
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
      seesConsumers
      lineageLoading={false}
      lineageError=""
      lineageGraph={null}
      onEdit={vi.fn()}
      onDelete={vi.fn()}
      {...lifecycleProps()}
    />,
  );
}

const part = (id: string) => screen.getByTestId(`${id}-pet_health`);

describe("a data product's panel hydrates from the meta query", () => {
  it("shows every value of the header and the Related section from the one meta read", () => {
    panelFromMetaAlone();
    expect(part("data-product-read")).toHaveTextContent("Pet Health");
    expect(part("data-product-read")).toHaveTextContent("2.1.0");
    expect(part("data-product-detail-custom-properties")).toHaveTextContent("tier");
    // Its tables' ways in, in the header; the checker table is no dataset of it.
    const outputs = part("data-product-detail-graphql-queries");
    expect(outputs).toHaveTextContent("pet-store.pets");
    expect(outputs).toHaveTextContent("pet-store.vets");
    expect(within(outputs).queryByText(/pets_quality/)).toBeNull();
    // The relationships between members' Cypher click-through, from the core too.
    expect(outputs).toHaveTextContent("BELONGS_TO_VET");
    // Data Quality is the product lineage endpoint's (REQ-2157): nothing given, nothing listed.
    openSection("quality", "pet_health");
    expect(screen.queryByTestId("data-product-card-dataQuality-pet_health")).toBeNull();
    // Its tags are the header's, drawn by the tag chip from the tag assignments it reads
    // (TagChipMounts.test).
    openSection("related", "pet_health");
    expect(part("data-product-detail-commands")).toHaveTextContent("add_pet");
    expect(part("data-product-detail-related-terms")).toHaveTextContent("Customer");
    expect(part("data-product-detail-related-tables")).toHaveTextContent("pet-store.owners");
    const rels = part("data-product-detail-relationships");
    expect(rels).toHaveTextContent("BELONGS_TO_VET");
    expect(rels).toHaveTextContent("pets.vet_id → vets.id (many-to-one)");
  });

  it("takes no input but the meta read and the decided lineage exception", () => {
    const source = readFileSync(resolve(__dirname, "../DataProductDetailPanel.tsx"), "utf8");
    const props = source.slice(
      source.indexOf("interface DataProductDetailPanelProps"),
      source.indexOf("\n}\n", source.indexOf("interface DataProductDetailPanelProps")),
    );
    const names = [...props.matchAll(/^ {2}(\w+)\??:/gm)].map((m) => m[1]).sort();
    expect(names).toEqual([...META, ...NOT_DATA, ...LIFECYCLE, ...LINEAGE, ...PAGE, ...CONTRACTS].sort());
  });

  it("reads inside its own parts only where named", () => {
    const source = readFileSync(resolve(__dirname, "../DataProductDetailPanel.tsx"), "utf8");
    // A type import reads nothing; nor do the section list and the Lineage section, which draws the
    // lineage it is given.
    const readers = [...source.matchAll(/^import (?!type ).* from "([^"]+)";$/gm)]
      .map((m) => m[1])
      .filter((from) =>
        /OwnerResolution|DqRulesModal|graph3d|lineage\/LineageDag|ProductContracts|offers\/|tabs\/(?!LineageTab)|api\/|hooks\//.test(
          from,
        ),
      )
      .filter(
        (from) => !from.startsWith("../../api/lineage") && !from.startsWith("../../api/metaGraph"),
      );
    expect(readers.sort()).toEqual(Object.keys(INSIDE).sort());
  });
});
