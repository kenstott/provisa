// Copyright (c) 2026 Kenneth Stott
// Canary: 6c0e9a35-4b7d-4f18-92e6-a3d8b1f50c74
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2174 (amended 2026-11-07, WHAT THE GRAPH DRAWS): a product page's Lineage is titled
// "Column lineage: where this product comes from", its lanes named Inputs and This product's
// tables (Upstream hop n beyond the inputs), with no downstream lanes; its hint says what it
// draws, and the Input Ports list links to it. REQ-2239: it is the Lineage tab, drawn by the one
// graph component; a view between an input and a table is folded onto an indirect edge.

import { describe, expect, it, vi } from "vitest";
import { fireEvent, render, screen } from "../../../test-utils/render";
import { DataProductDetailPanel } from "../DataProductDetailPanel";
import { upstreamLineage } from "../upstreamLineage";
import type { LineageNode, ProductLineage } from "../../../api/lineage";
import type { DataProduct } from "../../../types/admin";
import { lifecycleProps } from "./lifecycleProps";
import { openSection } from "./panelSections";
import type { GraphSpec } from "../../../components/provisa-graph/model";
import type { LanesOptions } from "../../../components/provisa-graph/lanes";
import en from "../../../i18n/locales/en/dataProductsTab.json";

vi.mock("../../../components/OwnerResolution", () => ({ OwnerResolutionInline: () => null }));
vi.mock("../../../components/graph3d/OntologyGraph", () => ({
  OntologySnippet: () => <div data-testid="graph-panel" />,
}));
// The graph as drawn: what it is given.
const drawn = vi.hoisted(() => ({
  last: null as null | { graph: GraphSpec; preset: string; lanes?: LanesOptions },
}));
vi.mock("../../../components/provisa-graph/ProvisaGraph", () => ({
  ProvisaGraph: (props: { graph: GraphSpec; preset: string; lanes?: LanesOptions }) => {
    drawn.last = props;
    return <div data-testid="provisa-graph" />;
  },
}));

const PRODUCT = {
  id: "pet_health",
  domainId: "pet-store",
  name: "Pet Health",
  customProperties: {},
} as unknown as DataProduct;

const node = (id: string, relation: string): LineageNode => ({
  id,
  relation,
  column: id.slice(id.lastIndexOf(".") + 1),
  kind: "source",
  materialized: false,
});
const feeds = (source: string, target: string) => ({ source, target, transform: "", ops: [] });

// raw.visits feeds staging.v, which feeds the member pet_store.visits; a report reads the member.
const LINEAGE = {
  nodes: [
    node("raw.visits.id", "raw.visits"),
    node("staging.v.id", "staging.v"),
    node("pet_store.visits.id", "pet_store.visits"),
    node("pet_store.report.id", "pet_store.report"),
  ],
  edges: [
    feeds("raw.visits.id", "staging.v.id"),
    feeds("staging.v.id", "pet_store.visits.id"),
    feeds("pet_store.visits.id", "pet_store.report.id"),
  ],
  outputs: ["pet_store.visits.id", "pet_store.report.id"],
  members: ["pet_store.visits"],
  consumers: [],
  inputs: [
    {
      relation: "raw.visits",
      kind: "source_table",
      table_id: 7,
      domain_id: "raw",
      table_name: "visits",
      alias: null,
      product_id: null,
      source_id: null,
      source_type: null,
      source_schema: null,
      source_table: null,
    },
  ],
  dq: { expectations: [], promises: [], related: [], unattached: [] },
} as unknown as ProductLineage;

function panel() {
  return render(
    <DataProductDetailPanel
      p={PRODUCT}
      core={{ product: PRODUCT, queries: { graphql: "query X { x }", sql: "", jsonapi: "" } }}
      coreError={null}
      reader={null}
      canEdit={false}
      canDelete={false}
      owner
      seesConsumers
      lineageLoading={false}
      lineageError=""
      lineageGraph={LINEAGE}
      onEdit={vi.fn()}
      onDelete={vi.fn()}
      {...lifecycleProps()}
    />,
  );
}

describe("the product's Lineage tab", () => {
  it("is titled by where the product comes from, its hint saying what it draws", () => {
    panel();
    openSection("lineage", "pet_health");
    expect(screen.getByTestId("data-product-lineage-title-pet_health")).toHaveTextContent(
      "Column lineage: where this product comes from",
    );
    expect(en.dataProductsTab.detail.lineageHint).toBe(
      "Each column this product publishes and the input column it comes from, with every step between them. Requires the view_governance capability.",
    );
  });

  it("opens from the Input Ports list's link, drawing nothing downstream of the product", () => {
    window.localStorage.clear();
    panel();
    openSection("quality", "pet_health");
    fireEvent.click(screen.getByTestId("data-product-detail-inputs-lineage-link-pet_health"));
    expect(screen.getByTestId("data-product-row-lineage-pet_health-toggle")).toHaveAttribute(
      "aria-expanded",
      "true",
    );
    const graph = drawn.last?.graph as GraphSpec;
    expect(drawn.last?.preset).toBe("lanes");
    // The report reading the member is downstream: not drawn. The view between the input and the
    // member is folded onto an indirect edge from one to the other.
    expect(graph.nodes.map((n) => n.id).sort()).toEqual(["pet_store.visits", "raw.visits"]);
    expect(graph.edges).toEqual([
      expect.objectContaining({
        source: "raw.visits",
        target: "pet_store.visits",
        dashed: true,
        label: "staging.v",
      }),
    ]);
  });

  it("names its lanes Inputs and This product's tables, each further lane by hops beyond the inputs", () => {
    window.localStorage.clear();
    render(
      <DataProductDetailPanel
        p={PRODUCT}
        core={{ product: PRODUCT, queries: { graphql: "query X { x }", sql: "", jsonapi: "" } }}
        coreError={null}
        reader={null}
        canEdit={false}
        canDelete={false}
        owner
        seesConsumers
        lineageLoading={false}
        lineageError=""
        lineageGraph={{
          ...LINEAGE,
          // Two relations feed the input, one hop and two hops beyond it.
          nodes: [...LINEAGE.nodes, node("ext.a.id", "ext.a"), node("ext.b.id", "ext.b")],
          edges: [
            ...LINEAGE.edges,
            feeds("ext.a.id", "raw.visits.id"),
            feeds("ext.b.id", "ext.a.id"),
          ],
        }}
        onEdit={vi.fn()}
        onDelete={vi.fn()}
        {...lifecycleProps()}
      />,
    );
    openSection("lineage", "pet_health");
    expect(drawn.last?.lanes?.lanes.map((l) => l.title)).toEqual([
      "Upstream, hop 2 beyond the inputs",
      "Upstream, hop 1 beyond the inputs",
      "Inputs",
      "This product's tables",
    ]);
    const lane = (id: string) => drawn.last?.graph.nodes.find((n) => n.id === id)?.lane;
    expect(lane("ext.b")).toBe(drawn.last?.lanes?.lanes[0].id);
    expect(lane("ext.a")).toBe(drawn.last?.lanes?.lanes[1].id);
    expect(lane("raw.visits")).toBe(drawn.last?.lanes?.lanes[2].id);
    expect(lane("pet_store.visits")).toBe(drawn.last?.lanes?.lanes[3].id);
  });
});

describe("upstreamLineage", () => {
  it("keeps the members' columns and everything upstream of them, nothing that reads them", () => {
    const up = upstreamLineage(LINEAGE);
    expect(up.nodes.map((n) => n.id)).toEqual([
      "raw.visits.id",
      "staging.v.id",
      "pet_store.visits.id",
    ]);
    expect(up.edges.map((e) => `${e.source}>${e.target}`)).toEqual([
      "raw.visits.id>staging.v.id",
      "staging.v.id>pet_store.visits.id",
    ]);
    expect(up.outputs).toEqual(["pet_store.visits.id"]);
    // The inputs and the rest of the answer are kept as they are.
    expect(up.inputs).toBe(LINEAGE.inputs);
  });
});
