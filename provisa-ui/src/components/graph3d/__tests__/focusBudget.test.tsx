// Copyright (c) 2026 Kenneth Stott
// Canary: 1ec027bb-2328-4f23-96e7-04e069c5b4d1
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2143: the Ontology View counts before it reads. Past the budget the whole ontology is its
// overview -- each domain with its count, picked to narrow to it -- and a search or a domain still
// past it is said, not drawn. A search's neighbourhood is two hops: no further.

import { describe, expect, it, vi, beforeEach } from "vitest";
import { render, screen, waitFor, fireEvent } from "../../../test-utils/render";
import type { GraphData } from "../graphModel";

type Calls = Record<string, ReturnType<typeof vi.fn>>;
type Callbacks = { onSelect: (h: { kind: string; id: string }) => void };
const made = vi.hoisted(() => ({ scenes: [] as Calls[], callbacks: [] as Callbacks[] }));
vi.mock("../graphScene", () => ({
  GraphScene: class {
    constructor(_canvas: unknown, _labels: unknown, callbacks: Callbacks) {
      made.callbacks.push(callbacks);
      const calls: Calls = {};
      for (const m of [
        "setTheme",
        "setRotate",
        "setGrid",
        "dispose",
        "resetView",
        "setHover",
        "setData",
        "setSelected",
        "focus",
        "setFlat",
      ])
        calls[m] = vi.fn();
      Object.assign(this, calls);
      made.scenes.push(calls);
    }
  },
}));
vi.mock("../neighbourhood", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../neighbourhood")>()),
  NODE_BUDGET: 3,
}));
vi.mock("../../../context/AuthContext", () => ({ useAuth: () => ({ actingRoles: "analyst" }) }));
vi.mock("../useProductGraph", () => ({
  metaReader: async () => ({ schema: null, roots: new Map() }),
}));
const asked = vi.hoisted(() => ({ scopes: [] as Record<string, unknown>[] }));
vi.mock("../glossaryGraphData", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../glossaryGraphData")>()),
  ontologyQuery: (_m: unknown, s: { layers: Set<string> }) =>
    JSON.stringify({ ...s, layers: [...s.layers] }),
  ontologyCountQuery: (_m: unknown, s: { layers: Set<string> }) =>
    JSON.stringify({ ...s, layers: [...s.layers], count: true }),
}));
const term = (id: number, name: string, domain: string) => ({
  id,
  name,
  definition: `${name}.`,
  ref_count: 0,
  deprecated: false,
  retired: false,
  live: true,
  domains: [{ domain_id: domain }],
});
// Five in sales (past a budget of 3), two in finance (within it).
const TERMS = [
  term(1, "customer", "sales"),
  term(2, "order", "sales"),
  term(3, "order line", "sales"),
  term(4, "invoice", "sales"),
  term(5, "refund", "sales"),
  term(6, "ledger", "finance"),
  term(7, "journal", "finance"),
];
vi.mock("../../../api/metaGraph", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../../api/metaGraph")>()),
  dataGraphql: async (query: string) => {
    const s = JSON.parse(query) as { domains: string[]; search: string[]; count?: boolean };
    if (!s.count) asked.scopes.push(s);
    const terms = TERMS.filter(
      (t) =>
        (s.domains.length === 0 || s.domains.includes(t.domains[0].domain_id)) &&
        (s.search.length === 0 || s.search.some((i) => t.name.includes(i))),
    );
    return {
      glossary_terms: s.count ? terms.map((t) => ({ id: t.id, domains: t.domains })) : terms,
    };
  },
}));

import { GlossaryOntologyView } from "../GlossaryOntologyView";

const drawn = () => made.scenes.at(-1)?.setData.mock.calls.at(-1)?.[0] as GraphData | undefined;

beforeEach(() => {
  made.scenes.length = 0;
  made.callbacks.length = 0;
  asked.scopes.length = 0;
  window.localStorage.clear();
});

describe("the Ontology View past its budget", () => {
  it("draws each domain with its count, and narrows to the one picked", async () => {
    render(<GlossaryOntologyView search="" />);
    await waitFor(() =>
      expect(drawn()?.nodes.map((n) => n.label)).toEqual(["finance (2)", "sales (5)"]),
    );
    // The graph itself is never read past the budget.
    expect(asked.scopes.filter((sc) => (sc.domains as string[]).length === 0)).toEqual([]);
    made.callbacks.at(-1)?.onSelect({ kind: "node", id: "d:finance" });
    await waitFor(() =>
      expect(
        drawn()
          ?.nodes.map((n) => n.id)
          .sort(),
      ).toEqual(["d:finance", "term:6", "term:7"]),
    );
  });

  it("says how many a search matches when they are past it, and draws none", async () => {
    render(<GlossaryOntologyView search="order invoice refund customer" />);
    expect(await screen.findByTestId("ontology-over-budget")).toHaveTextContent(
      "5 match; narrow the search or pick a domain",
    );
  });

  it("draws a search within it, and opens it as a query", async () => {
    render(<GlossaryOntologyView search="ledger" />);
    await waitFor(() => expect(drawn()?.nodes.map((n) => n.id)).toContain("term:6"));
    expect(asked.scopes.at(-1)?.search).toEqual(["ledger"]);
    fireEvent.click(screen.getByTestId("ontology-open-as-query"));
    expect(await screen.findByTestId("ontology-open-as-graphql")).not.toHaveAttribute(
      "data-disabled",
    );
  });
});

describe("a focus's neighbourhood", () => {
  it("is two hops, in the Ontology View and the product graph alike", async () => {
    const { reach } = await import("../graphModel");
    const { NEIGHBOURHOOD } = await import("../neighbourhood");
    const { GLOSSARY_SCOPE, PRODUCT_SCOPE } = await import("../graphModel");
    // term:1 - term:2 - term:3 - term:4, each a KIND_OF of the next.
    const nodes = [1, 2, 3, 4].map((i) => ({
      id: `term:${i}`,
      type: "term" as const,
      label: `${i}`,
    }));
    const edges = [1, 2, 3].map((i) => ({
      id: `e${i}`,
      a: `term:${i}`,
      b: `term:${i + 1}`,
      kind: "term_edge" as const,
      label: "KIND_OF",
    }));
    const byId = new Map(nodes.map((n) => [n.id, n]));
    expect([...reach(["term:1"], edges, byId, NEIGHBOURHOOD)].sort()).toEqual([
      "term:1",
      "term:2",
      "term:3",
    ]);
    // One rule: both graphs draw the same neighbourhood.
    expect(PRODUCT_SCOPE.neighbourhood).toBe(NEIGHBOURHOOD);
    expect(GLOSSARY_SCOPE.neighbourhood).toBe(NEIGHBOURHOOD);
  });
});

describe("a product's neighbourhood", () => {
  it("is counted from its member tables: members, their columns, the terms bound to those", async () => {
    const { visibleGraph, NO_FILTERS, PRODUCT_SCOPE } = await import("../graphModel");
    type K = "member" | "has_column" | "term_binding" | "term_edge" | "in_domain";
    const node = (id: string, type: string) => ({ id, type, label: id });
    const edge = (a: string, b: string, kind: K) => ({ id: `${a}-${b}`, a, b, kind });
    const data = {
      nodes: [
        node("p:x", "product"),
        node("d:sales", "domain"),
        node("t:1", "table"),
        node("c:1:a", "column"),
        node("term:a", "term"),
        node("term:b", "term"),
      ],
      edges: [
        edge("p:x", "d:sales", "in_domain"),
        edge("p:x", "t:1", "member"),
        edge("t:1", "c:1:a", "has_column"),
        edge("c:1:a", "term:a", "term_binding"),
        edge("term:a", "term:b", "term_edge"),
      ],
    } as never;
    const layers = new Set(PRODUCT_SCOPE.layerOrder);
    const drawn = (focus: string[]) =>
      visibleGraph(data, layers as never, { ...NO_FILTERS, focus }, new Set(), {
        ...PRODUCT_SCOPE,
        foldColumns: false,
      })
        .nodes.map((n) => n.id)
        .sort();
    const ids = drawn(["p:x"]);
    // Two hops from the members reaches the bound term; its neighbour is a third.
    expect(ids).toEqual(expect.arrayContaining(["p:x", "t:1", "c:1:a", "term:a"]));
    expect(ids).not.toContain("term:b");
  });
});
