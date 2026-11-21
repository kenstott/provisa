// Copyright (c) 2026 Kenneth Stott
// Canary: 8e3b6d21-4f7a-4c09-b2e5-9a1d0c7f3e64
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2136 / REQ-2143: Open as query hands over the query of the search now shown. A search is read
// a hop at a time, and the view opens as its root and every hop in one query; while those reads
// were in flight the view still offered the query of the search before, so Open as query run on
// /query answered the unsearched ontology (open-as-query.spec.ts, run 43725827456). While the
// current search's read is in flight the view has no query to open as.

import { act } from "react";
import { renderHook, waitFor } from "@testing-library/react";
import { beforeEach, describe, expect, it, vi } from "vitest";
import "../../../i18n";
import type { GraphData, LayerId, QueryScope } from "../graphModel";
import { scopeOf, WHOLE } from "../ontologyPresets";
import { searchItems } from "../../../lib/searchItems";
import { reader, server, world, type World } from "./termServer";

vi.mock("../../../context/AuthContext", () => ({ useAuth: () => ({ actingRoles: "analyst" }) }));
const wire = vi.hoisted(() => ({
  read: null as null | ((q: string) => Promise<unknown>),
}));
vi.mock("../../../api/metaGraph", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../../api/metaGraph")>()),
  metaReader: async () => reader(),
  dataGraphql: async (q: string) => wire.read!(q),
}));

import { useOntology } from "../useOntology";

const WORLD: World = world({
  products: [{ id: "p1", name: "shelter" }],
  tables: [{ id: 101, name: "animal", product: "p1", columns: ["species", "breed"] }],
  terms: [
    { id: 15, name: "species", columns: [[101, "species"]] },
    { id: 16, name: "breed", columns: [[101, "breed"]] },
  ],
  links: [[16, 15, "KIND_OF"]],
});

/** The Ontology View's scope: every layer it reads at once, no filter, and ``search``. */
function viewScope(search: string): QueryScope {
  const s = scopeOf(WHOLE);
  const asked = s.readOnAsking ?? {};
  return {
    layers: new Set<LayerId>(s.layerOrder.filter((l) => !(l in s.readWhenOn) && !(l in asked))),
    domains: [],
    edgeTypes: [],
    hiddenStatuses: [],
    orphans: true,
    tags: [],
    sensitivities: [],
    search: searchItems(search),
    focus: null,
    depth: WHOLE.depth,
    products: false,
  };
}

const EMPTY: GraphData = { nodes: [], edges: [] };

// Reads are answered by the world unless held: a held read waits until ``release``.
let held: Promise<void> | null = null;
let release: () => void = () => {};

beforeEach(() => {
  held = null;
  const s = server(WORLD);
  wire.read = async (q) => {
    if (held !== null) await held;
    return s.read(q);
  };
});

describe("Open as query while the view reads a new search", () => {
  it("offers no query until the search's reads are in, then the search's own, with its hops", async () => {
    const { result } = renderHook(() => useOntology(WHOLE, true));
    act(() => result.current.query(EMPTY).onScope(viewScope("")));
    await waitFor(() => expect(result.current.query(EMPTY).text).not.toBeNull());
    const unsearched = result.current.query(EMPTY).text as string;
    expect(unsearched).not.toContain("ilike");

    held = new Promise<void>((resolve) => {
      release = resolve;
    });
    act(() => result.current.query(EMPTY).onScope(viewScope("species")));
    // The search's reads are in flight: the query before it is not this view's query any more.
    expect(result.current.query(EMPTY).text).toBeNull();

    act(() => release());
    await waitFor(() => expect(result.current.query(EMPTY).text).not.toBeNull());
    const searched = result.current.query(EMPTY).text as string;
    expect(searched).toContain('ilike: "%species%"');
    expect(searched).toContain("_termsHop1:");
  });
});
