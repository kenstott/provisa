// Copyright (c) 2026 Kenneth Stott
// Canary: 9b3e6d24-1f7a-4c85-b2d9-8e5a1c7f3d06
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// The ontology graph reads the ontology again after a glossary change Polly makes, as after a
// person's edit: an edge her add_glossary_term_edge added is drawn without a page reload.

import { renderHook, waitFor } from "@testing-library/react";
import { beforeEach, describe, expect, it, vi } from "vitest";
import "../../../i18n";
import { WHOLE } from "../ontologyPresets";
import { serverToolRan } from "../../../mcp/pollyWrites";
import { reader, server, world, type WorldSpec } from "./termServer";

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

const SPEC: WorldSpec = {
  products: [{ id: "p1", name: "city" }],
  tables: [{ id: 101, name: "complaints", product: "p1", columns: ["neighborhood", "address"] }],
  terms: [
    { id: 41, name: "neighborhood", columns: [[101, "neighborhood"]] },
    { id: 7, name: "location", columns: [[101, "address"]] },
  ],
  links: [],
};

/** Whether the graph draws an edge between the two terms. */
function joins(edges: { a: string; b: string }[], from: number, to: number): boolean {
  const ends = (id: number) => (e: { a: string; b: string }) =>
    e.a.endsWith(`:${id}`) || e.b.endsWith(`:${id}`);
  return edges.some((e) => ends(from)(e) && ends(to)(e));
}

beforeEach(() => {
  wire.read = server(world(SPEC)).read;
});

describe("a Polly glossary write refreshes the ontology graph", () => {
  it("draws the edge add_glossary_term_edge added, read again on the change", async () => {
    const { result } = renderHook(() => useOntology(WHOLE, true));
    await waitFor(() => expect(result.current.read.done).toBe(true));
    expect(joins(result.current.read.all!.edges, 41, 7)).toBe(false);

    // Server-side, neighborhood KIND_OF location now holds.
    wire.read = server(world({ ...SPEC, links: [[41, 7, "KIND_OF"]] })).read;
    serverToolRan("add_glossary_term_edge");

    await waitFor(() => expect(joins(result.current.read.all!.edges, 41, 7)).toBe(true));
  });
});
