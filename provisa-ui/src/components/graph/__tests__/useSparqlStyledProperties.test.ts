// Copyright (c) 2026 Kenneth Stott
// Canary: 8e1c5f37-2a94-4d6b-b07e-93c4d1a6f820
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// A SELECT's nodes hold no literals, so the property a class was given to caption or size by is read
// from the model for the nodes that lack it, as a Cypher node has it from the start.

import { describe, expect, it, vi } from "vitest";
import { renderHook } from "@testing-library/react";
import { useSparqlStyledProperties } from "../frame/use-sparql-styled-properties";
import type { GNode } from "../graph-model";

const node = (i: number, label: string, properties: Record<string, unknown> = {}): GNode => ({
  id: i,
  label,
  tableLabel: label,
  properties: { name: `n${i}`, iri: `http://x/${i}`, ...properties },
});
const nodes = (...list: GNode[]) => new Map(list.map((n) => [`${n.label}:${n.id}`, n]));

describe("the properties a class is styled by", () => {
  it("asks once for the nodes of the class that lack the caption's property", () => {
    const ask = vi.fn();
    renderHook(() =>
      useSparqlStyledProperties(
        "f",
        undefined,
        nodes(node(1, "p:pets"), node(2, "p:pets", { "p:pets#breed": "Husky" }), node(3, "p:vets")),
        { "p:pets": "p:pets#breed" },
        {},
        ask,
      ),
    );
    expect(ask).toHaveBeenCalledTimes(1);
    expect(ask).toHaveBeenCalledWith("f", ["http://x/1"]);
  });

  it("asks for what a size reads too", () => {
    const ask = vi.fn();
    renderHook(() =>
      useSparqlStyledProperties("f", undefined, nodes(node(1, "p:pets")), {}, { "p:pets": "p:pets#age" }, ask),
    );
    expect(ask).toHaveBeenCalledWith("f", ["http://x/1"]);
  });

  it("asks nothing for a degree, which the drawing computes", () => {
    const ask = vi.fn();
    renderHook(() =>
      useSparqlStyledProperties("f", undefined, nodes(node(1, "p:pets", { degTotal: 2 })), {}, { "p:pets": "degTotal" }, ask),
    );
    expect(ask).not.toHaveBeenCalled();
  });

  it("asks nothing again of a resource already answered, even with nothing or a refusal", () => {
    const ask = vi.fn();
    renderHook(() =>
      useSparqlStyledProperties(
        "f",
        { "http://x/1": { literals: [] }, "http://x/2": { error: "no" } },
        nodes(node(1, "p:pets"), node(2, "p:pets")),
        { "p:pets": "p:pets#breed" },
        {},
        ask,
      ),
    );
    expect(ask).not.toHaveBeenCalled();
  });

  it("asks nothing where nothing was chosen", () => {
    const ask = vi.fn();
    renderHook(() => useSparqlStyledProperties("f", undefined, nodes(node(1, "p:pets")), {}, {}, ask));
    expect(ask).not.toHaveBeenCalled();
  });
});
