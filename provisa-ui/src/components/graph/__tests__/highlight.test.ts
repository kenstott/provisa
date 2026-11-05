// Copyright (c) 2026 Kenneth Stott
// Canary: cfb428a3-a928-47ad-b962-c9b482908d04
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// An Overview pill picks out its label's nodes, or its type's edges and their ends, in its frame's
// graph: those stand out and everything else dims; clearing it undims everything.

import { describe, it, expect } from "vitest";
import cytoscape from "cytoscape";
import { applyHighlight } from "../canvas/highlight";
import type { CyInstance } from "../cytoscape-types";

function graph() {
  return cytoscape({
    headless: true,
    elements: [
      { group: "nodes", data: { id: "Owners:1", label: "Owners" } },
      { group: "nodes", data: { id: "Pets:1", label: "Pets" } },
      { group: "nodes", data: { id: "Pets:2", label: "Pets" } },
      { group: "nodes", data: { id: "Vets:1", label: "Vets" } },
      { group: "edges", data: { id: "o1", source: "Owners:1", target: "Pets:1", label: "OWNS" } },
      { group: "edges", data: { id: "t1", source: "Vets:1", target: "Pets:2", label: "TREATS" } },
    ],
  });
}

const classed = (cy: ReturnType<typeof graph>, cls: string) =>
  cy
    .elements(`.${cls}`)
    .map((el) => el.id())
    .sort();

describe("an Overview pill's highlight", () => {
  it("picks out a label's nodes and dims the rest", () => {
    const cy = graph();
    applyHighlight(cy as unknown as CyInstance, { kind: "label", value: "Pets" });
    expect(classed(cy, "gc-lit")).toEqual(["Pets:1", "Pets:2"]);
    expect(classed(cy, "gc-dim")).toEqual(["Owners:1", "Vets:1", "o1", "t1"]);
  });

  it("picks out a type's edges, keeping their ends, and dims the rest", () => {
    const cy = graph();
    applyHighlight(cy as unknown as CyInstance, { kind: "type", value: "OWNS" });
    expect(classed(cy, "gc-lit")).toEqual(["o1"]);
    expect(classed(cy, "gc-dim")).toEqual(["Pets:2", "Vets:1", "t1"]);
  });

  it("clears, leaving nothing dimmed", () => {
    const cy = graph();
    applyHighlight(cy as unknown as CyInstance, { kind: "label", value: "Pets" });
    applyHighlight(cy as unknown as CyInstance, null);
    expect(classed(cy, "gc-lit")).toEqual([]);
    expect(classed(cy, "gc-dim")).toEqual([]);
  });
});
