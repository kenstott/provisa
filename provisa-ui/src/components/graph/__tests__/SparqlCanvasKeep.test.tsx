// Copyright (c) 2026 Kenneth Stott
// Canary: e4a1c672-08db-49f6-a3b5-7d2f90c6e181
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2135: a node's expansion grows the drawing around what is there. The nodes a new drawing
// shares with the last stay put, new ones start beside a neighbour, and a SPARQL node's menu offers
// its expansion on any resource, reading no model.

import { describe, expect, it, vi } from "vitest";
import { render, screen } from "../../../test-utils/render";
import { nodePositions, restorePositions } from "../canvas/canvas-helpers";
import { NodeContextMenu } from "../NodeContextMenu";
import type { CyInstance } from "../cytoscape-types";
import type { GNode } from "../graph-model";

function fakeNode(id: string, gid: number, x?: number, y?: number) {
  const n = {
    pos: x === undefined ? null : { x, y: y ?? 0 },
    links: [] as unknown[],
    id: () => id,
    data: (k: string) => (k === "_node" ? { id: gid } : undefined),
    position(p?: { x: number; y: number }) {
      if (p) n.pos = p;
      return n.pos ?? { x: 0, y: 0 };
    },
    neighborhood: () => Object.assign([...n.links], { filter: (f: (e: unknown) => boolean) => n.links.filter(f) }),
  };
  return n;
}
const fakeCy = (nodes: ReturnType<typeof fakeNode>[]) =>
  ({ nodes: () => ({ forEach: (f: (n: unknown) => void) => nodes.forEach(f) }) }) as unknown as CyInstance;

describe("a drawing that keeps its nodes", () => {
  it("holds each node's place by its own id", () => {
    const held = nodePositions(fakeCy([fakeNode("a:1", 1, 10, 20)]));
    expect(held.get(1)).toEqual({ x: 10, y: 20 });
  });

  it("puts shared nodes back, and a new one beside its neighbour", () => {
    // The node's label changed with the merge (its id in the drawing did), its own id did not.
    const kept = fakeNode("sales:orders:1", 1);
    const fresh = fakeNode("sales:customers:2", 2);
    fresh.links = [kept];
    const held = new Map([[1, { x: 100, y: 100 }]]);
    const placed = restorePositions(fakeCy([kept, fresh]), held, 50);
    expect(kept.pos).toEqual({ x: 100, y: 100 });
    expect(placed).toEqual(new Set(["sales:customers:2"]));
    const { x, y } = fresh.pos!;
    expect(Math.hypot(x - 100, y - 100)).toBeCloseTo(50);
  });
});

const NODE: GNode = {
  id: 1,
  label: "sales:orders",
  tableLabel: "sales:orders",
  properties: { name: "1", iri: "https://x/id/1" },
};
const BNODE: GNode = { id: 2, label: "Resource", tableLabel: "Resource", properties: { name: "_:b" } };

function menu(nodeId: string, onToggleChildrenBatch = vi.fn()) {
  render(
    <NodeContextMenu
      menu={{ x: 0, y: 0, nodeId, selectedNodeIds: [nodeId] }}
      menuRef={{ current: null }}
      nodes={
        new Map([
          ["n", NODE],
          ["b", BNODE],
        ])
      }
      overlayNodes={new Map()}
      describable
      pkMap={{}}
      labelToTableLabel={{}}
      relationships={[]}
      cyRef={{ current: null }}
      anchoredRef={{ current: new Set() }}
      activeLayoutRef={{ current: null }}
      layoutRunningRef={{ current: false }}
      nudgeLayoutRef={{ current: () => {} }}
      onExcludeNode={vi.fn()}
      onToggleChildrenBatch={onToggleChildrenBatch}
      onToggleParentsBatch={vi.fn()}
      onSelect={vi.fn()}
      setNodeCtxMenu={vi.fn()}
      showingChildrenNatural={new Set()}
      showingChildrenCircular={new Set()}
      showingParents={new Set()}
      showingParentsCircular={new Set()}
    />,
  );
  return onToggleChildrenBatch;
}

describe("a SPARQL node's menu", () => {
  it("offers expanding a resource, with no relationships to read", () => {
    const toggled = menu("n");
    const item = screen.getByTestId("node-ctx-toggle-children");
    expect(item).toBeEnabled();
    item.click();
    expect(toggled).toHaveBeenCalledWith(["n"], false);
  });

  it("offers only what a DESCRIBE does", () => {
    menu("n");
    expect(screen.queryByTestId("node-ctx-toggle-children-circular")).toBeNull();
    expect(screen.queryByTestId("node-ctx-toggle-parents")).toBeNull();
    expect(screen.queryByTestId("node-ctx-toggle-parents-circular")).toBeNull();
  });

  it("does not offer it on a blank node, which has no IRI to ask of", () => {
    menu("b");
    expect(screen.getByTestId("node-ctx-toggle-children")).toBeDisabled();
  });
});
