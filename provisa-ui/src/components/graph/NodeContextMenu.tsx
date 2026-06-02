// Copyright (c) 2026 Kenneth Stott
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

/* eslint-disable react-hooks/refs --
   The menu reads latest-value refs (anchored set, cy instance, layout state)
   that mirror the imperative Cytoscape engine; these reads are intentional. */

import type { Relationship } from "../../types/admin";
import type { GNode, GEdge } from "./graph-model";
import type { CyInstance } from "./cytoscape-types";

export interface NodeCtxMenuState {
  x: number;
  y: number;
  nodeId: string;
  selectedNodeIds: string[];
}

interface NodeContextMenuProps {
  menu: NodeCtxMenuState;
  menuRef: React.Ref<HTMLDivElement>;
  nodes: Map<string, GNode>;
  overlayNodes: Map<string, GNode>;
  pkMap: Record<string, string[]>;
  relationships: Relationship[];
  cyRef: { current: CyInstance | null };
  anchoredRef: { current: Set<string> };
  activeLayoutRef: { current: { stop: () => void } | null };
  layoutRunningRef: { current: boolean };
  nudgeLayoutRef: { current: (freeNodes?: Set<string>, aggressive?: boolean) => void };
  onExcludeNode: (nodeKeys: string[]) => void;
  onToggleChildrenBatch: (nodeKeys: string[], circular?: boolean) => void;
  onToggleParentsBatch: (nodeKeys: string[], circular?: boolean) => void;
  onSelect: (item: { kind: "node"; data: GNode } | { kind: "edge"; data: GEdge } | null) => void;
  setNodeCtxMenu: (m: NodeCtxMenuState | null) => void;
  showingChildrenNatural: Set<string>;
  showingChildrenCircular: Set<string>;
  showingParents: Set<string>;
  showingParentsCircular: Set<string>;
}

export function NodeContextMenu({
  menu: nodeCtxMenu,
  menuRef,
  nodes,
  overlayNodes,
  pkMap,
  relationships,
  cyRef,
  anchoredRef,
  activeLayoutRef,
  layoutRunningRef,
  nudgeLayoutRef,
  onExcludeNode,
  onToggleChildrenBatch,
  onToggleParentsBatch,
  onSelect,
  setNodeCtxMenu,
  showingChildrenNatural,
  showingChildrenCircular,
  showingParents,
  showingParentsCircular,
}: NodeContextMenuProps) {
  const ctxNode = nodes.get(nodeCtxMenu.nodeId) ?? overlayNodes.get(nodeCtxMenu.nodeId);
  const ctxLabel = ctxNode?.label ?? "";
  const tableLabel = ctxLabel.includes(":") ? ctxLabel.split(":").pop()! : ctxLabel;
  const ctxPkCols = pkMap[ctxLabel] ?? [];
  const hasPk = ctxPkCols.length > 0;
  const norm = (s: string) => s.toLowerCase().replace(/_/g, "");
  const tl = norm(tableLabel);
  const cl = norm(ctxLabel);
  const myPkKey = (pkMap[ctxLabel] ?? pkMap[tableLabel] ?? []).join(",");
  const siblingTls = myPkKey
    ? Object.entries(pkMap)
        .filter(([lbl, cols]) => cols.join(",") === myPkKey && lbl !== ctxLabel && lbl !== tableLabel)
        .map(([lbl]) => norm(lbl.includes(":") ? lbl.split(":").pop()! : lbl))
    : [];
  const isSource = (r: (typeof relationships)[0]) =>
    norm(r.sourceTableName) === tl ||
    norm(r.sourceTableName) === cl ||
    siblingTls.includes(norm(r.sourceTableName));
  const isTarget = (r: (typeof relationships)[0]) =>
    norm(r.targetTableName) === tl ||
    norm(r.targetTableName) === cl ||
    siblingTls.includes(norm(r.targetTableName));
  const hasChildRels = relationships.some(isSource);
  const hasParentRels = relationships.some(isTarget);
  return (
    <div
      ref={menuRef}
      className="gf-node-ctx-menu"
      style={{ left: nodeCtxMenu.x, top: nodeCtxMenu.y, visibility: "hidden" }}
      onMouseDown={(e) => e.stopPropagation()}
    >
      <button
        className="gf-node-ctx-item"
        disabled={!hasPk}
        title={
          hasPk
            ? "Exclude this node from the query"
            : "No primary key configured — cannot exclude"
        }
        style={!hasPk ? { opacity: 0.4, cursor: "not-allowed" } : undefined}
        onClick={() => {
          if (!hasPk) return;
          const cy = cyRef.current;
          nodeCtxMenu.selectedNodeIds.forEach((id) => {
            anchoredRef.current.delete(id);
            if (cy) cy.remove(cy.$id(id));
          });
          onExcludeNode(nodeCtxMenu.selectedNodeIds);
          onSelect(null);
          setNodeCtxMenu(null);
        }}
      >
        Exclude{" "}
        {nodeCtxMenu.selectedNodeIds.length > 1
          ? `${nodeCtxMenu.selectedNodeIds.length} nodes`
          : "from query"}
      </button>
      {nodeCtxMenu.selectedNodeIds.some((id) => anchoredRef.current.has(id)) && (
        <button
          className="gf-node-ctx-item"
          onClick={() => {
            const cy = cyRef.current;
            nodeCtxMenu.selectedNodeIds.forEach((id) => {
              anchoredRef.current.delete(id);
              if (cy) {
                cy.$id(id).unlock();
                cy.$id(id).removeClass("pinned");
              }
            });
            // Stop any in-progress layout and force-reset the gate so nudge can start fresh
            try {
              activeLayoutRef.current?.stop();
            } catch {
              /* ignore */
            }
            activeLayoutRef.current = null;
            layoutRunningRef.current = false;
            setNodeCtxMenu(null);
            nudgeLayoutRef.current();
          }}
        >
          Unfix position
        </button>
      )}
      <button
        className="gf-node-ctx-item"
        disabled={!hasChildRels}
        style={!hasChildRels ? { opacity: 0.4, cursor: "not-allowed" } : undefined}
        title={hasChildRels ? undefined : "No outgoing relationships for this node type"}
        onClick={() => {
          if (!hasChildRels) return;
          onToggleChildrenBatch(nodeCtxMenu.selectedNodeIds, false);
          setNodeCtxMenu(null);
        }}
      >
        {showingChildrenNatural.has(nodeCtxMenu.nodeId) ? "Hide children" : "Show children"}
      </button>
      <button
        className="gf-node-ctx-item"
        disabled={!hasChildRels}
        style={!hasChildRels ? { opacity: 0.4, cursor: "not-allowed" } : undefined}
        title={
          hasChildRels
            ? "Arrange children in a ring around this node"
            : "No outgoing relationships for this node type"
        }
        onClick={() => {
          if (!hasChildRels) return;
          onToggleChildrenBatch(nodeCtxMenu.selectedNodeIds, true);
          setNodeCtxMenu(null);
        }}
      >
        {showingChildrenCircular.has(nodeCtxMenu.nodeId)
          ? "Hide children (circular)"
          : "Show children (circular)"}
      </button>
      <button
        className="gf-node-ctx-item"
        disabled={!hasParentRels}
        style={!hasParentRels ? { opacity: 0.4, cursor: "not-allowed" } : undefined}
        title={hasParentRels ? undefined : "No incoming relationships for this node type"}
        onClick={() => {
          if (!hasParentRels) return;
          onToggleParentsBatch(nodeCtxMenu.selectedNodeIds, false);
          setNodeCtxMenu(null);
        }}
      >
        {showingParents.has(nodeCtxMenu.nodeId) ? "Hide parents" : "Show parents"}
      </button>
      <button
        className="gf-node-ctx-item"
        disabled={!hasParentRels}
        style={!hasParentRels ? { opacity: 0.4, cursor: "not-allowed" } : undefined}
        title={
          hasParentRels
            ? "Arrange parents in a ring around this node"
            : "No incoming relationships for this node type"
        }
        onClick={() => {
          if (!hasParentRels) return;
          onToggleParentsBatch(nodeCtxMenu.selectedNodeIds, true);
          setNodeCtxMenu(null);
        }}
      >
        {showingParentsCircular.has(nodeCtxMenu.nodeId)
          ? "Hide parents (circular)"
          : "Show parents (circular)"}
      </button>
      <button
        className="gf-node-ctx-item"
        onClick={() => {
          const cy = cyRef.current;
          if (cy) {
            const selectedIds = new Set(nodeCtxMenu.selectedNodeIds);
            cy.nodes().forEach((n) => {
              if (selectedIds.has(n.id())) n.unselect();
              else n.select();
            });
          }
          onSelect(null);
          setNodeCtxMenu(null);
        }}
      >
        Invert selection
      </button>
      <div className="gf-node-ctx-submenu-wrap">
        <button className="gf-node-ctx-item gf-node-ctx-item--has-sub">Select</button>
        <div className="gf-node-ctx-submenu">
          <button
            className="gf-node-ctx-item"
            onClick={() => {
              const cy = cyRef.current;
              if (cy) cy.nodes().select();
              onSelect(null);
              setNodeCtxMenu(null);
            }}
          >
            All
          </button>
          <button
            className="gf-node-ctx-item"
            onClick={() => {
              const cy = cyRef.current;
              if (cy) {
                const targetLabel = ctxNode?.label ?? "";
                cy.nodes().forEach((n) => {
                  if ((n.data("label") as string) === targetLabel) n.select();
                  else n.unselect();
                });
              }
              onSelect(null);
              setNodeCtxMenu(null);
            }}
          >
            All of this type
          </button>
          <button
            className="gf-node-ctx-item"
            onClick={() => {
              const cy = cyRef.current;
              if (cy) {
                cy.nodes().unselect();
                nodeCtxMenu.selectedNodeIds.forEach((id) => {
                  cy.$id(id).neighborhood("node").select();
                });
              }
              onSelect(null);
              setNodeCtxMenu(null);
            }}
          >
            Connected
          </button>
          <button
            className="gf-node-ctx-item"
            onClick={() => {
              const cy = cyRef.current;
              if (cy) {
                cy.nodes().unselect();
                nodeCtxMenu.selectedNodeIds.forEach((id) => {
                  // outgoing edges → targets
                  cy.$id(id)
                    .neighborhood("edge")
                    .forEach((e) => {
                      if ((e.source().id() as string) === id) e.target().select();
                    });
                });
              }
              onSelect(null);
              setNodeCtxMenu(null);
            }}
          >
            Children
          </button>
          <button
            className="gf-node-ctx-item"
            onClick={() => {
              const cy = cyRef.current;
              if (cy) {
                cy.nodes().unselect();
                nodeCtxMenu.selectedNodeIds.forEach((id) => {
                  // incoming edges → sources
                  cy.$id(id)
                    .neighborhood("edge")
                    .forEach((e) => {
                      if ((e.target().id() as string) === id) e.source().select();
                    });
                });
              }
              onSelect(null);
              setNodeCtxMenu(null);
            }}
          >
            Parents
          </button>
        </div>
      </div>
      <div className="gf-node-ctx-divider" />
      <button
        className="gf-node-ctx-item gf-node-ctx-item--danger"
        onClick={() => {
          const cy = cyRef.current;
          if (cy) {
            nodeCtxMenu.selectedNodeIds.forEach((id) => {
              anchoredRef.current.delete(id);
              cy.remove(cy.$id(id));
            });
          }
          onSelect(null);
          setNodeCtxMenu(null);
        }}
      >
        Remove{" "}
        {nodeCtxMenu.selectedNodeIds.length > 1
          ? `${nodeCtxMenu.selectedNodeIds.length} nodes`
          : "node"}
      </button>
    </div>
  );
}
