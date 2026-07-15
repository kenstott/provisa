// Copyright (c) 2026 Kenneth Stott
// Canary: ba809328-21fa-479d-bd93-bfdc18220666
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

import { useState } from "react";
import { Divider, UnstyledButton } from "@mantine/core";
import { useTranslation } from "react-i18next";
import type { Relationship } from "../../types/admin";
import type { GNode, GEdge } from "./graph-model";
import type { CyInstance } from "./cytoscape-types";
import { tableLabel as dbTableLabel } from "../../naming";

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
  labelToTableLabel: Record<string, string>;
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
  labelToTableLabel,
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
  const { t } = useTranslation();
  const [submenuOpen, setSubmenuOpen] = useState(false);
  const ctxNode = nodes.get(nodeCtxMenu.nodeId) ?? overlayNodes.get(nodeCtxMenu.nodeId);
  const ctxLabel = ctxNode?.label ?? "";
  const tableLabel = ctxNode?.tableLabel ?? "";
  const ctxPkCols = pkMap[ctxLabel] ?? [];
  const hasPk = ctxPkCols.length > 0;
  const myPkKey = ctxPkCols.join(",");
  const siblingLabels = myPkKey
    ? Object.entries(pkMap)
        .filter(([lbl, cols]) => cols.join(",") === myPkKey && lbl !== ctxLabel)
        .map(([lbl]) => labelToTableLabel[lbl] ?? lbl)
    : [];
  // Relationship endpoints and node labels both derive from the same label function,
  // so match the derived label exactly (directly or via a PK-equivalent sibling).
  const isSource = (r: (typeof relationships)[0]) =>
    dbTableLabel(r.sourceTableName) === tableLabel ||
    siblingLabels.includes(dbTableLabel(r.sourceTableName));
  const isTarget = (r: (typeof relationships)[0]) =>
    dbTableLabel(r.targetTableName) === tableLabel ||
    siblingLabels.includes(dbTableLabel(r.targetTableName));
  const hasChildRels = relationships.some(isSource);
  const hasParentRels = relationships.some(isTarget);
  const multiSelected = nodeCtxMenu.selectedNodeIds.length > 1;
  return (
    <div
      ref={menuRef}
      className="gf-node-ctx-menu"
      role="menu"
      aria-orientation="vertical"
      data-testid="node-ctx-menu"
      style={{ left: nodeCtxMenu.x, top: nodeCtxMenu.y, visibility: "hidden" }}
      onMouseDown={(e) => e.stopPropagation()}
    >
      <UnstyledButton
        component="button"
        role="menuitem"
        className="gf-node-ctx-item"
        data-testid="node-ctx-exclude"
        disabled={!hasPk}
        aria-disabled={!hasPk}
        title={hasPk ? t("nodeContextMenu.excludeTooltip") : t("nodeContextMenu.excludeDisabledTooltip")}
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
        {multiSelected
          ? t("nodeContextMenu.excludeCount", { count: nodeCtxMenu.selectedNodeIds.length })
          : t("nodeContextMenu.excludeFromQuery")}
      </UnstyledButton>
      {nodeCtxMenu.selectedNodeIds.some((id) => anchoredRef.current.has(id)) && (
        <UnstyledButton
          component="button"
          role="menuitem"
          className="gf-node-ctx-item"
          data-testid="node-ctx-unfix"
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
          {t("nodeContextMenu.unfixPosition")}
        </UnstyledButton>
      )}
      <UnstyledButton
        component="button"
        role="menuitem"
        className="gf-node-ctx-item"
        data-testid="node-ctx-toggle-children"
        disabled={!hasChildRels}
        aria-disabled={!hasChildRels}
        style={!hasChildRels ? { opacity: 0.4, cursor: "not-allowed" } : undefined}
        title={hasChildRels ? undefined : t("nodeContextMenu.noOutgoingTooltip")}
        onClick={() => {
          if (!hasChildRels) return;
          onToggleChildrenBatch(nodeCtxMenu.selectedNodeIds, false);
          setNodeCtxMenu(null);
        }}
      >
        {showingChildrenNatural.has(nodeCtxMenu.nodeId)
          ? t("nodeContextMenu.hideChildren")
          : t("nodeContextMenu.showChildren")}
      </UnstyledButton>
      <UnstyledButton
        component="button"
        role="menuitem"
        className="gf-node-ctx-item"
        data-testid="node-ctx-toggle-children-circular"
        disabled={!hasChildRels}
        aria-disabled={!hasChildRels}
        style={!hasChildRels ? { opacity: 0.4, cursor: "not-allowed" } : undefined}
        title={
          hasChildRels
            ? t("nodeContextMenu.showChildrenCircularTooltip")
            : t("nodeContextMenu.noOutgoingTooltip")
        }
        onClick={() => {
          if (!hasChildRels) return;
          onToggleChildrenBatch(nodeCtxMenu.selectedNodeIds, true);
          setNodeCtxMenu(null);
        }}
      >
        {showingChildrenCircular.has(nodeCtxMenu.nodeId)
          ? t("nodeContextMenu.hideChildrenCircular")
          : t("nodeContextMenu.showChildrenCircular")}
      </UnstyledButton>
      <UnstyledButton
        component="button"
        role="menuitem"
        className="gf-node-ctx-item"
        data-testid="node-ctx-toggle-parents"
        disabled={!hasParentRels}
        aria-disabled={!hasParentRels}
        style={!hasParentRels ? { opacity: 0.4, cursor: "not-allowed" } : undefined}
        title={hasParentRels ? undefined : t("nodeContextMenu.noIncomingTooltip")}
        onClick={() => {
          if (!hasParentRels) return;
          onToggleParentsBatch(nodeCtxMenu.selectedNodeIds, false);
          setNodeCtxMenu(null);
        }}
      >
        {showingParents.has(nodeCtxMenu.nodeId)
          ? t("nodeContextMenu.hideParents")
          : t("nodeContextMenu.showParents")}
      </UnstyledButton>
      <UnstyledButton
        component="button"
        role="menuitem"
        className="gf-node-ctx-item"
        data-testid="node-ctx-toggle-parents-circular"
        disabled={!hasParentRels}
        aria-disabled={!hasParentRels}
        style={!hasParentRels ? { opacity: 0.4, cursor: "not-allowed" } : undefined}
        title={
          hasParentRels
            ? t("nodeContextMenu.showParentsCircularTooltip")
            : t("nodeContextMenu.noIncomingTooltip")
        }
        onClick={() => {
          if (!hasParentRels) return;
          onToggleParentsBatch(nodeCtxMenu.selectedNodeIds, true);
          setNodeCtxMenu(null);
        }}
      >
        {showingParentsCircular.has(nodeCtxMenu.nodeId)
          ? t("nodeContextMenu.hideParentsCircular")
          : t("nodeContextMenu.showParentsCircular")}
      </UnstyledButton>
      <UnstyledButton
        component="button"
        role="menuitem"
        className="gf-node-ctx-item"
        data-testid="node-ctx-invert-selection"
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
        {t("nodeContextMenu.invertSelection")}
      </UnstyledButton>
      <div
        className="gf-node-ctx-submenu-wrap"
        onMouseEnter={() => setSubmenuOpen(true)}
        onMouseLeave={() => setSubmenuOpen(false)}
      >
        <UnstyledButton
          component="button"
          role="menuitem"
          className="gf-node-ctx-item gf-node-ctx-item--has-sub"
          data-testid="node-ctx-select-submenu"
          aria-haspopup="menu"
          aria-expanded={submenuOpen}
        >
          {t("nodeContextMenu.select")}
        </UnstyledButton>
        <div className="gf-node-ctx-submenu" role="menu">
          <UnstyledButton
            component="button"
            role="menuitem"
            className="gf-node-ctx-item"
            data-testid="node-ctx-select-all"
            onClick={() => {
              const cy = cyRef.current;
              if (cy) cy.nodes().select();
              onSelect(null);
              setNodeCtxMenu(null);
            }}
          >
            {t("nodeContextMenu.selectAll")}
          </UnstyledButton>
          <UnstyledButton
            component="button"
            role="menuitem"
            className="gf-node-ctx-item"
            data-testid="node-ctx-select-all-of-type"
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
            {t("nodeContextMenu.selectAllOfType")}
          </UnstyledButton>
          <UnstyledButton
            component="button"
            role="menuitem"
            className="gf-node-ctx-item"
            data-testid="node-ctx-select-connected"
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
            {t("nodeContextMenu.selectConnected")}
          </UnstyledButton>
          <UnstyledButton
            component="button"
            role="menuitem"
            className="gf-node-ctx-item"
            data-testid="node-ctx-select-children"
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
            {t("nodeContextMenu.selectChildren")}
          </UnstyledButton>
          <UnstyledButton
            component="button"
            role="menuitem"
            className="gf-node-ctx-item"
            data-testid="node-ctx-select-parents"
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
            {t("nodeContextMenu.selectParents")}
          </UnstyledButton>
        </div>
      </div>
      <Divider className="gf-node-ctx-divider" />
      <UnstyledButton
        component="button"
        role="menuitem"
        className="gf-node-ctx-item gf-node-ctx-item--danger"
        data-testid="node-ctx-remove"
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
        {multiSelected
          ? t("nodeContextMenu.removeCount", { count: nodeCtxMenu.selectedNodeIds.length })
          : t("nodeContextMenu.removeNode")}
      </UnstyledButton>
    </div>
  );
}
