// Copyright (c) 2026 Kenneth Stott
// Canary: a5c66ff8-7531-4422-9caf-151debb92bcf
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// The ERD: its domains, tables and relationships, drawn by the one graph (REQ-2239) in its layered
// preset. This view says what is drawn -- which domains are hidden or collapsed, what a focus names,
// what the page's filter matches, a saved layout's places -- and the graph draws it.

import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { useTranslation } from "react-i18next";
import { buildErdElements, erdJson } from "./erd-model";
import type { ErdNodeDomain, ErdNodeTable } from "./erd-model";
import { erdGraph } from "./erd-graph";
import { erdFocusIds, erdSearchMarks, heldTables } from "./erd-focus";
import type { ErdPanelProps } from "./sections/erd-types";
import { useErdCollapseLink } from "./useErdCollapseLink";
import { useErdOptions } from "./useErdOptions";
import { erdPlaces, standOf } from "./erd-saved-layout";
import type { SavedLayout } from "../../api/erdLayouts";
import { ProvisaGraph, type LayeredOptions } from "../provisa-graph/ProvisaGraph";
import { GraphOptionsBar } from "../provisa-graph/GraphOptionsBar";
import type { GraphPlaces } from "../provisa-graph/places";

const NO_TABLES: number[] = [];
const NO_RELATIONSHIPS: string[] = [];

const groupOf = (domainId: string) => `d:${domainId}`;

export function ErdPanel({
  tables,
  relationships,
  domains,
  checkedDomains,
  highlightTableIds = NO_TABLES,
  highlightRelationshipIds = NO_RELATIONSHIPS,
  search = "",
  page = "tables",
  collapsed = null,
  onCollapsed,
  onTableClick,
  onRelationshipClick,
  savedLayout = null,
  onArrange,
  placesRef,
  layoutMenu,
  overlay,
}: ErdPanelProps) {
  const { t } = useTranslation();
  const [options, changeOptions] = useErdOptions();
  const { columnDetail, edgeRouting, gridSnap, showOrphans } = options;
  const [collapsedDomains, setCollapsedDomains] = useState<Set<string>>(new Set());
  // REQ-2189: a saved layout chosen brings its options and what it collapses; its places are
  // stood after the automatic layout, with what was moved since over them.
  const [appliedLayout, setAppliedLayout] = useState<SavedLayout | null>(null);
  if (savedLayout !== appliedLayout) {
    setAppliedLayout(savedLayout);
    if (savedLayout !== null) {
      changeOptions(savedLayout.options);
      setCollapsedDomains(
        new Set(savedLayout.domains.filter((d) => d.collapsed).map((d) => d.domainId)),
      );
    }
  }
  const places = useMemo(
    () => (appliedLayout === null ? null : standOf(appliedLayout)),
    [appliedLayout],
  );
  // Domains to hide, from the host page's domain filter (not the panel's own state).
  const hiddenDomains = useMemo(() => {
    if (!checkedDomains) return new Set<string>();
    const hidden = new Set<string>();
    for (const tb of tables) if (!checkedDomains.has(tb.domainId)) hidden.add(tb.domainId);
    return hidden;
  }, [tables, checkedDomains]);

  const elements = useMemo(
    () =>
      buildErdElements(tables, relationships, domains, collapsedDomains, hiddenDomains, "all"),
    [tables, relationships, domains, collapsedDomains, hiddenDomains],
  );
  const graph = useMemo(() => erdGraph(elements, tables), [elements, tables]);

  const focused = useMemo(
    () => erdFocusIds(graph, highlightTableIds, highlightRelationshipIds),
    [graph, highlightTableIds, highlightRelationshipIds],
  );
  const filter = useMemo(() => erdSearchMarks(graph, search, page), [graph, search, page]);
  const [highlighted, setHighlighted] = useState<number[]>([]);
  const onFocused = useCallback((held: string[]) => {
    const next = heldTables(held);
    setHighlighted((before) => (before.join(",") === next.join(",") ? before : next));
  }, []);

  // The host's handlers, read when a click comes: the diagram is not rebuilt when they change.
  const clicks = useRef({ onTableClick, onRelationshipClick, onArrange });
  useEffect(() => {
    clicks.current = { onTableClick, onRelationshipClick, onArrange };
  }, [onTableClick, onRelationshipClick, onArrange]);
  const arrange = useCallback(() => clicks.current.onArrange?.(), []);

  const graphOptions: LayeredOptions = {
    detail: columnDetail,
    routing: edgeRouting,
    gridSnap,
    showOrphans,
  };
  /** An option the reader set: an edit of the arrangement. */
  const setOptions = (change: Partial<LayeredOptions>) => {
    changeOptions({
      ...(change.detail !== undefined ? { columnDetail: change.detail } : {}),
      ...(change.routing !== undefined ? { edgeRouting: change.routing } : {}),
      ...(change.gridSnap !== undefined ? { gridSnap: change.gridSnap } : {}),
      ...(change.showOrphans !== undefined ? { showOrphans: change.showOrphans } : {}),
    });
    clicks.current.onArrange?.();
  };

  // The domains drawn, by the graph's group ids.
  const allDomainIds = [...new Set(tables.map((tb) => tb.domainId))];
  const visibleDomainIds = allDomainIds.filter((id) => !hiddenDomains.has(id));
  const collapsedGroups = useMemo(
    () => new Set([...collapsedDomains].map(groupOf)),
    [collapsedDomains],
  );
  const setCollapsedGroups = useCallback((next: Set<string>) => {
    setCollapsedDomains(new Set([...next].map((id) => id.slice(2))));
  }, []);
  useErdCollapseLink(
    collapsed,
    onCollapsed,
    visibleDomainIds,
    collapsedDomains,
    setCollapsedDomains,
  );

  // REQ-2189: what a save reads off the canvas.
  const graphPlacesRef = useRef<(() => GraphPlaces | null) | null>(null);
  useEffect(() => {
    if (!placesRef) return;
    placesRef.current = () => {
      const read = graphPlacesRef.current?.() ?? null;
      return read === null ? null : erdPlaces(read, options, collapsedDomains);
    };
  }, [placesRef, options, collapsedDomains]);

  // REQ-524: the diagram as data -- built by the same function the ERD draws from, with the
  // panel's current collapse/hide/detail state, so it describes exactly what is drawn.
  const exportData = useCallback(
    () =>
      erdJson(
        buildErdElements(
          tables,
          relationships,
          domains,
          collapsedDomains,
          hiddenDomains,
          columnDetail,
        ),
      ),
    [tables, relationships, domains, collapsedDomains, hiddenDomains, columnDetail],
  );

  return (
    <div
      data-testid="erd-panel"
      data-highlighted={highlighted.join(",")}
      style={{ height: "100%", display: "flex", flexDirection: "column", overflow: "hidden" }}
    >
      <ProvisaGraph
        graph={graph}
        preset="layered"
        testId="erd"
        exportName="erd"
        exportData={exportData}
        options={graphOptions}
        collapsed={collapsedGroups}
        onCollapsedChange={setCollapsedGroups}
        onNodeClick={(_id, data) =>
          clicks.current.onTableClick(data("tableId") as number, data("tableName") as string)
        }
        onEdgeClick={(_id, data) =>
          clicks.current.onRelationshipClick?.(data("relationshipId") as string)
        }
        focused={focused}
        onFocused={onFocused}
        filter={filter}
        places={places}
        placesRef={graphPlacesRef}
        onArrange={arrange}
        overlay={overlay}
        header={
          <GraphOptionsBar
            testId="erd"
            tourId="rels-erd-modal"
            lead={layoutMenu}
            options={graphOptions}
            onOptions={setOptions}
            groupIds={visibleDomainIds.map(groupOf)}
            collapsed={collapsedGroups}
            onCollapsedChange={(next) => {
              arrange();
              setCollapsedGroups(next);
            }}
            help={{ title: t("erdModal.title"), text: t("erdModal.hint") }}
          />
        }
      />
    </div>
  );
}

export type { ErdNodeDomain, ErdNodeTable };
