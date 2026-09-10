// Copyright (c) 2026 Kenneth Stott
// Canary: 2d7b9e14-6a03-4c58-8f21-9b0c3e7a5d46
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1160/REQ-1161: column-level lineage DAG viz. Nodes are columns coloured by kind (source /
// derived / command boundary) with a materialized ring; edges carry the named transform; cycle
// members are ringed by classification (feedback vs error). Left-to-right, source → output.

import { useEffect, useRef, useState } from "react";
import cytoscape, { type NodeSingular } from "cytoscape";
import elkRaw from "cytoscape-elk";
import { ActionIcon, Text, Tooltip } from "@mantine/core";
import { Download, Expand, ChevronsDownUp, ChevronsUpDown } from "lucide-react";
import { FitScreenIcon } from "../graph/GraphIcons";
import type { LineageGraphData } from "../../api/lineage";
import type { CyLayoutOptions } from "../graph/cytoscape-types";
import { LineageLegend } from "./LineageLegend";
import type { DescribeColumn } from "./column-descriptions";
import { FOOTER_H, ROLE_STYLE, ROW_H, buildLineageModel, rowWidth } from "./lineage-layout";
import type { Role } from "./lineage-layout";

// ELK lays the relation boxes out in layers (REQ-1665); the ERD registers it the same way.
type CyExt = Parameters<typeof cytoscape.use>[0];
try {
  cytoscape.use((elkRaw as { default?: CyExt }).default ?? (elkRaw as CyExt));
} catch {
  /* already registered */
}

// The named `Core` export resolves to the package's own bundler-broken type (no fit/png);
// the factory return carries the ambient shim's full instance API, so derive it from there.
// That shim is still missing `resize`, a real runtime method (http://js.cytoscape.org/#cy.resize)
// — the same declaration breakage under moduleResolution "bundler" that components/graph/
// cytoscape-types.ts exists to work around. Name it here rather than casting the instance away.
type CyCore = ReturnType<typeof cytoscape> & { resize(): void };

interface LineageDagProps {
  graph: LineageGraphData;
  height?: number | string;
  onNodeClick?: (nodeId: string) => void;
  // REQ-1627: relations rendered as ONE node with their column-edges rolled up. Federation-scale
  // graphs are unreadable column-by-column, so Complete Lineage collapses everything by default and
  // the reader expands the relations they are actually tracing.
  collapsedRelations?: ReadonlySet<string>;
  onToggleRelation?: (relation: string) => void;
  onCollapseAll?: () => void;
  onExpandAll?: () => void;
  // Opens the same graph in a near-fullscreen modal; omitted when already rendering inside one.
  onOpenModal?: () => void;
  // REQ-1665: the box the statement's final projection lands in — the view or MV it defines, else
  // "result".
  resultLabel?: string;
  // REQ-1665: draw every column as a row. Complete Lineage is read from a role's vantage point and
  // every column that role can see is in use, so none is folded into "+N unused".
  allColumns?: boolean;
  // REQ-1667: swimlanes by distance from `members` (lane 0), separated by dotted rules and titled
  // by `title(lane)`. Every box is laid out in its lane; nothing is shelf-packed.
  lanes?: { members: ReadonlySet<string>; title: (lane: number) => string };
  // The role key, floated over the canvas's bottom-left corner.
  legend?: boolean;
  // Column and edge counts, floated over the bottom-right corner.
  stats?: boolean;
  // A field's registered description, shown at the cursor while it hovers the row.
  describeColumn?: DescribeColumn;
}

export function LineageDag({
  graph,
  height = 520,
  onNodeClick,
  collapsedRelations,
  onToggleRelation,
  onCollapseAll,
  onExpandAll,
  onOpenModal,
  resultLabel,
  allColumns = false,
  lanes,
  legend = false,
  stats = false,
  describeColumn,
}: LineageDagProps): React.ReactElement {
  const containerRef = useRef<HTMLDivElement | null>(null);
  const cyRef = useRef<CyCore | null>(null);
  const [hovered, setHovered] = useState(false);
  // REQ-1667: what a click on a box does, shown at the cursor while it hovers a box name or a
  // collapsed box — the affordance is otherwise invisible.
  const [hint, setHint] = useState<{
    x: number;
    y: number;
    text: string;
    columns: string[];
  } | null>(null);

  const fitToScreen = () => cyRef.current?.fit(undefined, 30);
  const downloadPng = () => {
    const cy = cyRef.current;
    if (!cy) return;
    const uri = cy.png({ full: true, scale: 2, bg: "#ffffff" });
    const a = document.createElement("a");
    a.href = uri;
    a.download = "lineage.png";
    a.click();
  };
  // The cytoscape effect binds this handler once; the ref keeps the latest callback without
  // rebuilding the graph. Assign it in a commit-phase effect — a render-phase ref write mutates
  // state while React is rendering.
  const clickRef = useRef(onNodeClick);
  const toggleRef = useRef(onToggleRelation);
  useEffect(() => {
    clickRef.current = onNodeClick;
    toggleRef.current = onToggleRelation;
  });

  useEffect(() => {
    if (!containerRef.current) return;
    const laned = lanes !== undefined;
    const model = buildLineageModel(graph, collapsedRelations, {
      resultLabel,
      allColumns,
      lanesAround: lanes?.members,
    });

    // A relation is a compound node whose children are its rows (and the "+N unused" footer).
    // ELK lays the hierarchy out as one layered graph (INCLUDE_CHILDREN): rows carry no edges
    // among themselves, so inside a box they share one layer and stack vertically, and the
    // hierarchical crossing minimization orders rows and boxes together — a column-for-column
    // copy draws as parallel wires. A collapsed relation is a plain node standing for the box.
    const elements = [
      ...model.boxes.map((b) => ({
        data: {
          id: b.id,
          box: "yes",
          label: b.collapsed ? `${b.label}\n${b.rows.length + b.unused} cols` : b.label,
          relation: b.relation ?? "",
          collapsed: b.collapsed ? "yes" : "no",
          w: b.width,
          h: b.height,
        },
      })),
      ...model.boxes.flatMap((b) =>
        b.collapsed
          ? []
          : b.rows.map((r) => ({
              data: {
                id: r.id,
                parent: b.id,
                row: "yes",
                label: r.label,
                kind: r.kind,
                // In lanes the position says what a column is, so every row is drawn alike: no
                // role colour, no output ring (REQ-1667).
                role: laned ? "" : r.role,
                materialized: !laned && r.materialized ? "yes" : "no",
                cycle: laned ? "no" : r.cycle,
                w: rowWidth(b),
              },
              grabbable: false,
            })),
      ),
      ...model.boxes
        .filter((b) => !b.collapsed && b.unused > 0)
        .map((b) => ({
          data: {
            id: `${b.id}:unused`,
            parent: b.id,
            footer: "yes",
            label: `+${b.unused} unused`,
            w: rowWidth(b),
          },
          grabbable: false,
        })),
      ...model.edges.map((e) => ({
        data: {
          id: e.id,
          source: e.source,
          target: e.target,
          label: e.count > 1 ? `${e.count} columns` : e.label,
          command: e.command ? "yes" : "no",
        },
      })),
    ];

    const boxById = new Map(model.boxes.map((b) => [b.id, b]));
    const cy = cytoscape({
      container: containerRef.current,
      elements,
      // No initial layout: cytoscape's default grid would scatter the rows of every box the ELK
      // pass does not touch (the isolated ones) — those are stacked by hand below.
      layout: { name: "preset" },
      style: [
        {
          // A relation box: the compound around its rows, named above its top edge.
          selector: 'node[box = "yes"]',
          style: {
            shape: "round-rectangle",
            "corner-radius": "7px",
            "background-color": "#ffffff",
            "background-opacity": 1,
            "border-width": 1,
            "border-color": "#adb5bd",
            padding: 1,
            label: "data(label)",
            "text-valign": "top",
            "text-halign": "center",
            "text-margin-y": -3,
            "font-size": 11,
            "font-weight": "bold",
            "font-family": "monospace",
            color: "#343a40",
            "text-wrap": "none",
            // The name sits outside the box, so it carries its own tab — readable on either theme —
            // and the tab is part of the node's hit area: grab the title to move the table.
            "text-events": "yes",
            "text-background-color": "#e9ecef",
            "text-background-opacity": 1,
            "text-background-padding": "3px",
            "text-background-shape": "round-rectangle",
          },
        },
        {
          selector: 'node[collapsed = "yes"]',
          style: {
            width: "data(w)",
            height: "data(h)",
            "background-color": "#edf2ff",
            "border-color": "#3b5bdb",
            "border-width": 2,
            "text-valign": "center",
            "text-margin-y": 0,
            "text-wrap": "wrap",
            "text-max-width": "data(w)",
          },
        },
        {
          // A column row: a full-width strip inside its box, coloured by role in the flow.
          selector: 'node[row = "yes"]',
          style: {
            shape: "rectangle",
            width: "data(w)",
            height: ROW_H - 1,
            "background-color": (el: NodeSingular) =>
              ROLE_STYLE[el.data("role") as Role]?.fill ?? "#ffffff",
            "border-width": 0,
            label: "data(label)",
            "text-valign": "center",
            "text-halign": "center",
            "font-size": 10,
            "font-family": "monospace",
            color: (el: NodeSingular) => ROLE_STYLE[el.data("role") as Role]?.color ?? "#495057",
            "font-style": (el: NodeSingular) =>
              ROLE_STYLE[el.data("role") as Role]?.fontStyle ?? "normal",
            "font-weight": (el: NodeSingular) =>
              ROLE_STYLE[el.data("role") as Role]?.fontWeight ?? "normal",
            "text-max-width": "data(w)",
            "text-wrap": "ellipsis",
          },
        },
        {
          // The one line between two fields: a 1px strip laid over the seam once rows are placed.
          selector: 'node[seam = "yes"]',
          style: {
            shape: "rectangle",
            width: "data(w)",
            height: 1,
            "background-color": "#ced4da",
            "background-opacity": 1,
            "border-width": 0,
            events: "no",
          },
        },
        {
          // Only the outermost corners are rounded: a row's shape is square where it meets its
          // neighbour, so the seam between fields stays one straight line.
          selector: 'node[row = "yes"][first = "yes"], node[row = "yes"][last = "yes"]',
          style: { shape: "round-rectangle", "corner-radius": "6px" },
        },
        {
          selector: 'node[footer = "yes"]',
          style: {
            shape: "round-rectangle",
            "corner-radius": "6px",
            width: "data(w)",
            height: FOOTER_H - 1,
            "background-color": "#ffffff",
            "background-opacity": 1,
            "border-width": 0,
            label: "data(label)",
            "text-valign": "center",
            "text-halign": "center",
            "font-size": 9,
            "font-style": "italic",
            color: "#868e96",
            events: "no",
            "text-events": "no",
          },
        },
        // A field's state is its fill, never a border: rows share their seams, so a ring around one
        // of them would break the grid and round itself at the shared edge.
        {
          selector: 'node[materialized = "yes"]',
          style: { "background-color": "#e9ecef" },
        },
        { selector: 'node[cycle = "error"]', style: { "background-color": "#ffc9c9" } },
        { selector: 'node[cycle = "feedback"]', style: { "background-color": "#ffec99" } },
        {
          selector: 'node[laneTitle = "yes"]',
          style: {
            width: 1,
            height: 1,
            "background-opacity": 0,
            "border-width": 0,
            label: "data(label)",
            "text-valign": "center",
            "text-halign": "center",
            "font-size": 12,
            "font-weight": "bold",
            color: "#868e96",
            events: "no",
          },
        },
        {
          selector: 'node[laneAnchor = "yes"]',
          style: { width: 1, height: 1, "background-opacity": 0, "border-width": 0, events: "no" },
        },
        {
          // The lane rule: one pixel, dotted, at half strength.
          selector: 'edge[laneRule = "yes"]',
          style: {
            width: 1,
            "curve-style": "straight",
            "line-style": "dotted",
            "line-color": "#868e96",
            "line-opacity": 0.5,
            "target-arrow-shape": "none",
            events: "no",
          },
        },
        {
          // Row-to-row edges leave the right side of a row and enter the left side of the next,
          // routed orthogonally so a column reads across the boxes like a wire.
          selector: 'edge[laneRule != "yes"]',
          style: {
            width: 1.5,
            "line-color": "#adb5bd",
            "target-arrow-color": "#adb5bd",
            "target-arrow-shape": "triangle",
            "arrow-scale": 0.8,
            "curve-style": "round-taxi",
            "taxi-direction": "rightward",
            "taxi-turn": 40,
            "taxi-turn-min-distance": 12,
            "taxi-radius": 14,
            "source-endpoint": "90deg",
            "target-endpoint": "270deg",
            // The transform names what the OUTPUT column is made of, so it sits by the target end.
            "target-label": "data(label)",
            "target-text-offset": 55,
            "font-size": 8,
            color: "#868e96",
            "text-background-color": "#fff",
            "text-background-opacity": 0.85,
            "text-background-padding": "1px",
          },
        },
        {
          selector: 'edge[command = "yes"]',
          style: {
            "line-color": "#9c36b5",
            "target-arrow-color": "#9c36b5",
            width: 2,
            "line-style": "dashed",
          },
        },
      ],
      wheelSensitivity: 0.2,
    }) as CyCore;

    if (describeColumn) {
      cy.on("mouseover", 'node[row = "yes"]', (evt) => {
        const target = evt.target as NodeSingular;
        const relation = target.parent().data("relation") as string;
        const text = relation ? describeColumn(relation, target.data("label") as string) : null;
        if (!text) return;
        const at = evt.renderedPosition ?? { x: 0, y: 0 };
        setHint({ x: at.x, y: at.y, text, columns: [] });
      });
      cy.on("mouseout", 'node[row = "yes"]', () => setHint(null));
    }
    if (onToggleRelation) {
      cy.on("mouseover", 'node[box = "yes"]', (evt) => {
        const target = evt.target as NodeSingular;
        if (!target.data("relation")) return;
        const at = evt.renderedPosition ?? { x: 0, y: 0 };
        const relation = target.data("relation") as string;
        const collapsed = target.data("collapsed") === "yes";
        setHint({
          x: at.x,
          y: at.y,
          text: collapsed ? "Click to expand" : "Click to collapse",
          // A collapsed box previews every column of its relation; an expanded one shows them.
          columns: collapsed
            ? graph.nodes.filter((n) => n.relation === relation).map((n) => n.column)
            : [],
        });
      });
      cy.on("mouseout", 'node[box = "yes"]', () => setHint(null));
      cy.on("tap", 'node[box = "yes"]', () => setHint(null));
    }
    cy.on("viewport", () => setHint(null));

    // A tap on a box toggles the relation; a tap on a row drives federation focus.
    cy.on("tap", "node", (evt) => {
      const target = evt.target as NodeSingular;
      if (target.data("box") === "yes") {
        const relation = target.data("relation") as string;
        if (relation) toggleRef.current?.(relation);
        return;
      }
      if (target.data("row") === "yes") clickRef.current?.(target.id());
    });

    // Layered left to right: the read order of lineage. Inside a box the rows are one layer,
    // packed with SIMPLE placement so they stay a table rather than fanning out to meet their
    // wires; the sweep descends into the hierarchy so their order is what minimizes crossings.
    //
    // Only the CONNECTED boxes go through ELK. Its connected-component packing does not apply
    // inside a hierarchy, so a federation full of unrelated tables would come out as one tall
    // column; the isolated boxes are shelf-packed into a grid under the laid-out part instead —
    // the same split the ERD makes between its connected tables and its orphan grid.
    const boxNodes = cy.nodes('[box = "yes"]');
    const connectedIds = new Set<string>();
    cy.edges().forEach((e) => {
      for (const end of [e.source(), e.target()]) {
        const box = end.data("box") === "yes" ? end : end.parent();
        if (box.nonempty()) connectedIds.add(box.id());
      }
    });
    // With lanes every box has a place — an unconnected member still sits in lane 0.
    const connected = laned ? boxNodes : boxNodes.filter((n) => connectedIds.has(n.id()));
    const isolated = laned ? cy.collection() : boxNodes.filter((n) => !connectedIds.has(n.id()));
    const laneOf = (n: NodeSingular): number => boxById.get(n.id())?.lane ?? 0;
    const minLane = laned ? Math.min(0, ...model.boxes.map((b) => b.lane ?? 0)) : 0;

    // Dotted rules between adjacent lanes and a title over each, as inert nodes: they pan, zoom
    // and export with the graph. Drawn once the boxes are placed, from their extents per lane.
    const drawLanes = () => {
      if (!laned) return;
      const extents = new Map<number, { x1: number; x2: number }>();
      let y1 = Infinity;
      let y2 = -Infinity;
      boxNodes.forEach((n) => {
        const bb = n.union(n.descendants()).boundingBox();
        const l = laneOf(n);
        const ex = extents.get(l) ?? { x1: Infinity, x2: -Infinity };
        extents.set(l, { x1: Math.min(ex.x1, bb.x1), x2: Math.max(ex.x2, bb.x2) });
        y1 = Math.min(y1, bb.y1);
        y2 = Math.max(y2, bb.y2);
      });
      if (extents.size === 0) return;
      const ordered = [...extents.entries()].sort((a, b) => a[0] - b[0]);
      const top = y1 - 44;
      const bottom = y2 + 16;
      cy.add(
        ordered.map(([l, ex]) => ({
          data: { id: `lane-title-${l}`, laneTitle: "yes", label: lanes.title(l) },
          position: { x: (ex.x1 + ex.x2) / 2, y: top },
          selectable: false,
          grabbable: false,
        })),
      );
      // Each rule is a 1px dotted EDGE between two invisible anchors — a node's border would be
      // drawn on both sides of its width and come out 2-3px.
      ordered.slice(1).forEach(([l, ex], i) => {
        const x = (ordered[i][1].x2 + ex.x1) / 2;
        cy.add([
          {
            data: { id: `lane-rule-${l}-a`, laneAnchor: "yes" },
            position: { x, y: top },
            selectable: false,
            grabbable: false,
          },
          {
            data: { id: `lane-rule-${l}-b`, laneAnchor: "yes" },
            position: { x, y: bottom },
            selectable: false,
            grabbable: false,
          },
          {
            data: {
              id: `lane-rule-${l}`,
              laneRule: "yes",
              source: `lane-rule-${l}-a`,
              target: `lane-rule-${l}-b`,
            },
            selectable: false,
          },
        ]);
      });
    };

    const packIsolated = () => {
      const laid = connected.union(connected.descendants());
      const anchor = laid.nonempty() ? laid.boundingBox() : { x1: 0, y1: 0, x2: 0, y2: 0, w: 0 };
      // Stack each isolated box's rows in model order first; the compound sizes itself around them.
      isolated.forEach((box) => {
        const model = boxById.get(box.id());
        if (!model || model.collapsed) return;
        model.rows.forEach((r, i) => cy.getElementById(r.id).position({ x: 0, y: i * ROW_H }));
        cy.getElementById(`${box.id()}:unused`).position({ x: 0, y: model.rows.length * ROW_H });
      });
      // Shelves wide enough that the grid takes the container's shape rather than a tall strip.
      const el = containerRef.current;
      const aspect = el && el.clientHeight > 0 ? el.clientWidth / el.clientHeight : 1.6;
      const area = isolated.reduce((sum, box) => {
        const bb = box.union(box.descendants()).boundingBox();
        return sum + (bb.w + 28) * (bb.h + 46);
      }, 0);
      const shelfWidth = Math.max(anchor.w, Math.sqrt(area * aspect));
      const gap = 28;
      let x = anchor.x1;
      let y = anchor.y2 + (laid.nonempty() ? 70 : 0);
      let shelfHeight = 0;
      cy.batch(() => {
        isolated.forEach((box) => {
          const eles = box.union(box.descendants());
          const bb = eles.boundingBox();
          if (x > anchor.x1 && x + bb.w > anchor.x1 + shelfWidth) {
            x = anchor.x1;
            y += shelfHeight + gap + 18; // 18 clears the header tab above the next shelf
            shelfHeight = 0;
          }
          // Shift the whole compound: a compound's own position is derived from its children.
          const movable = box.isParent() ? box.descendants() : box;
          movable.shift({ x: x - bb.x1, y: y - bb.y1 });
          x += bb.w + gap;
          shelfHeight = Math.max(shelfHeight, bb.h);
        });
      });
    };

    // Once rows are placed (ELK may have reordered them), the top and bottom rows take the box's
    // rounded corners and a 1px seam strip is laid between each neighbouring pair.
    const dressBoxes = () => {
      cy.batch(() => {
        boxNodes.forEach((box) => {
          if (!box.isParent()) return;
          const rows = box
            .children('[row = "yes"], [footer = "yes"]')
            .sort((a, b) => a.position("y") - b.position("y"));
          rows.forEach((r, i) => {
            r.data("first", i === 0 ? "yes" : "no");
            r.data("last", i === rows.length - 1 ? "yes" : "no");
          });
          for (let i = 1; i < rows.length; i += 1) {
            const above = rows[i - 1];
            const below = rows[i];
            cy.add({
              data: {
                id: `${box.id()}:seam:${i}`,
                seam: "yes",
                parent: box.id(),
                w: below.data("w"),
              },
              position: {
                x: below.position("x"),
                y:
                  (above.position("y") +
                    above.height() / 2 +
                    below.position("y") -
                    below.height() / 2) /
                  2,
              },
              selectable: false,
              grabbable: false,
            });
          }
        });
      });
    };

    const finish = () => {
      packIsolated();
      dressBoxes();
      drawLanes();
      cy.fit(undefined, 30);
    };
    if (connected.nonempty()) {
      const layout = cy.layout({
        name: "elk",
        eles: connected.union(connected.descendants()).union(cy.edges()),
        fit: false,
        nodeLayoutOptions: (n: NodeSingular) => {
          // A box's lane is its ELK partition: partitions are placed strictly left to right.
          const partition =
            laned && n.data("box") === "yes"
              ? { "elk.partitioning.partition": String(laneOf(n) - minLane) }
              : {};
          return n.isParent()
            ? {
                ...partition,
                "elk.padding": "[top=1,left=1,bottom=1,right=1]",
                "elk.spacing.nodeNode": "1",
                "elk.layered.spacing.nodeNodeBetweenLayers": "24",
                "elk.layered.nodePlacement.strategy": "SIMPLE",
              }
            : partition;
        },
        elk: {
          algorithm: "layered",
          "elk.direction": "RIGHT",
          "elk.partitioning.activate": laned ? "true" : "false",
          "elk.aspectRatio": "2.0",
          "elk.hierarchyHandling": "INCLUDE_CHILDREN",
          "elk.layered.spacing.nodeNodeBetweenLayers": "110",
          "elk.spacing.nodeNode": "44",
          "elk.layered.nodePlacement.strategy": "NETWORK_SIMPLEX",
          "elk.layered.crossingMinimization.strategy": "LAYER_SWEEP",
          "elk.layered.crossingMinimization.hierarchicalSweepiness": "1",
          "elk.layered.considerModelOrder.strategy": "NODES_AND_EDGES",
        },
      } as CyLayoutOptions);
      layout.on("layoutstop", finish);
      layout.run();
    } else {
      finish();
    }

    cyRef.current = cy;
    // The panel now grows with the window (height="100%"), and cytoscape reads its canvas size once
    // at init — without this the graph keeps the size it was born with and clips or floats.
    //
    // A DAG born inside a closed section (the product page's lineage Collapse keeps it mounted at
    // zero height) laid out against a canvas of no size, so the first time the container has an
    // area at all the graph is fitted to it — the same fit the toolbar button performs. Later
    // resizes only resize, so a zoom or pan the reader chose is not thrown away by a window drag.
    let hadArea = false;
    const observer = new ResizeObserver(() => {
      cy.resize();
      const el = containerRef.current;
      const hasArea = !!el && el.clientWidth > 0 && el.clientHeight > 0;
      if (hasArea && !hadArea) cy.fit(undefined, 30);
      hadArea = hasArea;
    });
    observer.observe(containerRef.current);
    return () => {
      observer.disconnect();
      cy.destroy();
      cyRef.current = null;
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps -- onToggleRelation is read through toggleRef; only its presence decides whether hints are wired
  }, [graph, collapsedRelations, resultLabel, allColumns, lanes, describeColumn]);

  return (
    <div
      style={{ position: "relative", width: "100%", height }}
      onMouseEnter={() => setHovered(true)}
      onMouseLeave={() => setHovered(false)}
    >
      <div ref={containerRef} style={{ width: "100%", height }} data-testid="lineage-dag" />
      {legend && (
        <div
          style={{ position: "absolute", insetInlineStart: 8, bottom: 8, pointerEvents: "none" }}
        >
          <LineageLegend />
        </div>
      )}
      {stats && (
        <Text
          size="xs"
          c="dimmed"
          data-testid="lineage-stats"
          style={{ position: "absolute", insetInlineEnd: 8, bottom: 8, pointerEvents: "none" }}
        >
          {graph.nodes.length} columns · {graph.edges.length} edges
        </Text>
      )}
      {hint && (
        <Tooltip
          label={
            <div data-testid="lineage-box-hint">
              <div style={{ fontWeight: 600 }}>{hint.text}</div>
              {hint.columns.map((c) => (
                <div key={c} style={{ fontFamily: "var(--mantine-font-family-monospace)" }}>
                  {c}
                </div>
              ))}
            </div>
          }
          opened
          withArrow
          position="top"
        >
          <div
            style={{
              position: "absolute",
              left: hint.x,
              top: hint.y - 8,
              width: 1,
              height: 1,
              pointerEvents: "none",
            }}
          />
        </Tooltip>
      )}
      <div
        style={{
          position: "absolute",
          top: 8,
          insetInlineEnd: 8,
          display: "flex",
          gap: 4,
          opacity: hovered ? 1 : 0,
          transition: "opacity 150ms ease",
          pointerEvents: hovered ? "auto" : "none",
        }}
      >
        {onExpandAll && (
          <Tooltip label="Expand all datasets">
            <ActionIcon
              variant="default"
              onClick={onExpandAll}
              aria-label="Expand all datasets"
              data-testid="lineage-expand-all"
            >
              <ChevronsUpDown size={16} />
            </ActionIcon>
          </Tooltip>
        )}
        {onCollapseAll && (
          <Tooltip label="Collapse all datasets">
            <ActionIcon
              variant="default"
              onClick={onCollapseAll}
              aria-label="Collapse all datasets"
              data-testid="lineage-collapse-all"
            >
              <ChevronsDownUp size={16} />
            </ActionIcon>
          </Tooltip>
        )}
        {onOpenModal && (
          <Tooltip label="Open full view">
            <ActionIcon
              variant="default"
              onClick={onOpenModal}
              aria-label="Open full view"
              data-testid="lineage-open-modal"
            >
              <Expand size={16} />
            </ActionIcon>
          </Tooltip>
        )}
        <Tooltip label="Fit to screen">
          <ActionIcon
            variant="default"
            onClick={fitToScreen}
            aria-label="Fit to screen"
            data-testid="lineage-fit"
          >
            <FitScreenIcon size={16} />
          </ActionIcon>
        </Tooltip>
        <Tooltip label="Download PNG">
          <ActionIcon
            variant="default"
            onClick={downloadPng}
            aria-label="Download PNG"
            data-testid="lineage-download"
          >
            <Download size={16} />
          </ActionIcon>
        </Tooltip>
      </div>
    </div>
  );
}
