// Copyright (c) 2026 Kenneth Stott
// Canary: b639e1ef-95dc-4fbd-8991-2c2dda4e9798
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2150: a term's neighbourhood drawn as a layered graph, left to right (term-graph.ts), laid
// out by ELK as the lineage DAG is. Compact in the term's panel; the expand control opens it at
// full size. A click on a term opens that term.

import { useEffect, useMemo, useRef, useState } from "react";
import { useTranslation } from "react-i18next";
import cytoscape, { type NodeSingular } from "cytoscape";
import elkRaw from "cytoscape-elk";
import { ActionIcon, Modal } from "@mantine/core";
import { Expand } from "lucide-react";
import type { GlossaryTermDetail, GlossaryTermTable } from "../../api/glossary";
import type { CyLayoutOptions } from "../graph/cytoscape-types";
import { FitScreenIcon } from "../graph/GraphIcons";
import { Tip } from "../Tip";
import { termGraph } from "./term-graph";

type CyExt = Parameters<typeof cytoscape.use>[0];
try {
  cytoscape.use((elkRaw as { default?: CyExt }).default ?? (elkRaw as CyExt));
} catch {
  /* already registered */
}

type CyCore = ReturnType<typeof cytoscape> & { resize(): void };

const FIT_PAD = 16;
const COMPACT_HEIGHT = 220;
// A node's text (11px) is legible at zoom 1; the full view enlarges a small graph no further.
const MAX_ZOOM = 1.4;
const NODE_H = 26;
// A node is as wide as its name: ELK lays it out by this size, before cytoscape draws any text.
const nodeWidth = (label: string) => Math.max(48, Math.ceil(label.length * 6.6) + 18);

// The canvas is white in either colour scheme, as the lineage DAG's is: its controls are drawn in
// the canvas's own text colour, not the page's.
const ON_CANVAS = "#495057";

const KIND_STYLE: Record<string, { fill: string; border: string; shape: string }> = {
  self: { fill: "#e7f5ff", border: "#1c7ed6", shape: "round-rectangle" },
  term: { fill: "#ffffff", border: "#868e96", shape: "round-rectangle" },
  column: { fill: "#f8f9fa", border: "#adb5bd", shape: "rectangle" },
  table: { fill: "#fff9db", border: "#f59f00", shape: "rectangle" },
};

function TermGraphCanvas({
  term,
  tables,
  relLabel,
  navigateToTerm,
  height,
  testId,
}: {
  term: GlossaryTermDetail;
  tables: GlossaryTermTable[] | null;
  relLabel: (relType: string) => string;
  navigateToTerm: (termId: number) => void;
  height: number | string;
  testId: string;
}) {
  const { t } = useTranslation();
  const containerRef = useRef<HTMLDivElement | null>(null);
  const cyRef = useRef<CyCore | null>(null);
  const graph = useMemo(() => termGraph(term, tables, relLabel), [term, tables, relLabel]);
  // The cytoscape effect binds this once; the ref holds the latest callback.
  const navigateRef = useRef(navigateToTerm);
  useEffect(() => {
    navigateRef.current = navigateToTerm;
  }, [navigateToTerm]);

  useEffect(() => {
    if (!containerRef.current) return;
    const cy = cytoscape({
      container: containerRef.current,
      elements: [
        ...graph.nodes.map((n) => ({
          group: "nodes" as const,
          data: {
            id: n.id,
            label: n.label,
            kind: n.kind,
            layer: n.layer,
            termId: n.termId,
            w: nodeWidth(n.label),
          },
        })),
        ...graph.edges.map((e) => ({
          group: "edges" as const,
          data: { id: e.id, source: e.source, target: e.target, label: e.label ?? "" },
        })),
      ],
      layout: { name: "preset" },
      style: [
        {
          selector: "node",
          style: {
            label: "data(label)",
            "font-size": "11px",
            "text-valign": "center",
            "text-halign": "center",
            width: "data(w)",
            height: NODE_H,
            "border-width": 1,
          },
        },
        ...Object.entries(KIND_STYLE).map(([kind, s]) => ({
          selector: `node[kind = "${kind}"]`,
          style: {
            "background-color": s.fill,
            "border-color": s.border,
            shape: s.shape,
            ...(kind === "self" ? { "font-weight": "bold", "border-width": 2 } : {}),
          },
        })),
        { selector: 'node[kind = "term"]', style: { cursor: "pointer" } },
        {
          selector: "edge",
          style: {
            width: 1,
            "line-color": "#adb5bd",
            "target-arrow-color": "#adb5bd",
            "target-arrow-shape": "triangle",
            "curve-style": "bezier",
            label: "data(label)",
            "font-size": "9px",
            color: "#495057",
            "text-background-color": "#ffffff",
            "text-background-opacity": 1,
            "text-background-padding": "2px",
          },
        },
      ],
      maxZoom: MAX_ZOOM,
      userZoomingEnabled: true,
      boxSelectionEnabled: false,
      autoungrabify: true,
    } as unknown as Parameters<typeof cytoscape>[0]) as CyCore;
    cyRef.current = cy;
    cy.on("tap", 'node[kind = "term"]', (evt: { target: NodeSingular }) => {
      const termId = evt.target.data("termId") as number | undefined;
      if (termId !== undefined) navigateRef.current(termId);
    });
    const layout = cy.layout({
      name: "elk",
      fit: true,
      padding: FIT_PAD,
      // Each node's layer is its ELK partition: partitions are placed strictly left to right.
      nodeLayoutOptions: (n: NodeSingular) => ({
        "elk.partitioning.partition": String(n.data("layer")),
      }),
      elk: {
        algorithm: "layered",
        "elk.direction": "RIGHT",
        "elk.partitioning.activate": "true",
        "elk.layered.spacing.nodeNodeBetweenLayers": "70",
        "elk.spacing.nodeNode": "14",
        "elk.layered.nodePlacement.strategy": "NETWORK_SIMPLEX",
      },
    } as CyLayoutOptions);
    layout.run();
    const observer = new ResizeObserver(() => {
      cy.resize();
      cy.fit(undefined, FIT_PAD);
    });
    observer.observe(containerRef.current);
    return () => {
      observer.disconnect();
      cy.destroy();
      cyRef.current = null;
    };
  }, [graph]);

  return (
    <div style={{ position: "relative", height }} data-testid={testId}>
      <div
        ref={containerRef}
        style={{
          width: "100%",
          height: "100%",
          border: "1px solid var(--mantine-color-default-border)",
          borderRadius: "var(--mantine-radius-sm)",
          background: "#ffffff",
        }}
      />
      <div style={{ position: "absolute", top: 4, right: 4 }}>
        <Tip text={t("canvasControls.fitToScreen")} named>
          <ActionIcon
            variant="subtle"
            c={ON_CANVAS}
            size="sm"
            onClick={() => cyRef.current?.fit(undefined, FIT_PAD)}
            aria-label={t("canvasControls.fitToScreen")}
            data-testid={`${testId}-fit`}
          >
            <FitScreenIcon size={14} />
          </ActionIcon>
        </Tip>
      </div>
    </div>
  );
}

export function TermDag({
  term,
  tables,
  relLabel,
  navigateToTerm,
}: {
  term: GlossaryTermDetail;
  tables: GlossaryTermTable[] | null;
  relLabel: (relType: string) => string;
  navigateToTerm: (termId: number) => void;
}) {
  const { t } = useTranslation();
  const [full, setFull] = useState(false);
  // Opening a term from the full view shows that term's graph in the panel.
  const openTerm = (termId: number) => {
    setFull(false);
    navigateToTerm(termId);
  };
  return (
    <div style={{ position: "relative" }}>
      <TermGraphCanvas
        term={term}
        tables={tables}
        relLabel={relLabel}
        navigateToTerm={navigateToTerm}
        height={COMPACT_HEIGHT}
        testId="glossary-term-dag"
      />
      <div style={{ position: "absolute", top: 4, right: 32 }}>
        <Tip text={t("graphFrame.expand")} named>
          <ActionIcon
            variant="subtle"
            c={ON_CANVAS}
            size="sm"
            onClick={() => setFull(true)}
            aria-label={t("graphFrame.expand")}
            data-testid="glossary-term-dag-expand"
          >
            <Expand size={14} />
          </ActionIcon>
        </Tip>
      </div>
      <Modal
        opened={full}
        onClose={() => setFull(false)}
        size="90%"
        title={term.name}
        data-testid="glossary-term-dag-modal"
      >
        {full && (
          <TermGraphCanvas
            term={term}
            tables={tables}
            relLabel={relLabel}
            navigateToTerm={openTerm}
            height="70vh"
            testId="glossary-term-dag-full"
          />
        )}
      </Modal>
    </div>
  );
}
