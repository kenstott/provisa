// Copyright (c) 2026 Kenneth Stott
// Canary: 8c2e5f19-4a7d-4b63-9f0e-3d6a1b8c7e54
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2128: the Data Product detail panel's Knowledge Graph panel — the product, its member tables,
// its commands and the terms bound to its columns, with no layer controls. three.js loads when the
// panel scrolls into view; its expand button opens the full graph on the Knowledge graph preset,
// focused on the product. The Data Products page's own entry point opens it on the Product map.

import { useMemo, useState, type ComponentProps } from "react";
import { useTranslation } from "react-i18next";
import { Alert, Button, Group, Loader, Modal } from "@mantine/core";
import { Network } from "lucide-react";
import { ExpandButton, FitModal } from "../../pages/tables/profile3d/Expandable3D";
import { GraphExplorer } from "./GraphExplorer";
import type { GraphKey } from "../../pages/tables/profile3d/graphSettings";
import { GraphStage, type StageState } from "./GraphStage";
import {
  PRODUCT_SCOPE,
  layoutGraph,
  panelGraph,
  type GraphData,
  type GraphScope,
  type PresetId,
} from "./graphModel";
import type { GraphHover } from "./graphScene";
import { useInView } from "./useInView";
import { productNode } from "./productGraphData";
import { useProductGraph } from "./useProductGraph";
import "../../pages/tables/profile3d/profile3d.css";
import "./graph3d.css";

export function GraphModal({
  opened,
  onClose,
  title,
  data,
  scope,
  preset,
  focus,
  zero,
  search,
  settingsKey,
  exportName,
  query,
}: {
  opened: boolean;
  onClose: () => void;
  title: string;
  data: GraphData;
  scope: GraphScope;
  preset: PresetId;
  focus: string | null;
  zero: string[];
  /** The page's search driving the focus (the Ontology View's), as GraphExplorer takes it. */
  search?: string;
  /** Which graph's remembered settings (graphSettings.ts): the same as its in-place view's. */
  settingsKey: GraphKey;
  /** What the graph's exports are named after. */
  exportName: string;
  /** The view's query scope, as GraphExplorer takes it (the Ontology View's). */
  query?: ComponentProps<typeof GraphExplorer>["query"];
}) {
  return (
    <FitModal opened={opened} onClose={onClose} title={title} testId="graph3d-modal">
      {opened && (
        <GraphExplorer
          data={data}
          scope={scope}
          preset={preset}
          focus={focus}
          zero={zero}
          search={search}
          settingsKey={settingsKey}
          exportName={exportName}
          query={query}
          testId="graph3d"
        />
      )}
    </FitModal>
  );
}

function PanelStage({
  graph,
  product,
  productName,
  covered,
}: {
  graph: GraphData;
  product: string;
  productName: string;
  /** Its expanded view is open over it: it holds still, drawing nothing it would not show. */
  covered: boolean;
}) {
  const at = useMemo(() => layoutGraph(graph, "force", []), [graph]);
  const [picked, setSelected] = useState<string | null>(product);
  const [hover, setHover] = useState<GraphHover | null>(null);
  // The card is a glance: it turns and draws no grid, whatever a viewer's saved graph defaults
  // say; a drag stops the turning, as it does anywhere. Its settings live in its expanded view.
  const [rotate, setRotate] = useState(true);
  const grid = false;
  const s: StageState = {
    graph,
    at,
    frame: 0,
    frontal: false,
    selected: picked,
    setSelected,
    hover,
    setHover,
    rotate: rotate && !covered,
    setRotate,
    grid,
  };
  return (
    <>
      <GraphStage s={s} command={null} exportName={productName} testId="kg-panel-stage" bare />
    </>
  );
}

/** The product's member tables: lane 0 of the Lineage preset. */
function membersOf(data: GraphData, product: string): string[] {
  return data.edges.filter((e) => e.kind === "member" && e.a === product).map((e) => e.b);
}

function Loading({ error, testId }: { error: string | null; testId: string }) {
  const { t } = useTranslation();
  return error ? (
    <Alert color="red" data-testid={`${testId}-error`}>
      {t("graph3d.loadError", { error })}
    </Alert>
  ) : (
    <Loader size="sm" data-testid={`${testId}-loading`} />
  );
}

// Before a view's first scoped read: nothing drawn yet.
const EMPTY: GraphData = { nodes: [], edges: [] };

export function KnowledgeGraphPanel({
  productId,
  productName,
  graph: core,
}: {
  productId: string;
  productName: string;
  /** The product's graph as the panel read it from the meta domain (productCore.coreGraph): the
   *  card is drawn from it, with no read of its own. */
  graph: GraphData;
}) {
  const { t } = useTranslation();
  const { ref, seen } = useInView<HTMLDivElement>();
  const [open, setOpen] = useState(false);
  // The expanded explorer reads its own query (its filters and presets need the whole model),
  // once it is opened.
  const { data, error, scoped, onScope } = useProductGraph(productId, open);
  const product = productNode(productId);
  const graph = useMemo(() => panelGraph(core, product), [core, product]);
  return (
    <div className="g3d-panel" ref={ref} data-testid="kg-panel">
      {/* Its card names it; the expand button opens the full graph. */}
      <Group justify="flex-end" gap="xs">
        <ExpandButton onClick={() => setOpen(true)} testId="kg-panel-expand" />
      </Group>
      <div className="p3d g3d-panel-view">
        {seen && (
          <PanelStage graph={graph} product={product} productName={productName} covered={open} />
        )}
      </div>
      {open && !data && (
        <Modal opened onClose={() => setOpen(false)} title={productName} size="sm">
          <Loading error={error} testId="kg-panel" />
        </Modal>
      )}
      {data && (
        <GraphModal
          opened={open}
          onClose={() => setOpen(false)}
          title={t("graph3d.expandTitle", { product: productName })}
          data={scoped?.data ?? EMPTY}
          query={{ all: data, onScope, text: scoped?.query ?? null, own: product }}
          scope={PRODUCT_SCOPE}
          preset="knowledge"
          exportName={productName}
          focus={product}
          zero={membersOf(data, product)}
          settingsKey={{ kind: "product-graph", subject: productId }}
        />
      )}
    </div>
  );
}

/** The Data Products page's entry point: the full graph on the Product map preset, every product
 * read when it is opened. */
export function ProductMapButton() {
  const { t } = useTranslation();
  const [open, setOpen] = useState(false);
  const { data, error, scoped, onScope } = useProductGraph(null, open);
  return (
    <>
      <Button
        size="xs"
        variant="default"
        leftSection={<Network size={14} />}
        onClick={() => setOpen(true)}
        data-testid="product-map-open"
      >
        {t("graph3d.productMap")}
      </Button>
      {open && !data && (
        <Modal opened onClose={() => setOpen(false)} title={t("graph3d.productMap")} size="sm">
          <Loading error={error} testId="product-map" />
        </Modal>
      )}
      {data && (
        <GraphModal
          opened={open}
          onClose={() => setOpen(false)}
          title={t("graph3d.productMap")}
          data={scoped?.data ?? EMPTY}
          query={{ all: data, onScope, text: scoped?.query ?? null }}
          scope={PRODUCT_SCOPE}
          preset="productMap"
          exportName="product-map"
          focus={null}
          zero={[]}
          settingsKey={{ kind: "product-map", subject: null }}
        />
      )}
    </>
  );
}
