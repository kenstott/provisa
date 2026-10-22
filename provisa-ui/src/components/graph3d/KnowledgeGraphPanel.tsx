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

import { useMemo, useState } from "react";
import { useTranslation } from "react-i18next";
import { Alert, Button, Chip, Group, Loader, Modal, Text } from "@mantine/core";
import { Network } from "lucide-react";
import { ExpandButton, FitModal } from "../../pages/tables/profile3d/Expandable3D";
import type { CameraCommand } from "../../pages/tables/profile3d/Stage3D";
import { GraphExplorer } from "./GraphExplorer";
import { GridToggle } from "../../pages/tables/profile3d/GridToggle";
import { ResetSettingsButton } from "../../pages/tables/profile3d/ResetSettingsButton";
import { useGraphSettings, type GraphKey } from "../../pages/tables/profile3d/graphSettings";
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
          testId="graph3d"
        />
      )}
    </FitModal>
  );
}

function PanelStage({
  graph,
  product,
  productId,
  productName,
}: {
  graph: GraphData;
  product: string;
  productId: string;
  productName: string;
}) {
  const at = useMemo(() => layoutGraph(graph, "force", []), [graph]);
  const [picked, setSelected] = useState<string | null>(product);
  const [hover, setHover] = useState<GraphHover | null>(null);
  // Remembered for this product's panel (graphSettings.ts).
  const [saved, save, resetSettings] = useGraphSettings(
    { kind: "product-panel", subject: productId },
    { rotate: true as boolean, grid: true as boolean },
  );
  const rotate = saved.rotate as boolean;
  const grid = saved.grid as boolean;
  const setRotate = (on: boolean) => save("rotate", on);
  const setGrid = (on: boolean) => save("grid", on);
  const [command, setCommand] = useState<CameraCommand | null>(null);
  const { t } = useTranslation();
  const s: StageState = {
    graph,
    at,
    frame: 0,
    frontal: false,
    selected: picked,
    setSelected,
    hover,
    setHover,
    rotate,
    setRotate,
    grid,
  };
  return (
    <>
      <Group className="p3d-toolbar" gap="xs">
        <Chip size="xs" checked={rotate} onChange={setRotate} data-testid="kg-panel-rotate">
          {t("profile3d.rotate")}
        </Chip>
        <GridToggle value={grid} onChange={setGrid} testId="kg-panel" />
        <Button
          size="xs"
          variant="default"
          onClick={() =>
            setCommand((c) => ({ kind: "reset", target: null, seq: c == null ? 1 : c.seq + 1 }))
          }
          data-testid="kg-panel-reset"
        >
          {t("profile3d.resetView")}
        </Button>
        <ResetSettingsButton onReset={resetSettings} testId="kg-panel" />
      </Group>
      <GraphStage s={s} command={command} exportName={productName} testId="kg-panel-stage" />
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

export function KnowledgeGraphPanel({
  productId,
  productName,
}: {
  productId: string;
  productName: string;
}) {
  const { t } = useTranslation();
  const { ref, seen } = useInView<HTMLDivElement>();
  const [open, setOpen] = useState(false);
  // Read once the panel is first in view, as three.js is loaded.
  const { data, error } = useProductGraph(productId, seen);
  const product = productNode(productId);
  const graph = useMemo(() => (data ? panelGraph(data, product) : null), [data, product]);
  return (
    <div className="g3d-panel" ref={ref} data-testid="kg-panel">
      <Group justify="space-between" gap="xs">
        <Text c="dimmed" fw={500} size="sm">
          {t("graph3d.panelTitle")}
        </Text>
        {data && <ExpandButton onClick={() => setOpen(true)} testId="kg-panel-expand" />}
      </Group>
      <div className="p3d g3d-panel-view">
        {seen &&
          (graph ? (
            <PanelStage
              graph={graph}
              product={product}
              productId={productId}
              productName={productName}
            />
          ) : (
            <Loading error={error} testId="kg-panel" />
          ))}
      </div>
      {data && (
        <GraphModal
          opened={open}
          onClose={() => setOpen(false)}
          title={t("graph3d.expandTitle", { product: productName })}
          data={data}
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
  const { data, error } = useProductGraph(null, open);
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
          data={data}
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
