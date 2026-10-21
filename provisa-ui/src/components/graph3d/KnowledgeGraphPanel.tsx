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
import { Button, Chip, Group, Modal, Text } from "@mantine/core";
import { Network } from "lucide-react";
import { ExpandButton } from "../../pages/tables/profile3d/Expandable3D";
import type { CameraCommand } from "../../pages/tables/profile3d/Stage3D";
import { GraphExplorer } from "./GraphExplorer";
import { GraphStage, type StageState } from "./GraphStage";
import { layoutGraph, panelGraph, type GraphData, type PresetId } from "./graphModel";
import type { GraphHover } from "./graphScene";
import { useInView } from "./useInView";
import "../../pages/tables/profile3d/profile3d.css";
import "./graph3d.css";

function GraphModal({
  opened,
  onClose,
  title,
  data,
  preset,
  focus,
  zero,
}: {
  opened: boolean;
  onClose: () => void;
  title: string;
  data: GraphData;
  preset: PresetId;
  focus: string | null;
  zero: string[];
}) {
  return (
    <Modal
      opened={opened}
      onClose={onClose}
      size="95%"
      title={title}
      styles={{
        content: { height: "95vh" },
        body: { height: "calc(95vh - 3.5rem)", overflow: "hidden" },
      }}
      data-testid="graph3d-modal"
    >
      {opened && (
        <GraphExplorer data={data} preset={preset} focus={focus} zero={zero} testId="graph3d" />
      )}
    </Modal>
  );
}

function PanelStage({ graph, product }: { graph: GraphData; product: string }) {
  const at = useMemo(() => layoutGraph(graph, "force", []), [graph]);
  const [picked, setSelected] = useState<string | null>(product);
  const [hover, setHover] = useState<GraphHover | null>(null);
  const [rotate, setRotate] = useState(true);
  const [command, setCommand] = useState<CameraCommand | null>(null);
  const { t } = useTranslation();
  const s: StageState = {
    graph,
    at,
    frame: 0,
    selected: picked,
    setSelected,
    hover,
    setHover,
    rotate,
    setRotate,
  };
  return (
    <>
      <Group className="p3d-toolbar" gap="xs">
        <Chip size="xs" checked={rotate} onChange={setRotate} data-testid="kg-panel-rotate">
          {t("profile3d.rotate")}
        </Chip>
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
      </Group>
      <GraphStage s={s} command={command} testId="kg-panel-stage" />
    </>
  );
}

export function KnowledgeGraphPanel({
  data,
  product,
  productName,
  zero,
}: {
  data: GraphData;
  /** The product's node id. */
  product: string;
  productName: string;
  /** The product's member tables, lane 0 of the Lineage preset. */
  zero: string[];
}) {
  const { t } = useTranslation();
  const { ref, seen } = useInView<HTMLDivElement>();
  const [open, setOpen] = useState(false);
  const graph = useMemo(() => panelGraph(data, product), [data, product]);
  return (
    <div className="g3d-panel" ref={ref} data-testid="kg-panel">
      <Group justify="space-between" gap="xs">
        <Text c="dimmed" fw={500} size="sm">
          {t("graph3d.panelTitle")}
        </Text>
        <ExpandButton onClick={() => setOpen(true)} testId="kg-panel-expand" />
      </Group>
      <div className="p3d g3d-panel-view">
        {seen && <PanelStage graph={graph} product={product} />}
      </div>
      <GraphModal
        opened={open}
        onClose={() => setOpen(false)}
        title={t("graph3d.expandTitle", { product: productName })}
        data={data}
        preset="knowledge"
        focus={product}
        zero={zero}
      />
    </div>
  );
}

/** The Data Products page's entry point: the full graph on the Product map preset. */
export function ProductMapButton({ data }: { data: GraphData }) {
  const { t } = useTranslation();
  const [open, setOpen] = useState(false);
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
      <GraphModal
        opened={open}
        onClose={() => setOpen(false)}
        title={t("graph3d.productMap")}
        data={data}
        preset="productMap"
        focus={null}
        zero={[]}
      />
    </>
  );
}
