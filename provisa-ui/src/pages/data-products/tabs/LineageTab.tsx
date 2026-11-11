// Copyright (c) 2026 Kenneth Stott
// Canary: 8b4d2f60-1c7e-4a93-9f58-d3a6e2b0c719
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1640, REQ-2156: a data product's Lineage tab -- where its tables come from, cut to what its
// reader may read. REQ-2239: drawn by the one graph component, as every 2D graph is.

import { useMemo } from "react";
import { useTranslation } from "react-i18next";
import { Text } from "@mantine/core";
import type { ProductLineage } from "../../../api/lineage";
import { ProvisaGraph } from "../../../components/provisa-graph/ProvisaGraph";
import { upstreamLineage } from "../upstreamLineage";
import { productLineageSpec } from "./productLineageSpec";

export function LineageTab({
  productId,
  loading,
  error,
  graph,
  relationLabel,
}: {
  productId: string;
  loading: boolean;
  error: string;
  graph: ProductLineage | null;
  relationLabel?: (relation: string) => string;
}) {
  const { t } = useTranslation();
  const view = useMemo(
    () =>
      graph
        ? productLineageSpec(upstreamLineage(graph), relationLabel ?? ((r: string) => r))
        : null,
    [graph, relationLabel],
  );
  // REQ-1667, REQ-2174: each lane named in words.
  const lanes = useMemo(
    () =>
      view && {
        lanes: view.lanes.map(({ id, hops }) => ({
          id,
          title:
            hops === 0
              ? t("dataProductsTab.detail.lineageLaneTables")
              : hops === -1
                ? t("dataProductsTab.detail.lineageLaneInputs")
                : t("dataProductsTab.detail.lineageLaneUpstream", { n: -hops - 1 }),
        })),
      },
    [view, t],
  );
  if (loading) {
    return (
      <Text size="sm" c="var(--text-muted)">
        {t("dataProductsTab.detail.lineageLoading")}
      </Text>
    );
  }
  if (error) {
    return (
      <Text size="sm" c="red">
        {error}
      </Text>
    );
  }
  if (!view || !lanes || view.spec.nodes.length === 0) {
    return (
      <Text size="sm" c="var(--text-muted)" data-testid={`data-product-lineage-empty-${productId}`}>
        {t("dataProductsTab.detail.lineageEmpty")}
      </Text>
    );
  }
  return (
    <div data-testid={`data-product-detail-lineage-${productId}`}>
      <Text size="sm" fw={500} data-testid={`data-product-lineage-title-${productId}`}>
        {t("dataProductsTab.detail.lineageTitle")}
      </Text>
      <Text size="xs" c="dimmed" mb="xs">
        {t("dataProductsTab.detail.lineageHint")}
      </Text>
      <div style={{ height: 560 }}>
        <ProvisaGraph
          graph={view.spec}
          preset="lanes"
          lanes={lanes}
          testId={`data-product-lineage-graph-${productId}`}
          ariaLabel={t("dataProductsTab.detail.lineageTitle")}
          exportName={`${productId}-lineage`}
        />
      </div>
    </div>
  );
}
