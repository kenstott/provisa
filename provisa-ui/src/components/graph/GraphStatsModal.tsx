// Copyright (c) 2026 Kenneth Stott
// Canary: a3f2c7d1-88b4-4e29-b5a1-9c3e6f0d2741
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { useEffect, useState } from "react";
import type { ReactNode } from "react";
import { Card, Group, Loader, SimpleGrid, Stack, Text, Tooltip } from "@mantine/core";
import { useTranslation } from "react-i18next";
import type { GNode, GEdge } from "./graph-model";

interface GraphWideStats {
  // Instant
  nodeCount: number;
  edgeCount: number;
  nodesByLabel: [string, number][];
  edgesByType: [string, number][];
  density: number;
  avgDegree: number;
  maxDegree: number;
  isolatedCount: number;
  topHubs: { label: string; name: string; degree: number }[];
  // Async
  componentCount: number | null;
  largestComponentSize: number | null;
  diameter: number | null;
  avgPathLength: number | null;
}

function buildAdjacency(
  nodes: Map<string, GNode>,
  edges: Map<string, GEdge>,
): { adj: Map<number, Set<number>>; idToKey: Map<number, string> } {
  const adj = new Map<number, Set<number>>();
  const idToKey = new Map<number, string>();
  nodes.forEach((n, k) => {
    adj.set(n.id, new Set());
    idToKey.set(n.id, k);
  });
  edges.forEach((e) => {
    adj.get(e.start)?.add(e.end);
    adj.get(e.end)?.add(e.start);
  });
  return { adj, idToKey };
}

function computeInstant(
  nodes: Map<string, GNode>,
  edges: Map<string, GEdge>,
): Omit<GraphWideStats, "componentCount" | "largestComponentSize" | "diameter" | "avgPathLength"> {
  const nodeCount = nodes.size;
  const edgeCount = edges.size;

  const labelCounts = new Map<string, number>();
  nodes.forEach((n) => labelCounts.set(n.label, (labelCounts.get(n.label) ?? 0) + 1));
  const nodesByLabel = [...labelCounts.entries()].sort((a, b) => b[1] - a[1]);

  const typeCounts = new Map<string, number>();
  edges.forEach((e) => typeCounts.set(e.type, (typeCounts.get(e.type) ?? 0) + 1));
  const edgesByType = [...typeCounts.entries()].sort((a, b) => b[1] - a[1]);

  const n = nodeCount;
  const density = n > 1 ? edgeCount / (n * (n - 1)) : 0;

  const degree = new Map<number, number>();
  nodes.forEach((nd) => degree.set(nd.id, 0));
  edges.forEach((e) => {
    degree.set(e.start, (degree.get(e.start) ?? 0) + 1);
    degree.set(e.end, (degree.get(e.end) ?? 0) + 1);
  });

  let degreeSum = 0;
  let maxDegree = 0;
  degree.forEach((d) => {
    degreeSum += d;
    if (d > maxDegree) maxDegree = d;
  });
  const avgDegree = nodeCount > 0 ? degreeSum / nodeCount : 0;
  const isolatedCount = [...degree.values()].filter((d) => d === 0).length;

  const hubEntries: { label: string; name: string; degree: number }[] = [];
  nodes.forEach((n) => {
    const d = degree.get(n.id) ?? 0;
    const name =
      (n.properties["name"] as string) ??
      (n.properties["id"] as string) ??
      String(n.id);
    hubEntries.push({ label: n.label, name: String(name).slice(0, 30), degree: d });
  });
  const topHubs = hubEntries.sort((a, b) => b.degree - a.degree).slice(0, 7);

  return { nodeCount, edgeCount, nodesByLabel, edgesByType, density, avgDegree, maxDegree, isolatedCount, topHubs };
}

async function computeComponents(
  nodes: Map<string, GNode>,
  edges: Map<string, GEdge>,
): Promise<{ componentCount: number; largestComponentSize: number }> {
  const { adj } = buildAdjacency(nodes, edges);
  const visited = new Set<number>();
  let componentCount = 0;
  let largestComponentSize = 0;
  const ids = [...adj.keys()];

  for (let i = 0; i < ids.length; i++) {
    const start = ids[i];
    if (visited.has(start)) continue;
    componentCount++;
    let size = 0;
    const queue = [start];
    visited.add(start);
    while (queue.length > 0) {
      const cur = queue.shift()!;
      size++;
      for (const nb of adj.get(cur) ?? []) {
        if (!visited.has(nb)) {
          visited.add(nb);
          queue.push(nb);
        }
      }
    }
    if (size > largestComponentSize) largestComponentSize = size;
    // yield to event loop every 500 nodes to stay responsive
    if (i % 500 === 499) await new Promise((r) => setTimeout(r, 0));
  }

  return { componentCount, largestComponentSize };
}

async function computeDiameter(
  nodes: Map<string, GNode>,
  edges: Map<string, GEdge>,
  signal: AbortSignal,
): Promise<{ diameter: number; avgPathLength: number }> {
  const { adj } = buildAdjacency(nodes, edges);
  const ids = [...adj.keys()];
  const n = ids.length;
  let diameter = 0;
  let totalPathLength = 0;
  let reachablePairs = 0;

  for (let i = 0; i < n; i++) {
    if (signal.aborted) break;
    const src = ids[i];
    const dist = new Map<number, number>();
    dist.set(src, 0);
    const queue = [src];
    let qi = 0;
    while (qi < queue.length) {
      const cur = queue[qi++];
      const d = dist.get(cur)!;
      for (const nb of adj.get(cur) ?? []) {
        if (!dist.has(nb)) {
          dist.set(nb, d + 1);
          queue.push(nb);
        }
      }
    }
    dist.forEach((d, id) => {
      if (id === src) return;
      if (d > diameter) diameter = d;
      totalPathLength += d;
      reachablePairs++;
    });
    if (i % 100 === 99) await new Promise((r) => setTimeout(r, 0));
  }

  const avgPathLength = reachablePairs > 0 ? totalPathLength / reachablePairs : 0;
  return { diameter, avgPathLength };
}

interface QueryStatsSource {
  field: string;
  source: string;
  strategy: string;
  elapsed_ms: number;
  rows: number;
  cache_hit?: boolean;
}

interface QueryStats {
  total_elapsed_ms?: number;
  sources?: QueryStatsSource[];
}

interface Props {
  nodes: Map<string, GNode>;
  edges: Map<string, GEdge>;
  queryStats?: unknown;
}

function StatRow({ label, tooltip, value }: { label: string; tooltip?: string; value: ReactNode }) {
  const labelNode = tooltip ? (
    <Tooltip label={tooltip} multiline w={260} withArrow>
      <Text size="sm" c="dimmed" style={{ cursor: "help", borderBottom: "1px dotted var(--text-muted)" }}>
        {label}
      </Text>
    </Tooltip>
  ) : (
    <Text size="sm" c="dimmed">
      {label}
    </Text>
  );

  return (
    <Group justify="space-between" wrap="nowrap" gap="xs">
      {labelNode}
      <Text size="sm" fw={500}>
        {value}
      </Text>
    </Group>
  );
}

function CardTitle({ children, tooltip }: { children: ReactNode; tooltip?: string }) {
  const title = (
    <Text fw={600} size="sm" mb="xs" style={tooltip ? { cursor: "help" } : undefined}>
      {children}
    </Text>
  );
  return tooltip ? (
    <Tooltip label={tooltip} multiline w={260} withArrow>
      {title}
    </Tooltip>
  ) : (
    title
  );
}

export function GraphStatsPanel({ nodes, edges, queryStats }: Props) {
  const { t } = useTranslation();
  const qs = queryStats as QueryStats | undefined;
  const [instant, setInstant] = useState<ReturnType<typeof computeInstant> | null>(null);
  const [components, setComponents] = useState<{ componentCount: number; largestComponentSize: number } | null>(null);
  const [pathStats, setPathStats] = useState<{ diameter: number; avgPathLength: number } | null>(null);
  const [pathRunning, setPathRunning] = useState(true);

  useEffect(() => {
    const ctrl = new AbortController();

    setTimeout(() => {
      if (ctrl.signal.aborted) return;
      setInstant(computeInstant(nodes, edges));
    }, 0);

    computeComponents(nodes, edges).then((r) => {
      if (!ctrl.signal.aborted) setComponents(r);
    });

    computeDiameter(nodes, edges, ctrl.signal).then((r) => {
      if (!ctrl.signal.aborted) {
        setPathStats(r);
        setPathRunning(false);
      }
    });

    return () => ctrl.abort();
  }, [nodes, edges]);

  const pct = (v: number, total: number) =>
    total > 0 ? ` (${Math.round((v / total) * 100)}%)` : "";

  if (!instant) {
    return (
      <Group gap="xs" p="md" data-testid="graph-stats-loading">
        <Loader size="xs" />
        <Text size="sm" c="dimmed">
          {t("graphStatsPanel.computing")}
        </Text>
      </Group>
    );
  }

  return (
    <SimpleGrid cols={{ base: 1, sm: 2, lg: 3 }} spacing="sm" p="md" data-testid="graph-stats-grid">
      <Card withBorder padding="sm" radius="md">
        <CardTitle>{t("graphStatsPanel.size")}</CardTitle>
        <Stack gap={4}>
          <StatRow label={t("graphStatsPanel.nodes")} tooltip={t("graphStatsPanel.tooltipNodes")} value={instant.nodeCount.toLocaleString()} />
          <StatRow label={t("graphStatsPanel.edges")} tooltip={t("graphStatsPanel.tooltipEdges")} value={instant.edgeCount.toLocaleString()} />
          <StatRow label={t("graphStatsPanel.density")} tooltip={t("graphStatsPanel.tooltipDensity")} value={instant.density.toExponential(2)} />
        </Stack>
      </Card>

      <Card withBorder padding="sm" radius="md">
        <CardTitle>{t("graphStatsPanel.degree")}</CardTitle>
        <Stack gap={4}>
          <StatRow label={t("graphStatsPanel.average")} tooltip={t("graphStatsPanel.tooltipAverage")} value={instant.avgDegree.toFixed(2)} />
          <StatRow label={t("graphStatsPanel.maximum")} tooltip={t("graphStatsPanel.tooltipMaximum")} value={instant.maxDegree} />
          <StatRow
            label={t("graphStatsPanel.isolatedNodes")}
            tooltip={t("graphStatsPanel.tooltipIsolatedNodes")}
            value={`${instant.isolatedCount}${pct(instant.isolatedCount, instant.nodeCount)}`}
          />
        </Stack>
      </Card>

      <Card withBorder padding="sm" radius="md">
        <CardTitle>{t("graphStatsPanel.connectivity")}</CardTitle>
        <Stack gap={4}>
          <StatRow
            label={t("graphStatsPanel.components")}
            tooltip={t("graphStatsPanel.tooltipComponents")}
            value={components ? components.componentCount : <Loader size="xs" />}
          />
          <StatRow
            label={t("graphStatsPanel.largestComponent")}
            tooltip={t("graphStatsPanel.tooltipLargestComponent")}
            value={
              components
                ? `${components.largestComponentSize.toLocaleString()}${pct(components.largestComponentSize, instant.nodeCount)}`
                : <Loader size="xs" />
            }
          />
          <StatRow
            label={t("graphStatsPanel.diameter")}
            tooltip={t("graphStatsPanel.tooltipDiameter")}
            value={pathRunning ? <Loader size="xs" /> : (pathStats?.diameter ?? "—")}
          />
          <StatRow
            label={t("graphStatsPanel.avgPathLength")}
            tooltip={t("graphStatsPanel.tooltipAvgPathLength")}
            value={pathRunning ? <Loader size="xs" /> : pathStats ? pathStats.avgPathLength.toFixed(2) : "—"}
          />
        </Stack>
      </Card>

      <Card withBorder padding="sm" radius="md">
        <CardTitle tooltip={t("graphStatsPanel.tooltipTopHubsByDegree")}>{t("graphStatsPanel.topHubsByDegree")}</CardTitle>
        <Stack gap={4}>
          {instant.topHubs.map((h, i) => (
            <Group key={i} justify="space-between" wrap="nowrap" gap="xs">
              <Text size="xs" c="dimmed">
                {h.label}
              </Text>
              <Text size="xs" style={{ flex: 1, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>
                {h.name}
              </Text>
              <Text size="xs" fw={500}>
                {h.degree}
              </Text>
            </Group>
          ))}
        </Stack>
      </Card>

      <Card withBorder padding="sm" radius="md">
        <CardTitle tooltip={t("graphStatsPanel.tooltipNodesByLabel")}>{t("graphStatsPanel.nodesByLabel")}</CardTitle>
        <Stack gap={4}>
          {instant.nodesByLabel.map(([lbl, cnt]) => (
            <Group key={lbl} justify="space-between" gap="xs">
              <Text size="xs" c="dimmed">
                {lbl}
              </Text>
              <Text size="xs" fw={500}>
                {cnt.toLocaleString()}
              </Text>
            </Group>
          ))}
        </Stack>
      </Card>

      <Card withBorder padding="sm" radius="md">
        <CardTitle tooltip={t("graphStatsPanel.tooltipEdgesByType")}>{t("graphStatsPanel.edgesByType")}</CardTitle>
        <Stack gap={4}>
          {instant.edgesByType.map(([type, cnt]) => (
            <Group key={type} justify="space-between" gap="xs">
              <Text size="xs" c="dimmed">
                {type}
              </Text>
              <Text size="xs" fw={500}>
                {cnt.toLocaleString()}
              </Text>
            </Group>
          ))}
        </Stack>
      </Card>

      {qs && (
        <Card withBorder padding="sm" radius="md">
          <CardTitle>{t("graphStatsPanel.queryExecution")}</CardTitle>
          <Stack gap={4}>
            {qs.total_elapsed_ms !== undefined && (
              <StatRow label={t("graphStatsPanel.total")} value={`${qs.total_elapsed_ms.toFixed(1)} ms`} />
            )}
            {(qs.sources ?? []).map((s, i) => (
              <Stack key={i} gap={2} mt={i > 0 ? "xs" : undefined}>
                <Text size="xs" fw={600}>
                  {s.field}
                </Text>
                {s.strategy !== "federated" && (
                  <Group justify="space-between" gap="xs">
                    <Text size="xs" c="dimmed">
                      {t("graphStatsPanel.source")}
                    </Text>
                    <Text size="xs" ff="monospace">
                      {s.source}
                    </Text>
                  </Group>
                )}
                <Group justify="space-between" gap="xs">
                  <Text size="xs" c="dimmed">
                    {t("graphStatsPanel.strategy")}
                  </Text>
                  <Text size="xs">{s.strategy}</Text>
                </Group>
                <Group justify="space-between" gap="xs">
                  <Text size="xs" c="dimmed">
                    {t("graphStatsPanel.elapsed")}
                  </Text>
                  <Text size="xs">{s.elapsed_ms.toFixed(1)} ms</Text>
                </Group>
                <Group justify="space-between" gap="xs">
                  <Text size="xs" c="dimmed">
                    {t("graphStatsPanel.rows")}
                  </Text>
                  <Text size="xs">{s.rows}</Text>
                </Group>
                {s.cache_hit !== undefined && (
                  <Group justify="space-between" gap="xs">
                    <Text size="xs" c="dimmed">
                      {t("graphStatsPanel.cache")}
                    </Text>
                    <Text size="xs">{s.cache_hit ? t("graphStatsPanel.cacheHit") : t("graphStatsPanel.cacheMiss")}</Text>
                  </Group>
                )}
              </Stack>
            ))}
          </Stack>
        </Card>
      )}
    </SimpleGrid>
  );
}
