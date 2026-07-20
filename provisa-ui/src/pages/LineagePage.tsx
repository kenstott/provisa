// Copyright (c) 2026 Kenneth Stott
// Canary: 8f1c0a37-4b62-4d09-9e18-2a6d7f3c5b04
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1160/REQ-1161: column-level lineage explorer. Enter a SQL statement to see its full DAG
// (command boundaries spliced continuous to source columns, transforms named), or load the
// federation-wide provenance graph over every view/MV with cycles characterized.

import { useState } from "react";
import { Alert, Badge, Button, Group, Paper, Stack, Text, Textarea, Title } from "@mantine/core";
import { LineageDag } from "../components/lineage/LineageDag";
import { fetchLineageGraph, fetchFederationGraph } from "../api/lineage";
import type { LineageGraphData } from "../api/lineage";

const LEGEND: { label: string; color: string }[] = [
  { label: "source column", color: "#2f9e44" },
  { label: "derived (SQL)", color: "#1c7ed6" },
  { label: "command boundary", color: "#9c36b5" },
  { label: "output (orange ring)", color: "#f08c00" },
];

export function LineagePage(): React.ReactElement {
  const [sql, setSql] = useState(
    "SELECT o.id, e.embedding, upper(e.geo) AS geo_u\nFROM orders o JOIN enrich_grpc_set('main.public.orders') e ON o.id = e.id",
  );
  const [graph, setGraph] = useState<LineageGraphData | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);

  const run = async (fn: () => Promise<LineageGraphData>) => {
    setLoading(true);
    setError(null);
    try {
      setGraph(await fn());
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
      setGraph(null);
    } finally {
      setLoading(false);
    }
  };

  const cycles = graph?.cycles ?? [];

  return (
    <Stack p="md" gap="md">
      <Title order={3}>Column-Level Lineage</Title>
      <Textarea
        label="SQL statement"
        description="Commands compose inline; their contracts splice the DAG continuous to source columns."
        value={sql}
        onChange={(e) => setSql(e.currentTarget.value)}
        autosize
        minRows={3}
        data-testid="lineage-sql"
      />
      <Group>
        <Button onClick={() => run(() => fetchLineageGraph(sql))} loading={loading} data-testid="lineage-build">
          Build statement graph
        </Button>
        <Button
          variant="light"
          onClick={() => run(() => fetchFederationGraph())}
          loading={loading}
          data-testid="lineage-federation"
        >
          Federation graph
        </Button>
      </Group>

      {error && (
        <Alert color="red" title="Lineage error" data-testid="lineage-error">
          {error}
        </Alert>
      )}

      {cycles.length > 0 && (
        <Alert color={cycles.some((c) => c.classification === "error") ? "red" : "yellow"} title="Cycles detected">
          <Stack gap={4}>
            {cycles.map((c, i) => (
              <Text key={i} size="sm">
                <Badge color={c.classification === "error" ? "red" : "yellow"} mr="xs">
                  {c.classification}
                </Badge>
                {c.nodes.join(" → ")}
                {c.classification === "feedback"
                  ? " (legal — crosses a materialized boundary)"
                  : " (no materialization boundary — likely a design error)"}
              </Text>
            ))}
          </Stack>
        </Alert>
      )}

      {graph && (
        <Paper withBorder p="xs">
          <Group gap="md" mb="xs">
            {LEGEND.map((l) => (
              <Group key={l.label} gap={4}>
                <div style={{ width: 12, height: 12, borderRadius: 3, background: l.color }} />
                <Text size="xs">{l.label}</Text>
              </Group>
            ))}
            <Text size="xs" c="dimmed">
              {graph.nodes.length} columns · {graph.edges.length} edges
            </Text>
          </Group>
          <LineageDag graph={graph} />
        </Paper>
      )}
    </Stack>
  );
}
