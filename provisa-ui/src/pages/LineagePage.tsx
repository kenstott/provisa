// Copyright (c) 2026 Kenneth Stott
// Canary: 8f1c0a37-4b62-4d09-9e18-2a6d7f3c5b04
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1160/REQ-1161: data lineage explorer (column-level under the hood). Enter a SQL statement to see its full DAG
// (command boundaries spliced continuous to source columns, transforms named), or load the
// federation-wide provenance graph over every view/MV with cycles characterized.

import { useEffect, useMemo, useState } from "react";
import { useSearchParams } from "react-router-dom";
import {
  ActionIcon,
  Alert,
  Badge,
  Button,
  Group,
  Input,
  Paper,
  Stack,
  Text,
  Title,
  Tooltip,
} from "@mantine/core";
import { Copy, Check } from "lucide-react";
import CodeMirror from "@uiw/react-codemirror";
import { sql as sqlLang, PostgreSQL } from "@codemirror/lang-sql";
import { EditorView } from "@codemirror/view";
import { LineageDag } from "../components/lineage/LineageDag";
import { fetchLineageGraph, fetchFederationGraph } from "../api/lineage";
import type { LineageGraphData } from "../api/lineage";
import { useDomainFilter } from "../context/DomainFilterContext";

const LEGEND: { label: string; color: string }[] = [
  { label: "source column", color: "#2f9e44" },
  { label: "intermediate (in → out)", color: "#0c8599" },
  { label: "result column", color: "#1c7ed6" },
  { label: "command boundary", color: "#9c36b5" },
  { label: "final output (orange ring)", color: "#f08c00" },
];

const DEFAULT_SQL =
  "SELECT o.id, e.embedding, upper(e.geo) AS geo_u\nFROM orders o JOIN enrich_grpc_set('main.public.orders') e ON o.id = e.id";
// Persist the last query + rendered graph so leaving and returning to the page restores the view.
const SQL_KEY = "provisa.lineage.sql";
const GRAPH_KEY = "provisa.lineage.graph";

function loadStoredGraph(): LineageGraphData | null {
  try {
    const raw = sessionStorage.getItem(GRAPH_KEY);
    return raw ? (JSON.parse(raw) as LineageGraphData) : null;
  } catch {
    return null;
  }
}

export function LineagePage(): React.ReactElement {
  const [params] = useSearchParams();
  const [sql, setSql] = useState(
    params.get("sql") || sessionStorage.getItem(SQL_KEY) || DEFAULT_SQL,
  );
  // Restore the previously rendered graph unless a deep-link is driving a fresh build.
  const [graph, setGraph] = useState<LineageGraphData | null>(() =>
    params.get("sql") || params.get("focus") ? null : loadStoredGraph(),
  );
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);
  const [sqlHovered, setSqlHovered] = useState(false);
  const [sqlCopied, setSqlCopied] = useState(false);
  const { checkedDomains } = useDomainFilter();

  // Persist query + graph on every change so a later remount restores exactly what was here.
  useEffect(() => {
    sessionStorage.setItem(SQL_KEY, sql);
  }, [sql]);
  useEffect(() => {
    if (graph) sessionStorage.setItem(GRAPH_KEY, JSON.stringify(graph));
    else sessionStorage.removeItem(GRAPH_KEY);
  }, [graph]);

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

  // Deep-link: /lineage?sql=... auto-builds the statement graph; ?focus=<node> loads the federation
  // graph scoped to that relation/column (the "show lineage" entry point from other pages).
  useEffect(() => {
    const focus = params.get("focus");
    if (focus) {
      run(() => fetchFederationGraph({ focus }));
    } else if (params.get("sql")) {
      run(() => fetchLineageGraph(params.get("sql") as string));
    }
    // run once on mount for the incoming deep-link params
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  const cycles = graph?.cycles ?? [];
  const sqlExtensions = useMemo(
    () => [sqlLang({ dialect: PostgreSQL }), EditorView.lineWrapping],
    [],
  );

  return (
    <Stack p="md" gap="md">
      <Title order={3}>Data Lineage</Title>
      <Text c="dimmed" size="sm">
        Trace where data comes from. Paste a query below and Provisa maps each result column back
        through its transforms and command calls to the original source columns — so you can answer
        "what feeds this column?" before you publish a view or command. Or choose{" "}
        <b>Complete Lineage</b> to see provenance across every registered view and dataset at once.
      </Text>
      <Group align="flex-end" wrap="nowrap">
        <Input.Wrapper
          label="Query to analyze"
          description="Any SELECT that reads your registered tables, views, or commands. This query is only analyzed, never run — nothing is executed and no data is read."
          style={{ flex: 1 }}
        >
          <div
            data-testid="lineage-sql"
            style={{
              position: "relative",
              border: "1px solid var(--mantine-color-default-border)",
              borderRadius: 4,
              marginTop: 4,
            }}
            onMouseEnter={() => setSqlHovered(true)}
            onMouseLeave={() => setSqlHovered(false)}
          >
            <CodeMirror
              value={sql}
              onChange={setSql}
              extensions={sqlExtensions}
              minHeight="72px"
              basicSetup={{ lineNumbers: true, highlightActiveLine: true, foldGutter: false }}
              style={{ fontSize: "0.85rem" }}
            />
            <Tooltip label={sqlCopied ? "Copied" : "Copy query"}>
              <ActionIcon
                variant="default"
                size="sm"
                aria-label="Copy query"
                data-testid="lineage-copy"
                onClick={() => {
                  navigator.clipboard.writeText(sql);
                  setSqlCopied(true);
                  window.setTimeout(() => setSqlCopied(false), 1500);
                }}
                style={{
                  position: "absolute",
                  top: 4,
                  right: 4,
                  zIndex: 1,
                  opacity: sqlHovered ? 1 : 0,
                  transition: "opacity 150ms ease",
                  pointerEvents: sqlHovered ? "auto" : "none",
                }}
              >
                {sqlCopied ? <Check size={14} /> : <Copy size={14} />}
              </ActionIcon>
            </Tooltip>
          </div>
        </Input.Wrapper>
        <Stack gap="xs">
          <Button onClick={() => run(() => fetchLineageGraph(sql))} loading={loading} data-testid="lineage-build">
            Statement Lineage
          </Button>
          <Button
            variant="light"
            onClick={() => run(() => fetchFederationGraph({ domains: Array.from(checkedDomains) }))}
            loading={loading}
            data-testid="lineage-federation"
          >
            Complete Lineage
          </Button>
        </Stack>
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

      {graph && graph.nodes.length === 0 && (
        <Alert color="gray" title="Nothing to show" data-testid="lineage-empty">
          No lineage was found. Complete Lineage spans your registered views — none are defined yet,
          so there is nothing to trace. Register a view (Model → Views), or analyze a query above to
          see its lineage directly.
        </Alert>
      )}

      {graph && graph.nodes.length > 0 && (
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
