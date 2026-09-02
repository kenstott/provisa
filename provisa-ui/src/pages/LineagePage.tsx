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
  Modal,
  MultiSelect,
  Paper,
  Stack,
  Text,
  Title,
  Tooltip,
  useMantineColorScheme,
} from "@mantine/core";
import { Copy, Check } from "lucide-react";
import CodeMirror from "@uiw/react-codemirror";
import { oneDark } from "@codemirror/theme-one-dark";
import { sql as sqlLang, PostgreSQL } from "@codemirror/lang-sql";
import { EditorView } from "@codemirror/view";
import { LineageDag } from "../components/lineage/LineageDag";
import { fetchLineageGraph, fetchFederationGraph } from "../api/lineage";
import type { LineageGraphData } from "../api/lineage";
import { useDomainFilter } from "../context/DomainFilterContext";
import { useAuth } from "../context/AuthContext";
import { CapabilityGate } from "../components/CapabilityGate";
import { fetchOrgRoles } from "../api/admin";

const LEGEND: { label: string; color: string }[] = [
  { label: "source column", color: "#2f9e44" },
  { label: "intermediate (in → out)", color: "#0c8599" },
  { label: "result column", color: "#1c7ed6" },
  { label: "command boundary", color: "#9c36b5" },
  { label: "final output (orange ring)", color: "#f08c00" },
  { label: "dataset (collapsed — click to expand)", color: "#5c7cfa" },
];

const DEFAULT_SQL =
  "SELECT o.id, e.embedding, upper(e.geo) AS geo_u\nFROM orders o JOIN enrich_grpc_set('main.public.orders') e ON o.id = e.id";
// Persist the last query + rendered graph so leaving and returning to the page restores the view.
const SQL_KEY = "provisa.lineage.sql";
const GRAPH_KEY = "provisa.lineage.graph";
// Collapse state is part of what the reader was looking at, so it is restored with the graph —
// otherwise a return to the page re-explodes a federation they had narrowed down.
const COLLAPSED_KEY = "provisa.lineage.collapsed";

function loadStoredGraph(): LineageGraphData | null {
  try {
    const raw = sessionStorage.getItem(GRAPH_KEY);
    return raw ? (JSON.parse(raw) as LineageGraphData) : null;
  } catch {
    return null;
  }
}

function loadStoredCollapsed(): Set<string> {
  try {
    const raw = sessionStorage.getItem(COLLAPSED_KEY);
    return new Set(raw ? (JSON.parse(raw) as string[]) : []);
  } catch {
    return new Set();
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
  // A deep-link starts fetching on mount, so the page is already loading at first paint —
  // initializing here rather than setting it inside the mount effect keeps that first render truthful.
  const [loading, setLoading] = useState(() => Boolean(params.get("sql") || params.get("focus")));
  const [sqlHovered, setSqlHovered] = useState(false);
  const [sqlCopied, setSqlCopied] = useState(false);
  const { checkedDomains } = useDomainFilter();
  // REQ-1625/REQ-1628: Complete Lineage is read from a role's vantage point, and the role is picked
  // HERE rather than taken from the NavBar. The NavBar picker chooses which of the user's own roles
  // they act as; this one chooses whose lineage is being analysed, and a governance analyst must be
  // able to name ANY role in the org — including ones they do not hold — to see what that role's data
  // is derived from. The right to do that is view_governance, the same right the visible_to columns
  // carry, enforced on the endpoint and gated here so the control is absent without it.
  const { selectedRoles, activeOrgId } = useAuth();
  const [orgRoles, setOrgRoles] = useState<string[]>([]);
  // The user's current roles are the natural opening perspective; empty means "every role".
  const [lineageRoles, setLineageRoles] = useState<string[]>(() => selectedRoles.map((r) => r.id));
  // REQ-1627: which relations are drawn as a single node. Complete Lineage arrives collapsed — a
  // federation is unreadable column-by-column — and statement lineage arrives expanded, being small.
  const [collapsed, setCollapsed] = useState<ReadonlySet<string>>(() => loadStoredCollapsed());
  const [modalOpen, setModalOpen] = useState(false);

  const relationsOf = (g: LineageGraphData): string[] => [
    ...new Set(g.nodes.map((n) => n.relation).filter((r): r is string => !!r)),
  ];
  const toggleRelation = (relation: string) =>
    setCollapsed((prev) => {
      const next = new Set(prev);
      if (!next.delete(relation)) next.add(relation);
      return next;
    });

  useEffect(() => {
    if (!activeOrgId) return;
    let cancelled = false;
    void (async () => {
      const rows = await fetchOrgRoles(activeOrgId);
      if (!cancelled) setOrgRoles(rows.map((r) => r.id));
    })();
    return () => {
      cancelled = true;
    };
  }, [activeOrgId]);

  // Persist query + graph on every change so a later remount restores exactly what was here.
  useEffect(() => {
    sessionStorage.setItem(SQL_KEY, sql);
  }, [sql]);
  useEffect(() => {
    if (graph) sessionStorage.setItem(GRAPH_KEY, JSON.stringify(graph));
    else sessionStorage.removeItem(GRAPH_KEY);
  }, [graph]);
  useEffect(() => {
    sessionStorage.setItem(COLLAPSED_KEY, JSON.stringify([...collapsed]));
  }, [collapsed]);

  const run = async (fn: () => Promise<LineageGraphData>, collapseAll = false) => {
    setLoading(true);
    setError(null);
    try {
      const built = await fn();
      setCollapsed(collapseAll ? new Set(relationsOf(built)) : new Set());
      setGraph(built);
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
    const sqlParam = params.get("sql");
    if (!focus && !sqlParam) return;
    let cancelled = false;
    // Every setState here lands after the await, so mounting does not immediately re-render.
    void (async () => {
      try {
        const built = focus
          ? await fetchFederationGraph({ focus })
          : await fetchLineageGraph(sqlParam as string);
        if (!cancelled) setGraph(built);
      } catch (e) {
        if (cancelled) return;
        setError(e instanceof Error ? e.message : String(e));
        setGraph(null);
      } finally {
        if (!cancelled) setLoading(false);
      }
    })();
    return () => {
      cancelled = true;
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps -- deep-link params are read once, at mount
  }, []);

  const cycles = graph?.cycles ?? [];
  const { colorScheme } = useMantineColorScheme();
  const sqlExtensions = useMemo(
    () => [sqlLang({ dialect: PostgreSQL }), EditorView.lineWrapping],
    [],
  );

  return (
    // The graph panel grows to the bottom of the page rather than ending at a fixed height, so the
    // canvas is as tall as the window allows and the DAG is not read through a letterbox.
    <Stack p="md" gap="md" style={{ flex: 1, minHeight: 0 }}>
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
              // Same scheme-aware pairing as GrpcCodeView/SqlEditorPanel: CodeMirror's built-in
              // theme is light, so without this the panel stays white on the dark app chrome.
              // The toggle only ever writes "light"/"dark" (theme/ColorSchemeToggle.tsx:27), so
              // there is no "auto" case to resolve here.
              theme={colorScheme === "light" ? undefined : oneDark}
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
                  insetInlineEnd: 4,
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
          <CapabilityGate capability="view_governance">
            <MultiSelect
              size="xs"
              label="Lineage for role"
              placeholder={lineageRoles.length ? undefined : "All roles"}
              data={orgRoles}
              value={lineageRoles}
              onChange={setLineageRoles}
              searchable
              clearable
              data-testid="lineage-roles"
            />
          </CapabilityGate>
          <Button
            onClick={() => run(() => fetchLineageGraph(sql))}
            loading={loading}
            data-testid="lineage-build"
          >
            Statement Lineage
          </Button>
          <CapabilityGate capability="view_governance">
            <Button
              variant="light"
              onClick={() =>
                run(
                  () =>
                    fetchFederationGraph({
                      domains: Array.from(checkedDomains),
                      roles: lineageRoles,
                    }),
                  true,
                )
              }
              loading={loading}
              data-testid="lineage-federation"
            >
              Complete Lineage
            </Button>
          </CapabilityGate>
        </Stack>
      </Group>

      {error && (
        <Alert color="red" title="Lineage error" data-testid="lineage-error">
          {error}
        </Alert>
      )}

      {cycles.length > 0 && (
        <Alert
          color={cycles.some((c) => c.classification === "error") ? "red" : "yellow"}
          title="Cycles detected"
        >
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
        <Paper
          withBorder
          p="xs"
          style={{ flex: 1, minHeight: 0, display: "flex", flexDirection: "column" }}
        >
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
          <div style={{ flex: 1, minHeight: 0 }}>
            <LineageDag
              graph={graph}
              height="100%"
              collapsedRelations={collapsed}
              onToggleRelation={toggleRelation}
              onCollapseAll={() => setCollapsed(new Set(relationsOf(graph)))}
              onExpandAll={() => setCollapsed(new Set())}
              onOpenModal={() => setModalOpen(true)}
            />
          </div>
        </Paper>
      )}

      {/* REQ-1627: the same graph at near-fullscreen. A federation carries far more than the inline
          panel can show, and the collapse state is shared, so opening the modal continues the trace
          rather than restarting it. */}
      <Modal
        opened={modalOpen && !!graph}
        onClose={() => setModalOpen(false)}
        size="90%"
        title="Data Lineage"
        data-testid="lineage-modal"
        styles={{ body: { padding: 0 } }}
      >
        {graph && (
          <LineageDag
            graph={graph}
            height="calc(90vh - 120px)"
            collapsedRelations={collapsed}
            onToggleRelation={toggleRelation}
            onCollapseAll={() => setCollapsed(new Set(relationsOf(graph)))}
            onExpandAll={() => setCollapsed(new Set())}
          />
        )}
      </Modal>
    </Stack>
  );
}
