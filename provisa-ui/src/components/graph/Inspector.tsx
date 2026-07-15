// Copyright (c) 2026 Kenneth Stott
// Canary: e5692b82-a5ec-4d86-9cd1-51afff8d3874
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

/* eslint-disable react-hooks/refs --
   A prev-value ref gates the documented render-phase setState that resyncs the
   alias inputs when the selected edge changes; this is intentional. */

import { useRef, useState, useMemo, useCallback } from "react";
import { useTranslation } from "react-i18next";
import { ActionIcon, Badge, Button, Menu, Table, Text, TextInput, Tooltip } from "@mantine/core";
import { Braces, Check, ChevronRight, Copy, X } from "lucide-react";
import type { Relationship } from "../../types/admin";
import { PALETTE, labelColor, getStableNodeId } from "./graph-model";
import type { GNode, GEdge, GraphStats } from "./graph-model";
import CodeMirror from "@uiw/react-codemirror";
import { json as jsonLang } from "@codemirror/lang-json";
import { oneDark } from "@codemirror/theme-one-dark";

interface OverviewData {
  nodesByLabel: [string, number][];
  edgesByType: [string, number][];
  nodeCount: number;
  edgeCount: number;
}

interface InspectorProps {
  selected: { kind: "node"; data: GNode } | { kind: "edge"; data: GEdge } | null;
  graphStats?: GraphStats;
  overviewData?: OverviewData;
  colorOverrides: Record<string, string>;
  onColorChange: (label: string, color: string) => void;
  onClose: () => void;
  width: number;
  onResizeStart: (e: React.MouseEvent) => void;
  relationships?: Relationship[];
  onSaveEdgeAlias?: (relId: number, cqlAlias: string, gqlAlias: string) => Promise<void>;
  pkMap: Record<string, string[]>;
}

const HIDDEN_PROPS = new Set(["l1Cluster", "l2Cluster", "l3Cluster", "scl1", "scl2", "scl3", "deg_in", "deg_out", "deg_total"]);

export function Inspector({
  selected,
  graphStats,
  overviewData,
  colorOverrides,
  onColorChange,
  onClose,
  width,
  onResizeStart,
  relationships,
  onSaveEdgeAlias,
  pkMap,
}: InspectorProps) {
  const { t } = useTranslation();
  const [inspView, setInspView] = useState<"details" | "json">("details");
  const [showPalette, setShowPalette] = useState(false);
  const [edgeCql, setEdgeCql] = useState("");
  const [edgeGql, setEdgeGql] = useState("");
  const [savingAlias, setSavingAlias] = useState(false);
  const [copiedKey, setCopiedKey] = useState<string | null>(null);

  const matchedRel = useMemo(() => {
    if (!selected || selected.kind !== "edge" || !relationships) return null;
    const edgeType = (selected.data as GEdge).type;
    return relationships.find((r) => (r.alias ?? r.computedCypherAlias) === edgeType) ?? null;
  }, [selected, relationships]);

  const prevRelId = useRef<number | null>(null);
  if (matchedRel && matchedRel.id !== prevRelId.current) {
    prevRelId.current = matchedRel.id;
    setEdgeCql(matchedRel.alias ?? "");
    setEdgeGql(matchedRel.graphqlAlias ?? "");
  }
  if (!matchedRel && prevRelId.current !== null) {
    prevRelId.current = null;
    setEdgeCql("");
    setEdgeGql("");
  }

  const handleCopy = useCallback((key: string, value: string) => {
    navigator.clipboard.writeText(value).catch(() => {});
    setCopiedKey(key);
    setTimeout(() => setCopiedKey(null), 1200);
  }, []);

  const isN = selected !== null && selected.kind === "node";
  const label = selected ? (isN ? selected.data.label : (selected.data as GEdge).type) : "";
  const color = colorOverrides[label] ?? labelColor(label);

  const nodeLabel = selected && isN ? (selected.data as GNode).label : "";
  const colonIdx = nodeLabel.indexOf(":");
  const typeName = colonIdx > 0 ? nodeLabel.slice(colonIdx + 1) : nodeLabel;
  const domainPrefix = colonIdx > 0 ? nodeLabel.slice(0, colonIdx) : null;

  const stableId = selected && isN ? getStableNodeId(selected.data as GNode, pkMap) : null;
  const pkCols = selected && isN ? (pkMap[(selected.data as GNode).label] ?? []) : [];
  const idColName = pkCols[0] ?? null;

  const propRows: [string, unknown][] = useMemo(() => {
    if (!selected) return [];
    const props = selected.data.properties ?? {};
    if (isN) {
      const idRow: [string, unknown][] = stableId ? [["<id>", stableId]] : [];
      const pkEntry: Record<string, unknown> =
        idColName && !(idColName in props) ? { [idColName]: (selected.data as GNode).id } : {};
      return [
        ...idRow,
        ...Object.entries(props).filter(([k]) => !HIDDEN_PROPS.has(k)).sort(([a], [b]) => a.localeCompare(b)),
        ...Object.entries(pkEntry),
      ];
    }
    const e = selected.data as GEdge;
    return [
      ["<id>", e.identity],
      ["start", e.start],
      ["end", e.end],
      ...Object.entries(props).sort(([a], [b]) => a.localeCompare(b)),
    ] as [string, unknown][];
  }, [selected, isN, stableId, idColName]);

  const handleCopyAll = useCallback(() => {
    const obj = Object.fromEntries(propRows);
    navigator.clipboard.writeText(JSON.stringify(obj, null, 2)).catch(() => {});
    setCopiedKey("__all__");
    setTimeout(() => setCopiedKey(null), 1200);
  }, [propRows]);

  if (!selected) {
    return (
      <div className="gf-inspector" style={{ width, flexShrink: 0 }}>
        <div
          className="gf-inspector-resize-handle"
          role="separator"
          aria-orientation="vertical"
          aria-label={t("graphInspector.resizePanel")}
          onMouseDown={onResizeStart}
        />
        <div className="gf-insp-header">
          <Text component="span" className="gf-insp-header-title">
            {t("graphInspector.overview")}
          </Text>
          <div className="gf-insp-header-actions">
            <ActionIcon
              variant="subtle"
              size="sm"
              className="gf-insp-close"
              aria-label={t("graphInspector.hidePanel")}
              onClick={onClose}
              data-testid="inspector-hide"
            >
              <ChevronRight size={14} />
            </ActionIcon>
          </div>
        </div>
        {overviewData && overviewData.nodeCount > 0 ? (
          <div className="gf-overview">
            <div className="gf-overview-section-label">{t("graphInspector.nodeLabels")}</div>
            <div className="gf-overview-chips">
              <Badge variant="filled" radius="xl" className="gf-overview-chip gf-overview-chip--all">
                *({overviewData.nodeCount.toLocaleString()})
              </Badge>
              {overviewData.nodesByLabel.map(([lbl, cnt]) => (
                <Badge
                  key={lbl}
                  variant="filled"
                  radius="xl"
                  className="gf-overview-chip"
                  style={{ background: colorOverrides[lbl] ?? labelColor(lbl) }}
                >
                  {lbl} ({cnt.toLocaleString()})
                </Badge>
              ))}
            </div>
            <div className="gf-overview-section-label">{t("graphInspector.relationshipTypes")}</div>
            <div className="gf-overview-chips">
              <Badge variant="filled" radius="xl" className="gf-overview-chip gf-overview-chip--all">
                *({overviewData.edgeCount.toLocaleString()})
              </Badge>
              {overviewData.edgesByType.map(([type, cnt]) => (
                <Badge key={type} variant="filled" radius="sm" className="gf-overview-chip gf-overview-chip--rel">
                  {type} ({cnt.toLocaleString()})
                </Badge>
              ))}
            </div>
            <div className="gf-overview-summary">
              {t("graphInspector.summary", {
                nodeCount: overviewData.nodeCount.toLocaleString(),
                edgeCount: overviewData.edgeCount.toLocaleString(),
              })}
            </div>
          </div>
        ) : (
          <div className="gf-inspector-hint">{t("graphInspector.hint")}</div>
        )}
      </div>
    );
  }

  const headerLabel = isN ? t("graphInspector.nodeProperties") : t("graphInspector.relationshipProperties");
  const chipLabel = isN ? (typeName || label) : label;

  return (
    <div className="gf-inspector" style={{ width }}>
      <div
        className="gf-inspector-resize-handle"
        role="separator"
        aria-orientation="vertical"
        aria-label={t("graphInspector.resizePanel")}
        onMouseDown={onResizeStart}
      />
      <div className="gf-insp-header">
        <Text component="span" className="gf-insp-header-title">
          {headerLabel}
        </Text>
        <div className="gf-insp-header-actions">
          <Tooltip label={t("graphInspector.copyAll")} withinPortal>
            <ActionIcon
              variant="subtle"
              size="sm"
              className={`gf-insp-viewbtn gf-insp-copy-all${copiedKey === "__all__" ? " active" : ""}`}
              aria-label={t("graphInspector.copyAll")}
              onClick={handleCopyAll}
              data-testid="inspector-copy-all"
            >
              <Copy size={12} />
            </ActionIcon>
          </Tooltip>
          <Tooltip label={inspView === "json" ? t("graphInspector.showDetails") : t("graphInspector.showJson")} withinPortal>
            <ActionIcon
              variant="subtle"
              size="sm"
              className={`gf-insp-viewbtn ${inspView === "json" ? "active" : ""}`}
              aria-label={inspView === "json" ? t("graphInspector.showDetails") : t("graphInspector.showJson")}
              aria-pressed={inspView === "json"}
              onClick={() => setInspView(inspView === "json" ? "details" : "json")}
              data-testid="inspector-toggle-view"
            >
              <Braces size={14} />
            </ActionIcon>
          </Tooltip>
          <ActionIcon
            variant="subtle"
            size="sm"
            className="gf-insp-close"
            aria-label={t("graphInspector.close")}
            onClick={onClose}
            data-testid="inspector-close"
          >
            <X size={14} />
          </ActionIcon>
        </div>
      </div>

      <div className="gf-insp-chip-row">
        {isN && domainPrefix && (
          <Badge
            variant="filled"
            radius="xl"
            className="gf-inspector-badge"
            style={{ background: colorOverrides[domainPrefix] ?? labelColor(domainPrefix) }}
            title={t("graphInspector.domain")}
          >
            {domainPrefix}
          </Badge>
        )}
        <Menu position="bottom-start" withinPortal transitionProps={{ duration: 0 }} opened={showPalette} onChange={setShowPalette}>
          <Menu.Target>
            <Badge
              component="button"
              type="button"
              variant="filled"
              radius={isN ? "xl" : "sm"}
              className={isN ? "gf-inspector-badge" : "graph-rel-badge"}
              style={isN ? { background: color, cursor: "pointer" } : { cursor: "pointer" }}
              title={t("graphInspector.changeColor")}
              aria-label={t("graphInspector.changeColor")}
              data-testid="inspector-color-trigger"
            >
              {chipLabel}
            </Badge>
          </Menu.Target>
          <Menu.Dropdown className="gf-color-palette">
            {PALETTE.map((c) => (
              <Menu.Item
                key={c}
                className="gf-color-swatch"
                aria-label={t("graphInspector.colorSwatch", { color: c })}
                aria-current={color === c ? "true" : undefined}
                style={{ background: c, outline: color === c ? "2px solid #fff" : "none" }}
                onClick={() => onColorChange(label, c)}
              />
            ))}
          </Menu.Dropdown>
        </Menu>
      </div>

      {!isN && (
        <>
          <div className="gf-inspector-endpoints">
            <span
              style={{
                color:
                  colorOverrides[(selected.data as GEdge).startNode.label] ??
                  labelColor((selected.data as GEdge).startNode.label),
              }}
            >
              {(selected.data as GEdge).startNode.label || (selected.data as GEdge).start}
            </span>
            {" → "}
            <span
              style={{
                color:
                  colorOverrides[(selected.data as GEdge).endNode.label] ??
                  labelColor((selected.data as GEdge).endNode.label),
              }}
            >
              {(selected.data as GEdge).endNode.label || (selected.data as GEdge).end}
            </span>
          </div>
          {matchedRel && onSaveEdgeAlias && (
            <div className="gf-insp-alias-form">
              <TextInput
                className="gf-insp-alias-label"
                classNames={{ input: "gf-insp-alias-input" }}
                size="xs"
                label={t("graphInspector.cqlAliasLabel")}
                value={edgeCql}
                onChange={(e) => setEdgeCql(e.currentTarget.value)}
                placeholder={matchedRel.computedCypherAlias ?? (selected.data as GEdge).type}
                data-testid="inspector-cql-alias"
              />
              <TextInput
                className="gf-insp-alias-label"
                classNames={{ input: "gf-insp-alias-input" }}
                size="xs"
                label={t("graphInspector.gqlAliasLabel")}
                value={edgeGql}
                onChange={(e) => setEdgeGql(e.currentTarget.value)}
                placeholder={matchedRel.graphqlAlias ?? ""}
                data-testid="inspector-gql-alias"
              />
              <Button
                className="gf-insp-alias-save"
                variant="default"
                size="compact-xs"
                aria-label={t("graphInspector.saveAlias")}
                loading={savingAlias}
                onClick={async () => {
                  setSavingAlias(true);
                  await onSaveEdgeAlias(matchedRel.id, edgeCql, edgeGql);
                  setSavingAlias(false);
                }}
                data-testid="inspector-save-alias"
              >
                {!savingAlias && <Check size={14} />}
              </Button>
            </div>
          )}
        </>
      )}

      {inspView === "details" && (
        <div className="gf-insp-props-section">
          {isN && graphStats && (
            <>
              <div className="gf-insp-section-label">{t("graphInspector.graphStats")}</div>
              <Table className="gf-inspector-table">
                <Table.Tbody>
                  {(Object.entries(graphStats) as [string, string | number][])
                    .sort(([a], [b]) => a.localeCompare(b))
                    .map(([k, v]) => {
                      const vs = String(v);
                      return (
                        <Table.Tr key={k} className="gf-prop-row">
                          <Table.Td className="gf-prop-key">{k}</Table.Td>
                          <Table.Td className="gf-prop-val">{vs}</Table.Td>
                          <Table.Td className="gf-prop-copy-cell">
                            <ActionIcon
                              variant="subtle"
                              size="xs"
                              className={`gf-prop-copy${copiedKey === `stats:${k}` ? " copied" : ""}`}
                              aria-label={t("graphInspector.copyValue")}
                              onClick={() => handleCopy(`stats:${k}`, vs)}
                            >
                              <Copy size={12} />
                            </ActionIcon>
                          </Table.Td>
                        </Table.Tr>
                      );
                    })}
                </Table.Tbody>
              </Table>
            </>
          )}
          <div className="gf-insp-section-label">{t("graphInspector.properties")}</div>
          <Table className="gf-inspector-table">
            <Table.Tbody>
              {propRows.map(([k, v]) => {
                const vs = v === null || v === undefined ? "" : typeof v === "object" ? JSON.stringify(v) : String(v);
                return (
                  <Table.Tr key={k} className="gf-prop-row">
                    <Table.Td className="gf-prop-key">{k}</Table.Td>
                    <Table.Td className="gf-prop-val">{vs}</Table.Td>
                    <Table.Td className="gf-prop-copy-cell">
                      <ActionIcon
                        variant="subtle"
                        size="xs"
                        className={`gf-prop-copy${copiedKey === k ? " copied" : ""}`}
                        aria-label={t("graphInspector.copyValue")}
                        onClick={() => handleCopy(k, vs)}
                      >
                        <Copy size={12} />
                      </ActionIcon>
                    </Table.Td>
                  </Table.Tr>
                );
              })}
            </Table.Tbody>
          </Table>
        </div>
      )}

      {inspView === "json" && (
        <CodeMirror
          value={JSON.stringify(selected.data, null, 2)}
          extensions={[jsonLang()]}
          theme={oneDark}
          basicSetup={{ foldGutter: true, lineNumbers: false, highlightActiveLine: false }}
          readOnly
          style={{ fontSize: 12, flex: 1, overflow: "auto" }}
        />
      )}
    </div>
  );
}
