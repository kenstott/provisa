// Copyright (c) 2026 Kenneth Stott
// Canary: e203b774-09b9-4f3a-a172-efc74bdcf20b
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import React from "react";
import { useTranslation } from "react-i18next";
import { ActionIcon, Box, Button, Group, NumberInput, Select, TextInput } from "@mantine/core";
import { Play, Copy, Check, X, Sparkles } from "lucide-react";
import { format as formatSql } from "sql-formatter";
import CodeMirror from "@uiw/react-codemirror";
import { oneDark } from "@codemirror/theme-one-dark";
import { EditorView } from "@codemirror/view";
import type { Extension } from "@codemirror/state";
import { nlToSql } from "../../api/admin";
import type { SqlTab } from "./types";
import type { RegisteredTable } from "../../types/admin";

interface SqlEditorPanelProps {
  // Tab strip
  tabs: SqlTab[];
  activeTabId: string;
  editingTabId: string | null;
  editingTitle: string;
  setEditingTabId: React.Dispatch<React.SetStateAction<string | null>>;
  setEditingTitle: React.Dispatch<React.SetStateAction<string>>;
  switchTab: (id: string) => void;
  addTab: () => void;
  closeTab: (id: string) => void;
  renameTab: (id: string, title: string) => void;
  // NL strip
  nlText: string;
  setNlText: React.Dispatch<React.SetStateAction<string>>;
  nlLoading: boolean;
  setNlLoading: React.Dispatch<React.SetStateAction<boolean>>;
  nlError: string;
  setNlError: React.Dispatch<React.SetStateAction<string>>;
  setSqlText: React.Dispatch<React.SetStateAction<string>>;
  role: string;
  // Editor
  sqlText: string;
  sqlExtensions: Extension[];
  editorViewRef: React.MutableRefObject<EditorView | null>;
  copied: boolean;
  handleCopy: () => void;
  // Toolbar
  running: boolean;
  handleRun: () => void;
  sampleMode: "first" | "last" | "random";
  setSampleMode: React.Dispatch<React.SetStateAction<"first" | "last" | "random">>;
  sampleSize: number;
  setSampleSize: React.Dispatch<React.SetStateAction<number>>;
  roles: string[];
  setRole: React.Dispatch<React.SetStateAction<string>>;
  viewTable: RegisteredTable | null;
  viewSaving: boolean;
  setViewSaving: React.Dispatch<React.SetStateAction<boolean>>;
  updateTable: (input: Record<string, unknown>) => Promise<{ success: boolean; message: string }>;
  canCreateView: boolean;
  canRequestView: boolean;
  onOpenViewModal: () => void;
}

export function SqlEditorPanel({
  tabs,
  activeTabId,
  editingTabId,
  editingTitle,
  setEditingTabId,
  setEditingTitle,
  switchTab,
  addTab,
  closeTab,
  renameTab,
  nlText,
  setNlText,
  nlLoading,
  setNlLoading,
  nlError,
  setNlError,
  setSqlText,
  role,
  sqlText,
  sqlExtensions,
  editorViewRef,
  copied,
  handleCopy,
  running,
  handleRun,
  sampleMode,
  setSampleMode,
  sampleSize,
  setSampleSize,
  roles,
  setRole,
  viewTable,
  viewSaving,
  setViewSaving,
  updateTable,
  canCreateView,
  canRequestView,
  onOpenViewModal,
}: SqlEditorPanelProps) {
  const { t } = useTranslation();

  return (
    <>
      {/* Query tab strip */}
      <div
        style={{
          display: "flex",
          alignItems: "stretch",
          gap: "2px",
          padding: "0.25rem 0.5rem 0",
          borderBottom: "1px solid var(--border)",
          background: "var(--surface)",
          flexShrink: 0,
          overflowX: "auto",
        }}
      >
        {tabs.map((t2) => {
          const isActive = t2.id === activeTabId;
          return (
            <div
              key={t2.id}
              onClick={() => switchTab(t2.id)}
              onDoubleClick={() => {
                setEditingTabId(t2.id);
                setEditingTitle(t2.title);
              }}
              title={t("sqlEditorPanel.renameHint")}
              data-testid={`sql-tab-${t2.id}`}
              style={{
                display: "flex",
                alignItems: "center",
                gap: "0.35rem",
                padding: "0.25rem 0.5rem",
                fontSize: "0.75rem",
                cursor: "pointer",
                whiteSpace: "nowrap",
                borderTopLeftRadius: "5px",
                borderTopRightRadius: "5px",
                border: "1px solid var(--border)",
                borderBottom: isActive ? "1px solid var(--bg)" : "1px solid var(--border)",
                marginBottom: "-1px",
                background: isActive ? "var(--bg)" : "transparent",
                color: isActive ? "var(--text)" : "var(--text-muted)",
                fontWeight: isActive ? 600 : 400,
              }}
            >
              {editingTabId === t2.id ? (
                <TextInput
                  autoFocus
                  size="xs"
                  aria-label={t("sqlEditorPanel.renameHint")}
                  value={editingTitle}
                  onClick={(e) => e.stopPropagation()}
                  onChange={(e) => setEditingTitle(e.currentTarget.value)}
                  onBlur={() => {
                    const v = editingTitle.trim();
                    if (v) renameTab(t2.id, v);
                    setEditingTabId(null);
                  }}
                  onKeyDown={(e) => {
                    if (e.key === "Enter") {
                      const v = editingTitle.trim();
                      if (v) renameTab(t2.id, v);
                      setEditingTabId(null);
                    } else if (e.key === "Escape") {
                      setEditingTabId(null);
                    }
                  }}
                  styles={{ input: { width: "80px", fontSize: "0.75rem", height: "22px", minHeight: "22px" } }}
                />
              ) : (
                <span>{t2.title}</span>
              )}
              <ActionIcon
                variant="transparent"
                size="xs"
                aria-label={t("sqlEditorPanel.closeTab", { title: t2.title })}
                onClick={(e) => {
                  e.stopPropagation();
                  closeTab(t2.id);
                }}
                style={{ opacity: 0.6 }}
              >
                <X size={11} />
              </ActionIcon>
            </div>
          );
        })}
        <ActionIcon
          variant="transparent"
          onClick={addTab}
          aria-label={t("sqlEditorPanel.newTab")}
          title={t("sqlEditorPanel.newTab")}
          data-testid="sql-new-tab"
        >
          +
        </ActionIcon>
      </div>

      {/* NL-to-SQL strip */}
      <Group
        gap="sm"
        wrap="nowrap"
        align="center"
        style={{
          padding: "0.35rem 0.75rem",
          borderBottom: "1px solid var(--border)",
          background: "var(--surface)",
          flexShrink: 0,
        }}
      >
        <Box style={{ flex: 1, position: "relative", display: "flex", alignItems: "center" }}>
          <TextInput
            aria-label={t("sqlEditorPanel.nlPlaceholder")}
            placeholder={t("sqlEditorPanel.nlPlaceholder")}
            value={nlText}
            data-testid="sql-nl-input"
            style={{ width: "100%" }}
            size="xs"
            rightSection={
              nlText ? (
                <ActionIcon
                  variant="transparent"
                  size="xs"
                  aria-label={t("sqlEditorPanel.clear")}
                  title={t("sqlEditorPanel.clear")}
                  onClick={() => {
                    setNlText("");
                    setNlError("");
                  }}
                >
                  <X size={12} />
                </ActionIcon>
              ) : null
            }
            onChange={(e) => {
              setNlText(e.currentTarget.value);
              setNlError("");
            }}
            onKeyDown={async (e) => {
              if (e.key === "Enter" && nlText.trim() && !nlLoading) {
                setNlLoading(true);
                setNlError("");
                const result = await nlToSql(nlText.trim(), role);
                setNlLoading(false);
                if (result.error) {
                  setNlError(result.error);
                } else {
                  setSqlText(result.sql);
                }
              }
            }}
          />
        </Box>
        <Button
          size="xs"
          disabled={nlLoading || !nlText.trim()}
          data-testid="sql-nl-generate"
          onClick={async () => {
            setNlLoading(true);
            setNlError("");
            const result = await nlToSql(nlText.trim(), role);
            setNlLoading(false);
            if (result.error) {
              setNlError(result.error);
            } else {
              setSqlText(result.sql);
            }
          }}
          style={{ whiteSpace: "nowrap" }}
        >
          {nlLoading ? t("sqlEditorPanel.generating") : t("sqlEditorPanel.generate")}
        </Button>
        {nlError && (
          <span
            role="alert"
            style={{
              fontSize: "0.75rem",
              color: "var(--error)",
              maxWidth: "300px",
              overflow: "hidden",
              textOverflow: "ellipsis",
              whiteSpace: "nowrap",
            }}
            title={nlError}
          >
            {nlError}
          </span>
        )}
      </Group>

      {/* Editor */}
      <div
        style={{
          flex: "0 0 220px",
          overflow: "hidden",
          borderBottom: "1px solid var(--border)",
          position: "relative",
        }}
        onMouseEnter={(e) => {
          e.currentTarget
            .querySelectorAll<HTMLElement>(".hover-sql-btn")
            .forEach((btn) => (btn.style.opacity = "1"));
        }}
        onMouseLeave={(e) => {
          e.currentTarget
            .querySelectorAll<HTMLElement>(".hover-sql-btn")
            .forEach((btn) => (btn.style.opacity = "0"));
        }}
      >
        {nlLoading && (
          <div
            role="status"
            style={{
              position: "absolute",
              inset: 0,
              zIndex: 10,
              background: "rgba(0,0,0,0.45)",
              display: "flex",
              alignItems: "center",
              justifyContent: "center",
              gap: "0.5rem",
              color: "var(--text-muted)",
              fontSize: "0.8rem",
              pointerEvents: "none",
            }}
          >
            <span
              style={{
                display: "inline-block",
                width: "14px",
                height: "14px",
                border: "2px solid var(--text-muted)",
                borderTopColor: "transparent",
                borderRadius: "50%",
                animation: "spin 0.7s linear infinite",
              }}
            />
            {t("sqlEditorPanel.generatingSql")}
          </div>
        )}
        <CodeMirror
          value={sqlText}
          height="220px"
          theme={oneDark}
          extensions={sqlExtensions}
          onChange={(v) => {
            setSqlText(v);
          }}
          onCreateEditor={(view) => {
            editorViewRef.current = view;
          }}
          style={{ fontSize: "0.8rem" }}
        />
        <button
          className="hover-sql-btn"
          onClick={() => {
            try {
              setSqlText(formatSql(sqlText, { language: "postgresql" }));
            } catch {
              /* leave SQL unchanged if it can't be parsed */
            }
          }}
          title={t("sqlEditorPanel.prettify")}
          aria-label={t("sqlEditorPanel.prettify")}
          disabled={!sqlText.trim()}
          data-testid="sql-prettify"
          style={{
            position: "absolute",
            top: "0.4rem",
            right: "4.2rem",
            opacity: 0,
            transition: "opacity 0.15s",
            background: "rgba(30,30,40,0.85)",
            border: "1px solid var(--border)",
            borderRadius: "4px",
            color: "var(--text-muted)",
            cursor: "pointer",
            padding: "0.2rem 0.35rem",
            display: "flex",
            alignItems: "center",
            gap: "0.25rem",
            fontSize: "0.72rem",
          }}
        >
          <Sparkles size={11} />
          {t("sqlEditorPanel.prettify")}
        </button>
        <button
          className="hover-sql-btn"
          onClick={handleCopy}
          title={t("sqlEditorPanel.copySql")}
          aria-label={t("sqlEditorPanel.copySql")}
          data-testid="sql-copy"
          style={{
            position: "absolute",
            top: "0.4rem",
            right: "0.4rem",
            opacity: 0,
            transition: "opacity 0.15s",
            background: "rgba(30,30,40,0.85)",
            border: "1px solid var(--border)",
            borderRadius: "4px",
            color: "var(--text-muted)",
            cursor: "pointer",
            padding: "0.2rem 0.35rem",
            display: "flex",
            alignItems: "center",
            gap: "0.25rem",
            fontSize: "0.72rem",
          }}
        >
          {copied ? (
            <Check size={11} style={{ color: "var(--approve)" }} />
          ) : (
            <Copy size={11} />
          )}
          {copied ? t("sqlEditorPanel.copied") : t("sqlEditorPanel.copy")}
        </button>
      </div>

      {/* Toolbar */}
      <Group
        gap="sm"
        wrap="nowrap"
        align="center"
        style={{
          padding: "0.4rem 0.75rem",
          borderBottom: "1px solid var(--border)",
          flexShrink: 0,
          background: "var(--surface)",
        }}
      >
        <Button
          size="xs"
          leftSection={<Play size={11} />}
          onClick={handleRun}
          disabled={running || !sqlText.trim()}
          data-testid="sql-run"
        >
          {running ? t("sqlEditorPanel.running") : t("sqlEditorPanel.run")}
        </Button>
        {viewTable && (
          <Button
            size="xs"
            title={t("sqlEditorPanel.saveViewTitle", { tableName: viewTable.tableName })}
            disabled={viewSaving || !sqlText.trim()}
            data-testid="sql-update-view"
            onClick={async () => {
              setViewSaving(true);
              await updateTable({
                sourceId: viewTable.sourceId,
                domainId: viewTable.domainId ?? "",
                schemaName: viewTable.schemaName ?? "",
                tableName: viewTable.tableName,
                alias: viewTable.alias ?? undefined,
                description: viewTable.description ?? undefined,
                watermarkColumn: viewTable.watermarkColumn ?? null,
                viewSql: sqlText.trim(),
                dataProduct: viewTable.dataProduct ?? false,
                columns: viewTable.columns.map((c) => ({
                  name: c.columnName,
                  visibleTo: c.visibleTo ?? [],
                  writableBy: c.writableBy,
                  unmaskedTo: c.unmaskedTo,
                  maskType: c.maskType ?? undefined,
                  maskPattern: c.maskPattern ?? undefined,
                  maskReplace: c.maskReplace ?? undefined,
                  maskValue: c.maskValue ?? undefined,
                  maskPrecision: c.maskPrecision ?? undefined,
                  alias: c.alias ?? undefined,
                  description: c.description ?? undefined,
                  nativeFilterType: c.nativeFilterType ?? undefined,
                  isPrimaryKey: c.isPrimaryKey ?? undefined,
                  isForeignKey: c.isForeignKey ?? undefined,
                  isAlternateKey: c.isAlternateKey ?? undefined,
                  scope: c.scope ?? "domain",
                })),
              });
              setViewSaving(false);
            }}
          >
            {viewSaving
              ? t("sqlEditorPanel.saving")
              : t("sqlEditorPanel.updateView", { tableName: viewTable.tableName })}
          </Button>
        )}
        <Select
          aria-label={t("sqlEditorPanel.sampleModeLabel")}
          size="xs"
          data={[
            { value: "first", label: t("sqlEditorPanel.sampleModeFirst") },
            { value: "last", label: t("sqlEditorPanel.sampleModeLast") },
            { value: "random", label: t("sqlEditorPanel.sampleModeRandom") },
          ]}
          value={sampleMode}
          onChange={(v) => setSampleMode((v as "first" | "last" | "random") ?? "first")}
          allowDeselect={false}
          data-testid="sql-sample-mode"
          style={{ width: "110px" }}
        />
        <NumberInput
          aria-label={t("sqlEditorPanel.rowCountLabel")}
          title={t("sqlEditorPanel.rowCountLabel")}
          value={sampleSize}
          min={1}
          max={10000}
          onChange={(v) => setSampleSize(Math.max(1, typeof v === "number" ? v : parseInt(String(v)) || 100))}
          size="xs"
          data-testid="sql-sample-size"
          style={{ width: "90px" }}
        />
        <Select
          aria-label={t("sqlEditorPanel.roleLabel")}
          size="xs"
          data={roles}
          value={role}
          onChange={(v) => setRole(v ?? role)}
          allowDeselect={false}
          data-testid="sql-role"
          style={{ width: "140px" }}
        />
        <div style={{ flex: 1 }} />
        {(canCreateView || canRequestView) && sqlText.trim() && (
          <Button
            size="xs"
            variant="default"
            onClick={onOpenViewModal}
            data-testid="sql-open-view-modal"
          >
            {canCreateView ? t("sqlEditorPanel.createView") : t("sqlEditorPanel.requestView")}
          </Button>
        )}
      </Group>
    </>
  );
}
