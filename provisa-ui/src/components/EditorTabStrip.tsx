// Copyright (c) 2026 Kenneth Stott
// Canary: 47a898d1-1d83-4107-bb1f-2f3343ca48ea
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// The strip of an explorer's editor tabs (the SQL explorer's queries, the Python tab's scripts):
// switch by click, rename by double-click, close by its x, add by +, and -- where the explorer
// orders its tabs -- reorder by dragging a tab or by Alt+ArrowLeft / Alt+ArrowRight on it.
import { useState, type ReactNode } from "react";
import { ActionIcon, TextInput } from "@mantine/core";
import { X } from "lucide-react";
import { Tip } from "./Tip";

export interface EditorTab {
  id: string;
  title: string;
  /** Shown after the title (the SQL explorer's "detached" badge). */
  badge?: ReactNode;
}

export interface EditorTabLabels {
  renameHint: string;
  newTab: string;
  closeTab: (title: string) => string;
}

export function EditorTabStrip({
  tabs,
  activeId,
  labels,
  testIdPrefix,
  onSwitch,
  onAdd,
  onClose,
  onRename,
  onMove,
}: {
  tabs: EditorTab[];
  activeId: string;
  labels: EditorTabLabels;
  /** Test ids are `<prefix>-tab-<id>` and `<prefix>-new-tab`. */
  testIdPrefix: string;
  onSwitch: (id: string) => void;
  onAdd: () => void;
  onClose: (id: string) => void;
  onRename: (id: string, title: string) => void;
  /** Move tab ``id`` to position ``index``; no reordering without it. */
  onMove?: (id: string, index: number) => void;
}) {
  const [editingId, setEditingId] = useState<string | null>(null);
  const [editingTitle, setEditingTitle] = useState("");
  const [dragged, setDragged] = useState<string | null>(null);

  const commit = (id: string) => {
    const v = editingTitle.trim();
    if (v) onRename(id, v);
    setEditingId(null);
  };

  return (
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
      {tabs.map((tab, index) => {
        const isActive = tab.id === activeId;
        return (
          <Tip key={tab.id} text={labels.renameHint}>
            <div
              onClick={() => onSwitch(tab.id)}
              onDoubleClick={() => {
                setEditingId(tab.id);
                setEditingTitle(tab.title);
              }}
              tabIndex={onMove ? 0 : undefined}
              onKeyDown={
                onMove
                  ? (e) => {
                      if (!e.altKey || editingId === tab.id) return;
                      if (e.key === "ArrowLeft" && index > 0) {
                        e.preventDefault();
                        onMove(tab.id, index - 1);
                      } else if (e.key === "ArrowRight" && index < tabs.length - 1) {
                        e.preventDefault();
                        onMove(tab.id, index + 1);
                      }
                    }
                  : undefined
              }
              draggable={onMove !== undefined && editingId !== tab.id}
              onDragStart={onMove ? () => setDragged(tab.id) : undefined}
              onDragOver={onMove ? (e) => e.preventDefault() : undefined}
              onDrop={
                onMove
                  ? (e) => {
                      e.preventDefault();
                      if (dragged !== null && dragged !== tab.id) onMove(dragged, index);
                      setDragged(null);
                    }
                  : undefined
              }
              onDragEnd={onMove ? () => setDragged(null) : undefined}
              data-testid={`${testIdPrefix}-tab-${tab.id}`}
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
                opacity: dragged === tab.id ? 0.5 : 1,
              }}
            >
              {editingId === tab.id ? (
                <TextInput
                  autoFocus
                  size="xs"
                  aria-label={labels.renameHint}
                  value={editingTitle}
                  onClick={(e) => e.stopPropagation()}
                  onChange={(e) => setEditingTitle(e.currentTarget.value)}
                  onBlur={() => commit(tab.id)}
                  onKeyDown={(e) => {
                    if (e.key === "Enter") commit(tab.id);
                    else if (e.key === "Escape") setEditingId(null);
                  }}
                  styles={{
                    input: {
                      width: "80px",
                      fontSize: "0.75rem",
                      height: "22px",
                      minHeight: "22px",
                    },
                  }}
                />
              ) : (
                <span>{tab.title}</span>
              )}
              {tab.badge}
              <ActionIcon
                variant="transparent"
                size="xs"
                aria-label={labels.closeTab(tab.title)}
                onClick={(e) => {
                  e.stopPropagation();
                  onClose(tab.id);
                }}
                style={{ opacity: 0.6 }}
              >
                <X size={11} />
              </ActionIcon>
            </div>
          </Tip>
        );
      })}
      <Tip text={labels.newTab} named>
        <ActionIcon
          variant="transparent"
          onClick={onAdd}
          aria-label={labels.newTab}
          data-testid={`${testIdPrefix}-new-tab`}
        >
          +
        </ActionIcon>
      </Tip>
    </div>
  );
}
