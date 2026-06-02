// Copyright (c) 2026 Kenneth Stott
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { useState, useRef, useEffect } from "react";
import { labelColor, PALETTE } from "./graph-model";
import type { RelLineOverride } from "./graph-model";

// ── Context menu state ──────────────────────────────────────────────────────────
export interface ContextMenuState {
  x: number;
  y: number;
  compoundLabel: string;
  tableLabel: string;
  properties: string[];
}

export interface RelContextMenuState {
  x: number;
  y: number;
  type: string;
}

// ── Node context menu ───────────────────────────────────────────────────────────
interface NodeContextMenuProps {
  menu: ContextMenuState;
  colorOverrides: Record<string, string>;
  sizeOverrides: Record<string, number>;
  labelProperty: Record<string, string>;
  onColorChange: (label: string, color: string) => void;
  onSizeChange: (label: string, size: number) => void;
  onLabelPropertyChange: (label: string, prop: string) => void;
  onClose: () => void;
}

export function NodeContextMenu({
  menu,
  colorOverrides,
  sizeOverrides,
  labelProperty,
  onColorChange,
  onSizeChange,
  onLabelPropertyChange,
  onClose,
}: NodeContextMenuProps) {
  const ref = useRef<HTMLDivElement>(null);
  const currentColor = colorOverrides[menu.compoundLabel] ?? labelColor(menu.compoundLabel);
  const currentSize = sizeOverrides[menu.compoundLabel] ?? 44;
  const currentProp = labelProperty[menu.compoundLabel] ?? "";

  useEffect(() => {
    const handler = (e: MouseEvent) => {
      if (ref.current && !ref.current.contains(e.target as Node)) onClose();
    };
    document.addEventListener("mousedown", handler);
    return () => document.removeEventListener("mousedown", handler);
  }, [onClose]);

  return (
    <div
      ref={ref}
      className="node-ctx-menu"
      style={{ left: menu.x, top: menu.y }}
      onMouseDown={(e) => e.stopPropagation()}
    >
      <div className="node-ctx-title">{menu.tableLabel}</div>
      <div className="node-ctx-section-label">Color</div>
      <div className="node-ctx-palette">
        {PALETTE.map((c) => (
          <button
            key={c}
            className={`node-ctx-swatch${currentColor === c ? " active" : ""}`}
            style={{ background: c }}
            onClick={() => {
              onColorChange(menu.compoundLabel, c);
              onClose();
            }}
          />
        ))}
      </div>
      <div className="node-ctx-section-label">Size</div>
      <div className="node-ctx-size-row">
        <input
          type="range"
          min={20}
          max={120}
          value={currentSize}
          onChange={(e) => onSizeChange(menu.compoundLabel, Number(e.target.value))}
        />
        <span>{currentSize}px</span>
      </div>
      {menu.properties.length > 0 && (
        <>
          <div className="node-ctx-section-label">Label by</div>
          <select
            className="node-ctx-select"
            value={currentProp}
            onChange={(e) => {
              onLabelPropertyChange(menu.compoundLabel, e.target.value);
              onClose();
            }}
          >
            <option value="">default</option>
            {menu.properties.map((p) => (
              <option key={p} value={p}>
                {p}
              </option>
            ))}
          </select>
        </>
      )}
    </div>
  );
}

// ── Relationship context menu ─────────────────────────────────────────────────
interface RelContextMenuProps {
  menu: RelContextMenuState;
  relLineOverrides: Record<string, RelLineOverride>;
  onRelLineChange: (type: string, override: RelLineOverride) => void;
  onClose: () => void;
}

const LINE_STYLES: Array<RelLineOverride["style"]> = ["solid", "dashed", "dotted"];
const LINE_STYLE_LABELS: Record<RelLineOverride["style"], string> = {
  solid: "—",
  dashed: "╌",
  dotted: "···",
};

export function RelContextMenu({
  menu,
  relLineOverrides,
  onRelLineChange,
  onClose,
}: RelContextMenuProps) {
  const ref = useRef<HTMLDivElement>(null);
  const current = relLineOverrides[menu.type] ?? { width: 1.5, style: "solid" as const };

  useEffect(() => {
    const handler = (e: MouseEvent) => {
      if (ref.current && !ref.current.contains(e.target as Node)) onClose();
    };
    document.addEventListener("mousedown", handler);
    return () => document.removeEventListener("mousedown", handler);
  }, [onClose]);

  return (
    <div
      ref={ref}
      className="node-ctx-menu"
      style={{ left: menu.x, top: menu.y }}
      onMouseDown={(e) => e.stopPropagation()}
    >
      <div className="node-ctx-title">{menu.type}</div>
      <div className="node-ctx-section-label">Line Style</div>
      <div className="rel-ctx-style-row">
        {LINE_STYLES.map((s) => (
          <button
            key={s}
            className={`rel-ctx-style-btn${current.style === s ? " active" : ""}`}
            onClick={() => onRelLineChange(menu.type, { ...current, style: s })}
            title={s}
          >
            {LINE_STYLE_LABELS[s]}
          </button>
        ))}
      </div>
      <div className="node-ctx-section-label">Line Width</div>
      <div className="node-ctx-size-row">
        <input
          type="range"
          min={0.5}
          max={8}
          step={0.5}
          value={current.width}
          onChange={(e) => onRelLineChange(menu.type, { ...current, width: Number(e.target.value) })}
        />
        <span>{current.width}px</span>
      </div>
    </div>
  );
}

// ── Native filter modal ───────────────────────────────────────────────────────
interface NativeFilterModalProps {
  label: string;
  filterColumns: string[];
  onConfirm: (params: Record<string, string>) => void;
  onCancel: () => void;
}

export function NativeFilterModal({
  label,
  filterColumns,
  onConfirm,
  onCancel,
}: NativeFilterModalProps) {
  const [values, setValues] = useState<Record<string, string>>(() =>
    Object.fromEntries(filterColumns.map((c) => [c, ""])),
  );

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    onConfirm(values);
  };

  return (
    <div className="nf-modal-backdrop" onClick={onCancel}>
      <div className="nf-modal" onClick={(e) => e.stopPropagation()}>
        <div className="nf-modal-title">Parameters for {label}</div>
        <form onSubmit={handleSubmit}>
          {filterColumns.map((col) => (
            <div key={col} className="nf-modal-field">
              <label className="nf-modal-label">{col}</label>
              <input
                className="nf-modal-input"
                value={values[col]}
                onChange={(e) => setValues((v) => ({ ...v, [col]: e.target.value }))}
                placeholder={col}
                autoFocus={filterColumns[0] === col}
              />
            </div>
          ))}
          <div className="nf-modal-actions">
            <button type="button" className="nf-modal-cancel" onClick={onCancel}>
              Cancel
            </button>
            <button type="submit" className="nf-modal-run">
              Run
            </button>
          </div>
        </form>
      </div>
    </div>
  );
}
