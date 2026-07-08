// Copyright (c) 2026 Kenneth Stott
// Canary: 91caf8b6-bf56-42b1-92b2-77bfbaa804d1
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { useState, useRef, useEffect, useLayoutEffect } from "react";
import { labelColor, PALETTE } from "./graph-model";
import type { RelLineOverride } from "./graph-model";

// ── Context menu state ──────────────────────────────────────────────────────────
export interface ContextMenuState {
  x: number;
  y: number;
  compoundLabel: string;
  tableLabel: string;
  properties: string[];
  numericProperties: string[];
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
  sizeByProperty: Record<string, string>;
  sizeMultiplier: Record<string, number>;
  onColorChange: (label: string, color: string) => void;
  onSizeChange: (label: string, size: number) => void;
  onLabelPropertyChange: (label: string, prop: string) => void;
  onSizeByPropertyChange: (label: string, prop: string) => void;
  onSizeMultiplierChange: (label: string, multiplier: number) => void;
  onClose: () => void;
}

export function NodeContextMenu({
  menu,
  colorOverrides,
  sizeOverrides,
  labelProperty,
  sizeByProperty,
  sizeMultiplier,
  onColorChange,
  onSizeChange,
  onLabelPropertyChange,
  onSizeByPropertyChange,
  onSizeMultiplierChange,
  onClose,
}: NodeContextMenuProps) {
  const ref = useRef<HTMLDivElement>(null);
  const currentColor = colorOverrides[menu.compoundLabel] ?? labelColor(menu.compoundLabel);
  const currentSize = sizeOverrides[menu.compoundLabel] ?? 44;
  const currentProp = labelProperty[menu.compoundLabel] ?? "";
  const currentSizeByProp = sizeByProperty[menu.compoundLabel] ?? "";
  const currentMultiplier = sizeMultiplier[menu.compoundLabel] ?? 3;

  useEffect(() => {
    const handler = (e: MouseEvent) => {
      if (ref.current && !ref.current.contains(e.target as Node)) onClose();
    };
    document.addEventListener("mousedown", handler);
    return () => document.removeEventListener("mousedown", handler);
  }, [onClose]);

  useLayoutEffect(() => {
    const el = ref.current;
    if (!el) return;
    const r = el.getBoundingClientRect();
    let left = menu.x;
    let top = menu.y;
    if (r.right > window.innerWidth) left -= r.right - window.innerWidth;
    if (left < 0) left = 0;
    if (r.bottom > window.innerHeight) top -= r.height;
    if (top < 0) top = 0;
    el.style.left = `${left}px`;
    el.style.top = `${top}px`;
    el.style.visibility = "visible";
  }, [menu.x, menu.y]);

  return (
    <div
      ref={ref}
      className="node-ctx-menu"
      style={{ left: menu.x, top: menu.y, visibility: "hidden" }}
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
      {menu.numericProperties.length > 0 && (
        <>
          <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between" }}>
            <div className="node-ctx-section-label" style={{ margin: 0 }}>Size by</div>
            {currentSizeByProp && (
              <div style={{ display: "flex", alignItems: "center", gap: "0.25rem" }}>
                <span style={{ fontSize: "0.72rem", color: "var(--text-muted)" }}>×</span>
                <input
                  type="number"
                  min={1}
                  max={10}
                  step={0.5}
                  value={currentMultiplier}
                  onChange={(e) => {
                    const v = parseFloat(e.target.value);
                    if (!isNaN(v) && v >= 1) onSizeMultiplierChange(menu.compoundLabel, v);
                  }}
                  className="node-ctx-select"
                  style={{ width: 52 }}
                />
              </div>
            )}
          </div>
          <select
            className="node-ctx-select"
            value={currentSizeByProp}
            onChange={(e) => {
              onSizeByPropertyChange(menu.compoundLabel, e.target.value);
              onClose();
            }}
          >
            <option value="">fixed size</option>
            {menu.numericProperties.map((p) => (
              <option key={p} value={p}>{p}</option>
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

  useLayoutEffect(() => {
    const el = ref.current;
    if (!el) return;
    const r = el.getBoundingClientRect();
    let left = menu.x;
    let top = menu.y;
    if (r.right > window.innerWidth) left -= r.right - window.innerWidth;
    if (left < 0) left = 0;
    if (r.bottom > window.innerHeight) top -= r.height;
    if (top < 0) top = 0;
    el.style.left = `${left}px`;
    el.style.top = `${top}px`;
    el.style.visibility = "visible";
  }, [menu.x, menu.y]);

  return (
    <div
      ref={ref}
      className="node-ctx-menu"
      style={{ left: menu.x, top: menu.y, visibility: "hidden" }}
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
  filterColumns: { name: string; type: string }[];
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
    Object.fromEntries(filterColumns.map((c) => [c.name, ""])),
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
            <div key={col.name} className="nf-modal-field">
              <label className="nf-modal-label">{col.name}</label>
              <input
                className="nf-modal-input"
                value={values[col.name]}
                onChange={(e) => setValues((v) => ({ ...v, [col.name]: e.target.value }))}
                placeholder={col.name}
                autoFocus={filterColumns[0].name === col.name}
              />
            </div>
          ))}
          <div className="nf-modal-actions">
            <button type="button" className="nf-modal-cancel" onClick={onCancel}>
              ✕
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
