// Copyright (c) 2026 Kenneth Stott
// Canary: b4e9c3a1-5f2d-4b8e-9c7a-6d0e1f2a3b4c
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { useState } from "react";
import type { ColumnPreset } from "../../types/admin";

interface Props {
  presets: ColumnPreset[];
  columns: string[];
  columnTypes?: Record<string, string>;
  onChange: (presets: ColumnPreset[]) => void;
}

const SOURCES: { value: ColumnPreset["source"]; label: string }[] = [
  { value: "now", label: "now (UTC timestamp)" },
  { value: "header", label: "header (HTTP request header)" },
  { value: "literal", label: "literal (fixed value)" },
];

const TIMESTAMP_TYPES = new Set(["timestamp", "timestamp with time zone", "timestamp without time zone", "timestamptz", "datetime"]);
const DATE_TYPES = new Set(["date"]);
const TIME_TYPES = new Set(["time", "time with time zone", "time without time zone", "timetz"]);
const NUMERIC_TYPES = new Set(["integer", "int", "bigint", "smallint", "tinyint", "float", "double", "real", "decimal", "numeric", "double precision"]);
const BOOL_TYPES = new Set(["boolean", "bool"]);

function normalizeType(t: string): string {
  return t.toLowerCase().split("(")[0].trim();
}

function isTemporalType(t: string): boolean {
  const n = normalizeType(t);
  return TIMESTAMP_TYPES.has(n) || DATE_TYPES.has(n) || TIME_TYPES.has(n);
}

function getLiteralInput(colType: string | undefined, value: string | null, onChange: (v: string | null) => void) {
  if (!colType) {
    return <input placeholder="Literal value" value={value ?? ""} onChange={(e) => onChange(e.target.value || null)} />;
  }
  const n = normalizeType(colType);
  if (NUMERIC_TYPES.has(n)) {
    return <input type="number" placeholder="Numeric value" value={value ?? ""} onChange={(e) => onChange(e.target.value || null)} />;
  }
  if (TIMESTAMP_TYPES.has(n)) {
    return <input type="datetime-local" value={value ?? ""} onChange={(e) => onChange(e.target.value || null)} />;
  }
  if (DATE_TYPES.has(n)) {
    return <input type="date" value={value ?? ""} onChange={(e) => onChange(e.target.value || null)} />;
  }
  if (TIME_TYPES.has(n)) {
    return <input type="time" value={value ?? ""} onChange={(e) => onChange(e.target.value || null)} />;
  }
  if (BOOL_TYPES.has(n)) {
    return (
      <select value={value ?? ""} onChange={(e) => onChange(e.target.value || null)}>
        <option value="">— select —</option>
        <option value="true">true</option>
        <option value="false">false</option>
      </select>
    );
  }
  return <input placeholder="Literal value" value={value ?? ""} onChange={(e) => onChange(e.target.value || null)} />;
}

const EMPTY: ColumnPreset = { column: "", source: "now", name: null, value: null, dataType: null };

export function ColumnPresetsEditor({ presets, columns, columnTypes, onChange }: Props) {
  const [draft, setDraft] = useState<ColumnPreset>({ ...EMPTY });

  const remove = (i: number) => {
    const next = presets.filter((_, idx) => idx !== i);
    onChange(next);
  };

  const add = () => {
    if (!draft.column) return;
    onChange([...presets, { ...draft }]);
    setDraft({ ...EMPTY });
  };

  return (
    <div className="cp-editor">
      <div className="cp-editor-label">Column Presets</div>
      {presets.length > 0 && (
        <table className="cp-table">
          <thead>
            <tr>
              <th>Column</th>
              <th>Source</th>
              <th>Header / Value</th>
              <th></th>
            </tr>
          </thead>
          <tbody>
            {presets.map((p, i) => (
              <tr key={i}>
                <td>{p.column}</td>
                <td>{p.source}</td>
                <td>{p.source === "header" ? p.name : p.source === "literal" ? p.value : "—"}</td>
                <td>
                  <button className="cp-remove-btn" onClick={() => remove(i)} title="Remove">✕</button>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
      <div className="cp-add-row">
        <select
          value={draft.column}
          onChange={(e) => {
            const col = e.target.value;
            const colType = columnTypes?.[col] ?? null;
            const temporal = colType ? isTemporalType(colType) : true;
            const nextSource = draft.source === "now" && !temporal ? "literal" : draft.source;
            setDraft((d) => ({ ...d, column: col, source: nextSource as ColumnPreset["source"], name: null, value: null, dataType: colType }));
          }}
        >
          <option value="">— column —</option>
          {columns.map((c) => <option key={c} value={c}>{c}</option>)}
        </select>
        <select
          value={draft.source}
          onChange={(e) => setDraft((d) => ({ ...d, source: e.target.value as ColumnPreset["source"], name: null, value: null }))}
        >
          {SOURCES.filter((s) => {
            if (s.value === "now" && draft.column && columnTypes?.[draft.column]) {
              return isTemporalType(columnTypes[draft.column]);
            }
            return true;
          }).map((s) => <option key={s.value} value={s.value}>{s.label}</option>)}
        </select>
        {draft.source === "header" && (
          <input
            placeholder="Header name (e.g. X-User-Id)"
            value={draft.name ?? ""}
            onChange={(e) => setDraft((d) => ({ ...d, name: e.target.value || null }))}
          />
        )}
        {draft.source === "literal" && getLiteralInput(
          draft.column ? columnTypes?.[draft.column] : undefined,
          draft.value,
          (v) => setDraft((d) => ({ ...d, value: v }))
        )}
        <button onClick={add} disabled={!draft.column}>+</button>
      </div>
    </div>
  );
}
