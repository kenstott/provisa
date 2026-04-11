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
  onChange: (presets: ColumnPreset[]) => void;
}

const SOURCES: { value: ColumnPreset["source"]; label: string }[] = [
  { value: "now", label: "now (UTC timestamp)" },
  { value: "header", label: "header (HTTP request header)" },
  { value: "literal", label: "literal (fixed value)" },
];

const EMPTY: ColumnPreset = { column: "", source: "now", name: null, value: null };

export function ColumnPresetsEditor({ presets, columns, onChange }: Props) {
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
          onChange={(e) => setDraft((d) => ({ ...d, column: e.target.value }))}
        >
          <option value="">— column —</option>
          {columns.map((c) => <option key={c} value={c}>{c}</option>)}
        </select>
        <select
          value={draft.source}
          onChange={(e) => setDraft((d) => ({ ...d, source: e.target.value as ColumnPreset["source"], name: null, value: null }))}
        >
          {SOURCES.map((s) => <option key={s.value} value={s.value}>{s.label}</option>)}
        </select>
        {draft.source === "header" && (
          <input
            placeholder="Header name (e.g. X-User-Id)"
            value={draft.name ?? ""}
            onChange={(e) => setDraft((d) => ({ ...d, name: e.target.value || null }))}
          />
        )}
        {draft.source === "literal" && (
          <input
            placeholder="Literal value"
            value={draft.value ?? ""}
            onChange={(e) => setDraft((d) => ({ ...d, value: e.target.value || null }))}
          />
        )}
        <button onClick={add} disabled={!draft.column}>Add</button>
      </div>
    </div>
  );
}
