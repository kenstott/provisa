// Copyright (c) 2025 Kenneth Stott
// Canary: a03f3a03-b6a1-4788-99c9-ce711ccd8264
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { useState } from "react";
import { discoverSourceSchema, registerTable } from "../api/admin";
import type { DiscoveredColumn } from "../api/admin";

const TRINO_TYPES = [
  "VARCHAR", "INTEGER", "BIGINT", "SMALLINT", "TINYINT",
  "DOUBLE", "REAL", "DECIMAL", "BOOLEAN", "DATE",
  "TIMESTAMP", "VARBINARY", "JSON",
];

interface ColumnRow {
  selected: boolean;
  name: string;
  type: string;
  alias: string;
  description: string;
  sourcePath: string;
}

interface SchemaDiscoveryProps {
  sourceId: string;
  sourceType: string;
  onClose: () => void;
  onRegistered?: () => void;
}

function toColumnRows(cols: DiscoveredColumn[]): ColumnRow[] {
  return cols.map((c) => ({
    selected: true,
    name: c.name,
    type: c.type,
    alias: "",
    description: c.description,
    sourcePath: c.source_path,
  }));
}

/** Hint fields vary by source type. */
function DiscoverHints({
  sourceType,
  hints,
  setHints,
}: {
  sourceType: string;
  hints: Record<string, string>;
  setHints: (h: Record<string, string>) => void;
}) {
  if (sourceType === "mongodb") {
    return (
      <label>
        Collection
        <input
          value={hints.collection ?? ""}
          onChange={(e) => setHints({ ...hints, collection: e.target.value })}
          placeholder="e.g. users"
        />
      </label>
    );
  }
  if (sourceType === "elasticsearch") {
    return (
      <label>
        Index Pattern
        <input
          value={hints.index ?? ""}
          onChange={(e) => setHints({ ...hints, index: e.target.value })}
          placeholder="e.g. nginx-access-*"
        />
      </label>
    );
  }
  if (sourceType === "cassandra") {
    return (
      <>
        <label>
          Keyspace
          <input
            value={hints.keyspace ?? ""}
            onChange={(e) => setHints({ ...hints, keyspace: e.target.value })}
            placeholder="e.g. analytics"
          />
        </label>
        <label>
          Table
          <input
            value={hints.table ?? ""}
            onChange={(e) => setHints({ ...hints, table: e.target.value })}
            placeholder="e.g. user_events"
          />
        </label>
      </>
    );
  }
  if (sourceType === "prometheus") {
    return (
      <label>
        Metric Name
        <input
          value={hints.metric ?? ""}
          onChange={(e) => setHints({ ...hints, metric: e.target.value })}
          placeholder="e.g. http_request_duration_seconds"
        />
      </label>
    );
  }
  return null;
}

export function SchemaDiscovery({ sourceId, sourceType, onClose, onRegistered }: SchemaDiscoveryProps) {
  const [columns, setColumns] = useState<ColumnRow[]>([]);
  const [hints, setHints] = useState<Record<string, string>>({});
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [regForm, setRegForm] = useState({
    domainId: "",
    schemaName: "default",
    tableName: "",
    governance: "open",
  });
  const [registering, setRegistering] = useState(false);

  const handleDiscover = async () => {
    setLoading(true);
    setError(null);
    try {
      const resp = await discoverSourceSchema(sourceId, hints);
      setColumns(toColumnRows(resp.columns));
      if (resp.columns.length === 0) {
        setError("No columns discovered. The source may need live connection data, or you can add columns manually.");
      }
    } catch (e: any) {
      setError(e.message);
    } finally {
      setLoading(false);
    }
  };

  const toggleAll = (checked: boolean) => {
    setColumns(columns.map((c) => ({ ...c, selected: checked })));
  };

  const updateColumn = (idx: number, patch: Partial<ColumnRow>) => {
    setColumns(columns.map((c, i) => (i === idx ? { ...c, ...patch } : c)));
  };

  const addManualColumn = () => {
    setColumns([...columns, {
      selected: true,
      name: "",
      type: "VARCHAR",
      alias: "",
      description: "",
      sourcePath: "",
    }]);
  };

  const removeColumn = (idx: number) => {
    setColumns(columns.filter((_, i) => i !== idx));
  };

  const handleRegister = async () => {
    const selected = columns.filter((c) => c.selected && c.name.trim());
    if (selected.length === 0) {
      setError("Select at least one column with a name");
      return;
    }
    if (!regForm.domainId.trim() || !regForm.tableName.trim()) {
      setError("Domain ID and Table Name are required");
      return;
    }
    setRegistering(true);
    setError(null);
    try {
      const result = await registerTable({
        sourceId,
        domainId: regForm.domainId,
        schemaName: regForm.schemaName,
        tableName: regForm.tableName,
        governance: regForm.governance,
        columns: selected.map((c) => ({
          name: c.alias || c.name,
          visibleTo: ["*"],
          alias: c.alias || undefined,
          description: c.description || undefined,
        })),
      });
      if (!result.success) throw new Error(result.message);
      onRegistered?.();
      onClose();
    } catch (e: any) {
      setError(e.message);
    } finally {
      setRegistering(false);
    }
  };

  const allSelected = columns.length > 0 && columns.every((c) => c.selected);

  return (
    <div className="form-card" style={{ marginTop: "1rem" }}>
      <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center" }}>
        <h3 style={{ margin: 0 }}>Schema Discovery: {sourceId}</h3>
        <button onClick={onClose} style={{ padding: "0.25rem 0.5rem" }}>Close</button>
      </div>

      {error && <div className="error">{error}</div>}

      <div style={{ display: "flex", gap: "0.5rem", flexWrap: "wrap", alignItems: "flex-end", marginTop: "0.5rem" }}>
        <DiscoverHints sourceType={sourceType} hints={hints} setHints={setHints} />
        <button onClick={handleDiscover} disabled={loading}>
          {loading ? "Discovering..." : "Discover Schema"}
        </button>
        <button onClick={addManualColumn} style={{ fontSize: "0.85rem" }}>
          + Add Column
        </button>
      </div>

      {columns.length > 0 && (
        <>
          <table className="data-table" style={{ marginTop: "0.75rem" }}>
            <thead>
              <tr>
                <th>
                  <input
                    type="checkbox"
                    checked={allSelected}
                    onChange={(e) => toggleAll(e.target.checked)}
                  />
                </th>
                <th>Name</th>
                <th>Type</th>
                <th>Alias</th>
                <th>Description</th>
                <th>Source Path</th>
                <th></th>
              </tr>
            </thead>
            <tbody>
              {columns.map((col, idx) => (
                <tr key={idx} style={{ opacity: col.selected ? 1 : 0.5 }}>
                  <td>
                    <input
                      type="checkbox"
                      checked={col.selected}
                      onChange={(e) => updateColumn(idx, { selected: e.target.checked })}
                    />
                  </td>
                  <td>
                    <input
                      value={col.name}
                      onChange={(e) => updateColumn(idx, { name: e.target.value })}
                      style={{ width: "8rem" }}
                    />
                  </td>
                  <td>
                    <select
                      value={col.type}
                      onChange={(e) => updateColumn(idx, { type: e.target.value })}
                    >
                      {TRINO_TYPES.map((t) => (
                        <option key={t} value={t}>{t}</option>
                      ))}
                    </select>
                  </td>
                  <td>
                    <input
                      value={col.alias}
                      onChange={(e) => updateColumn(idx, { alias: e.target.value })}
                      placeholder="optional"
                      style={{ width: "7rem" }}
                    />
                  </td>
                  <td>
                    <input
                      value={col.description}
                      onChange={(e) => updateColumn(idx, { description: e.target.value })}
                      placeholder="optional"
                      style={{ width: "10rem" }}
                    />
                  </td>
                  <td style={{ color: "var(--text-muted)", fontSize: "0.8rem" }}>
                    {col.sourcePath}
                  </td>
                  <td>
                    <button
                      onClick={() => removeColumn(idx)}
                      style={{ padding: "0.15rem 0.4rem", fontSize: "0.75rem" }}
                      className="destructive"
                    >
                      X
                    </button>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>

          <h4 style={{ marginTop: "1rem", marginBottom: "0.25rem" }}>Register Table</h4>
          <div style={{ display: "flex", gap: "0.5rem", flexWrap: "wrap", alignItems: "flex-end" }}>
            <label>
              Domain ID
              <input
                required
                value={regForm.domainId}
                onChange={(e) => setRegForm({ ...regForm, domainId: e.target.value })}
                placeholder="e.g. analytics"
              />
            </label>
            <label>
              Schema
              <input
                value={regForm.schemaName}
                onChange={(e) => setRegForm({ ...regForm, schemaName: e.target.value })}
              />
            </label>
            <label>
              Table Name
              <input
                required
                value={regForm.tableName}
                onChange={(e) => setRegForm({ ...regForm, tableName: e.target.value })}
                placeholder="e.g. user_events"
              />
            </label>
            <label>
              Governance
              <select
                value={regForm.governance}
                onChange={(e) => setRegForm({ ...regForm, governance: e.target.value })}
              >
                <option value="open">open</option>
                <option value="restricted">restricted</option>
                <option value="confidential">confidential</option>
              </select>
            </label>
            <button onClick={handleRegister} disabled={registering}>
              {registering ? "Registering..." : "Register Table"}
            </button>
          </div>
        </>
      )}
    </div>
  );
}
