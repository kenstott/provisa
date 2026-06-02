// Copyright (c) 2026 Kenneth Stott
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import type { RegisteredTable } from "../../types/admin";
import type { TrackedFunction } from "../../api/actions";
import type { RelForm } from "./relationship-types";

interface AddRelationshipFormProps {
  form: RelForm;
  setForm: (f: RelForm) => void;
  tables: RegisteredTable[];
  functions: TrackedFunction[];
  saving: string | null;
  onSave: () => void;
}

export function AddRelationshipForm({
  form,
  setForm,
  tables,
  functions,
  saving,
  onSave,
}: AddRelationshipFormProps) {
  return (
    <div className="form-card">
      <div className="form-row">
        <label>
          ID
          <input
            value={form.id}
            onChange={(e) => setForm({ ...form, id: e.target.value })}
            placeholder="orders-to-customers"
          />
        </label>
        <label>
          CQL Alias (UPPER_SNAKE)
          <input
            value={form.alias}
            onChange={(e) => setForm({ ...form, alias: e.target.value })}
            placeholder="PLACED_BY"
          />
        </label>
        <label>
          GQL Alias (camelCase)
          <input
            value={form.graphqlAlias}
            onChange={(e) => setForm({ ...form, graphqlAlias: e.target.value })}
            placeholder="e.g. orders"
          />
        </label>
        <label>
          Source Table
          <select
            value={form.sourceTableId}
            onChange={(e) => setForm({ ...form, sourceTableId: e.target.value })}
          >
            <option value="">Select...</option>
            {tables.map((t) => (
              <option key={t.id} value={t.tableName}>
                {t.tableName}
              </option>
            ))}
          </select>
        </label>
        <label>
          Source Column
          <input
            value={form.sourceColumn}
            onChange={(e) => setForm({ ...form, sourceColumn: e.target.value })}
            placeholder="customer_id"
          />
        </label>
      </div>
      <div className="form-row">
        <label>
          Target Type
          <select
            value={form.targetType}
            onChange={(e) =>
              setForm({
                ...form,
                targetType: e.target.value as "table" | "function",
                targetTableId: "",
                targetColumn: "",
                targetFunctionName: "",
                functionArg: "",
              })
            }
          >
            <option value="table">Table</option>
            <option value="function">Function (computed)</option>
          </select>
        </label>
        {form.targetType === "table" ? (
          <>
            <label>
              Target Table
              <select
                value={form.targetTableId}
                onChange={(e) => setForm({ ...form, targetTableId: e.target.value })}
              >
                <option value="">Select...</option>
                {tables.map((t) => (
                  <option key={t.id} value={t.tableName}>
                    {t.tableName}
                  </option>
                ))}
              </select>
            </label>
            <label>
              Target Column
              <input
                value={form.targetColumn}
                onChange={(e) => setForm({ ...form, targetColumn: e.target.value })}
                placeholder="id"
              />
            </label>
          </>
        ) : (
          <>
            <label>
              Function
              <select
                value={form.targetFunctionName}
                onChange={(e) => setForm({ ...form, targetFunctionName: e.target.value })}
              >
                <option value="">Select...</option>
                {functions.map((f) => (
                  <option key={f.name} value={f.name}>
                    {f.name}
                  </option>
                ))}
              </select>
            </label>
            <label>
              Function Arg (receives source column)
              <input
                value={form.functionArg}
                onChange={(e) => setForm({ ...form, functionArg: e.target.value })}
                placeholder="arg name"
              />
            </label>
          </>
        )}
      </div>
      <div className="form-row">
        {form.targetType === "table" && (
          <label>
            Cardinality
            <select
              value={form.cardinality}
              onChange={(e) => setForm({ ...form, cardinality: e.target.value })}
            >
              <option value="many-to-one">many-to-one</option>
              <option value="one-to-many">one-to-many</option>
            </select>
            {form.cardinality === "many-to-one" && (
              <span
                style={{
                  color: "var(--warning, #b45309)",
                  fontSize: "0.78rem",
                  marginTop: "0.25rem",
                  display: "block",
                }}
              >
                Warning: if this join returns more than one row per parent, only the first value
                will be used.
              </span>
            )}
          </label>
        )}
      </div>
      <div className="form-row">
        <label className="checkbox-label">
          <input
            type="checkbox"
            checked={form.materialize}
            onChange={(e) => setForm({ ...form, materialize: e.target.checked })}
          />
          Materialize (auto-create MV for cross-source joins)
        </label>
        <label className="checkbox-label">
          <input
            type="checkbox"
            checked={form.disableCypher}
            onChange={(e) => setForm({ ...form, disableCypher: e.target.checked })}
          />
          Exclude from Cypher graph
        </label>
        {form.materialize && (
          <label>
            Refresh Interval (s)
            <input
              type="number"
              value={form.refreshInterval}
              onChange={(e) => setForm({ ...form, refreshInterval: e.target.value })}
            />
          </label>
        )}
        <button className="btn-primary" onClick={onSave} disabled={saving === "new"}>
          Save
        </button>
      </div>
    </div>
  );
}
