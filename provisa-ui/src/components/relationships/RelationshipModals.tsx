// Copyright (c) 2026 Kenneth Stott
// Canary: ee9310b9-1f41-4d77-8c12-3a724024d5b8
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { X } from "lucide-react";
import type { Relationship } from "../../types/admin";
import type { RelForm } from "./relationship-types";

interface ConflictModalProps {
  rel: Relationship;
  onClose: () => void;
}

export function ConflictModal({ rel, onClose }: ConflictModalProps) {
  return (
    <div className="modal-overlay" onClick={onClose}>
      <div
        className="modal"
        style={{ width: "500px", maxWidth: "500px" }}
        onClick={(e) => e.stopPropagation()}
      >
        <div className="modal-header">
          <h3>Relationship Already Exists</h3>
          <button className="modal-close" onClick={onClose}>
            <X size={14} />
          </button>
        </div>
        <div className="form-card" style={{ display: "flex", flexDirection: "column", gap: "0.75rem" }}>
          <p style={{ margin: 0, fontSize: "0.85rem", color: "var(--text-muted)" }}>
            A relationship with this source and target already exists:
          </p>
          <dl
            style={{
              margin: 0,
              display: "grid",
              gridTemplateColumns: "auto 1fr",
              gap: "0.35rem 1rem",
              fontSize: "0.82rem",
            }}
          >
            <dt style={{ color: "var(--text-muted)", fontWeight: 600 }}>ID</dt>
            <dd style={{ margin: 0, fontFamily: "monospace" }}>{rel.id}</dd>
            <dt style={{ color: "var(--text-muted)", fontWeight: 600 }}>Source</dt>
            <dd style={{ margin: 0, fontFamily: "monospace" }}>
              {rel.sourceTableName}.{rel.sourceColumn}
            </dd>
            <dt style={{ color: "var(--text-muted)", fontWeight: 600 }}>Target</dt>
            <dd style={{ margin: 0, fontFamily: "monospace" }}>
              {rel.targetTableName}.{rel.targetColumn}
            </dd>
            <dt style={{ color: "var(--text-muted)", fontWeight: 600 }}>Cardinality</dt>
            <dd style={{ margin: 0 }}>{rel.cardinality}</dd>
            {rel.alias && (
              <>
                <dt style={{ color: "var(--text-muted)", fontWeight: 600 }}>Alias</dt>
                <dd style={{ margin: 0, fontFamily: "monospace" }}>{rel.alias}</dd>
              </>
            )}
          </dl>
          <div style={{ display: "flex", justifyContent: "flex-end" }}>
            <button className="btn-secondary" onClick={onClose}>
              Close
            </button>
          </div>
        </div>
      </div>
    </div>
  );
}

interface ReverseRelationshipModalProps {
  reverseForm: RelForm;
  setReverseForm: (f: RelForm | null) => void;
  saving: string | null;
  onSave: () => void;
}

export function ReverseRelationshipModal({
  reverseForm,
  setReverseForm,
  saving,
  onSave,
}: ReverseRelationshipModalProps) {
  return (
    <div className="modal-overlay" onClick={() => setReverseForm(null)}>
      <div
        className="modal"
        style={{ width: "730px", maxWidth: "730px" }}
        onClick={(e) => e.stopPropagation()}
      >
        <div className="modal-header">
          <h3>Generate Reverse Relationship</h3>
          <button className="modal-close" onClick={() => setReverseForm(null)}>
            <X size={14} />
          </button>
        </div>
        <div className="form-card">
          <div className="form-row">
            <label>
              ID
              <input
                value={reverseForm.id}
                onChange={(e) => setReverseForm({ ...reverseForm, id: e.target.value })}
              />
            </label>
            <label>
              CQL Alias (UPPER_SNAKE)
              <input
                value={reverseForm.alias}
                onChange={(e) => setReverseForm({ ...reverseForm, alias: e.target.value })}
                placeholder="PLACED_BY"
              />
            </label>
            <label>
              GQL Alias (camelCase)
              <input
                value={reverseForm.graphqlAlias}
                onChange={(e) => setReverseForm({ ...reverseForm, graphqlAlias: e.target.value })}
              />
            </label>
            <label className="checkbox-label">
              <input
                type="checkbox"
                checked={reverseForm.materialize}
                onChange={(e) => setReverseForm({ ...reverseForm, materialize: e.target.checked })}
              />
              Materialize
            </label>
            {reverseForm.materialize && (
              <label>
                Refresh Interval (s)
                <input
                  type="number"
                  value={reverseForm.refreshInterval}
                  onChange={(e) =>
                    setReverseForm({ ...reverseForm, refreshInterval: e.target.value })
                  }
                />
              </label>
            )}
          </div>
        </div>
        <div className="modal-actions">
          <button className="btn-secondary" onClick={() => setReverseForm(null)}>
            ✕
          </button>
          <button className="btn-primary" onClick={onSave} disabled={saving === "reverse"}>
            Save
          </button>
        </div>
      </div>
    </div>
  );
}
