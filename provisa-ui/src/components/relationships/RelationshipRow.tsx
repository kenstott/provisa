// Copyright (c) 2026 Kenneth Stott
// Canary: 9c9d740c-8fca-47e8-ad1a-32409bac647d
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import React from "react";
import { Trash2, Pencil, Save, X, ArrowLeftRight } from "lucide-react";
import type { Relationship, RegisteredTable } from "../../types/admin";
import type { TrackedFunction } from "../../api/actions";
import type { RelForm } from "./relationship-types";

interface RelationshipRowProps {
  rel: Relationship;
  isExpanded: boolean;
  onToggle: () => void;
  editingRel: RelForm | null;
  setEditingRel: (f: RelForm | null) => void;
  canManage: boolean;
  onStartEdit: () => void;
  onReverse: () => void;
  onDelete: () => void;
  onEditSave: () => void;
  saving: string | null;
  tables: RegisteredTable[];
  functions: TrackedFunction[];
  tableDomainById: Record<string, string>;
  normalizeDomain: (id: string) => string;
}

export function RelationshipRow({
  rel: r,
  isExpanded,
  onToggle,
  editingRel,
  setEditingRel,
  canManage,
  onStartEdit,
  onReverse,
  onDelete,
  onEditSave,
  saving,
  tables,
  functions,
  tableDomainById,
  normalizeDomain,
}: RelationshipRowProps) {
  return (
    <React.Fragment>
      <tr
        onClick={onToggle}
        style={{
          cursor: "pointer",
          background: isExpanded ? "var(--surface)" : undefined,
        }}
      >
        <td>
          <div style={{ display: "flex", alignItems: "center", gap: "0.4rem", overflowWrap: "anywhere" }}>
            {r.autoSuggested && (
              <span
                title="Auto-tracked from FK constraint"
                style={{
                  fontSize: "0.65rem",
                  fontWeight: 600,
                  padding: "1px 5px",
                  borderRadius: 3,
                  background: "var(--text-muted)",
                  color: "var(--bg)",
                  letterSpacing: "0.03em",
                  flexShrink: 0,
                }}
              >
                FK
              </span>
            )}
            <span>{r.id.replace(/([_\-:])/g, "$1​")}</span>
          </div>
        </td>
        <td>{r.sourceDomainId || "—"}</td>
        <td style={{ wordBreak: "break-all", overflowWrap: "anywhere" }}>
          {r.sourceDomainId
            ? `${r.sourceDomainId}.${r.sourceTableName}.${r.sourceColumn}`
            : `${r.sourceTableName}.${r.sourceColumn}`}
        </td>
        <td style={{ wordBreak: "break-all", overflowWrap: "anywhere" }}>
          {r.targetFunctionName
            ? `fn:${r.targetFunctionName}(${r.functionArg ?? ""})`
            : tableDomainById[r.targetTableId!]
              ? `${tableDomainById[r.targetTableId!]}.${r.targetTableName}.${r.targetColumn}`
              : `${r.targetTableName}.${r.targetColumn}`}
        </td>
        <td>
          <div style={{ fontSize: "0.8rem", lineHeight: 1.4 }}>
            <div>
              <span style={{ color: "var(--text-muted)" }}>GQL:</span>{" "}
              <code>{r.graphqlAlias ?? "—"}</code>
            </div>
            <div>
              <span style={{ color: "var(--text-muted)" }}>CQL:</span>{" "}
              <code>
                {r.alias ?? (
                  <em style={{ color: "var(--text-muted)" }}>{r.computedCypherAlias ?? "—"}</em>
                )}
              </code>
            </div>
          </div>
        </td>
        <td>{r.cardinality}</td>
        <td>{r.materialize ? "Yes" : "No"}</td>
        <td>{r.materialize ? r.refreshInterval : "—"}</td>
      </tr>
      {isExpanded && (
        <tr>
          <td
            colSpan={8}
            style={{
              padding: "0.75rem 1rem",
              background: "var(--bg)",
              borderTop: "1px solid var(--border)",
            }}
          >
            {!editingRel ? (
              <div style={{ display: "flex", flexDirection: "column", gap: "0.5rem" }}>
                <dl
                  style={{
                    display: "grid",
                    gridTemplateColumns: "max-content 1fr",
                    gap: "0.25rem 1rem",
                    margin: 0,
                    color: "var(--text)",
                  }}
                >
                  <dt style={{ color: "var(--text-muted)" }}>
                    <strong>ID</strong>
                  </dt>
                  <dd style={{ color: "var(--text)", margin: 0 }}>
                    {r.id}
                    {r.autoSuggested && (
                      <span
                        title="Auto-tracked from FK constraint"
                        style={{
                          marginLeft: 6,
                          fontSize: "0.65rem",
                          fontWeight: 600,
                          padding: "1px 5px",
                          borderRadius: 3,
                          background: "var(--text-muted)",
                          color: "var(--bg)",
                        }}
                      >
                        FK
                      </span>
                    )}
                  </dd>
                  <dt style={{ color: "var(--text-muted)" }}>
                    <strong>Source</strong>
                  </dt>
                  <dd style={{ color: "var(--text)", margin: 0 }}>
                    {r.sourceDomainId
                      ? `${r.sourceDomainId}.${r.sourceTableName}.${r.sourceColumn}`
                      : `${r.sourceTableName}.${r.sourceColumn}`}
                  </dd>
                  <dt style={{ color: "var(--text-muted)" }}>
                    <strong>Target</strong>
                  </dt>
                  <dd style={{ color: "var(--text)", margin: 0 }}>
                    {r.targetFunctionName
                      ? `fn:${r.targetFunctionName}(${r.functionArg ?? ""})`
                      : tableDomainById[r.targetTableId!]
                        ? `${tableDomainById[r.targetTableId!]}.${r.targetTableName}.${r.targetColumn}`
                        : `${r.targetTableName}.${r.targetColumn}`}
                  </dd>
                  <dt style={{ color: "var(--text-muted)" }}>
                    <strong>GQL Alias</strong>
                  </dt>
                  <dd style={{ color: "var(--text)", margin: 0 }}>
                    <code>{r.graphqlAlias ?? "—"}</code>
                  </dd>
                  <dt style={{ color: "var(--text-muted)" }}>
                    <strong>CQL Alias</strong>
                  </dt>
                  <dd style={{ color: "var(--text)", margin: 0 }}>
                    <code>
                      {r.alias ?? (
                        <em style={{ color: "var(--text-muted)" }}>{r.computedCypherAlias ?? "—"}</em>
                      )}
                    </code>
                  </dd>
                  <dt style={{ color: "var(--text-muted)" }}>
                    <strong>Cardinality</strong>
                  </dt>
                  <dd style={{ color: "var(--text)", margin: 0 }}>{r.cardinality}</dd>
                  <dt style={{ color: "var(--text-muted)" }}>
                    <strong>Materialize</strong>
                  </dt>
                  <dd style={{ color: "var(--text)", margin: 0 }}>{r.materialize ? "Yes" : "No"}</dd>
                  {r.materialize && (
                    <>
                      <dt style={{ color: "var(--text-muted)" }}>
                        <strong>Refresh Interval (s)</strong>
                      </dt>
                      <dd style={{ color: "var(--text)", margin: 0 }}>{r.refreshInterval ?? "—"}</dd>
                    </>
                  )}
                  <dt style={{ color: "var(--text-muted)" }}>
                    <strong>Cypher Graph</strong>
                  </dt>
                  <dd style={{ color: "var(--text)", margin: 0 }}>
                    {r.disableCypher ? (
                      <em style={{ color: "var(--text-muted)" }}>excluded</em>
                    ) : (
                      "included"
                    )}
                  </dd>
                </dl>
                {canManage && (
                  <div style={{ display: "flex", gap: "0.5rem", marginTop: "0.25rem" }}>
                    <button
                      className="btn-icon"
                      title="Edit"
                      onClick={(e) => {
                        e.stopPropagation();
                        onStartEdit();
                      }}
                    >
                      <Pencil size={14} />
                    </button>
                    <button
                      className="btn-icon"
                      title="Generate reverse relationship"
                      onClick={(e) => {
                        e.stopPropagation();
                        onReverse();
                      }}
                    >
                      <ArrowLeftRight size={14} />
                    </button>
                    <button
                      className="btn-icon-danger"
                      title="Delete"
                      onClick={(e) => {
                        e.stopPropagation();
                        onDelete();
                      }}
                    >
                      <Trash2 size={14} />
                    </button>
                  </div>
                )}
              </div>
            ) : (
              <div style={{ display: "flex", flexDirection: "column", gap: "0.75rem" }}>
                <div className="form-row">
                  <label>
                    Name
                    <input
                      value={editingRel.id}
                      onChange={(e) => setEditingRel({ ...editingRel, id: e.target.value })}
                    />
                  </label>
                  <label>
                    CQL Alias (UPPER_SNAKE)
                    <input
                      value={editingRel.alias}
                      onChange={(e) => setEditingRel({ ...editingRel, alias: e.target.value })}
                      placeholder={r.computedCypherAlias ?? "PLACED_BY"}
                    />
                  </label>
                  <label>
                    GQL Alias (camelCase)
                    <input
                      value={editingRel.graphqlAlias}
                      onChange={(e) => setEditingRel({ ...editingRel, graphqlAlias: e.target.value })}
                      placeholder={r.graphqlAlias ?? ""}
                    />
                  </label>
                </div>
                <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: "1rem" }}>
                  {/* Source panel */}
                  {(() => {
                    const uniqueDomains = [
                      ...new Set(tables.map((t) => normalizeDomain(t.domainId)).filter(Boolean)),
                    ].sort();
                    const filteredSrcTables = editingRel.sourceDomain
                      ? tables.filter((t) => normalizeDomain(t.domainId) === editingRel.sourceDomain)
                      : tables;
                    return (
                      <div
                        style={{
                          border: "1px solid var(--border)",
                          borderRadius: "4px",
                          padding: "0.75rem",
                          display: "flex",
                          flexDirection: "column",
                          gap: "0.5rem",
                        }}
                      >
                        <strong
                          style={{
                            color: "var(--text-muted)",
                            fontSize: "0.75rem",
                            textTransform: "uppercase" as const,
                          }}
                        >
                          Source
                        </strong>
                        <label>
                          Domain
                          <select
                            value={editingRel.sourceDomain}
                            onChange={(e) =>
                              setEditingRel({
                                ...editingRel,
                                sourceDomain: e.target.value,
                                sourceTableId: "",
                              })
                            }
                          >
                            <option value="">All</option>
                            {uniqueDomains.map((d) => (
                              <option key={d} value={d}>
                                {d}
                              </option>
                            ))}
                          </select>
                        </label>
                        <label>
                          Table
                          <select
                            value={editingRel.sourceTableId}
                            onChange={(e) =>
                              setEditingRel({ ...editingRel, sourceTableId: e.target.value })
                            }
                          >
                            <option value="">Select...</option>
                            {filteredSrcTables.map((t) => (
                              <option key={t.id} value={t.tableName}>
                                {t.tableName}
                              </option>
                            ))}
                          </select>
                        </label>
                        <label>
                          Column
                          <input
                            value={editingRel.sourceColumn}
                            onChange={(e) =>
                              setEditingRel({ ...editingRel, sourceColumn: e.target.value })
                            }
                          />
                        </label>
                      </div>
                    );
                  })()}
                  {/* Target panel */}
                  <div
                    style={{
                      border: "1px solid var(--border)",
                      borderRadius: "4px",
                      padding: "0.75rem",
                      display: "flex",
                      flexDirection: "column",
                      gap: "0.5rem",
                    }}
                  >
                    <strong
                      style={{
                        color: "var(--text-muted)",
                        fontSize: "0.75rem",
                        textTransform: "uppercase" as const,
                      }}
                    >
                      Target
                    </strong>
                    <label>
                      Type
                      <select
                        value={editingRel.targetType}
                        onChange={(e) =>
                          setEditingRel({
                            ...editingRel,
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
                    {editingRel.targetType === "table" ? (
                      (() => {
                        const uniqueDomains = [
                          ...new Set(tables.map((t) => normalizeDomain(t.domainId)).filter(Boolean)),
                        ].sort();
                        const filteredTgtTables = editingRel.targetDomain
                          ? tables.filter(
                              (t) => normalizeDomain(t.domainId) === editingRel.targetDomain,
                            )
                          : tables;
                        return (
                          <>
                            <label>
                              Domain
                              <select
                                value={editingRel.targetDomain}
                                onChange={(e) =>
                                  setEditingRel({
                                    ...editingRel,
                                    targetDomain: e.target.value,
                                    targetTableId: "",
                                  })
                                }
                              >
                                <option value="">All</option>
                                {uniqueDomains.map((d) => (
                                  <option key={d} value={d}>
                                    {d}
                                  </option>
                                ))}
                              </select>
                            </label>
                            <label>
                              Table
                              <select
                                value={editingRel.targetTableId}
                                onChange={(e) =>
                                  setEditingRel({ ...editingRel, targetTableId: e.target.value })
                                }
                              >
                                <option value="">Select...</option>
                                {filteredTgtTables.map((t) => (
                                  <option key={t.id} value={t.tableName}>
                                    {t.tableName}
                                  </option>
                                ))}
                              </select>
                            </label>
                            <label>
                              Column
                              <input
                                value={editingRel.targetColumn}
                                onChange={(e) =>
                                  setEditingRel({ ...editingRel, targetColumn: e.target.value })
                                }
                              />
                            </label>
                          </>
                        );
                      })()
                    ) : (
                      <>
                        <label>
                          Function
                          <select
                            value={editingRel.targetFunctionName}
                            onChange={(e) =>
                              setEditingRel({ ...editingRel, targetFunctionName: e.target.value })
                            }
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
                            value={editingRel.functionArg}
                            onChange={(e) =>
                              setEditingRel({ ...editingRel, functionArg: e.target.value })
                            }
                            placeholder="arg name"
                          />
                        </label>
                      </>
                    )}
                  </div>
                </div>
                <div className="form-row">
                  {editingRel.targetType === "table" && (
                    <label style={{ flex: "0 0 auto" }}>
                      Cardinality
                      <select
                        value={editingRel.cardinality}
                        onChange={(e) =>
                          setEditingRel({ ...editingRel, cardinality: e.target.value })
                        }
                        style={{ width: `${editingRel.cardinality.length + 4}ch` }}
                      >
                        <option value="many-to-one">many-to-one</option>
                        <option value="one-to-many">one-to-many</option>
                      </select>
                    </label>
                  )}
                  <label
                    style={{
                      flexDirection: "row",
                      alignItems: "center",
                      gap: "0.5rem",
                      whiteSpace: "nowrap",
                      flex: "0 0 auto",
                    }}
                  >
                    <input
                      type="checkbox"
                      checked={editingRel.materialize}
                      onChange={(e) => setEditingRel({ ...editingRel, materialize: e.target.checked })}
                      style={{ width: "auto", padding: 0 }}
                    />
                    Materialize
                  </label>
                  <label
                    style={{
                      flexDirection: "row",
                      alignItems: "center",
                      gap: "0.5rem",
                      whiteSpace: "nowrap",
                      flex: "0 0 auto",
                    }}
                  >
                    <input
                      type="checkbox"
                      checked={editingRel.disableCypher}
                      onChange={(e) =>
                        setEditingRel({ ...editingRel, disableCypher: e.target.checked })
                      }
                      style={{ width: "auto", padding: 0 }}
                    />
                    Exclude from Cypher
                  </label>
                  {editingRel.materialize && (
                    <label>
                      Refresh Interval (s)
                      <input
                        type="number"
                        value={editingRel.refreshInterval}
                        onChange={(e) =>
                          setEditingRel({ ...editingRel, refreshInterval: e.target.value })
                        }
                      />
                    </label>
                  )}
                </div>
                {editingRel.targetType === "table" && editingRel.cardinality === "many-to-one" && (
                  <span
                    style={{
                      color: "var(--warning, #b45309)",
                      fontSize: "0.78rem",
                      display: "block",
                    }}
                  >
                    Warning: if this join returns more than one row per parent, only the first value will be used.
                  </span>
                )}
                <div style={{ display: "flex", gap: "0.5rem", justifyContent: "flex-end" }}>
                  <button className="btn-icon" title="Cancel" onClick={() => setEditingRel(null)}>
                    <X size={14} />
                  </button>
                  <button
                    className="btn-icon-primary"
                    title="Save"
                    onClick={onEditSave}
                    disabled={!!saving}
                  >
                    <Save size={14} />
                  </button>
                </div>
              </div>
            )}
          </td>
        </tr>
      )}
    </React.Fragment>
  );
}
