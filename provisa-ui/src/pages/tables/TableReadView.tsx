// Copyright (c) 2026 Kenneth Stott
// Canary: 2b6d9e47-3f1a-4c8b-a5e2-7d0f4c9b2e65
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { Fragment } from "react";
import { Trash2, Pencil } from "lucide-react";
import type { NavigateFunction } from "react-router-dom";
import type { RegisteredTable } from "../../types/admin";
import { computeProfile } from "./helpers";

interface TableProfileResult {
  columns: string[];
  rows: Record<string, unknown>[];
  rowCount: number;
}

interface TableReadViewProps {
  t: RegisteredTable;
  navigate: NavigateFunction;
  viewsOnly: boolean;
  deploying: Record<number, boolean>;
  setDeploying: React.Dispatch<React.SetStateAction<Record<number, boolean>>>;
  deployMsg: Record<number, { success: boolean; message: string }>;
  setDeployMsg: React.Dispatch<
    React.SetStateAction<Record<number, { success: boolean; message: string }>>
  >;
  tableProfiles: Record<number, TableProfileResult | "loading" | string>;
  deployViewToDb: (id: number) => Promise<{ success: boolean; message: string }>;
  reload: () => void;
  startEditing: (t: RegisteredTable) => void;
  handleDelete: (id: number) => void;
  handleProfile: (id: number) => void;
}

export function TableReadView({
  t,
  navigate,
  viewsOnly,
  deploying,
  setDeploying,
  deployMsg,
  setDeployMsg,
  tableProfiles,
  deployViewToDb,
  reload,
  startEditing,
  handleDelete,
  handleProfile,
}: TableReadViewProps) {
  return (
    <>
      {t.description && (
        <div
          style={{
            padding: "0.5rem 0.75rem",
            fontSize: "0.85rem",
            color: "var(--text-muted)",
            borderBottom: "1px solid var(--border)",
          }}
        >
          {t.description}
        </div>
      )}
      <table className="data-table" style={{ margin: 0 }}>
        <thead>
          <tr>
            <th>Column</th>
            <th>PK</th>
            <th>SQL Alias</th>
            <th>Description</th>
            <th>Visible To (Read)</th>
            <th>Writable By (R/W)</th>
            <th>Masking</th>
            <th>Scope</th>
          </tr>
        </thead>
        <tbody>
          {t.columns.map((c) => (
            <Fragment key={c.id}>
              <tr>
                <td>
                  <code>{c.columnName}</code>
                  {c.nativeFilterType && (
                    <span
                      style={{
                        marginLeft: "0.4rem",
                        fontSize: "0.65rem",
                        padding: "0.1rem 0.35rem",
                        borderRadius: "0.25rem",
                        background:
                          c.nativeFilterType === "path_param"
                            ? "hsl(var(--color-warning) / 0.2)"
                            : "hsl(var(--color-info) / 0.2)",
                        color:
                          c.nativeFilterType === "path_param"
                            ? "hsl(var(--color-warning))"
                            : "hsl(var(--color-info))",
                        fontFamily: "monospace",
                      }}
                    >
                      {c.nativeFilterType === "path_param" ? "path" : "query"}
                    </span>
                  )}
                  {c.isForeignKey && (
                    <span
                      style={{
                        marginLeft: "0.4rem",
                        fontSize: "0.65rem",
                        padding: "0.1rem 0.35rem",
                        borderRadius: "0.25rem",
                        background: "hsl(var(--color-success) / 0.2)",
                        color: "hsl(var(--color-success))",
                        fontFamily: "monospace",
                      }}
                    >
                      FK
                    </span>
                  )}
                  {c.isAlternateKey && (
                    <span
                      style={{
                        marginLeft: "0.4rem",
                        fontSize: "0.65rem",
                        padding: "0.1rem 0.35rem",
                        borderRadius: "0.25rem",
                        background: "hsl(var(--color-warning) / 0.2)",
                        color: "hsl(var(--color-warning))",
                        fontFamily: "monospace",
                      }}
                    >
                      AK
                    </span>
                  )}
                </td>
                <td style={{ textAlign: "center" }}>
                  {c.isPrimaryKey && (
                    <span style={{ color: "hsl(var(--color-info))" }}>&#10003;</span>
                  )}
                </td>
                <td style={{ color: c.alias ? "white" : "var(--text-muted)" }}>
                  {c.computedSqlAlias}
                </td>
                <td className="reasoning-cell">{c.description || ""}</td>
                <td>{c.visibleTo.length > 0 ? c.visibleTo.join(", ") : "all"}</td>
                <td>{c.writableBy.length > 0 ? c.writableBy.join(", ") : "none"}</td>
                <td>{c.maskType || "none"}</td>
                <td>{c.scope || "domain"}</td>
              </tr>
              {c.maskType && (
                <tr>
                  <td
                    colSpan={2}
                    style={{
                      color: "var(--text-muted)",
                      fontSize: "0.75rem",
                      paddingLeft: "1.5rem",
                    }}
                  >
                    ↳{" "}
                    {c.maskType === "regex"
                      ? `/${c.maskPattern}/ → ${c.maskReplace}`
                      : c.maskType === "constant"
                        ? `= ${c.maskValue ?? "NULL"}`
                        : `truncate(${c.maskPrecision})`}
                  </td>
                  <td
                    colSpan={4}
                    style={{
                      color: "var(--text-muted)",
                      fontSize: "0.75rem",
                    }}
                  >
                    unmasked:{" "}
                    {c.unmaskedTo.length > 0 ? c.unmaskedTo.join(", ") : "none"}
                  </td>
                </tr>
              )}
            </Fragment>
          ))}
        </tbody>
      </table>
      {t.apiEndpoint && (
        <div
          style={{
            padding: "0.5rem 0.75rem",
            fontSize: "0.85rem",
            color: "var(--text-muted)",
          }}
        >
          API endpoint: <code>{t.apiEndpoint}</code>
        </div>
      )}
      {t.watermarkColumn && (
        <div
          style={{
            padding: "0.5rem 0.75rem",
            fontSize: "0.85rem",
            color: "var(--text-muted)",
          }}
        >
          Watermark column: <code>{t.watermarkColumn}</code>
        </div>
      )}
      {t.viewSql && (
        <div style={{ padding: "0.5rem 0.75rem", fontSize: "0.85rem" }}>
          <span style={{ color: "var(--text-muted)", marginRight: "0.5rem" }}>View SQL:</span>
          <code style={{ fontSize: "0.78rem", wordBreak: "break-all" }}>
            {t.viewSql.length > 120 ? t.viewSql.slice(0, 120) + "…" : t.viewSql}
          </code>
        </div>
      )}
      <div
        style={{
          padding: "0.5rem 0.75rem",
          fontSize: "0.85rem",
          display: "flex",
          alignItems: "center",
          gap: "0.4rem",
        }}
      >
        <span style={{ color: "var(--text-muted)" }}>Data Product:</span>
        {t.dataProduct ? (
          <span
            style={{
              color: "var(--color-success, #22c55e)",
              fontWeight: 600,
            }}
          >
            Yes
          </span>
        ) : (
          <span style={{ color: "var(--text-muted)" }}>No</span>
        )}
      </div>
      <div
        style={{
          display: "flex",
          justifyContent: "flex-start",
          padding: "0.5rem",
          gap: "0.5rem",
          flexWrap: "wrap",
        }}
      >
        {t.viewSql && (
          <button
            onClick={(e) => {
              e.stopPropagation();
              navigate("/sql", { state: { sql: t.viewSql, viewTable: t } });
            }}
            style={{ padding: "0.25rem 0.6rem", fontSize: "0.78rem" }}
            title="Edit this view's SQL in the Explorer"
          >
            {viewsOnly ? "Edit SQL" : "Open in Explorer"}
          </button>
        )}
        {t.canDeployToDb && (
          <button
            onClick={async (e) => {
              e.stopPropagation();
              setDeploying((prev) => ({ ...prev, [t.id]: true }));
              setDeployMsg((prev) => {
                const next = { ...prev };
                delete next[t.id];
                return next;
              });
              const result = await deployViewToDb(t.id);
              setDeploying((prev) => ({ ...prev, [t.id]: false }));
              setDeployMsg((prev) => ({ ...prev, [t.id]: result }));
              if (result.success) reload();
            }}
            style={{ padding: "0.25rem 0.6rem", fontSize: "0.78rem" }}
            title="Promote this virtual view to a real database view"
            disabled={deploying[t.id]}
          >
            {deploying[t.id] ? "Deploying…" : "Deploy to DB"}
          </button>
        )}
        <button
          onClick={(e) => {
            e.stopPropagation();
            handleProfile(t.id);
          }}
          style={{ padding: "0.25rem 0.6rem", fontSize: "0.78rem" }}
          title="Sample and profile this table's columns"
          disabled={tableProfiles[t.id] === "loading"}
        >
          {tableProfiles[t.id] === "loading" ? "Profiling…" : "Profile"}
        </button>
        <button
          className="btn-icon"
          title="View RLS policies for this table"
          onClick={(e) => {
            e.stopPropagation();
            navigate("/security/rls", {
              state: { tableFilter: t.tableName },
            });
          }}
        >
          Policies
        </button>
        <button
          className="btn-icon"
          title="Edit"
          onClick={(e) => {
            e.stopPropagation();
            startEditing(t);
          }}
        >
          <Pencil size={14} />
        </button>
        <button
          className="btn-icon-danger"
          title="Delete"
          onClick={(e) => {
            e.stopPropagation();
            handleDelete(t.id);
          }}
        >
          <Trash2 size={14} />
        </button>
      </div>
      {deployMsg[t.id] && (
        <div
          style={{
            padding: "0.5rem 0.75rem",
            fontSize: "0.8rem",
            color: deployMsg[t.id].success
              ? "var(--color-success, #22c55e)"
              : "var(--destructive)",
          }}
        >
          {deployMsg[t.id].message}
        </div>
      )}
      {(() => {
        const p = tableProfiles[t.id];
        if (!p || p === "loading") return null;
        if (typeof p === "string")
          return (
            <div
              style={{
                padding: "0.5rem 0.75rem",
                color: "var(--destructive)",
                fontSize: "0.8rem",
              }}
            >
              {p}
            </div>
          );
        const prof = computeProfile(p.columns, p.rows);
        return (
          <div
            style={{
              borderTop: "1px solid var(--border)",
              padding: "0.5rem 0.75rem",
            }}
          >
            <div
              style={{
                fontSize: "0.75rem",
                color: "var(--text-muted)",
                marginBottom: "0.4rem",
              }}
            >
              Profile — {p.rowCount} sampled rows
            </div>
            <div style={{ overflowX: "auto" }}>
              <table className="data-table" style={{ fontSize: "0.72rem" }}>
                <thead>
                  <tr>
                    <th>Column</th>
                    <th title="Null values">Nulls</th>
                    <th title="Empty strings">Blanks</th>
                    <th title="Unique values">Distinct</th>
                    <th>Min</th>
                    <th>Max</th>
                    <th>Mean</th>
                    <th>Top values</th>
                  </tr>
                </thead>
                <tbody>
                  {prof.map((c) => {
                    const nullPct =
                      p.rowCount > 0 ? Math.round((c.nullCount / p.rowCount) * 100) : 0;
                    const isHighNull = nullPct >= 50;
                    return (
                      <tr key={c.col}>
                        <td style={{ fontFamily: "monospace", fontWeight: 600 }}>{c.col}</td>
                        <td>
                          <div
                            style={{
                              display: "flex",
                              alignItems: "center",
                              gap: "0.4rem",
                            }}
                          >
                            <div
                              style={{
                                width: 52,
                                height: 5,
                                borderRadius: 3,
                                background: "var(--border)",
                                position: "relative",
                                flexShrink: 0,
                              }}
                            >
                              {c.nullCount > 0 && (
                                <div
                                  style={{
                                    position: "absolute",
                                    left: 0,
                                    top: 0,
                                    bottom: 0,
                                    width: `${nullPct}%`,
                                    borderRadius: 3,
                                    background: isHighNull
                                      ? "var(--destructive)"
                                      : "var(--text-muted)",
                                  }}
                                />
                              )}
                            </div>
                            <span
                              style={{
                                color: isHighNull
                                  ? "var(--destructive)"
                                  : c.nullCount > 0
                                    ? "var(--text)"
                                    : "var(--text-muted)",
                                fontSize: "0.7rem",
                              }}
                            >
                              {c.nullCount > 0 ? `${nullPct}%` : "—"}
                            </span>
                          </div>
                        </td>
                        <td
                          style={{
                            color: c.blankCount > 0 ? "var(--text)" : "var(--text-muted)",
                          }}
                        >
                          {c.blankCount > 0 ? c.blankCount : "—"}
                        </td>
                        <td>{c.distinctCount}</td>
                        <td style={{ fontFamily: "monospace" }}>
                          {c.min !== null ? String(c.min).slice(0, 16) : "—"}
                        </td>
                        <td style={{ fontFamily: "monospace" }}>
                          {c.max !== null ? String(c.max).slice(0, 16) : "—"}
                        </td>
                        <td style={{ fontFamily: "monospace" }}>
                          {c.mean !== null ? c.mean.toFixed(2) : "—"}
                        </td>
                        <td>
                          <div
                            style={{
                              display: "flex",
                              flexDirection: "column",
                              gap: "0.18rem",
                              minWidth: 140,
                            }}
                          >
                            {c.topValues.map(({ value, count }) => {
                              const barPct =
                                c.topValues[0].count > 0
                                  ? (count / c.topValues[0].count) * 100
                                  : 0;
                              return (
                                <div
                                  key={value}
                                  style={{
                                    display: "flex",
                                    alignItems: "center",
                                    gap: "0.3rem",
                                  }}
                                >
                                  <div
                                    style={{
                                      width: 52,
                                      height: 5,
                                      borderRadius: 2,
                                      background: "var(--border)",
                                      position: "relative",
                                      flexShrink: 0,
                                    }}
                                  >
                                    <div
                                      style={{
                                        position: "absolute",
                                        left: 0,
                                        top: 0,
                                        bottom: 0,
                                        width: `${barPct}%`,
                                        borderRadius: 2,
                                        background: "var(--primary)",
                                      }}
                                    />
                                  </div>
                                  <span
                                    style={{
                                      fontFamily: "monospace",
                                      fontSize: "0.68rem",
                                      whiteSpace: "nowrap",
                                      overflow: "hidden",
                                      maxWidth: 110,
                                      textOverflow: "ellipsis",
                                    }}
                                    title={value}
                                  >
                                    {value.slice(0, 22)}
                                  </span>
                                  <span
                                    style={{
                                      color: "var(--text-muted)",
                                      fontSize: "0.65rem",
                                      marginLeft: "auto",
                                      flexShrink: 0,
                                    }}
                                  >
                                    ×{count}
                                  </span>
                                </div>
                              );
                            })}
                          </div>
                        </td>
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            </div>
          </div>
        );
      })()}
    </>
  );
}
