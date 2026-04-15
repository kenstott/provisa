// Copyright (c) 2026 Kenneth Stott
// Canary: 09cc2288-9f68-4d9b-914e-1ba0f0e346d0
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import React, { useState, useEffect, useCallback, useRef, useLayoutEffect } from "react";
import { Trash2, Pencil, Save, X } from "lucide-react";
import { FilterInput } from "../components/admin/FilterInput";
import { createPortal } from "react-dom";
import {
  fetchRoles,
  fetchRlsRules,
  fetchTables,
  fetchDomains,
  upsertRole,
  deleteRole,
  upsertRlsRule,
  deleteRlsRule,
} from "../api/admin";
import type { Role, Capability } from "../types/auth";
import type { RLSRule, RegisteredTable, Domain } from "../types/admin";

function MultiSelect({ options, value, onChange }: {
  options: { id: string; label: string }[];
  value: string[];
  onChange: (selected: string[]) => void;
}) {
  const [open, setOpen] = useState(false);
  const [pos, setPos] = useState<{ top: number; left: number; width: number } | null>(null);
  const triggerRef = useRef<HTMLDivElement>(null);
  const dropdownRef = useRef<HTMLDivElement>(null);

  const updatePos = () => {
    if (triggerRef.current) {
      const rect = triggerRef.current.getBoundingClientRect();
      setPos({ top: rect.bottom + 2, left: rect.left, width: rect.width });
    }
  };

  useLayoutEffect(() => { if (open) updatePos(); }, [open]);

  useEffect(() => {
    if (!open) return;
    const handleClickOutside = (e: MouseEvent) => {
      const target = e.target as Node;
      if (
        triggerRef.current && !triggerRef.current.contains(target) &&
        dropdownRef.current && !dropdownRef.current.contains(target)
      ) setOpen(false);
    };
    document.addEventListener("mousedown", handleClickOutside);
    window.addEventListener("scroll", updatePos, true);
    window.addEventListener("resize", updatePos);
    return () => {
      document.removeEventListener("mousedown", handleClickOutside);
      window.removeEventListener("scroll", updatePos, true);
      window.removeEventListener("resize", updatePos);
    };
  }, [open]);

  const display = value.length > 0 ? value.join(", ") : "none";

  return (
    <div className="multiselect" ref={triggerRef}>
      <div className="multiselect-trigger" onClick={() => setOpen(!open)}>
        <span className="multiselect-text">{display}</span>
        <span className="multiselect-arrow">{open ? "\u25B4" : "\u25BE"}</span>
      </div>
      {open && pos && createPortal(
        <div
          className="multiselect-dropdown"
          ref={dropdownRef}
          style={{ top: pos.top, left: pos.left, width: Math.max(pos.width, 180) }}
        >
          {options.map((opt) => (
            <label key={opt.id} className="multiselect-option">
              <input
                type="checkbox"
                checked={value.includes(opt.id)}
                onChange={(e) => {
                  const next = e.target.checked
                    ? [...value, opt.id]
                    : value.filter((v) => v !== opt.id);
                  onChange(next);
                }}
              />
              {opt.label}
            </label>
          ))}
        </div>,
        document.body
      )}
    </div>
  );
}

const ALL_CAPABILITIES: Capability[] = [
  "source_registration",
  "table_registration",
  "relationship_registration",
  "security_config",
  "query_development",
  "query_approval",
  "full_results",
  "admin",
];

const EMPTY_ROLE = { id: "", capabilities: [] as Capability[], domainAccess: [] as string[] };
const EMPTY_RULE = { tableId: "", domainId: "", roleId: "", filterExpr: "", domainFilter: "", applyToDomain: false };

export function SecurityPage() {
  const [roles, setRoles] = useState<Role[]>([]);
  const [rules, setRules] = useState<RLSRule[]>([]);
  const [tables, setTables] = useState<RegisteredTable[]>([]);
  const [domains, setDomains] = useState<Domain[]>([]);
  const [loading, setLoading] = useState(true);
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState("");

  // Roles
  const [showRoleForm, setShowRoleForm] = useState(false);
  const [roleForm, setRoleForm] = useState(EMPTY_ROLE);
  const [expandedRole, setExpandedRole] = useState<string | null>(null);
  const [editingRoleInRow, setEditingRoleInRow] = useState<string | null>(null);
  const [roleSearch, setRoleSearch] = useState("");

  // RLS Rules
  const [showRuleForm, setShowRuleForm] = useState(false);
  const [ruleForm, setRuleForm] = useState(EMPTY_RULE);
  const [expandedRule, setExpandedRule] = useState<number | null>(null);
  const [editingRuleInRow, setEditingRuleInRow] = useState<number | null>(null);
  const [ruleSearch, setRuleSearch] = useState("");

  const load = useCallback(async () => {
    setLoading(true);
    const [r, rls, t, d] = await Promise.all([
      fetchRoles(),
      fetchRlsRules(),
      fetchTables(),
      fetchDomains(),
    ]);
    setRoles(r);
    setRules(rls);
    setTables(t);
    setDomains(d);
    setLoading(false);
  }, []);

  useEffect(() => { load(); }, [load]);

  const normalizeDomain = (id: string) => id.replace(/[^a-zA-Z0-9]/g, "_").replace(/^_+|_+$/g, "");
  const tableNameById = Object.fromEntries(
    tables.map((t) => [t.id, t.tableName]),
  );
  const tableLabelById = Object.fromEntries(
    tables.map((t) => [t.id, `${normalizeDomain(t.domainId)}.${t.tableName}`]),
  );

  // --- Role handlers ---
  const handleNewRole = () => {
    setRoleForm({ ...EMPTY_ROLE });
    setShowRoleForm(true);
    setError("");
  };

  const handleSaveRole = async () => {
    if (!roleForm.id) return;
    setSaving(true);
    setError("");
    try {
      const res = await upsertRole(roleForm);
      if (!res.success) { setError(res.message); return; }
      setShowRoleForm(false);
      setRoleForm({ ...EMPTY_ROLE });
      setEditingRoleInRow(null);
      await load();
    } catch (e: any) {
      setError(e.message);
    } finally {
      setSaving(false);
    }
  };

  const handleDeleteRole = async (id: string) => {
    setSaving(true);
    setError("");
    try {
      await deleteRole(id);
      if (expandedRole === id) setExpandedRole(null);
      await load();
    } catch (e: any) {
      setError(e.message);
    } finally {
      setSaving(false);
    }
  };

  const startEditingRole = (role: Role) => {
    setRoleForm({
      id: role.id,
      capabilities: [...role.capabilities],
      domainAccess: [...role.domain_access],
    });
    setEditingRoleInRow(role.id);
    setError("");
  };

  const toggleCapability = (cap: Capability) => {
    setRoleForm((f) => ({
      ...f,
      capabilities: f.capabilities.includes(cap)
        ? f.capabilities.filter((c) => c !== cap)
        : [...f.capabilities, cap],
    }));
  };

  // --- RLS Rule handlers ---
  const handleNewRule = () => {
    setRuleForm({ ...EMPTY_RULE });
    setShowRuleForm(true);
    setError("");
  };

  const handleSaveRule = async () => {
    const valid = ruleForm.applyToDomain
      ? ruleForm.domainFilter && ruleForm.roleId && ruleForm.filterExpr
      : ruleForm.tableId && ruleForm.roleId && ruleForm.filterExpr;
    if (!valid) return;
    setSaving(true);
    setError("");
    try {
      const res = await upsertRlsRule({
        tableId: ruleForm.applyToDomain ? null : ruleForm.tableId || null,
        domainId: ruleForm.applyToDomain ? ruleForm.domainFilter || null : null,
        roleId: ruleForm.roleId,
        filterExpr: ruleForm.filterExpr,
      });
      if (!res.success) { setError(res.message); return; }
      setShowRuleForm(false);
      setRuleForm({ ...EMPTY_RULE });
      setEditingRuleInRow(null);
      await load();
    } catch (e: any) {
      setError(e.message);
    } finally {
      setSaving(false);
    }
  };

  const handleDeleteRule = async (rule: RLSRule) => {
    setSaving(true);
    setError("");
    try {
      await deleteRlsRule(rule.roleId, rule.tableId, rule.domainId);
      if (expandedRule === rule.id) setExpandedRule(null);
      await load();
    } catch (e: any) {
      setError(e.message);
    } finally {
      setSaving(false);
    }
  };

  const startEditingRule = (rule: RLSRule) => {
    if (rule.domainId) {
      setRuleForm({
        tableId: "",
        domainId: rule.domainId,
        roleId: rule.roleId,
        filterExpr: rule.filterExpr,
        domainFilter: rule.domainId,
        applyToDomain: true,
      });
    } else {
      const tableName = rule.tableId != null ? (tableNameById[rule.tableId] ?? String(rule.tableId)) : "";
      const tbl = rule.tableId != null ? tables.find((t) => t.id === rule.tableId) : undefined;
      setRuleForm({
        tableId: tableName,
        domainId: "",
        roleId: rule.roleId,
        filterExpr: rule.filterExpr,
        domainFilter: tbl ? tbl.domainId : "",
        applyToDomain: false,
      });
    }
    setEditingRuleInRow(rule.id);
    setError("");
  };

  if (loading) return <div className="page">Loading security config...</div>;

  return (
    <div className="page">
      {error && <div className="error-banner">{error}</div>}

      {/* Roles Section */}
      <div className="page-header">
        <h2>Roles</h2>
        <FilterInput value={roleSearch} onChange={setRoleSearch} placeholder="Filter by role ID…" />
        <div className="page-actions">
          <button className="btn-primary" onClick={() => {
            if (showRoleForm) { setShowRoleForm(false); }
            else { setExpandedRole(null); handleNewRole(); }
          }}>
            {showRoleForm ? "Cancel" : "+ Role"}
          </button>
        </div>
      </div>

      {showRoleForm && (
        <div className="form-card">
          <div className="form-row" style={{ alignSelf: "start" }}>
            <label>
              Role ID
              <input
                value={roleForm.id}
                onChange={(e) => setRoleForm({ ...roleForm, id: e.target.value })}
                placeholder="analyst"
              />
            </label>
          </div>
          <div className="form-row">
            <label>
              Capabilities
              <div className="checkbox-grid">
                {ALL_CAPABILITIES.map((cap) => (
                  <label key={cap} style={{ display: "flex", flexDirection: "row", alignItems: "center", gap: "0.35rem", whiteSpace: "nowrap" }}>
                    <input
                      type="checkbox"
                      checked={roleForm.capabilities.includes(cap)}
                      onChange={() => toggleCapability(cap)}
                      style={{ width: "auto", padding: 0 }}
                    />
                    {cap}
                  </label>
                ))}
              </div>
            </label>
          </div>
          <label style={{ gridColumn: "1 / -1", display: "flex", flexDirection: "column", gap: "0.25rem", fontSize: "0.875rem", color: "var(--text-muted)" }}>
            Domain Access
            <MultiSelect
              options={[{ id: "*", label: "All Domains" }, ...domains.map((d) => ({ id: d.id, label: d.id }))]}
              value={roleForm.domainAccess}
              onChange={(selected) => setRoleForm({ ...roleForm, domainAccess: selected })}
            />
          </label>
          <div style={{ gridColumn: "1 / -1", display: "flex", justifyContent: "flex-end" }}>
            <button className="btn-icon-primary" title="Save" onClick={handleSaveRole} disabled={saving}><Save size={14} /></button>
          </div>
        </div>
      )}

      <table className="data-table">
        <thead>
          <tr><th>ID</th><th>Capabilities</th><th>Domain Access</th></tr>
        </thead>
        <tbody>
          {roles.filter((r) => !roleSearch.trim() || r.id.toLowerCase().includes(roleSearch.toLowerCase())).map((r) => (
            <React.Fragment key={r.id}>
              <tr
                style={{ cursor: "pointer", background: expandedRole === r.id ? "var(--surface)" : undefined }}
                onClick={() => { setExpandedRole(expandedRole === r.id ? null : r.id); setEditingRoleInRow(null); }}
              >
                <td>{r.id}</td>
                <td>{r.capabilities.join(", ")}</td>
                <td>{r.domain_access.join(", ")}</td>
              </tr>
              {expandedRole === r.id && (
                <tr>
                  <td colSpan={3} style={{ padding: "0.75rem 1rem", background: "var(--bg)", borderTop: "1px solid var(--border)" }}>
                    {editingRoleInRow !== r.id ? (
                      <div style={{ display: "flex", flexDirection: "column", gap: "0.5rem" }}>
                        <div><strong>ID:</strong> {r.id}</div>
                        <div><strong>Capabilities:</strong> {r.capabilities.join(", ") || "none"}</div>
                        <div><strong>Domain Access:</strong> {r.domain_access.join(", ") || "none"}</div>
                        <div style={{ display: "flex", gap: "0.5rem", marginTop: "0.25rem" }}>
                          <button className="btn-icon" title="Edit" onClick={(e) => { e.stopPropagation(); startEditingRole(r); }}><Pencil size={14} /></button>
                          <button className="btn-icon-danger" title="Delete" onClick={(e) => { e.stopPropagation(); handleDeleteRole(r.id); }}><Trash2 size={14} /></button>
                        </div>
                      </div>
                    ) : (
                      <div style={{ display: "flex", flexDirection: "column", gap: "0.75rem" }}>
                        <div className="form-row">
                          <label>
                            Capabilities
                            <div className="checkbox-grid">
                              {ALL_CAPABILITIES.map((cap) => (
                                <label key={cap} style={{ display: "flex", flexDirection: "row", alignItems: "center", gap: "0.35rem", whiteSpace: "nowrap" }}>
                                  <input
                                    type="checkbox"
                                    checked={roleForm.capabilities.includes(cap)}
                                    onChange={() => toggleCapability(cap)}
                                    style={{ width: "auto", padding: 0 }}
                                  />
                                  {cap}
                                </label>
                              ))}
                            </div>
                          </label>
                        </div>
                        <label style={{ display: "flex", flexDirection: "column", gap: "0.25rem", fontSize: "0.875rem", color: "var(--text-muted)" }}>
                          Domain Access
                          <MultiSelect
                            options={[{ id: "*", label: "All Domains" }, ...domains.map((d) => ({ id: d.id, label: d.id }))]}
                            value={roleForm.domainAccess}
                            onChange={(selected) => setRoleForm({ ...roleForm, domainAccess: selected })}
                          />
                        </label>
                        <div style={{ display: "flex", gap: "0.5rem", justifyContent: "flex-end" }}>
                          <button className="btn-icon" title="Cancel" onClick={() => setEditingRoleInRow(null)}><X size={14} /></button>
                          <button className="btn-icon-primary" title="Save" onClick={handleSaveRole} disabled={saving}><Save size={14} /></button>
                        </div>
                      </div>
                    )}
                  </td>
                </tr>
              )}
            </React.Fragment>
          ))}
        </tbody>
      </table>

      {/* RLS Rules Section */}
      <div className="page-header" style={{ marginTop: "2rem" }}>
        <h2>RLS Rules</h2>
        <FilterInput value={ruleSearch} onChange={setRuleSearch} placeholder="Filter by role or table…" />
        <div className="page-actions">
          <button className="btn-primary" onClick={() => {
            if (showRuleForm) { setShowRuleForm(false); }
            else { setExpandedRule(null); handleNewRule(); }
          }}>
            {showRuleForm ? "Cancel" : "+ RLS"}
          </button>
        </div>
      </div>

      {showRuleForm && (
        <div className="form-card">
          <div className="form-row">
            <label>
              Apply To
              <select
                value={ruleForm.applyToDomain ? "domain" : "table"}
                onChange={(e) => setRuleForm({ ...ruleForm, applyToDomain: e.target.value === "domain", tableId: "" })}
              >
                <option value="table">Specific Table</option>
                <option value="domain">Entire Domain</option>
              </select>
            </label>
            <label>
              Domain
              <select
                value={ruleForm.domainFilter}
                onChange={(e) => setRuleForm({ ...ruleForm, domainFilter: e.target.value, tableId: "" })}
              >
                <option value="">Select...</option>
                {domains.map((d) => <option key={d.id} value={d.id}>{d.id}</option>)}
              </select>
            </label>
            {!ruleForm.applyToDomain && (
              <label>
                Table
                <select
                  value={ruleForm.tableId}
                  onChange={(e) => setRuleForm({ ...ruleForm, tableId: e.target.value })}
                >
                  <option value="">Select...</option>
                  {tables
                    .filter((t) => !ruleForm.domainFilter || t.domainId === ruleForm.domainFilter)
                    .map((t) => (
                      <option key={t.id} value={t.tableName}>{t.tableName}</option>
                    ))}
                </select>
              </label>
            )}
            <label>
              Role
              <select
                value={ruleForm.roleId}
                onChange={(e) => setRuleForm({ ...ruleForm, roleId: e.target.value })}
              >
                <option value="">Select...</option>
                {roles.map((r) => (
                  <option key={r.id} value={r.id}>{r.id}</option>
                ))}
              </select>
            </label>
          </div>
          <div className="form-row">
            <label style={{ flex: 1 }}>
              Filter Expression
              <textarea
                rows={2}
                value={ruleForm.filterExpr}
                onChange={(e) => setRuleForm({ ...ruleForm, filterExpr: e.target.value })}
                placeholder="region = 'US' AND status = 'active'"
                style={{ resize: "vertical", fontFamily: "monospace", fontSize: "0.875rem" }}
              />
            </label>
          </div>
          <div className="form-row">
            <button className="btn-icon-primary" title="Save" onClick={handleSaveRule} disabled={saving}><Save size={14} /></button>
          </div>
        </div>
      )}

      <table className="data-table">
        <thead>
          <tr><th>ID</th><th>Table / Domain</th><th>Role</th><th>Filter</th></tr>
        </thead>
        <tbody>
          {rules.filter((r) => {
            if (!ruleSearch.trim()) return true;
            const q = ruleSearch.toLowerCase();
            const scope = r.domainId ? `domain:${r.domainId}` : (tableLabelById[r.tableId!] ?? String(r.tableId));
            return r.roleId.toLowerCase().includes(q) || scope.toLowerCase().includes(q);
          }).map((r) => {
            const scope = r.domainId
              ? `domain: ${r.domainId}`
              : (tableLabelById[r.tableId!] ?? String(r.tableId));
            return (
              <React.Fragment key={r.id}>
                <tr
                  style={{ cursor: "pointer", background: expandedRule === r.id ? "var(--surface)" : undefined }}
                  onClick={() => { setExpandedRule(expandedRule === r.id ? null : r.id); setEditingRuleInRow(null); }}
                >
                  <td>{r.id}</td>
                  <td>
                    {r.domainId
                      ? <><em style={{ color: "var(--muted, #888)", fontSize: "0.75em" }}>domain: </em>{r.domainId}</>
                      : tableLabelById[r.tableId!] ?? String(r.tableId)
                    }
                  </td>
                  <td>{r.roleId}</td>
                  <td><code>{r.filterExpr}</code></td>
                </tr>
                {expandedRule === r.id && (
                  <tr>
                    <td colSpan={4} style={{ padding: "0.75rem 1rem", background: "var(--bg)", borderTop: "1px solid var(--border)" }}>
                      {editingRuleInRow !== r.id ? (
                        <div style={{ display: "flex", flexDirection: "column", gap: "0.5rem" }}>
                          <div><strong>ID:</strong> {r.id}</div>
                          {r.domainId
                            ? <div><strong>Domain:</strong> {r.domainId}</div>
                            : <div><strong>Table:</strong> {tableLabelById[r.tableId!] ?? String(r.tableId)}</div>
                          }
                          <div><strong>Role:</strong> {r.roleId}</div>
                          <div><strong>Filter:</strong> <code>{r.filterExpr}</code></div>
                          <div style={{ display: "flex", gap: "0.5rem", marginTop: "0.25rem" }}>
                            <button className="btn-icon" title="Edit" onClick={(e) => { e.stopPropagation(); startEditingRule(r); }}><Pencil size={14} /></button>
                            <button className="btn-icon-danger" title="Delete" onClick={(e) => { e.stopPropagation(); handleDeleteRule(r); }}><Trash2 size={14} /></button>
                          </div>
                        </div>
                      ) : (
                        <div style={{ display: "flex", flexDirection: "column", gap: "0.75rem" }}>
                          <div className="form-row">
                            <label>
                              Apply To
                              <select
                                value={ruleForm.applyToDomain ? "domain" : "table"}
                                onChange={(e) => setRuleForm({ ...ruleForm, applyToDomain: e.target.value === "domain", tableId: "" })}
                              >
                                <option value="table">Specific Table</option>
                                <option value="domain">Entire Domain</option>
                              </select>
                            </label>
                            <label>
                              Domain
                              <select
                                value={ruleForm.domainFilter}
                                onChange={(e) => setRuleForm({ ...ruleForm, domainFilter: e.target.value, tableId: "" })}
                              >
                                <option value="">Select...</option>
                                {domains.map((d) => <option key={d.id} value={d.id}>{d.id}</option>)}
                              </select>
                            </label>
                            {!ruleForm.applyToDomain && (
                              <label>
                                Table
                                <select
                                  value={ruleForm.tableId}
                                  onChange={(e) => setRuleForm({ ...ruleForm, tableId: e.target.value })}
                                >
                                  <option value="">Select...</option>
                                  {tables
                                    .filter((t) => !ruleForm.domainFilter || t.domainId === ruleForm.domainFilter)
                                    .map((t) => (
                                      <option key={t.id} value={t.tableName}>{t.tableName}</option>
                                    ))}
                                </select>
                              </label>
                            )}
                            <label>
                              Role
                              <select
                                value={ruleForm.roleId}
                                onChange={(e) => setRuleForm({ ...ruleForm, roleId: e.target.value })}
                              >
                                <option value="">Select...</option>
                                {roles.map((role) => (
                                  <option key={role.id} value={role.id}>{role.id}</option>
                                ))}
                              </select>
                            </label>
                          </div>
                          <div className="form-row">
                            <label style={{ flex: 1 }}>
                              Filter Expression
                              <textarea
                                rows={2}
                                value={ruleForm.filterExpr}
                                onChange={(e) => setRuleForm({ ...ruleForm, filterExpr: e.target.value })}
                                placeholder="region = 'US' AND status = 'active'"
                                style={{ resize: "vertical", fontFamily: "monospace", fontSize: "0.875rem" }}
                              />
                            </label>
                          </div>
                          <div style={{ display: "flex", gap: "0.5rem", justifyContent: "flex-end" }}>
                            <button className="btn-icon" title="Cancel" onClick={() => setEditingRuleInRow(null)}><X size={14} /></button>
                            <button className="btn-icon-primary" title="Save" onClick={handleSaveRule} disabled={saving}><Save size={14} /></button>
                          </div>
                        </div>
                      )}
                    </td>
                  </tr>
                )}
              </React.Fragment>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}
