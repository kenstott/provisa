// Copyright (c) 2025 Kenneth Stott
// Canary: 09cc2288-9f68-4d9b-914e-1ba0f0e346d0
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { useState, useEffect, useCallback, useRef, useLayoutEffect } from "react";
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
const EMPTY_RULE = { tableId: "", roleId: "", filterExpr: "" };

export function SecurityPage() {
  const [roles, setRoles] = useState<Role[]>([]);
  const [rules, setRules] = useState<RLSRule[]>([]);
  const [tables, setTables] = useState<RegisteredTable[]>([]);
  const [domains, setDomains] = useState<Domain[]>([]);
  const [loading, setLoading] = useState(true);
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState("");

  const [showRoleForm, setShowRoleForm] = useState(false);
  const [roleForm, setRoleForm] = useState(EMPTY_ROLE);
  const [editingRoleId, setEditingRoleId] = useState<string | null>(null);

  const [showRuleForm, setShowRuleForm] = useState(false);
  const [ruleForm, setRuleForm] = useState(EMPTY_RULE);
  const [editingRuleId, setEditingRuleId] = useState<number | null>(null);

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

  const tableNameById = Object.fromEntries(
    tables.map((t) => [t.id, t.tableName]),
  );

  // --- Role handlers ---
  const handleEditRole = (role: Role) => {
    setRoleForm({
      id: role.id,
      capabilities: [...role.capabilities],
      domainAccess: [...role.domain_access],
    });
    setEditingRoleId(role.id);
    setShowRoleForm(true);
    setError("");
  };

  const handleNewRole = () => {
    setRoleForm({ ...EMPTY_ROLE });
    setEditingRoleId(null);
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
      setEditingRoleId(null);
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
      await load();
    } catch (e: any) {
      setError(e.message);
    } finally {
      setSaving(false);
    }
  };

  const toggleCapability = (cap: Capability) => {
    setRoleForm((f) => ({
      ...f,
      capabilities: f.capabilities.includes(cap)
        ? f.capabilities.filter((c) => c !== cap)
        : [...f.capabilities, cap],
    }));
  };

  const toggleDomain = (domain: string) => {
    setRoleForm((f) => ({
      ...f,
      domainAccess: f.domainAccess.includes(domain)
        ? f.domainAccess.filter((d) => d !== domain)
        : [...f.domainAccess, domain],
    }));
  };

  // --- RLS Rule handlers ---
  const handleEditRule = (rule: RLSRule) => {
    const tableName = tableNameById[rule.tableId] ?? String(rule.tableId);
    setRuleForm({
      tableId: tableName,
      roleId: rule.roleId,
      filterExpr: rule.filterExpr,
    });
    setEditingRuleId(rule.id);
    setShowRuleForm(true);
    setError("");
  };

  const handleNewRule = () => {
    setRuleForm({ ...EMPTY_RULE });
    setEditingRuleId(null);
    setShowRuleForm(true);
    setError("");
  };

  const handleSaveRule = async () => {
    if (!ruleForm.tableId || !ruleForm.roleId || !ruleForm.filterExpr) return;
    setSaving(true);
    setError("");
    try {
      const res = await upsertRlsRule(ruleForm);
      if (!res.success) { setError(res.message); return; }
      setShowRuleForm(false);
      setRuleForm({ ...EMPTY_RULE });
      setEditingRuleId(null);
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
      await deleteRlsRule(rule.tableId, rule.roleId);
      await load();
    } catch (e: any) {
      setError(e.message);
    } finally {
      setSaving(false);
    }
  };

  if (loading) return <div className="page">Loading security config...</div>;

  return (
    <div className="page">
      {error && <div className="error-banner">{error}</div>}

      {/* Roles Section */}
      <div className="page-header">
        <h2>Roles</h2>
        <div className="page-actions">
          <button className="btn-primary" onClick={() => showRoleForm ? (setShowRoleForm(false), setEditingRoleId(null)) : handleNewRole()}>
            {showRoleForm ? "Cancel" : "Add Role"}
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
                disabled={editingRoleId !== null}
              />
            </label>
          </div>
          <div className="form-row">
            <label>
              Capabilities
              <div className="checkbox-grid">
                {ALL_CAPABILITIES.map((cap) => (
                  <label key={cap} className="checkbox-label">
                    <input
                      type="checkbox"
                      checked={roleForm.capabilities.includes(cap)}
                      onChange={() => toggleCapability(cap)}
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
            <button className="btn-primary" onClick={handleSaveRole} disabled={saving}>
              {saving ? "Saving..." : "Save"}
            </button>
          </div>
        </div>
      )}

      <table className="data-table">
        <thead>
          <tr><th>ID</th><th>Capabilities</th><th>Domain Access</th><th></th></tr>
        </thead>
        <tbody>
          {roles.map((r) => (
            <tr key={r.id}>
              <td>{r.id}</td>
              <td>{r.capabilities.join(", ")}</td>
              <td>{r.domain_access.join(", ")}</td>
              <td style={{ whiteSpace: "nowrap" }}>
                <button className="btn-secondary btn-sm" onClick={() => handleEditRole(r)}>Edit</button>{" "}
                <button className="btn-danger btn-sm" onClick={() => handleDeleteRole(r.id)}>Delete</button>
              </td>
            </tr>
          ))}
        </tbody>
      </table>

      {/* RLS Rules Section */}
      <div className="page-header">
        <h2>RLS Rules</h2>
        <div className="page-actions">
          <button className="btn-primary" onClick={() => showRuleForm ? (setShowRuleForm(false), setEditingRuleId(null)) : handleNewRule()}>
            {showRuleForm ? "Cancel" : "Add RLS Rule"}
          </button>
        </div>
      </div>

      {showRuleForm && (
        <div className="form-card">
          <div className="form-row">
            <label>
              Table
              <select
                value={ruleForm.tableId}
                onChange={(e) => setRuleForm({ ...ruleForm, tableId: e.target.value })}
              >
                <option value="">Select...</option>
                {tables.map((t) => (
                  <option key={t.id} value={t.tableName}>{t.tableName}</option>
                ))}
              </select>
            </label>
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
              <input
                value={ruleForm.filterExpr}
                onChange={(e) => setRuleForm({ ...ruleForm, filterExpr: e.target.value })}
                placeholder="region = 'US' AND status = 'active'"
              />
            </label>
          </div>
          <div className="form-row">
            <button className="btn-primary" onClick={handleSaveRule} disabled={saving}>
              {saving ? "Saving..." : "Save"}
            </button>
          </div>
        </div>
      )}

      <table className="data-table">
        <thead>
          <tr><th>ID</th><th>Table</th><th>Role</th><th>Filter</th><th></th></tr>
        </thead>
        <tbody>
          {rules.map((r) => (
            <tr key={r.id}>
              <td>{r.id}</td>
              <td>{tableNameById[r.tableId] ?? r.tableId}</td>
              <td>{r.roleId}</td>
              <td><code>{r.filterExpr}</code></td>
              <td>
                <button className="btn-secondary btn-sm" onClick={() => handleEditRule(r)}>Edit</button>
                <button className="btn-danger btn-sm" onClick={() => handleDeleteRule(r)}>Delete</button>
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
