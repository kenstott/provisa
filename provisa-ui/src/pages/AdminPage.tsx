// Copyright (c) 2026 Kenneth Stott
// Canary: b8b1d1a3-e713-464e-8e0a-c8bc5b43544d
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { useState, useEffect, useRef, useCallback } from "react";
import { useLocation } from "react-router-dom";
import { Trash2 } from "lucide-react";
import { useDomains, useTables, useRelationships, useSources, useRLSRules, useCreateDomain, useDeleteDomain } from "../hooks/useAdminQueries";
import {
  fetchRoles,
  downloadConfig,
  uploadConfig,
  fetchSettings,
  updateSettings,
  reloadQueryEngineCatalog,
  restartQueryEngine,
  recomputeSchemaClusters,
  fetchLocalUsers,
  createLocalUser,
  deleteLocalUser,
  fetchUserAssignments,
  addUserAssignment,
  removeUserAssignment,
  fetchOrgs,
  createOrg,
  deleteOrg,
  fetchOrgMembers,
  addOrgMember,
  removeOrgMember,
  fetchOrgRoles,
  deleteOrgRole,
  fetchInvites,
  createInvite,
  revokeInvite,
} from "../api/admin";
import type { LocalUser, UserAssignment, Org, OrgMember, OrgInvite } from "../api/admin";
import type { PlatformSettings } from "../api/admin";
import { useAuth } from "../context/AuthContext";
import { domainGqlAlias } from "../types/admin";
import { MVManager } from "../components/admin/MVManager";
import { CacheManager } from "../components/admin/CacheManager";
import { SystemHealth } from "../components/admin/SystemHealth";
import { ScheduledTasks } from "../components/admin/ScheduledTasks";

const FORMAT_OPTIONS = ["parquet", "orc", "json", "ndjson", "csv", "arrow"];

const ROUTE_TO_SECTION: Record<string, string> = {
  "/admin/overview": "Overview",
  "/admin/domains": "Domains",
  "/admin/materialized-views": "Materialized Views",
  "/admin/cache": "Cache",
  "/admin/scheduled-tasks": "Scheduled Tasks",
  "/admin/system-health": "System Health",
  "/admin/observability": "Observability",
  "/admin/local-users": "Local Users",
  "/admin/orgs": "Orgs",
  "/admin/roles": "Roles",
};

/** Admin overview page — dashboard, config management, platform settings. */
export function AdminPage() {
  const location = useLocation();
  const activeTab = ROUTE_TO_SECTION[location.pathname] ?? "Overview";
  const { capabilities, activeOrgId } = useAuth();
  const isSuperAdmin = capabilities.includes("superadmin") || capabilities.includes("admin");
  const orgId = activeOrgId ?? "root";
  const [stats, setStats] = useState<Record<string, number>>({});
  const [newDomainId, setNewDomainId] = useState("");
  const [newDomainDesc, setNewDomainDesc] = useState("");
  const [newDomainAlias, setNewDomainAlias] = useState("");
  const [domainMsg, setDomainMsg] = useState("");
  const [loading, setLoading] = useState(true);
  const [configYaml, setConfigYaml] = useState<string | null>(null);
  const [uploading, setUploading] = useState(false);
  const [uploadMsg, setUploadMsg] = useState("");
  const [settings, setSettings] = useState<PlatformSettings | null>(null);
  const [settingsSaving, setSettingsSaving] = useState(false);
  const [settingsMsg, setSettingsMsg] = useState("");
  const fileInputRef = useRef<HTMLInputElement>(null);
  const [localUsers, setLocalUsers] = useState<LocalUser[]>([]);
  const [allRoles, setAllRoles] = useState<string[]>([]);
  const [allDomains, setAllDomains] = useState<string[]>([]);
  const [newUsername, setNewUsername] = useState("");
  const [newPassword, setNewPassword] = useState("");
  const [newEmail, setNewEmail] = useState("");
  const [newDisplayName, setNewDisplayName] = useState("");
  const [userMsg, setUserMsg] = useState("");
  const [expandedUserId, setExpandedUserId] = useState<string | null>(null);
  const [userAssignments, setUserAssignments] = useState<Record<string, UserAssignment[]>>({});
  const [assignRole, setAssignRole] = useState("");
  const [assignDomain, setAssignDomain] = useState("");

  const [orgs, setOrgs] = useState<Org[]>([]);
  const [newOrgId, setNewOrgId] = useState("");
  const [newOrgName, setNewOrgName] = useState("");
  const [orgMsg, setOrgMsg] = useState("");
  const [expandedOrgId, setExpandedOrgId] = useState<string | null>(null);
  const [orgMembers, setOrgMembers] = useState<Record<string, OrgMember[]>>({});
  const [addMemberUserId, setAddMemberUserId] = useState("");

  const [orgRoles, setOrgRoles] = useState<import("../types/auth").Role[]>([]);
  const [roleMsg, setRoleMsg] = useState("");

  const [orgInvites, setOrgInvites] = useState<OrgInvite[]>([]);
  const [inviteOrgId, setInviteOrgId] = useState("");
  const [inviteMsg, setInviteMsg] = useState("");
  const [copiedToken, setCopiedToken] = useState<string | null>(null);

  // Pagination state
  const [domainPage, setDomainPage] = useState(0);
  const [userPage, setUserPage] = useState(0);
  const [orgPage, setOrgPage] = useState(0);
  const [invitePage, setInvitePage] = useState(0);
  const [rolePage, setRolePage] = useState(0);
  const PAGE_SIZE = 50;

  // Apollo hooks for cache-and-network queries and mutations
  const { sources, loading: sourcesLoading } = useSources();
  const { domains, loading: domainsLoading, refetch: refetchDomains } = useDomains();
  const { tables, loading: tablesLoading } = useTables();
  const { relationships, loading: relsLoading } = useRelationships();
  const { rlsRules, loading: rlsLoading } = useRLSRules();
  const { createDomain } = useCreateDomain();
  const { deleteDomain } = useDeleteDomain();

  // Update state and stats when hook data arrives
  useEffect(() => {
    const loading = sourcesLoading || domainsLoading || tablesLoading || relsLoading || rlsLoading;
    setLoading(loading);

    if (!loading) {
      setStats({
        Sources: sources.length,
        Domains: domains.length,
        Tables: tables.length,
        Relationships: relationships.length,
        Roles: allRoles.length,
        "RLS Rules": rlsRules.length,
      });
      setAllDomains(domains.filter((d) => d.id !== "").map((d) => d.id));
    }
  }, [sources, domains, tables, relationships, rlsRules, rlsLoading, domainsLoading, tablesLoading, relsLoading, sourcesLoading, allRoles.length]);

  // Fetch remaining data (non-cached queries and mutations only)
  useEffect(() => {
    Promise.all([
      fetchRoles(),
      fetchSettings(),
      fetchLocalUsers().catch(() => [] as LocalUser[]),
      fetchOrgs().catch(() => [] as Org[]),
      fetchOrgRoles(orgId).catch(() => [] as import("../types/auth").Role[]),
      fetchInvites().catch(() => [] as OrgInvite[]),
    ])
      .then(([roles, s, users, orgsResult, rolesResult, invitesResult]) => {
        setAllRoles(roles.map((r) => r.id));
        setSettings(s);
        setLocalUsers(users as LocalUser[]);
        setOrgs(orgsResult as Org[]);
        setOrgRoles(rolesResult as import("../types/auth").Role[]);
        setOrgInvites(invitesResult as OrgInvite[]);
      });
  }, [orgId]);

  const handleDownload = async () => {
    const yaml = await downloadConfig();
    const blob = new Blob([yaml], { type: "application/x-yaml" });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = "provisa.yaml";
    a.click();
    URL.revokeObjectURL(url);
  };

  const handleViewConfig = async () => {
    if (configYaml !== null) {
      setConfigYaml(null);
      return;
    }
    const yaml = await downloadConfig();
    setConfigYaml(yaml);
  };

  const handleUploadClick = () => fileInputRef.current?.click();

  const handleFileChange = async (e: React.ChangeEvent<HTMLInputElement>) => {
    const file = e.target.files?.[0];
    if (!file) return;
    setUploading(true);
    setUploadMsg("");
    const text = await file.text();
    const result = await uploadConfig(text);
    setUploadMsg(result.message);
    setUploading(false);
    if (fileInputRef.current) fileInputRef.current.value = "";
  };

  const saveSettings = useCallback(async () => {
    if (!settings) return;
    setSettingsSaving(true);
    setSettingsMsg("");
    const result = await updateSettings(settings);
    setSettingsMsg(`Updated: ${result.updated.join(", ")}`);
    setSettingsSaving(false);
  }, [settings]);

  const updateRedirect = (key: string, value: unknown) => {
    if (!settings) return;
    setSettings({
      ...settings,
      redirect: { ...settings.redirect, [key]: value },
    });
  };

  const handleAddDomain = async () => {
    if (!newDomainId.trim()) return;
    await createDomain(newDomainId.trim(), newDomainDesc.trim(), newDomainAlias.trim() || null);
    await refetchDomains();
    setNewDomainId("");
    setNewDomainDesc("");
    setNewDomainAlias("");
    setDomainMsg(`Added "${newDomainId.trim()}"`);
  };

  const handleDeleteDomain = async (id: string) => {
    await deleteDomain(id);
    await refetchDomains();
    setDomainMsg(`Deleted "${id}"`);
  };

  const handleAddUser = async () => {
    if (!newUsername.trim() || !newPassword.trim()) return;
    setUserMsg("");
    try {
      await createLocalUser({
        username: newUsername.trim(),
        password: newPassword,
        email: newEmail.trim() || undefined,
        display_name: newDisplayName.trim() || undefined,
      });
      const updated = await fetchLocalUsers();
      setLocalUsers(updated);
      setNewUsername("");
      setNewPassword("");
      setNewEmail("");
      setNewDisplayName("");
      setUserMsg(`Created "${newUsername.trim()}"`);
    } catch (e: unknown) {
      setUserMsg(e instanceof Error ? e.message : "Create failed");
    }
  };

  const handleDeleteUser = async (userId: string, username: string) => {
    await deleteLocalUser(userId);
    setLocalUsers((prev) => prev.filter((u) => u.id !== userId));
    if (expandedUserId === userId) setExpandedUserId(null);
    setUserMsg(`Deleted "${username}"`);
  };

  const handleExpandUser = async (userId: string) => {
    if (expandedUserId === userId) {
      setExpandedUserId(null);
      return;
    }
    setExpandedUserId(userId);
    setAssignRole(allRoles[0] ?? "");
    setAssignDomain("*");
    if (!userAssignments[userId]) {
      const rows = await fetchUserAssignments(userId);
      setUserAssignments((prev) => ({ ...prev, [userId]: rows }));
    }
  };

  const handleAddAssignment = async (userId: string) => {
    if (!assignRole) return;
    try {
      await addUserAssignment(userId, assignRole, assignDomain || "*");
      const rows = await fetchUserAssignments(userId);
      setUserAssignments((prev) => ({ ...prev, [userId]: rows }));
    } catch (e: unknown) {
      setUserMsg(e instanceof Error ? e.message : "Assignment failed");
    }
  };

  const handleRemoveAssignment = async (userId: string, assignmentId: number) => {
    await removeUserAssignment(userId, assignmentId);
    setUserAssignments((prev) => ({
      ...prev,
      [userId]: (prev[userId] ?? []).filter((a) => a.id !== assignmentId),
    }));
  };

  const handleCreateOrg = async () => {
    if (!newOrgId.trim() || !newOrgName.trim()) return;
    await createOrg(newOrgId.trim(), newOrgName.trim());
    setOrgs(await fetchOrgs());
    setNewOrgId(""); setNewOrgName(""); setOrgMsg(`Created "${newOrgName.trim()}"`);
  };

  const handleDeleteOrg = async (id: string) => {
    await deleteOrg(id);
    setOrgs(await fetchOrgs());
    setOrgMsg(`Deleted "${id}"`);
  };

  const handleExpandOrg = async (id: string) => {
    if (expandedOrgId === id) { setExpandedOrgId(null); return; }
    setExpandedOrgId(id);
    if (!orgMembers[id]) {
      const members = await fetchOrgMembers(id);
      setOrgMembers((prev) => ({ ...prev, [id]: members }));
    }
  };

  const handleAddOrgMember = async (oid: string) => {
    if (!addMemberUserId.trim()) return;
    await addOrgMember(oid, addMemberUserId.trim());
    const members = await fetchOrgMembers(oid);
    setOrgMembers((prev) => ({ ...prev, [oid]: members }));
    setAddMemberUserId("");
  };

  const handleRemoveOrgMember = async (oid: string, userId: string) => {
    await removeOrgMember(oid, userId);
    setOrgMembers((prev) => ({
      ...prev,
      [oid]: (prev[oid] ?? []).filter((m) => m.user_id !== userId),
    }));
  };

  const handleDeleteOrgRole = async (roleId: string) => {
    await deleteOrgRole(orgId, roleId);
    setOrgRoles((prev) => prev.filter((r) => r.id !== roleId));
    setRoleMsg(`Deleted "${roleId}"`);
  };

  const handleCreateInvite = async () => {
    if (!inviteOrgId.trim()) return;
    const invite = await createInvite(inviteOrgId.trim());
    setOrgInvites(await fetchInvites());
    const url = `${window.location.origin}/register?invite=${invite.token}`;
    await navigator.clipboard.writeText(url);
    setInviteMsg(`Invite created and copied: ${url}`);
    setInviteOrgId("");
  };

  const handleRevokeInvite = async (token: string) => {
    await revokeInvite(token);
    setOrgInvites((prev) => prev.filter((i) => i.token !== token));
  };

  const handleCopyInvite = async (token: string) => {
    const url = `${window.location.origin}/register?invite=${token}`;
    await navigator.clipboard.writeText(url);
    setCopiedToken(token);
    setTimeout(() => setCopiedToken(null), 2000);
  };

  if (loading) return <div className="page">Loading admin dashboard...</div>;

  return (
    <div className="page">
      <h2>Admin Dashboard</h2>

      <div className="admin-tab-content">
      {activeTab === "Overview" && (
        <>
          <div className="stats-grid">
            {Object.entries(stats).map(([label, count]) => (
              <div key={label} className="stat-card">
                <div className="stat-count">{count}</div>
                <div className="stat-label">{label}</div>
              </div>
            ))}
          </div>

          <h3>Platform Settings</h3>
          {settings && (
            <div className="settings-grid">
              <div className="settings-section">
                <h4>Redirect</h4>
                <label style={{ flexDirection: "row", alignItems: "center", gap: "0.5rem", whiteSpace: "nowrap" }}>
                  <input
                    type="checkbox"
                    checked={settings.redirect.enabled}
                    onChange={(e) => updateRedirect("enabled", e.target.checked)}
                    style={{ width: "auto" }}
                  />
                  Enabled
                </label>
                <label>
                  Default Threshold (rows)
                  <input
                    type="number"
                    value={settings.redirect.threshold}
                    onChange={(e) =>
                      updateRedirect("threshold", parseInt(e.target.value) || 0)
                    }
                  />
                </label>
                <label>
                  Default Format
                  <select
                    value={settings.redirect.default_format}
                    onChange={(e) =>
                      updateRedirect("default_format", e.target.value)
                    }
                  >
                    {FORMAT_OPTIONS.map((f) => (
                      <option key={f} value={f}>
                        {f}
                      </option>
                    ))}
                  </select>
                </label>
                <label>
                  Presigned URL TTL (seconds)
                  <input
                    type="number"
                    value={settings.redirect.ttl}
                    onChange={(e) =>
                      updateRedirect("ttl", parseInt(e.target.value) || 0)
                    }
                  />
                </label>
              </div>
              <div className="settings-section">
                <h4>Naming</h4>
                <label style={{ flexDirection: "row", alignItems: "center", gap: "0.5rem", whiteSpace: "nowrap" }}>
                  <input
                    type="checkbox"
                    checked={settings.naming.domain_prefix}
                    onChange={(e) =>
                      setSettings({
                        ...settings,
                        naming: { ...settings.naming, domain_prefix: e.target.checked },
                      })
                    }
                    style={{ width: "auto" }}
                  />
                  Domain prefix (domain_id__ prepended to all names)
                </label>
                <label>
                  Naming Convention
                  <select
                    value={settings.naming.convention || "snake_case"}
                    onChange={(e) =>
                      setSettings({
                        ...settings,
                        naming: {
                          ...settings.naming,
                          convention: e.target.value,
                        },
                      })
                    }
                  >
                    <option value="none">None (raw DB names)</option>
                    <option value="snake_case">snake_case</option>
                    <option value="camelCase">camelCase</option>
                    <option value="PascalCase">PascalCase</option>
                  </select>
                </label>
              </div>
              <div className="settings-section">
                <h4>Sampling</h4>
                <label>
                  Default Sample Size
                  <input
                    type="number"
                    value={settings.sampling.default_sample_size}
                    onChange={(e) =>
                      setSettings({
                        ...settings,
                        sampling: {
                          default_sample_size: parseInt(e.target.value) || 0,
                        },
                      })
                    }
                  />
                </label>
              </div>
              <div className="settings-section">
                <h4>Cache</h4>
                <label>
                  Default TTL (seconds)
                  <input
                    type="number"
                    value={settings.cache.default_ttl}
                    onChange={(e) =>
                      setSettings({
                        ...settings,
                        cache: {
                          default_ttl: parseInt(e.target.value) || 0,
                        },
                      })
                    }
                  />
                </label>
              </div>
              <div className="settings-actions">
                <button
                  className="btn-primary"
                  onClick={saveSettings}
                  disabled={settingsSaving}
                >
                  {settingsSaving ? "Saving..." : "Save Settings"}
                </button>
                {settingsMsg && (
                  <span className="upload-msg">{settingsMsg}</span>
                )}
              </div>
            </div>
          )}

          <h3>Configuration File</h3>
          <div className="config-actions">
            <button className="btn-secondary" onClick={handleDownload}>
              Download
            </button>
            <button className="btn-secondary" onClick={handleViewConfig}>
              {configYaml !== null ? "Hide" : "View"}
            </button>
            <button
              className="btn-primary"
              onClick={handleUploadClick}
              disabled={uploading}
            >
              {uploading ? "Uploading..." : "Upload"}
            </button>
            <input
              ref={fileInputRef}
              type="file"
              accept=".yaml,.yml"
              style={{ display: "none" }}
              onChange={handleFileChange}
            />
            {uploadMsg && <span className="upload-msg">{uploadMsg}</span>}
          </div>

          {configYaml !== null && (
            <pre className="config-preview">{configYaml}</pre>
          )}
        </>
      )}

      {activeTab === "Domains" && (
        <>
          {domainMsg && <div className="success" style={{ marginBottom: "0.5rem" }}>{domainMsg}</div>}
          {(() => {
            const totalPages = Math.max(1, Math.ceil(domains.length / PAGE_SIZE));
            const paged = domains.slice(domainPage * PAGE_SIZE, (domainPage + 1) * PAGE_SIZE);
            return (
              <div>
                <table className="data-table" style={{ marginBottom: "1rem" }}>
                  <thead><tr><th>ID</th><th>Description</th><th>GQL Alias</th><th></th></tr></thead>
                  <tbody>
                    {domains.length === 0 && (
                      <tr><td colSpan={4} style={{ color: "var(--text-muted)", textAlign: "center" }}>No domains defined</td></tr>
                    )}
                    {paged.map((d) => (
                      <tr key={d.id}>
                        <td>{d.id}</td>
                        <td>{d.description || "—"}</td>
                        <td style={{ color: "var(--text-muted)", fontFamily: "monospace" }}>{domainGqlAlias(d)}</td>
                        <td>
                          <button className="btn-icon-danger" title="Delete" onClick={() => handleDeleteDomain(d.id)}><Trash2 size={14} /></button>
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
                {totalPages > 1 && (
                  <div style={{ display: "flex", gap: "0.5rem", alignItems: "center", justifyContent: "flex-end", padding: "0.5rem 0", marginBottom: "1rem" }}>
                    <button onClick={() => setDomainPage(0)} disabled={domainPage === 0}>«</button>
                    <button onClick={() => setDomainPage(p => p - 1)} disabled={domainPage === 0}>‹</button>
                    <span>Page {domainPage + 1} / {totalPages}</span>
                    <button onClick={() => setDomainPage(p => p + 1)} disabled={domainPage >= totalPages - 1}>›</button>
                    <button onClick={() => setDomainPage(totalPages - 1)} disabled={domainPage >= totalPages - 1}>»</button>
                  </div>
                )}
              </div>
            );
          })()}
          <div style={{ display: "flex", gap: "0.5rem", alignItems: "center" }}>
            <input value={newDomainId} onChange={(e) => setNewDomainId(e.target.value)} placeholder="domain-id" style={{ width: "160px", background: "var(--bg)", color: "var(--text)", border: "1px solid var(--border)", padding: "0.5rem", borderRadius: "4px" }} />
            <input value={newDomainDesc} onChange={(e) => setNewDomainDesc(e.target.value)} placeholder="description (optional)" style={{ flex: 1, background: "var(--bg)", color: "var(--text)", border: "1px solid var(--border)", padding: "0.5rem", borderRadius: "4px" }} />
            <input value={newDomainAlias} onChange={(e) => setNewDomainAlias(e.target.value)} placeholder={newDomainId.trim() ? `alias (default: ${domainGqlAlias({ id: newDomainId.trim(), description: "" })})` : "gql alias (optional)"} style={{ width: "180px", background: "var(--bg)", color: "var(--text)", border: "1px solid var(--border)", padding: "0.5rem", borderRadius: "4px" }} />
            <button className="btn-primary" onClick={handleAddDomain} disabled={!newDomainId.trim()}>Add Domain</button>
          </div>
        </>
      )}
      {activeTab === "Materialized Views" && <MVManager />}
      {activeTab === "Cache" && <CacheManager />}
      {activeTab === "Scheduled Tasks" && <ScheduledTasks />}
      {activeTab === "System Health" && <SystemHealth />}
      {activeTab === "Observability" && settings && (
        <ObservabilityTab settings={settings} setSettings={setSettings} />
      )}
      {activeTab === "Local Users" && (
        <>
          {userMsg && <div className="success" style={{ marginBottom: "0.5rem" }}>{userMsg}</div>}
          {(() => {
            const totalPages = Math.max(1, Math.ceil(localUsers.length / PAGE_SIZE));
            const paged = localUsers.slice(userPage * PAGE_SIZE, (userPage + 1) * PAGE_SIZE);
            return (
              <div>
                <table className="data-table" style={{ marginBottom: "1rem" }}>
                  <thead>
                    <tr><th>Username</th><th>Email</th><th>Display Name</th><th>Active</th><th></th></tr>
                  </thead>
                  <tbody>
                    {localUsers.length === 0 && (
                      <tr><td colSpan={5} style={{ color: "var(--text-muted)", textAlign: "center" }}>No local users</td></tr>
                    )}
                    {paged.map((u) => (
                <>
                  <tr key={u.id}>
                    <td style={{ fontFamily: "monospace" }}>
                      <button
                        style={{ background: "none", border: "none", color: "var(--accent)", cursor: "pointer", padding: 0, fontFamily: "monospace" }}
                        onClick={() => handleExpandUser(u.id)}
                      >
                        {expandedUserId === u.id ? "▾" : "▸"} {u.username}
                      </button>
                    </td>
                    <td>{u.email || "—"}</td>
                    <td>{u.display_name || "—"}</td>
                    <td>{u.is_active ? "Yes" : "No"}</td>
                    <td>
                      <button className="btn-icon-danger" title="Delete" onClick={() => handleDeleteUser(u.id, u.username)}>
                        <Trash2 size={14} />
                      </button>
                    </td>
                  </tr>
                  {expandedUserId === u.id && (
                    <tr key={`${u.id}-assign`}>
                      <td colSpan={5} style={{ paddingLeft: "2rem", background: "var(--bg-alt, var(--bg))" }}>
                        <div style={{ padding: "0.75rem 0" }}>
                          <strong style={{ fontSize: "0.85rem" }}>Role:Domain Assignments</strong>
                          <div style={{ marginTop: "0.5rem", display: "flex", flexWrap: "wrap", gap: "0.4rem" }}>
                            {(userAssignments[u.id] ?? []).length === 0 && (
                              <span style={{ color: "var(--text-muted)", fontSize: "0.85rem" }}>No assignments</span>
                            )}
                            {(userAssignments[u.id] ?? []).map((a) => (
                              <span key={a.id} style={{ display: "inline-flex", alignItems: "center", gap: "0.25rem", background: "var(--border)", borderRadius: "4px", padding: "0.2rem 0.5rem", fontSize: "0.8rem" }}>
                                {a.role_id}:{a.domain_id}
                                <button
                                  style={{ background: "none", border: "none", color: "var(--danger, #e55)", cursor: "pointer", padding: 0, lineHeight: 1 }}
                                  onClick={() => handleRemoveAssignment(u.id, a.id)}
                                  title="Remove"
                                >×</button>
                              </span>
                            ))}
                          </div>
                          <div style={{ marginTop: "0.5rem", display: "flex", gap: "0.5rem", alignItems: "center" }}>
                            <select
                              value={assignRole}
                              onChange={(e) => setAssignRole(e.target.value)}
                              style={{ background: "var(--bg)", color: "var(--text)", border: "1px solid var(--border)", padding: "0.3rem", borderRadius: "4px", fontSize: "0.85rem" }}
                            >
                              {allRoles.map((r) => <option key={r} value={r}>{r}</option>)}
                            </select>
                            <select
                              value={assignDomain}
                              onChange={(e) => setAssignDomain(e.target.value)}
                              style={{ background: "var(--bg)", color: "var(--text)", border: "1px solid var(--border)", padding: "0.3rem", borderRadius: "4px", fontSize: "0.85rem" }}
                            >
                              <option value="*">* (all domains)</option>
                              {allDomains.map((d) => <option key={d} value={d}>{d}</option>)}
                            </select>
                            <button className="btn-primary" style={{ fontSize: "0.8rem", padding: "0.3rem 0.7rem" }} onClick={() => handleAddAssignment(u.id)} disabled={!assignRole}>
                              Add
                            </button>
                          </div>
                        </div>
                      </td>
                    </tr>
                  )}
                </>
                    ))}
                  </tbody>
                </table>
                {totalPages > 1 && (
                  <div style={{ display: "flex", gap: "0.5rem", alignItems: "center", justifyContent: "flex-end", padding: "0.5rem 0", marginBottom: "1rem" }}>
                    <button onClick={() => setUserPage(0)} disabled={userPage === 0}>«</button>
                    <button onClick={() => setUserPage(p => p - 1)} disabled={userPage === 0}>‹</button>
                    <span>Page {userPage + 1} / {totalPages}</span>
                    <button onClick={() => setUserPage(p => p + 1)} disabled={userPage >= totalPages - 1}>›</button>
                    <button onClick={() => setUserPage(totalPages - 1)} disabled={userPage >= totalPages - 1}>»</button>
                  </div>
                )}
              </div>
            );
          })()}
          <h4 style={{ marginBottom: "0.5rem" }}>Create User</h4>
          <div style={{ display: "flex", flexDirection: "column", gap: "0.5rem", maxWidth: "480px" }}>
            <input
              value={newUsername}
              onChange={(e) => setNewUsername(e.target.value)}
              placeholder="Username *"
              style={{ background: "var(--bg)", color: "var(--text)", border: "1px solid var(--border)", padding: "0.5rem", borderRadius: "4px" }}
            />
            <input
              type="password"
              value={newPassword}
              onChange={(e) => setNewPassword(e.target.value)}
              placeholder="Password *"
              style={{ background: "var(--bg)", color: "var(--text)", border: "1px solid var(--border)", padding: "0.5rem", borderRadius: "4px" }}
            />
            <input
              value={newEmail}
              onChange={(e) => setNewEmail(e.target.value)}
              placeholder="Email (optional)"
              style={{ background: "var(--bg)", color: "var(--text)", border: "1px solid var(--border)", padding: "0.5rem", borderRadius: "4px" }}
            />
            <input
              value={newDisplayName}
              onChange={(e) => setNewDisplayName(e.target.value)}
              placeholder="Display name (optional)"
              style={{ background: "var(--bg)", color: "var(--text)", border: "1px solid var(--border)", padding: "0.5rem", borderRadius: "4px" }}
            />
            <button
              className="btn-primary"
              onClick={handleAddUser}
              disabled={!newUsername.trim() || !newPassword.trim()}
              style={{ alignSelf: "flex-start" }}
            >
              Create User
            </button>
          </div>
        </>
      )}
      {activeTab === "Orgs" && isSuperAdmin && (
        <div>
          <h3>Organizations</h3>
          {orgMsg && <p className="form-msg">{orgMsg}</p>}
          {(() => {
            const totalPages = Math.max(1, Math.ceil(orgs.length / PAGE_SIZE));
            const paged = orgs.slice(orgPage * PAGE_SIZE, (orgPage + 1) * PAGE_SIZE);
            return (
              <div>
                <table className="admin-table">
                  <thead><tr><th>ID</th><th>Name</th><th>Members</th><th></th></tr></thead>
                  <tbody>
                    {paged.map((org) => (
                <>
                  <tr key={org.id}>
                    <td>{org.id}</td>
                    <td>{org.name}</td>
                    <td>
                      <button className="btn-secondary" onClick={() => handleExpandOrg(org.id)}>
                        {expandedOrgId === org.id ? "Hide" : "Members"}
                      </button>
                    </td>
                    <td>
                      {org.id !== "root" && (
                        <button className="btn-danger" onClick={() => handleDeleteOrg(org.id)}>
                          <Trash2 size={14} />
                        </button>
                      )}
                    </td>
                  </tr>
                  {expandedOrgId === org.id && (
                    <tr key={`${org.id}-members`}>
                      <td colSpan={4}>
                        <div className="assignment-panel">
                          {(orgMembers[org.id] ?? []).map((m) => (
                            <span key={m.user_id} className="assignment-chip">
                              {m.display_name ?? m.email ?? m.user_id}
                              <button onClick={() => handleRemoveOrgMember(org.id, m.user_id)}>×</button>
                            </span>
                          ))}
                          <div className="assignment-add">
                            <input
                              placeholder="user_id"
                              value={addMemberUserId}
                              onChange={(e) => setAddMemberUserId(e.target.value)}
                            />
                            <button className="btn-primary" onClick={() => handleAddOrgMember(org.id)}>Add</button>
                          </div>
                        </div>
                      </td>
                    </tr>
                    )}
                  </>
                ))}
                  </tbody>
                </table>
                {totalPages > 1 && (
                  <div style={{ display: "flex", gap: "0.5rem", alignItems: "center", justifyContent: "flex-end", padding: "0.5rem 0", marginBottom: "1rem" }}>
                    <button onClick={() => setOrgPage(0)} disabled={orgPage === 0}>«</button>
                    <button onClick={() => setOrgPage(p => p - 1)} disabled={orgPage === 0}>‹</button>
                    <span>Page {orgPage + 1} / {totalPages}</span>
                    <button onClick={() => setOrgPage(p => p + 1)} disabled={orgPage >= totalPages - 1}>›</button>
                    <button onClick={() => setOrgPage(totalPages - 1)} disabled={orgPage >= totalPages - 1}>»</button>
                  </div>
                )}
              </div>
            );
          })()}
          <h4>Create Org</h4>
          <div className="form-row">
            <input placeholder="ID (slug)" value={newOrgId} onChange={(e) => setNewOrgId(e.target.value)} />
            <input placeholder="Name" value={newOrgName} onChange={(e) => setNewOrgName(e.target.value)} />
            <button className="btn-primary" onClick={handleCreateOrg}>Create</button>
          </div>
          <h3 style={{ marginTop: 24 }}>Invite Links</h3>
          <div style={{ display: "flex", gap: 8, marginBottom: 12 }}>
            <select value={inviteOrgId} onChange={(e) => setInviteOrgId(e.target.value)}>
              <option value="">Select org...</option>
              {orgs.map((o) => <option key={o.id} value={o.id}>{o.name} ({o.id})</option>)}
            </select>
            <button className="btn-primary" onClick={handleCreateInvite} disabled={!inviteOrgId}>
              Generate Invite Link
            </button>
          </div>
          {inviteMsg && <div style={{ marginBottom: 8, color: "var(--muted-foreground)", fontSize: 13 }}>{inviteMsg}</div>}
          {(() => {
            const totalPages = Math.max(1, Math.ceil(orgInvites.length / PAGE_SIZE));
            const paged = orgInvites.slice(invitePage * PAGE_SIZE, (invitePage + 1) * PAGE_SIZE);
            return (
              <div>
                <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 13 }}>
                  <thead>
                    <tr>
                      {["Org", "Token", "Created By", "Expires", "Status", ""].map((h) => (
                        <th key={h} style={{ textAlign: "left", padding: "4px 8px", borderBottom: "1px solid var(--border)" }}>{h}</th>
                      ))}
                    </tr>
                  </thead>
                  <tbody>
                    {paged.map((inv) => (
                <tr key={inv.token}>
                  <td style={{ padding: "4px 8px" }}>{inv.org_name}</td>
                  <td style={{ padding: "4px 8px", fontFamily: "monospace", fontSize: 11 }}>{inv.token.slice(0, 8)}…</td>
                  <td style={{ padding: "4px 8px" }}>{inv.created_by}</td>
                  <td style={{ padding: "4px 8px" }}>{new Date(inv.expires_at).toLocaleDateString()}</td>
                  <td style={{ padding: "4px 8px" }}>{inv.used_at ? `Used ${new Date(inv.used_at).toLocaleDateString()}` : "Active"}</td>
                  <td style={{ padding: "4px 8px", display: "flex", gap: 4 }}>
                    {!inv.used_at && (
                      <button onClick={() => handleCopyInvite(inv.token)} style={{ fontSize: 12 }}>
                        {copiedToken === inv.token ? "Copied!" : "Copy"}
                      </button>
                    )}
                    {!inv.used_at && (
                      <button onClick={() => handleRevokeInvite(inv.token)} style={{ fontSize: 12, color: "var(--destructive)" }}>
                        Revoke
                      </button>
                    )}
                  </td>
                </tr>
                    ))}
                    {orgInvites.length === 0 && (
                      <tr><td colSpan={6} style={{ padding: "8px", color: "var(--muted-foreground)" }}>No invites</td></tr>
                    )}
                  </tbody>
                </table>
                {totalPages > 1 && (
                  <div style={{ display: "flex", gap: "0.5rem", alignItems: "center", justifyContent: "flex-end", padding: "0.5rem 0", marginBottom: "1rem" }}>
                    <button onClick={() => setInvitePage(0)} disabled={invitePage === 0}>«</button>
                    <button onClick={() => setInvitePage(p => p - 1)} disabled={invitePage === 0}>‹</button>
                    <span>Page {invitePage + 1} / {totalPages}</span>
                    <button onClick={() => setInvitePage(p => p + 1)} disabled={invitePage >= totalPages - 1}>›</button>
                    <button onClick={() => setInvitePage(totalPages - 1)} disabled={invitePage >= totalPages - 1}>»</button>
                  </div>
                )}
              </div>
            );
          })()}
        </div>
      )}
      {activeTab === "Roles" && (
        <div>
          <h3>Roles — {orgId}</h3>
          {roleMsg && <p className="form-msg">{roleMsg}</p>}
          {(() => {
            const totalPages = Math.max(1, Math.ceil(orgRoles.length / PAGE_SIZE));
            const paged = orgRoles.slice(rolePage * PAGE_SIZE, (rolePage + 1) * PAGE_SIZE);
            return (
              <div>
                <table className="admin-table">
                  <thead><tr><th>ID</th><th>Capabilities</th><th>Domain Access</th><th></th></tr></thead>
                  <tbody>
                    {paged.map((role) => (
                <tr key={role.id}>
                  <td>{role.id}</td>
                  <td>{role.capabilities.join(", ")}</td>
                  <td>{role.domain_access.join(", ")}</td>
                  <td>
                    <button className="btn-danger" onClick={() => handleDeleteOrgRole(role.id)}>
                      <Trash2 size={14} />
                    </button>
                  </td>
                </tr>
                    ))}
                  </tbody>
                </table>
                {totalPages > 1 && (
                  <div style={{ display: "flex", gap: "0.5rem", alignItems: "center", justifyContent: "flex-end", padding: "0.5rem 0", marginBottom: "1rem" }}>
                    <button onClick={() => setRolePage(0)} disabled={rolePage === 0}>«</button>
                    <button onClick={() => setRolePage(p => p - 1)} disabled={rolePage === 0}>‹</button>
                    <span>Page {rolePage + 1} / {totalPages}</span>
                    <button onClick={() => setRolePage(p => p + 1)} disabled={rolePage >= totalPages - 1}>›</button>
                    <button onClick={() => setRolePage(totalPages - 1)} disabled={rolePage >= totalPages - 1}>»</button>
                  </div>
                )}
              </div>
            );
          })()}
        </div>
      )}
      </div>
    </div>
  );
}

interface ObsTabProps {
  settings: import("../api/admin").PlatformSettings;
  setSettings: (s: import("../api/admin").PlatformSettings) => void;
}

interface TraceEntry {
  ts: number;
  trace_id: string;
  span_id: string;
  name: string;
  status: string;
  duration_ms: number | null;
  attrs: Record<string, unknown>;
}

const API_BASE = import.meta.env.VITE_API_BASE || "http://localhost:8001";

function TraceFeed() {
  const [traces, setTraces] = useState<TraceEntry[]>([]);
  const [paused, setPaused] = useState(false);
  const [selected, setSelected] = useState<TraceEntry | null>(null);
  const listRef = useRef<HTMLDivElement>(null);
  const pausedRef = useRef(paused);
  pausedRef.current = paused;

  useEffect(() => {
    let alive = true;
    const poll = async () => {
      if (!alive || pausedRef.current) return;
      try {
        const resp = await fetch(`${API_BASE}/admin/traces/recent?limit=50`);
        const json = await resp.json();
        if (alive && !pausedRef.current) setTraces(json.traces ?? []);
      } catch { /* ignore */ }
    };
    poll();
    const id = setInterval(poll, 2000);
    return () => { alive = false; clearInterval(id); };
  }, []);

  const statusColor = (s: string) =>
    s === "OK" ? "var(--success)" : s === "ERROR" ? "var(--error, #e55)" : "var(--text-muted)";

  return (
    <div className="trace-feed">
      <div className="trace-feed-header">
        <span>Live Traces</span>
        <button
          className="trace-pause-btn"
          onClick={() => setPaused((p) => !p)}
          title={paused ? "Resume" : "Pause"}
        >
          {paused ? "▶ Resume" : "⏸ Pause"}
        </button>
      </div>
      <div className="trace-feed-list" ref={listRef}>
        {traces.length === 0 ? (
          <div className="trace-empty">No spans yet — make a request to see traces.</div>
        ) : (
          traces.map((t) => (
            <div
              key={t.span_id}
              className={`trace-row${selected?.span_id === t.span_id ? " trace-row--selected" : ""}`}
              onClick={() => setSelected(selected?.span_id === t.span_id ? null : t)}
            >
              <span className="trace-name">{t.name}</span>
              <span className="trace-status" style={{ color: statusColor(t.status) }}>{t.status}</span>
              <span className="trace-dur">{t.duration_ms != null ? `${t.duration_ms}ms` : "—"}</span>
              {selected?.span_id === t.span_id && (
                <div className="trace-detail" onClick={(e) => e.stopPropagation()}>
                  <div><strong>Trace:</strong> <code>{t.trace_id}</code></div>
                  <div><strong>Span:</strong> <code>{t.span_id}</code></div>
                  <div><strong>Time:</strong> {new Date(t.ts * 1000).toLocaleTimeString()}</div>
                  {Object.keys(t.attrs).length > 0 && (
                    <pre className="trace-attrs">{JSON.stringify(t.attrs, null, 2)}</pre>
                  )}
                </div>
              )}
            </div>
          ))
        )}
      </div>
    </div>
  );
}

function ObservabilityTab({ settings, setSettings }: ObsTabProps) {
  const [saving, setSaving] = useState(false);
  const [msg, setMsg] = useState("");

  const update = (key: keyof typeof settings.otel, value: unknown) =>
    setSettings({ ...settings, otel: { ...settings.otel, [key]: value } });

  const save = async () => {
    setSaving(true);
    setMsg("");
    try {
      const result = await updateSettings({ otel: settings.otel });
      setMsg(`Saved: ${result.updated.join(", ")}`);
    } catch (e: unknown) {
      setMsg(e instanceof Error ? e.message : "Save failed");
    } finally {
      setSaving(false);
    }
  };

  const active = Boolean(settings.otel.endpoint);

  return (
    <div className="observability-layout">
      <div className="settings-section">
        <h4>OpenTelemetry Tracing</h4>
        <p style={{ color: "var(--text-muted)", fontSize: "0.85rem", marginBottom: "1rem" }}>
          Status: <strong style={{ color: active ? "var(--success)" : "var(--text-muted)" }}>
            {active ? `Exporting → ${settings.otel.endpoint}` : "Active (spans dropped — no collector configured)"}
          </strong>
        </p>
        <label>
          OTLP Collector Endpoint
          <input
            type="text"
            value={settings.otel.endpoint}
            onChange={(e) => update("endpoint", e.target.value)}
            placeholder="http://otel-collector:4317"
          />
          <span style={{ fontSize: "0.75rem", color: "var(--text-muted)" }}>
            Leave empty to generate traces without exporting. Override with OTEL_EXPORTER_OTLP_ENDPOINT env var.
          </span>
        </label>
        <label>
          Service Name
          <input
            type="text"
            value={settings.otel.service_name}
            onChange={(e) => update("service_name", e.target.value)}
            placeholder="provisa"
          />
          <span style={{ fontSize: "0.75rem", color: "var(--text-muted)" }}>
            Override with OTEL_SERVICE_NAME env var.
          </span>
        </label>
        <label>
          Sample Rate
          <input
            type="number"
            min={0}
            max={1}
            step={0.01}
            value={settings.otel.sample_rate}
            onChange={(e) => update("sample_rate", parseFloat(e.target.value) || 0)}
          />
          <span style={{ fontSize: "0.75rem", color: "var(--text-muted)" }}>
            0.0–1.0. 1.0 = 100% of traces sampled.
          </span>
        </label>
        <div style={{ marginTop: "1rem", display: "flex", gap: "0.75rem", alignItems: "center" }}>
          <button className="btn-primary" onClick={save} disabled={saving}>
            {saving ? "Saving..." : "Save"}
          </button>
          {msg && <span className="upload-msg">{msg}</span>}
        </div>
        <p style={{ marginTop: "1.5rem", fontSize: "0.8rem", color: "var(--text-muted)" }}>
          Note: endpoint and service_name changes take effect on next restart. Sample rate is applied immediately.
        </p>

        <h4 style={{ marginTop: "1.5rem" }}>Support Telemetry</h4>
        <p style={{ color: "var(--text-muted)", fontSize: "0.85rem", marginBottom: "1rem" }}>
          Optionally forward telemetry to Provisa support. SQL literals are stripped by default.
        </p>
        <label>
          Support OTLP Endpoint
          <input
            type="text"
            value={settings.otel.support_endpoint}
            onChange={(e) => update("support_endpoint", e.target.value)}
            placeholder="https://otel.provisa.io:4318"
          />
          <span style={{ fontSize: "0.75rem", color: "var(--text-muted)" }}>
            Leave empty to disable support telemetry. Override with PROVISA_SUPPORT_OTLP_ENDPOINT env var.
          </span>
        </label>
        <label style={{ flexDirection: "row", alignItems: "center", gap: "0.5rem" }}>
          <input
            type="checkbox"
            checked={settings.otel.support_redact_sql_literals}
            onChange={(e) => update("support_redact_sql_literals", e.target.checked)}
          />
          Redact SQL literals before forwarding to support
        </label>
        <label>
          Redact Attributes (comma-separated)
          <input
            type="text"
            value={(settings.otel.support_redact_attributes ?? []).join(", ")}
            onChange={(e) =>
              update(
                "support_redact_attributes",
                e.target.value
                  .split(",")
                  .map((s) => s.trim())
                  .filter(Boolean),
              )
            }
            placeholder="user.id, db.user, ..."
          />
          <span style={{ fontSize: "0.75rem", color: "var(--text-muted)" }}>
            Attribute keys to drop entirely before sending to support.
          </span>
        </label>
      </div>
      <div style={{ display: "flex", flexDirection: "column", gap: "1rem" }}>
        <TraceFeed />
        <QueryEngineActions />
      </div>
    </div>
  );
}

function QueryEngineActions() {
  const [reloadStatus, setReloadStatus] = useState<string>("");
  const [restartStatus, setRestartStatus] = useState<string>("");
  const [clusterStatus, setClusterStatus] = useState<string>("");
  const [reloading, setReloading] = useState(false);
  const [restarting, setRestarting] = useState(false);
  const [reclustering, setReclustering] = useState(false);

  const handleReload = async () => {
    setReloading(true);
    setReloadStatus("");
    try {
      const result = await reloadQueryEngineCatalog();
      setReloadStatus(result.success ? "Catalog reloaded." : `Errors: ${result.errors.join("; ")}`);
    } catch (e: unknown) {
      setReloadStatus(e instanceof Error ? e.message : "Failed");
    } finally {
      setReloading(false);
    }
  };

  const handleRestart = async () => {
    setRestarting(true);
    setRestartStatus("");
    try {
      const result = await restartQueryEngine();
      setRestartStatus(`Restarted: ${result.container}`);
    } catch (e: unknown) {
      setRestartStatus(e instanceof Error ? e.message : "Failed");
    } finally {
      setRestarting(false);
    }
  };

  const handleRecluster = async () => {
    setReclustering(true);
    setClusterStatus("");
    try {
      const result = await recomputeSchemaClusters();
      setClusterStatus(`Clustered ${result.tables_clustered} tables.`);
    } catch (e: unknown) {
      setClusterStatus(e instanceof Error ? e.message : "Failed");
    } finally {
      setReclustering(false);
    }
  };

  return (
    <div className="settings-section">
      <h4>Query Engine</h4>
      <div style={{ display: "flex", gap: "0.75rem", alignItems: "center", flexWrap: "wrap" }}>
        <button className="btn-primary" onClick={handleReload} disabled={reloading}>
          {reloading ? "Reloading..." : "Reload Catalog"}
        </button>
        <button className="btn-warning" onClick={handleRestart} disabled={restarting}>
          {restarting ? "Restarting..." : "Restart Engine"}
        </button>
        <button className="btn-secondary" onClick={handleRecluster} disabled={reclustering}>
          {reclustering ? "Clustering..." : "Recompute Schema Clusters"}
        </button>
      </div>
      {reloadStatus && <span className="upload-msg" style={{ marginTop: "0.5rem", display: "block" }}>{reloadStatus}</span>}
      {restartStatus && <span className="upload-msg" style={{ marginTop: "0.5rem", display: "block" }}>{restartStatus}</span>}
      {clusterStatus && <span className="upload-msg" style={{ marginTop: "0.5rem", display: "block" }}>{clusterStatus}</span>}
    </div>
  );
}
