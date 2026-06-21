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
import {
  useDomains,
  useTables,
  useRelationships,
  useSources,
  useRLSRules,
  useRoles,
  useCreateDomain,
  useDeleteDomain,
} from "../hooks/useAdminQueries";
import {
  downloadConfig,
  uploadConfig,
  fetchSettings,
  updateSettings,
  setDomainPolicy,
} from "../api/admin";
import type { PlatformSettings } from "../api/admin";
import { useAuth } from "../context/AuthContext";
import { domainGqlAlias } from "../types/admin";
import { MVManager } from "../components/admin/MVManager";
import { CacheManager } from "../components/admin/CacheManager";
import { SystemHealth } from "../components/admin/SystemHealth";
import { ScheduledTasks } from "../components/admin/ScheduledTasks";
import { ObservabilityTab } from "../components/admin/ObservabilityTab";
import { LocalUsersTab } from "../components/admin/LocalUsersTab";
import { OrgsTab } from "../components/admin/OrgsTab";

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
  // Domain policy controls (NOT saved via updateSettings — destructive, applied separately)
  const [policyUseDomains, setPolicyUseDomains] = useState<boolean | null>(false);
  const [policyDefaultDomain, setPolicyDefaultDomain] = useState("default");
  const [policyModalOpen, setPolicyModalOpen] = useState(false);
  const [policyConfirmText, setPolicyConfirmText] = useState("");
  const [policyApplying, setPolicyApplying] = useState(false);
  const [policyMsg, setPolicyMsg] = useState("");
  const [policyError, setPolicyError] = useState("");
  const fileInputRef = useRef<HTMLInputElement>(null);
  const [allDomains, setAllDomains] = useState<string[]>([]);

  // Pagination state
  const [domainPage, setDomainPage] = useState(0);
  const PAGE_SIZE = 50;

  // Apollo hooks for cache-and-network queries and mutations
  const { sources, loading: sourcesLoading } = useSources();
  const { domains, loading: domainsLoading, refetch: refetchDomains } = useDomains();
  const { tables, loading: tablesLoading } = useTables();
  const { relationships, loading: relsLoading } = useRelationships();
  const { rlsRules, loading: rlsLoading } = useRLSRules();
  const { roles } = useRoles();
  const { createDomain } = useCreateDomain();
  const { deleteDomain } = useDeleteDomain();
  const allRoles = roles.map((r) => r.id);

  // Update state and stats when hook data arrives
  useEffect(() => {
    const loading = sourcesLoading || domainsLoading || tablesLoading || relsLoading || rlsLoading;
    /* eslint-disable-next-line react-hooks/set-state-in-effect --
       derived state synced from multiple Apollo query results (documented useState+useEffect derived pattern) */
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
  }, [
    sources,
    domains,
    tables,
    relationships,
    rlsRules,
    rlsLoading,
    domainsLoading,
    tablesLoading,
    relsLoading,
    sourcesLoading,
    allRoles.length,
  ]);

  // Platform settings (REST); per-tab data is loaded by each tab component.
  useEffect(() => {
    fetchSettings().then((s) => {
      setSettings(s);
      setPolicyUseDomains(s.naming.use_domains);
      setPolicyDefaultDomain(s.naming.default_domain);
    });
  }, []);

  const domainsEnabled = settings?.naming.use_domains !== false;

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
    // Domain policy (use_domains/default_domain) is applied separately via the
    // destructive /admin/domain-policy endpoint — never through the normal save.
    const { use_domains: _ud, default_domain: _dd, ...naming } = settings.naming;
    const payload = { ...settings, naming } as unknown as Partial<PlatformSettings>;
    const result = await updateSettings(payload);
    setSettingsMsg(`Updated: ${result.updated.join(", ")}`);
    setSettingsSaving(false);
  }, [settings]);

  const applyDomainPolicy = useCallback(async () => {
    setPolicyApplying(true);
    setPolicyError("");
    setPolicyMsg("");
    try {
      const result = await setDomainPolicy({
        use_domains: policyUseDomains,
        default_domain: policyDefaultDomain,
      });
      setPolicyModalOpen(false);
      setPolicyConfirmText("");
      setPolicyMsg(`Domain policy applied. Backup saved: ${result.backup}`);
      const s = await fetchSettings();
      setSettings(s);
      setPolicyUseDomains(s.naming.use_domains);
      setPolicyDefaultDomain(s.naming.default_domain);
    } catch (err) {
      setPolicyError(err instanceof Error ? err.message : "Failed to apply domain policy");
    } finally {
      setPolicyApplying(false);
    }
  }, [policyUseDomains, policyDefaultDomain]);

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

  if (loading) return <div className="page">Loading admin dashboard...</div>;

  return (
    <div className="page">
      <h2>Admin Dashboard{activeTab !== "Overview" ? ` — ${activeTab}` : ""}</h2>

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
                  <label
                    style={{
                      flexDirection: "row",
                      alignItems: "center",
                      gap: "0.5rem",
                      whiteSpace: "nowrap",
                    }}
                  >
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
                      onChange={(e) => updateRedirect("threshold", parseInt(e.target.value) || 0)}
                    />
                  </label>
                  <label>
                    Default Format
                    <select
                      value={settings.redirect.default_format}
                      onChange={(e) => updateRedirect("default_format", e.target.value)}
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
                      onChange={(e) => updateRedirect("ttl", parseInt(e.target.value) || 0)}
                    />
                  </label>
                </div>
                <div className="settings-section">
                  <h4>Naming</h4>
                  <label
                    style={{
                      flexDirection: "row",
                      alignItems: "center",
                      gap: "0.5rem",
                      whiteSpace: "nowrap",
                    }}
                  >
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
                  <label>
                    Domain Mode
                    <select
                      value={
                        policyUseDomains === null
                          ? "legacy"
                          : policyUseDomains
                            ? "namespaced"
                            : "single"
                      }
                      onChange={(e) =>
                        setPolicyUseDomains(
                          e.target.value === "legacy" ? null : e.target.value === "namespaced",
                        )
                      }
                    >
                      <option value="legacy">Legacy (unset)</option>
                      <option value="single">Single domain</option>
                      <option value="namespaced">Namespaced</option>
                    </select>
                  </label>
                  <label>
                    Default Domain
                    <input
                      type="text"
                      value={policyDefaultDomain}
                      disabled={policyUseDomains !== false}
                      onChange={(e) => setPolicyDefaultDomain(e.target.value)}
                    />
                  </label>
                  <button
                    className="btn-secondary"
                    data-testid="apply-domain-policy"
                    onClick={() => {
                      setPolicyError("");
                      setPolicyConfirmText("");
                      setPolicyModalOpen(true);
                    }}
                  >
                    Apply Domain Policy
                  </button>
                  {policyMsg && <span className="upload-msg">{policyMsg}</span>}
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
                  <button className="btn-primary" onClick={saveSettings} disabled={settingsSaving}>
                    {settingsSaving ? "Saving..." : "Save Settings"}
                  </button>
                  {settingsMsg && <span className="upload-msg">{settingsMsg}</span>}
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
              <button className="btn-primary" onClick={handleUploadClick} disabled={uploading}>
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

            {configYaml !== null && <pre className="config-preview">{configYaml}</pre>}
          </>
        )}

        {activeTab === "Domains" && domainsEnabled && (
          <>
            {domainMsg && (
              <div className="success" style={{ marginBottom: "0.5rem" }}>
                {domainMsg}
              </div>
            )}
            {(() => {
              const IMPLICIT_DOMAIN_IDS = new Set(["", "meta", "ops"]);
              const userDomains = domains.filter((d) => !IMPLICIT_DOMAIN_IDS.has(d.id));
              const totalPages = Math.max(1, Math.ceil(userDomains.length / PAGE_SIZE));
              const paged = userDomains.slice(domainPage * PAGE_SIZE, (domainPage + 1) * PAGE_SIZE);
              return (
                <div>
                  <table className="data-table" style={{ marginBottom: "1rem" }}>
                    <thead>
                      <tr>
                        <th>ID</th>
                        <th>Description</th>
                        <th>GQL Alias</th>
                        <th></th>
                      </tr>
                    </thead>
                    <tbody>
                      {userDomains.length === 0 && (
                        <tr>
                          <td
                            colSpan={4}
                            style={{ color: "var(--text-muted)", textAlign: "center" }}
                          >
                            No domains defined
                          </td>
                        </tr>
                      )}
                      {paged.map((d) => (
                        <tr key={d.id}>
                          <td>{d.id}</td>
                          <td>{d.description || "—"}</td>
                          <td style={{ color: "var(--text-muted)", fontFamily: "monospace" }}>
                            {domainGqlAlias(d)}
                          </td>
                          <td>
                            <button
                              className="btn-icon-danger"
                              title="Delete"
                              onClick={() => handleDeleteDomain(d.id)}
                            >
                              <Trash2 size={14} />
                            </button>
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                  {totalPages > 1 && (
                    <div
                      style={{
                        display: "flex",
                        gap: "0.5rem",
                        alignItems: "center",
                        justifyContent: "flex-end",
                        padding: "0.5rem 0",
                        marginBottom: "1rem",
                      }}
                    >
                      <button onClick={() => setDomainPage(0)} disabled={domainPage === 0}>
                        «
                      </button>
                      <button
                        onClick={() => setDomainPage((p) => p - 1)}
                        disabled={domainPage === 0}
                      >
                        ‹
                      </button>
                      <span>
                        Page {domainPage + 1} / {totalPages}
                      </span>
                      <button
                        onClick={() => setDomainPage((p) => p + 1)}
                        disabled={domainPage >= totalPages - 1}
                      >
                        ›
                      </button>
                      <button
                        onClick={() => setDomainPage(totalPages - 1)}
                        disabled={domainPage >= totalPages - 1}
                      >
                        »
                      </button>
                    </div>
                  )}
                </div>
              );
            })()}
            <div style={{ display: "flex", gap: "0.5rem", alignItems: "center" }}>
              <input
                value={newDomainId}
                onChange={(e) => setNewDomainId(e.target.value)}
                placeholder="domain-id"
                style={{
                  width: "160px",
                  background: "var(--bg)",
                  color: "var(--text)",
                  border: "1px solid var(--border)",
                  padding: "0.5rem",
                  borderRadius: "4px",
                }}
              />
              <input
                value={newDomainDesc}
                onChange={(e) => setNewDomainDesc(e.target.value)}
                placeholder="description (optional)"
                style={{
                  flex: 1,
                  background: "var(--bg)",
                  color: "var(--text)",
                  border: "1px solid var(--border)",
                  padding: "0.5rem",
                  borderRadius: "4px",
                }}
              />
              <input
                value={newDomainAlias}
                onChange={(e) => setNewDomainAlias(e.target.value)}
                placeholder={
                  newDomainId.trim()
                    ? `alias (default: ${domainGqlAlias({ id: newDomainId.trim(), description: "" })})`
                    : "gql alias (optional)"
                }
                style={{
                  width: "180px",
                  background: "var(--bg)",
                  color: "var(--text)",
                  border: "1px solid var(--border)",
                  padding: "0.5rem",
                  borderRadius: "4px",
                }}
              />
              <button
                className="btn-primary"
                onClick={handleAddDomain}
                disabled={!newDomainId.trim()}
              >
                Add Domain
              </button>
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
          <LocalUsersTab allRoles={allRoles} allDomains={allDomains} />
        )}
        {activeTab === "Orgs" && isSuperAdmin && <OrgsTab />}
      </div>

      {policyModalOpen && (
        <div
          style={{
            position: "fixed",
            inset: 0,
            background: "rgba(0,0,0,0.6)",
            display: "flex",
            alignItems: "center",
            justifyContent: "center",
            zIndex: 1000,
          }}
        >
          <div
            data-testid="domain-policy-modal"
            style={{
              background: "var(--bg)",
              border: "2px solid var(--destructive)",
              borderRadius: "8px",
              padding: "1.5rem",
              maxWidth: 520,
              width: "90%",
            }}
          >
            <h3 style={{ color: "var(--destructive)", marginTop: 0 }}>
              ⚠ Reset Entire Configuration
            </h3>
            <div
              style={{
                background: "var(--destructive)",
                color: "#fff",
                padding: "1rem",
                borderRadius: "4px",
                marginBottom: "1rem",
              }}
            >
              Changing the domain policy will RESET your entire configuration. All sources, tables,
              domains, and relationships will be deleted. A backup of your current config will be
              saved. This cannot be undone.
            </div>
            <label style={{ display: "block", marginBottom: "1rem" }}>
              Type <strong>RESET</strong> to confirm:
              <input
                type="text"
                data-testid="domain-policy-confirm-input"
                value={policyConfirmText}
                onChange={(e) => setPolicyConfirmText(e.target.value)}
                style={{ display: "block", width: "100%", marginTop: 4 }}
              />
            </label>
            {policyError && (
              <div style={{ color: "var(--destructive)", marginBottom: "1rem" }}>{policyError}</div>
            )}
            <div style={{ display: "flex", gap: "0.5rem", justifyContent: "flex-end" }}>
              <button
                className="btn-secondary"
                onClick={() => {
                  setPolicyModalOpen(false);
                  setPolicyConfirmText("");
                  setPolicyError("");
                }}
                disabled={policyApplying}
              >
                Cancel
              </button>
              <button
                className="btn-primary"
                data-testid="domain-policy-confirm-btn"
                style={{ background: "var(--destructive)", borderColor: "var(--destructive)" }}
                disabled={policyConfirmText !== "RESET" || policyApplying}
                onClick={applyDomainPolicy}
              >
                {policyApplying ? "Applying..." : "Reset & Apply"}
              </button>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}

