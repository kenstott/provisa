import { useState, useEffect, useRef, useCallback } from "react";
import {
  fetchSources,
  fetchDomains,
  fetchTables,
  fetchRelationships,
  fetchRlsRules,
  fetchRoles,
  downloadConfig,
  uploadConfig,
  fetchSettings,
  updateSettings,
} from "../api/admin";
import type { PlatformSettings } from "../api/admin";

const FORMAT_OPTIONS = ["parquet", "orc", "json", "ndjson", "csv", "arrow"];

/** Admin overview page — dashboard, config management, platform settings. */
export function AdminPage() {
  const [stats, setStats] = useState<Record<string, number>>({});
  const [loading, setLoading] = useState(true);
  const [configYaml, setConfigYaml] = useState<string | null>(null);
  const [uploading, setUploading] = useState(false);
  const [uploadMsg, setUploadMsg] = useState("");
  const [settings, setSettings] = useState<PlatformSettings | null>(null);
  const [settingsSaving, setSettingsSaving] = useState(false);
  const [settingsMsg, setSettingsMsg] = useState("");
  const fileInputRef = useRef<HTMLInputElement>(null);

  useEffect(() => {
    Promise.all([
      fetchSources(),
      fetchDomains(),
      fetchTables(),
      fetchRelationships(),
      fetchRoles(),
      fetchRlsRules(),
      fetchSettings(),
    ])
      .then(([sources, domains, tables, rels, roles, rls, s]) => {
        setStats({
          Sources: sources.length,
          Domains: domains.length,
          Tables: tables.length,
          Relationships: rels.length,
          Roles: roles.length,
          "RLS Rules": rls.length,
        });
        setSettings(s);
      })
      .finally(() => setLoading(false));
  }, []);

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

  if (loading) return <div className="page">Loading admin dashboard...</div>;

  return (
    <div className="page">
      <h2>Admin Dashboard</h2>
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
            <label>
              <input
                type="checkbox"
                checked={settings.redirect.enabled}
                onChange={(e) => updateRedirect("enabled", e.target.checked)}
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
    </div>
  );
}
