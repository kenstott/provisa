// Copyright (c) 2026 Kenneth Stott
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { useEffect, useMemo, useState } from "react";
import { fetchAuthConfig, setAuthConfig, type AuthConfigState } from "../../api/admin";

// REQ-919: configure the authentication provider (firebase/keycloak/oauth/simple) + role settings.
// The provider binds at startup, so changes take effect on restart.
export function AuthTab() {
  const [s, setS] = useState<AuthConfigState | null>(null);
  const [provider, setProvider] = useState("none");
  const [config, setConfig] = useState<Record<string, Record<string, string>>>({});
  const [common, setCommon] = useState<AuthConfigState["common"] | null>(null);
  const [saving, setSaving] = useState(false);
  const [msg, setMsg] = useState("");
  const [error, setError] = useState("");

  useEffect(() => {
    fetchAuthConfig()
      .then((a) => {
        setS(a);
        setProvider(a.provider);
        setCommon(a.common);
        const cfg: Record<string, Record<string, string>> = {};
        for (const [pk, vals] of Object.entries(a.config)) {
          cfg[pk] = {};
          for (const [k, v] of Object.entries(vals)) cfg[pk][k] = v == null ? "" : String(v);
        }
        setConfig(cfg);
      })
      .catch((e) => setError(String(e)));
  }, []);

  const current = useMemo(() => s?.providers.find((p) => p.key === provider), [s, provider]);

  const missingRequired = useMemo(
    () =>
      (current?.config_fields ?? []).some(
        (f) => f.required && !(config[provider]?.[f.config_key] ?? "").trim(),
      ),
    [current, config, provider],
  );

  const save = async () => {
    if (!common) return;
    setSaving(true);
    setMsg("");
    setError("");
    try {
      const providerConfig: Record<string, unknown> = {};
      for (const f of current?.config_fields ?? []) {
        const v = (config[provider]?.[f.config_key] ?? "").trim();
        if (v !== "") providerConfig[f.config_key] = v;
      }
      const res = await setAuthConfig({ provider, config: providerConfig, common });
      setMsg(res.restart_required ? "Saved. Restart the service to apply." : "Saved.");
    } catch (e) {
      setError(String(e));
    } finally {
      setSaving(false);
    }
  };

  if (error && !s) return <div className="error-banner">{error}</div>;
  if (!s || !common) return <div>Loading…</div>;

  const setField = (key: string, value: string) =>
    setConfig((c) => ({ ...c, [provider]: { ...(c[provider] ?? {}), [key]: value } }));

  return (
    <div className="auth-tab" style={{ maxWidth: 720 }}>
      <p className="muted">
        Authentication verifies who is calling; roles + RLS decide what they can see. Pick the
        identity provider and configure its connection.
      </p>

      <div className="form-card">
        <label style={{ gridColumn: "1 / -1" }}>
          Provider
          <select value={provider} onChange={(e) => setProvider(e.target.value)}>
            {s.providers.map((p) => (
              <option key={p.key} value={p.key}>
                {p.label}
                {p.key === s.provider ? " (current)" : ""}
              </option>
            ))}
          </select>
        </label>
        {current && (
          <p className="muted" style={{ gridColumn: "1 / -1", margin: 0, fontSize: "0.8rem" }}>
            {current.description}
          </p>
        )}

        {(current?.config_fields ?? []).map((f) => (
          <label key={f.config_key} style={{ gridColumn: "1 / -1" }}>
            {f.label}
            {f.required ? " *" : ""}
            <input
              type={f.secret ? "password" : "text"}
              value={config[provider]?.[f.config_key] ?? ""}
              placeholder={f.placeholder}
              autoComplete={f.secret ? "new-password" : "off"}
              onChange={(e) => setField(f.config_key, e.target.value)}
            />
          </label>
        ))}

        {provider === "simple" && (
          <label style={{ gridColumn: "1 / -1" }}>
            <input
              type="checkbox"
              checked={common.allow_simple_auth}
              onChange={(e) => setCommon({ ...common, allow_simple_auth: e.target.checked })}
            />
            Allow simple auth (production guard — required to enable username/password)
          </label>
        )}
      </div>

      <h3>Roles</h3>
      <div className="form-card">
        <label>
          Default role
          <input
            type="text"
            value={common.default_role}
            onChange={(e) => setCommon({ ...common, default_role: e.target.value })}
          />
        </label>
        <label>
          Role assignments from
          <select
            value={common.assignments_source}
            onChange={(e) => setCommon({ ...common, assignments_source: e.target.value })}
          >
            <option value="claims">Token claims</option>
            <option value="provisa">Provisa (local assignments)</option>
          </select>
        </label>
        <label style={{ gridColumn: "1 / -1" }}>
          <input
            type="checkbox"
            checked={common.trust_upstream}
            onChange={(e) => setCommon({ ...common, trust_upstream: e.target.checked })}
          />
          Trust upstream proxy identity headers
        </label>
      </div>

      <div
        className="warn-banner"
        style={{ marginTop: "1rem", padding: "0.5rem 0.75rem", border: "1px solid #b8860b", borderRadius: 4 }}
      >
        ⚠ {s.restart_required_note}
      </div>

      <div style={{ marginTop: "1rem", display: "flex", gap: "0.75rem", alignItems: "center" }}>
        <button className="btn-primary" onClick={save} disabled={saving || missingRequired}>
          {saving ? "Saving…" : "Save auth settings"}
        </button>
        {msg && <span className="success-text">{msg}</span>}
        {error && <span className="error-text">{error}</span>}
      </div>
    </div>
  );
}
