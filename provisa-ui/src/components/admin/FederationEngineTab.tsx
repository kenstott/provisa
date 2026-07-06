// Copyright (c) 2026 Kenneth Stott
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { useEffect, useMemo, useState } from "react";
import {
  fetchFederationEngine,
  setFederationEngine,
  type FederationEngineState,
} from "../../api/admin";

// REQ-916: select + configure the federation engine. Changes persist to the platform config and
// take effect on the next service restart (the engine is bound once at boot).
export function FederationEngineTab() {
  const [state, setState] = useState<FederationEngineState | null>(null);
  const [selected, setSelected] = useState<string>("");
  const [values, setValues] = useState<Record<string, string>>({});
  const [saving, setSaving] = useState(false);
  const [msg, setMsg] = useState("");
  const [error, setError] = useState("");

  useEffect(() => {
    fetchFederationEngine()
      .then((s) => {
        setState(s);
        setSelected(s.current);
        setValues({
          federation_engine_url: s.config.federation_engine_url ?? "",
          federation_engine_host: s.config.federation_engine_host ?? "",
          federation_engine_port:
            s.config.federation_engine_port != null ? String(s.config.federation_engine_port) : "",
        });
      })
      .catch((e) => setError(String(e)));
  }, []);

  const currentEngine = useMemo(
    () => state?.engines.find((e) => e.key === selected),
    [state, selected],
  );

  const missingRequired = useMemo(
    () =>
      (currentEngine?.config_fields ?? []).some(
        (f) => f.required && !(values[f.config_key] ?? "").trim(),
      ),
    [currentEngine, values],
  );

  const save = async () => {
    if (!state) return;
    setSaving(true);
    setMsg("");
    setError("");
    try {
      const body: Record<string, unknown> = { engine: selected };
      for (const f of currentEngine?.config_fields ?? []) {
        const raw = (values[f.config_key] ?? "").trim();
        if (raw === "") continue;
        body[f.config_key] = f.type === "number" ? Number(raw) : raw;
      }
      const res = await setFederationEngine(body);
      setMsg(
        res.restart_required
          ? "Saved. Restart the service to apply the new federation engine."
          : "Saved.",
      );
    } catch (e) {
      setError(String(e));
    } finally {
      setSaving(false);
    }
  };

  if (error && !state) return <div className="error-banner">{error}</div>;
  if (!state) return <div>Loading…</div>;

  return (
    <div className="federation-engine-tab" style={{ maxWidth: 640 }}>
      <p className="muted">
        The federation engine executes every federated query. Pick the engine and configure its
        connection; other components (routing, governance, cache) are unchanged.
      </p>

      <label style={{ display: "block", fontWeight: 600, marginTop: "1rem" }}>Engine</label>
      <select
        value={selected}
        onChange={(e) => {
          setSelected(e.target.value);
          setMsg("");
        }}
        style={{ width: "100%", padding: "0.5rem", marginTop: "0.25rem" }}
      >
        {state.engines.map((e) => (
          <option key={e.key} value={e.key}>
            {e.label}
            {e.key === state.current ? " (current)" : ""}
          </option>
        ))}
      </select>
      {currentEngine && <p className="muted" style={{ marginTop: "0.25rem" }}>{currentEngine.description}</p>}

      {(currentEngine?.config_fields ?? []).map((f) => (
        <div key={f.config_key} style={{ marginTop: "0.75rem" }}>
          <label style={{ display: "block", fontWeight: 600 }}>
            {f.label}
            {f.required ? " *" : ""}
          </label>
          <input
            type={f.type === "number" ? "number" : "text"}
            value={values[f.config_key] ?? ""}
            placeholder={f.placeholder}
            onChange={(e) => setValues((v) => ({ ...v, [f.config_key]: e.target.value }))}
            style={{ width: "100%", padding: "0.5rem", marginTop: "0.25rem" }}
          />
        </div>
      ))}

      <div
        className="warn-banner"
        style={{
          marginTop: "1rem",
          padding: "0.5rem 0.75rem",
          border: "1px solid #b8860b",
          borderRadius: 4,
        }}
      >
        ⚠ {state.restart_required_note}
      </div>

      <div style={{ marginTop: "1rem", display: "flex", gap: "0.75rem", alignItems: "center" }}>
        <button className="btn-primary" onClick={save} disabled={saving || missingRequired}>
          {saving ? "Saving…" : "Save engine selection"}
        </button>
        {msg && <span className="success-text">{msg}</span>}
        {error && <span className="error-text">{error}</span>}
      </div>
    </div>
  );
}
