// Copyright (c) 2026 Kenneth Stott
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { useEffect, useMemo, useState } from "react";
import { Save } from "lucide-react";
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
  // String for text/number fields; boolean for checkbox fields.
  const [values, setValues] = useState<Record<string, string | boolean>>({});
  const [saving, setSaving] = useState(false);
  const [msg, setMsg] = useState("");
  const [error, setError] = useState("");

  useEffect(() => {
    fetchFederationEngine()
      .then((s) => {
        setState(s);
        setSelected(s.current);
        // Seed every declared key from the returned config, keeping booleans as booleans.
        const seeded: Record<string, string | boolean> = {};
        for (const key of Object.keys(s.config)) {
          const v = s.config[key];
          seeded[key] = typeof v === "boolean" ? v : v == null ? "" : String(v);
        }
        setValues(seeded);
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
        (f) => f.required && !String(values[f.config_key] ?? "").trim(),
      ),
    [currentEngine, values],
  );

  const save = async () => {
    if (!state) return;
    setSaving(true);
    setMsg("");
    setError("");
    try {
      const body: { engine: string } & Record<string, unknown> = { engine: selected };
      for (const f of currentEngine?.config_fields ?? []) {
        const v = values[f.config_key];
        if (f.type === "boolean") {
          body[f.config_key] = v === true;
          continue;
        }
        const raw = String(v ?? "").trim();
        // Send blanks too, so clearing a field resets it server-side.
        body[f.config_key] = f.type === "number" && raw !== "" ? Number(raw) : raw;
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
    <div className="federation-engine-tab" style={{ maxWidth: 720 }}>
      <p className="muted">
        The federation engine executes every federated query. Pick the engine and configure it;
        other components (routing, governance, cache) are unchanged.
      </p>

      <div className="form-card">
        <label style={{ gridColumn: "1 / -1" }}>
          Engine
          <select
            value={selected}
            onChange={(e) => {
              setSelected(e.target.value);
              setMsg("");
            }}
          >
            {state.engines.map((e) => (
              <option key={e.key} value={e.key}>
                {e.label}
                {e.key === state.current ? " (current)" : ""}
              </option>
            ))}
          </select>
        </label>
        {currentEngine && (
          <p className="muted" style={{ gridColumn: "1 / -1", margin: 0, fontSize: "0.8rem" }}>
            {currentEngine.description}
          </p>
        )}

        {(currentEngine?.config_fields ?? []).map((f) =>
          f.type === "boolean" ? (
            <label key={f.config_key} style={{ gridColumn: "1 / -1" }}>
              <input
                type="checkbox"
                checked={values[f.config_key] === true}
                onChange={(e) => setValues((v) => ({ ...v, [f.config_key]: e.target.checked }))}
              />
              {f.label}
            </label>
          ) : f.type === "select" ? (
            <label key={f.config_key} style={{ gridColumn: "1 / -1" }}>
              {f.label}
              {f.required ? " *" : ""}
              <select
                value={String(values[f.config_key] ?? "")}
                onChange={(e) => setValues((v) => ({ ...v, [f.config_key]: e.target.value }))}
              >
                {(f.options ?? []).map((o) => (
                  <option key={o.value} value={o.value}>
                    {o.label}
                  </option>
                ))}
              </select>
            </label>
          ) : (
            <label key={f.config_key} style={{ gridColumn: "1 / -1" }}>
              {f.label}
              {f.required ? " *" : ""}
              <input
                type={f.type === "number" ? "number" : "text"}
                value={String(values[f.config_key] ?? "")}
                placeholder={f.placeholder}
                onChange={(e) => setValues((v) => ({ ...v, [f.config_key]: e.target.value }))}
              />
            </label>
          ),
        )}
      </div>

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
        <button
          className="btn-primary"
          onClick={save}
          disabled={saving || missingRequired}
          title="Save engine selection"
        >
          {saving ? <span className="btn-spinner" /> : <Save size={14} />}
        </button>
        {msg && <span className="success-text">{msg}</span>}
        {error && <span className="error-text">{error}</span>}
      </div>
    </div>
  );
}
