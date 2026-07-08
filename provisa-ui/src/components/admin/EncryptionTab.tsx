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
  fetchEncryption,
  setEncryption,
  generateEncryptionKey,
  type EncryptionState,
} from "../../api/admin";

// REQ-918: manage the encryption provider + master key. The provider binds at startup, so provider
// changes take effect on restart; generating a key stores it in the OS keychain immediately.
export function EncryptionTab() {
  const [s, setS] = useState<EncryptionState | null>(null);
  const [provider, setProvider] = useState("null");
  const [keyId, setKeyId] = useState("");
  const [saving, setSaving] = useState(false);
  const [generating, setGenerating] = useState(false);
  const [msg, setMsg] = useState("");
  const [error, setError] = useState("");
  const [generatedKey, setGeneratedKey] = useState<string | null>(null);

  const load = () =>
    fetchEncryption()
      .then((e) => {
        setS(e);
        setProvider(e.provider);
        setKeyId(e.key_id ?? "");
      })
      .catch((e) => setError(String(e)));

  useEffect(() => {
    load();
  }, []);

  const current = useMemo(() => s?.providers.find((p) => p.key === provider), [s, provider]);

  const save = async () => {
    setSaving(true);
    setMsg("");
    setError("");
    try {
      const res = await setEncryption({ provider, key_id: keyId || null });
      setMsg(res.restart_required ? "Saved. Restart the service to apply." : "Saved.");
      load();
    } catch (e) {
      setError(String(e));
    } finally {
      setSaving(false);
    }
  };

  const generate = async () => {
    setGenerating(true);
    setMsg("");
    setError("");
    setGeneratedKey(null);
    try {
      const res = await generateEncryptionKey({ key_id: keyId || null });
      if (res.stored) {
        setMsg(`Master key generated and stored in the OS keychain (key id: ${res.key_id}).`);
      } else {
        setGeneratedKey(res.key_b64);
        setMsg(
          `No OS keychain available — copy this key and set it as ${res.env_var}. It will not be shown again.`,
        );
      }
      load();
    } catch (e) {
      setError(String(e));
    } finally {
      setGenerating(false);
    }
  };

  if (error && !s) return <div className="error-banner">{error}</div>;
  if (!s) return <div>Loading…</div>;

  return (
    <div className="encryption-tab" style={{ maxWidth: 720 }}>
      <p className="muted">
        Column encryption at rest. Choose a provider and manage its master key. Envelope encryption
        wraps a per-column data key with the master key.
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

        {provider === "local" && (
          <>
            <label style={{ gridColumn: "1 / -1" }}>
              Key ID
              <input
                type="text"
                value={keyId}
                placeholder="master"
                onChange={(e) => setKeyId(e.target.value)}
              />
            </label>

            <div
              style={{
                gridColumn: "1 / -1",
                display: "flex",
                gap: "0.75rem",
                alignItems: "center",
              }}
            >
              <span>
                Master key:{" "}
                {s.key_present ? (
                  <strong style={{ color: "#2e7d32" }}>present</strong>
                ) : (
                  <strong style={{ color: "#c62828" }}>missing</strong>
                )}
              </span>
              <button className="btn-secondary" onClick={generate} disabled={generating}>
                {generating
                  ? "Generating…"
                  : s.key_present
                    ? "Rotate master key"
                    : "Generate master key"}
              </button>
            </div>

            {generatedKey && (
              <div
                style={{
                  gridColumn: "1 / -1",
                  padding: "0.5rem 0.75rem",
                  border: "1px solid #c62828",
                  borderRadius: 4,
                  wordBreak: "break-all",
                  fontFamily: "monospace",
                  fontSize: "0.85rem",
                }}
              >
                {generatedKey}
              </div>
            )}
          </>
        )}
      </div>

      <div
        className="warn-banner"
        style={{ marginTop: "1rem", padding: "0.5rem 0.75rem", border: "1px solid #b8860b", borderRadius: 4 }}
      >
        ⚠ {s.restart_required_note} Rotating a master key while encrypted data exists may make prior
        rows unreadable — rotate before storing sensitive data, or plan a re-encryption.
      </div>

      <div style={{ marginTop: "1rem", display: "flex", gap: "0.75rem", alignItems: "center" }}>
        <button className="btn-primary" onClick={save} disabled={saving}>
          {saving ? "Saving…" : "Save encryption settings"}
        </button>
        {msg && <span className="success-text">{msg}</span>}
        {error && <span className="error-text">{error}</span>}
      </div>
    </div>
  );
}
