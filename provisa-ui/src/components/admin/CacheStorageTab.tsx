// Copyright (c) 2026 Kenneth Stott
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { useEffect, useState } from "react";
import { Save } from "lucide-react";
import { fetchCacheStorage, setCacheStorage, type CacheStorageState } from "../../api/admin";

// REQ-917: configure the Redis hot cache + materialize store. Both bind connections at startup,
// so changes take effect on the next service restart.
export function CacheStorageTab() {
  const [s, setS] = useState<CacheStorageState | null>(null);
  const [saving, setSaving] = useState(false);
  const [msg, setMsg] = useState("");
  const [error, setError] = useState("");

  useEffect(() => {
    fetchCacheStorage().then(setS).catch((e) => setError(String(e)));
  }, []);

  const save = async () => {
    if (!s) return;
    setSaving(true);
    setMsg("");
    setError("");
    try {
      const res = await setCacheStorage({
        cache: s.cache,
        hot_tables: s.hot_tables,
        materialize: s.materialize,
      });
      setMsg(res.restart_required ? "Saved. Restart the service to apply." : "Saved.");
    } catch (e) {
      setError(String(e));
    } finally {
      setSaving(false);
    }
  };

  if (error && !s) return <div className="error-banner">{error}</div>;
  if (!s) return <div>Loading…</div>;

  const num = (v: string) => (v.trim() === "" ? null : Number(v));

  return (
    <div className="cache-storage-tab" style={{ maxWidth: 720 }}>
      <h3>Hot Cache (Redis)</h3>
      <p className="muted">
        The hot cache promotes frequently-queried tables into cache. Leave the URL empty to use the
        embedded in-process cache.
      </p>

      <div className="form-card">
        <label style={{ gridColumn: "1 / -1" }}>
          <input
            type="checkbox"
            checked={s.cache.enabled}
            onChange={(e) => setS({ ...s, cache: { ...s.cache, enabled: e.target.checked } })}
          />
          Enable hot cache
        </label>

        <label style={{ gridColumn: "1 / -1" }}>
          Redis URL
          <input
            type="text"
            value={s.cache.redis_url}
            placeholder="redis://:password@host:6379/0  (empty → embedded)"
            onChange={(e) => setS({ ...s, cache: { ...s.cache, redis_url: e.target.value } })}
          />
        </label>

        <label>
          Default TTL (s)
          <input
            type="number"
            value={s.cache.default_ttl ?? ""}
            onChange={(e) => setS({ ...s, cache: { ...s.cache, default_ttl: num(e.target.value) } })}
          />
        </label>
        <label>
          Promote after N queries
          <input
            type="number"
            value={s.hot_tables.auto_threshold}
            onChange={(e) =>
              setS({
                ...s,
                hot_tables: { ...s.hot_tables, auto_threshold: Number(e.target.value) },
              })
            }
          />
        </label>
        <label>
          Max rows
          <input
            type="number"
            value={s.hot_tables.max_rows}
            onChange={(e) =>
              setS({ ...s, hot_tables: { ...s.hot_tables, max_rows: Number(e.target.value) } })
            }
          />
        </label>
        <label>
          Max bytes
          <input
            type="number"
            value={s.hot_tables.max_bytes}
            onChange={(e) =>
              setS({ ...s, hot_tables: { ...s.hot_tables, max_bytes: Number(e.target.value) } })
            }
          />
        </label>
      </div>

      <h3>Materialize Store</h3>
      <p className="muted">
        Durable store where non-attachable sources (OpenAPI, GraphQL) and materialized views are
        landed. Leave empty to use the active federation engine's default
        {s.materialize.default_store_url ? (
          <> — <code>{s.materialize.default_store_url}</code></>
        ) : (
          <> — this engine declares none, so a URL is required</>
        )}
        . Set a URL to override.
      </p>
      <div className="form-card">
        <label style={{ gridColumn: "1 / -1" }}>
          Store URL
          <input
            type="text"
            value={s.materialize.store_url}
            placeholder={
              s.materialize.default_store_url
                ? `empty → ${s.materialize.default_store_url}`
                : "postgresql://user:pass@host:5432/materialize (required)"
            }
            onChange={(e) =>
              setS({ ...s, materialize: { ...s.materialize, store_url: e.target.value } })
            }
          />
        </label>
      </div>

      <div
        className="warn-banner"
        style={{ marginTop: "1rem", padding: "0.5rem 0.75rem", border: "1px solid #b8860b", borderRadius: 4 }}
      >
        ⚠ {s.restart_required_note}
      </div>

      <div style={{ marginTop: "1rem", display: "flex", gap: "0.75rem", alignItems: "center" }}>
        <button
          className="btn-primary"
          onClick={save}
          disabled={saving}
          title="Save cache & storage settings"
        >
          {saving ? <span className="btn-spinner" /> : <Save size={14} />}
        </button>
        {msg && <span className="success-text">{msg}</span>}
        {error && <span className="error-text">{error}</span>}
      </div>
    </div>
  );
}
