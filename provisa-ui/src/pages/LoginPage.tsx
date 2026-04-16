// Copyright (c) 2026 Kenneth Stott
// Canary: ce0e4d9c-dae1-40a1-a524-77b59b7c7bec
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { useState } from "react";
import type { FormEvent } from "react";

const API_BASE = import.meta.env.VITE_API_BASE || "http://localhost:8001";

interface LoginPageProps {
  onLoginSuccess: (token: string) => void;
  authDisabled?: boolean;
}

export function LoginPage({ onLoginSuccess, authDisabled }: LoginPageProps) {
  const [username, setUsername] = useState("");
  const [password, setPassword] = useState("");
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);

  if (authDisabled) {
    return (
      <div className="page">
        <h2>Login</h2>
        <p>Authentication not configured</p>
      </div>
    );
  }

  const handleSubmit = async (e: FormEvent) => {
    e.preventDefault();
    setError(null);
    setLoading(true);

    const resp = await fetch(`${API_BASE}/auth/login`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ username, password }),
    });

    if (!resp.ok) {
      const body = await resp.json().catch(() => ({ detail: resp.statusText }));
      setError(body.detail || `Login failed: ${resp.status}`);
      setLoading(false);
      return;
    }

    const data = await resp.json();
    localStorage.setItem("provisa_token", data.access_token);
    setLoading(false);
    onLoginSuccess(data.access_token);
  };

  return (
    <div className="page">
      <h2>Login</h2>
      <form onSubmit={handleSubmit} style={{ maxWidth: 360 }}>
        <div style={{ marginBottom: 12 }}>
          <label htmlFor="username">Username</label>
          <input
            id="username"
            type="text"
            value={username}
            onChange={(e) => setUsername(e.target.value)}
            required
            autoComplete="username"
            style={{ display: "block", width: "100%", marginTop: 4 }}
          />
        </div>
        <div style={{ marginBottom: 12 }}>
          <label htmlFor="password">Password</label>
          <input
            id="password"
            type="password"
            value={password}
            onChange={(e) => setPassword(e.target.value)}
            required
            autoComplete="current-password"
            style={{ display: "block", width: "100%", marginTop: 4 }}
          />
        </div>
        {error && <div style={{ color: "red", marginBottom: 12 }}>{error}</div>}
        <button type="submit" className="btn-primary" disabled={loading}>
          {loading ? "Logging in..." : "Login"}
        </button>
      </form>
    </div>
  );
}
