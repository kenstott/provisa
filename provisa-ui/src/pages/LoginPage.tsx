// Copyright (c) 2026 Kenneth Stott
// Canary: ce0e4d9c-dae1-40a1-a524-77b59b7c7bec
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { useState, useEffect } from "react";
import type { FormEvent } from "react";
import { fetchProviderType, registerAccount, fetchInviteInfo } from "../api/admin";
import type { InviteInfo } from "../api/admin";

const API_BASE = import.meta.env.VITE_API_BASE || "";

interface LoginPageProps {
  onLoginSuccess: (token: string) => void;
  authDisabled?: boolean;
}

export function LoginPage({ onLoginSuccess, authDisabled }: LoginPageProps) {
  const [provider, setProvider] = useState<string | null>(null);
  const [providerLoading, setProviderLoading] = useState(true);

  const [mode, setMode] = useState<"login" | "register">("login");
  const [username, setUsername] = useState("");
  const [password, setPassword] = useState("");
  const [confirmPassword, setConfirmPassword] = useState("");
  const [regEmail, setRegEmail] = useState("");
  const [regDisplayName, setRegDisplayName] = useState("");
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);
  const [inviteInfo, setInviteInfo] = useState<InviteInfo | null>(null);
  const [inviteError, setInviteError] = useState<string | null>(null);

  useEffect(() => {
    fetchProviderType()
      .then(setProvider)
      .catch(() => setProvider(null))
      .finally(() => setProviderLoading(false));

    const params = new URLSearchParams(window.location.search);
    const token = params.get("invite");
    if (token) {
      fetchInviteInfo(token)
        .then(setInviteInfo)
        .catch((err) => setInviteError(err.message));
      setMode("register");
    }
  }, []);

  if (authDisabled) {
    return (
      <div className="page">
        <h2>Login</h2>
        <p>Authentication not configured</p>
      </div>
    );
  }

  if (providerLoading) {
    return <div className="page"><p>Loading...</p></div>;
  }

  const handleBasicLogin = async (e: FormEvent) => {
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

  const handleRegister = async (e: FormEvent) => {
    e.preventDefault();
    setError(null);
    if (password !== confirmPassword) {
      setError("Passwords do not match");
      return;
    }
    setLoading(true);
    try {
      await registerAccount({
        username,
        password,
        email: regEmail || undefined,
        display_name: regDisplayName || undefined,
        invite_token: inviteInfo?.token,
      });
      setMode("login");
      setPassword("");
      setConfirmPassword("");
      setError(null);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Registration failed");
    } finally {
      setLoading(false);
    }
  };

  const handleFirebaseLogin = async () => {
    setError(null);
    setLoading(true);
    try {
      const { signInWithGoogle } = await import("../lib/firebase");
      const idToken = await signInWithGoogle();
      localStorage.setItem("provisa_token", idToken);
      onLoginSuccess(idToken);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Firebase sign-in failed");
    } finally {
      setLoading(false);
    }
  };

  if (provider === "firebase") {
    return (
      <div className="page">
        <h2>Sign In</h2>
        {error && <div style={{ color: "var(--destructive)", marginBottom: 12 }}>{error}</div>}
        <button className="btn-primary" onClick={handleFirebaseLogin} disabled={loading}>
          {loading ? "Signing in..." : "Sign in with Google"}
        </button>
      </div>
    );
  }

  if (mode === "register" && provider === "basic") {
    return (
      <div className="page">
        <h2>Create Account</h2>
        <form onSubmit={handleRegister} style={{ maxWidth: 360 }}>
          <div style={{ marginBottom: 12 }}>
            <label htmlFor="reg-username">Username</label>
            <input id="reg-username" type="text" value={username} onChange={(e) => setUsername(e.target.value)}
              required autoComplete="username" style={{ display: "block", width: "100%", marginTop: 4 }} />
          </div>
          <div style={{ marginBottom: 12 }}>
            <label htmlFor="reg-email">Email</label>
            <input id="reg-email" type="email" value={regEmail} onChange={(e) => setRegEmail(e.target.value)}
              autoComplete="email" style={{ display: "block", width: "100%", marginTop: 4 }} />
          </div>
          <div style={{ marginBottom: 12 }}>
            <label htmlFor="reg-displayname">Display Name</label>
            <input id="reg-displayname" type="text" value={regDisplayName} onChange={(e) => setRegDisplayName(e.target.value)}
              style={{ display: "block", width: "100%", marginTop: 4 }} />
          </div>
          {inviteInfo && (
            <div style={{ marginBottom: 12 }}>
              <label>Organization</label>
              <input type="text" value={inviteInfo.org_name} readOnly
                style={{ display: "block", width: "100%", marginTop: 4, opacity: 0.7 }} />
            </div>
          )}
          {inviteError && <div style={{ color: "var(--destructive)", marginBottom: 12 }}>{inviteError}</div>}
          <div style={{ marginBottom: 12 }}>
            <label htmlFor="reg-password">Password</label>
            <input id="reg-password" type="password" value={password} onChange={(e) => setPassword(e.target.value)}
              required autoComplete="new-password" style={{ display: "block", width: "100%", marginTop: 4 }} />
          </div>
          <div style={{ marginBottom: 12 }}>
            <label htmlFor="reg-confirm">Confirm Password</label>
            <input id="reg-confirm" type="password" value={confirmPassword} onChange={(e) => setConfirmPassword(e.target.value)}
              required autoComplete="new-password" style={{ display: "block", width: "100%", marginTop: 4 }} />
          </div>
          {error && <div style={{ color: "var(--destructive)", marginBottom: 12 }}>{error}</div>}
          <button type="submit" className="btn-primary" disabled={loading}>
            {loading ? "Creating..." : "Create Account"}
          </button>
          <button type="button" style={{ marginLeft: 8 }} onClick={() => { setMode("login"); setError(null); }}>
            Back to Login
          </button>
        </form>
      </div>
    );
  }

  return (
    <div className="page">
      <h2>Login</h2>
      <form onSubmit={handleBasicLogin} style={{ maxWidth: 360 }}>
        <div style={{ marginBottom: 12 }}>
          <label htmlFor="username">Username</label>
          <input id="username" type="text" value={username} onChange={(e) => setUsername(e.target.value)}
            required autoComplete="username" style={{ display: "block", width: "100%", marginTop: 4 }} />
        </div>
        <div style={{ marginBottom: 12 }}>
          <label htmlFor="password">Password</label>
          <input id="password" type="password" value={password} onChange={(e) => setPassword(e.target.value)}
            required autoComplete="current-password" style={{ display: "block", width: "100%", marginTop: 4 }} />
        </div>
        {error && <div style={{ color: "var(--destructive)", marginBottom: 12 }}>{error}</div>}
        <button type="submit" className="btn-primary" disabled={loading}>
          {loading ? "Logging in..." : "Login"}
        </button>
        {provider === "basic" && (
          <button type="button" style={{ marginLeft: 8 }} onClick={() => { setMode("register"); setError(null); }}>
            Create Account
          </button>
        )}
      </form>
    </div>
  );
}
