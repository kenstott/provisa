// Copyright (c) 2026 Kenneth Stott
// Canary: 56f95443-fb45-4d9f-94bd-13ad316b8806
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { useState } from "react";
import type { FormEvent } from "react";
import { runSetup } from "../api/setup";

interface SetupPageProps {
  onSetupComplete: () => void;
}

export function SetupPage({ onSetupComplete }: SetupPageProps) {
  const [step, setStep] = useState<1 | 2 | 3>(1);
  const [mode, setMode] = useState<"single" | "multi">("single");
  const [provider, setProvider] = useState<"basic" | "firebase" | "none">("basic");
  const [adminUsername, setAdminUsername] = useState("admin");
  const [adminPassword, setAdminPassword] = useState("");
  const [confirmPassword, setConfirmPassword] = useState("");
  const [firebaseProjectId, setFirebaseProjectId] = useState("");
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);

  const handleFinish = async (e: FormEvent) => {
    e.preventDefault();
    setError(null);
    if (provider === "basic" && adminPassword !== confirmPassword) {
      setError("Passwords do not match");
      return;
    }
    setLoading(true);
    try {
      await runSetup({
        provider,
        mode,
        admin_username: provider === "basic" ? adminUsername : undefined,
        admin_password: provider === "basic" ? adminPassword : undefined,
        firebase_project_id: provider === "firebase" ? firebaseProjectId : undefined,
      });
      onSetupComplete();
    } catch (err) {
      setError(err instanceof Error ? err.message : "Setup failed");
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="page" style={{ maxWidth: 480, margin: "80px auto" }}>
      <h2>Provisa Setup</h2>
      <div style={{ marginBottom: 24, color: "var(--muted-foreground)", fontSize: 13 }}>
        Step {step} of 3
      </div>

      {step === 1 && (
        <div>
          <h3>Deployment Mode</h3>
          <label style={{ display: "block", marginBottom: 12, cursor: "pointer" }}>
            <input type="radio" name="mode" value="single" checked={mode === "single"}
              onChange={() => setMode("single")} style={{ marginRight: 8 }} />
            <strong>Single-tenant</strong>
            <div style={{ marginLeft: 24, fontSize: 13, color: "var(--muted-foreground)" }}>
              One organization. All users share a single workspace.
            </div>
          </label>
          <label style={{ display: "block", marginBottom: 24, cursor: "pointer" }}>
            <input type="radio" name="mode" value="multi" checked={mode === "multi"}
              onChange={() => setMode("multi")} style={{ marginRight: 8 }} />
            <strong>Multi-tenant</strong>
            <div style={{ marginLeft: 24, fontSize: 13, color: "var(--muted-foreground)" }}>
              Multiple organizations with isolated access. Users are assigned to orgs via invite links.
            </div>
          </label>
          <button className="btn-primary" onClick={() => setStep(2)}>Next</button>
        </div>
      )}

      {step === 2 && (
        <div>
          <h3>Identity Provider</h3>
          <label style={{ display: "block", marginBottom: 12, cursor: "pointer" }}>
            <input type="radio" name="provider" value="basic" checked={provider === "basic"}
              onChange={() => setProvider("basic")} style={{ marginRight: 8 }} />
            <strong>Basic Auth</strong>
            <div style={{ marginLeft: 24, fontSize: 13, color: "var(--muted-foreground)" }}>
              Username and password stored in Provisa. Self-service registration or invite-based.
            </div>
          </label>
          <label style={{ display: "block", marginBottom: 12, cursor: "pointer" }}>
            <input type="radio" name="provider" value="firebase" checked={provider === "firebase"}
              onChange={() => setProvider("firebase")} style={{ marginRight: 8 }} />
            <strong>Firebase (Google)</strong>
            <div style={{ marginLeft: 24, fontSize: 13, color: "var(--muted-foreground)" }}>
              Sign in with Google via Firebase Authentication. Requires Firebase project configuration.
            </div>
          </label>
          <label style={{ display: "block", marginBottom: 24, cursor: "pointer" }}>
            <input type="radio" name="provider" value="none" checked={provider === "none"}
              onChange={() => setProvider("none")} style={{ marginRight: 8 }} />
            <strong>None (no authentication)</strong>
            <div style={{ marginLeft: 24, fontSize: 13, color: "var(--muted-foreground)" }}>
              All users have full access. Suitable for local development only.
            </div>
          </label>
          <div style={{ display: "flex", gap: 8 }}>
            <button onClick={() => setStep(1)}>Back</button>
            {provider === "none" ? (
              <button
                className="btn-primary"
                disabled={loading}
                onClick={async () => {
                  setError(null);
                  setLoading(true);
                  try {
                    await runSetup({ provider: "none", mode });
                    onSetupComplete();
                  } catch (err) {
                    setError(err instanceof Error ? err.message : "Setup failed");
                  } finally {
                    setLoading(false);
                  }
                }}
              >
                {loading ? "Setting up..." : "Complete Setup"}
              </button>
            ) : (
              <button className="btn-primary" onClick={() => setStep(3)}>Next</button>
            )}
          </div>
        </div>
      )}

      {step === 3 && (
        <form onSubmit={handleFinish}>
          {provider === "basic" && (
            <>
              <h3>Admin Account</h3>
              <div style={{ marginBottom: 12 }}>
                <label htmlFor="setup-username">Username</label>
                <input id="setup-username" type="text" value={adminUsername}
                  onChange={(e) => setAdminUsername(e.target.value)} required
                  autoComplete="username"
                  style={{ display: "block", width: "100%", marginTop: 4 }} />
              </div>
              <div style={{ marginBottom: 12 }}>
                <label htmlFor="setup-password">Password</label>
                <input id="setup-password" type="password" value={adminPassword}
                  onChange={(e) => setAdminPassword(e.target.value)} required
                  autoComplete="new-password"
                  style={{ display: "block", width: "100%", marginTop: 4 }} />
              </div>
              <div style={{ marginBottom: 20 }}>
                <label htmlFor="setup-confirm">Confirm Password</label>
                <input id="setup-confirm" type="password" value={confirmPassword}
                  onChange={(e) => setConfirmPassword(e.target.value)} required
                  autoComplete="new-password"
                  style={{ display: "block", width: "100%", marginTop: 4 }} />
              </div>
            </>
          )}
          {provider === "firebase" && (
            <>
              <h3>Firebase Configuration</h3>
              <div style={{ marginBottom: 12 }}>
                <label htmlFor="setup-project-id">Firebase Project ID</label>
                <input id="setup-project-id" type="text" value={firebaseProjectId}
                  onChange={(e) => setFirebaseProjectId(e.target.value)} required
                  placeholder="my-firebase-project"
                  style={{ display: "block", width: "100%", marginTop: 4 }} />
              </div>
              <div style={{ marginBottom: 20, padding: 12, background: "var(--muted)", borderRadius: 4, fontSize: 13 }}>
                <strong>Required environment variables:</strong>
                <ul style={{ margin: "8px 0 0 16px", padding: 0 }}>
                  <li>VITE_FIREBASE_API_KEY</li>
                  <li>VITE_FIREBASE_AUTH_DOMAIN</li>
                  <li>VITE_FIREBASE_PROJECT_ID</li>
                  <li>FIREBASE_PROJECT_ID (backend)</li>
                </ul>
              </div>
            </>
          )}
          {error && <div style={{ color: "var(--destructive)", marginBottom: 12 }}>{error}</div>}
          <div style={{ display: "flex", gap: 8 }}>
            <button type="button" onClick={() => setStep(2)}>Back</button>
            <button type="submit" className="btn-primary" disabled={loading}>
              {loading ? "Setting up..." : "Complete Setup"}
            </button>
          </div>
        </form>
      )}
    </div>
  );
}
