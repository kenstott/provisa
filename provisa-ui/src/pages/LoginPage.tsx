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
import { Alert, Button, PasswordInput, Stack, Text, TextInput, Title } from "@mantine/core";
import { useTranslation } from "react-i18next";
import { fetchProviderType, registerAccount, fetchInviteInfo } from "../api/admin";
import type { InviteInfo } from "../api/admin";

const API_BASE = import.meta.env.VITE_API_BASE || "";

interface LoginPageProps {
  onLoginSuccess: (token: string) => void;
  authDisabled?: boolean;
}

export function LoginPage({ onLoginSuccess, authDisabled }: LoginPageProps) {
  const { t } = useTranslation();
  const [provider, setProvider] = useState<string | null>(null);
  const [providerLoading, setProviderLoading] = useState(true);

  const [mode, setMode] = useState<"login" | "register">(() =>
    new URLSearchParams(window.location.search).get("invite") ? "register" : "login",
  );
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
    }
  }, []);

  if (authDisabled) {
    return (
      <div className="page">
        <Title order={2}>{t("loginPage.loginTitle")}</Title>
        <Text>{t("loginPage.authNotConfigured")}</Text>
      </div>
    );
  }

  if (providerLoading) {
    return (
      <div className="page">
        <Text>{t("loginPage.loading")}</Text>
      </div>
    );
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
      setError(t("loginPage.passwordsDoNotMatch"));
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
      setError(err instanceof Error ? err.message : t("loginPage.registrationFailed"));
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
      setError(err instanceof Error ? err.message : t("loginPage.firebaseSignInFailed"));
    } finally {
      setLoading(false);
    }
  };

  if (provider === "firebase") {
    return (
      <div className="page">
        <Title order={2}>{t("loginPage.signInTitle")}</Title>
        {error && (
          <Alert color="red" mb="md" data-testid="login-error">
            {error}
          </Alert>
        )}
        <Button data-testid="firebase-signin-button" onClick={handleFirebaseLogin} disabled={loading}>
          {loading ? t("loginPage.signingIn") : t("loginPage.signInWithGoogle")}
        </Button>
      </div>
    );
  }

  if (mode === "register" && provider === "basic") {
    return (
      <div className="page">
        <Title order={2}>{t("loginPage.createAccountTitle")}</Title>
        <form onSubmit={handleRegister} style={{ maxWidth: 360 }}>
          <Stack gap="md">
            <TextInput
              id="reg-username"
              label={t("loginPage.username")}
              value={username}
              onChange={(e) => setUsername(e.target.value)}
              required
              withAsterisk={false}
              autoComplete="username"
              data-testid="reg-username-input"
            />
            <TextInput
              id="reg-email"
              type="email"
              label={t("loginPage.email")}
              value={regEmail}
              onChange={(e) => setRegEmail(e.target.value)}
              autoComplete="email"
              data-testid="reg-email-input"
            />
            <TextInput
              id="reg-displayname"
              label={t("loginPage.displayName")}
              value={regDisplayName}
              onChange={(e) => setRegDisplayName(e.target.value)}
              data-testid="reg-displayname-input"
            />
            {inviteInfo && (
              <TextInput
                label={t("loginPage.organization")}
                value={inviteInfo.org_name}
                readOnly
                data-testid="reg-org-input"
              />
            )}
            {inviteError && (
              <Alert color="red" data-testid="invite-error">
                {inviteError}
              </Alert>
            )}
            <PasswordInput
              id="reg-password"
              label={t("loginPage.password")}
              value={password}
              onChange={(e) => setPassword(e.target.value)}
              required
              withAsterisk={false}
              autoComplete="new-password"
              data-testid="reg-password-input"
            />
            <PasswordInput
              id="reg-confirm"
              label={t("loginPage.confirmPassword")}
              value={confirmPassword}
              onChange={(e) => setConfirmPassword(e.target.value)}
              required
              withAsterisk={false}
              autoComplete="new-password"
              data-testid="reg-confirm-input"
            />
            {error && (
              <Alert color="red" data-testid="register-error">
                {error}
              </Alert>
            )}
            <div>
              <Button type="submit" disabled={loading} data-testid="create-account-button">
                {loading ? t("loginPage.creating") : t("loginPage.createAccount")}
              </Button>
              <Button
                type="button"
                variant="default"
                ml="xs"
                onClick={() => {
                  setMode("login");
                  setError(null);
                }}
                data-testid="back-to-login-button"
              >
                {t("loginPage.backToLogin")}
              </Button>
            </div>
          </Stack>
        </form>
      </div>
    );
  }

  return (
    <div className="page">
      <Title order={2}>{t("loginPage.loginTitle")}</Title>
      <form onSubmit={handleBasicLogin} style={{ maxWidth: 360 }}>
        <Stack gap="md">
          <TextInput
            id="username"
            label={t("loginPage.username")}
            value={username}
            onChange={(e) => setUsername(e.target.value)}
            required
            withAsterisk={false}
            autoComplete="username"
            data-testid="username-input"
          />
          <PasswordInput
            id="password"
            label={t("loginPage.password")}
            value={password}
            onChange={(e) => setPassword(e.target.value)}
            required
            withAsterisk={false}
            autoComplete="current-password"
            data-testid="password-input"
          />
          {error && (
            <Alert color="red" data-testid="login-error">
              {error}
            </Alert>
          )}
          <div>
            <Button type="submit" disabled={loading} data-testid="login-button">
              {loading ? t("loginPage.loggingIn") : t("loginPage.loginTitle")}
            </Button>
            {provider === "basic" && (
              <Button
                type="button"
                variant="default"
                ml="xs"
                onClick={() => {
                  setMode("register");
                  setError(null);
                }}
                data-testid="create-account-link-button"
              >
                {t("loginPage.createAccount")}
              </Button>
            )}
          </div>
        </Stack>
      </form>
    </div>
  );
}
