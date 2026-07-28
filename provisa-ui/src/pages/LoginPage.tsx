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
import { useNavigate } from "react-router-dom";
import { Alert, Button, PasswordInput, Stack, Text, TextInput, Title } from "@mantine/core";
import { useTranslation } from "react-i18next";
import {
  fetchProviderType,
  fetchBootstrapStatus,
  claimBootstrap,
  registerAccount,
  fetchInviteInfo,
  redeemInvite,
} from "../api/admin";
import type { InviteInfo } from "../api/admin";
import { CLAIMED_ADMIN_FLAG } from "../components/PlatformAdminWelcomeModal";

const API_BASE = import.meta.env.VITE_API_BASE || "";

interface LoginPageProps {
  onLoginSuccess: (token: string) => void;
  authDisabled?: boolean;
}

export function LoginPage({ onLoginSuccess, authDisabled }: LoginPageProps) {
  const { t } = useTranslation();
  const navigate = useNavigate();
  const [provider, setProvider] = useState<string | null>(null);
  const [providerLoading, setProviderLoading] = useState(true);
  // REQ-1288: the first identity to authenticate silently becomes the platform admin. Say so
  // before any provider is picked, so signing in is a decision rather than a surprise.
  const [firstLogin, setFirstLogin] = useState(false);

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

    // Kept independent of the provider fetch: a failure here must not decide which sign-in UI
    // renders. The banner is an added warning, so its absence degrades to today's behavior.
    fetchBootstrapStatus()
      .then(setFirstLogin)
      .catch(() => setFirstLogin(false));

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

  // REQ-1294: claiming happens behind a provider redirect, and the app shell that must disclose it
  // is a different render tree than this page. Record the claim so the shell can state, once, what
  // this sign-in just made the user. Only a true response sets it — a claim the server refused
  // (the slot was already taken) must not produce a "you are now the administrator" modal.
  const claimAndRecord = async () => {
    if (await claimBootstrap()) {
      localStorage.setItem(CLAIMED_ADMIN_FLAG, "1");
    }
  };

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
    // REQ-1290: same explicit claim as the Firebase paths — the server never claims the
    // platform-admin slot on its own, so signing in from the first-login page is what takes it.
    if (firstLogin) {
      await claimAndRecord();
    }
    setLoading(false);
    onLoginSuccess(data.access_token);
    // Leave /login now that a token exists, or the /login route keeps rendering this page.
    navigate("/", { replace: true });
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

  const handleFirebaseLogin = async (idp: "google" | "github" | "microsoft") => {
    setError(null);
    setLoading(true);
    try {
      const firebase = await import("../lib/firebase");
      const idToken =
        idp === "github"
          ? await firebase.signInWithGithub()
          : idp === "microsoft"
            ? await firebase.signInWithMicrosoft()
            : await firebase.signInWithGoogle();
      localStorage.setItem("provisa_token", idToken);
      // REQ-1290: this click IS the consent the first-login notice asked for, so claim the
      // platform-admin slot here. The server never claims it on its own, which is why a refresh
      // with a still-valid token can no longer take it before this page has been seen.
      if (firstLogin) {
        await claimAndRecord();
      }
      // A bearer identity has no /register step, so an ?invite= link is redeemed here — after the
      // token is stored (authFetch attaches it) and before navigating — to add org membership + the
      // invite's role. This is how a GitHub-authed user becomes the first admin of an invited org.
      const inviteToken = new URLSearchParams(window.location.search).get("invite");
      if (inviteToken) {
        await redeemInvite(inviteToken);
      }
      onLoginSuccess(idToken);
      // Leave /login now that a token exists, or the /login route keeps rendering this page.
      navigate("/", { replace: true });
    } catch (err) {
      setError(err instanceof Error ? err.message : t("loginPage.firebaseSignInFailed"));
    } finally {
      setLoading(false);
    }
  };

  const handleFirebaseEmail = async (e: FormEvent) => {
    e.preventDefault();
    setError(null);
    if (mode === "register" && password !== confirmPassword) {
      setError(t("loginPage.passwordsDoNotMatch"));
      return;
    }
    setLoading(true);
    try {
      const firebase = await import("../lib/firebase");
      const idToken =
        mode === "register"
          ? await firebase.registerWithEmailPassword(regEmail, password)
          : await firebase.signInWithEmailPassword(regEmail, password);
      localStorage.setItem("provisa_token", idToken);
      // REQ-1290: same explicit claim as the provider buttons — submitting this form on the
      // first-login page is the deliberate act that takes the platform-admin slot.
      if (firstLogin) {
        await claimAndRecord();
      }
      const inviteToken = new URLSearchParams(window.location.search).get("invite");
      if (inviteToken) {
        await redeemInvite(inviteToken);
      }
      onLoginSuccess(idToken);
      navigate("/", { replace: true });
    } catch (err) {
      setError(err instanceof Error ? err.message : t("loginPage.firebaseSignInFailed"));
    } finally {
      setLoading(false);
    }
  };

  const firstLoginNotice = firstLogin ? (
    <Alert color="blue" mb="md" title={t("loginPage.firstLoginTitle")} data-testid="first-login-notice">
      {t("loginPage.firstLoginBody")}
    </Alert>
  ) : null;

  if (provider === "firebase") {
    return (
      <div className="page">
        <Title order={2}>
          {firstLogin ? t("loginPage.firstLoginHeading") : t("loginPage.signInTitle")}
        </Title>
        {firstLoginNotice}
        {error && (
          <Alert color="red" mb="md" data-testid="login-error">
            {error}
          </Alert>
        )}
        <Stack gap="sm" style={{ maxWidth: 320 }}>
          <Button
            data-testid="firebase-signin-button"
            onClick={() => handleFirebaseLogin("google")}
            disabled={loading}
          >
            {loading ? t("loginPage.signingIn") : t("loginPage.signInWithGoogle")}
          </Button>
          <Button
            variant="default"
            data-testid="firebase-signin-github-button"
            onClick={() => handleFirebaseLogin("github")}
            disabled={loading}
          >
            {loading ? t("loginPage.signingIn") : t("loginPage.signInWithGithub")}
          </Button>
          <Button
            variant="default"
            data-testid="firebase-signin-microsoft-button"
            onClick={() => handleFirebaseLogin("microsoft")}
            disabled={loading}
          >
            {loading ? t("loginPage.signingIn") : t("loginPage.signInWithMicrosoft")}
          </Button>
          <Text c="dimmed" size="sm" ta="center">
            {t("loginPage.orDivider")}
          </Text>
          <form onSubmit={handleFirebaseEmail}>
            <Stack gap="sm">
              <TextInput
                type="email"
                label={t("loginPage.email")}
                data-testid="firebase-email-input"
                value={regEmail}
                onChange={(e) => setRegEmail(e.currentTarget.value)}
                required
              />
              <PasswordInput
                label={t("loginPage.password")}
                data-testid="firebase-password-input"
                value={password}
                onChange={(e) => setPassword(e.currentTarget.value)}
                required
              />
              {mode === "register" && (
                <PasswordInput
                  label={t("loginPage.confirmPassword")}
                  data-testid="firebase-confirm-password-input"
                  value={confirmPassword}
                  onChange={(e) => setConfirmPassword(e.currentTarget.value)}
                  required
                />
              )}
              <Button type="submit" data-testid="firebase-email-submit" disabled={loading}>
                {loading
                  ? t("loginPage.signingIn")
                  : mode === "register"
                    ? t("loginPage.createAccount")
                    : t("loginPage.signInWithEmail")}
              </Button>
              <Button
                variant="subtle"
                size="compact-sm"
                data-testid="firebase-email-toggle"
                onClick={() => {
                  setError(null);
                  setMode(mode === "register" ? "login" : "register");
                }}
              >
                {mode === "register"
                  ? t("loginPage.haveAccountSignIn")
                  : t("loginPage.needAccountRegister")}
              </Button>
            </Stack>
          </form>
        </Stack>
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
      <Title order={2}>
        {firstLogin ? t("loginPage.firstLoginHeading") : t("loginPage.loginTitle")}
      </Title>
      {firstLoginNotice}
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
