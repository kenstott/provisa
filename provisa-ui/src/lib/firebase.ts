// Copyright (c) 2026 Kenneth Stott
// Canary: 8944fb3e-8cd6-47dc-92a1-d3deb680a931
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { initializeApp, getApps, type FirebaseApp } from "firebase/app";
import {
  getAuth,
  GithubAuthProvider,
  GoogleAuthProvider,
  OAuthProvider,
  onIdTokenChanged,
  signInWithPopup,
  createUserWithEmailAndPassword,
  signInWithEmailAndPassword,
  signOut as fbSignOut,
  type Auth,
} from "firebase/auth";

type FirebaseWebConfig = { apiKey: string; authDomain: string; projectId: string };

// True only on a firebase deploy (config injected at runtime or baked at build time).
// Used to decide whether there is a Firebase session to tear down on logout.
function hasFirebaseConfig(): boolean {
  const injected = (
    window as unknown as { __PROVISA_FIREBASE__?: FirebaseWebConfig | null }
  ).__PROVISA_FIREBASE__;
  if (injected) return true;
  const env = (import.meta as unknown as Record<string, Record<string, string>>).env;
  return Boolean(env.VITE_FIREBASE_API_KEY && env.VITE_FIREBASE_AUTH_DOMAIN && env.VITE_FIREBASE_PROJECT_ID);
}

// The web config source: runtime-injected first, build-time env second (REQ-1266).
// In a cloud deploy ui_server serves /firebase-config.js from the node env, which
// index.html loads before the app bundle — so one built image serves any Firebase
// project. Local dev builds instead bake VITE_FIREBASE_* at vite build time.
function resolveFirebaseConfig(): FirebaseWebConfig {
  const injected = (
    window as unknown as { __PROVISA_FIREBASE__?: FirebaseWebConfig | null }
  ).__PROVISA_FIREBASE__;
  if (injected) return injected;

  const env = (import.meta as unknown as Record<string, Record<string, string>>).env;
  const apiKey = env.VITE_FIREBASE_API_KEY;
  const authDomain = env.VITE_FIREBASE_AUTH_DOMAIN;
  const projectId = env.VITE_FIREBASE_PROJECT_ID;
  if (!apiKey || !authDomain || !projectId) {
    throw new Error(
      "Firebase web config missing: the server did not inject /firebase-config.js " +
        "and VITE_FIREBASE_* was not built in.",
    );
  }
  return { apiKey, authDomain, projectId };
}

const googleProvider = new GoogleAuthProvider();
// Request the profile + email scopes explicitly so the Firebase ID token carries
// name/email/picture claims the backend reads into AuthIdentity (firebase.py). Google
// grants these by default, but naming them keeps the consent screen honest about what
// Provisa reads and matches the GitHub provider below, which must ask for email.
googleProvider.addScope("profile");
googleProvider.addScope("email");
// Force the Google account chooser on every sign-in. Without prompt=select_account
// Google silently reuses the single active browser session and never asks which
// account to use — wrong for a shared machine and for the bootstrap super-admin
// capture, where the operator must deliberately pick the identity that claims the
// slot. This makes "which account?" the explicit first step of the auth flow.
googleProvider.setCustomParameters({ prompt: "select_account" });

const githubProvider = new GithubAuthProvider();
// read:user + user:email so the token carries the GitHub display name and a verified
// email even when the user keeps their email private — without user:email GitHub omits
// it and the backend gets a nameless/emailless identity.
githubProvider.addScope("read:user");
githubProvider.addScope("user:email");
// allow_signup=false keeps the OAuth screen on the "authorize" step; prompt=consent so
// the operator explicitly picks/authorizes an account rather than silent reuse.
githubProvider.setCustomParameters({ prompt: "consent" });

// Microsoft (Azure AD / Entra ID) via Firebase's generic OAuth provider. Enable the
// Microsoft sign-in method in the Firebase console with the Azure app registration's
// Application (client) ID + secret; the ID token Firebase mints is the same shape as
// Google/GitHub, so the backend (firebase.py) reads name/email identically. openid +
// email + profile so the token carries the claims AuthIdentity needs.
const microsoftProvider = new OAuthProvider("microsoft.com");
microsoftProvider.addScope("openid");
microsoftProvider.addScope("email");
microsoftProvider.addScope("profile");
// prompt=select_account forces the account chooser (parity with Google) so the operator
// deliberately picks the identity — right for shared machines and superadmin capture.
// tenant defaults to "common" (any Entra tenant + personal accounts); set VITE_AZURE_TENANT
// to a tenant ID to restrict sign-in to one org's directory.
microsoftProvider.setCustomParameters(
  ((import.meta as unknown as Record<string, Record<string, string>>).env.VITE_AZURE_TENANT
    ? {
        prompt: "select_account",
        tenant: (import.meta as unknown as Record<string, Record<string, string>>).env
          .VITE_AZURE_TENANT,
      }
    : { prompt: "select_account" }) as Record<string, string>,
);

let cachedAuth: Auth | null = null;

// Lazily initialize so a missing config surfaces to the sign-in caller (LoginPage
// catches it and shows the error) rather than throwing at module import.
function firebaseAuth(): Auth {
  if (cachedAuth) return cachedAuth;
  const config = resolveFirebaseConfig();
  const app: FirebaseApp = getApps().length === 0 ? initializeApp(config) : getApps()[0];
  cachedAuth = getAuth(app);
  return cachedAuth;
}

export async function signInWithGoogle(): Promise<string> {
  const result = await signInWithPopup(firebaseAuth(), googleProvider);
  return result.user.getIdToken();
}

export async function signInWithGithub(): Promise<string> {
  const result = await signInWithPopup(firebaseAuth(), githubProvider);
  return result.user.getIdToken();
}

export async function signInWithMicrosoft(): Promise<string> {
  const result = await signInWithPopup(firebaseAuth(), microsoftProvider);
  return result.user.getIdToken();
}

// Firebase Email/Password sign-in — the "local" auth method (self-hosted look, but
// credentials live in the Firebase Identity Platform user pool, not local_users). The
// ID token Firebase mints is the same shape as the social providers, so the backend
// (firebase.py) reads name/email identically.
export async function signInWithEmailPassword(email: string, password: string): Promise<string> {
  const result = await signInWithEmailAndPassword(firebaseAuth(), email, password);
  return result.user.getIdToken();
}

export async function registerWithEmailPassword(email: string, password: string): Promise<string> {
  const result = await createUserWithEmailAndPassword(firebaseAuth(), email, password);
  return result.user.getIdToken();
}

// REQ-1266: Firebase ID tokens expire after ~1h. The SDK rotates the token internally,
// but authFetch/apolloClient read a static copy from localStorage, so without this the
// stored bearer goes stale and every request 401s an hour after sign-in (and on any
// reload of an older session). onIdTokenChanged fires on registration with the restored
// user, then on each background refresh and on sign-out — mirror the live token into
// localStorage each time so the interceptors always send a valid bearer.
//
// The returned promise resolves once the FIRST token state is settled (fresh token written,
// or cleared when signed out / refresh rejected). main.tsx awaits it before rendering so the
// dashboard's initial queries never fire against a stale/expired localStorage token — the
// boot-time race that 401'd every call on reload of an hour-old session. No-op (resolves
// immediately) on non-firebase deploys.
export function installFirebaseTokenSync(): Promise<void> {
  if (!hasFirebaseConfig()) return Promise.resolve();
  return new Promise<void>((resolve) => {
    let settled = false;
    const settle = () => {
      if (!settled) {
        settled = true;
        resolve();
      }
    };
    onIdTokenChanged(firebaseAuth(), (user) => {
      if (user) {
        // getIdToken() returns the cached token, transparently refreshing if it is expired or
        // near expiry, so this always writes a currently-valid token.
        user
          .getIdToken()
          .then((idToken) => localStorage.setItem("provisa_token", idToken))
          .catch((err) => {
            // A rejected refresh (revoked/disabled session) is a legitimate signed-out state,
            // not a bug to swallow: surface it, clear the dead bearer, and let boot proceed to
            // the login page rather than hanging render forever.
            console.error("Firebase token refresh failed:", err);
            localStorage.removeItem("provisa_token");
          })
          .finally(settle);
      } else {
        localStorage.removeItem("provisa_token");
        settle();
      }
    });
  });
}

// Tear down the Firebase session so a later sign-in shows the Google account chooser
// instead of silently reusing the persisted account. No-op on basic/none deploys (no
// firebase config), so logout stays safe for every provider.
export async function signOutFirebase(): Promise<void> {
  if (!hasFirebaseConfig()) return;
  await fbSignOut(firebaseAuth());
}
