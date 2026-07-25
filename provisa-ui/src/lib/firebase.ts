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
  GoogleAuthProvider,
  signInWithPopup,
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

const provider = new GoogleAuthProvider();
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
  const result = await signInWithPopup(firebaseAuth(), provider);
  return result.user.getIdToken();
}

// Tear down the Firebase session so a later sign-in shows the Google account chooser
// instead of silently reusing the persisted account. No-op on basic/none deploys (no
// firebase config), so logout stays safe for every provider.
export async function signOutFirebase(): Promise<void> {
  if (!hasFirebaseConfig()) return;
  await fbSignOut(firebaseAuth());
}
