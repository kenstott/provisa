// Copyright (c) 2026 Kenneth Stott
// Canary: 8944fb3e-8cd6-47dc-92a1-d3deb680a931
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { initializeApp, getApps } from "firebase/app";
import { getAuth, GoogleAuthProvider, signInWithPopup } from "firebase/auth";

const firebaseConfig = {
  apiKey: (import.meta as unknown as Record<string, Record<string, string>>).env
    .VITE_FIREBASE_API_KEY,
  authDomain: (import.meta as unknown as Record<string, Record<string, string>>).env
    .VITE_FIREBASE_AUTH_DOMAIN,
  projectId: (import.meta as unknown as Record<string, Record<string, string>>).env
    .VITE_FIREBASE_PROJECT_ID,
};

const app = getApps().length === 0 ? initializeApp(firebaseConfig) : getApps()[0];
const auth = getAuth(app);
const provider = new GoogleAuthProvider();

export async function signInWithGoogle(): Promise<string> {
  const result = await signInWithPopup(auth, provider);
  return result.user.getIdToken();
}
