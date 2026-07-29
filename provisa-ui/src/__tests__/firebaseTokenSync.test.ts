// Copyright (c) 2026 Kenneth Stott
// Canary: 468a3c8d-3f2e-4d22-8c51-f85a76c8ab49
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1318: only a DEAD credential may delete the stored bearer.
//
// The class of defect: a failure path treats every error the same. Firebase reports "could not
// reach the server" and "this credential is revoked" through the same rejected promise, so a catch
// that deletes the token on any rejection signs the user out on a network blip — the forced
// logout/sign-in cycle, which looks like an auth bug and is really an error-classification bug.
// These tests fix the classification: transient → keep, terminal → clear.

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';

// Captured `onIdTokenChanged` listener, so a test can drive token states directly.
let listener: ((user: unknown) => void) | null = null;

vi.mock('firebase/app', () => ({
  initializeApp: vi.fn(() => ({})),
  getApps: vi.fn(() => []),
}));

vi.mock('firebase/auth', () => ({
  getAuth: vi.fn(() => ({})),
  GoogleAuthProvider: class { addScope() {} setCustomParameters() {} },
  GithubAuthProvider: class { addScope() {} setCustomParameters() {} },
  OAuthProvider: class { addScope() {} setCustomParameters() {} },
  onIdTokenChanged: vi.fn((_auth: unknown, cb: (u: unknown) => void) => {
    listener = cb;
    return () => {};
  }),
  signInWithPopup: vi.fn(),
  createUserWithEmailAndPassword: vi.fn(),
  signInWithEmailAndPassword: vi.fn(),
  signOut: vi.fn(),
}));

import { installFirebaseTokenSync } from '../lib/firebase';

const LIVE_TOKEN = 'still-valid-bearer';

function userWhoseRefresh(outcome: Promise<string>) {
  return { getIdToken: () => outcome };
}

beforeEach(() => {
  listener = null;
  localStorage.clear();
  // hasFirebaseConfig(): runtime-injected config is the cloud path.
  (window as unknown as { __PROVISA_FIREBASE__?: unknown }).__PROVISA_FIREBASE__ = {
    apiKey: 'k',
    authDomain: 'd',
    projectId: 'p',
  };
  vi.spyOn(console, 'error').mockImplementation(() => {});
});

afterEach(() => {
  delete (window as unknown as { __PROVISA_FIREBASE__?: unknown }).__PROVISA_FIREBASE__;
  vi.restoreAllMocks();
});

async function drive(user: unknown): Promise<void> {
  const settled = installFirebaseTokenSync();
  listener!(user);
  await settled;
}

describe('installFirebaseTokenSync token lifetime', () => {
  it('writes the refreshed token for a live user', async () => {
    await drive(userWhoseRefresh(Promise.resolve('fresh')));
    expect(localStorage.getItem('provisa_token')).toBe('fresh');
  });

  // Each code names a condition where the credential is fine and the NETWORK is not. Deleting the
  // bearer on any of these strands a signed-in user until they sign in again.
  it.each([
    'auth/network-request-failed',
    'auth/timeout',
    'auth/too-many-requests',
    'auth/internal-error',
  ])('keeps the stored token when the refresh fails with %s', async (code) => {
    localStorage.setItem('provisa_token', LIVE_TOKEN);
    await drive(userWhoseRefresh(Promise.reject(Object.assign(new Error(code), { code }))));
    expect(localStorage.getItem('provisa_token')).toBe(LIVE_TOKEN);
  });

  it.each(['auth/user-token-expired', 'auth/user-disabled', 'auth/invalid-user-token'])(
    'clears the stored token when the credential itself is dead (%s)',
    async (code) => {
      localStorage.setItem('provisa_token', LIVE_TOKEN);
      await drive(userWhoseRefresh(Promise.reject(Object.assign(new Error(code), { code }))));
      expect(localStorage.getItem('provisa_token')).toBeNull();
    },
  );

  it('clears the stored token on an explicit sign-out', async () => {
    localStorage.setItem('provisa_token', LIVE_TOKEN);
    await drive(null);
    expect(localStorage.getItem('provisa_token')).toBeNull();
  });

  it('settles even when the refresh rejects, so boot never hangs', async () => {
    const settled = installFirebaseTokenSync();
    listener!(userWhoseRefresh(Promise.reject(Object.assign(new Error('x'), { code: 'auth/x' }))));
    await expect(settled).resolves.toBeUndefined();
  });
});
