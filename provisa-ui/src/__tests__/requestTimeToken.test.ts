// Copyright (c) 2026 Kenneth Stott
// Canary: aec461e8-ab7f-4714-beae-328e41627fb8
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1434: the bearer is asked for at request time, not read from the mirrored copy.
//
// The class of defect: a value is cached at one moment and used at another. `installFirebaseTokenSync`
// mirrors the ID token into localStorage on every rotation, but rotation only runs while the tab is
// awake — after a sleep, a throttled background tab, or a transient refresh failure that REQ-1318
// deliberately survives by keeping the old token, the mirror is expired and every request sends it.
// The server answers "Invalid or expired token" on a query the user just typed.

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';

let currentUser: { getIdToken: () => Promise<string> } | null = null;

vi.mock('firebase/app', () => ({
  initializeApp: vi.fn(() => ({})),
  getApps: vi.fn(() => []),
}));

vi.mock('firebase/auth', () => ({
  getAuth: vi.fn(() => ({
    get currentUser() {
      return currentUser;
    },
  })),
  GoogleAuthProvider: class { addScope() {} setCustomParameters() {} },
  GithubAuthProvider: class { addScope() {} setCustomParameters() {} },
  OAuthProvider: class { addScope() {} setCustomParameters() {} },
  onIdTokenChanged: vi.fn(() => () => {}),
  signInWithPopup: vi.fn(),
  createUserWithEmailAndPassword: vi.fn(),
  signInWithEmailAndPassword: vi.fn(),
  signOut: vi.fn(),
}));

import { currentBearer, installAuthFetch, ORG_HEADER } from '../lib/authFetch';

function withFirebaseConfig(): void {
  (window as unknown as { __PROVISA_FIREBASE__?: unknown }).__PROVISA_FIREBASE__ = {
    apiKey: 'k',
    authDomain: 'd',
    projectId: 'p',
  };
}

beforeEach(() => {
  currentUser = null;
  localStorage.clear();
});

afterEach(() => {
  delete (window as unknown as { __PROVISA_FIREBASE__?: unknown }).__PROVISA_FIREBASE__;
  vi.restoreAllMocks();
  localStorage.clear();
});

describe('currentBearer', () => {
  it('sends the token Firebase mints now, not the expired one in storage', async () => {
    withFirebaseConfig();
    localStorage.setItem('provisa_token', 'expired-mirror');
    currentUser = { getIdToken: () => Promise.resolve('freshly-minted') };

    expect(await currentBearer()).toBe('freshly-minted');
  });

  it('writes the fresh token back, so the relay hands the subdomain a live one', async () => {
    withFirebaseConfig();
    localStorage.setItem('provisa_token', 'expired-mirror');
    currentUser = { getIdToken: () => Promise.resolve('freshly-minted') };

    await currentBearer();

    expect(localStorage.getItem('provisa_token')).toBe('freshly-minted');
  });

  it('keeps the stored token on a deploy with no Firebase session to ask', async () => {
    // Basic auth mints its own JWT, and an org subdomain holds a copy borrowed from the control
    // plane. Neither passes through Firebase, so storage is the only source.
    localStorage.setItem('provisa_token', 'basic-auth-jwt');

    expect(await currentBearer()).toBe('basic-auth-jwt');
  });

  it('reports no bearer at all rather than an empty one', async () => {
    expect(await currentBearer()).toBeNull();
  });
});

describe('the fetch interceptor', () => {
  it('puts the refreshed token on a same-origin request', async () => {
    withFirebaseConfig();
    localStorage.setItem('provisa_token', 'expired-mirror');
    localStorage.setItem('provisa_org', 'kstott');
    currentUser = { getIdToken: () => Promise.resolve('freshly-minted') };

    const sent: RequestInit[] = [];
    window.fetch = vi.fn((_input: RequestInfo | URL, init?: RequestInit) => {
      sent.push(init ?? {});
      return Promise.resolve(new Response(''));
    }) as typeof window.fetch;
    installAuthFetch();

    await window.fetch('/auth/me');

    const headers = new Headers(sent[0].headers);
    expect(headers.get('authorization')).toBe('Bearer freshly-minted');
    expect(headers.get(ORG_HEADER)).toBe('kstott');
  });

  it('never leaks the token to another origin', async () => {
    withFirebaseConfig();
    currentUser = { getIdToken: () => Promise.resolve('freshly-minted') };

    const sent: RequestInit[] = [];
    window.fetch = vi.fn((_input: RequestInfo | URL, init?: RequestInit) => {
      sent.push(init ?? {});
      return Promise.resolve(new Response(''));
    }) as typeof window.fetch;
    installAuthFetch();

    await window.fetch('https://securetoken.googleapis.com/v1/token');

    expect(new Headers(sent[0].headers).get('authorization')).toBeNull();
  });
});
