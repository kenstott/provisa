// Copyright (c) 2026 Kenneth Stott
// Canary: 6b0a1c74-3d5e-4a1f-9c2b-7e84d05f1a63
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1286: an unresolved identity is not one state. A 5xx from /auth/me means the deployment
// failed to answer; reporting that as "this account has no access" states an access decision that
// was never made and tells the user to go get an invitation that would not help. Only 401/403 is
// an access answer. A resolved identity with no org memberships continues to onboarding.
//
// Driven through the real AuthProvider with /auth/me stubbed, not a mocked AuthContext module: the
// gate's whole job is to read provider state that the provider itself computes, and the vitest pool
// shares one module registry across files, so mocking AuthContext here decided what OTHER files saw
// as well.
import { describe, it, expect, vi, beforeEach } from 'vitest';
import { useState } from 'react';
import { render, screen, waitFor } from '../test-utils/render';
import { OnboardGate } from '../components/OnboardGate';
import { AuthProvider, useAuth } from '../context/AuthContext';

vi.mock('../api/admin', async (importOriginal) => {
  const actual = await importOriginal<typeof import('../api/admin')>();
  return { ...actual, fetchMe: vi.fn(), fetchBootstrapStatus: vi.fn() };
});
// Spread the real module: vmThreads + fileParallelism:false share one module registry, so a
// replace-everything factory here leaks into other test files and strips the hooks they render
// against (a TablesPage test saw useTables become undefined).
vi.mock('../hooks/useAdminQueries', async (importOriginal) => ({
  ...(await importOriginal<typeof import('../hooks/useAdminQueries')>()),
  useRoles: () => ({ refetch: vi.fn().mockResolvedValue({ data: { roles: [] } }) }),
  useDomains: () => ({ refetch: vi.fn().mockResolvedValue({ data: { domains: [] } }) }),
}));
vi.mock('../pages/OnboardOrgPage', () => ({
  OnboardOrgPage: () => <div data-testid="onboard-org-page" />,
}));

import { AuthMeError, fetchBootstrapStatus, fetchMe } from '../api/admin';
const mockFetchMe = vi.mocked(fetchMe);
const mockBootstrapStatus = vi.mocked(fetchBootstrapStatus);

type Identity = {
  userId: string;
  orgMemberships?: { org_id: string; org_name: string }[];
  assignments?: { role_id: string; domain_id: string }[];
};

function identity({ userId, orgMemberships = [], assignments = [] }: Identity) {
  return {
    user_id: userId,
    dev_mode: false,
    assignments,
    org_memberships: orgMemberships,
    active_org_id: null,
  } as unknown as Awaited<ReturnType<typeof fetchMe>>;
}

function resolvesTo(spec: Identity) {
  mockFetchMe.mockResolvedValue(identity(spec));
}

function renderGate(onSessionExpired: () => void = vi.fn()) {
  return render(
    <AuthProvider authEnabled>
      <OnboardGate onSessionExpired={onSessionExpired}>
        <div data-testid="app-shell" />
      </OnboardGate>
    </AuthProvider>,
  );
}

describe('OnboardGate', () => {
  beforeEach(() => {
    mockFetchMe.mockReset();
    mockBootstrapStatus.mockReset();
    // Default for these cases: the deployment already has a platform administrator, so the
    // first-login path (REQ-1292) is not what is under test here.
    mockBootstrapStatus.mockResolvedValue(false);
    localStorage.setItem('provisa_token', 'tok');
  });

  it('shows a server-unavailable retry screen when /auth/me 500s', async () => {
    mockFetchMe.mockRejectedValue(new AuthMeError(500));
    renderGate();
    expect(await screen.findByTestId('identity-unavailable')).toBeInTheDocument();
    expect(screen.queryByTestId('no-account')).not.toBeInTheDocument();
  });

  it('treats an unreachable server (status 0) as unavailable, not as denied access', async () => {
    mockFetchMe.mockRejectedValue(new AuthMeError(0));
    renderGate();
    expect(await screen.findByTestId('identity-unavailable')).toBeInTheDocument();
  });

  // REQ-1289: a rejected credential is not a destination. There is nothing to decide on a "no
  // access" panel, and on a deployment with no administrator yet its advice — ask an administrator
  // for an invitation — names somebody who does not exist. Drop the stale token and go to sign-in.
  it.each([401, 403])(
    'sends a rejected credential (%i) back to sign-in, not a dead end',
    async (status) => {
      const onSessionExpired = vi.fn();
      mockFetchMe.mockRejectedValue(new AuthMeError(status));
      renderGate(onSessionExpired);
      await waitFor(() => expect(onSessionExpired).toHaveBeenCalled());
      expect(localStorage.getItem('provisa_token')).toBeNull();
      expect(screen.queryByTestId('no-account')).not.toBeInTheDocument();
      expect(screen.queryByTestId('identity-unavailable')).not.toBeInTheDocument();
      expect(screen.queryByTestId('app-shell')).not.toBeInTheDocument();
    },
  );

  it('routes a resolved identity with no org memberships to org onboarding', async () => {
    resolvesTo({ userId: 'u1' });
    renderGate();
    expect(await screen.findByTestId('onboard-org-page')).toBeInTheDocument();
  });

  it('lets a platform admin with no memberships through to the shell', async () => {
    // REQ-1297: the platform operator's role is `platform_admin` — the gate keys on it exactly.
    resolvesTo({ userId: 'u1', assignments: [{ role_id: 'platform_admin', domain_id: '*' }] });
    renderGate();
    expect(await screen.findByTestId('app-shell')).toBeInTheDocument();
  });

  it('lets an org member through to the shell', async () => {
    resolvesTo({ userId: 'u1', orgMemberships: [{ org_id: 'acme', org_name: 'Acme' }] });
    renderGate();
    expect(await screen.findByTestId('app-shell')).toBeInTheDocument();
  });
});

// REQ-1291: AuthProvider outlives the login page. It resolved identity once, at mount, when the
// visitor was unauthenticated — and never again. Signing in stored a token but left userId null, so
// the gate above read the fresh token as a rejected credential, deleted it, and sent the user back
// to sign-in. Every subsequent sign-in did the same: an unbreakable loop, and the exact symptom
// reported after claiming the platform-admin slot.
function CurrentIdentity() {
  const { userId, loading } = useAuth();
  return <div data-testid="identity">{loading ? 'loading' : (userId ?? 'anonymous')}</div>;
}

/** App's shape: one authVersion counter shared by the login callback and the provider. */
function SignInHarness({ children }: { children: React.ReactNode }) {
  const [authVersion, setAuthVersion] = useState(0);
  return (
    <AuthProvider authEnabled authVersion={authVersion}>
      <button
        data-testid="sign-in"
        onClick={() => {
          localStorage.setItem('provisa_token', 'fresh-token');
          setAuthVersion((v) => v + 1);
        }}
      >
        sign in
      </button>
      {children}
    </AuthProvider>
  );
}

describe('identity refresh after sign-in (REQ-1291)', () => {
  beforeEach(() => {
    mockFetchMe.mockReset();
    mockBootstrapStatus.mockReset();
    mockBootstrapStatus.mockResolvedValue(false);
    localStorage.clear();
  });

  it('refetches /auth/me when a sign-in bumps authVersion', async () => {
    mockFetchMe
      .mockRejectedValueOnce(new AuthMeError(401))
      .mockResolvedValueOnce(identity({ userId: 'u1', assignments: [{ role_id: 'admin', domain_id: '*' }] }));
    render(
      <SignInHarness>
        <CurrentIdentity />
      </SignInHarness>,
    );
    await waitFor(() => expect(screen.getByTestId('identity')).toHaveTextContent('anonymous'));
    expect(mockFetchMe).toHaveBeenCalledTimes(1);

    screen.getByTestId('sign-in').click();

    await waitFor(() => expect(screen.getByTestId('identity')).toHaveTextContent('u1'));
    expect(mockFetchMe).toHaveBeenCalledTimes(2);
  });

  it('does not delete the token a sign-in just stored, and lands in the app shell', async () => {
    const onSessionExpired = vi.fn();
    mockFetchMe
      .mockRejectedValueOnce(new AuthMeError(401))
      .mockResolvedValueOnce(identity({ userId: 'u1', assignments: [{ role_id: 'admin', domain_id: '*' }] }));
    render(
      <SignInHarness>
        <OnboardGate onSessionExpired={onSessionExpired}>
          <div data-testid="app-shell" />
        </OnboardGate>
      </SignInHarness>,
    );
    // Pre-login there is no token, so the gate has nothing to reject.
    await waitFor(() => expect(mockFetchMe).toHaveBeenCalledTimes(1));

    screen.getByTestId('sign-in').click();

    await waitFor(() => expect(screen.getByTestId('app-shell')).toBeInTheDocument());
    expect(localStorage.getItem('provisa_token')).toBe('fresh-token');
    expect(onSessionExpired).not.toHaveBeenCalled();
  });

  it('holds `loading` across the re-bootstrap so the gate never judges a stale identity', async () => {
    // The re-fetch is deliberately left pending: during it the token exists and userId is still
    // null. If loading were false there, OnboardGate would delete the token mid-flight.
    const onSessionExpired = vi.fn();
    let resolveSecond: (v: Awaited<ReturnType<typeof fetchMe>>) => void = () => {};
    mockFetchMe.mockRejectedValueOnce(new AuthMeError(401)).mockImplementationOnce(
      () =>
        new Promise((resolve) => {
          resolveSecond = resolve;
        }),
    );
    render(
      <SignInHarness>
        <CurrentIdentity />
        <OnboardGate onSessionExpired={onSessionExpired}>
          <div data-testid="app-shell" />
        </OnboardGate>
      </SignInHarness>,
    );
    await waitFor(() => expect(mockFetchMe).toHaveBeenCalledTimes(1));

    screen.getByTestId('sign-in').click();

    await waitFor(() => expect(screen.getByTestId('identity')).toHaveTextContent('loading'));
    expect(localStorage.getItem('provisa_token')).toBe('fresh-token');
    expect(onSessionExpired).not.toHaveBeenCalled();

    resolveSecond(identity({ userId: 'u1', assignments: [{ role_id: 'admin', domain_id: '*' }] }));
    await waitFor(() => expect(screen.getByTestId('app-shell')).toBeInTheDocument());
  });
});

// REQ-1292: the reported scenario. The control plane was wiped, so the deployment has no
// administrator (`/auth/bootstrap-status` -> unclaimed), but the browser still holds a Firebase
// credential that verifies. "Token present" is therefore not proof the deployment has an
// administrator: the SPA skipped sign-in — the only place the first-login disclosure (REQ-1288)
// renders — and a hard refresh at /query landed on "create an organization". An unclaimed
// platform-admin slot outranks org onboarding.
describe('unclaimed platform-admin slot outranks org onboarding (REQ-1292)', () => {
  beforeEach(() => {
    mockFetchMe.mockReset();
    mockBootstrapStatus.mockReset();
    localStorage.setItem('provisa_token', 'stale-but-valid');
  });

  it('sends a token-holding user back to sign-in instead of org onboarding', async () => {
    const onSessionExpired = vi.fn();
    resolvesTo({ userId: 'u1' }); // valid credential, zero memberships — yesterday: "Get started"
    mockBootstrapStatus.mockResolvedValue(true);

    renderGate(onSessionExpired);

    await waitFor(() => expect(onSessionExpired).toHaveBeenCalled());
    expect(localStorage.getItem('provisa_token')).toBeNull();
    expect(screen.queryByTestId('onboard-org-page')).not.toBeInTheDocument();
    expect(screen.queryByTestId('app-shell')).not.toBeInTheDocument();
  });

  it('sends a token-holding platform admin back to sign-in too', async () => {
    // Assignments resolved from a stale identity do not administer THIS deployment: its admin slot
    // is still unclaimed. Falling through to the shell would hide that from the operator.
    const onSessionExpired = vi.fn();
    resolvesTo({ userId: 'u1', assignments: [{ role_id: 'admin', domain_id: '*' }] });
    mockBootstrapStatus.mockResolvedValue(true);

    renderGate(onSessionExpired);

    await waitFor(() => expect(onSessionExpired).toHaveBeenCalled());
    expect(localStorage.getItem('provisa_token')).toBeNull();
    expect(screen.queryByTestId('app-shell')).not.toBeInTheDocument();
  });

  it('decides nothing while the slot answer is still in flight', async () => {
    const onSessionExpired = vi.fn();
    resolvesTo({ userId: 'u1' });
    let resolveSlot: (unclaimed: boolean) => void = () => {};
    mockBootstrapStatus.mockImplementation(
      () => new Promise((resolve) => { resolveSlot = resolve; }),
    );

    renderGate(onSessionExpired);

    await waitFor(() => expect(mockBootstrapStatus).toHaveBeenCalled());
    expect(screen.queryByTestId('onboard-org-page')).not.toBeInTheDocument();
    expect(screen.queryByTestId('app-shell')).not.toBeInTheDocument();
    expect(onSessionExpired).not.toHaveBeenCalled();
    expect(localStorage.getItem('provisa_token')).toBe('stale-but-valid');

    // Claimed after all -> today's behavior, unchanged.
    resolveSlot(false);
    expect(await screen.findByTestId('onboard-org-page')).toBeInTheDocument();
    expect(localStorage.getItem('provisa_token')).toBe('stale-but-valid');
    expect(onSessionExpired).not.toHaveBeenCalled();
  });

  it('surfaces a failed slot check instead of guessing which screen this is', async () => {
    resolvesTo({ userId: 'u1' });
    mockBootstrapStatus.mockRejectedValue(new Error('bootstrap-status 503'));

    renderGate();

    expect(await screen.findByTestId('identity-unavailable')).toBeInTheDocument();
    expect(screen.queryByTestId('onboard-org-page')).not.toBeInTheDocument();
  });
});
