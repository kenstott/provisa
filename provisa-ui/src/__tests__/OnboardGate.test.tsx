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
import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen } from '../test-utils/render';
import { OnboardGate } from '../components/OnboardGate';

vi.mock('../context/AuthContext', () => ({ useAuth: vi.fn() }));
vi.mock('../pages/OnboardOrgPage', () => ({
  OnboardOrgPage: () => <div data-testid="onboard-org-page" />,
}));

import { useAuth } from '../context/AuthContext';
const mockUseAuth = vi.mocked(useAuth);

type AuthOverrides = {
  userId?: string | null;
  identityErrorStatus?: number | null;
  orgMemberships?: { org_id: string; org_name: string }[];
  assignments?: { role_id: string; domain_id: string }[];
};

function auth(overrides: AuthOverrides) {
  return {
    authEnabled: true,
    loading: false,
    userId: overrides.userId ?? null,
    identityErrorStatus: overrides.identityErrorStatus ?? null,
    orgMemberships: overrides.orgMemberships ?? [],
    assignments: overrides.assignments ?? [],
    refresh: vi.fn(),
  } as unknown as ReturnType<typeof useAuth>;
}

function renderGate(onSessionExpired: () => void = vi.fn()) {
  return render(
    <OnboardGate onSessionExpired={onSessionExpired}>
      <div data-testid="app-shell" />
    </OnboardGate>,
  );
}

describe('OnboardGate', () => {
  beforeEach(() => {
    mockUseAuth.mockReset();
    localStorage.setItem('provisa_token', 'tok');
  });

  it('shows a server-unavailable retry screen when /auth/me 500s', () => {
    mockUseAuth.mockReturnValue(auth({ userId: null, identityErrorStatus: 500 }));
    renderGate();
    expect(screen.getByTestId('identity-unavailable')).toBeInTheDocument();
    expect(screen.queryByTestId('no-account')).not.toBeInTheDocument();
  });

  it('treats an unreachable server (status 0) as unavailable, not as denied access', () => {
    mockUseAuth.mockReturnValue(auth({ userId: null, identityErrorStatus: 0 }));
    renderGate();
    expect(screen.getByTestId('identity-unavailable')).toBeInTheDocument();
  });

  // REQ-1289: a rejected credential is not a destination. There is nothing to decide on a "no
  // access" panel, and on a deployment with no administrator yet its advice — ask an administrator
  // for an invitation — names somebody who does not exist. Drop the stale token and go to sign-in.
  it.each([401, 403])('sends a rejected credential (%i) back to sign-in, not a dead end', (status) => {
    const onSessionExpired = vi.fn();
    mockUseAuth.mockReturnValue(auth({ userId: null, identityErrorStatus: status }));
    renderGate(onSessionExpired);
    expect(onSessionExpired).toHaveBeenCalled();
    expect(localStorage.getItem('provisa_token')).toBeNull();
    expect(screen.queryByTestId('no-account')).not.toBeInTheDocument();
    expect(screen.queryByTestId('identity-unavailable')).not.toBeInTheDocument();
    expect(screen.queryByTestId('app-shell')).not.toBeInTheDocument();
  });

  it('routes a resolved identity with no org memberships to org onboarding', async () => {
    mockUseAuth.mockReturnValue(auth({ userId: 'u1', orgMemberships: [] }));
    renderGate();
    expect(await screen.findByTestId('onboard-org-page')).toBeInTheDocument();
  });

  it('lets a platform admin with no memberships through to the shell', () => {
    mockUseAuth.mockReturnValue(
      auth({ userId: 'u1', assignments: [{ role_id: 'admin', domain_id: '*' }] }),
    );
    renderGate();
    expect(screen.getByTestId('app-shell')).toBeInTheDocument();
  });

  it('lets an org member through to the shell', () => {
    mockUseAuth.mockReturnValue(
      auth({ userId: 'u1', orgMemberships: [{ org_id: 'acme', org_name: 'Acme' }] }),
    );
    renderGate();
    expect(screen.getByTestId('app-shell')).toBeInTheDocument();
  });
});
