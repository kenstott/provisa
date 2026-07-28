// Copyright (c) 2026 Kenneth Stott
// Canary: 7b3e5c81-42af-4d06-9e77-1a90c6d3b284
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { describe, it, expect, afterEach, vi } from 'vitest';
import type { ReactElement } from 'react';
import { MemoryRouter, Routes, Route } from 'react-router-dom';
import { render, screen, fireEvent, waitFor } from '../test-utils/render';
import {
  PlatformAdminWelcomeModal,
  CLAIMED_ADMIN_FLAG,
} from '../components/PlatformAdminWelcomeModal';

// The modal calls useNavigate; the shared render wrapper has no Router, so provide one here. The
// /team route is real so the call-to-action assertion is about navigation, not a mocked spy.
const renderModal = (ui: ReactElement) =>
  render(
    <MemoryRouter initialEntries={['/query']}>
      <Routes>
        <Route path="/query" element={ui} />
        <Route path="/team" element={<div data-testid="team-page" />} />
      </Routes>
    </MemoryRouter>,
  );

describe('PlatformAdminWelcomeModal (REQ-1294)', () => {
  afterEach(() => {
    localStorage.removeItem(CLAIMED_ADMIN_FLAG);
    vi.restoreAllMocks();
  });

  it('names the platform_admin role and the root org they now administer', async () => {
    localStorage.setItem(CLAIMED_ADMIN_FLAG, '1');

    renderModal(<PlatformAdminWelcomeModal />);

    const modal = await screen.findByTestId('platform-admin-welcome');
    expect(modal).toHaveTextContent(/platform_admin/);
    // REQ-1296: root is already provisioned with the demo assets, and this user administers it —
    // the modal must not read as though the deployment were empty.
    expect(modal).toHaveTextContent(/administrator of the root organization/i);
    expect(modal).toHaveTextContent(/demo sources, tables, and domains/i);
  });

  it('gives the invite-into-root path to a backup platform_admin', async () => {
    localStorage.setItem(CLAIMED_ADMIN_FLAG, '1');

    renderModal(<PlatformAdminWelcomeModal />);

    const modal = await screen.findByTestId('platform-admin-welcome');
    // REQ-1298: the bootstrap slot never reopens, so a second administrator can only come from a
    // root invitation followed by a platform_admin assignment in root.
    expect(modal).toHaveTextContent(/Invite the person into the root organization/i);
    expect(modal).toHaveTextContent(/redeem the invitation/i);
    expect(modal).toHaveTextContent(/Assign them the platform_admin role in root/i);
  });

  it('states that the deployment is now open for org creation', async () => {
    localStorage.setItem(CLAIMED_ADMIN_FLAG, '1');

    renderModal(<PlatformAdminWelcomeModal />);

    const modal = await screen.findByTestId('platform-admin-welcome');
    expect(modal).toHaveTextContent(/anyone who authenticates creates their own organization/i);
    // Signing in confers no platform administration — that is the whole point of a single claim.
    expect(modal).toHaveTextContent(/never made a platform administrator by signing in/i);
  });

  it('stays closed for an ordinary sign-in that claimed nothing', () => {
    renderModal(<PlatformAdminWelcomeModal />);

    expect(screen.queryByTestId('platform-admin-welcome')).not.toBeInTheDocument();
  });

  it('clears the flag on dismiss so the disclosure is shown exactly once', async () => {
    localStorage.setItem(CLAIMED_ADMIN_FLAG, '1');

    const { unmount } = renderModal(<PlatformAdminWelcomeModal />);
    fireEvent.click(await screen.findByTestId('platform-admin-welcome-dismiss'));

    await waitFor(() =>
      expect(screen.queryByTestId('platform-admin-welcome')).not.toBeInTheDocument(),
    );
    expect(localStorage.getItem(CLAIMED_ADMIN_FLAG)).toBeNull();

    // A later mount — the next page load — must not resurrect it.
    unmount();
    renderModal(<PlatformAdminWelcomeModal />);
    expect(screen.queryByTestId('platform-admin-welcome')).not.toBeInTheDocument();
  });

  it("takes the new administrator to root's Team page, where the backup invite starts", async () => {
    localStorage.setItem(CLAIMED_ADMIN_FLAG, '1');

    renderModal(<PlatformAdminWelcomeModal />);
    fireEvent.click(await screen.findByTestId('platform-admin-welcome-team'));

    expect(await screen.findByTestId('team-page')).toBeInTheDocument();
    expect(localStorage.getItem(CLAIMED_ADMIN_FLAG)).toBeNull();
  });
});
