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
// /admin/orgs route is real so the call-to-action assertion is about navigation, not a mocked spy.
const renderModal = (ui: ReactElement) =>
  render(
    <MemoryRouter initialEntries={['/query']}>
      <Routes>
        <Route path="/query" element={ui} />
        <Route path="/admin/orgs" element={<div data-testid="orgs-page" />} />
      </Routes>
    </MemoryRouter>,
  );

describe('PlatformAdminWelcomeModal (REQ-1294)', () => {
  afterEach(() => {
    localStorage.removeItem(CLAIMED_ADMIN_FLAG);
    vi.restoreAllMocks();
  });

  it('states the new role and sends the admin to create an org, not to invite anyone', async () => {
    localStorage.setItem(CLAIMED_ADMIN_FLAG, '1');

    renderModal(<PlatformAdminWelcomeModal />);

    const modal = await screen.findByTestId('platform-admin-welcome');
    expect(modal).toHaveTextContent(/platform administrator/i);
    // Creating an org is the only next step that exists here — the role alone leaves the user
    // with nothing to do.
    expect(modal).toHaveTextContent(/create an organization/i);
    expect(modal).toHaveTextContent(/Organizations under Administration/i);
    // Invite instructions belong to an org admin. A platform admin holds no org and the deployment
    // has none, so telling them to go invite someone names a screen with nothing on it.
    expect(modal).not.toHaveTextContent(/inviting other people/i);
    expect(modal).not.toHaveTextContent(/Open Team/i);
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

  it('takes the new administrator to Organizations, where the first org is created', async () => {
    localStorage.setItem(CLAIMED_ADMIN_FLAG, '1');

    renderModal(<PlatformAdminWelcomeModal />);
    fireEvent.click(await screen.findByTestId('platform-admin-welcome-orgs'));

    expect(await screen.findByTestId('orgs-page')).toBeInTheDocument();
    expect(localStorage.getItem(CLAIMED_ADMIN_FLAG)).toBeNull();
  });
});
