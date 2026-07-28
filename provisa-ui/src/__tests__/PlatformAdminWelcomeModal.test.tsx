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
// /team route is real so the "Go to Team" assertion is about navigation, not a mocked spy.
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

  it('states the new role and how to invite others after a claim', async () => {
    localStorage.setItem(CLAIMED_ADMIN_FLAG, '1');

    renderModal(<PlatformAdminWelcomeModal />);

    const modal = await screen.findByTestId('platform-admin-welcome');
    expect(modal).toHaveTextContent(/platform administrator/i);
    // The invite path is the point of the modal — the role alone leaves the user with no next step.
    expect(modal).toHaveTextContent(/inviting other people/i);
    expect(modal).toHaveTextContent(/Team/);
    expect(modal).toHaveTextContent(/invitation/i);
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

  it('takes the new administrator to Team, where invitations are created', async () => {
    localStorage.setItem(CLAIMED_ADMIN_FLAG, '1');

    renderModal(<PlatformAdminWelcomeModal />);
    fireEvent.click(await screen.findByTestId('platform-admin-welcome-team'));

    expect(await screen.findByTestId('team-page')).toBeInTheDocument();
    expect(localStorage.getItem(CLAIMED_ADMIN_FLAG)).toBeNull();
  });
});
