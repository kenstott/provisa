// Copyright (c) 2026 Kenneth Stott
// Canary: 69e87226-9fab-45f3-8b34-a17e7a72c53b
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { describe, it, expect, vi, beforeEach } from 'vitest';
import userEvent from '@testing-library/user-event';
import { render, screen, waitFor } from '../test-utils/render';
import { SecurityTab } from '../components/admin/SecurityTab';
import type { SecurityState } from '../api/security';

vi.mock('../api/security', () => ({
  fetchSecurity: vi.fn(),
  setSecurity: vi.fn(),
}));

import { fetchSecurity, setSecurity } from '../api/security';
const mockFetch = vi.mocked(fetchSecurity);
const mockSet = vi.mocked(setSecurity);

const MODES = [
  { key: 'standard', label: 'Standard', description: 'Default posture.' },
  { key: 'high', label: 'High (zero-trust)', description: 'pgwire disabled; 403s.' },
];

function state(overrides: Partial<SecurityState> = {}): SecurityState {
  return {
    mode: 'standard',
    modes: MODES,
    restart_required_note: 'Restart required.',
    ...overrides,
  };
}

describe('SecurityTab', () => {
  beforeEach(() => {
    mockFetch.mockReset();
    mockSet.mockReset();
  });

  it('renders the loaded mode', async () => {
    mockFetch.mockResolvedValue(state({ mode: 'standard' }));
    render(<SecurityTab />);

    await waitFor(() =>
      expect(screen.getByTestId('security-mode-select')).toHaveValue('Standard'),
    );
    expect(screen.queryByTestId('security-high-warning')).not.toBeInTheDocument();
  });

  it('shows the warning when high is the loaded mode', async () => {
    mockFetch.mockResolvedValue(state({ mode: 'high' }));
    render(<SecurityTab />);

    await waitFor(() =>
      expect(screen.getByTestId('security-high-warning')).toBeInTheDocument(),
    );
  });

  it('saves with the loaded high mode', async () => {
    mockFetch.mockResolvedValue(state({ mode: 'high' }));
    mockSet.mockResolvedValue({ success: true, restart_required: true });
    render(<SecurityTab />);

    await waitFor(() =>
      expect(screen.getByTestId('security-high-warning')).toBeInTheDocument(),
    );
    await userEvent.click(screen.getByTestId('security-save'));

    await waitFor(() => expect(mockSet).toHaveBeenCalledWith({ mode: 'high' }));
  });
});
