// Copyright (c) 2026 Kenneth Stott
// Canary: 8589a3f0-598c-452a-b1db-e1d2927f4e4d
// Canary: placeholder
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, fireEvent, waitFor, within } from '../test-utils/render';
import i18n from '../i18n';
import { RoleSelector } from '../components/RoleSelector';
import type { Role, Capability } from '../types/auth';

const t = i18n.getFixedT('en');

vi.mock('../context/AuthContext', () => ({
  useAuth: vi.fn(),
}));

import { useAuth } from '../context/AuthContext';
const mockUseAuth = vi.mocked(useAuth);

const ADMIN_ROLE: Role = {
  id: 'admin',
  capabilities: ['admin'] as Capability[],
  domain_access: ['*'],
};

const ANALYST_ROLE: Role = {
  id: 'analyst',
  capabilities: ['query_development', 'full_results'] as Capability[],
  domain_access: ['sales'],
};

function makeAuthValue(overrides: {
  selectedRole?: Role | 'all';
  availableRoles?: Role[];
  selectRole?: (role: Role | 'all') => void;
  devMode?: boolean;
}) {
  const selectedRole = overrides.selectedRole ?? 'all';
  const availableRoles = overrides.availableRoles ?? [ADMIN_ROLE, ANALYST_ROLE];
  return {
    role: selectedRole === 'all' ? (availableRoles[0] ?? null) : selectedRole,
    selectedRoles: selectedRole === 'all' ? availableRoles : [selectedRole as Role],
    capabilities: ['admin'] as Capability[],
    domainAccess: ['*'],
    selectedRole,
    selectedDomain: null,
    selectRole: overrides.selectRole ?? vi.fn(),
    selectDomain: vi.fn(),
    availableRoles,
    availableDomains: [],
    assignments: [],
    devMode: overrides.devMode ?? false,
    loading: false,
    error: null,
    selectOrg: vi.fn(),
    activeOrgId: null,
    orgMemberships: [],
    userId: null,
    email: null,
    displayName: null,
  };
}

describe('RoleSelector', () => {
  beforeEach(() => {
    mockUseAuth.mockReset();
  });

  it('shows "No roles configured" when availableRoles is empty', () => {
    mockUseAuth.mockReturnValue(makeAuthValue({ availableRoles: [], selectedRole: 'all' }));

    render(<RoleSelector />);
    expect(screen.getByText(t('roleSelector.none'))).toBeInTheDocument();
  });

  it('renders trigger button with "All" label when selectedRole is "all"', () => {
    mockUseAuth.mockReturnValue(makeAuthValue({ selectedRole: 'all' }));

    render(<RoleSelector />);
    expect(screen.getByRole('button')).toHaveTextContent(t('roleSelector.role', { role: 'All' }));
  });

  it('renders trigger button with role id when a specific role is selected', () => {
    mockUseAuth.mockReturnValue(makeAuthValue({ selectedRole: ADMIN_ROLE }));

    render(<RoleSelector />);
    expect(screen.getByRole('button')).toHaveTextContent(t('roleSelector.role', { role: 'admin' }));
  });

  it('menu is hidden before trigger is clicked', () => {
    mockUseAuth.mockReturnValue(makeAuthValue({}));

    render(<RoleSelector />);
    expect(screen.queryByRole('menuitem')).not.toBeInTheDocument();
  });

  // Mantine Menu.Item accessible-name computation is unreliable in jsdom, so
  // menu items are located by their (i18n) visible text scoped to role="menu".
  const openMenu = async () => {
    fireEvent.click(screen.getByRole('button'));
    return within(await screen.findByRole('menu'));
  };

  it('opens the menu with "All" and all available roles when trigger is clicked', async () => {
    mockUseAuth.mockReturnValue(makeAuthValue({}));

    render(<RoleSelector />);
    const menu = await openMenu();

    expect(menu.getByText('All')).toBeInTheDocument();
    expect(menu.getByText('admin')).toBeInTheDocument();
    expect(menu.getByText('analyst')).toBeInTheDocument();
  });

  it('closes the menu when an item is selected', async () => {
    const selectRole = vi.fn();
    mockUseAuth.mockReturnValue(makeAuthValue({ selectRole }));

    render(<RoleSelector />);
    const menu = await openMenu();
    fireEvent.click(menu.getByText('analyst'));

    await waitFor(() => expect(screen.queryByRole('menu')).not.toBeInTheDocument());
    expect(selectRole).toHaveBeenCalledWith(ANALYST_ROLE);
  });

  it('calls selectRole with "all" when the All item is clicked', async () => {
    const selectRole = vi.fn();
    mockUseAuth.mockReturnValue(makeAuthValue({ selectedRole: ADMIN_ROLE, selectRole }));

    render(<RoleSelector />);
    const menu = await openMenu();
    fireEvent.click(menu.getByText('All'));

    expect(selectRole).toHaveBeenCalledWith('all');
  });

  it('marks the selected item with aria-current=true', async () => {
    mockUseAuth.mockReturnValue(makeAuthValue({ selectedRole: ADMIN_ROLE }));

    render(<RoleSelector />);
    const menu = await openMenu();

    expect(menu.getByText('admin').closest('[role="menuitem"]')).toHaveAttribute(
      'aria-current',
      'true',
    );
    expect(menu.getByText('analyst').closest('[role="menuitem"]')).not.toHaveAttribute(
      'aria-current',
    );
  });

  it('shows DEV badge in dev mode', () => {
    mockUseAuth.mockReturnValue(makeAuthValue({ devMode: true }));

    render(<RoleSelector />);
    expect(screen.getByText(t('roleSelector.dev'))).toBeInTheDocument();
  });

  it('does not show DEV badge outside dev mode', () => {
    mockUseAuth.mockReturnValue(makeAuthValue({ devMode: false }));

    render(<RoleSelector />);
    expect(screen.queryByText(t('roleSelector.dev'))).not.toBeInTheDocument();
  });

  it('trigger button reflects expanded state via aria-expanded', async () => {
    mockUseAuth.mockReturnValue(makeAuthValue({}));

    render(<RoleSelector />);
    const trigger = screen.getByRole('button');

    expect(trigger).toHaveAttribute('aria-expanded', 'false');
    fireEvent.click(trigger);
    await waitFor(() => expect(trigger).toHaveAttribute('aria-expanded', 'true'));
  });
});
