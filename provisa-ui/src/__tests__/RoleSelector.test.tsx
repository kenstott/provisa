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

import { describe, it, expect, vi, beforeEach } from 'vitest'
import { render, screen, fireEvent } from '@testing-library/react'
import { RoleSelector } from '../components/RoleSelector'
import type { Role, Capability } from '../types/auth'

vi.mock('../context/AuthContext', () => ({
  useAuth: vi.fn(),
}))

import { useAuth } from '../context/AuthContext'
const mockUseAuth = vi.mocked(useAuth)

const ADMIN_ROLE: Role = {
  id: 'admin',
  capabilities: ['admin'] as Capability[],
  domain_access: ['*'],
}

const ANALYST_ROLE: Role = {
  id: 'analyst',
  capabilities: ['query_development', 'full_results'] as Capability[],
  domain_access: ['sales'],
}

function makeAuthValue(overrides: {
  selectedRole?: Role | 'all'
  availableRoles?: Role[]
  selectRole?: (role: Role | 'all') => void
  devMode?: boolean
}) {
  const selectedRole = overrides.selectedRole ?? 'all'
  const availableRoles = overrides.availableRoles ?? [ADMIN_ROLE, ANALYST_ROLE]
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
  }
}

describe('RoleSelector', () => {
  beforeEach(() => {
    mockUseAuth.mockReset()
  })

  it('shows "No roles configured" when availableRoles is empty', () => {
    mockUseAuth.mockReturnValue(makeAuthValue({ availableRoles: [], selectedRole: 'all' }))

    render(<RoleSelector />)
    expect(screen.getByText('No roles configured')).toBeInTheDocument()
  })

  it('renders trigger button with "All" label when selectedRole is "all"', () => {
    mockUseAuth.mockReturnValue(makeAuthValue({ selectedRole: 'all' }))

    render(<RoleSelector />)
    expect(screen.getByRole('button')).toHaveTextContent('Role: All')
  })

  it('renders trigger button with role id when a specific role is selected', () => {
    mockUseAuth.mockReturnValue(makeAuthValue({ selectedRole: ADMIN_ROLE }))

    render(<RoleSelector />)
    expect(screen.getByRole('button')).toHaveTextContent('Role: admin')
  })

  it('dropdown is hidden before trigger is clicked', () => {
    mockUseAuth.mockReturnValue(makeAuthValue({}))

    render(<RoleSelector />)
    expect(screen.queryByRole('option')).not.toBeInTheDocument()
  })

  it('opens dropdown with "All" and all available roles when trigger is clicked', () => {
    mockUseAuth.mockReturnValue(makeAuthValue({}))

    render(<RoleSelector />)
    fireEvent.click(screen.getByRole('button'))

    expect(screen.getByRole('option', { name: 'All' })).toBeInTheDocument()
    expect(screen.getByRole('option', { name: 'admin' })).toBeInTheDocument()
    expect(screen.getByRole('option', { name: 'analyst' })).toBeInTheDocument()
  })

  it('closes dropdown when an option is selected', () => {
    const selectRole = vi.fn()
    mockUseAuth.mockReturnValue(makeAuthValue({ selectRole }))

    render(<RoleSelector />)
    fireEvent.click(screen.getByRole('button'))
    fireEvent.click(screen.getByRole('option', { name: 'analyst' }))

    expect(screen.queryByRole('option')).not.toBeInTheDocument()
    expect(selectRole).toHaveBeenCalledWith(ANALYST_ROLE)
  })

  it('calls selectRole with "all" when All option is clicked', () => {
    const selectRole = vi.fn()
    mockUseAuth.mockReturnValue(makeAuthValue({ selectedRole: ADMIN_ROLE, selectRole }))

    render(<RoleSelector />)
    fireEvent.click(screen.getByRole('button'))
    fireEvent.click(screen.getByRole('option', { name: 'All' }))

    expect(selectRole).toHaveBeenCalledWith('all')
  })

  it('marks selected option with aria-selected=true', () => {
    mockUseAuth.mockReturnValue(makeAuthValue({ selectedRole: ADMIN_ROLE }))

    render(<RoleSelector />)
    fireEvent.click(screen.getByRole('button'))

    expect(screen.getByRole('option', { name: 'admin' })).toHaveAttribute('aria-selected', 'true')
    expect(screen.getByRole('option', { name: 'analyst' })).toHaveAttribute('aria-selected', 'false')
  })

  it('shows DEV badge in dev mode', () => {
    mockUseAuth.mockReturnValue(makeAuthValue({ devMode: true }))

    render(<RoleSelector />)
    expect(screen.getByText('DEV')).toBeInTheDocument()
  })

  it('does not show DEV badge outside dev mode', () => {
    mockUseAuth.mockReturnValue(makeAuthValue({ devMode: false }))

    render(<RoleSelector />)
    expect(screen.queryByText('DEV')).not.toBeInTheDocument()
  })

  it('trigger button has aria-expanded=true when open', () => {
    mockUseAuth.mockReturnValue(makeAuthValue({}))

    render(<RoleSelector />)
    const trigger = screen.getByRole('button')

    expect(trigger).toHaveAttribute('aria-expanded', 'false')
    fireEvent.click(trigger)
    expect(trigger).toHaveAttribute('aria-expanded', 'true')
  })
})
