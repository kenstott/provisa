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
  selectedRoles?: Role[]
  availableRoles?: Role[]
  toggleRole?: ReturnType<typeof vi.fn>
}) {
  return {
    role: overrides.selectedRoles?.[0] ?? ADMIN_ROLE,
    selectedRoles: overrides.selectedRoles ?? [ADMIN_ROLE],
    capabilities: ['admin'] as Capability[],
    domainAccess: ['*'],
    toggleRole: overrides.toggleRole ?? vi.fn(),
    availableRoles: overrides.availableRoles ?? [ADMIN_ROLE, ANALYST_ROLE],
    loading: false,
    error: null,
  }
}

describe('RoleSelector', () => {
  beforeEach(() => {
    mockUseAuth.mockReset()
  })

  it('shows "No roles configured" when availableRoles is empty', () => {
    mockUseAuth.mockReturnValue(makeAuthValue({ availableRoles: [], selectedRoles: [] }))

    render(<RoleSelector />)
    expect(screen.getByText('No roles configured')).toBeInTheDocument()
  })

  it('renders trigger button with current role label', () => {
    mockUseAuth.mockReturnValue(makeAuthValue({ selectedRoles: [ADMIN_ROLE] }))

    render(<RoleSelector />)

    expect(screen.getByRole('button')).toHaveTextContent('Role: admin')
  })

  it('shows role label for multiple selected roles joined by comma', () => {
    mockUseAuth.mockReturnValue(
      makeAuthValue({ selectedRoles: [ADMIN_ROLE, ANALYST_ROLE] })
    )

    render(<RoleSelector />)

    expect(screen.getByRole('button')).toHaveTextContent('admin, analyst')
  })

  it('dropdown is hidden before trigger is clicked', () => {
    mockUseAuth.mockReturnValue(makeAuthValue({}))

    render(<RoleSelector />)

    expect(screen.queryByRole('checkbox')).not.toBeInTheDocument()
  })

  it('opens dropdown with all available roles when trigger is clicked', () => {
    mockUseAuth.mockReturnValue(makeAuthValue({}))

    render(<RoleSelector />)
    fireEvent.click(screen.getByRole('button'))

    // Should show checkbox labels for each available role
    expect(screen.getByLabelText('admin')).toBeInTheDocument()
    expect(screen.getByLabelText('analyst')).toBeInTheDocument()
  })

  it('closes dropdown when trigger is clicked again', () => {
    mockUseAuth.mockReturnValue(makeAuthValue({}))

    render(<RoleSelector />)
    fireEvent.click(screen.getByRole('button'))
    expect(screen.getByLabelText('admin')).toBeInTheDocument()

    fireEvent.click(screen.getByRole('button'))
    expect(screen.queryByLabelText('admin')).not.toBeInTheDocument()
  })

  it('marks the selected role checkbox as checked', () => {
    mockUseAuth.mockReturnValue(
      makeAuthValue({ selectedRoles: [ADMIN_ROLE], availableRoles: [ADMIN_ROLE, ANALYST_ROLE] })
    )

    render(<RoleSelector />)
    fireEvent.click(screen.getByRole('button'))

    expect(screen.getByLabelText('admin')).toBeChecked()
    expect(screen.getByLabelText('analyst')).not.toBeChecked()
  })

  it('calls toggleRole when a role checkbox is changed', () => {
    const toggleRole = vi.fn()
    mockUseAuth.mockReturnValue(makeAuthValue({ toggleRole }))

    render(<RoleSelector />)
    fireEvent.click(screen.getByRole('button'))
    fireEvent.click(screen.getByLabelText('analyst'))

    expect(toggleRole).toHaveBeenCalledWith(ANALYST_ROLE)
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
