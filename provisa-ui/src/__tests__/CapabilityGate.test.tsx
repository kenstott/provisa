// Copyright (c) 2026 Kenneth Stott
// Canary: f8be57dd-826e-4cdd-863b-83a5f63a018c
// Canary: placeholder
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { describe, it, expect, vi, beforeEach } from 'vitest'
import { render, screen } from '@testing-library/react'
import { CapabilityGate } from '../components/CapabilityGate'
import type { Capability } from '../types/auth'

// Mock the useCapability hook so we can control its return value without
// mounting a full AuthProvider (which makes network requests).
vi.mock('../hooks/useCapability', () => ({
  useCapability: vi.fn(),
  useCapabilities: vi.fn(),
}))

import { useCapability } from '../hooks/useCapability'

const mockUseCapability = vi.mocked(useCapability)

describe('CapabilityGate', () => {
  beforeEach(() => {
    mockUseCapability.mockReset()
  })

  it('renders children when the capability is allowed', () => {
    mockUseCapability.mockReturnValue(true)

    render(
      <CapabilityGate capability={'query_development' as Capability}>
        <span>Protected Content</span>
      </CapabilityGate>
    )

    expect(screen.getByText('Protected Content')).toBeInTheDocument()
  })

  it('renders nothing when capability is not allowed and no fallback provided', () => {
    mockUseCapability.mockReturnValue(false)

    const { container } = render(
      <CapabilityGate capability={'admin' as Capability}>
        <span>Admin Only</span>
      </CapabilityGate>
    )

    expect(screen.queryByText('Admin Only')).not.toBeInTheDocument()
    // Container should be essentially empty (just the root div from RTL)
    expect(container.textContent).toBe('')
  })

  it('renders fallback when capability is not allowed and fallback provided', () => {
    mockUseCapability.mockReturnValue(false)

    render(
      <CapabilityGate
        capability={'security_config' as Capability}
        fallback={<div>Not Authorized</div>}
      >
        <span>Security Settings</span>
      </CapabilityGate>
    )

    expect(screen.queryByText('Security Settings')).not.toBeInTheDocument()
    expect(screen.getByText('Not Authorized')).toBeInTheDocument()
  })

  it('does not render fallback when capability is allowed', () => {
    mockUseCapability.mockReturnValue(true)

    render(
      <CapabilityGate
        capability={'source_registration' as Capability}
        fallback={<div>Not Authorized</div>}
      >
        <span>Sources</span>
      </CapabilityGate>
    )

    expect(screen.getByText('Sources')).toBeInTheDocument()
    expect(screen.queryByText('Not Authorized')).not.toBeInTheDocument()
  })

  it('passes the correct capability to useCapability', () => {
    mockUseCapability.mockReturnValue(true)

    render(
      <CapabilityGate capability={'query_approval' as Capability}>
        <span>Approvals</span>
      </CapabilityGate>
    )

    expect(mockUseCapability).toHaveBeenCalledWith('query_approval')
  })
})
