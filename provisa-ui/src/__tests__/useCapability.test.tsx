// Copyright (c) 2026 Kenneth Stott
// Canary: e1efe76f-25f6-4740-a70a-1c24fc584996
// Canary: placeholder
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { describe, it, expect, vi } from 'vitest'
import { renderHook } from '@testing-library/react'
import type { ReactNode } from 'react'
import { useCapability, useCapabilities } from '../hooks/useCapability'
import type { Capability, Role } from '../types/auth'

// We need to supply the AuthContext value without making real network requests.
// Rather than mounting the full AuthProvider (which calls fetchRoles), we mock
// the useAuth hook at the module level and control the returned capabilities.

vi.mock('../context/AuthContext', () => ({
  useAuth: vi.fn(),
}))

import { useAuth } from '../context/AuthContext'
const mockUseAuth = vi.mocked(useAuth)

function makeAuthValue(caps: Capability[]) {
  const role: Role = { id: 'test', capabilities: caps, domain_access: ['*'] }
  return {
    role,
    selectedRoles: [role],
    capabilities: caps,
    domainAccess: ['*'],
    toggleRole: vi.fn(),
    availableRoles: [role],
    loading: false,
    error: null,
  }
}

describe('useCapability', () => {
  it('returns false when capabilities list is empty', () => {
    mockUseAuth.mockReturnValue(makeAuthValue([]))

    const { result } = renderHook(() => useCapability('query_development'))
    expect(result.current).toBe(false)
  })

  it('returns true when the exact capability is present', () => {
    mockUseAuth.mockReturnValue(makeAuthValue(['query_development', 'full_results']))

    const { result } = renderHook(() => useCapability('query_development'))
    expect(result.current).toBe(true)
  })

  it('returns false when capability is not in the list and admin is absent', () => {
    mockUseAuth.mockReturnValue(makeAuthValue(['query_development', 'full_results']))

    const { result } = renderHook(() => useCapability('admin'))
    expect(result.current).toBe(false)
  })

  it('returns true for any capability when admin is present', () => {
    mockUseAuth.mockReturnValue(makeAuthValue(['admin']))

    const capabilitiesToCheck: Capability[] = [
      'source_registration',
      'table_registration',
      'relationship_registration',
      'security_config',
      'query_development',
      'query_approval',
      'full_results',
    ]

    for (const cap of capabilitiesToCheck) {
      const { result } = renderHook(() => useCapability(cap))
      expect(result.current, `admin should grant ${cap}`).toBe(true)
    }
  })

  it('returns true when capability is explicitly listed alongside admin', () => {
    mockUseAuth.mockReturnValue(makeAuthValue(['admin', 'source_registration']))

    const { result } = renderHook(() => useCapability('source_registration'))
    expect(result.current).toBe(true)
  })
})

describe('useCapabilities', () => {
  it('returns false when capabilities list is empty', () => {
    mockUseAuth.mockReturnValue(makeAuthValue([]))

    const { result } = renderHook(() => useCapabilities(['query_development', 'full_results']))
    expect(result.current).toBe(false)
  })

  it('returns true when all required capabilities are present', () => {
    mockUseAuth.mockReturnValue(makeAuthValue(['query_development', 'full_results', 'query_approval']))

    const { result } = renderHook(() => useCapabilities(['query_development', 'full_results']))
    expect(result.current).toBe(true)
  })

  it('returns false when only some required capabilities are present', () => {
    mockUseAuth.mockReturnValue(makeAuthValue(['query_development']))

    const { result } = renderHook(() => useCapabilities(['query_development', 'full_results']))
    expect(result.current).toBe(false)
  })

  it('returns true for any combination when admin is present', () => {
    mockUseAuth.mockReturnValue(makeAuthValue(['admin']))

    const { result } = renderHook(() =>
      useCapabilities(['source_registration', 'security_config', 'query_approval'])
    )
    expect(result.current).toBe(true)
  })

  it('returns true for empty required list when capabilities are present', () => {
    // Every capability satisfies an empty requirements list (every() on [] is true)
    mockUseAuth.mockReturnValue(makeAuthValue(['query_development']))

    const { result } = renderHook(() => useCapabilities([]))
    expect(result.current).toBe(true)
  })
})
