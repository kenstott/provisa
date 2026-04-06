// Copyright (c) 2026 Kenneth Stott
// Canary: 4f67543d-95be-47e3-a3e1-50e859d09fe3
// Canary: placeholder
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { describe, it, expect } from 'vitest'
import type { CompileResult } from '../../api/admin'

// Inline the normalization logic (same as ProvisaToolsContent) so we can test it
// without mounting a React component.
function normalizeCompileResponse(
  raw: CompileResult | { queries: CompileResult[] },
): CompileResult[] {
  if (Array.isArray(raw)) return raw as unknown as CompileResult[]
  if ('queries' in raw && Array.isArray(raw.queries)) return raw.queries
  return [raw as CompileResult]
}

function isAliased(c: CompileResult): boolean {
  return !!(c.root_field && c.canonical_field && c.root_field !== c.canonical_field)
}

function hasColumnAliases(c: CompileResult): boolean {
  return c.column_aliases?.length > 0
}

const base: CompileResult = {
  sql: 'SELECT 1',
  semantic_sql: 'SELECT 1',
  trino_sql: null,
  direct_sql: null,
  params: [],
  route: 'direct',
  route_reason: 'single source',
  sources: ['pg'],
  root_field: 'orders',
  canonical_field: 'orders',
  column_aliases: [],
}

describe('normalizeCompileResponse', () => {
  it('wraps a single result in an array', () => {
    const result = normalizeCompileResponse(base)
    expect(result).toHaveLength(1)
    expect(result[0]).toBe(base)
  })

  it('unwraps { queries: [...] } into an array', () => {
    const second = { ...base, root_field: 'customers', canonical_field: 'customers' }
    const result = normalizeCompileResponse({ queries: [base, second] })
    expect(result).toHaveLength(2)
    expect(result[0].root_field).toBe('orders')
    expect(result[1].root_field).toBe('customers')
  })

  it('preserves each root_field from the multi-root response', () => {
    const fields = ['a', 'b', 'c']
    const queries = fields.map(f => ({ ...base, root_field: f, canonical_field: f }))
    const result = normalizeCompileResponse({ queries })
    expect(result.map(r => r.root_field)).toEqual(fields)
  })
})

describe('isAliased', () => {
  it('returns false when root_field equals canonical_field', () => {
    expect(isAliased(base)).toBe(false)
  })

  it('returns true when root_field differs from canonical_field', () => {
    const aliased = { ...base, root_field: 'my_alias', canonical_field: 'orders' }
    expect(isAliased(aliased)).toBe(true)
  })

  it('returns false when canonical_field is empty string', () => {
    const noCanonical = { ...base, canonical_field: '' }
    expect(isAliased(noCanonical)).toBe(false)
  })
})

describe('hasColumnAliases', () => {
  it('returns false when column_aliases is empty', () => {
    expect(hasColumnAliases(base)).toBe(false)
  })

  it('returns true when column_aliases has entries', () => {
    const withAlias = { ...base, column_aliases: [{ field_name: 'userId', column: 'user_id' }] }
    expect(hasColumnAliases(withAlias)).toBe(true)
  })
})
