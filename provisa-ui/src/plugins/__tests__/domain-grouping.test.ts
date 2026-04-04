import { describe, it, expect, vi } from 'vitest'
// @ts-ignore — CJS fork with no type declarations
import { _renderGroupedFields } from 'graphiql-explorer'

describe('_renderGroupedFields', () => {
  const renderField = vi.fn((name: string) => ({ field: name }))

  function calledFieldNames() {
    return renderField.mock.calls.map((c: unknown[]) => c[0])
  }

  beforeEach(() => {
    renderField.mockClear()
  })

  it('groups fields by domain prefix (split on "__")', () => {
    const fields: Record<string, unknown> = {
      sales__orders: {},
      sales__returns: {},
      product__items: {},
    }

    const result = _renderGroupedFields(fields, renderField)

    // Two domain folders: product, sales (sorted)
    expect(result).toHaveLength(2)
    expect(renderField).toHaveBeenCalledTimes(3)
    expect(calledFieldNames()).toEqual([
      'product__items',
      'sales__orders',
      'sales__returns',
    ])
  })

  it('places ungrouped fields (no "__") after domain folders', () => {
    const fields: Record<string, unknown> = {
      sales__orders: {},
      health: {},
      version: {},
    }

    const result = _renderGroupedFields(fields, renderField)

    // 1 domain folder + 2 ungrouped fields
    expect(result).toHaveLength(3)
    expect(calledFieldNames()).toEqual([
      'sales__orders',
      'health',
      'version',
    ])
  })

  it('returns flat list when no fields have "__" separator', () => {
    const fields: Record<string, unknown> = {
      health: {},
      version: {},
      status: {},
    }

    const result = _renderGroupedFields(fields, renderField)

    expect(result).toHaveLength(3)
    expect(renderField).toHaveBeenCalledTimes(3)
  })

  it('handles empty fields object', () => {
    const result = _renderGroupedFields({}, renderField)

    expect(result).toHaveLength(0)
    expect(renderField).not.toHaveBeenCalled()
  })

  it('sorts domains alphabetically', () => {
    const fields: Record<string, unknown> = {
      zebra__a: {},
      alpha__b: {},
      middle__c: {},
    }

    const result = _renderGroupedFields(fields, renderField)

    expect(result).toHaveLength(3)
    expect(calledFieldNames()).toEqual(['alpha__b', 'middle__c', 'zebra__a'])
  })

  it('groups multiple fields under the same domain', () => {
    const fields: Record<string, unknown> = {
      crm__contacts: {},
      crm__accounts: {},
      crm__deals: {},
    }

    const result = _renderGroupedFields(fields, renderField)

    // 1 domain folder containing 3 fields
    expect(result).toHaveLength(1)
    expect(renderField).toHaveBeenCalledTimes(3)
  })

  it('does not split on leading "__"', () => {
    const fields: Record<string, unknown> = {
      __internal: {},
      sales__orders: {},
    }

    const result = _renderGroupedFields(fields, renderField)

    // __internal has sepIdx === 0, so it's ungrouped
    // 1 domain folder (sales) + 1 ungrouped (__internal)
    expect(result).toHaveLength(2)
    expect(calledFieldNames()).toEqual(['sales__orders', '__internal'])
  })
})
