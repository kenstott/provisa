// Copyright (c) 2026 Kenneth Stott
// Canary: df12d23a-b49e-4531-8a33-22c93c9e2b2d
// Canary: placeholder
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { describe, it, expect, vi, beforeEach } from 'vitest'
import { render, screen, fireEvent, waitFor } from '@testing-library/react'
import { ApprovalsPage } from '../pages/ApprovalsPage'

// --------------------------------------------------------------------------
// Mock helpers
// --------------------------------------------------------------------------

const PENDING_QUERIES = [
  {
    id: 1,
    queryText: 'query GetOrders { orders { id total } }',
    developerId: 'dev@co.com',
    status: 'pending',
  },
  {
    id: 2,
    queryText: 'query GetCustomers { customers { id name } }',
    developerId: 'dev@co.com',
    status: 'pending',
  },
]

function makeFetchMock(pendingQueries = PENDING_QUERIES) {
  return vi.fn().mockImplementation((url: string, opts?: RequestInit) => {
    const body = opts?.body ? JSON.parse(opts.body as string) : {}
    const query: string = body.query ?? ''

    if (query.includes('persistedQueries')) {
      return Promise.resolve({
        ok: true,
        json: async () => ({
          data: { persistedQueries: pendingQueries },
        }),
      } as Response)
    }

    // approveQuery / rejectQuery mutations
    if (query.includes('mutation')) {
      return Promise.resolve({
        ok: true,
        json: async () => ({ data: { approveQuery: { success: true }, rejectQuery: { success: true } } }),
      } as Response)
    }

    return Promise.resolve({ ok: true, json: async () => ({ data: {} }) } as Response)
  })
}

describe('ApprovalsPage', () => {
  beforeEach(() => {
    vi.restoreAllMocks()
  })

  // ── Loading state ──────────────────────────────────────────────────────────

  it('shows loading message initially', () => {
    // Never resolve fetch so we stay in loading state
    vi.spyOn(globalThis, 'fetch').mockReturnValue(new Promise(() => {}))

    render(<ApprovalsPage />)

    expect(screen.getByText('Loading approval queue...')).toBeInTheDocument()
  })

  // ── Empty state ────────────────────────────────────────────────────────────

  it('shows empty state message when no queries are pending', async () => {
    vi.spyOn(globalThis, 'fetch').mockImplementation(makeFetchMock([]))

    render(<ApprovalsPage />)

    await waitFor(() => {
      expect(screen.getByText('No queries pending approval.')).toBeInTheDocument()
    })
  })

  // ── List rendering ─────────────────────────────────────────────────────────

  it('renders the correct number of pending query cards', async () => {
    vi.spyOn(globalThis, 'fetch').mockImplementation(makeFetchMock())

    render(<ApprovalsPage />)

    await waitFor(() => {
      expect(screen.getAllByRole('button', { name: 'Reject' })).toHaveLength(2)
    })
    expect(screen.getAllByRole('button', { name: 'Approve' })).toHaveLength(2)
  })

  it('displays extracted query operation names as headings', async () => {
    vi.spyOn(globalThis, 'fetch').mockImplementation(makeFetchMock())

    render(<ApprovalsPage />)

    await waitFor(() => {
      expect(screen.getByRole('heading', { name: 'GetOrders' })).toBeInTheDocument()
      expect(screen.getByRole('heading', { name: 'GetCustomers' })).toBeInTheDocument()
    })
  })

  it('displays developer id as "submitted by"', async () => {
    vi.spyOn(globalThis, 'fetch').mockImplementation(makeFetchMock())

    render(<ApprovalsPage />)

    await waitFor(() => {
      const byLines = screen.getAllByText('by dev@co.com')
      expect(byLines).toHaveLength(2)
    })
  })

  it('shows query text in a pre element', async () => {
    vi.spyOn(globalThis, 'fetch').mockImplementation(makeFetchMock())

    render(<ApprovalsPage />)

    await waitFor(() => {
      expect(screen.getByText('query GetOrders { orders { id total } }')).toBeInTheDocument()
    })
  })

  // ── Reject flow ────────────────────────────────────────────────────────────

  it('shows rejection form when Reject is clicked', async () => {
    vi.spyOn(globalThis, 'fetch').mockImplementation(makeFetchMock())

    render(<ApprovalsPage />)

    await waitFor(() => screen.getAllByRole('button', { name: 'Reject' }))
    fireEvent.click(screen.getAllByRole('button', { name: 'Reject' })[0])

    expect(screen.getByRole('button', { name: 'Submit Rejection' })).toBeInTheDocument()
    expect(screen.getByRole('textbox')).toBeInTheDocument()
  })

  it('Submit Rejection button is disabled when reason is empty', async () => {
    vi.spyOn(globalThis, 'fetch').mockImplementation(makeFetchMock())

    render(<ApprovalsPage />)

    await waitFor(() => screen.getAllByRole('button', { name: 'Reject' }))
    fireEvent.click(screen.getAllByRole('button', { name: 'Reject' })[0])

    expect(screen.getByRole('button', { name: 'Submit Rejection' })).toBeDisabled()
  })

  it('Submit Rejection button is enabled when reason is filled in', async () => {
    vi.spyOn(globalThis, 'fetch').mockImplementation(makeFetchMock())

    render(<ApprovalsPage />)

    await waitFor(() => screen.getAllByRole('button', { name: 'Reject' }))
    fireEvent.click(screen.getAllByRole('button', { name: 'Reject' })[0])

    fireEvent.change(screen.getByRole('textbox'), {
      target: { value: 'Missing WHERE clause for RLS compliance' },
    })

    expect(screen.getByRole('button', { name: 'Submit Rejection' })).not.toBeDisabled()
  })

  it('hides rejection form when Cancel is clicked', async () => {
    vi.spyOn(globalThis, 'fetch').mockImplementation(makeFetchMock())

    render(<ApprovalsPage />)

    await waitFor(() => screen.getAllByRole('button', { name: 'Reject' }))
    fireEvent.click(screen.getAllByRole('button', { name: 'Reject' })[0])
    expect(screen.getByRole('button', { name: 'Submit Rejection' })).toBeInTheDocument()

    fireEvent.click(screen.getByRole('button', { name: 'Cancel' }))

    expect(screen.queryByRole('button', { name: 'Submit Rejection' })).not.toBeInTheDocument()
  })

  // ── Approve flow ───────────────────────────────────────────────────────────

  it('opens confirm dialog when Approve is clicked', async () => {
    vi.spyOn(globalThis, 'fetch').mockImplementation(makeFetchMock())

    render(<ApprovalsPage />)

    await waitFor(() => screen.getAllByRole('button', { name: 'Approve' }))
    fireEvent.click(screen.getAllByRole('button', { name: 'Approve' })[0])

    // ConfirmDialog should be visible with the Confirm button
    expect(screen.getByRole('button', { name: 'Confirm' })).toBeInTheDocument()
    expect(screen.getByRole('button', { name: 'Cancel' })).toBeInTheDocument()
  })

  it('shows fallback query name for unnamed queries', async () => {
    const unnamed = [{ id: 99, queryText: '{ orders { id } }', developerId: null, status: 'pending' }]
    vi.spyOn(globalThis, 'fetch').mockImplementation(makeFetchMock(unnamed))

    render(<ApprovalsPage />)

    await waitFor(() => {
      expect(screen.getByRole('heading', { name: 'Query #99' })).toBeInTheDocument()
    })
  })

  it('shows "unknown" when developerId is null', async () => {
    const noAuthor = [{ id: 5, queryText: 'query Foo { foo }', developerId: null, status: 'pending' }]
    vi.spyOn(globalThis, 'fetch').mockImplementation(makeFetchMock(noAuthor))

    render(<ApprovalsPage />)

    await waitFor(() => {
      expect(screen.getByText('by unknown')).toBeInTheDocument()
    })
  })
})
