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

// CodeMirror renders complex DOM that breaks text-content queries and introduces
// extra role="textbox" elements.  Stub it to a plain <pre> for unit tests.
vi.mock('@uiw/react-codemirror', () => ({
  default: ({ value }: { value: string }) => <pre>{value}</pre>,
}))

// --------------------------------------------------------------------------
// Mock helpers
// --------------------------------------------------------------------------

interface MockQuery {
  id: number
  queryText: string
  developerId: string | null
  status: string
  visibleTo?: string[]
  [key: string]: unknown
}

const PENDING_QUERIES: MockQuery[] = [
  {
    id: 1,
    queryText: 'query GetOrders { orders { id total } }',
    developerId: 'dev@co.com',
    status: 'pending',
    visibleTo: [],
  },
  {
    id: 2,
    queryText: 'query GetCustomers { customers { id name } }',
    developerId: 'dev@co.com',
    status: 'pending',
    visibleTo: [],
  },
]

function makeFetchMock(queries: MockQuery[] = PENDING_QUERIES) {
  return vi.fn().mockImplementation((_url: string, opts?: RequestInit) => {
    const body = opts?.body ? JSON.parse(opts.body as string) : {}
    const query: string = body.query ?? ''

    if (query.includes('governedQueries')) {
      return Promise.resolve({
        ok: true,
        json: async () => ({ data: { governedQueries: queries } }),
      } as Response)
    }

    if (query.includes('mutation')) {
      return Promise.resolve({
        ok: true,
        json: async () => ({
          data: {
            approveQuery: { success: true },
            rejectQuery: { success: true },
            revokeQuery: { success: true },
          },
        }),
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
    vi.spyOn(globalThis, 'fetch').mockReturnValue(new Promise(() => {}))
    render(<ApprovalsPage />)
    expect(screen.getByText('Loading governed queries...')).toBeInTheDocument()
  })

  // ── Empty state ────────────────────────────────────────────────────────────

  it('shows empty state when no queries exist', async () => {
    vi.spyOn(globalThis, 'fetch').mockImplementation(makeFetchMock([]))
    render(<ApprovalsPage />)
    await waitFor(() => {
      expect(screen.getByText('No governed queries.')).toBeInTheDocument()
    })
  })

  // ── List rendering ─────────────────────────────────────────────────────────

  it('renders a summary row for each query', async () => {
    vi.spyOn(globalThis, 'fetch').mockImplementation(makeFetchMock())
    render(<ApprovalsPage />)
    await waitFor(() => {
      expect(screen.getByText('GetOrders')).toBeInTheDocument()
      expect(screen.getByText('GetCustomers')).toBeInTheDocument()
    })
  })

  it('displays extracted query operation names', async () => {
    vi.spyOn(globalThis, 'fetch').mockImplementation(makeFetchMock())
    render(<ApprovalsPage />)
    await waitFor(() => {
      expect(screen.getByText('GetOrders')).toBeInTheDocument()
      expect(screen.getByText('GetCustomers')).toBeInTheDocument()
    })
  })

  it('displays developer id in summary row', async () => {
    vi.spyOn(globalThis, 'fetch').mockImplementation(makeFetchMock())
    render(<ApprovalsPage />)
    await waitFor(() => {
      const devLines = screen.getAllByText('dev@co.com')
      expect(devLines.length).toBeGreaterThanOrEqual(2)
    })
  })

  // ── Expand on click ────────────────────────────────────────────────────────

  it('shows query text after clicking to expand', async () => {
    vi.spyOn(globalThis, 'fetch').mockImplementation(makeFetchMock())
    render(<ApprovalsPage />)

    await waitFor(() => screen.getByText('GetOrders'))
    fireEvent.click(screen.getByText('GetOrders').closest('[role="button"]')!)

    await waitFor(() => {
      expect(screen.getByText('query GetOrders { orders { id total } }')).toBeInTheDocument()
    })
  })

  it('shows Approve and Reject buttons after expanding a pending item', async () => {
    vi.spyOn(globalThis, 'fetch').mockImplementation(makeFetchMock())
    render(<ApprovalsPage />)

    await waitFor(() => screen.getByText('GetOrders'))
    fireEvent.click(screen.getByText('GetOrders').closest('[role="button"]')!)

    await waitFor(() => {
      expect(screen.getByRole('button', { name: 'Approve' })).toBeInTheDocument()
      expect(screen.getByRole('button', { name: 'Reject' })).toBeInTheDocument()
    })
  })

  it('collapses row when clicked again', async () => {
    vi.spyOn(globalThis, 'fetch').mockImplementation(makeFetchMock())
    render(<ApprovalsPage />)

    await waitFor(() => screen.getByText('GetOrders'))
    const summaryRow = screen.getByText('GetOrders').closest('[role="button"]')!
    fireEvent.click(summaryRow)
    await waitFor(() => screen.getByText('query GetOrders { orders { id total } }'))

    fireEvent.click(summaryRow)
    await waitFor(() => {
      expect(screen.queryByText('query GetOrders { orders { id total } }')).not.toBeInTheDocument()
    })
  })

  // ── Reject flow ────────────────────────────────────────────────────────────

  it('shows rejection form when Reject is clicked', async () => {
    vi.spyOn(globalThis, 'fetch').mockImplementation(makeFetchMock())
    render(<ApprovalsPage />)

    await waitFor(() => screen.getByText('GetOrders'))
    fireEvent.click(screen.getByText('GetOrders').closest('[role="button"]')!)
    await waitFor(() => screen.getByRole('button', { name: 'Reject' }))
    fireEvent.click(screen.getByRole('button', { name: 'Reject' }))

    expect(screen.getByRole('button', { name: 'Submit Rejection' })).toBeInTheDocument()
    expect(screen.getByRole('textbox', { name: /rejection reason/i })).toBeInTheDocument()
  })

  it('Submit Rejection button is disabled when reason is empty', async () => {
    vi.spyOn(globalThis, 'fetch').mockImplementation(makeFetchMock())
    render(<ApprovalsPage />)

    await waitFor(() => screen.getByText('GetOrders'))
    fireEvent.click(screen.getByText('GetOrders').closest('[role="button"]')!)
    await waitFor(() => screen.getByRole('button', { name: 'Reject' }))
    fireEvent.click(screen.getByRole('button', { name: 'Reject' }))

    expect(screen.getByRole('button', { name: 'Submit Rejection' })).toBeDisabled()
  })

  it('Submit Rejection button is enabled when reason is filled in', async () => {
    vi.spyOn(globalThis, 'fetch').mockImplementation(makeFetchMock())
    render(<ApprovalsPage />)

    await waitFor(() => screen.getByText('GetOrders'))
    fireEvent.click(screen.getByText('GetOrders').closest('[role="button"]')!)
    await waitFor(() => screen.getByRole('button', { name: 'Reject' }))
    fireEvent.click(screen.getByRole('button', { name: 'Reject' }))

    fireEvent.change(screen.getByRole('textbox', { name: /rejection reason/i }), {
      target: { value: 'Missing WHERE clause for RLS compliance' },
    })

    expect(screen.getByRole('button', { name: 'Submit Rejection' })).not.toBeDisabled()
  })

  it('hides rejection form when Cancel is clicked', async () => {
    vi.spyOn(globalThis, 'fetch').mockImplementation(makeFetchMock())
    render(<ApprovalsPage />)

    await waitFor(() => screen.getByText('GetOrders'))
    fireEvent.click(screen.getByText('GetOrders').closest('[role="button"]')!)
    await waitFor(() => screen.getByRole('button', { name: 'Reject' }))
    fireEvent.click(screen.getByRole('button', { name: 'Reject' }))
    expect(screen.getByRole('button', { name: 'Submit Rejection' })).toBeInTheDocument()

    fireEvent.click(screen.getByRole('button', { name: 'Cancel' }))

    expect(screen.queryByRole('button', { name: 'Submit Rejection' })).not.toBeInTheDocument()
  })

  // ── Approve flow ───────────────────────────────────────────────────────────

  it('opens confirm dialog when Approve is clicked', async () => {
    vi.spyOn(globalThis, 'fetch').mockImplementation(makeFetchMock())
    render(<ApprovalsPage />)

    await waitFor(() => screen.getByText('GetOrders'))
    fireEvent.click(screen.getByText('GetOrders').closest('[role="button"]')!)
    await waitFor(() => screen.getByRole('button', { name: 'Approve' }))
    fireEvent.click(screen.getByRole('button', { name: 'Approve' }))

    expect(screen.getByRole('button', { name: 'Confirm' })).toBeInTheDocument()
    expect(screen.getByRole('button', { name: 'Cancel' })).toBeInTheDocument()
  })

  // ── Revoke flow ────────────────────────────────────────────────────────────

  it('shows Revoke button for approved queries after expand', async () => {
    const approved: MockQuery[] = [
      { id: 10, queryText: 'query ApprovedQ { foo }', developerId: 'admin', status: 'approved', visibleTo: [] },
    ]
    vi.spyOn(globalThis, 'fetch').mockImplementation(makeFetchMock(approved))
    render(<ApprovalsPage />)

    await waitFor(() => screen.getByText('ApprovedQ'))
    fireEvent.click(screen.getByText('ApprovedQ').closest('[role="button"]')!)

    await waitFor(() => {
      expect(screen.getByRole('button', { name: 'Revoke' })).toBeInTheDocument()
    })
  })

  // ── Search ─────────────────────────────────────────────────────────────────

  it('filters list by search term', async () => {
    vi.spyOn(globalThis, 'fetch').mockImplementation(makeFetchMock())
    render(<ApprovalsPage />)

    await waitFor(() => screen.getByText('GetOrders'))

    const searchInput = screen.getByPlaceholderText('Search by name, status, submitter...')
    fireEvent.change(searchInput, { target: { value: 'GetOrders' } })

    expect(screen.getByText('GetOrders')).toBeInTheDocument()
    expect(screen.queryByText('GetCustomers')).not.toBeInTheDocument()
  })

  it('shows "no results" message when search matches nothing', async () => {
    vi.spyOn(globalThis, 'fetch').mockImplementation(makeFetchMock())
    render(<ApprovalsPage />)

    await waitFor(() => screen.getByText('GetOrders'))

    const searchInput = screen.getByPlaceholderText('Search by name, status, submitter...')
    fireEvent.change(searchInput, { target: { value: 'xyznonexistent' } })

    expect(screen.getByText('No results match your search.')).toBeInTheDocument()
  })

  // ── Edge cases ─────────────────────────────────────────────────────────────

  it('shows fallback query name for unnamed queries', async () => {
    const unnamed: MockQuery[] = [
      { id: 99, queryText: '{ orders { id } }', developerId: null, status: 'pending', visibleTo: [] },
    ]
    vi.spyOn(globalThis, 'fetch').mockImplementation(makeFetchMock(unnamed))
    render(<ApprovalsPage />)

    await waitFor(() => {
      expect(screen.getByText('Query #99')).toBeInTheDocument()
    })
  })

  it('shows "unknown" when developerId is null', async () => {
    const noAuthor: MockQuery[] = [
      { id: 5, queryText: 'query Foo { foo }', developerId: null, status: 'pending', visibleTo: [] },
    ]
    vi.spyOn(globalThis, 'fetch').mockImplementation(makeFetchMock(noAuthor))
    render(<ApprovalsPage />)

    await waitFor(() => {
      expect(screen.getByText('unknown')).toBeInTheDocument()
    })
  })
})
