// Copyright (c) 2026 Kenneth Stott
// Canary: cf9299a7-d2e9-4c31-82bd-928e8f371520
// Canary: placeholder
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import { render, screen, fireEvent, waitFor } from '@testing-library/react'
import { LoginPage } from '../pages/LoginPage'

describe('LoginPage', () => {
  const onLoginSuccess = vi.fn()

  beforeEach(() => {
    onLoginSuccess.mockReset()
    // Reset fetch mock between tests
    vi.restoreAllMocks()
  })

  afterEach(() => {
    localStorage.removeItem('provisa_token')
  })

  // ── authDisabled mode ──────────────────────────────────────────────────────

  it('renders "Authentication not configured" when authDisabled is true', () => {
    render(<LoginPage onLoginSuccess={onLoginSuccess} authDisabled />)

    expect(screen.getByRole('heading', { name: 'Login' })).toBeInTheDocument()
    expect(screen.getByText('Authentication not configured')).toBeInTheDocument()
    expect(screen.queryByLabelText('Username')).not.toBeInTheDocument()
  })

  // ── Form rendering ─────────────────────────────────────────────────────────

  it('renders login form with username, password fields and submit button', () => {
    render(<LoginPage onLoginSuccess={onLoginSuccess} authDisabled={false} />)

    expect(screen.getByRole('heading', { name: 'Login' })).toBeInTheDocument()
    expect(screen.getByLabelText('Username')).toBeInTheDocument()
    expect(screen.getByLabelText('Password')).toBeInTheDocument()
    expect(screen.getByRole('button', { name: 'Login' })).toBeInTheDocument()
  })

  it('password field has type="password"', () => {
    render(<LoginPage onLoginSuccess={onLoginSuccess} authDisabled={false} />)

    expect(screen.getByLabelText('Password')).toHaveAttribute('type', 'password')
  })

  it('username field has autocomplete="username"', () => {
    render(<LoginPage onLoginSuccess={onLoginSuccess} authDisabled={false} />)

    expect(screen.getByLabelText('Username')).toHaveAttribute('autocomplete', 'username')
  })

  // ── Successful login ───────────────────────────────────────────────────────

  it('calls onLoginSuccess with the token on successful login', async () => {
    vi.spyOn(globalThis, 'fetch').mockResolvedValueOnce({
      ok: true,
      json: async () => ({ access_token: 'my-test-token' }),
    } as Response)

    render(<LoginPage onLoginSuccess={onLoginSuccess} authDisabled={false} />)

    fireEvent.change(screen.getByLabelText('Username'), { target: { value: 'admin' } })
    fireEvent.change(screen.getByLabelText('Password'), { target: { value: 'secret' } })
    fireEvent.click(screen.getByRole('button', { name: 'Login' }))

    await waitFor(() => {
      expect(onLoginSuccess).toHaveBeenCalledWith('my-test-token')
    })
  })

  it('stores token in localStorage on successful login', async () => {
    vi.spyOn(globalThis, 'fetch').mockResolvedValueOnce({
      ok: true,
      json: async () => ({ access_token: 'stored-token' }),
    } as Response)

    render(<LoginPage onLoginSuccess={onLoginSuccess} authDisabled={false} />)

    fireEvent.change(screen.getByLabelText('Username'), { target: { value: 'admin' } })
    fireEvent.change(screen.getByLabelText('Password'), { target: { value: 'secret' } })
    fireEvent.click(screen.getByRole('button', { name: 'Login' }))

    await waitFor(() => {
      expect(localStorage.getItem('provisa_token')).toBe('stored-token')
    })
  })

  // ── Failed login ───────────────────────────────────────────────────────────

  it('shows error message when credentials are invalid', async () => {
    vi.spyOn(globalThis, 'fetch').mockResolvedValueOnce({
      ok: false,
      status: 401,
      statusText: 'Unauthorized',
      json: async () => ({ detail: 'Invalid credentials' }),
    } as Response)

    render(<LoginPage onLoginSuccess={onLoginSuccess} authDisabled={false} />)

    fireEvent.change(screen.getByLabelText('Username'), { target: { value: 'wrong' } })
    fireEvent.change(screen.getByLabelText('Password'), { target: { value: 'bad' } })
    fireEvent.click(screen.getByRole('button', { name: 'Login' }))

    await waitFor(() => {
      expect(screen.getByText('Invalid credentials')).toBeInTheDocument()
    })
    expect(onLoginSuccess).not.toHaveBeenCalled()
  })

  it('does not call onLoginSuccess on failed login', async () => {
    vi.spyOn(globalThis, 'fetch').mockResolvedValueOnce({
      ok: false,
      status: 403,
      statusText: 'Forbidden',
      json: async () => ({ detail: 'Forbidden' }),
    } as Response)

    render(<LoginPage onLoginSuccess={onLoginSuccess} authDisabled={false} />)

    fireEvent.change(screen.getByLabelText('Username'), { target: { value: 'u' } })
    fireEvent.change(screen.getByLabelText('Password'), { target: { value: 'p' } })
    fireEvent.click(screen.getByRole('button', { name: 'Login' }))

    await waitFor(() => {
      expect(screen.getByText('Forbidden')).toBeInTheDocument()
    })
    expect(onLoginSuccess).not.toHaveBeenCalled()
  })

  // ── Loading state ──────────────────────────────────────────────────────────

  it('shows "Logging in..." and disables button while request is in flight', async () => {
    let resolveRequest!: (value: Response) => void
    const pendingFetch = new Promise<Response>((res) => { resolveRequest = res })
    vi.spyOn(globalThis, 'fetch').mockReturnValueOnce(pendingFetch)

    render(<LoginPage onLoginSuccess={onLoginSuccess} authDisabled={false} />)

    fireEvent.change(screen.getByLabelText('Username'), { target: { value: 'admin' } })
    fireEvent.change(screen.getByLabelText('Password'), { target: { value: 'secret' } })
    fireEvent.click(screen.getByRole('button', { name: 'Login' }))

    await waitFor(() => {
      expect(screen.getByRole('button', { name: 'Logging in...' })).toBeDisabled()
    })

    // Resolve so we don't leave the test hanging
    resolveRequest({ ok: true, json: async () => ({ access_token: 'tok' }) } as Response)
  })

  // ── Input binding ──────────────────────────────────────────────────────────

  it('updates username and password fields as user types', () => {
    render(<LoginPage onLoginSuccess={onLoginSuccess} authDisabled={false} />)

    const userInput = screen.getByLabelText('Username')
    const passInput = screen.getByLabelText('Password')

    fireEvent.change(userInput, { target: { value: 'alice' } })
    fireEvent.change(passInput, { target: { value: 'hunter2' } })

    expect(userInput).toHaveValue('alice')
    expect(passInput).toHaveValue('hunter2')
  })
})
