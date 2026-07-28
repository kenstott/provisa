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

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import type { ReactElement } from 'react';
import { MemoryRouter } from 'react-router-dom';
import { render, screen, fireEvent, waitFor } from '../test-utils/render';
import { LoginPage } from '../pages/LoginPage';
import { CLAIMED_ADMIN_FLAG } from '../components/PlatformAdminWelcomeModal';

// LoginPage calls useNavigate; the shared render wrapper has no Router, so provide one here.
const renderLogin = (ui: ReactElement) => render(<MemoryRouter>{ui}</MemoryRouter>);

// Spread the real module: vmThreads + fileParallelism:false share one module registry, so a
// replace-everything factory here leaks into other files and drops exports they need.
vi.mock('../api/admin', async (importOriginal) => ({
  ...(await importOriginal<typeof import('../api/admin')>()),
  fetchProviderType: vi.fn().mockResolvedValue(null),
  // Stubbed rather than left real: it fetches on mount, and these tests queue single-use
  // global-fetch responses for the login POST that a stray call would consume.
  fetchBootstrapStatus: vi.fn().mockResolvedValue(false),
  // REQ-1290: the claim is a real POST; stub it so these tests can assert whether the page issues
  // it, without a network call consuming a queued global-fetch response.
  claimBootstrap: vi.fn().mockResolvedValue(true),
  registerAccount: vi.fn(),
}));

// The Firebase sign-in paths import this lazily; stub it so a provider click resolves to a token
// without an SDK or network.
vi.mock('../lib/firebase', () => ({
  signInWithGoogle: vi.fn().mockResolvedValue('firebase-id-token'),
  signInWithGithub: vi.fn(),
  signInWithMicrosoft: vi.fn(),
  signInWithEmailPassword: vi.fn(),
  registerWithEmailPassword: vi.fn(),
}));

import { fetchProviderType, fetchBootstrapStatus, claimBootstrap } from '../api/admin';
const mockFetchProviderType = vi.mocked(fetchProviderType);
const mockFetchBootstrapStatus = vi.mocked(fetchBootstrapStatus);
const mockClaimBootstrap = vi.mocked(claimBootstrap);

describe('LoginPage', () => {
  const onLoginSuccess = vi.fn();

  beforeEach(() => {
    onLoginSuccess.mockReset();
    mockFetchProviderType.mockResolvedValue(null);
    mockFetchBootstrapStatus.mockResolvedValue(false);
    vi.restoreAllMocks();
    // Re-apply the mocks after restoreAllMocks
    mockFetchProviderType.mockResolvedValue(null);
    mockFetchBootstrapStatus.mockResolvedValue(false);
    mockClaimBootstrap.mockReset();
    mockClaimBootstrap.mockResolvedValue(true);
  });

  afterEach(() => {
    localStorage.removeItem('provisa_token');
    localStorage.removeItem(CLAIMED_ADMIN_FLAG);
  });

  // ── authDisabled mode ──────────────────────────────────────────────────────

  it('renders "Authentication not configured" when authDisabled is true', () => {
    renderLogin(<LoginPage onLoginSuccess={onLoginSuccess} authDisabled />);

    expect(screen.getByRole('heading', { name: 'Login' })).toBeInTheDocument();
    expect(screen.getByText('Authentication not configured')).toBeInTheDocument();
    expect(screen.queryByLabelText('Username')).not.toBeInTheDocument();
  });

  // ── Form rendering ─────────────────────────────────────────────────────────

  it('renders login form with username, password fields and submit button', async () => {
    renderLogin(<LoginPage onLoginSuccess={onLoginSuccess} authDisabled={false} />);

    expect(await screen.findByRole('heading', { name: 'Login' })).toBeInTheDocument();
    expect(screen.getByLabelText('Username')).toBeInTheDocument();
    expect(screen.getByLabelText('Password')).toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Login' })).toBeInTheDocument();
  });

  it('password field has type="password"', async () => {
    renderLogin(<LoginPage onLoginSuccess={onLoginSuccess} authDisabled={false} />);

    expect(await screen.findByLabelText('Password')).toHaveAttribute('type', 'password');
  });

  it('username field has autocomplete="username"', async () => {
    renderLogin(<LoginPage onLoginSuccess={onLoginSuccess} authDisabled={false} />);

    expect(await screen.findByLabelText('Username')).toHaveAttribute('autocomplete', 'username');
  });

  // ── Successful login ───────────────────────────────────────────────────────

  it('calls onLoginSuccess with the token on successful login', async () => {
    vi.spyOn(globalThis, 'fetch').mockResolvedValueOnce({
      ok: true,
      json: async () => ({ access_token: 'my-test-token' }),
    } as Response);

    renderLogin(<LoginPage onLoginSuccess={onLoginSuccess} authDisabled={false} />);

    fireEvent.change(await screen.findByLabelText('Username'), { target: { value: 'admin' } });
    fireEvent.change(screen.getByLabelText('Password'), { target: { value: 'secret' } });
    fireEvent.click(screen.getByRole('button', { name: 'Login' }));

    await waitFor(() => {
      expect(onLoginSuccess).toHaveBeenCalledWith('my-test-token');
    });
  });

  it('stores token in localStorage on successful login', async () => {
    vi.spyOn(globalThis, 'fetch').mockResolvedValueOnce({
      ok: true,
      json: async () => ({ access_token: 'stored-token' }),
    } as Response);

    renderLogin(<LoginPage onLoginSuccess={onLoginSuccess} authDisabled={false} />);

    fireEvent.change(await screen.findByLabelText('Username'), { target: { value: 'admin' } });
    fireEvent.change(screen.getByLabelText('Password'), { target: { value: 'secret' } });
    fireEvent.click(screen.getByRole('button', { name: 'Login' }));

    await waitFor(() => {
      expect(localStorage.getItem('provisa_token')).toBe('stored-token');
    });
  });

  // ── Failed login ───────────────────────────────────────────────────────────

  it('shows error message when credentials are invalid', async () => {
    vi.spyOn(globalThis, 'fetch').mockResolvedValueOnce({
      ok: false,
      status: 401,
      statusText: 'Unauthorized',
      json: async () => ({ detail: 'Invalid credentials' }),
    } as Response);

    renderLogin(<LoginPage onLoginSuccess={onLoginSuccess} authDisabled={false} />);

    fireEvent.change(await screen.findByLabelText('Username'), { target: { value: 'wrong' } });
    fireEvent.change(screen.getByLabelText('Password'), { target: { value: 'bad' } });
    fireEvent.click(screen.getByRole('button', { name: 'Login' }));

    await waitFor(() => {
      expect(screen.getByText('Invalid credentials')).toBeInTheDocument();
    });
    expect(onLoginSuccess).not.toHaveBeenCalled();
  });

  it('does not call onLoginSuccess on failed login', async () => {
    vi.spyOn(globalThis, 'fetch').mockResolvedValueOnce({
      ok: false,
      status: 403,
      statusText: 'Forbidden',
      json: async () => ({ detail: 'Forbidden' }),
    } as Response);

    renderLogin(<LoginPage onLoginSuccess={onLoginSuccess} authDisabled={false} />);

    fireEvent.change(await screen.findByLabelText('Username'), { target: { value: 'u' } });
    fireEvent.change(screen.getByLabelText('Password'), { target: { value: 'p' } });
    fireEvent.click(screen.getByRole('button', { name: 'Login' }));

    await waitFor(() => {
      expect(screen.getByText('Forbidden')).toBeInTheDocument();
    });
    expect(onLoginSuccess).not.toHaveBeenCalled();
  });

  // ── Loading state ──────────────────────────────────────────────────────────

  it('shows "Logging in..." and disables button while request is in flight', async () => {
    let resolveRequest!: (value: Response) => void;
    const pendingFetch = new Promise<Response>((res) => {
      resolveRequest = res;
    });
    vi.spyOn(globalThis, 'fetch').mockReturnValueOnce(pendingFetch);

    renderLogin(<LoginPage onLoginSuccess={onLoginSuccess} authDisabled={false} />);

    fireEvent.change(await screen.findByLabelText('Username'), { target: { value: 'admin' } });
    fireEvent.change(screen.getByLabelText('Password'), { target: { value: 'secret' } });
    fireEvent.click(screen.getByRole('button', { name: 'Login' }));

    await waitFor(() => {
      expect(screen.getByRole('button', { name: 'Logging in...' })).toBeDisabled();
    });

    // Resolve so we don't leave the test hanging
    resolveRequest({ ok: true, json: async () => ({ access_token: 'tok' }) } as Response);
  });

  // ── First-login platform-admin notice (REQ-1288) ───────────────────────────

  it('warns that signing in first claims platform admin, and still offers the providers', async () => {
    mockFetchBootstrapStatus.mockResolvedValue(true);
    mockFetchProviderType.mockResolvedValue('firebase');

    renderLogin(<LoginPage onLoginSuccess={onLoginSuccess} authDisabled={false} />);

    expect(await screen.findByTestId('first-login-notice')).toHaveTextContent(
      /platform administrator/i,
    );
    // The warning precedes the choice rather than replacing it — the user still signs in here.
    expect(screen.getByTestId('firebase-signin-button')).toBeInTheDocument();
  });

  it('shows the notice on the basic-auth form too', async () => {
    mockFetchBootstrapStatus.mockResolvedValue(true);

    renderLogin(<LoginPage onLoginSuccess={onLoginSuccess} authDisabled={false} />);

    expect(await screen.findByTestId('first-login-notice')).toBeInTheDocument();
    expect(screen.getByLabelText('Username')).toBeInTheDocument();
  });

  it('stays silent once the platform-admin slot is claimed', async () => {
    mockFetchProviderType.mockResolvedValue('firebase');

    renderLogin(<LoginPage onLoginSuccess={onLoginSuccess} authDisabled={false} />);

    expect(await screen.findByTestId('firebase-signin-button')).toBeInTheDocument();
    expect(screen.queryByTestId('first-login-notice')).not.toBeInTheDocument();
  });

  it('renders the sign-in page normally when the bootstrap probe fails', async () => {
    mockFetchProviderType.mockResolvedValue('firebase');
    mockFetchBootstrapStatus.mockRejectedValue(new Error('boom'));

    renderLogin(<LoginPage onLoginSuccess={onLoginSuccess} authDisabled={false} />);

    // An unreachable probe must not decide which sign-in UI renders.
    expect(await screen.findByTestId('firebase-signin-button')).toBeInTheDocument();
    expect(screen.queryByTestId('first-login-notice')).not.toBeInTheDocument();
  });

  // ── Claiming the platform-admin slot (REQ-1290) ────────────────────────────

  it('claims the platform-admin slot when a provider is chosen on the first-login page', async () => {
    // The server no longer claims the slot while validating a token, so this click is the
    // deliberate act the notice asked for — without it the deployment stays unadministered.
    mockFetchBootstrapStatus.mockResolvedValue(true);
    mockFetchProviderType.mockResolvedValue('firebase');

    renderLogin(<LoginPage onLoginSuccess={onLoginSuccess} authDisabled={false} />);

    fireEvent.click(await screen.findByTestId('firebase-signin-button'));

    await waitFor(() => expect(mockClaimBootstrap).toHaveBeenCalled());
    expect(onLoginSuccess).toHaveBeenCalledWith('firebase-id-token');
  });

  it('does not claim when the slot is already held', async () => {
    mockFetchProviderType.mockResolvedValue('firebase');

    renderLogin(<LoginPage onLoginSuccess={onLoginSuccess} authDisabled={false} />);

    fireEvent.click(await screen.findByTestId('firebase-signin-button'));

    await waitFor(() => expect(onLoginSuccess).toHaveBeenCalled());
    expect(mockClaimBootstrap).not.toHaveBeenCalled();
  });

  it('claims from the basic-auth form on the first-login page too', async () => {
    mockFetchBootstrapStatus.mockResolvedValue(true);
    vi.spyOn(globalThis, 'fetch').mockResolvedValueOnce({
      ok: true,
      json: async () => ({ access_token: 'basic-token' }),
    } as Response);

    renderLogin(<LoginPage onLoginSuccess={onLoginSuccess} authDisabled={false} />);

    fireEvent.change(await screen.findByLabelText('Username'), { target: { value: 'admin' } });
    fireEvent.change(screen.getByLabelText('Password'), { target: { value: 'secret' } });
    fireEvent.click(screen.getByRole('button', { name: 'Login' }));

    await waitFor(() => expect(mockClaimBootstrap).toHaveBeenCalled());
  });

  // ── Recording the claim for the welcome modal (REQ-1294) ───────────────────

  it('records a successful claim so the shell can disclose the new platform-admin role', async () => {
    mockFetchBootstrapStatus.mockResolvedValue(true);
    mockFetchProviderType.mockResolvedValue('firebase');
    mockClaimBootstrap.mockResolvedValue(true);

    renderLogin(<LoginPage onLoginSuccess={onLoginSuccess} authDisabled={false} />);

    fireEvent.click(await screen.findByTestId('firebase-signin-button'));

    await waitFor(() =>
      expect(localStorage.getItem(CLAIMED_ADMIN_FLAG)).toBe('1'),
    );
  });

  it('records nothing when the server refused the claim', async () => {
    // A refused claim means someone else holds the slot; announcing "you are now the platform
    // administrator" would be a statement about an outcome that did not happen.
    mockFetchBootstrapStatus.mockResolvedValue(true);
    mockFetchProviderType.mockResolvedValue('firebase');
    mockClaimBootstrap.mockResolvedValue(false);

    renderLogin(<LoginPage onLoginSuccess={onLoginSuccess} authDisabled={false} />);

    fireEvent.click(await screen.findByTestId('firebase-signin-button'));

    await waitFor(() => expect(onLoginSuccess).toHaveBeenCalled());
    expect(localStorage.getItem(CLAIMED_ADMIN_FLAG)).toBeNull();
  });

  it('records nothing on an ordinary sign-in to an already-administered deployment', async () => {
    mockFetchProviderType.mockResolvedValue('firebase');

    renderLogin(<LoginPage onLoginSuccess={onLoginSuccess} authDisabled={false} />);

    fireEvent.click(await screen.findByTestId('firebase-signin-button'));

    await waitFor(() => expect(onLoginSuccess).toHaveBeenCalled());
    expect(localStorage.getItem(CLAIMED_ADMIN_FLAG)).toBeNull();
  });

  // ── Input binding ──────────────────────────────────────────────────────────

  it('updates username and password fields as user types', async () => {
    renderLogin(<LoginPage onLoginSuccess={onLoginSuccess} authDisabled={false} />);

    const userInput = await screen.findByLabelText('Username');
    const passInput = screen.getByLabelText('Password');

    fireEvent.change(userInput, { target: { value: 'alice' } });
    fireEvent.change(passInput, { target: { value: 'hunter2' } });

    expect(userInput).toHaveValue('alice');
    expect(passInput).toHaveValue('hunter2');
  });
});
