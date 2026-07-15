// Copyright (c) 2026 Kenneth Stott
// Canary: d746d5fb-9de3-4871-afd4-aadbfe397278
// Canary: placeholder
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, fireEvent, waitFor } from '../test-utils/render';
import i18n from '../i18n';
import { ConfirmDialog } from '../components/ConfirmDialog';

const t = i18n.getFixedT('en');

describe('ConfirmDialog', () => {
  const defaultProps = {
    title: 'Delete this item?',
    consequence: 'This action cannot be undone.',
    onConfirm: vi.fn(),
  };

  beforeEach(() => {
    defaultProps.onConfirm.mockReset();
  });

  const openDialog = () =>
    fireEvent.click(screen.getByRole('button', { name: 'Open Dialog' }));

  const renderDialog = () =>
    render(
      <ConfirmDialog {...defaultProps}>
        {(open) => <button onClick={open}>Open Dialog</button>}
      </ConfirmDialog>,
    );

  it('renders only the trigger element when closed', () => {
    renderDialog();
    expect(screen.getByRole('button', { name: 'Open Dialog' })).toBeInTheDocument();
    expect(screen.queryByText('Delete this item?')).not.toBeInTheDocument();
    expect(screen.queryByText('This action cannot be undone.')).not.toBeInTheDocument();
  });

  it('opens an accessible dialog with title and consequence when triggered', async () => {
    renderDialog();
    openDialog();
    // Mantine Modal exposes role="dialog" — the a11y win over the old div.
    expect(await screen.findByRole('dialog')).toBeInTheDocument();
    expect(screen.getByText('Delete this item?')).toBeInTheDocument();
    expect(screen.getByText('This action cannot be undone.')).toBeInTheDocument();
  });

  it('shows Cancel and Confirm buttons when open', async () => {
    renderDialog();
    openDialog();
    expect(
      await screen.findByRole('button', { name: t('common.cancel') }),
    ).toBeInTheDocument();
    expect(screen.getByRole('button', { name: t('common.confirm') })).toBeInTheDocument();
  });

  it('closes the dialog when Cancel is clicked', async () => {
    renderDialog();
    openDialog();
    fireEvent.click(await screen.findByRole('button', { name: t('common.cancel') }));
    await waitFor(() =>
      expect(screen.queryByText('Delete this item?')).not.toBeInTheDocument(),
    );
  });

  it('calls onConfirm when Confirm is clicked', async () => {
    defaultProps.onConfirm.mockResolvedValue(undefined);
    renderDialog();
    openDialog();
    fireEvent.click(await screen.findByRole('button', { name: t('common.confirm') }));
    await waitFor(() => expect(defaultProps.onConfirm).toHaveBeenCalledTimes(1));
  });

  it('closes the dialog after onConfirm resolves', async () => {
    defaultProps.onConfirm.mockResolvedValue(undefined);
    renderDialog();
    openDialog();
    fireEvent.click(await screen.findByRole('button', { name: t('common.confirm') }));
    await waitFor(() =>
      expect(screen.queryByText('Delete this item?')).not.toBeInTheDocument(),
    );
  });

  it('shows Processing... and disables actions while onConfirm is in flight', async () => {
    let resolveConfirm!: () => void;
    defaultProps.onConfirm.mockReturnValue(
      new Promise<void>((res) => {
        resolveConfirm = res;
      }),
    );
    renderDialog();
    openDialog();
    fireEvent.click(await screen.findByRole('button', { name: t('common.confirm') }));

    await waitFor(() =>
      expect(screen.getByRole('button', { name: t('common.processing') })).toBeInTheDocument(),
    );
    expect(screen.getByRole('button', { name: t('common.cancel') })).toBeDisabled();
    expect(screen.getByRole('button', { name: t('common.processing') })).toBeDisabled();

    resolveConfirm();
  });

  it('closes the dialog on Escape', async () => {
    renderDialog();
    openDialog();
    const dialog = await screen.findByRole('dialog');
    fireEvent.keyDown(dialog, { key: 'Escape', code: 'Escape' });
    await waitFor(() =>
      expect(screen.queryByText('Delete this item?')).not.toBeInTheDocument(),
    );
  });
});
