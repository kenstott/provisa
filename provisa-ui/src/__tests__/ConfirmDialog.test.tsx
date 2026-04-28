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

import { describe, it, expect, vi, beforeEach } from 'vitest'
import { render, screen, fireEvent, waitFor } from '@testing-library/react'
import { ConfirmDialog } from '../components/ConfirmDialog'

describe('ConfirmDialog', () => {
  const defaultProps = {
    title: 'Delete this item?',
    consequence: 'This action cannot be undone.',
    onConfirm: vi.fn(),
  }

  beforeEach(() => {
    defaultProps.onConfirm.mockReset()
  })

  it('renders only the trigger element when closed', () => {
    render(
      <ConfirmDialog {...defaultProps}>
        {(open) => <button onClick={open}>Open Dialog</button>}
      </ConfirmDialog>
    )

    expect(screen.getByRole('button', { name: 'Open Dialog' })).toBeInTheDocument()
    expect(screen.queryByText('Delete this item?')).not.toBeInTheDocument()
    expect(screen.queryByText('This action cannot be undone.')).not.toBeInTheDocument()
  })

  it('shows modal with title and consequence text when trigger is clicked', () => {
    render(
      <ConfirmDialog {...defaultProps}>
        {(open) => <button onClick={open}>Open Dialog</button>}
      </ConfirmDialog>
    )

    fireEvent.click(screen.getByRole('button', { name: 'Open Dialog' }))

    expect(screen.getByText('Delete this item?')).toBeInTheDocument()
    expect(screen.getByText('This action cannot be undone.')).toBeInTheDocument()
  })

  it('shows Cancel and Confirm buttons when open', () => {
    render(
      <ConfirmDialog {...defaultProps}>
        {(open) => <button onClick={open}>Open Dialog</button>}
      </ConfirmDialog>
    )

    fireEvent.click(screen.getByRole('button', { name: 'Open Dialog' }))

    expect(screen.getByRole('button', { name: 'Cancel' })).toBeInTheDocument()
    expect(screen.getByRole('button', { name: 'Confirm' })).toBeInTheDocument()
  })

  it('closes modal when Cancel is clicked', () => {
    render(
      <ConfirmDialog {...defaultProps}>
        {(open) => <button onClick={open}>Open Dialog</button>}
      </ConfirmDialog>
    )

    fireEvent.click(screen.getByRole('button', { name: 'Open Dialog' }))
    expect(screen.getByText('Delete this item?')).toBeInTheDocument()

    fireEvent.click(screen.getByRole('button', { name: 'Cancel' }))

    expect(screen.queryByText('Delete this item?')).not.toBeInTheDocument()
  })

  it('calls onConfirm when Confirm is clicked', async () => {
    defaultProps.onConfirm.mockResolvedValue(undefined)

    render(
      <ConfirmDialog {...defaultProps}>
        {(open) => <button onClick={open}>Open Dialog</button>}
      </ConfirmDialog>
    )

    fireEvent.click(screen.getByRole('button', { name: 'Open Dialog' }))
    fireEvent.click(screen.getByRole('button', { name: 'Confirm' }))

    await waitFor(() => {
      expect(defaultProps.onConfirm).toHaveBeenCalledTimes(1)
    })
  })

  it('closes modal after onConfirm resolves', async () => {
    defaultProps.onConfirm.mockResolvedValue(undefined)

    render(
      <ConfirmDialog {...defaultProps}>
        {(open) => <button onClick={open}>Open Dialog</button>}
      </ConfirmDialog>
    )

    fireEvent.click(screen.getByRole('button', { name: 'Open Dialog' }))
    fireEvent.click(screen.getByRole('button', { name: 'Confirm' }))

    await waitFor(() => {
      expect(screen.queryByText('Delete this item?')).not.toBeInTheDocument()
    })
  })

  it('shows Processing... and disables buttons while onConfirm is in flight', async () => {
    let resolveConfirm!: () => void
    const pendingConfirm = new Promise<void>((res) => { resolveConfirm = res })
    defaultProps.onConfirm.mockReturnValue(pendingConfirm)

    render(
      <ConfirmDialog {...defaultProps}>
        {(open) => <button onClick={open}>Open Dialog</button>}
      </ConfirmDialog>
    )

    fireEvent.click(screen.getByRole('button', { name: 'Open Dialog' }))
    fireEvent.click(screen.getByRole('button', { name: 'Confirm' }))

    await waitFor(() => {
      expect(screen.getByRole('button', { name: 'Processing...' })).toBeInTheDocument()
    })

    const cancelBtn = screen.getByRole('button', { name: 'Cancel' })
    expect(cancelBtn).toBeDisabled()
    expect(screen.getByRole('button', { name: 'Processing...' })).toBeDisabled()

    resolveConfirm()
  })

  it('closes modal when overlay background is clicked', () => {
    render(
      <ConfirmDialog {...defaultProps}>
        {(open) => <button onClick={open}>Open Dialog</button>}
      </ConfirmDialog>
    )

    fireEvent.click(screen.getByRole('button', { name: 'Open Dialog' }))
    expect(screen.getByText('Delete this item?')).toBeInTheDocument()

    // Click the overlay (modal-overlay div)
    const overlay = document.querySelector('.modal-overlay') as HTMLElement
    fireEvent.click(overlay)

    expect(screen.queryByText('Delete this item?')).not.toBeInTheDocument()
  })

  it('does not close modal when clicking inside the modal card', () => {
    render(
      <ConfirmDialog {...defaultProps}>
        {(open) => <button onClick={open}>Open Dialog</button>}
      </ConfirmDialog>
    )

    fireEvent.click(screen.getByRole('button', { name: 'Open Dialog' }))

    const modalCard = document.querySelector('.modal') as HTMLElement
    fireEvent.click(modalCard)

    // Title should still be visible — stopPropagation prevents overlay close
    expect(screen.getByText('Delete this item?')).toBeInTheDocument()
  })
})
