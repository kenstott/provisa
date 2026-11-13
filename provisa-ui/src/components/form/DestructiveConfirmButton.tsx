// Copyright (c) 2026 Kenneth Stott
// Canary: 7e4b1c93-2a58-4d6f-8c10-3b9a5e7d2f46
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// THE destructive confirm: the button that confirms a delete, a removal or an end, in a dialog or
// at a form's foot. Filled red (Mantine's `filled` variant, red), a white label, Mantine's
// standard size -- the same size as the Cancel beside it (CancelButton). It carries the action's
// own word as its label (Delete, Remove, End ...), or "Confirm" where the dialog knows no better.
// It takes nothing that changes its look, so no page can draw a destructive confirm of its own
// (destructiveConfirmGuard.test.ts refuses a filled red Button anywhere else). It is for dialogs
// and form feet only: a panel's head has no such button (the head's actions have no fill).

import type { ReactNode } from "react";
import { Button } from "@mantine/core";

export function DestructiveConfirmButton({
  onClick,
  loading = false,
  disabled = false,
  testId,
  children,
}: {
  onClick: () => void;
  /** The action is in flight: Mantine's spinner in place of the label's start. */
  loading?: boolean;
  disabled?: boolean;
  testId?: string;
  /** The action's word. */
  children: ReactNode;
}) {
  return (
    <Button
      variant="filled"
      color="red"
      size="sm"
      type="button"
      onClick={onClick}
      loading={loading}
      disabled={disabled}
      data-testid={testId}
      // REQ-2115: what Polly presses here destroys something; she asks the user first.
      data-changes="true"
    >
      {children}
    </Button>
  );
}
