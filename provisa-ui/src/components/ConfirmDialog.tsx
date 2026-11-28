// Copyright (c) 2026 Kenneth Stott
// Canary: d59e130f-265e-482d-b41e-f689fc8b6c56
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { useState } from "react";
import type { ReactNode } from "react";
import { Button, Text } from "@mantine/core";
import { ModalLayout } from "./modal/ModalLayout";
import { useTranslation } from "react-i18next";
import { DestructiveConfirmButton } from "./form/DestructiveConfirmButton";
import { FormFooter } from "./form/FormLayout";

interface Props {
  title: string;
  consequence: string;
  onConfirm: () => void | Promise<void>;
  /** The control that opens the dialog. Left out by an owner that opens it itself (`opened`). */
  children?: (open: () => void) => ReactNode;
  /** Opened by its owner: true shows the dialog, and `onCancel` is told it was dismissed. */
  opened?: boolean;
  onCancel?: () => void;
  /** The action's own word, where "Confirm" does not say what it does. */
  confirmLabel?: string;
  /** False: the non-destructive form, for an action that removes nothing (REQ-413, Accept all).
   *  Its confirm is the filled primary button, not the red destructive one. */
  destructive?: boolean;
  testId?: string;
}

/** Confirmation dialog with consequence summary (REQ-061): destructive by default, or its
 *  non-destructive form (`destructive={false}`, REQ-413).
 *  Backed by Mantine Modal for focus-trap, `role="dialog"`, aria-modal, ESC /
 *  overlay dismissal, and focus restoration on close (REQ-1013). */
export function ConfirmDialog({
  title,
  consequence,
  onConfirm,
  children,
  opened,
  onCancel,
  confirmLabel,
  destructive = true,
  testId,
}: Props) {
  const { t } = useTranslation();
  const [own, setOwn] = useState(false);
  const [loading, setLoading] = useState(false);
  const open = opened ?? own;
  const setOpen = (next: boolean) => {
    setOwn(next);
    if (!next) onCancel?.();
  };

  const handleConfirm = async () => {
    setLoading(true);
    try {
      await onConfirm();
    } finally {
      setLoading(false);
      setOwn(false);
    }
  };

  const label = loading ? t("common.processing") : (confirmLabel ?? t("common.confirm"));
  const confirmTestId = testId ? `${testId}-confirm` : undefined;

  return (
    <>
      {children?.(() => setOwn(true))}
      <ModalLayout
        opened={open}
        onClose={() => setOpen(false)}
        title={title}
        centered
        closeOnClickOutside={!loading}
        closeOnEscape={!loading}
        withCloseButton={false}
        data-testid={testId}
      >
        <Text mb="md">{consequence}</Text>
        {/* The action first, then the shared Cancel, at the start of the row like every form. */}
        <FormFooter
          onCancel={() => setOpen(false)}
          cancelProps={{ disabled: loading }}
          action={
            destructive ? (
              <DestructiveConfirmButton
                onClick={handleConfirm}
                loading={loading}
                testId={confirmTestId}
              >
                {label}
              </DestructiveConfirmButton>
            ) : (
              <Button
                variant="filled"
                size="sm"
                type="button"
                onClick={handleConfirm}
                loading={loading}
                data-testid={confirmTestId}
                data-tone="confirm"
                // REQ-2115: it changes the model, so Polly asks the user first.
                data-changes="true"
              >
                {label}
              </Button>
            )
          }
        />
      </ModalLayout>
    </>
  );
}
