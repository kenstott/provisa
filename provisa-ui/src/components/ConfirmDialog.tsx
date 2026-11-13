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
import { Modal, Text } from "@mantine/core";
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
  testId?: string;
}

/** Confirmation dialog for destructive actions with consequence summary (REQ-061).
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

  return (
    <>
      {children?.(() => setOwn(true))}
      <Modal
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
            <DestructiveConfirmButton
              onClick={handleConfirm}
              loading={loading}
              testId={testId ? `${testId}-confirm` : undefined}
            >
              {loading ? t("common.processing") : (confirmLabel ?? t("common.confirm"))}
            </DestructiveConfirmButton>
          }
        />
      </Modal>
    </>
  );
}
