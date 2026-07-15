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
import { Button, Group, Modal, Text } from "@mantine/core";
import { useTranslation } from "react-i18next";

interface Props {
  title: string;
  consequence: string;
  onConfirm: () => void | Promise<void>;
  children: (open: () => void) => ReactNode;
}

/** Confirmation dialog for destructive actions with consequence summary (REQ-061).
 *  Backed by Mantine Modal for focus-trap, `role="dialog"`, aria-modal, ESC /
 *  overlay dismissal, and focus restoration on close (REQ-1013). */
export function ConfirmDialog({ title, consequence, onConfirm, children }: Props) {
  const { t } = useTranslation();
  const [open, setOpen] = useState(false);
  const [loading, setLoading] = useState(false);

  const handleConfirm = async () => {
    setLoading(true);
    try {
      await onConfirm();
    } finally {
      setLoading(false);
      setOpen(false);
    }
  };

  return (
    <>
      {children(() => setOpen(true))}
      <Modal
        opened={open}
        onClose={() => setOpen(false)}
        title={title}
        centered
        closeOnClickOutside={!loading}
        closeOnEscape={!loading}
        withCloseButton={false}
      >
        <Text mb="md">{consequence}</Text>
        <Group justify="flex-end">
          <Button variant="default" onClick={() => setOpen(false)} disabled={loading}>
            {t("common.cancel")}
          </Button>
          <Button color="red" onClick={handleConfirm} loading={loading}>
            {loading ? t("common.processing") : t("common.confirm")}
          </Button>
        </Group>
      </Modal>
    </>
  );
}
