// Copyright (c) 2026 Kenneth Stott
// Canary: 2b9e4d17-83c5-4f60-b1a7-5d0c6e93f4a8
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// THE Stop: the button that halts an operation already running (a scan, a query, a job). It says
// "Stop". "Cancel" is kept for leaving a form or dialog with nothing changed (CancelButton), and
// this is never that. Its tip says what it stops. Where a toolbar is one icon wide (GraphiQL's),
// it is the icon alone (``iconOnly``), named by its tip and its accessible name.

import { ActionIcon, Button } from "@mantine/core";
import { Square } from "lucide-react";
import { useTranslation } from "react-i18next";
import { Tip } from "./Tip";

export function StopButton({
  onClick,
  tip,
  disabled = false,
  testId,
  iconOnly = false,
}: {
  onClick: () => void;
  /** What stopping does, e.g. "Stop the scan". */
  tip: string;
  disabled?: boolean;
  testId?: string;
  /** The square alone, for a toolbar one icon wide. */
  iconOnly?: boolean;
}) {
  const { t } = useTranslation();
  if (iconOnly) {
    return (
      <Tip text={tip}>
        <ActionIcon
          variant="subtle"
          size="lg"
          type="button"
          onClick={onClick}
          disabled={disabled}
          data-testid={testId}
          aria-label={t("common.stop")}
        >
          <Square size={16} />
        </ActionIcon>
      </Tip>
    );
  }
  return (
    <Tip text={tip}>
      <Button
        variant="default"
        size="sm"
        type="button"
        onClick={onClick}
        disabled={disabled}
        data-testid={testId}
        leftSection={<Square size={14} />}
      >
        {t("common.stop")}
      </Button>
    </Tip>
  );
}
