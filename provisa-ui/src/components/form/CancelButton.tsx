// Copyright (c) 2026 Kenneth Stott
// Canary: 26fe12d2-0c10-4594-9350-6db758752a82
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// THE Cancel: one button, one look, wherever a form, a dialog, a panel's head or a row offers to
// leave what it was doing. Bordered (Mantine's `default` variant), the cross, the shared word,
// Mantine's standard size. It takes nothing that changes its look, so no page can draw a Cancel
// of its own (cancelGuard.test.ts refuses a Button labelled Cancel anywhere else).

import { Button } from "@mantine/core";
import { X } from "lucide-react";
import { useTranslation } from "react-i18next";

export function CancelButton({
  onClick,
  disabled = false,
  head = false,
  testId,
}: {
  onClick: () => void;
  disabled?: boolean;
  /** Drawn in a detail panel's head: the head's style (no border, no fill), not the foot's. */
  head?: boolean;
  testId?: string;
}) {
  const { t } = useTranslation();
  return (
    <Button
      // Set here, not left to a theme, for the foot and a dialog. In a head nothing is set: the
      // head's theme (PanelHeadStyle) gives the one head style.
      {...(head ? {} : { variant: "default", size: "sm" } as const)}
      type="button"
      onClick={onClick}
      disabled={disabled}
      data-testid={testId}
      // REQ-2115: leaving a form changes nothing; Polly presses it without asking.
      data-changes="false"
      leftSection={<X size={14} />}
    >
      {t("common.cancel")}
    </Button>
  );
}
