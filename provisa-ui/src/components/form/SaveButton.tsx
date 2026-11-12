// Copyright (c) 2026 Kenneth Stott
// Canary: 8b1d5c3e-2f4a-4e67-9a0c-5d7e3b1f6a42
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// THE Save: one button, one look, wherever a form's foot, a panel's head or a dialog offers to
// keep what was entered. Filled primary (Mantine's `filled` variant), the check, the shared word,
// Mantine's standard size -- the same size as the Cancel beside it (CancelButton). A busy Save
// shows a spinner in place of its check. It takes nothing that changes its look.

import type { MouseEventHandler } from "react";
import { Button, Loader } from "@mantine/core";
import { Check } from "lucide-react";
import { useTranslation } from "react-i18next";

export function SaveButton({
  onClick,
  submit = false,
  form,
  busy = false,
  disabled = false,
  testId,
}: {
  onClick?: MouseEventHandler<HTMLButtonElement>;
  submit?: boolean;
  /** The id of the <form> a submit Save belongs to, when it sits outside that form's element. */
  form?: string;
  busy?: boolean;
  disabled?: boolean;
  testId?: string;
}) {
  const { t } = useTranslation();
  return (
    <Button
      // Set here, not left to a theme: a panel's head makes its actions borderless and compact
      // by default (PanelHeadStyle), and Save is the same there as anywhere.
      variant="filled"
      size="sm"
      form={form}
      type={submit ? "submit" : "button"}
      onClick={submit ? undefined : onClick}
      leftSection={busy ? <Loader size={14} color="currentColor" /> : <Check size={14} />}
      disabled={busy || disabled}
      data-saving={busy || undefined}
      data-testid={testId}
      // REQ-2115: what Polly presses here changes something; she asks the user first.
      data-changes="true"
    >
      {t("common.save")}
    </Button>
  );
}
