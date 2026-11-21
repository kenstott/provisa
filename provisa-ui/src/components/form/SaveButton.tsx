// Copyright (c) 2026 Kenneth Stott
// Canary: 8b1d5c3e-2f4a-4e67-9a0c-5d7e3b1f6a42
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// THE Save: one button. At a form's foot and in a dialog it is filled primary (Mantine's `filled`
// variant), the check, the shared word, Mantine's standard size -- the same size as the Cancel
// beside it (CancelButton). In a detail panel's head (`head`) it has no border and no fill, as the
// head's other actions are (REQ-1968): it takes the head's one style (PanelHeadStyle). A busy Save
// shows a spinner in place of its check. It takes nothing else that changes its look.

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
  head = false,
  testId,
}: {
  onClick?: MouseEventHandler<HTMLButtonElement>;
  submit?: boolean;
  /** The id of the <form> a submit Save belongs to, when it sits outside that form's element. */
  form?: string;
  busy?: boolean;
  disabled?: boolean;
  /** Drawn in a detail panel's head: the head's style, not the foot's. */
  head?: boolean;
  testId?: string;
}) {
  const { t } = useTranslation();
  return (
    <Button
      // Set here, not left to a theme, for the foot and a dialog. In a head nothing is set: the
      // head's theme (PanelHeadStyle) gives the one head style.
      {...(head ? {} : { variant: "filled", size: "sm" } as const)}
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
