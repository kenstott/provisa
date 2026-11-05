// Copyright (c) 2026 Kenneth Stott
// Canary: 4f1a8c26-9e3b-4d70-a5c1-7b2e0d9f63a8
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// Deprecate a live object -- a term (REQ-2144), a data product: a reason, and an optional planned
// retirement, today or later. It stays in service until then, when the scheduler retires it.

import { useState } from "react";
import { Alert, Button, Modal, Textarea } from "@mantine/core";
import { DateInput } from "@mantine/dates";
import { FormField, FormFooter, FormLayout } from "../form/FormLayout";

/** Today in the reader's calendar, as the date input takes it: the earliest planned retirement. */
function today(): string {
  const d = new Date();
  const two = (n: number) => String(n).padStart(2, "0");
  return `${d.getFullYear()}-${two(d.getMonth() + 1)}-${two(d.getDate())}`;
}

export interface DeprecateDialogText {
  title: string;
  reason: string;
  retireOn: string;
  retireOnHint: string;
  submit: string;
}

/** Drawn while it is open: each opening starts from an empty form. */
export function DeprecateDialog({
  text,
  deprecate,
  onClose,
  onDeprecated,
  testId,
}: {
  text: DeprecateDialogText;
  /** Deprecates the object; a refusal throws, its message shown. */
  deprecate: (reason: string, retireOn: string | null) => Promise<unknown>;
  onClose: () => void;
  onDeprecated: () => void;
  /** Each field's is ``<testId>-deprecate-<field>``. */
  testId: string;
}) {
  const [reason, setReason] = useState("");
  const [retireOn, setRetireOn] = useState<string | null>(null);
  const [problem, setProblem] = useState("");
  const [saving, setSaving] = useState(false);

  const submit = async () => {
    setSaving(true);
    setProblem("");
    try {
      await deprecate(reason.trim(), retireOn);
      onDeprecated();
    } catch (e) {
      setProblem((e as Error).message);
    } finally {
      setSaving(false);
    }
  };

  return (
    <Modal opened onClose={onClose} title={text.title}>
      <FormLayout>
        <FormField size="full">
          <Textarea
            label={text.reason}
            value={reason}
            onChange={(e) => setReason(e.currentTarget.value)}
            autosize
            minRows={2}
            required
            data-testid={`${testId}-deprecate-reason`}
          />
        </FormField>
        <FormField size="short" help={text.retireOnHint}>
          <DateInput
            label={text.retireOn}
            valueFormat="YYYY-MM-DD"
            clearable
            minDate={today()}
            value={retireOn}
            onChange={setRetireOn}
            data-testid={`${testId}-deprecate-retire-on`}
          />
        </FormField>
        {problem && (
          <FormField size="full">
            <Alert color="red" data-testid={`${testId}-deprecate-error`}>
              {problem}
            </Alert>
          </FormField>
        )}
        <FormFooter
          onCancel={onClose}
          action={
            <Button
              onClick={() => void submit()}
              loading={saving}
              disabled={!reason.trim()}
              data-testid={`${testId}-deprecate-submit`}
            >
              {text.submit}
            </Button>
          }
        />
      </FormLayout>
    </Modal>
  );
}
