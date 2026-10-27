// Copyright (c) 2026 Kenneth Stott
// Canary: 9c5d2e17-3b84-4a6f-8e01-c4a7b3d5f926
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// Retire an object -- a term (REQ-2144), a data product: a reason, and the successor what it holds
// moves to, required while it holds anything. A refusal the object's page can resolve (a term's
// tag loss) takes the dialog's place until the reader resolves it or goes back.

import { useState, type ReactNode } from "react";
import { Alert, Button, Modal, Select, Textarea } from "@mantine/core";
import { FormField, FormFooter, FormLayout } from "../form/FormLayout";

export interface RetireDialogText {
  title: string;
  reason: string;
  successor: string;
  /** The successor's help: what moves to it, or that nothing does. */
  successorHint: string;
  submit: string;
}

/** What a refusal the page resolves shows in the dialog's place. */
export interface RetireRefusal {
  title: string;
  body: ReactNode;
}

/** Drawn while it is open: each opening starts from an empty form. */
export function RetireDialog<R = undefined>({
  text,
  successorOptions,
  successorRequired,
  moves,
  retire,
  refusal,
  onClose,
  onRetired,
  testId,
}: {
  text: RetireDialogText;
  successorOptions: { value: string; label: string }[];
  /** It holds something, which only a successor takes. */
  successorRequired: boolean;
  /** What moves to the successor, said under the picker. */
  moves?: ReactNode;
  /** Retires the object, with ``resolve`` when a refusal was resolved; a refusal throws. */
  retire: (reason: string, successor: string | null, resolve?: R) => Promise<unknown>;
  /** A refusal the page resolves: what to show for it, given how to try again and how to go back;
   * null for one it does not. */
  refusal?: (
    error: unknown,
    successor: string | null,
    retry: (resolve: R) => void,
    back: () => void,
    saving: boolean,
  ) => RetireRefusal | null;
  onClose: () => void;
  onRetired: () => void;
  /** Each field's is ``<testId>-retire-<field>``. */
  testId: string;
}) {
  const [reason, setReason] = useState("");
  const [successor, setSuccessor] = useState<string | null>(null);
  const [refused, setRefused] = useState<unknown>(null);
  const [problem, setProblem] = useState("");
  const [saving, setSaving] = useState(false);

  const submit = async (resolve?: R) => {
    setSaving(true);
    setProblem("");
    try {
      await retire(reason.trim(), successor, resolve);
      onRetired();
    } catch (e) {
      if (
        refusal?.(
          e,
          successor,
          () => {},
          () => {},
          false,
        )
      )
        setRefused(e);
      else setProblem((e as Error).message);
    } finally {
      setSaving(false);
    }
  };

  const shown =
    refused === null
      ? null
      : (refusal?.(
          refused,
          successor,
          (resolve) => void submit(resolve),
          () => setRefused(null),
          saving,
        ) ?? null);

  return (
    <Modal opened onClose={onClose} title={shown ? shown.title : text.title} size="lg">
      {shown ? (
        shown.body
      ) : (
        <FormLayout>
          <FormField size="full">
            <Textarea
              label={text.reason}
              value={reason}
              onChange={(e) => setReason(e.currentTarget.value)}
              autosize
              minRows={2}
              required
              data-testid={`${testId}-retire-reason`}
            />
          </FormField>
          <FormField size="long" help={text.successorHint}>
            <Select
              label={text.successor}
              data={successorOptions}
              value={successor}
              onChange={setSuccessor}
              searchable
              clearable
              required={successorRequired}
              data-testid={`${testId}-retire-successor`}
            />
          </FormField>
          {moves && (
            <FormField size="full">
              <div data-testid={`${testId}-retire-moves`}>{moves}</div>
            </FormField>
          )}
          {problem && (
            <FormField size="full">
              <Alert color="red" data-testid={`${testId}-retire-error`}>
                {problem}
              </Alert>
            </FormField>
          )}
          <FormFooter
            onCancel={onClose}
            action={
              <Button
                color="orange"
                onClick={() => void submit()}
                loading={saving}
                disabled={!reason.trim() || (successorRequired && successor === null)}
                data-testid={`${testId}-retire-submit`}
              >
                {text.submit}
              </Button>
            }
          />
        </FormLayout>
      )}
    </Modal>
  );
}
