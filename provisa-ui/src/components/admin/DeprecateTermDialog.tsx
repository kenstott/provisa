// Copyright (c) 2026 Kenneth Stott
// Canary: 4d8b1e57-a2c3-4f69-9e0d-73b5c2a1f846
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// Deprecate a live term (REQ-2144): a reason, and an optional planned retirement, today or later.
// The term stays in service until then, when the scheduler retires it.

import { useState } from "react";
import { useTranslation } from "react-i18next";
import { Alert, Button, Modal, Textarea } from "@mantine/core";
import { DateInput } from "@mantine/dates";
import { deprecateGlossaryTerm, type GlossaryTermDetail } from "../../api/glossary";
import { FormField, FormFooter, FormLayout } from "../form/FormLayout";

/** Today in the reader's calendar, as the date input takes it: the earliest planned retirement. */
function today(): string {
  const d = new Date();
  const two = (n: number) => String(n).padStart(2, "0");
  return `${d.getFullYear()}-${two(d.getMonth() + 1)}-${two(d.getDate())}`;
}

/** Drawn while it is open: each opening starts from an empty form. */
export function DeprecateTermDialog({
  term,
  onClose,
  onDeprecated,
}: {
  term: GlossaryTermDetail;
  onClose: () => void;
  onDeprecated: () => void;
}) {
  const { t } = useTranslation();
  const [reason, setReason] = useState("");
  const [retireOn, setRetireOn] = useState<string | null>(null);
  const [problem, setProblem] = useState("");
  const [saving, setSaving] = useState(false);

  const deprecate = async () => {
    setSaving(true);
    setProblem("");
    try {
      await deprecateGlossaryTerm(term.id, { reason: reason.trim(), retire_on: retireOn });
      onDeprecated();
    } catch (e) {
      setProblem((e as Error).message);
    } finally {
      setSaving(false);
    }
  };

  return (
    <Modal opened onClose={onClose} title={t("glossaryTab.deprecateTitle", { term: term.name })}>
      <FormLayout>
        <FormField size="full">
          <Textarea
            label={t("glossaryTab.retireReason")}
            value={reason}
            onChange={(e) => setReason(e.currentTarget.value)}
            autosize
            minRows={2}
            required
            data-testid="glossary-deprecate-reason"
          />
        </FormField>
        <FormField size="short" help={t("glossaryTab.retireOnHint")}>
          <DateInput
            label={t("glossaryTab.retireOnLabel")}
            valueFormat="YYYY-MM-DD"
            clearable
            minDate={today()}
            value={retireOn}
            onChange={setRetireOn}
            data-testid="glossary-deprecate-retire-on"
          />
        </FormField>
        {problem && (
          <FormField size="full">
            <Alert color="red" data-testid="glossary-deprecate-error">
              {problem}
            </Alert>
          </FormField>
        )}
        <FormFooter
          onCancel={onClose}
          action={
            <Button
              onClick={() => void deprecate()}
              loading={saving}
              disabled={!reason.trim()}
              data-testid="glossary-deprecate-submit"
            >
              {t("glossaryTab.deprecate")}
            </Button>
          }
        />
      </FormLayout>
    </Modal>
  );
}
