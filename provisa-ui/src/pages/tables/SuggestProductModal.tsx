// Copyright (c) 2026 Kenneth Stott
// Canary: 2a5d8e19-6f43-4b70-9c12-8e3b1f7a4d05
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2180 (amended 2026-10-28, SUGGEST A PRODUCT FROM A LISTED TABLE): a listed table is not
// requested directly; its reader suggests a data product of it, or of several listed tables of
// the same domain, stating why. The suggestion goes to the domain's data product owners and data
// stewards, who are emailed.

import { useState } from "react";
import { useTranslation } from "react-i18next";
import { Button, Modal, MultiSelect, Text, Textarea } from "@mantine/core";
import { FormField, FormFooter, FormLayout } from "../../components/form/FormLayout";
import { suggestProduct, type Suggestion } from "../../api/productSuggestions";

export interface SuggestTarget {
  domainId: string;
  /** The table the reader chose the action on. */
  tableName: string;
  /** Every table of the domain listed to the reader; a suggestion names any of them. */
  domainTables: string[];
}

function message(e: unknown): string {
  return e instanceof Error ? e.message : String(e);
}

export function SuggestProductModal({
  target,
  onClose,
}: {
  target: SuggestTarget | null;
  onClose: () => void;
}) {
  const { t } = useTranslation();
  return (
    <Modal
      opened={target !== null}
      onClose={onClose}
      title={t("suggestProduct.title", { domain: target?.domainId ?? "" })}
      data-testid="suggest-product-modal"
    >
      {target && (
        <SuggestForm
          key={`${target.domainId}.${target.tableName}`}
          target={target}
          onClose={onClose}
        />
      )}
    </Modal>
  );
}

/** The suggestion's form, fresh for each table the action is chosen on. */
function SuggestForm({ target, onClose }: { target: SuggestTarget; onClose: () => void }) {
  const { t } = useTranslation();
  const [tables, setTables] = useState<string[]>([target.tableName]);
  const [reason, setReason] = useState("");
  const [sending, setSending] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [sent, setSent] = useState<Suggestion | null>(null);
  const ready = tables.length > 0 && reason.trim() !== "" && !sending;
  const submit = () => {
    setSending(true);
    setError(null);
    suggestProduct({ domain_id: target.domainId, tables, reason: reason.trim() })
      .then(setSent, (e: unknown) => setError(message(e)))
      .finally(() => setSending(false));
  };
  if (sent) {
    return (
      <FormLayout>
        <FormField size="full">
          <Text size="sm" data-testid="suggest-product-sent">
            {t("suggestProduct.sent", { count: sent.recipients.length, domain: sent.domain_id })}
          </Text>
        </FormField>
        <FormFooter
          action={
            <Button onClick={onClose} data-testid="suggest-product-close">
              {t("suggestProduct.close")}
            </Button>
          }
        />
      </FormLayout>
    );
  }
  return (
    <FormLayout>
      <FormField size="long" hint={t("suggestProduct.hint")}>
        <MultiSelect
          label={t("suggestProduct.tables")}
          required
          data={target.domainTables}
          value={tables}
          onChange={setTables}
          searchable
          data-testid="suggest-product-tables"
        />
      </FormField>
      <FormField size="full" hint={t("suggestProduct.reasonHint")}>
        <Textarea
          label={t("suggestProduct.reason")}
          required
          autosize
          minRows={2}
          value={reason}
          onChange={(e) => setReason(e.currentTarget.value)}
          data-testid="suggest-product-reason"
        />
      </FormField>
      <FormFooter
        onCancel={onClose}
        problem={error}
        action={
          <Button onClick={submit} disabled={!ready} data-testid="suggest-product-send">
            {sending ? t("suggestProduct.sending") : t("suggestProduct.send")}
          </Button>
        }
      />
    </FormLayout>
  );
}
