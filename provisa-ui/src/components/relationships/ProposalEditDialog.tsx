// Copyright (c) 2026 Kenneth Stott
// Canary: 3f8a1d62-7c4e-4b95-a0d3-9e61b2c85f47
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2262: a candidate's edit dialog in the relationship review. Before accepting it a person
// sets the name its relationship takes and, where the proposal is a plain one, its cardinality.
// A proposal accepted through a junction or a shared dimension has its cardinality fixed by what
// accepting it makes, so the dialog offers its name only.

import { useState } from "react";
import { useTranslation } from "react-i18next";
import { Select, TextInput } from "@mantine/core";
import { ModalLayout } from "../modal/ModalLayout";
import { FormField, FormFooter, FormLayout } from "../form/FormLayout";
import type { RowEdit } from "./useRunProposals";

const CARDINALITIES = ["many-to-one", "one-to-many", "one-to-one"] as const;

interface ProposalEditDialogProps {
  /** The name accepting the row gives its relationship when left as it is. */
  name: string;
  /** The row's measured cardinality, where it can be changed; null where accepting fixes it. */
  cardinality: string | null;
  /** What was already edited for the row. */
  edit: RowEdit | undefined;
  onSave: (edit: RowEdit | null) => void;
  onClose: () => void;
}

export function ProposalEditDialog({
  name,
  cardinality,
  edit,
  onSave,
  onClose,
}: ProposalEditDialogProps) {
  const { t } = useTranslation();
  const [nameValue, setNameValue] = useState(edit?.name ?? name);
  const [cardinalityValue, setCardinalityValue] = useState(edit?.cardinality ?? cardinality);
  const trimmed = nameValue.trim();

  const save = () => {
    const next: RowEdit = {};
    if (trimmed !== name) next.name = trimmed;
    if (cardinality !== null && cardinalityValue !== null && cardinalityValue !== cardinality) {
      next.cardinality = cardinalityValue;
    }
    onSave(Object.keys(next).length === 0 ? null : next);
    onClose();
  };

  return (
    <ModalLayout opened onClose={onClose} title={t("proposalEdit.title")} centered>
      <FormLayout>
        <FormField size="medium">
          <TextInput
            label={t("proposalEdit.name")}
            data-testid="proposal-edit-name"
            value={nameValue}
            onChange={(e) => setNameValue(e.currentTarget.value)}
          />
        </FormField>
        {cardinality !== null && (
          <FormField size="short">
            <Select
              label={t("proposalEdit.cardinality")}
              data-testid="proposal-edit-cardinality"
              data={CARDINALITIES.map((c) => ({ value: c, label: c }))}
              value={cardinalityValue}
              onChange={setCardinalityValue}
              allowDeselect={false}
            />
          </FormField>
        )}
        <FormFooter
          onSave={save}
          onCancel={onClose}
          saveProps={{ disabled: trimmed === "", "data-testid": "proposal-edit-save" }}
          cancelProps={{ "data-testid": "proposal-edit-cancel" }}
        />
      </FormLayout>
    </ModalLayout>
  );
}
