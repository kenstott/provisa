// Copyright (c) 2026 Kenneth Stott
// Canary: 6d3f0b82-a4e1-49c7-8e25-b17c9a4d0f63
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2237: the steward's own changes to an imported item before it is applied: its name and
// definition, and an alignment of their own with a glossary term or another item of the import,
// added approved (adding it is the steward's decision).

import { useState } from "react";
import { useTranslation } from "react-i18next";
import { Alert, Button, Modal, SegmentedControl, Select, TextInput, Textarea } from "@mantine/core";
import { Plus } from "lucide-react";
import {
  ALIGNMENT_TYPES,
  addAlignment,
  editImportItem,
  type AlignmentType,
  type ImportItem,
  type ImportReview,
} from "../../../api/ontologyImport";
import type { GlossaryTermSummary } from "../../../api/glossary";
import { FormField, FormFooter, FormIdentifiers, FormLayout } from "../../form/FormLayout";

function failure(e: unknown): string {
  return e instanceof Error ? e.message : String(e);
}

export function EditItemDialog({
  item,
  onClose,
  onSaved,
}: {
  item: ImportItem;
  onClose: () => void;
  onSaved: (review: ImportReview) => void;
}) {
  const { t } = useTranslation();
  const [name, setName] = useState(item.name);
  const [definition, setDefinition] = useState(item.definition ?? "");
  const [error, setError] = useState("");
  const save = async () => {
    setError("");
    try {
      onSaved(
        await editImportItem(item.id, {
          ...(name.trim() !== item.name ? { name: name.trim() } : {}),
          definition: definition.trim() ? definition.trim() : null,
        }),
      );
    } catch (e) {
      setError(failure(e));
    }
  };
  return (
    <Modal
      opened
      onClose={onClose}
      title={t("ontologyImport.editTitle", { name: item.name })}
      size="lg"
      data-testid="ontology-item-edit-dialog"
    >
      <FormLayout>
        <FormIdentifiers>
          <FormField size="medium" help={t("ontologyImport.renameHelp")}>
            <TextInput
              label={t("ontologyImport.name")}
              value={name}
              onChange={(e) => setName(e.currentTarget.value)}
              required
              data-testid="ontology-item-name"
            />
          </FormField>
        </FormIdentifiers>
        <FormField size="full">
          <Textarea
            label={t("ontologyImport.definition")}
            value={definition}
            onChange={(e) => setDefinition(e.currentTarget.value)}
            autosize
            minRows={3}
            data-testid="ontology-item-definition"
          />
        </FormField>
        {error && (
          <FormField size="full">
            <Alert color="red" data-testid="ontology-item-edit-error">
              {error}
            </Alert>
          </FormField>
        )}
        <FormFooter
          onSave={save}
          onCancel={onClose}
          saveProps={{ "data-testid": "ontology-item-save", disabled: !name.trim() }}
        />
      </FormLayout>
    </Modal>
  );
}

type With = "term" | "item";

export function AddAlignmentDialog({
  importId,
  item,
  items,
  terms,
  onClose,
  onAdded,
}: {
  importId: number;
  item: ImportItem;
  /** The import's items: another of them may be the other end. */
  items: ImportItem[];
  /** The glossary terms the steward may read. */
  terms: GlossaryTermSummary[];
  onClose: () => void;
  onAdded: () => void;
}) {
  const { t } = useTranslation();
  const [withWhat, setWithWhat] = useState<With>("term");
  const [relType, setRelType] = useState<AlignmentType>("SAME_AS");
  const [target, setTarget] = useState<string | null>(null);
  const [itemIsSource, setItemIsSource] = useState(true);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState("");
  // An item is the same entity as a term, never as another item.
  const types = ALIGNMENT_TYPES.filter((rt) => withWhat === "term" || rt !== "SAME_AS");
  const targets =
    withWhat === "term"
      ? terms.map((term) => ({ value: String(term.id), label: term.name }))
      : items
          .filter((other) => other.id !== item.id)
          .map((other) => ({ value: String(other.id), label: other.name }));
  const targetName = targets.find((o) => o.value === target)?.label ?? "…";
  const phrase = (source: boolean) =>
    t(`ontologyImport.phrase_${relType}`, {
      from: source ? item.name : targetName,
      to: source ? targetName : item.name,
    });

  const add = async () => {
    if (target === null) return;
    setBusy(true);
    setError("");
    try {
      await addAlignment(importId, {
        item_id: item.id,
        rel_type: relType,
        item_is_source: relType === "SAME_AS" ? true : itemIsSource,
        ...(withWhat === "term" ? { term_id: Number(target) } : { other_item_id: Number(target) }),
      });
      onAdded();
    } catch (e) {
      setError(failure(e));
    } finally {
      setBusy(false);
    }
  };

  return (
    <Modal
      opened
      onClose={onClose}
      title={t("ontologyImport.addAlignmentTitle", { name: item.name })}
      size="lg"
      data-testid="ontology-add-alignment-dialog"
    >
      <FormLayout>
        <FormField size="full">
          <SegmentedControl
            value={withWhat}
            onChange={(v) => {
              setWithWhat(v as With);
              setTarget(null);
              if (v === "item" && relType === "SAME_AS") setRelType("SYNONYM_OF");
            }}
            data={[
              { value: "term", label: t("ontologyImport.withTerm") },
              { value: "item", label: t("ontologyImport.withItem") },
            ]}
            data-testid="ontology-add-alignment-with"
          />
        </FormField>
        <FormField size="medium">
          <Select
            label={t("ontologyImport.relType")}
            data={types.map((rt) => ({ value: rt, label: t(`ontologyImport.rel_${rt}`) }))}
            value={relType}
            onChange={(v) => v && setRelType(v as AlignmentType)}
            allowDeselect={false}
            comboboxProps={{ withinPortal: true }}
            data-testid="ontology-add-alignment-type"
          />
        </FormField>
        <FormField size="medium">
          <Select
            label={withWhat === "term" ? t("ontologyImport.term") : t("ontologyImport.otherItem")}
            data={targets}
            value={target}
            onChange={setTarget}
            searchable
            required
            comboboxProps={{ withinPortal: true }}
            data-testid="ontology-add-alignment-target"
          />
        </FormField>
        {relType !== "SAME_AS" && (
          <FormField size="long" help={t("ontologyImport.directionHelp")}>
            <Select
              label={t("ontologyImport.direction")}
              data={[
                { value: "from", label: phrase(true) },
                { value: "to", label: phrase(false) },
              ]}
              value={itemIsSource ? "from" : "to"}
              onChange={(v) => setItemIsSource(v !== "to")}
              allowDeselect={false}
              comboboxProps={{ withinPortal: true }}
              data-testid="ontology-add-alignment-direction"
            />
          </FormField>
        )}
        {error && (
          <FormField size="full">
            <Alert color="red" data-testid="ontology-add-alignment-error">
              {error}
            </Alert>
          </FormField>
        )}
        <FormFooter
          onCancel={onClose}
          action={
            <Button
              leftSection={<Plus size={14} />}
              onClick={() => void add()}
              loading={busy}
              disabled={target === null}
              data-testid="ontology-add-alignment-save"
            >
              {t("ontologyImport.addAlignment")}
            </Button>
          }
        />
      </FormLayout>
    </Modal>
  );
}
