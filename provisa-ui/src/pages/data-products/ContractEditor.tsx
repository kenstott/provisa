// Copyright (c) 2026 Kenneth Stott
// Canary: 1340ed77-3468-4fa8-b060-b625b22ff350
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2158: a product member's contract as the product's form edits it: the same fields the panel
// shows. A change that stops promising a column is a new version; the server refuses the same
// version, naming the contract.

import { useTranslation } from "react-i18next";
import {
  ActionIcon,
  Button,
  Group,
  MultiSelect,
  Select,
  Stack,
  Text,
  Textarea,
  TextInput,
} from "@mantine/core";
import { Plus, X } from "lucide-react";
import type { ContractColumn, DataContract } from "../../types/admin";
import { CONTRACT_STATUSES } from "./contractDraft";
import type { DataContractInput } from "../../hooks/useDataProductMutations";
import { FormField, FormLayout } from "../../components/form/FormLayout";

export interface MemberColumn {
  name: string;
  dataType: string | null;
}

export function ContractEditor({
  contract,
  draft,
  memberColumns,
  onChange,
}: {
  contract: DataContract;
  draft: DataContractInput;
  /** The member's registered columns: what the contract may promise. */
  memberColumns: MemberColumn[];
  onChange: (draft: DataContractInput) => void;
}) {
  const { t } = useTranslation();
  const id = contract.tableId;
  const set = (patch: Partial<DataContractInput>) => onChange({ ...draft, ...patch });
  const setTerms = (patch: Partial<DataContractInput["terms"]>) =>
    set({ terms: { ...draft.terms, ...patch } });
  const promised = draft.columns.map((c) => c.name);
  const typeOf = new Map(memberColumns.map((c) => [c.name, c.dataType]));
  // A promise keeps the type it was made at; a newly promised column takes the member's type.
  const promise = (names: string[]): ContractColumn[] =>
    names.map(
      (name) =>
        draft.columns.find((c) => c.name === name) ?? {
          name,
          type: typeOf.get(name) ?? "",
          description: null,
          required: false,
        },
    );
  const choices = Array.from(new Set([...memberColumns.map((c) => c.name), ...promised]));
  return (
    <Stack gap="xs" data-testid={`data-contract-editor-${id}`}>
      <Text size="sm" ff="monospace" fw={500}>
        {contract.schemaName}.{contract.tableName}
      </Text>
      <FormLayout>
        <FormField size="short">
          <TextInput
            label={t("dataProductsTab.contract.versionLabel")}
            required
            value={draft.version}
            onChange={(e) => set({ version: e.target.value })}
            data-testid={`data-contract-version-input-${id}`}
          />
        </FormField>
        <FormField size="short">
          <Select
            label={t("dataProductsTab.contract.statusLabel")}
            value={draft.status}
            onChange={(v) => set({ status: v ?? "draft" })}
            data={CONTRACT_STATUSES.map((s) => ({
              value: s,
              label: t(`dataProductsTab.contract.status.${s}`),
            }))}
            allowDeselect={false}
            data-testid={`data-contract-status-input-${id}`}
          />
        </FormField>
        <FormField size="medium">
          <TextInput
            label={t("dataProductsTab.contract.noticePeriod")}
            value={draft.terms.noticePeriod ?? ""}
            onChange={(e) => setTerms({ noticePeriod: e.target.value || null })}
            placeholder={t("dataProductsTab.contract.noticePeriodPlaceholder")}
            data-testid={`data-contract-notice-input-${id}`}
          />
        </FormField>
      </FormLayout>
      <FormLayout>
        <FormField size="full">
          <Textarea
            label={t("dataProductsTab.contract.descriptionLabel")}
            value={draft.description ?? ""}
            onChange={(e) => set({ description: e.target.value || null })}
            rows={2}
            data-testid={`data-contract-description-input-${id}`}
          />
        </FormField>
        <FormField size="full">
          <Textarea
            label={t("dataProductsTab.contract.usage")}
            value={draft.terms.usage ?? ""}
            onChange={(e) => setTerms({ usage: e.target.value || null })}
            rows={2}
            data-testid={`data-contract-usage-input-${id}`}
          />
        </FormField>
        <FormField size="full">
          <Textarea
            label={t("dataProductsTab.contract.limitations")}
            value={draft.terms.limitations ?? ""}
            onChange={(e) => setTerms({ limitations: e.target.value || null })}
            rows={2}
            data-testid={`data-contract-limitations-input-${id}`}
          />
        </FormField>
      </FormLayout>
      <FormLayout>
        <FormField size="long" help={t("dataProductsTab.contract.columnsHelp")}>
          <MultiSelect
            label={t("dataProductsTab.contract.columnsLabel")}
            value={promised}
            onChange={(names) => set({ columns: promise(names) })}
            data={choices}
            searchable
            data-testid={`data-contract-columns-input-${id}`}
          />
        </FormField>
        <FormField size="long">
          <MultiSelect
            label={t("dataProductsTab.contract.requiredLabel")}
            value={draft.columns.filter((c) => c.required).map((c) => c.name)}
            onChange={(names) =>
              set({ columns: draft.columns.map((c) => ({ ...c, required: names.includes(c.name) })) })
            }
            data={promised}
            data-testid={`data-contract-required-input-${id}`}
          />
        </FormField>
      </FormLayout>
      <Stack gap={4}>
        <Text size="sm" fw={500}>
          {t("dataProductsTab.contract.slaLabel")}
        </Text>
        {draft.sla.map((s, i) => (
          <Group key={i} gap="xs" wrap="nowrap" align="flex-end">
            <TextInput
              aria-label={t("dataProductsTab.contract.slaProperty")}
              placeholder={t("dataProductsTab.contract.slaProperty")}
              value={s.property}
              onChange={(e) =>
                set({
                  sla: draft.sla.map((x, j) => (j === i ? { ...x, property: e.target.value } : x)),
                })
              }
              data-testid={`data-contract-sla-property-${id}-${i}`}
            />
            <TextInput
              aria-label={t("dataProductsTab.contract.slaValue")}
              placeholder={t("dataProductsTab.contract.slaValue")}
              value={s.value === null || s.value === undefined ? "" : String(s.value)}
              onChange={(e) =>
                set({
                  sla: draft.sla.map((x, j) =>
                    j === i ? { ...x, value: e.target.value === "" ? null : e.target.value } : x,
                  ),
                })
              }
              data-testid={`data-contract-sla-value-${id}-${i}`}
            />
            <TextInput
              aria-label={t("dataProductsTab.contract.slaUnit")}
              placeholder={t("dataProductsTab.contract.slaUnit")}
              value={s.unit ?? ""}
              onChange={(e) =>
                set({
                  sla: draft.sla.map((x, j) =>
                    j === i ? { ...x, unit: e.target.value || null } : x,
                  ),
                })
              }
              w={90}
              data-testid={`data-contract-sla-unit-${id}-${i}`}
            />
            <ActionIcon
              variant="subtle"
              color="gray"
              aria-label={t("dataProductsTab.contract.slaRemove")}
              onClick={() => set({ sla: draft.sla.filter((_, j) => j !== i) })}
              data-testid={`data-contract-sla-remove-${id}-${i}`}
            >
              <X size={14} />
            </ActionIcon>
          </Group>
        ))}
        <Group>
          <Button
            variant="subtle"
            size="xs"
            leftSection={<Plus size={14} />}
            onClick={() =>
              set({
                sla: [...draft.sla, { property: "", value: null, unit: null, description: null }],
              })
            }
            data-testid={`data-contract-sla-add-${id}`}
          >
            {t("dataProductsTab.contract.slaAdd")}
          </Button>
        </Group>
      </Stack>
    </Stack>
  );
}
