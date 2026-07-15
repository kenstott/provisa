// Copyright (c) 2026 Kenneth Stott
// Canary: ee9310b9-1f41-4d77-8c12-3a724024d5b8
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import {
  Button,
  Checkbox,
  Group,
  Modal,
  NumberInput,
  Stack,
  Text,
  TextInput,
} from "@mantine/core";
import { useTranslation } from "react-i18next";
import type { Relationship } from "../../types/admin";
import type { RelForm } from "./relationship-types";

interface ConflictModalProps {
  rel: Relationship;
  onClose: () => void;
}

export function ConflictModal({ rel, onClose }: ConflictModalProps) {
  const { t } = useTranslation();
  return (
    <Modal opened onClose={onClose} title={t("relationshipModals.conflictTitle")} centered>
      <Stack gap="sm">
        <Text size="sm" c="dimmed">
          {t("relationshipModals.conflictIntro")}
        </Text>
        <Stack
          gap={6}
          style={{
            display: "grid",
            gridTemplateColumns: "auto 1fr",
            columnGap: "1rem",
            rowGap: "0.35rem",
          }}
        >
          <Text size="sm" fw={600} c="dimmed">
            {t("relationshipModals.conflictId")}
          </Text>
          <Text size="sm" ff="monospace">
            {rel.id}
          </Text>
          <Text size="sm" fw={600} c="dimmed">
            {t("relationshipModals.conflictSource")}
          </Text>
          <Text size="sm" ff="monospace">
            {rel.sourceTableName}.{rel.sourceColumn}
          </Text>
          <Text size="sm" fw={600} c="dimmed">
            {t("relationshipModals.conflictTarget")}
          </Text>
          <Text size="sm" ff="monospace">
            {rel.targetTableName}.{rel.targetColumn}
          </Text>
          <Text size="sm" fw={600} c="dimmed">
            {t("relationshipModals.conflictCardinality")}
          </Text>
          <Text size="sm">{rel.cardinality}</Text>
          {rel.alias && (
            <>
              <Text size="sm" fw={600} c="dimmed">
                {t("relationshipModals.conflictAlias")}
              </Text>
              <Text size="sm" ff="monospace">
                {rel.alias}
              </Text>
            </>
          )}
        </Stack>
        <Group justify="flex-end">
          <Button variant="default" onClick={onClose} data-testid="conflict-modal-close">
            {t("relationshipModals.conflictClose")}
          </Button>
        </Group>
      </Stack>
    </Modal>
  );
}

interface ReverseRelationshipModalProps {
  reverseForm: RelForm;
  setReverseForm: (f: RelForm | null) => void;
  saving: string | null;
  onSave: () => void;
}

export function ReverseRelationshipModal({
  reverseForm,
  setReverseForm,
  saving,
  onSave,
}: ReverseRelationshipModalProps) {
  const { t } = useTranslation();
  return (
    <Modal
      opened
      onClose={() => setReverseForm(null)}
      title={t("relationshipModals.reverseTitle")}
      centered
      size="730px"
    >
      <Stack gap="sm">
        <TextInput
          label={t("relationshipModals.idLabel")}
          value={reverseForm.id}
          onChange={(e) => setReverseForm({ ...reverseForm, id: e.target.value })}
          data-testid="reverse-rel-id"
        />
        <TextInput
          label={t("relationshipModals.aliasLabel")}
          placeholder={t("relationshipModals.aliasPlaceholder")}
          value={reverseForm.alias}
          onChange={(e) => setReverseForm({ ...reverseForm, alias: e.target.value })}
          data-testid="reverse-rel-alias"
        />
        <TextInput
          label={t("relationshipModals.graphqlAliasLabel")}
          value={reverseForm.graphqlAlias}
          onChange={(e) => setReverseForm({ ...reverseForm, graphqlAlias: e.target.value })}
          data-testid="reverse-rel-graphql-alias"
        />
        <Checkbox
          label={t("relationshipModals.materializeLabel")}
          checked={reverseForm.materialize}
          onChange={(e) => setReverseForm({ ...reverseForm, materialize: e.target.checked })}
          data-testid="reverse-rel-materialize"
        />
        {reverseForm.materialize && (
          <NumberInput
            label={t("relationshipModals.refreshIntervalLabel")}
            value={reverseForm.refreshInterval}
            onChange={(value) =>
              setReverseForm({ ...reverseForm, refreshInterval: String(value ?? "") })
            }
            data-testid="reverse-rel-refresh-interval"
          />
        )}
      </Stack>
      <Group justify="flex-end" mt="md">
        <Button
          variant="default"
          onClick={() => setReverseForm(null)}
          data-testid="reverse-rel-cancel"
        >
          {t("relationshipModals.cancel")}
        </Button>
        <Button onClick={onSave} loading={saving === "reverse"} data-testid="reverse-rel-save">
          {t("relationshipModals.save")}
        </Button>
      </Group>
    </Modal>
  );
}
