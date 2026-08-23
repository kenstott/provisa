// Copyright (c) 2026 Kenneth Stott
// Canary: fdbadaed-2513-42aa-8781-07cb8a032249
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { useEffect, useState } from "react";
import { useTranslation } from "react-i18next";
import {
  Alert,
  Button,
  Code,
  Group,
  Modal,
  PasswordInput,
  Stack,
  Table,
  Text,
  TextInput,
  Tooltip,
} from "@mantine/core";
import { Copy, Plus } from "lucide-react";
import { useAuth } from "../../context/AuthContext";
import { ConfirmDialog } from "../ConfirmDialog";
import {
  fetchSecrets,
  putSecret,
  deleteSecret,
  type Secret,
  type SecretsState,
} from "../../api/secrets";

/**
 * REQ-1557, REQ-1558: the org's secrets — NAMES GO IN, VALUES NEVER COME BACK OUT.
 *
 * There is no "show" button here and there is no endpoint behind one: the list says what exists,
 * what it is for, who last set it, and the reference to paste. Somebody who has lost a value
 * REPLACES it, which is the same act that created it, so the form is the same form.
 */
export function SecretsTab() {
  const { t } = useTranslation();
  const { activeOrgId } = useAuth();
  const [state, setState] = useState<SecretsState | null>(null);
  const [error, setError] = useState("");
  const [message, setMessage] = useState("");
  // The name being replaced, "" for a new secret, null when the form is closed. A replacement
  // fixes the name, because the name is the identity and changing it would create a second secret.
  const [editing, setEditing] = useState<string | null>(null);
  const [name, setName] = useState("");
  const [description, setDescription] = useState("");
  const [value, setValue] = useState("");
  const [saving, setSaving] = useState(false);

  const load = () => {
    if (!activeOrgId) return;
    fetchSecrets(activeOrgId)
      .then(setState)
      .catch((e) => setError(String(e)));
  };

  useEffect(load, [activeOrgId]);

  const open = (secret: Secret | null) => {
    setEditing(secret ? secret.name : "");
    setName(secret ? secret.name : "");
    setDescription(secret?.description ?? "");
    setValue("");
    setError("");
    setMessage("");
  };

  const save = async () => {
    if (!activeOrgId) return;
    setSaving(true);
    setError("");
    try {
      await putSecret(activeOrgId, name, { value, description: description || null });
      setMessage(t(editing ? "secretsTab.replaced" : "secretsTab.created", { name }));
      setEditing(null);
      setValue("");
      load();
    } catch (e) {
      setError(String(e));
    } finally {
      setSaving(false);
    }
  };

  const remove = async (secret: Secret) => {
    if (!activeOrgId) return;
    setError("");
    try {
      await deleteSecret(activeOrgId, secret.name);
      setMessage(t("secretsTab.deleted", { name: secret.name }));
      load();
    } catch (e) {
      setError(String(e));
    }
  };

  if (error && !state) return <Alert color="red">{error}</Alert>;
  if (!state) return <Text>{t("secretsTab.loading")}</Text>;

  const writable = state.provider.writable;

  return (
    <Stack gap="md" data-testid="secrets-tab">
      <Text c="dimmed" fz="sm">
        {t("secretsTab.intro")}
      </Text>
      <Alert color={writable ? "blue" : "yellow"} title={state.provider.label}>
        {writable
          ? t("secretsTab.providerBuiltIn")
          : t("secretsTab.providerCentral", { provider: state.provider.label })}
      </Alert>

      {error && <Alert color="red">{error}</Alert>}
      {message && <Alert color="green">{message}</Alert>}

      {state.secrets.length === 0 ? (
        <Text c="dimmed">{t("secretsTab.empty")}</Text>
      ) : (
        <Table striped withTableBorder data-testid="secrets-table">
          <Table.Thead>
            <Table.Tr>
              <Table.Th>{t("secretsTab.columnName")}</Table.Th>
              <Table.Th>{t("secretsTab.columnDescription")}</Table.Th>
              <Table.Th>{t("secretsTab.columnUpdated")}</Table.Th>
              <Table.Th>{t("secretsTab.columnReference")}</Table.Th>
              <Table.Th />
            </Table.Tr>
          </Table.Thead>
          <Table.Tbody>
            {state.secrets.map((s) => (
              <Table.Tr key={s.name} data-testid={`secret-row-${s.name}`}>
                <Table.Td>
                  <Code>{s.name}</Code>
                </Table.Td>
                <Table.Td>{s.description ?? ""}</Table.Td>
                <Table.Td>
                  <Text fz="sm">
                    {s.updated_at ? new Date(s.updated_at).toLocaleString() : t("secretsTab.never")}
                  </Text>
                  {s.updated_by && (
                    <Text fz="xs" c="dimmed">
                      {t("secretsTab.updatedBy", { actor: s.updated_by })}
                    </Text>
                  )}
                </Table.Td>
                <Table.Td>
                  <Group gap="xs">
                    <Code>{s.reference}</Code>
                    <Tooltip label={t("secretsTab.copyReference")}>
                      <Button
                        variant="subtle"
                        size="compact-xs"
                        aria-label={t("secretsTab.copyReference")}
                        onClick={() => navigator.clipboard?.writeText(s.reference)}
                      >
                        <Copy size={14} />
                      </Button>
                    </Tooltip>
                  </Group>
                </Table.Td>
                <Table.Td>
                  {writable && (
                    <Group gap="xs" justify="flex-end">
                      <Button
                        variant="default"
                        size="compact-sm"
                        data-testid={`secret-replace-${s.name}`}
                        onClick={() => open(s)}
                      >
                        {t("secretsTab.replaceAction")}
                      </Button>
                      <ConfirmDialog
                        title={t("secretsTab.deleteAction")}
                        consequence={t("secretsTab.deleteConfirm", { name: s.name })}
                        onConfirm={() => remove(s)}
                      >
                        {(openConfirm) => (
                          <Button
                            color="red"
                            variant="light"
                            size="compact-sm"
                            data-testid={`secret-delete-${s.name}`}
                            onClick={openConfirm}
                          >
                            {t("secretsTab.deleteAction")}
                          </Button>
                        )}
                      </ConfirmDialog>
                    </Group>
                  )}
                </Table.Td>
              </Table.Tr>
            ))}
          </Table.Tbody>
        </Table>
      )}

      {writable && (
        <Group>
          <Button
            leftSection={<Plus size={16} />}
            data-testid="secrets-add"
            onClick={() => open(null)}
          >
            {t("secretsTab.add")}
          </Button>
        </Group>
      )}

      <Modal
        opened={editing !== null}
        onClose={() => setEditing(null)}
        title={editing ? t("secretsTab.replaceTitle", { name: editing }) : t("secretsTab.addTitle")}
        centered
      >
        <Stack gap="sm">
          <TextInput
            label={t("secretsTab.nameLabel")}
            placeholder={t("secretsTab.namePlaceholder")}
            description={t("secretsTab.nameHelp")}
            value={name}
            // A replacement keeps its name: the name is the identity of the secret being replaced.
            disabled={!!editing}
            onChange={(e) => setName(e.currentTarget.value)}
          />
          <TextInput
            label={t("secretsTab.descriptionLabel")}
            placeholder={t("secretsTab.descriptionPlaceholder")}
            value={description}
            onChange={(e) => setDescription(e.currentTarget.value)}
          />
          <PasswordInput
            label={t("secretsTab.valueLabel")}
            description={t("secretsTab.valueHelp")}
            value={value}
            onChange={(e) => setValue(e.currentTarget.value)}
          />
          <Group justify="flex-end">
            <Button variant="default" onClick={() => setEditing(null)} disabled={saving}>
              {t("secretsTab.cancel")}
            </Button>
            <Button
              onClick={save}
              loading={saving}
              data-testid="secret-submit"
              disabled={!name.trim() || !value}
            >
              {editing ? t("secretsTab.replace") : t("secretsTab.add")}
            </Button>
          </Group>
        </Stack>
      </Modal>
    </Stack>
  );
}
