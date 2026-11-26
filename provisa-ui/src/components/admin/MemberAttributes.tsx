// Copyright (c) 2026 Kenneth Stott
// Canary: 990d0bcf-7da7-4a66-bd81-eeb838daa47f
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// A member's attributes: what the org records for them and what their identity provider supplied
// at their last sign-in. A row rule reads either the same way; where both name one, the org's
// value is in force and the provider's is shown overridden.

import { useCallback, useEffect, useState } from "react";
import { useTranslation } from "react-i18next";
import {
  ActionIcon,
  Alert,
  Badge,
  Button,
  Group,
  Stack,
  Table,
  Text,
  TextInput,
} from "@mantine/core";
import { Check, Pencil, Trash2, X } from "lucide-react";
import { FieldGroup, FormField, FormLayout } from "../form/FormLayout";
import { useRequiredFields } from "../form/useRequiredFields";
import {
  deleteMemberAttribute,
  fetchMemberAttributes,
  setMemberAttribute,
  type MemberAttribute,
} from "../../api/admin";

export function MemberAttributes({ userId }: { userId: string }) {
  const { t } = useTranslation();
  const [rows, setRows] = useState<MemberAttribute[] | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [name, setName] = useState("");
  const [value, setValue] = useState("");
  const [editing, setEditing] = useState<{ name: string; value: string } | null>(null);
  // REQ-1968: an attribute needs a name, checked when Add is pressed.
  const required = useRequiredFields({ name });

  const report = (e: unknown) => setError(e instanceof Error ? e.message : String(e));

  const load = useCallback(async () => {
    setRows(await fetchMemberAttributes(userId));
  }, [userId]);

  // A member's panel is mounted afresh for each member (keyed by the caller), so nothing of the
  // last member's is carried over.
  useEffect(() => {
    fetchMemberAttributes(userId).then(setRows).catch(report);
  }, [userId]);

  const save = async (attr: string, val: string) => {
    setError(null);
    try {
      await setMemberAttribute(userId, attr, val);
      await load();
      return true;
    } catch (e) {
      report(e);
      return false;
    }
  };

  const add = async () => {
    if (!required.validate()) return;
    if (await save(name, value)) {
      setName("");
      setValue("");
      required.reset();
    }
  };

  const saveEdit = async () => {
    if (editing && (await save(editing.name, editing.value))) setEditing(null);
  };

  const remove = async (attr: string) => {
    setError(null);
    try {
      await deleteMemberAttribute(userId, attr);
      await load();
    } catch (e) {
      report(e);
    }
  };

  return (
    <Stack gap="sm" data-testid="member-attributes">
      <Text size="sm" c="dimmed">
        {t("teamPage.attributes.help")}
      </Text>
      {error && (
        <Alert color="red" data-testid="member-attributes-error">
          {error}
        </Alert>
      )}
      <Table withRowBorders>
        <Table.Thead>
          <Table.Tr>
            <Table.Th>{t("teamPage.attributes.colName")}</Table.Th>
            <Table.Th>{t("teamPage.attributes.colValue")}</Table.Th>
            <Table.Th>{t("teamPage.attributes.colSource")}</Table.Th>
            <Table.Th />
          </Table.Tr>
        </Table.Thead>
        <Table.Tbody>
          {rows !== null && rows.length === 0 && (
            <Table.Tr>
              <Table.Td colSpan={4}>
                <Text size="sm" c="dimmed">
                  {t("teamPage.attributes.empty")}
                </Text>
              </Table.Td>
            </Table.Tr>
          )}
          {(rows ?? []).map((a) => {
            const isEditing = a.source === "org" && editing?.name === a.name;
            return (
              <Table.Tr
                key={`${a.source}:${a.name}`}
                data-testid={`member-attr-${a.source}-${a.name}`}
              >
                <Table.Td>
                  <Text size="sm" ff="monospace">
                    {a.name}
                  </Text>
                </Table.Td>
                <Table.Td>
                  {isEditing ? (
                    <TextInput
                      size="xs"
                      aria-label={t("teamPage.attributes.valueLabel")}
                      value={editing.value}
                      onChange={(e) => setEditing({ name: a.name, value: e.currentTarget.value })}
                      onKeyDown={(e) => {
                        if (e.key === "Enter") void saveEdit();
                        if (e.key === "Escape") setEditing(null);
                      }}
                      data-testid={`member-attr-edit-${a.name}`}
                    />
                  ) : (
                    <Text
                      size="sm"
                      td={a.overridden ? "line-through" : undefined}
                      c={a.overridden ? "dimmed" : undefined}
                      title={a.overridden ? t("teamPage.attributes.overriddenHint") : undefined}
                    >
                      {a.value}
                    </Text>
                  )}
                </Table.Td>
                <Table.Td>
                  <Badge variant="light" color={a.source === "org" ? "blue" : "gray"}>
                    {a.source === "org"
                      ? t("teamPage.attributes.sourceOrg")
                      : t("teamPage.attributes.sourceIdp")}
                  </Badge>
                </Table.Td>
                <Table.Td>
                  {a.source === "org" && (
                    <Group gap={4} wrap="nowrap" justify="flex-end">
                      {isEditing ? (
                        <>
                          <ActionIcon
                            variant="subtle"
                            aria-label={t("teamPage.attributes.saveEdit", { name: a.name })}
                            onClick={() => void saveEdit()}
                          >
                            <Check size={14} />
                          </ActionIcon>
                          <ActionIcon
                            variant="subtle"
                            color="gray"
                            aria-label={t("teamPage.attributes.cancelEdit")}
                            onClick={() => setEditing(null)}
                          >
                            <X size={14} />
                          </ActionIcon>
                        </>
                      ) : (
                        <ActionIcon
                          variant="subtle"
                          aria-label={t("teamPage.attributes.edit", { name: a.name })}
                          onClick={() => setEditing({ name: a.name, value: a.value })}
                        >
                          <Pencil size={14} />
                        </ActionIcon>
                      )}
                      <ActionIcon
                        variant="subtle"
                        color="red"
                        aria-label={t("teamPage.attributes.remove", { name: a.name })}
                        onClick={() => void remove(a.name)}
                        data-testid={`member-attr-remove-${a.name}`}
                      >
                        <Trash2 size={14} />
                      </ActionIcon>
                    </Group>
                  )}
                </Table.Td>
              </Table.Tr>
            );
          })}
        </Table.Tbody>
      </Table>
      <FormLayout>
        <FieldGroup row>
          <FormField size="short">
            <TextInput
              label={t("teamPage.attributes.nameLabel")}
              placeholder="region"
              {...required.field("name")}
              value={name}
              onChange={(e) => setName(e.currentTarget.value)}
              data-testid="member-attr-name"
            />
          </FormField>
          <FormField size="short">
            <TextInput
              label={t("teamPage.attributes.valueLabel")}
              placeholder="eu"
              value={value}
              onChange={(e) => setValue(e.currentTarget.value)}
              data-testid="member-attr-value"
            />
          </FormField>
          <Button onClick={() => void add()} data-testid="member-attr-add">
            {t("teamPage.attributes.add")}
          </Button>
        </FieldGroup>
      </FormLayout>
    </Stack>
  );
}
