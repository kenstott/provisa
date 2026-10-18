// Copyright (c) 2026 Kenneth Stott
// Canary: 220ec6b0-4686-4e9c-a912-768923663e7e
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2078, REQ-2079: derived roles in the Roles list. Each attribute the org declares is a parent
// role, marked derived, with a value role (region__eu) under it for each value it has been seen
// with. Its panel shows the attribute, its description, the values seen and what names it; it
// carries no grant controls, since a derived role holds no rights and is never granted by hand.

import { useState } from "react";
import { useTranslation } from "react-i18next";
import { ActionIcon, Badge, Group, Table, Text, TextInput, Textarea } from "@mantine/core";
import { Pencil, Trash2 } from "lucide-react";
import {
  deleteDerivedRole,
  saveDerivedRole,
  type DeclaredDerivedRole as DerivedRole,
} from "../../api/admin";
import { useAuth } from "../../context/AuthContext";
import { ConfirmDialog } from "../../components/ConfirmDialog";
import { DetailFields } from "../../components/detail/DetailFields";
import {
  FormField,
  FormFooter,
  FormIdentifiers,
  FormLayout,
} from "../../components/form/FormLayout";
import { ListRow } from "../../components/list/ListTable";
import { PanelActions } from "../../components/list/SplitPane";
import { Tip } from "../../components/Tip";
import { SEPARATOR, type DerivedPick } from "../../hooks/useDerivedRoles";

/** The list rows of the derived roles that match ``search``: each parent, then its value roles. */
export function DerivedRoleRows({
  roles,
  search,
  picked,
  onPick,
}: {
  roles: DerivedRole[];
  search: string;
  picked: DerivedPick | null;
  onPick: (pick: DerivedPick) => void;
}) {
  const q = search.trim().toLowerCase();
  return (
    <>
      {roles
        .filter(
          (d) =>
            !q ||
            d.attribute.includes(q) ||
            d.values.some((v) => `${d.attribute}${SEPARATOR}${v}`.toLowerCase().includes(q)),
        )
        .map((d) => (
          <DerivedRowGroup key={d.attribute} d={d} picked={picked} onPick={onPick} />
        ))}
    </>
  );
}

function DerivedRowGroup({
  d,
  picked,
  onPick,
}: {
  d: DerivedRole;
  picked: DerivedPick | null;
  onPick: (pick: DerivedPick) => void;
}) {
  const { t } = useTranslation();
  const isPicked = (value: string | null) =>
    picked !== null && picked.attribute === d.attribute && picked.value === value;
  return (
    <>
      <ListRow
        testId={`derived-role-row-${d.attribute}`}
        selected={isPicked(null)}
        onClick={() => onPick({ attribute: d.attribute, value: null })}
      >
        <Table.Td>
          <Group gap={6} wrap="nowrap">
            <span>{d.attribute}</span>
            <Badge size="xs" variant="light" data-testid={`derived-badge-${d.attribute}`}>
              {t("securityPage.derivedBadge")}
            </Badge>
          </Group>
        </Table.Td>
        <Table.Td>{t("securityPage.derivedNoRights")}</Table.Td>
        <Table.Td>{t("securityPage.derivedNoDomains")}</Table.Td>
        <Table.Td>{d.description}</Table.Td>
      </ListRow>
      {d.values.map((v) => (
        <ListRow
          key={v}
          testId={`derived-value-row-${d.attribute}${SEPARATOR}${v}`}
          selected={isPicked(v)}
          onClick={() => onPick({ attribute: d.attribute, value: v })}
        >
          <Table.Td className="derived-value-cell">{`${d.attribute}${SEPARATOR}${v}`}</Table.Td>
          <Table.Td>{t("securityPage.derivedNoRights")}</Table.Td>
          <Table.Td>{t("securityPage.derivedNoDomains")}</Table.Td>
          <Table.Td>
            {t("securityPage.valueRoleHeld", { attribute: d.attribute, value: v })}
          </Table.Td>
        </ListRow>
      ))}
    </>
  );
}

/** The form that declares a derived role: the attribute and what it is for. */
export function DerivedRoleNewForm({
  onSaved,
  onCancel,
}: {
  onSaved: (attribute: string) => Promise<void>;
  onCancel: () => void;
}) {
  const { t } = useTranslation();
  const { rebind } = useAuth();
  const [attribute, setAttribute] = useState("");
  const [description, setDescription] = useState("");
  const [problem, setProblem] = useState<string | null>(null);
  const save = async () => {
    setProblem(null);
    try {
      await saveDerivedRole(attribute.trim(), description.trim());
      // The roles the caller holds follow the list: re-read, so the role picker shows them now.
      await rebind();
      await onSaved(attribute.trim().toLowerCase());
    } catch (e) {
      setProblem(e instanceof Error ? e.message : String(e));
    }
  };
  return (
    <FormLayout data-testid="derived-role-form">
      <Text size="sm" c="dimmed">
        {t("securityPage.derivedRolesHelp")}
      </Text>
      <FormIdentifiers>
        <FormField size="medium" help={t("securityPage.derivedAttributeHelp")}>
          <TextInput
            label={t("securityPage.derivedAttribute")}
            placeholder={t("securityPage.attributesPlaceholder")}
            required
            value={attribute}
            onChange={(e) => setAttribute(e.target.value)}
            data-testid="derived-role-attribute"
          />
        </FormField>
      </FormIdentifiers>
      <FormField size="full" help={t("securityPage.roleDescriptionHelp")}>
        <Textarea
          label={t("securityPage.roleDescription")}
          required
          autosize
          minRows={2}
          value={description}
          onChange={(e) => setDescription(e.target.value)}
          data-testid="derived-role-description"
        />
      </FormField>
      <FormFooter
        panel
        problem={problem ?? undefined}
        onSave={save}
        onCancel={onCancel}
        saveProps={{
          "data-testid": "save-derived-role",
          disabled: !attribute.trim() || !description.trim(),
        }}
      />
    </FormLayout>
  );
}

/** A derived parent role's detail: read, or its description edited; or a value role's line. */
export function DerivedRoleDetail({
  role,
  value,
  onChanged,
  onDeleted,
}: {
  role: DerivedRole;
  value: string | null;
  onChanged: () => Promise<void>;
  onDeleted: () => Promise<void>;
}) {
  const { t } = useTranslation();
  const { rebind } = useAuth();
  const [editing, setEditing] = useState(false);
  const [description, setDescription] = useState(role.description);
  const [problem, setProblem] = useState<string | null>(null);

  if (value !== null) {
    return (
      <DetailFields
        testId={`value-role-read-${role.attribute}${SEPARATOR}${value}`}
        fields={[
          { label: t("securityPage.roleId"), value: `${role.attribute}${SEPARATOR}${value}` },
          {
            label: t("securityPage.roleDescription"),
            value: t("securityPage.valueRoleHeld", { attribute: role.attribute, value }),
          },
          { label: t("securityPage.derivedFrom"), value: role.attribute },
        ]}
      />
    );
  }

  const save = async () => {
    setProblem(null);
    try {
      await saveDerivedRole(role.attribute, description.trim());
      await onChanged();
      setEditing(false);
    } catch (e) {
      setProblem(e instanceof Error ? e.message : String(e));
    }
  };
  const remove = async () => {
    setProblem(null);
    try {
      await deleteDerivedRole(role.attribute);
      await rebind();
      await onDeleted();
    } catch (e) {
      setProblem(e instanceof Error ? e.message : String(e));
    }
  };

  if (editing) {
    return (
      <FormLayout data-testid={`derived-role-edit-${role.attribute}`}>
        <FormField size="full" help={t("securityPage.roleDescriptionHelp")}>
          <Textarea
            label={t("securityPage.roleDescription")}
            required
            autosize
            minRows={2}
            value={description}
            onChange={(e) => setDescription(e.target.value)}
            data-testid="derived-role-description"
          />
        </FormField>
        <FormFooter
          panel
          problem={problem ?? undefined}
          onSave={save}
          onCancel={() => {
            setDescription(role.description);
            setEditing(false);
          }}
          saveProps={{ "data-testid": "save-derived-role", disabled: !description.trim() }}
        />
      </FormLayout>
    );
  }

  return (
    <>
      <PanelActions>
        <fieldset className="panel-action-set">
          <Group gap="xs">
            <Tip text={t("securityPage.edit")} named>
              <ActionIcon
                aria-label={t("securityPage.edit")}
                data-testid={`edit-derived-role-${role.attribute}`}
                onClick={() => setEditing(true)}
              >
                <Pencil size={14} />
              </ActionIcon>
            </Tip>
            <ConfirmDialog
              title={t("securityPage.deleteDerivedTitle", { id: role.attribute })}
              consequence={t("securityPage.deleteDerivedConsequence")}
              onConfirm={remove}
            >
              {(open) => (
                <Tip text={t("securityPage.delete")} named>
                  <ActionIcon
                    color="red"
                    aria-label={t("securityPage.delete")}
                    data-testid={`delete-derived-role-${role.attribute}`}
                    onClick={open}
                  >
                    <Trash2 size={14} />
                  </ActionIcon>
                </Tip>
              )}
            </ConfirmDialog>
          </Group>
        </fieldset>
      </PanelActions>
      {problem && (
        <Text c="red" size="sm" role="alert">
          {problem}
        </Text>
      )}
      <DetailFields
        testId={`derived-role-read-${role.attribute}`}
        fields={[
          { label: t("securityPage.derivedAttribute"), value: role.attribute },
          { label: t("securityPage.roleDescription"), value: role.description },
          {
            label: t("securityPage.derivedValuesSeen"),
            value:
              role.values.length > 0
                ? role.values.map((v) => `${role.attribute}${SEPARATOR}${v}`).join(", ")
                : t("securityPage.derivedNoValuesYet"),
            testId: `derived-values-${role.attribute}`,
          },
          {
            label: t("securityPage.derivedUsedBy"),
            value:
              role.uses.length > 0
                ? role.uses.map((u) => (
                    <span key={`${u.kind}:${u.name}`} className="role-session-value">
                      {t(`securityPage.useKind.${u.kind}`)} {u.name}
                    </span>
                  ))
                : t("securityPage.derivedUnused"),
            testId: `derived-uses-${role.attribute}`,
          },
        ]}
      />
    </>
  );
}
