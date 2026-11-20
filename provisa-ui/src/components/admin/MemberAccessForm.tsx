// Copyright (c) 2026 Kenneth Stott
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { useState } from "react";
import { useTranslation } from "react-i18next";
import { ActionIcon, Button, Select, TextInput } from "@mantine/core";
import { DateInput } from "@mantine/dates";
import { Plus, X } from "lucide-react";
import { grantMemberAccess } from "../../api/admin";
import { useCapability } from "../../hooks/useCapability";
import { tomorrow, untilEndOf } from "../../lib/grantUntil";
import { FormField, FormFooter, FormLayout } from "../form/FormLayout";
import { MultiSelect } from "../MultiSelect";
import { EntitlementPicker } from "../entitlements/EntitlementPicker";
import { kindInfo, useEntitlementKinds } from "../entitlements/kinds";

/** One entitlement list of a grant the save makes. */
interface ListRow {
  key: number;
  roleId: string | null;
  kind: string | null;
  identifiers: string[];
}

export interface MemberAccessFormProps {
  userId: string;
  /** The org's role ids, offered for the member. */
  allRoles: string[];
  /** The domains a grant may be scoped to, `*` first. */
  domainOptions: { value: string; label: string }[];
  onSaved: () => void;
  onError: (e: unknown) => void;
}

/**
 * A member's access in this environment, given in one save (REQ-2155): several roles over a
 * domain until a day, and -- for a caller holding assign_entitlements -- entitlement lists of
 * those grants with the reason every list records. The server grants all of it or none.
 */
export function MemberAccessForm({
  userId,
  allRoles,
  domainOptions,
  onSaved,
  onError,
}: MemberAccessFormProps) {
  const { t } = useTranslation();
  const assigns = useCapability("assign_entitlements");
  const [roleIds, setRoleIds] = useState<string[]>([]);
  const [domain, setDomain] = useState("*");
  const [until, setUntil] = useState<string | null>(null);
  const [lists, setLists] = useState<ListRow[]>([]);
  const [nextKey, setNextKey] = useState(0);
  const [reason, setReason] = useState("");

  const kinds = useEntitlementKinds(onError);
  const kindOptions = (kinds?.kinds ?? []).map((k) => ({
    value: k.kind,
    label: t(`entitlements.kind.${k.kind}`),
  }));
  const listsReady = lists.every((l) => l.roleId !== null && l.kind !== null);
  const ready = roleIds.length > 0 && listsReady && (lists.length === 0 || reason.trim() !== "");

  const patch = (key: number, change: Partial<ListRow>) =>
    setLists((prev) => prev.map((l) => (l.key === key ? { ...l, ...change } : l)));

  const chooseRoles = (next: string[]) => {
    setRoleIds(next);
    // A list belongs to a grant the save makes: a role taken out takes its lists' role with it.
    setLists((prev) =>
      prev.map((l) => (l.roleId !== null && !next.includes(l.roleId) ? { ...l, roleId: null } : l)),
    );
  };

  const addList = () => {
    setLists((prev) => [
      ...prev,
      {
        key: nextKey,
        roleId: roleIds.length === 1 ? roleIds[0] : null,
        kind: null,
        identifiers: [],
      },
    ]);
    setNextKey((k) => k + 1);
  };

  const save = async () => {
    if (!ready) return;
    try {
      await grantMemberAccess(userId, {
        role_ids: roleIds,
        domain_id: domain,
        expires_at: until ? untilEndOf(until) : null,
        entitlements: lists.map((l) => ({
          role_id: l.roleId as string,
          kind: l.kind as string,
          identifiers: l.identifiers,
        })),
        reason: reason.trim(),
      });
      setRoleIds([]);
      setUntil(null);
      setLists([]);
      setReason("");
      onSaved();
    } catch (e) {
      onError(e);
    }
  };

  return (
    <FormLayout data-testid="member-access-form">
      <FormField size="long">
        <MultiSelect
          label={t("localUsers.rolesLabel")}
          placeholder={t("localUsers.rolesPlaceholder")}
          options={allRoles.map((r) => ({ id: r, label: r }))}
          value={roleIds}
          onChange={chooseRoles}
        />
      </FormField>
      <FormField size="medium">
        <Select
          label={t("localUsers.domainLabel")}
          data={domainOptions}
          value={domain}
          onChange={(v) => setDomain(v ?? "*")}
          allowDeselect={false}
          data-testid="member-access-domain"
        />
      </FormField>
      <FormField size="short">
        <DateInput
          label={t("localUsers.untilLabel")}
          placeholder={t("localUsers.untilPlaceholder")}
          valueFormat="YYYY-MM-DD"
          clearable
          minDate={tomorrow()}
          value={until}
          onChange={setUntil}
          popoverProps={{ withinPortal: true }}
          data-testid="assignment-until"
        />
      </FormField>
      {assigns &&
        lists.map((l) => {
          const info = l.kind === null ? undefined : kindInfo(kinds, l.kind);
          return [
            <FormField size="medium" key={`role-${l.key}`}>
              <Select
                label={t("entitlements.form.roleLabel")}
                placeholder={t("entitlements.form.rolePlaceholder")}
                data={roleIds}
                value={l.roleId}
                onChange={(v) => patch(l.key, { roleId: v })}
                nothingFoundMessage={t("entitlements.form.noRoles")}
                data-testid={`member-access-list-${l.key}-role`}
              />
            </FormField>,
            <FormField size="medium" key={`kind-${l.key}`}>
              <Select
                label={t("entitlements.form.kindLabel")}
                placeholder={t("entitlements.form.kindPlaceholder")}
                data={kindOptions}
                value={l.kind}
                onChange={(v) => patch(l.key, { kind: v, identifiers: [] })}
                data-testid={`member-access-list-${l.key}-kind`}
              />
            </FormField>,
            info !== undefined ? (
              <FormField
                size="long"
                key={`entries-${l.key}`}
                help={t("entitlements.form.entriesHelp")}
              >
                <EntitlementPicker
                  label={t("entitlements.form.entriesLabel")}
                  kind={info}
                  names={kinds?.names ?? null}
                  value={l.identifiers}
                  onChange={(ids) => patch(l.key, { identifiers: ids })}
                  roleIds={allRoles}
                  testId={`member-access-list-${l.key}-picker`}
                />
              </FormField>
            ) : null,
            <FormField size="short" key={`remove-${l.key}`}>
              <ActionIcon
                variant="subtle"
                color="red"
                aria-label={t("localUsers.removeList")}
                onClick={() => setLists((prev) => prev.filter((x) => x.key !== l.key))}
                data-testid={`member-access-list-${l.key}-remove`}
              >
                <X size={14} />
              </ActionIcon>
            </FormField>,
          ];
        })}
      {assigns && (
        <FormField size="medium">
          <Button
            variant="default"
            size="xs"
            leftSection={<Plus size={14} />}
            onClick={addList}
            disabled={roleIds.length === 0}
            data-testid="member-access-add-list"
          >
            {t("localUsers.addList")}
          </Button>
        </FormField>
      )}
      {assigns && lists.length > 0 && (
        <FormField size="long" hint={t("entitlements.form.reasonHint")}>
          <TextInput
            label={t("entitlements.form.reasonLabel")}
            required
            value={reason}
            onChange={(e) => setReason(e.currentTarget.value)}
            data-testid="member-access-reason"
          />
        </FormField>
      )}
      <FormFooter
        onSave={save}
        saveProps={{ disabled: !ready, "data-testid": "member-access-save" }}
      />
    </FormLayout>
  );
}
