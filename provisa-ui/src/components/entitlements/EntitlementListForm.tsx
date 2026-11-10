// Copyright (c) 2026 Kenneth Stott
// Canary: c3f09e9c-f09b-4ead-b9d6-e84fbf67896f
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2219: set one entitlement list of a person's role grant, with a reason and an optional end
// date, through the one write API. What the form is opened on (a person, a role, a kind) is shown
// fixed; the rest is asked. The server decides whether the write stands and says why when not.

import { useState } from "react";
import { useTranslation } from "react-i18next";
import { Select, TextInput } from "@mantine/core";
import { DateInput } from "@mantine/dates";
import { setList, type AccessView, type Grantee, type PendingGrant } from "../../api/entitlements";
import { tomorrow, untilEndOf } from "../../lib/grantUntil";
import { FormField, FormFooter, FormLayout } from "../form/FormLayout";
import { EntitlementPicker } from "./EntitlementPicker";
import { ENTITLEMENT_KINDS, SEMANTIC_KINDS, isKind, type EntitlementKind } from "./kinds";
import { PeoplePicker } from "./PeoplePicker";

export interface EntitlementListFormProps {
  /** The person, when the form is opened on one; otherwise the form asks for them. */
  grantee?: Grantee;
  /** The role grant, when the form is opened on one; otherwise the form asks for it. */
  roleId?: string;
  /** The kind, when an existing list is edited; otherwise the form asks for it. */
  kind?: EntitlementKind;
  /** Kinds not offered when adding: those already held, or sent by the identity provider. */
  takenKinds?: ReadonlySet<string>;
  identifiers?: string[];
  /** The day the list ends (YYYY-MM-DD), when it has an end. */
  endsOn?: string | null;
  roleIds: string[];
  onSaved: (result: AccessView | PendingGrant) => void;
  onCancel: () => void;
  onError: (e: unknown) => void;
  testId?: string;
}

export function EntitlementListForm({
  grantee: fixedGrantee,
  roleId: fixedRole,
  kind: fixedKind,
  takenKinds,
  identifiers: initialIdentifiers = [],
  endsOn: initialEndsOn = null,
  roleIds,
  onSaved,
  onCancel,
  onError,
  testId = "entitlement-list-form",
}: EntitlementListFormProps) {
  const { t } = useTranslation();
  const [grantee, setGrantee] = useState<Grantee | null>(fixedGrantee ?? null);
  const [roleId, setRoleId] = useState<string | null>(fixedRole ?? null);
  const [kind, setKind] = useState<EntitlementKind | null>(fixedKind ?? null);
  const [identifiers, setIdentifiers] = useState<string[]>(initialIdentifiers);
  const [endsOn, setEndsOn] = useState<string | null>(initialEndsOn);
  const [reason, setReason] = useState("");

  const kindOptions = ENTITLEMENT_KINDS.filter((k) => k === fixedKind || !takenKinds?.has(k)).map(
    (k) => ({ value: k, label: t(`entitlements.kind.${k}`) }),
  );
  // The server refuses an end on a list that would remove nothing: an empty list, or a kind whose
  // objects the mask does not hold. The field is not offered there.
  const canEnd = kind !== null && SEMANTIC_KINDS.has(kind) && identifiers.length > 0;
  const ready = grantee !== null && roleId !== null && kind !== null && reason.trim() !== "";

  const save = async () => {
    if (!ready) return;
    try {
      onSaved(
        await setList({
          grantee,
          roleId,
          kind,
          identifiers,
          reason: reason.trim(),
          expiresAt: canEnd && endsOn ? untilEndOf(endsOn) : null,
        }),
      );
    } catch (e) {
      onError(e);
    }
  };

  return (
    <FormLayout data-testid={testId}>
      {fixedGrantee === undefined && (
        <FormField size="long" hint={t("entitlements.form.personHint")}>
          <PeoplePicker
            label={t("entitlements.form.personLabel")}
            value={grantee}
            onChange={setGrantee}
            onError={onError}
            testId={`${testId}-person`}
          />
        </FormField>
      )}
      {fixedRole === undefined && (
        <FormField size="medium">
          <Select
            label={t("entitlements.form.roleLabel")}
            placeholder={t("entitlements.form.rolePlaceholder")}
            data={[...roleIds].sort()}
            value={roleId}
            onChange={setRoleId}
            searchable
            data-testid={`${testId}-role`}
          />
        </FormField>
      )}
      {fixedKind === undefined && (
        <FormField size="medium">
          <Select
            label={t("entitlements.form.kindLabel")}
            placeholder={t("entitlements.form.kindPlaceholder")}
            data={kindOptions}
            value={kind}
            onChange={(v) => {
              setKind(v !== null && isKind(v) ? v : null);
              setIdentifiers([]);
            }}
            data-testid={`${testId}-kind`}
          />
        </FormField>
      )}
      {kind !== null && (
        <FormField size="long" help={t("entitlements.form.entriesHelp")}>
          <EntitlementPicker
            label={t("entitlements.form.entriesLabel")}
            kind={kind}
            value={identifiers}
            onChange={setIdentifiers}
            roleIds={roleIds}
            testId={`${testId}-picker`}
          />
        </FormField>
      )}
      {canEnd && (
        <FormField size="short" hint={t("entitlements.form.endsOnHint")}>
          <DateInput
            label={t("entitlements.form.endsOnLabel")}
            valueFormat="YYYY-MM-DD"
            clearable
            minDate={tomorrow()}
            value={endsOn}
            onChange={setEndsOn}
            popoverProps={{ withinPortal: true }}
            data-testid={`${testId}-ends-on`}
          />
        </FormField>
      )}
      <FormField size="long" hint={t("entitlements.form.reasonHint")}>
        <TextInput
          label={t("entitlements.form.reasonLabel")}
          required
          value={reason}
          onChange={(e) => setReason(e.currentTarget.value)}
          data-testid={`${testId}-reason`}
        />
      </FormField>
      <FormFooter
        onSave={save}
        onCancel={onCancel}
        saveProps={{ disabled: !ready, "data-testid": `${testId}-save` }}
        cancelProps={{ "data-testid": `${testId}-cancel` }}
      />
    </FormLayout>
  );
}
