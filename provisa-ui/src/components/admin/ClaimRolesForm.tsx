// Copyright (c) 2026 Kenneth Stott
// Canary: edef0d01-6585-47c5-b0c3-fa14688cbfc3
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// The claim types an org declares as claim roles: each becomes a role, and each value a member's
// attributes give it a sub-role (region -> region__eu). They carry no rights; a column grant or a
// mask names them like any role, and a request acts as one only by naming it.

import { useEffect, useState } from "react";
import { useTranslation } from "react-i18next";
import { TagsInput, Text } from "@mantine/core";
import { fetchClaimRoles, saveClaimRoles, type ClaimRoles } from "../../api/admin";
import { FormField, FormFooter, FormLayout } from "../form/FormLayout";

export function ClaimRolesForm({ onClose }: { onClose: () => void }) {
  const { t } = useTranslation();
  const [loaded, setLoaded] = useState<ClaimRoles | null>(null);
  const [types, setTypes] = useState<string[]>([]);
  const [saving, setSaving] = useState(false);
  const [problem, setProblem] = useState<string | null>(null);
  // Text typed but not yet a tag. It counts: blur makes it a tag, and Save takes it as one.
  const [typing, setTyping] = useState("");
  const [saved, setSaved] = useState(false);

  useEffect(() => {
    fetchClaimRoles()
      .then((c) => {
        setLoaded(c);
        setTypes(c.claim_types);
      })
      .catch((e: unknown) => setProblem(e instanceof Error ? e.message : String(e)));
  }, []);

  const save = async () => {
    setSaving(true);
    setProblem(null);
    setSaved(false);
    const pending = typing.trim();
    const next = pending && !types.includes(pending) ? [...types, pending] : types;
    try {
      const stored = await saveClaimRoles(next);
      setLoaded(stored);
      setTypes(stored.claim_types);
      setTyping("");
      setSaved(true);
    } catch (e) {
      setProblem(e instanceof Error ? e.message : String(e));
    } finally {
      setSaving(false);
    }
  };

  return (
    <FormLayout data-testid="claim-roles-form">
      <Text size="sm" c="dimmed">
        {t("securityPage.claimRolesIntro")}
      </Text>
      <FormField
        size="long"
        hint={
          loaded?.source === "deployment"
            ? t("securityPage.claimRolesFromDeployment")
            : t("securityPage.claimRolesFromOrg")
        }
        help={t("securityPage.claimRolesHelp")}
      >
        <TagsInput
          label={t("securityPage.claimTypes")}
          placeholder={t("securityPage.claimTypesPlaceholder")}
          value={types}
          onChange={(v) => {
            setTypes(v);
            setSaved(false);
          }}
          searchValue={typing}
          onSearchChange={(v) => {
            setTyping(v);
            setSaved(false);
          }}
          acceptValueOnBlur
          disabled={loaded === null}
          data-testid="claim-roles-input"
        />
      </FormField>
      {saved && (
        <Text size="sm" c="green" role="status" data-testid="claim-roles-saved">
          {t("securityPage.claimRolesSaved")}
        </Text>
      )}
      <FormFooter
        panel
        problem={problem ?? undefined}
        onSave={save}
        saving={saving}
        onCancel={onClose}
        saveProps={{ "data-testid": "claim-roles-save", disabled: loaded === null }}
      />
    </FormLayout>
  );
}
