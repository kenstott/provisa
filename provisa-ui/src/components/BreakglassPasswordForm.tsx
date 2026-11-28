// Copyright (c) 2026 Kenneth Stott
// Canary: 2bd8c57a-0262-43fd-968e-aea3e924c8e5
// Canary: placeholder
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1264: the break-glass account signs in first with an installation-generated password, and
// the server refuses that sign-in (403 auth.password_change_required) until it is replaced. This
// is the form the sign-in page answers that refusal with.

import { useState } from "react";
import type { FormEvent } from "react";
import { Alert, Button, PasswordInput, Text } from "@mantine/core";
import { useTranslation } from "react-i18next";
import { FormField, FormFooter, FormLayout } from "./form/FormLayout";
import { useRequiredFields } from "./form/useRequiredFields";
import { BREAKGLASS_MIN_PASSWORD_LENGTH, changeBreakglassPassword } from "../api/breakglass";

interface BreakglassPasswordFormProps {
  /** The account the refused sign-in named. */
  username: string;
  /** The password that sign-in was refused with: the one being replaced. */
  currentPassword: string;
  /** The password was changed; the session the server issued with it, or null when the
   *  deployment cannot issue one (no signing key). */
  onChanged: (token: string | null) => void;
  onCancel: () => void;
}

interface RuleErrors {
  newPassword?: string;
  confirmPassword?: string;
}

export function BreakglassPasswordForm({
  username,
  currentPassword: initialCurrent,
  onChanged,
  onCancel,
}: BreakglassPasswordFormProps) {
  const { t } = useTranslation();
  const [currentPassword, setCurrentPassword] = useState(initialCurrent);
  const [newPassword, setNewPassword] = useState("");
  const [confirmPassword, setConfirmPassword] = useState("");
  const [ruleErrors, setRuleErrors] = useState<RuleErrors>({});
  const [error, setError] = useState<string | null>(null);
  const [saving, setSaving] = useState(false);
  const required = useRequiredFields({ currentPassword, newPassword, confirmPassword });

  const checkRules = (): RuleErrors => {
    const found: RuleErrors = {};
    if (newPassword.length < BREAKGLASS_MIN_PASSWORD_LENGTH) {
      found.newPassword = t("breakglassPassword.tooShort", {
        min: BREAKGLASS_MIN_PASSWORD_LENGTH,
      });
    } else if (newPassword === currentPassword) {
      found.newPassword = t("breakglassPassword.unchanged");
    }
    if (confirmPassword !== newPassword) {
      found.confirmPassword = t("breakglassPassword.mismatch");
    }
    return found;
  };

  const handleSubmit = async (e: FormEvent) => {
    e.preventDefault();
    setError(null);
    if (!required.validate()) return;
    const found = checkRules();
    setRuleErrors(found);
    if (found.newPassword || found.confirmPassword) return;
    setSaving(true);
    let token: string | null;
    try {
      token = await changeBreakglassPassword({
        username,
        current_password: currentPassword,
        new_password: newPassword,
      });
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
      return;
    } finally {
      setSaving(false);
    }
    onChanged(token);
  };

  return (
    <form onSubmit={handleSubmit} noValidate style={{ maxWidth: 360 }}>
      <Text size="sm" mb="sm" data-testid="breakglass-password-intro">
        {t("breakglassPassword.intro", { min: BREAKGLASS_MIN_PASSWORD_LENGTH })}
      </Text>
      <FormLayout>
        <FormField size="long">
          <PasswordInput
            label={t("breakglassPassword.currentPassword")}
            {...required.field("currentPassword")}
            value={currentPassword}
            onChange={(e) => setCurrentPassword(e.currentTarget.value)}
            autoComplete="current-password"
            data-testid="breakglass-current-password"
          />
        </FormField>
        <FormField size="long">
          <PasswordInput
            label={t("breakglassPassword.newPassword")}
            {...required.field("newPassword")}
            error={required.errorOf("newPassword") ?? ruleErrors.newPassword}
            value={newPassword}
            onChange={(e) => {
              setNewPassword(e.currentTarget.value);
              setRuleErrors({});
            }}
            autoComplete="new-password"
            data-testid="breakglass-new-password"
          />
        </FormField>
        <FormField size="long">
          <PasswordInput
            label={t("breakglassPassword.confirmPassword")}
            {...required.field("confirmPassword")}
            error={required.errorOf("confirmPassword") ?? ruleErrors.confirmPassword}
            value={confirmPassword}
            onChange={(e) => {
              setConfirmPassword(e.currentTarget.value);
              setRuleErrors({});
            }}
            autoComplete="new-password"
            data-testid="breakglass-confirm-password"
          />
        </FormField>
        {error && (
          <Alert color="red" data-testid="breakglass-password-error">
            {error}
          </Alert>
        )}
        <FormFooter
          action={
            <Button
              type="submit"
              disabled={saving}
              loading={saving}
              data-testid="breakglass-password-submit"
            >
              {t("breakglassPassword.submit")}
            </Button>
          }
        >
          <Button
            type="button"
            variant="default"
            onClick={onCancel}
            disabled={saving}
            data-testid="breakglass-password-cancel"
          >
            {t("loginPage.backToSignIn")}
          </Button>
        </FormFooter>
      </FormLayout>
    </form>
  );
}
