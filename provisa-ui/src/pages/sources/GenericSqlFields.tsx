// Copyright (c) 2026 Kenneth Stott
// Canary: 224dbf05-6be8-48d2-b8eb-833857278379
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2191: the Generic SQL fields of the source form -- the connection URL, the SQL dialect
// Provisa writes, the user and password, and the capabilities turned on for the active engine,
// which Detect ticks from checks against the real connection. Nothing is saved until the form is.

import { Alert, Button, Checkbox, List, PasswordInput, Select, TextInput } from "@mantine/core";
import { useState } from "react";
import { useTranslation } from "react-i18next";
import { FieldGroup, FormChecks, FormField } from "../../components/form/FormLayout";
import {
  useDetectGenericSql,
  useGenericSqlSetup,
  type GenericSqlDetection,
} from "../../hooks/useGenericSql";
import type { SourceFormFieldsProps } from "./SourceFormFields";
import { ATTACH, attachAllowed, capabilitiesOn, withCapabilities } from "./genericSql";

export function GenericSqlFields({
  form,
  setForm,
  authFields,
  setAuthFields,
}: Pick<SourceFormFieldsProps, "form" | "setForm" | "authFields" | "setAuthFields">) {
  const { t } = useTranslation();
  const { setup } = useGenericSqlSetup(false);
  const { detect, loading: detecting } = useDetectGenericSql();
  const [detection, setDetection] = useState<GenericSqlDetection | null>(null);
  const [detectError, setDetectError] = useState<string | null>(null);
  const url = authFields.url ?? "";
  const dialect = authFields.sql_dialect ?? "";
  const engine = setup?.engine ?? null;
  const on = engine ? capabilitiesOn(authFields, engine) : new Set<string>();
  // A changed URL or dialect is a different source to the checks: press Detect again.
  const setSetting = (key: "url" | "sql_dialect", value: string) => {
    setDetection(null);
    setAuthFields({ ...authFields, [key]: value });
  };
  const toggle = (capability: string, checked: boolean) => {
    if (!engine) return;
    const next = new Set(on);
    if (checked) next.add(capability);
    else next.delete(capability);
    setAuthFields(withCapabilities(authFields, engine, next));
  };
  const runDetect = async () => {
    setDetectError(null);
    try {
      const found = await detect({
        url,
        sqlDialect: dialect,
        username: form.username,
        password: form.password,
      });
      setDetection(found);
      const next = new Set(capabilitiesOn(authFields, found.engine));
      for (const check of found.checks) if (check.passed) next.add(check.capability);
      setAuthFields(withCapabilities(authFields, found.engine, next));
    } catch (e) {
      setDetectError(e instanceof Error ? e.message : String(e));
    }
  };
  return (
    <>
      <FormField size="long" hint={t("genericSqlFields.urlHint")}>
        <TextInput
          required
          label={t("genericSqlFields.url")}
          value={url}
          onChange={(e) => setSetting("url", e.currentTarget.value)}
          placeholder="postgresql+psycopg2://db.example.com:5432/sales"
          data-testid="generic-sql-url-input"
        />
      </FormField>
      <FormField size="medium" help={t("genericSqlFields.dialectHelp")}>
        <Select
          required
          searchable
          label={t("genericSqlFields.dialect")}
          data={(setup?.sqlDialects ?? []).map((d) => ({
            value: d,
            label: d === "ansi" ? t("genericSqlFields.ansi") : d,
          }))}
          value={dialect || null}
          onChange={(v) => setSetting("sql_dialect", v ?? "")}
          comboboxProps={{ withinPortal: true }}
          data-testid="generic-sql-dialect-select"
        />
      </FormField>
      <FieldGroup>
        <FormField section="authentication" size="medium">
          <TextInput
            label={t("genericSqlFields.username")}
            value={form.username}
            onChange={(e) => setForm({ ...form, username: e.currentTarget.value })}
            data-testid="generic-sql-username-input"
          />
        </FormField>
        <FormField section="authentication" size="medium">
          <PasswordInput
            label={t("genericSqlFields.password")}
            value={form.password}
            onChange={(e) => setForm({ ...form, password: e.currentTarget.value })}
            data-testid="generic-sql-password-input"
          />
        </FormField>
      </FieldGroup>
      {engine && (
        <FormChecks>
          {(setup?.capabilities ?? []).map((capability) =>
            capability === ATTACH && !attachAllowed(url) ? null : (
              <FormField
                key={capability}
                size="short"
                help={capability === ATTACH ? t("genericSqlFields.attachUnbuilt") : undefined}
              >
                <Checkbox
                  label={t(`genericSqlFields.capability.${capability}`, { engine })}
                  checked={on.has(capability)}
                  // Live attach is not built yet: the save would be refused.
                  disabled={capability === ATTACH}
                  onChange={(e) => toggle(capability, e.currentTarget.checked)}
                  data-testid={`generic-sql-capability-${capability}`}
                />
              </FormField>
            ),
          )}
        </FormChecks>
      )}
      <div>
        <Button
          variant="light"
          loading={detecting}
          disabled={!url.trim() || !dialect}
          onClick={runDetect}
          data-testid="generic-sql-detect"
        >
          {t("genericSqlFields.detect")}
        </Button>
      </div>
      {detectError && (
        <Alert color="red" data-testid="generic-sql-detect-error">
          {detectError}
        </Alert>
      )}
      {detection && (
        <List size="sm" data-testid="generic-sql-detect-results">
          {detection.checks.map((check) => (
            <List.Item key={check.capability} data-testid={`generic-sql-check-${check.capability}`}>
              {check.passed
                ? t("genericSqlFields.checkPassed", {
                    capability: t(`genericSqlFields.capabilityName.${check.capability}`),
                  })
                : t("genericSqlFields.checkFailed", {
                    capability: t(`genericSqlFields.capabilityName.${check.capability}`),
                    reason: check.reason,
                  })}
            </List.Item>
          ))}
        </List>
      )}
    </>
  );
}
