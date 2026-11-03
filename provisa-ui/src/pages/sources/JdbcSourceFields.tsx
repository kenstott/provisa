// Copyright (c) 2026 Kenneth Stott
// Canary: 224dbf05-6be8-48d2-b8eb-833857278379
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2191: the JDBC fields of the source form -- the connection URL, the SQL dialect
// Provisa writes, the user and password, and the capabilities turned on for the active engine,
// which Detect ticks from checks against the real connection. Nothing is saved until the form is.

import { Alert, Button, Checkbox, List, PasswordInput, Select, TextInput } from "@mantine/core";
import { useState } from "react";
import { useTranslation } from "react-i18next";
import { FieldGroup, FormChecks, FormField } from "../../components/form/FormLayout";
import {
  useDetectJdbcSource,
  useJdbcSourceSetup,
  type JdbcSourceDetection,
} from "../../hooks/useJdbcSource";
import type { SourceFormFieldsProps } from "./SourceFormFields";
import {
  ATTACH,
  attachAllowed,
  capabilitiesOn,
  isJdbc,
  JDBC_CLASS_KEY,
  JDBC_JAR_KEY,
  withCapabilities,
} from "./jdbcSource";

export function JdbcSourceFields({
  form,
  setForm,
  authFields,
  setAuthFields,
}: Pick<SourceFormFieldsProps, "form" | "setForm" | "authFields" | "setAuthFields">) {
  const { t } = useTranslation();
  const { setup } = useJdbcSourceSetup(false);
  const { detect, loading: detecting } = useDetectJdbcSource();
  const [detection, setDetection] = useState<JdbcSourceDetection | null>(null);
  const [detectError, setDetectError] = useState<string | null>(null);
  const url = authFields.url ?? "";
  const dialect = authFields.sql_dialect ?? "";
  const engine = setup?.engine ?? null;
  const on = engine ? capabilitiesOn(authFields, engine) : new Set<string>();
  // A changed URL or dialect is a different source to the checks: press Detect again.
  const jdbc = isJdbc(url);
  const setSetting = (key: string, value: string) => {
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
        jdbcDriverJar: jdbc ? (authFields[JDBC_JAR_KEY] ?? "") : "",
        jdbcDriverClass: jdbc ? (authFields[JDBC_CLASS_KEY] ?? "") : "",
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
      <FormField size="long" hint={t("jdbcSourceFields.urlHint")}>
        <TextInput
          required
          label={t("jdbcSourceFields.url")}
          value={url}
          onChange={(e) => setSetting("url", e.currentTarget.value)}
          placeholder="postgresql+psycopg2://db.example.com:5432/sales"
          data-testid="jdbc-source-url-input"
        />
      </FormField>
      {jdbc && (
        <FieldGroup>
          <FormField size="long" hint={t("jdbcSourceFields.jdbcDriverJarHint")}>
            <TextInput
              required
              label={t("jdbcSourceFields.jdbcDriverJar")}
              value={authFields[JDBC_JAR_KEY] ?? ""}
              onChange={(e) => setSetting(JDBC_JAR_KEY, e.currentTarget.value)}
              placeholder="{files}/drivers/vendor-jdbc.jar"
              data-testid="jdbc-source-jdbc-jar-input"
            />
          </FormField>
          <FormField size="medium">
            <TextInput
              required
              label={t("jdbcSourceFields.jdbcDriverClass")}
              value={authFields[JDBC_CLASS_KEY] ?? ""}
              onChange={(e) => setSetting(JDBC_CLASS_KEY, e.currentTarget.value)}
              placeholder="com.vendor.jdbc.Driver"
              data-testid="jdbc-source-jdbc-class-input"
            />
          </FormField>
        </FieldGroup>
      )}
      <FormField size="medium" help={t("jdbcSourceFields.dialectHelp")}>
        <Select
          required
          searchable
          label={t("jdbcSourceFields.dialect")}
          data={(setup?.sqlDialects ?? []).map((d) => ({
            value: d,
            label: d === "ansi" ? t("jdbcSourceFields.ansi") : d,
          }))}
          value={dialect || null}
          onChange={(v) => setSetting("sql_dialect", v ?? "")}
          comboboxProps={{ withinPortal: true }}
          data-testid="jdbc-source-dialect-select"
        />
      </FormField>
      <FieldGroup>
        <FormField section="authentication" size="medium">
          <TextInput
            label={t("jdbcSourceFields.username")}
            value={form.username}
            onChange={(e) => setForm({ ...form, username: e.currentTarget.value })}
            data-testid="jdbc-source-username-input"
          />
        </FormField>
        <FormField section="authentication" size="medium">
          <PasswordInput
            label={t("jdbcSourceFields.password")}
            value={form.password}
            onChange={(e) => setForm({ ...form, password: e.currentTarget.value })}
            data-testid="jdbc-source-password-input"
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
                help={capability === ATTACH ? t("jdbcSourceFields.attachUnbuilt") : undefined}
              >
                <Checkbox
                  label={t(`jdbcSourceFields.capability.${capability}`, { engine })}
                  checked={on.has(capability)}
                  // Live attach is not built yet: the save would be refused.
                  disabled={capability === ATTACH}
                  onChange={(e) => toggle(capability, e.currentTarget.checked)}
                  data-testid={`jdbc-source-capability-${capability}`}
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
          data-testid="jdbc-source-detect"
        >
          {t("jdbcSourceFields.detect")}
        </Button>
      </div>
      {detectError && (
        <Alert color="red" data-testid="jdbc-source-detect-error">
          {detectError}
        </Alert>
      )}
      {detection && (
        <List size="sm" data-testid="jdbc-source-detect-results">
          {detection.checks.map((check) => (
            <List.Item key={check.capability} data-testid={`jdbc-source-check-${check.capability}`}>
              {check.passed
                ? t("jdbcSourceFields.checkPassed", {
                    capability: t(`jdbcSourceFields.capabilityName.${check.capability}`),
                  })
                : t("jdbcSourceFields.checkFailed", {
                    capability: t(`jdbcSourceFields.capabilityName.${check.capability}`),
                    reason: check.reason,
                  })}
            </List.Item>
          ))}
        </List>
      )}
    </>
  );
}
