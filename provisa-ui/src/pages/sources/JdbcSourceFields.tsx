// Copyright (c) 2026 Kenneth Stott
// Canary: 224dbf05-6be8-48d2-b8eb-833857278379
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2191: the JDBC fields of the source form -- the driver first: a standard one, whose
// connection URL the form builds from host, port and database (or file), or the admin's own jar,
// uploaded with the source beside its class and jdbc: URL. Then the user and password, the SQL
// dialect Provisa writes, and the capabilities turned on for the active engine, which Detect ticks
// from checks against the real connection. Nothing is saved until the form is.

import { Alert, Button, Checkbox, List, PasswordInput, Select, TextInput } from "@mantine/core";
import { useState } from "react";
import { useTranslation } from "react-i18next";
import { FieldGroup, FormChecks, FormField } from "../../components/form/FormLayout";
import {
  useDetectJdbcSource,
  useJdbcSourceSetup,
  type JdbcDriver,
  type JdbcSourceDetection,
} from "../../hooks/useJdbcSource";
import { SOURCE_TYPES } from "./constants";
import type { SourceFormFieldsProps } from "./SourceFormFields";
import { SourceFileUpload } from "./SourceFileUpload";
import {
  ATTACH,
  buildUrl,
  capabilitiesOn,
  DRIVER_KEY,
  driverFilled,
  driverOf,
  JAR_DRIVER,
  JDBC_CLASS_KEY,
  JDBC_JAR_KEY,
  urlParts,
  withCapabilities,
  type UrlParts,
} from "./jdbcSource";

const TYPES = new Map(SOURCE_TYPES.map((s) => [s.value, s]));

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
  const picked = driverOf(authFields);
  const jar = picked === JAR_DRIVER;
  const driver = setup?.drivers.find((d) => d.backend === picked) ?? null;
  // Live attach is offered where the active engine attaches the driver's built-in type; never
  // for a jar.
  const offered = (capability: string) => capability !== ATTACH || (driver?.attach ?? false);
  // A changed connection or dialect is a different source to the checks: press Detect again.
  const setSettings = (changed: Record<string, string>) => {
    setDetection(null);
    setAuthFields({ ...authFields, ...changed });
  };
  const pickDriver = (next: string) => {
    const standard = setup?.drivers.find((d) => d.backend === next);
    setSettings({
      [DRIVER_KEY]: next,
      url: standard
        ? buildUrl(standard, { host: "", port: "", database: "", file: "", query: "" })
        : "",
    });
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
        jdbcDriverJar: jar ? (authFields[JDBC_JAR_KEY] ?? "") : "",
        jdbcDriverClass: jar ? (authFields[JDBC_CLASS_KEY] ?? "") : "",
      });
      setDetection(found);
      const next = new Set(capabilitiesOn(authFields, found.engine));
      for (const check of found.checks) {
        if (check.passed && offered(check.capability)) next.add(check.capability);
      }
      setAuthFields(withCapabilities(authFields, found.engine, next));
    } catch (e) {
      setDetectError(e instanceof Error ? e.message : String(e));
    }
  };
  return (
    <>
      <FormField size="medium">
        <Select
          required
          searchable
          label={t("jdbcSourceFields.driver")}
          data={[
            ...(setup?.drivers ?? []).map((d) => ({
              value: d.backend,
              label: TYPES.get(d.sourceType)?.label ?? d.backend,
            })),
            { value: JAR_DRIVER, label: t("jdbcSourceFields.jarDriver") },
          ]}
          value={picked || null}
          onChange={(v) => v && pickDriver(v)}
          allowDeselect={false}
          comboboxProps={{ withinPortal: true }}
          data-testid="jdbc-source-driver-select"
        />
      </FormField>
      {driver && (
        <StandardDriverFields
          driver={driver}
          url={url}
          setUrl={(next) => setSettings({ url: next })}
        />
      )}
      {jar && (
        <>
          <FieldGroup>
            <FormField size="long" hint={t("jdbcSourceFields.jdbcDriverJarHint")}>
              <TextInput
                required
                readOnly
                label={t("jdbcSourceFields.jdbcDriverJar")}
                value={authFields[JDBC_JAR_KEY] ?? ""}
                placeholder={t("jdbcSourceFields.jdbcDriverJarPlaceholder")}
                data-testid="jdbc-source-jdbc-jar-input"
              />
            </FormField>
            <SourceFileUpload
              sourceId={form.id}
              type="jdbc"
              accept=".jar"
              uploaded={!!authFields[JDBC_JAR_KEY]}
              onUploaded={(path) => setSettings({ [JDBC_JAR_KEY]: path })}
            />
          </FieldGroup>
          <FieldGroup>
            <FormField size="medium">
              <TextInput
                required
                label={t("jdbcSourceFields.jdbcDriverClass")}
                value={authFields[JDBC_CLASS_KEY] ?? ""}
                onChange={(e) => setSettings({ [JDBC_CLASS_KEY]: e.currentTarget.value })}
                placeholder="com.vendor.jdbc.Driver"
                data-testid="jdbc-source-jdbc-class-input"
              />
            </FormField>
            <FormField size="long" hint={t("jdbcSourceFields.jdbcUrlHint")}>
              <TextInput
                required
                label={t("jdbcSourceFields.jdbcUrl")}
                value={url}
                onChange={(e) => setSettings({ url: e.currentTarget.value })}
                placeholder="jdbc:vendor://db.example.com:1521/sales"
                data-testid="jdbc-source-url-input"
              />
            </FormField>
          </FieldGroup>
        </>
      )}
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
          onChange={(v) => setSettings({ sql_dialect: v ?? "" })}
          comboboxProps={{ withinPortal: true }}
          data-testid="jdbc-source-dialect-select"
        />
      </FormField>
      {engine && (
        <FormChecks>
          {(setup?.capabilities ?? []).filter(offered).map((capability) => (
            <FormField key={capability} size="short">
              <Checkbox
                label={t(`jdbcSourceFields.capabilityName.${capability}`)}
                checked={on.has(capability)}
                onChange={(e) => toggle(capability, e.currentTarget.checked)}
                data-testid={`jdbc-source-capability-${capability}`}
              />
            </FormField>
          ))}
        </FormChecks>
      )}
      <div>
        <Button
          variant="light"
          loading={detecting}
          disabled={!driverFilled(authFields, driver) || !dialect}
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
          {detection.checks
            .filter((check) => offered(check.capability))
            .map((check) => (
              <List.Item
                key={check.capability}
                data-testid={`jdbc-source-check-${check.capability}`}
              >
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

/** A standard driver's host, port and database, or its file, from which the URL is built. */
function StandardDriverFields({
  driver,
  url,
  setUrl,
}: {
  driver: JdbcDriver;
  url: string;
  setUrl: (url: string) => void;
}) {
  const { t } = useTranslation();
  const parts = urlParts(url, driver);
  const set = (changed: Partial<UrlParts>) => setUrl(buildUrl(driver, { ...parts, ...changed }));
  if (driver.file) {
    return (
      <FormField size="long" hint={t("jdbcSourceFields.fileHint")}>
        <TextInput
          required
          label={t("jdbcSourceFields.file")}
          value={parts.file}
          onChange={(e) => set({ file: e.currentTarget.value })}
          placeholder={`{files}/shop.${driver.backend === "duckdb" ? "duckdb" : "db"}`}
          data-testid="jdbc-source-file-input"
        />
      </FormField>
    );
  }
  const defaultPort = TYPES.get(driver.sourceType)?.defaultPort;
  return (
    <FieldGroup>
      <FormField size="medium">
        <TextInput
          required
          label={t("jdbcSourceFields.host")}
          value={parts.host}
          onChange={(e) => set({ host: e.currentTarget.value })}
          placeholder="db.example.com"
          data-testid="jdbc-source-host-input"
        />
      </FormField>
      <FormField size="short">
        <TextInput
          label={t("jdbcSourceFields.port")}
          inputMode="numeric"
          value={parts.port}
          onChange={(e) => set({ port: e.currentTarget.value.replace(/\D/g, "") })}
          placeholder={defaultPort ? String(defaultPort) : undefined}
          data-testid="jdbc-source-port-input"
        />
      </FormField>
      <FormField size="medium">
        <TextInput
          label={t("jdbcSourceFields.database")}
          value={parts.database}
          onChange={(e) => set({ database: e.currentTarget.value })}
          data-testid="jdbc-source-database-input"
        />
      </FormField>
    </FieldGroup>
  );
}
