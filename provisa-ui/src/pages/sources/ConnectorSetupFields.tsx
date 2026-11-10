// Copyright (c) 2026 Kenneth Stott
// Canary: 72d8f3a6-1b04-4c79-8e52-b9c04a7d1f23
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// The setup form of a connector the organization offers: where the host its operator runs is, the
// pairing code that host printed, and each value its code requires. A required secret is picked
// from the organization's secrets; its value is never entered here.

// Requirements: REQ-2221, REQ-2225

import { useEffect, useState } from "react";
import { PasswordInput, Select, Text, Textarea, TextInput } from "@mantine/core";
import { useTranslation } from "react-i18next";
import { fetchSecrets } from "../../api/secrets";
import type { Connector, ConnectorRequirement } from "../../api/connectors";
import { FieldGroup, FormField } from "../../components/form/FormLayout";
import { useAuth } from "../../context/AuthContext";
import { requiredField } from "./connector";

interface Props {
  connector: Connector;
  fields: Record<string, string>;
  setFields: (fields: Record<string, string>) => void;
}

/** A required value that is not a secret: read-only when its connector fixes it. */
function RequiredValue({
  required,
  value,
  onChange,
}: {
  required: ConnectorRequirement;
  value: string;
  onChange: (value: string) => void;
}) {
  const { t } = useTranslation();
  const fixed = required.fixed_value !== undefined;
  const common = {
    required: !fixed,
    readOnly: fixed,
    label: required.name,
    value: required.fixed_value ?? value,
    onChange: (e: { currentTarget: { value: string } }) => onChange(e.currentTarget.value),
    "data-testid": `connector-require-${required.name}`,
  };
  return (
    <FormField
      size={required.multiline ? "full" : "medium"}
      hint={fixed ? t("connectorSetup.fixedHint") : undefined}
    >
      {required.multiline ? (
        <Textarea
          {...common}
          autosize
          minRows={6}
          maxRows={24}
          styles={{ input: { fontFamily: "var(--mantine-font-family-monospace)" } }}
        />
      ) : (
        <TextInput {...common} />
      )}
    </FormField>
  );
}

/** A saved connector source's values: each one its code requires, as they are kept. */
export function ConnectorSourceValues({ connector, fields, setFields }: Props) {
  const settings = JSON.parse(fields.code_connector_settings || "{}") as Record<string, unknown>;
  return (
    <>
      {connector.requires
        .filter((r) => !r.secret)
        .map((r) => (
          <RequiredValue
            key={r.name}
            required={r}
            value={String(settings[r.name] ?? "")}
            onChange={(value) =>
              setFields({
                ...fields,
                code_connector_settings: JSON.stringify({ ...settings, [r.name]: value }),
              })
            }
          />
        ))}
    </>
  );
}

export function ConnectorSetupFields({ connector, fields, setFields }: Props) {
  const { t } = useTranslation();
  const { activeOrgId } = useAuth();
  const [orgSecrets, setOrgSecrets] = useState<string[]>([]);
  const [secretsFailed, setSecretsFailed] = useState("");
  const needsSecrets = connector.requires.some((r) => r.secret);

  useEffect(() => {
    if (!activeOrgId || !needsSecrets) return;
    let live = true;
    fetchSecrets(activeOrgId, "org")
      .then((s) => live && setOrgSecrets(s.secrets.map((x) => x.name)))
      .catch((e) => live && setSecretsFailed(String(e instanceof Error ? e.message : e)));
    return () => {
      live = false;
    };
  }, [activeOrgId, needsSecrets]);

  const set = (key: string, value: string) => setFields({ ...fields, [key]: value });

  return (
    <>
      <FormField size="long">
        <Text size="sm" data-testid="connector-identity">
          {t("connectorSetup.identity", { name: connector.name, ref: connector.ref })}
        </Text>
      </FormField>
      <FormField size="long" hint={t("connectorSetup.imageHint")}>
        <TextInput
          label={t("connectorSetup.image")}
          value={`${connector.image}@${connector.digest}`}
          readOnly
          data-testid="connector-image"
        />
      </FormField>
      <FieldGroup>
        <FormField size="medium" hint={t("connectorSetup.hostHint")}>
          <TextInput
            required
            label={t("connectorSetup.host")}
            value={fields.connector_host ?? ""}
            onChange={(e) => set("connector_host", e.currentTarget.value)}
            data-testid="connector-host"
          />
        </FormField>
        <FormField size="short">
          <TextInput
            required
            label={t("connectorSetup.port")}
            value={fields.connector_port ?? ""}
            onChange={(e) => set("connector_port", e.currentTarget.value)}
            data-testid="connector-port"
          />
        </FormField>
      </FieldGroup>
      <FormField size="long" help={t("connectorSetup.pairingHint")}>
        <PasswordInput
          required
          label={t("connectorSetup.pairing")}
          value={fields.connector_pairing_code ?? ""}
          onChange={(e) => set("connector_pairing_code", e.currentTarget.value)}
          data-testid="connector-pairing"
        />
      </FormField>
      {connector.requires.map((r) =>
        r.secret ? (
          <FormField key={r.name} size="medium" hint={t("connectorSetup.secretHint")}>
            <Select
              required
              searchable
              label={r.name}
              data={orgSecrets}
              value={fields[requiredField(r.name)] ?? null}
              onChange={(value) => set(requiredField(r.name), value ?? "")}
              error={secretsFailed || undefined}
              data-testid={`connector-require-${r.name}`}
            />
          </FormField>
        ) : (
          <RequiredValue
            key={r.name}
            required={r}
            value={fields[requiredField(r.name)] ?? ""}
            onChange={(value) => set(requiredField(r.name), value)}
          />
        ),
      )}
    </>
  );
}
