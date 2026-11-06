// Copyright (c) 2026 Kenneth Stott
// Canary: dddea9d6-f0a6-4f82-9787-37d20f14cd64
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-147: a Kafka source's TLS and SASL settings. Each protocol shows exactly the fields it uses.

import { PasswordInput, Select, Textarea, TextInput } from "@mantine/core";
import { useTranslation } from "react-i18next";
import { FieldGroup, FormField } from "../../components/form/FormLayout";
import {
  KAFKA_SASL_MECHANISMS,
  KAFKA_SECURITY_PROTOCOLS,
  kafkaUsesSasl,
  kafkaUsesTls,
} from "./kafka";

interface Props {
  fields: Record<string, string>;
  setFields: (fields: Record<string, string>) => void;
}

export function KafkaFields({ fields, setFields }: Props) {
  const { t } = useTranslation();
  const set = (key: string, value: string) => setFields({ ...fields, [key]: value });
  return (
    <>
      <FormField
        section="authentication"
        size="medium"
        help={t("kafkaFields.securityProtocolHint")}
      >
        <Select
          label={t("kafkaFields.securityProtocol")}
          value={fields.kafka_security_protocol ?? "PLAINTEXT"}
          onChange={(v) => {
            if (v) set("kafka_security_protocol", v);
          }}
          data={KAFKA_SECURITY_PROTOCOLS}
          allowDeselect={false}
          data-testid="kafka-security-protocol-select"
        />
      </FormField>
      {kafkaUsesSasl(fields) && (
        <>
          <FormField section="authentication" size="medium">
            <Select
              required
              label={t("kafkaFields.saslMechanism")}
              value={fields.kafka_sasl_mechanism || null}
              onChange={(v) => {
                if (v) set("kafka_sasl_mechanism", v);
              }}
              data={KAFKA_SASL_MECHANISMS}
              allowDeselect={false}
              data-testid="kafka-sasl-mechanism-select"
            />
          </FormField>
          <FieldGroup>
            <FormField section="authentication" size="medium" help={t("kafkaFields.usernameHint")}>
              <TextInput
                required
                label={t("kafkaFields.username")}
                value={fields.username ?? ""}
                onChange={(e) => set("username", e.currentTarget.value)}
                data-testid="kafka-username-input"
              />
            </FormField>
            <FormField section="authentication" size="medium" help={t("kafkaFields.passwordHint")}>
              <PasswordInput
                required
                label={t("kafkaFields.password")}
                value={fields.password ?? ""}
                onChange={(e) => set("password", e.currentTarget.value)}
                data-testid="kafka-password-input"
              />
            </FormField>
          </FieldGroup>
        </>
      )}
      {kafkaUsesTls(fields) && (
        <FormField section="authentication" size="full" help={t("kafkaFields.caCertificateHint")}>
          <Textarea
            label={t("kafkaFields.caCertificate")}
            value={fields.kafka_ssl_ca ?? ""}
            onChange={(e) => set("kafka_ssl_ca", e.currentTarget.value)}
            placeholder="-----BEGIN CERTIFICATE-----"
            autosize
            minRows={3}
            maxRows={8}
            data-testid="kafka-ssl-ca-input"
          />
        </FormField>
      )}
    </>
  );
}
