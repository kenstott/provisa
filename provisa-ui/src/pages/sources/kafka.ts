// Copyright (c) 2026 Kenneth Stott
// Canary: 4b9aea9f-2b03-4935-85c8-dff4a5b95a51
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-147: how a Kafka source reaches a secured cluster. The protocol, mechanism and CA live in the
// source's federation hints (provisa/kafka/security.py reads them); the username and password in
// its standard fields, as entered: the value, or a ${secret:...} reference, resolved at use.

export const KAFKA = "kafka";

export const KAFKA_SECURITY_PROTOCOLS = ["PLAINTEXT", "SSL", "SASL_PLAINTEXT", "SASL_SSL"];
export const KAFKA_SASL_MECHANISMS = ["PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512"];

export const kafkaUsesSasl = (fields: Record<string, string>) =>
  fields.kafka_security_protocol?.startsWith("SASL_") ?? false;

export const kafkaUsesTls = (fields: Record<string, string>) =>
  fields.kafka_security_protocol === "SSL" || fields.kafka_security_protocol === "SASL_SSL";

/** The source's federation hints for what the fields say; a plain cluster's are none. */
export function kafkaHints(fields: Record<string, string>): Record<string, string> {
  if (!fields.kafka_security_protocol || fields.kafka_security_protocol === "PLAINTEXT") return {};
  return {
    kafka_security_protocol: fields.kafka_security_protocol,
    ...(kafkaUsesSasl(fields) && fields.kafka_sasl_mechanism
      ? { kafka_sasl_mechanism: fields.kafka_sasl_mechanism }
      : {}),
    ...(kafkaUsesTls(fields) && fields.kafka_ssl_ca ? { kafka_ssl_ca: fields.kafka_ssl_ca } : {}),
  };
}

/** The source's standard username and password. A cluster with no SASL holds neither. */
export function kafkaCredentials(fields: Record<string, string>): {
  username: string;
  password: string;
} {
  return kafkaUsesSasl(fields)
    ? { username: fields.username ?? "", password: fields.password ?? "" }
    : { username: "", password: "" };
}

/** The fields a registered source fills in. */
export function kafkaFieldsFromSource(s: {
  federationHintsJson?: string | null;
  username?: string | null;
  passwordRef?: string | null;
}): Record<string, string> {
  const hints = s.federationHintsJson
    ? (JSON.parse(s.federationHintsJson) as Record<string, string>)
    : {};
  return {
    kafka_security_protocol: hints.kafka_security_protocol ?? "PLAINTEXT",
    kafka_sasl_mechanism: hints.kafka_sasl_mechanism ?? "",
    kafka_ssl_ca: hints.kafka_ssl_ca ?? "",
    username: s.username ?? "",
    password: s.passwordRef ?? "",
  };
}
