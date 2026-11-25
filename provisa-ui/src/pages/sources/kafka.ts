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

import { fieldChoices, fieldDefault, hintKey } from "./descriptor";

export const KAFKA = "kafka";

// REQ-2114: the hint keys, the protocols and mechanisms, and the default protocol are the
// descriptor's (provisa/api/admin/source_types.py, from provisa/kafka/security.py).
const HINTS = "federationHintsJson";
const PROTOCOL = hintKey(KAFKA, "kafka_security_protocol");
const MECHANISM = hintKey(KAFKA, "kafka_sasl_mechanism");
const SSL_CA = hintKey(KAFKA, "kafka_ssl_ca");

export const KAFKA_SECURITY_PROTOCOLS = fieldChoices(KAFKA, HINTS, PROTOCOL);
export const KAFKA_SASL_MECHANISMS = fieldChoices(KAFKA, HINTS, MECHANISM);
/** The protocol of a plain cluster, which saves no settings. */
const PLAINTEXT = fieldDefault(KAFKA, HINTS, PROTOCOL) as string;

export const kafkaUsesSasl = (fields: Record<string, string>) =>
  fields[PROTOCOL]?.startsWith("SASL_") ?? false;

export const kafkaUsesTls = (fields: Record<string, string>) =>
  fields[PROTOCOL] === "SSL" || fields[PROTOCOL] === "SASL_SSL";

/** The source's federation hints for what the fields say; a plain cluster's are none. */
export function kafkaHints(fields: Record<string, string>): Record<string, string> {
  if (!fields[PROTOCOL] || fields[PROTOCOL] === PLAINTEXT) return {};
  return {
    [PROTOCOL]: fields[PROTOCOL],
    ...(kafkaUsesSasl(fields) && fields[MECHANISM] ? { [MECHANISM]: fields[MECHANISM] } : {}),
    ...(kafkaUsesTls(fields) && fields[SSL_CA] ? { [SSL_CA]: fields[SSL_CA] } : {}),
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
    [PROTOCOL]: hints[PROTOCOL] ?? PLAINTEXT,
    [MECHANISM]: hints[MECHANISM] ?? "",
    [SSL_CA]: hints[SSL_CA] ?? "",
    username: s.username ?? "",
    password: s.passwordRef ?? "",
  };
}
