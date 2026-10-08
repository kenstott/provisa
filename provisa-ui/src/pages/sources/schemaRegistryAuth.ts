// Copyright (c) 2026 Kenneth Stott
// Canary: 8453b5ee-b8c0-448d-82ee-9da9def36814
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1951: schema registry authentication methods, their fields, and the save payload.

import type { CdcState } from "./SourceFormFields";

export const REGISTRY_AUTH_METHODS = ["none", "basic", "bearer", "mtls"] as const;
export type RegistryAuthMethod = (typeof REGISTRY_AUTH_METHODS)[number];

export type RegistryFieldKey =
  | "schemaRegistryUsername"
  | "schemaRegistryPassword"
  | "schemaRegistryToken"
  | "schemaRegistryClientCert"
  | "schemaRegistryClientKey";

export const METHOD_FIELDS: Record<RegistryAuthMethod, RegistryFieldKey[]> = {
  none: [],
  basic: ["schemaRegistryUsername", "schemaRegistryPassword"],
  bearer: ["schemaRegistryToken"],
  mtls: ["schemaRegistryClientCert", "schemaRegistryClientKey"],
};

export const ALL_FIELDS: RegistryFieldKey[] = [
  "schemaRegistryUsername",
  "schemaRegistryPassword",
  "schemaRegistryToken",
  "schemaRegistryClientCert",
  "schemaRegistryClientKey",
];

// Save payload: only the chosen method's fields carry a value; the CA path goes with any method.
export function schemaRegistryAuthPayload(cdc: CdcState) {
  if (!cdc.schemaRegistryUrl) {
    return {
      schemaRegistryAuth: "none",
      schemaRegistryUsername: null,
      schemaRegistryPassword: null,
      schemaRegistryToken: null,
      schemaRegistryClientCert: null,
      schemaRegistryClientKey: null,
      schemaRegistryCa: null,
    };
  }
  const used = METHOD_FIELDS[cdc.schemaRegistryAuth as RegistryAuthMethod];
  const value = (key: RegistryFieldKey) => (used.includes(key) ? cdc[key] || null : null);
  return {
    schemaRegistryAuth: cdc.schemaRegistryAuth,
    schemaRegistryUsername: value("schemaRegistryUsername"),
    schemaRegistryPassword: value("schemaRegistryPassword"),
    schemaRegistryToken: value("schemaRegistryToken"),
    schemaRegistryClientCert: value("schemaRegistryClientCert"),
    schemaRegistryClientKey: value("schemaRegistryClientKey"),
    schemaRegistryCa: cdc.schemaRegistryCa || null,
  };
}
