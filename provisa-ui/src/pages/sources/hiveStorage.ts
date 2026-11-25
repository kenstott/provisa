// Copyright (c) 2026 Kenneth Stott
// Canary: 0787ecdb-14a1-4a07-a827-4aee071a1737
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// A Hive lake's storage is a config choice, not a separate source type: the object store its
// tables live on (Hadoop/local, S3, ADLS, GCS) and that store's credentials are saved in the
// source's mapping, discriminated by mapping.storage (provisa/federation/trino_connectors.py's
// TrinoHiveConnector). REQ-229: a hive_s3 source DECLARES S3 storage.

import { fieldValue, fieldsWhen, mappingKey, mappingKeys } from "./descriptor";
import { heldOf, type AuthSeed } from "./descriptorPick";

export const HIVE = "hive";
export const HIVE_S3 = "hive_s3";
export const HIVE_TYPES: ReadonlySet<string> = new Set([HIVE, HIVE_S3]);

// REQ-2114: the keys, and hive_s3's fixed storage, are the descriptor's
// (provisa/api/admin/source_types.py).
const STORAGE = mappingKey(HIVE, "storage");
const HIVE_S3_STORAGE = fieldValue(HIVE_S3, "mappingJson", STORAGE) as string;
/** Storage on the Hadoop-native filesystem: no object-store credentials. */
const HADOOP = "hadoop";

/** The storage each choice of the form's Storage Authentication select names. */
const STORAGE_OF_AUTH: Record<string, string> = {
  none: HADOOP,
  aws: "s3",
  azure: "adls",
  gcs: "gcs",
};

/** The Storage Authentication choice each saved storage opens in; the aliases are the ones
 * TrinoHiveConnector takes for the same store. */
const AUTH_OF_STORAGE: Record<string, string> = {
  [HADOOP]: "none",
  "": "none",
  hdfs: "none",
  local: "none",
  s3: "aws",
  adls: "azure",
  azure: "azure",
  gcs: "gcs",
};

/** The storage a form's Storage Authentication choice saves. */
export function hiveStorageOf(type: string, authType: string): string {
  if (type === HIVE_S3) return HIVE_S3_STORAGE;
  const storage = STORAGE_OF_AUTH[authType];
  if (!storage) throw new Error(`hive storage authentication ${authType} names no storage`);
  return storage;
}

/** The credential keys a storage is saved with. */
function storageKeys(type: string, storage: string): string[] {
  return type === HIVE_S3
    ? mappingKeys(HIVE_S3).filter((k) => k !== STORAGE)
    : fieldsWhen(HIVE, "mappingJson", STORAGE, storage).map((f) => f.name);
}

/** The mapping a form saves: the storage, and only that storage's credentials, as given. */
export function hiveMapping(
  type: string,
  authType: string,
  fields: Record<string, string>,
): Record<string, string> {
  const storage = hiveStorageOf(type, authType);
  return {
    [STORAGE]: storage,
    ...Object.fromEntries(
      storageKeys(type, storage)
        .filter((k) => fields[k] !== undefined)
        .map((k) => [k, fields[k]]),
    ),
  };
}

/** A saved source's mapping into the form: its storage's choice and credentials. */
export function hiveSeed(type: string, mapping: Record<string, unknown>): AuthSeed {
  // TrinoHiveConnector reads a mapping without a storage as Hadoop-native storage.
  const saved = mapping[STORAGE] === undefined ? HADOOP : String(mapping[STORAGE]).toLowerCase();
  const authType = AUTH_OF_STORAGE[saved];
  if (!authType) throw new Error(`hive storage ${saved} is not one the form offers`);
  const storage = type === HIVE_S3 ? HIVE_S3_STORAGE : STORAGE_OF_AUTH[authType];
  return { authType, authFields: heldOf(storageKeys(type, storage), mapping) };
}
