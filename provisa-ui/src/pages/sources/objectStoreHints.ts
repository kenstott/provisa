// Copyright (c) 2026 Kenneth Stott
// Canary: 33472449-e581-44bc-89f6-30e69b3dd159
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-990: a file source's object-store credentials, as federation_hints. One mapping, used both
// when the source form is saved and when a saved source is opened for editing, so a field the
// form shows is a field that is saved and comes back. Mirrors the backend's
// provisa/federation/singlestore_pipeline.py (object_store, _STORE_HINTS).

import generated from "../../generated/sourceTypes.json";
import { fieldsWhen } from "./descriptor";

export type ObjectStore = "S3" | "GCS" | "AZURE";

const GCS_SCHEMES = new Set(["gs", "gcs"]);
const AZURE_SCHEMES = new Set(["az", "azure", "abfs", "abfss", "wasb", "wasbs"]);

/** Source types whose file location may be in an object store, read by its own credentials
 * (REQ-1997: the file databases too, attached read-only there). */
export const CLOUD_FILE_TYPES = new Set<string>(generated.families.cloudFileTypes);

/** The object store a file location is in, or null for a local path or another transport. */
export function objectStoreOf(path: string | null | undefined): ObjectStore | null {
  const match = /^([a-z0-9+.-]+):\/\//i.exec(path ?? "");
  if (!match) return null;
  const scheme = match[1].toLowerCase();
  if (scheme === "s3") return "S3";
  if (GCS_SCHEMES.has(scheme)) return "GCS";
  if (AZURE_SCHEMES.has(scheme)) return "AZURE";
  return null;
}

/** The location each store's credentials are stated for in the descriptor (`path=<scheme>`). */
const STORE_SCHEME: Record<ObjectStore, string> = { S3: "s3://", GCS: "gs://", AZURE: "az://" };

/** The federation_hints each store's credentials are saved under (secrets as ${secret:…}): the
 * descriptor's (REQ-2114, provisa/api/admin/source_types.py OBJECT_STORE_HINTS). */
export const OBJECT_STORE_HINT_KEYS: Record<ObjectStore, readonly string[]> = Object.fromEntries(
  (Object.keys(STORE_SCHEME) as ObjectStore[]).map((store) => [
    store,
    fieldsWhen("csv", "federationHintsJson", "path", STORE_SCHEME[store]).map((f) => f.name),
  ]),
) as unknown as Record<ObjectStore, readonly string[]>;

/** The hints to save for ``store`` from the form's fields: every filled field, nothing else. */
export function objectStoreHints(
  store: ObjectStore,
  fields: Record<string, string>,
): Record<string, string> {
  return Object.fromEntries(
    OBJECT_STORE_HINT_KEYS[store].filter((k) => fields[k]).map((k) => [k, fields[k]]),
  );
}

/** The form's fields for ``store`` from a saved source's hints (the inverse of the above). */
export function objectStoreFields(
  store: ObjectStore,
  hints: Record<string, string>,
): Record<string, string> {
  return Object.fromEntries(
    OBJECT_STORE_HINT_KEYS[store].filter((k) => hints[k]).map((k) => [k, hints[k]]),
  );
}
