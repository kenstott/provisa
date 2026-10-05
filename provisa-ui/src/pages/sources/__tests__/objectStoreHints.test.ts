// Copyright (c) 2026 Kenneth Stott
// Canary: 3d95332c-ef32-4b3a-8c85-b20c49963810
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-990: a file source's object-store credentials are federation_hints. The one mapping the form
// saves with and reopens with round-trips every field it shows, the S3 region included.

import { describe, expect, it } from "vitest";
import {
  objectStoreFields,
  objectStoreHints,
  objectStoreOf,
  OBJECT_STORE_HINT_KEYS,
} from "../objectStoreHints";

describe("objectStoreOf", () => {
  it.each([
    ["s3://bucket/k.csv", "S3"],
    ["gs://bucket/k.csv", "GCS"],
    ["gcs://bucket/k.csv", "GCS"],
    ["azure://container/k.parquet", "AZURE"],
    ["abfss://c@acct.dfs.core.windows.net/k.csv", "AZURE"],
    ["wasbs://c@acct.blob.core.windows.net/k.csv", "AZURE"],
  ])("%s is in %s", (path, store) => {
    expect(objectStoreOf(path)).toBe(store);
  });

  it.each(["./demo/files/customers.csv", "/data/k.parquet", "sftp://h/k.csv", "", null])(
    "%s is in no object store",
    (path) => {
      expect(objectStoreOf(path)).toBeNull();
    },
  );
});

describe("object-store hints round-trip", () => {
  it("saves every filled S3 field, the region included, and reopens them", () => {
    const fields = {
      access_key_id: "${secret:aws_key}",
      secret_access_key: "${secret:aws_secret}",
      region: "eu-west-1",
      endpoint: "",
    };
    const hints = objectStoreHints("S3", fields);
    expect(hints).toEqual({
      access_key_id: "${secret:aws_key}",
      secret_access_key: "${secret:aws_secret}",
      region: "eu-west-1",
    });
    expect(objectStoreFields("S3", hints)).toEqual(hints);
  });

  it("saves the GCS HMAC keys and the Azure account under their own hints", () => {
    const gcs = objectStoreHints("GCS", { gcs_access_id: "GOOG1", gcs_secret_key: "${secret:h}" });
    expect(gcs).toEqual({ gcs_access_id: "GOOG1", gcs_secret_key: "${secret:h}" });
    const azure = objectStoreHints("AZURE", {
      azure_account_name: "acct",
      azure_account_key: "${secret:k}",
      access_key_id: "not an azure field",
    });
    expect(azure).toEqual({ azure_account_name: "acct", azure_account_key: "${secret:k}" });
  });

  it("reopens only the fields of the store the path names", () => {
    const hints = { access_key_id: "a", gcs_access_id: "g", credentials_path: "/sa.json" };
    expect(objectStoreFields("GCS", hints)).toEqual({ gcs_access_id: "g" });
    expect(Object.keys(OBJECT_STORE_HINT_KEYS)).toEqual(["S3", "GCS", "AZURE"]);
  });
});
