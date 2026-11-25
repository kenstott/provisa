// Copyright (c) 2026 Kenneth Stott
// Canary: 2f81fd0b-b3a9-4daf-a668-e2a692a76d33
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2114 (amended 2026-11-24): the Sources picker is the server's source-type descriptor
// (provisa/api/admin/source_types.py, written to src/generated/sourceTypes.json), which Polly's
// source_types tool reads. These fail when the picker, or a form helper's keys, and the
// descriptor disagree.

import { describe, expect, it } from "vitest";

import generated from "../../../generated/sourceTypes.json";
import { ATHENA_HINTS } from "../athena";
import { CLOUDOPS_CLOUDS } from "../cloudops";
import { CODE_SOURCE_TYPES, NAMED_ENTRY, PACKAGE_KIND } from "../codeSource";
import {
  BRAND_AUTH,
  BRAND_CARRIER,
  BRAND_SETUP,
  SHOPIFY_API_VERSION,
  SOURCE_TYPES,
  TYPE_ALIAS,
} from "../constants";
import { ICEBERG_CATALOG_FIELDS } from "../icebergCatalog";
import { OBJECT_STORE_HINT_KEYS } from "../objectStoreHints";
import { PROFILER_DEFAULTS } from "../profilerMapping";
import { SALESFORCE_AUTH_FIELDS } from "../salesforce";
import { backendType } from "../sourceHelpers";

type Descriptor = (typeof generated.types)[number];
type DescriptorField = Descriptor["fields"][number] & {
  required?: boolean;
  secret?: boolean;
  default?: unknown;
};

const byName = new Map<string, Descriptor>(generated.types.map((t) => [t.name, t]));
const descriptor = (name: string): Descriptor => {
  const found = byName.get(name);
  if (!found) throw new Error(`${name} is not in the descriptor`);
  return found;
};
const fieldsOf = (name: string): DescriptorField[] => descriptor(name).fields as DescriptorField[];
const field = (type: string, name: string): DescriptorField => {
  const found = fieldsOf(type).find((f) => f.name === name);
  if (!found) throw new Error(`${type} has no field ${name}`);
  return found;
};

describe("the picker and the descriptor", () => {
  it("offers exactly the descriptor's types, in its order, with its labels", () => {
    expect(SOURCE_TYPES.map((s) => [s.value, s.label, s.category, s.defaultPort])).toEqual(
      generated.types.map((t) => [t.name, t.label, t.category, t.default_port]),
    );
  });

  it("registers each type as the descriptor's backend type", () => {
    for (const t of SOURCE_TYPES) {
      // Kaggle is staged by its own form into a files source (KaggleFormSection).
      if (t.value === "kaggle") continue;
      expect(backendType(t.value), t.value).toBe(descriptor(t.value).backend_type);
    }
  });

  it("knows every UI brand, carried as the descriptor says", () => {
    const brands = new Set([
      ...Object.keys(BRAND_CARRIER),
      ...Object.keys(BRAND_AUTH),
      ...Object.keys(BRAND_SETUP),
    ]);
    for (const brand of brands) {
      expect(descriptor(brand).brand, brand).toBe(brand);
      expect(descriptor(brand).carrier, brand).toBe(BRAND_CARRIER[brand]);
    }
    expect(TYPE_ALIAS).toEqual({ graphql: "graphql_remote", grpc: "grpc_remote" });
  });

  it("asks each brand for its registry's setup", () => {
    for (const [brand, setup] of Object.entries(BRAND_SETUP)) {
      const own = fieldsOf(brand).filter((f) => f.at === "setup");
      expect(
        setup.map((f) => f.name),
        brand,
      ).toEqual(own.map((f) => f.name));
      for (const f of setup) {
        const theirs = own.find((o) => o.name === f.name)!;
        expect(f.defaultValue ?? null, `${brand}.${f.name}`).toBe(theirs.default ?? null);
      }
    }
    expect(SHOPIFY_API_VERSION).toBe(field("shopify", "api_version").default);
  });

  it("offers the descriptor's code types", () => {
    const code = generated.types.filter((t) => t.code !== null);
    expect([...CODE_SOURCE_TYPES].sort()).toEqual(code.map((t) => t.name).sort());
    for (const t of code) {
      expect(PACKAGE_KIND[t.name], t.name).toBe(t.code!.package_kind);
      // A built-in host (Spark) is joined, never entered.
      if (!t.code!.builtin_host) expect(NAMED_ENTRY.has(t.name), t.name).toBe(t.code!.named_entry);
    }
  });
});

describe("the form helpers' keys are the descriptor's", () => {
  it("athena", () => {
    for (const key of ATHENA_HINTS) expect(descriptor("athena").federation_hints).toContain(key);
  });

  it("an object store's credentials", () => {
    for (const keys of Object.values(OBJECT_STORE_HINT_KEYS))
      for (const key of keys) expect(descriptor("csv").federation_hints).toContain(key);
    expect(field("csv", "secret_access_key").secret).toBe(true);
    expect(field("csv", "gcs_secret_key").secret).toBe(true);
    expect(field("csv", "azure_account_key").secret).toBe(true);
  });

  it("an iceberg catalog", () => {
    for (const entries of Object.values(ICEBERG_CATALOG_FIELDS))
      for (const { key } of entries) expect(descriptor("iceberg").federation_hints).toContain(key);
  });

  it("cloud inventory, with its secrets marked", () => {
    for (const entries of Object.values(CLOUDOPS_CLOUDS))
      for (const f of entries) {
        expect(field("cloudops", f.key).required ?? false, f.key).toBe(f.required);
        expect(field("cloudops", f.key).secret ?? false, f.key).toBe(f.secret ?? false);
      }
  });

  it("salesforce", () => {
    for (const keys of Object.values(SALESFORCE_AUTH_FIELDS))
      for (const key of keys) expect(descriptor("salesforce").mapping_json).toContain(key);
  });

  it("the data profiler", () => {
    for (const [key, value] of Object.entries(PROFILER_DEFAULTS))
      expect(String(field("data_profiler", key).default), key).toBe(value);
  });
});
