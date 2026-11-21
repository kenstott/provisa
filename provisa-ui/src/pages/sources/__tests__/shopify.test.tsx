// Copyright (c) 2026 Kenneth Stott
// Canary: 5b8e2c14-9f37-4a06-b1d2-7c4e0a6f9d83
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1923: Shopify is in the source pick list, carried by the remote GraphQL source. Its setup
// asks where the store is -- the store name and the Admin API version -- and for its token.

import { readFileSync } from "node:fs";
import { resolve } from "node:path";
import { describe, expect, it, vi } from "vitest";
import { fireEvent, renderInForm, screen } from "../../../test-utils/render";
import {
  BRAND_AUTH,
  BRAND_CARRIER,
  BRAND_SETUP,
  SHOPIFY_API_VERSION,
  SOURCE_TYPES,
  brandSetupDefaults,
} from "../constants";
import { SourceFormFieldsExtended } from "../SourceFormFieldsExtended";
import type { SourceFormFieldsProps } from "../SourceFormFields";
import { backendType, sourceBrand, sourceTypeLabel } from "../sourceHelpers";
import { MARKS } from "../../../components/sourceLogoMarks";

describe("the source pick list", () => {
  it("offers Shopify, carried by the remote GraphQL source and asking for a token", () => {
    expect(SOURCE_TYPES.find((s) => s.value === "shopify")).toMatchObject({
      label: "Shopify",
      category: "Enterprise",
    });
    expect(BRAND_CARRIER.shopify).toBe("graphql");
    expect(BRAND_AUTH.shopify).toBe("token");
    expect(backendType("shopify")).toBe("graphql_remote");
    expect(sourceBrand('{"brand":"shopify","namespace":"shopify"}')).toBe("shopify");
    expect(sourceTypeLabel("graphql_remote", '{"brand":"shopify"}')).toBe("Shopify");
    expect(MARKS.shopify?.title).toBe("Shopify");
  });
});

describe("what a Shopify setup asks for", () => {
  it("is the store, and the API version filled in with the one the shipped schema is", () => {
    expect(BRAND_SETUP.shopify.map((f) => f.name)).toEqual(["store", "api_version"]);
    expect(brandSetupDefaults("shopify")).toEqual({ store: "", api_version: SHOPIFY_API_VERSION });
    expect(brandSetupDefaults("github")).toEqual({});
    // The brand's own pinned version, which the shipped schema was baked at.
    const brands = readFileSync(
      resolve(__dirname, "../../../../../provisa/graphql_remote/brands.py"),
      "utf8",
    );
    expect(/^SHOPIFY_API_VERSION = "([^"]+)"$/m.exec(brands)?.[1]).toBe(SHOPIFY_API_VERSION);
  });
});

describe("the Shopify setup in the Sources form", () => {
  const props = (type: string, brandSetup: Record<string, string>, setBrandSetup = vi.fn()) =>
    ({
      // The Sources page's empty form (SourcesPage.tsx), with the type under test.
      form: {
        id: "s",
        type,
        host: "",
        port: 443,
        database: "",
        username: "",
        password: "",
        gqlNamingConvention: "",
        cacheTtl: "",
        cacheEnabled: true,
        replicate: null,
        region: null,
        loadProtected: false,
        offPeakWindow: "",
        offPeakTz: "UTC",
        changeSignal: "ttl",
        sentinelPath: "",
        freshnessGate: false,
        maxLiveConcurrency: "",
        path: "",
        description: "",
      },
      setForm: vi.fn(),
      authType: "none",
      setAuthType: vi.fn(),
      authFields: {},
      setAuthFields: vi.fn(),
      gqlNamespace: "",
      setGqlNamespace: vi.fn(),
      brandSetup,
      setBrandSetup,
      domains: [],
    }) as unknown as SourceFormFieldsProps;

  it("asks for the store, the API version and the token, in plain words", () => {
    renderInForm(<SourceFormFieldsExtended {...props("shopify", brandSetupDefaults("shopify"))} />);
    const store = screen.getByTestId("brand-setup-store-input") as HTMLInputElement;
    const version = screen.getByTestId("brand-setup-api_version-input") as HTMLInputElement;
    expect(store.value).toBe("");
    expect(store.required).toBe(true);
    expect(version.value).toBe(SHOPIFY_API_VERSION);
    expect(screen.getByLabelText(/Store name/)).toBe(store);
    expect(screen.getByLabelText(/API version/)).toBe(version);
    expect(screen.getByText(/before \.myshopify\.com/)).toBeTruthy();
    expect(screen.getByTestId("brand-token-input")).toBeTruthy();
    expect(screen.getByTestId("brand-namespace-input")).toBeTruthy();
    // No endpoint: the store name and version say where it is.
    expect(screen.queryByTestId("graphql-endpoint-input")).toBeNull();
  });

  it("keeps what is typed under the field's setup name", () => {
    const setBrandSetup = vi.fn();
    renderInForm(
      <SourceFormFieldsExtended
        {...props("shopify", brandSetupDefaults("shopify"), setBrandSetup)}
      />,
    );
    fireEvent.change(screen.getByTestId("brand-setup-store-input"), {
      target: { value: "acme" },
    });
    expect(setBrandSetup).toHaveBeenCalledWith({ store: "acme", api_version: SHOPIFY_API_VERSION });
  });

  it("asks a brand at one fixed address for nothing but its token", () => {
    renderInForm(<SourceFormFieldsExtended {...props("github", {})} />);
    expect(screen.queryByTestId("brand-setup-store-input")).toBeNull();
    expect(screen.getByTestId("brand-token-input")).toBeTruthy();
  });
});
