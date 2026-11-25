// Copyright (c) 2026 Kenneth Stott
// Canary: 96ab9ac6-542e-48e5-b8d2-565dc3c35a5f
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2265: a governance catalog each customer runs at its own address asks, in the Sources form,
// for that address and for the token the catalog issues, in the catalog's own words.

import { describe, expect, it, vi } from "vitest";
import { fireEvent, renderInForm, screen } from "../../../test-utils/render";

// SourceFormFieldsExtended calls useAuth() unconditionally; stub it so the form
// renders without an AuthProvider (the brand setup tests have no connector/script panel).
vi.mock("../../../context/AuthContext", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../../context/AuthContext")>()),
  useAuth: () => ({ activeOrgId: null }),
}));
import { brandSetupDefaults } from "../constants";
import { SourceFormFieldsExtended } from "../SourceFormFieldsExtended";
import type { SourceFormFieldsProps } from "../SourceFormFields";

const props = (type: string, setBrandSetup = vi.fn()) =>
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
    brandSetup: brandSetupDefaults(type),
    setBrandSetup,
    domains: [],
  }) as unknown as SourceFormFieldsProps;

describe("a governance catalog's setup in the Sources form", () => {
  it.each([
    ["openmetadata", "https://openmetadata.example.com", /without \/api/, /Settings > Bots/],
    ["datahub", "https://datahub.example.com/gms", /\/openapi\/v3\/api-docs/, /Access Tokens/],
  ])("asks %s for its base URL and its token", (brand, placeholder, urlHint, tokenHint) => {
    renderInForm(<SourceFormFieldsExtended {...props(brand)} />);
    const url = screen.getByTestId("brand-setup-base_url-input") as HTMLInputElement;
    expect(url.required).toBe(true);
    expect(url.placeholder).toBe(placeholder);
    expect(screen.getByText(urlHint)).toBeTruthy();
    expect(screen.getByTestId("brand-token-input")).toBeTruthy();
    expect(screen.getByText(tokenHint)).toBeTruthy();
  });

  it("keeps the base URL typed under its setup name", () => {
    const setBrandSetup = vi.fn();
    renderInForm(<SourceFormFieldsExtended {...props("datahub", setBrandSetup)} />);
    fireEvent.change(screen.getByTestId("brand-setup-base_url-input"), {
      target: { value: "https://dh.acme.test/gms" },
    });
    expect(setBrandSetup).toHaveBeenCalledWith({ base_url: "https://dh.acme.test/gms" });
  });
});
