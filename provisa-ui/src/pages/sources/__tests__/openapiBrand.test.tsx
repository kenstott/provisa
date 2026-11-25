// Copyright (c) 2026 Kenneth Stott
// Canary: b3ad2f10-7d5b-4527-9d1e-48a398db8ade
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2265: an OpenAPI brand passes through the OpenAPI source's settings: a base URL its setup
// asks for, and OAuth client credentials. Stripe asks for its token as before. The brand here is
// a test brand; the catalogs using the mechanism are defined elsewhere.

import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { fireEvent, renderInForm, screen } from "../../../test-utils/render";

// SourceFormFieldsExtended calls useAuth() unconditionally; stub it so the form
// renders without an AuthProvider (the brand setup tests have no connector/script panel).
vi.mock("../../../context/AuthContext", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../../context/AuthContext")>()),
  useAuth: () => ({ activeOrgId: null }),
}));
import {
  BRAND_AUTH,
  BRAND_BASE_URL_FIELD,
  BRAND_CARRIER,
  BRAND_SETUP,
  brandSetupDefaults,
} from "../constants";
import { brandCredentialTyped, openapiBrandRegistration } from "../openapiBrand";
import { SourceFormFieldsExtended } from "../SourceFormFieldsExtended";
import type { SourceFormFieldsProps } from "../SourceFormFields";
import { fetchRemoteSeed } from "../sourceSeed";
import type { Source } from "../../../types/admin";

const CATALOG = "acme_catalog";

beforeEach(() => {
  BRAND_CARRIER[CATALOG] = "openapi";
  BRAND_AUTH[CATALOG] = "client_credentials";
  BRAND_SETUP[CATALOG] = [BRAND_BASE_URL_FIELD];
});

afterEach(() => {
  delete BRAND_CARRIER[CATALOG];
  delete BRAND_AUTH[CATALOG];
  delete BRAND_SETUP[CATALOG];
  vi.unstubAllGlobals();
});

describe("what an OpenAPI brand's registration sends", () => {
  it("is Stripe's brand and token, with no setup", () => {
    expect(
      openapiBrandRegistration("pay", "stripe", brandSetupDefaults("stripe"), { token: "sk_1" }),
    ).toEqual({ source_id: "pay", brand: "stripe", setup: {}, token: "sk_1" });
  });

  it("is a client credentials brand's base URL and client ID and secret, and no token", () => {
    expect(
      openapiBrandRegistration(
        "cat",
        CATALOG,
        { base_url: "https://catalog.acme.test" },
        { client_id: "app", client_secret: "${secret:CAT}" },
      ),
    ).toEqual({
      source_id: "cat",
      brand: CATALOG,
      setup: { base_url: "https://catalog.acme.test" },
      client_id: "app",
      client_secret: "${secret:CAT}",
    });
  });

  it("counts a credential as given by the part the brand's auth type keeps secret", () => {
    expect(brandCredentialTyped("stripe", { token: "sk" })).toBe(true);
    expect(brandCredentialTyped("stripe", {})).toBe(false);
    expect(brandCredentialTyped(CATALOG, { client_id: "app" })).toBe(false);
    expect(brandCredentialTyped(CATALOG, { client_id: "app", client_secret: "s" })).toBe(true);
  });
});

describe("an OpenAPI brand's setup in the Sources form", () => {
  const props = (
    type: string,
    brandSetup: Record<string, string>,
    authFields: Record<string, string> = {},
    setBrandSetup = vi.fn(),
    setAuthFields = vi.fn(),
  ) =>
    ({
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
      authFields,
      setAuthFields,
      gqlNamespace: "",
      setGqlNamespace: vi.fn(),
      brandSetup,
      setBrandSetup,
      domains: [],
    }) as unknown as SourceFormFieldsProps;

  it("asks a client credentials brand for its base URL, client ID and secret", () => {
    const setBrandSetup = vi.fn();
    const setAuthFields = vi.fn();
    renderInForm(
      <SourceFormFieldsExtended
        {...props(CATALOG, brandSetupDefaults(CATALOG), {}, setBrandSetup, setAuthFields)}
      />,
    );
    const baseUrl = screen.getByTestId("brand-setup-base_url-input") as HTMLInputElement;
    expect(baseUrl.required).toBe(true);
    expect(screen.getByLabelText(/Base URL/)).toBe(baseUrl);
    expect(screen.getByText(/OpenAPI description is read from it/)).toBeTruthy();
    expect(screen.getByTestId("brand-client-id-input")).toBeTruthy();
    expect(screen.getByTestId("brand-client-secret-input")).toBeTruthy();
    expect(screen.queryByTestId("brand-token-input")).toBeNull();
    // An OpenAPI table is named for its operation: no table name prefix.
    expect(screen.queryByTestId("brand-namespace-input")).toBeNull();

    fireEvent.change(baseUrl, { target: { value: "https://catalog.acme.test" } });
    expect(setBrandSetup).toHaveBeenCalledWith({ base_url: "https://catalog.acme.test" });
    fireEvent.change(screen.getByTestId("brand-client-secret-input"), {
      target: { value: "s3cret" },
    });
    expect(setAuthFields).toHaveBeenCalledWith({ client_secret: "s3cret" });
  });

  it("asks Stripe for nothing but its token", () => {
    renderInForm(<SourceFormFieldsExtended {...props("stripe", {})} />);
    expect(screen.getByTestId("brand-token-input")).toBeTruthy();
    expect(screen.queryByTestId("brand-setup-base_url-input")).toBeNull();
    expect(screen.queryByTestId("brand-client-id-input")).toBeNull();
  });
});

describe("an OpenAPI source's edit form", () => {
  it("shows again what its brand's setup was given", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn(async () => ({
        ok: true,
        json: async () => [
          {
            source_id: "cat",
            spec_path: "https://catalog.acme.test/swagger.json",
            has_inline_spec: false,
            base_url: "https://catalog.acme.test",
            cache_ttl: 300,
            auth_config: { type: "oauth2_client_credentials", client_id: "app" },
            setup: { base_url: "https://catalog.acme.test" },
          },
        ],
      })),
    );
    const seed = await fetchRemoteSeed({ id: "cat", type: "openapi" } as Source);
    expect(seed).toMatchObject({
      kind: "openapi",
      setup: { base_url: "https://catalog.acme.test" },
    });
  });
});
