// Copyright (c) 2026 Kenneth Stott
// Canary: 4c8a2f61-d93e-4b70-a5e2-9f1d6b3c8e07
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1923: Wikidata is in the source pick list, carried by the SPARQL source; picking it fills
// the query service endpoint, asks for no credential, and replicates its tables on a schedule.

import { describe, expect, it, vi } from "vitest";
import { renderInForm, screen } from "../../../test-utils/render";
import { MARKS } from "../../../components/sourceLogoMarks";
import { BRAND_AUTH, BRAND_CARRIER, SOURCE_TYPES } from "../constants";
import { SourceFormFieldsExtended } from "../SourceFormFieldsExtended";
import type { SourceFormFieldsProps, SourceFormState } from "../SourceFormFields";
import { emptySourceForm } from "../sourceSeed";
import { backendType, sourceBrand, sourceTypeLabel } from "../sourceHelpers";
import {
  WIKIDATA_ENDPOINT,
  WIKIDATA_REFRESH_SECONDS,
  wikidataHints,
  wikidataPreset,
} from "../wikidata";

describe("the source pick list", () => {
  it("offers Wikidata as a graph source the SPARQL source carries", () => {
    expect(SOURCE_TYPES.find((s) => s.value === "wikidata")).toMatchObject({
      label: "Wikidata",
      category: "Graph",
    });
    expect(BRAND_CARRIER.wikidata).toBe("sparql");
    expect(backendType("wikidata")).toBe("sparql");
    expect(MARKS.wikidata.title).toBe("Wikidata");
  });

  it("shows a SPARQL source recording the brand as Wikidata", () => {
    const hints = JSON.stringify(wikidataHints());
    expect(sourceBrand(hints)).toBe("wikidata");
    expect(sourceTypeLabel("sparql", hints)).toBe("Wikidata");
    expect(sourceTypeLabel("sparql", "{}")).toBe("SPARQL");
  });
});

describe("picking Wikidata", () => {
  it("fills the query service endpoint and scheduled replication", () => {
    const form = wikidataPreset({ ...emptySourceForm(), type: "wikidata" });
    expect(form.host).toBe(WIKIDATA_ENDPOINT);
    expect(WIKIDATA_ENDPOINT).toBe("https://query.wikidata.org/sparql");
    expect(form.replicate).toBe(0);
    expect(form.cacheTtl).toBe(String(WIKIDATA_REFRESH_SECONDS));
    expect(form.username).toBe("");
    expect(form.password).toBe("");
  });

  it("asks for no credential and shows the endpoint", () => {
    expect(BRAND_AUTH.wikidata).toBe("none");
    const form: SourceFormState = { ...emptySourceForm(), type: "wikidata" };
    const props = {
      form,
      setForm: vi.fn(),
      authType: "none",
      setAuthType: vi.fn(),
      authFields: {},
      setAuthFields: vi.fn(),
      domains: [],
    } as unknown as SourceFormFieldsProps;
    renderInForm(<SourceFormFieldsExtended {...props} />);
    const endpoint = screen.getByTestId("wikidata-endpoint") as HTMLInputElement;
    expect(endpoint.value).toBe(WIKIDATA_ENDPOINT);
    expect(endpoint.readOnly).toBe(true);
    expect(screen.queryByTestId("brand-token-input")).toBeNull();
    expect(screen.queryByTestId("sparql-auth-type-select")).toBeNull();
    expect(screen.queryByLabelText(/token|password|secret|api key/i)).toBeNull();
  });
});
