// Copyright (c) 2026 Kenneth Stott
// Canary: b596a5cc-b631-4b69-af11-f8625c639dc8
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2138: a sparql source names a reasoner and its OWL axioms in its mapping; the form shows
// the axioms only once a reasoner is chosen, and changing the auth mode keeps both.

import { fireEvent } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import { renderInForm, screen } from "../../../test-utils/render";
import { SparqlFormSection } from "../SparqlFormSection";
import {
  sparqlCredentialFromSource,
  sparqlCredentials,
  sparqlFieldsFromMapping,
  sparqlMappingJson,
} from "../sparqlReasoner";
import type { SourceFormState } from "../SourceFormFields";

const AXIOMS = "@prefix : <http://example.org/kyc#> .";

describe("the reasoner mapping", () => {
  it("saves the reasoner with its axioms, and nothing without a reasoner", () => {
    expect(JSON.parse(sparqlMappingJson({ reasoner: "hermit", axioms: AXIOMS }, "none"))).toEqual({
      reasoner: "hermit",
      axioms: AXIOMS,
    });
    expect(sparqlMappingJson({ axioms: AXIOMS }, "none")).toBe("{}");
    expect(sparqlMappingJson({ reasoner: "", default_graph_uri: "g" }, "none")).toBe("{}");
  });

  it("reads the reasoner back, and nothing from a mapping without one", () => {
    expect(sparqlFieldsFromMapping(JSON.stringify({ reasoner: "hermit", axioms: AXIOMS }))).toEqual(
      { reasoner: "hermit", axioms: AXIOMS },
    );
    expect(sparqlFieldsFromMapping("{}")).toEqual({});
  });
});

describe("the credential (REQ-2138)", () => {
  it("names its kind in the mapping beside the reasoner", () => {
    expect(JSON.parse(sparqlMappingJson({ reasoner: "hermit", axioms: AXIOMS }, "basic"))).toEqual({
      reasoner: "hermit",
      axioms: AXIOMS,
      auth: "basic",
    });
    expect(JSON.parse(sparqlMappingJson({}, "bearer"))).toEqual({ auth: "bearer" });
  });

  it("rides in the source's username and password as entered", () => {
    expect(sparqlCredentials("bearer", { token: "${secret:t}" })).toEqual({
      username: "",
      password: "${secret:t}",
    });
    expect(sparqlCredentials("basic", { username: "svc", password: "${secret:p}" })).toEqual({
      username: "svc",
      password: "${secret:p}",
    });
    expect(sparqlCredentials("none", { token: "left over" })).toEqual({
      username: "",
      password: "",
    });
  });

  it("opens again as the source holds it", () => {
    expect(sparqlCredentialFromSource('{"auth":"bearer"}', "", "${secret:t}")).toEqual({
      authType: "bearer",
      fields: { token: "${secret:t}" },
    });
    expect(sparqlCredentialFromSource('{"auth":"basic"}', "svc", "${secret:p}")).toEqual({
      authType: "basic",
      fields: { username: "svc", password: "${secret:p}" },
    });
    expect(sparqlCredentialFromSource("{}", "", "")).toEqual({ authType: "none", fields: {} });
    expect(sparqlCredentialFromSource(null, "", "")).toEqual({ authType: "none", fields: {} });
  });
});

describe("SparqlFormSection", () => {
  const show = (fields: Record<string, string>, setAuthFields = vi.fn(), setAuthType = vi.fn()) =>
    renderInForm(
      <SparqlFormSection
        form={{ host: "https://graph.test/sparql" } as SourceFormState}
        setForm={vi.fn()}
        authType="none"
        setAuthType={setAuthType}
        authFields={fields}
        setAuthFields={setAuthFields}
      />,
    );

  it("asks for the axioms only once a reasoner is chosen", () => {
    show({});
    expect(screen.getByTestId("sparql-reasoner-select")).toBeInTheDocument();
    expect(screen.queryByTestId("sparql-axioms-input")).toBeNull();
  });

  it("edits the axioms of a chosen reasoner", () => {
    const set = vi.fn();
    show({ reasoner: "hermit", axioms: "" }, set);
    fireEvent.change(screen.getByTestId("sparql-axioms-input"), { target: { value: AXIOMS } });
    expect(set).toHaveBeenCalledWith({ reasoner: "hermit", axioms: AXIOMS });
  });
});
