// Copyright (c) 2026 Kenneth Stott
// Canary: 4c8559ca-32e9-4c68-8860-0537dc92d054
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1984: an OpenAPI source's inline spec is edited in the code editor, highlighted as the
// language it is written in: JSON for a JSON document, YAML otherwise.

import { describe, expect, it, vi } from "vitest";
import { renderInForm } from "../../../test-utils/render";
import { editorOf, languageOf, typeInto } from "../../../test-utils/codeEditor";
import { OpenApiFormSection } from "../OpenApiFormSection";

function show(spec: string, setSpec = vi.fn()) {
  renderInForm(
    <OpenApiFormSection
      openapiSpecMode="inline"
      setOpenapiSpecMode={vi.fn()}
      openapiSpecPath=""
      setOpenapiSpecPath={vi.fn()}
      openapiSpecInline={spec}
      setOpenapiSpecInline={setSpec}
      openapiBaseUrl=""
      setOpenapiBaseUrl={vi.fn()}
      openapiCacheTtl=""
      setOpenapiCacheTtl={vi.fn()}
      authType="none"
      setAuthType={vi.fn()}
      authFields={{}}
      setAuthFields={vi.fn()}
      onOpenapiPreview={vi.fn()}
      openapiPreviewing={false}
      openapiPreviewError={null}
      openapiPreview={null}
    />,
  );
}

const YAML_SPEC = "openapi: '3.0.0'\ninfo:\n  title: Items\n";
const JSON_SPEC = '{\n  "openapi": "3.0.0",\n  "info": { "title": "Items" }\n}';

describe("the OpenAPI inline spec", () => {
  it("is highlighted as YAML when written in YAML", () => {
    show(YAML_SPEC);
    const view = editorOf("openapi-spec-inline-input");
    expect(view.state.doc.toString()).toBe(YAML_SPEC);
    expect(languageOf(view)).toBe("yaml");
  });

  it("is highlighted as JSON when written in JSON", () => {
    show(JSON_SPEC);
    const view = editorOf("openapi-spec-inline-input");
    expect(view.state.doc.toString()).toBe(JSON_SPEC);
    expect(languageOf(view)).toBe("json");
  });

  it("sends each edit, whole, to the form", () => {
    const set = vi.fn();
    show("", set);
    typeInto(editorOf("openapi-spec-inline-input"), YAML_SPEC);
    expect(set).toHaveBeenLastCalledWith(YAML_SPEC);
  });
});
