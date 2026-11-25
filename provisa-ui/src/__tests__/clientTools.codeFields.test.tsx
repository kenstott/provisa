// Copyright (c) 2026 Kenneth Stott
// Canary: b833b030-a0c3-4a63-856e-68137f6dfa09
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2114, REQ-2115: every code-editor field (CodeEditorField) is a field Polly reads and fills.
// read_screen reports its label, value, language and read-only state from the editor's own state;
// navigate's fill replaces its text by a CodeMirror transaction, so the form's state takes it as a
// person's typing; a read-only one is reported and refuses a fill by name.

import { useState } from "react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { cleanup, renderInForm, screen } from "../test-utils/render";
import { editorOf } from "../test-utils/codeEditor";
import { executeClientTool, type ClientToolContext } from "../mcp/clientTools";
import { resetDataAddresses } from "../lib/dataAddress";

vi.mock("../context/AuthContext", () => ({ useAuth: () => ({ activeOrgId: "acme" }) }));
vi.mock("../api/secrets", () => ({ fetchSecrets: vi.fn(async () => ({ secrets: [] })) }));

import { CodeEditorField } from "../components/CodeEditorField";
import { OpenApiFormSection } from "../pages/sources/OpenApiFormSection";
import { CommandFormFields } from "../pages/commands/CommandFormFields";
import { EMPTY_FORM, type FormState } from "../pages/commands/types";

const ctx = (): ClientToolContext => ({
  navigate: vi.fn(),
  confirm: vi.fn(),
  runMutation: vi.fn(),
  presentChoice: vi.fn(),
  runAdminMutation: vi.fn(),
  chooseFile: vi.fn(),
});

async function fields() {
  const result = await executeClientTool("read_screen", {}, ctx());
  expect(result.success).toBe(true);
  return JSON.parse(result.message).fields;
}

function fill(route: string, target: Record<string, string>, value: string) {
  return executeClientTool("navigate", { route, actions: [{ do: "fill", target, value }] }, ctx());
}

// A code field on its own: the editable one keeps its text in form state; a read-only one is a
// view of code a person cannot change.
function CodeForm({ readOnly = false }: { readOnly?: boolean }) {
  const [code, setCode] = useState(readOnly ? "print(1)" : "df = spark.range(3)\n");
  return (
    <>
      {readOnly ? (
        <CodeEditorField label="code" value={code} language="python" readOnly data-testid="code" />
      ) : (
        <CodeEditorField
          label="code"
          value={code}
          language="python"
          onChange={setCode}
          data-testid="code"
        />
      )}
      <pre data-testid="form-state">{code}</pre>
    </>
  );
}

function OpenApiForm() {
  const [spec, setSpec] = useState("openapi: '3.0.0'\n");
  return (
    <>
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
      />
      <pre data-testid="form-state">{spec}</pre>
    </>
  );
}

function CommandForm() {
  const [form, setForm] = useState<FormState>({
    ...EMPTY_FORM,
    actionType: "function",
    implKind: "statement",
    binding: { sql: "DELETE FROM kb.docs" },
  });
  return (
    <>
      <CommandFormFields
        form={form}
        setForm={setForm}
        sources={[]}
        tables={[]}
        domainHints={[]}
        availableFunctions={[]}
        loadingFunctions={false}
        roleOptions={[]}
      />
      <pre data-testid="form-state">{String(form.binding.sql)}</pre>
    </>
  );
}

type Field = { label: string; value: string | boolean; language?: string; readOnly?: boolean };

/** The code fields among the fields read_screen reports. */
function codeFields(all: Field[]) {
  return all.filter((f) => f.language !== undefined);
}

beforeEach(() => {
  resetDataAddresses();
  // jsdom lays nothing out; every element counts as drawn.
  HTMLElement.prototype.checkVisibility = () => true;
  window.history.pushState({}, "", "/sources");
});

afterEach(cleanup);

describe("a code field", () => {
  it("is on screen: read_screen reports its label, value, language and read-only state", async () => {
    renderInForm(<CodeForm />);
    expect(codeFields(await fields())).toEqual([
      { label: "code", value: "df = spark.range(3)\n", language: "python", readOnly: false },
    ]);
  });

  it("takes a fill by its test id, and the form state holds the new code", async () => {
    renderInForm(<CodeForm />);
    const code = "df = spark.read.parquet('s3://b/k')\ndf.show()\n";
    const out = await fill("/sources", { testId: "code" }, code);
    expect(out.success).toBe(true);
    expect(out.message).toContain('Filled the element with test id "code"');
    expect(screen.getByTestId("form-state").textContent).toBe(code);
    expect(editorOf("code").state.doc.toString()).toBe(code);
    expect(codeFields(await fields())[0].value).toBe(code);
  });

  it("takes a fill by its role and label", async () => {
    renderInForm(<CodeForm />);
    const out = await fill("/sources", { role: "textbox", name: "code" }, "print(2)\n");
    expect(out.success).toBe(true);
    expect(screen.getByTestId("form-state").textContent).toBe("print(2)\n");
  });

  it("is reported read-only when it is a view, and refuses a fill by name", async () => {
    renderInForm(<CodeForm readOnly />);
    expect(codeFields(await fields())).toEqual([
      { label: "code", value: "print(1)", language: "python", readOnly: true },
    ]);
    const out = await fill("/sources", { testId: "code" }, "print(2)");
    expect(out.success).toBe(false);
    expect(out.message).toContain(
      'Could not fill the element with test id "code": "code" is a read-only code editor.',
    );
    expect(editorOf("code").state.doc.toString()).toBe("print(1)");
  });
});

describe("an OpenAPI source's inline spec", () => {
  const LABEL = "OpenAPI Spec (YAML or JSON)";

  it("is on screen with its value and the language it is written in", async () => {
    renderInForm(<OpenApiForm />);
    expect(codeFields(await fields())).toEqual([
      { label: LABEL, value: "openapi: '3.0.0'\n", language: "yaml", readOnly: false },
    ]);
  });

  it("takes a fill, and the form state holds the new spec", async () => {
    renderInForm(<OpenApiForm />);
    const spec = '{\n  "openapi": "3.0.0"\n}';
    const out = await fill("/sources", { testId: "openapi-spec-inline-input" }, spec);
    expect(out.success).toBe(true);
    expect(screen.getByTestId("form-state").textContent).toBe(spec);
    expect(codeFields(await fields())).toEqual([
      { label: LABEL, value: spec, language: "json", readOnly: false },
    ]);
  });
});

describe("a command's SQL field", () => {
  it("is on screen with its SQL", async () => {
    window.history.pushState({}, "", "/commands");
    renderInForm(<CommandForm />);
    expect(codeFields(await fields())).toEqual([
      { label: "SQL statement", value: "DELETE FROM kb.docs", language: "sql", readOnly: false },
    ]);
  });

  it("takes a fill, and the form state holds the new SQL", async () => {
    window.history.pushState({}, "", "/commands");
    renderInForm(<CommandForm />);
    const sql = "DELETE FROM kb.docs WHERE id = {{id}}";
    const out = await fill("/commands", { testId: "command-binding-sql" }, sql);
    expect(out.success).toBe(true);
    expect(screen.getByTestId("form-state").textContent).toBe(sql);
    expect(codeFields(await fields())[0].value).toBe(sql);
  });
});
