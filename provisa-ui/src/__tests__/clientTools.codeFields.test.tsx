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

import type { Connector } from "../api/connectors";
import { ConnectorSetupFields } from "../pages/sources/ConnectorSetupFields";
import { requiredField } from "../pages/sources/connector";
import { OpenApiFormSection } from "../pages/sources/OpenApiFormSection";
import { CodeSourceFields } from "../pages/sources/CodeSourceFields";
import { CommandFormFields } from "../pages/commands/CommandFormFields";
import { EMPTY_FORM, type FormState } from "../pages/commands/types";

const ctx = (): ClientToolContext => ({
  navigate: vi.fn(),
  confirm: vi.fn(),
  runMutation: vi.fn(),
  presentChoice: vi.fn(),
  runAdminMutation: vi.fn(),
  chooseFile: vi.fn(),
  changed: vi.fn(),
});

async function fields() {
  const result = await executeClientTool("read_screen", {}, ctx());
  expect(result.success).toBe(true);
  return JSON.parse(result.message).fields;
}

function fill(route: string, target: Record<string, string>, value: string) {
  return executeClientTool("navigate", { route, actions: [{ do: "fill", target, value }] }, ctx());
}

const SPARK: Connector = {
  name: "spark",
  git: "https://git.example/spark.git",
  ref: "main",
  commit: "a".repeat(40),
  language: "python",
  image: "registry.example/data/spark",
  digest: `sha256:${"b".repeat(64)}`,
  code_hash: "c".repeat(64),
  arch: "amd64",
  requires: [{ name: "script", multiline: true, script: true }],
  description: "",
  icon: "",
  settings: [],
};

const SCRIPT = requiredField("script");

function SparkForm({ connector = SPARK }: { connector?: Connector }) {
  const [form, setForm] = useState<Record<string, string>>({
    connector_host: "spark-host.acme.internal",
    connector_port: "7443",
    [SCRIPT]: "df = spark.range(3)\n",
  });
  return (
    <>
      <ConnectorSetupFields connector={connector} fields={form} setFields={setForm} />
      <pre data-testid="form-state">{form[SCRIPT]}</pre>
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

// REQ-1993 (amended 2026-11-25): a code source's code entered here.
function EnteredCodeForm({ type }: { type: string }) {
  const [fields, setFields] = useState<Record<string, string>>({
    code_origin: "entered",
    code_source: "",
  });
  return (
    <>
      <CodeSourceFields type={type} fields={fields} setFields={setFields} />
      <pre data-testid="form-state">{fields.code_source}</pre>
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

describe("the Spark connector's script field", () => {
  it("is on screen: read_screen reports its label, value, language and read-only state", async () => {
    renderInForm(<SparkForm />);
    expect(codeFields(await fields())).toEqual([
      { label: "script", value: "df = spark.range(3)\n", language: "python", readOnly: false },
    ]);
  });

  it("takes a fill by its test id, and the form state holds the new script", async () => {
    renderInForm(<SparkForm />);
    const script = "df = spark.read.parquet('s3://b/k')\ndf.show()\n";
    const out = await fill("/sources", { testId: "connector-require-script" }, script);
    expect(out.success).toBe(true);
    expect(out.message).toContain('Filled the element with test id "connector-require-script"');
    expect(screen.getByTestId("form-state").textContent).toBe(script);
    expect(editorOf("connector-require-script").state.doc.toString()).toBe(script);
    expect(codeFields(await fields())[0].value).toBe(script);
  });

  it("takes a fill by its role and label", async () => {
    renderInForm(<SparkForm />);
    const out = await fill("/sources", { role: "textbox", name: "script" }, "print(2)\n");
    expect(out.success).toBe(true);
    expect(screen.getByTestId("form-state").textContent).toBe("print(2)\n");
  });

  it("is reported read-only when the connector fixes it, and refuses a fill by name", async () => {
    const fixed: Connector = {
      ...SPARK,
      requires: [{ name: "script", multiline: true, script: true, fixed_value: "print(1)" }],
    };
    renderInForm(<SparkForm connector={fixed} />);
    expect(codeFields(await fields())).toEqual([
      { label: "script", value: "print(1)", language: "python", readOnly: true },
    ]);
    const out = await fill("/sources", { testId: "connector-require-script" }, "print(2)");
    expect(out.success).toBe(false);
    expect(out.message).toContain(
      'Could not fill the element with test id "connector-require-script": ' +
        '"script" is a read-only code editor.',
    );
    expect(editorOf("connector-require-script").state.doc.toString()).toBe("print(1)");
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

describe("a code source's code entered here", () => {
  it.each([
    ["python", "python", 'def rates() -> list[dict]:\n    """@table"""\n    return []\n'],
    ["node", "javascript", "/** @table */\nexport function rates() { return []; }\n"],
    ["go", "go", "package entered\n"],
    ["rust", "rust", "pub fn rates() -> Vec<String> { vec![] }\n"],
  ])(
    "%s: Polly drafts the code into the editor, and nothing is saved",
    async (type, language, code) => {
      const c = ctx();
      renderInForm(<EnteredCodeForm type={type} />);
      expect(codeFields(await fields())).toEqual([
        { label: "Code", value: "", language, readOnly: false },
      ]);
      const out = await executeClientTool(
        "navigate",
        {
          route: "/sources",
          actions: [{ do: "fill", target: { testId: "code-source" }, value: code }],
        },
        c,
      );
      expect(out.success).toBe(true);
      // The draft is the form's, as typed: the source is saved only when the person saves the form.
      expect(screen.getByTestId("form-state").textContent).toBe(code);
      expect(editorOf("code-source").state.doc.toString()).toBe(code);
      expect(c.runMutation).not.toHaveBeenCalled();
      expect(c.runAdminMutation).not.toHaveBeenCalled();
    },
  );
});
