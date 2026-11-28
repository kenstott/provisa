// Copyright (c) 2026 Kenneth Stott
// Canary: 4d17b6e2-9a03-4c8f-8e21-7b5c0f9d3a62
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-885: the function/command editor exposes the implementation-kind selector and
// per-kind binding fields (script/http/grpc/python) plus relation-argument kinds.

import { useState } from "react";
import { describe, it, expect, vi } from "vitest";
import { fireEvent, render, screen, within } from "../test-utils/render";
import { CommandFormFields } from "../pages/commands/CommandFormFields";
import { EMPTY_FORM } from "../pages/commands/types";
import { editorOf, languageOf, typeInto } from "../test-utils/codeEditor";
import type { FormState } from "../pages/commands/types";

function Harness({ initial }: { initial: Partial<FormState> }) {
  const [form, setForm] = useState<FormState>({ ...EMPTY_FORM, ...initial });
  return (
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
  );
}

describe("CommandFormFields — REQ-885 implementation kinds", () => {
  it("shows the implementation-kind selector for functions", () => {
    render(<Harness initial={{ actionType: "function" }} />);
    expect(screen.getByTestId("command-impl-kind-select")).toBeInTheDocument();
  });

  it("shows source-procedure fields only for the source_procedure kind", () => {
    render(<Harness initial={{ actionType: "function", implKind: "source_procedure" }} />);
    expect(screen.getByTestId("command-source-select")).toBeInTheDocument();
    expect(screen.queryByTestId("command-binding-url")).not.toBeInTheDocument();
  });

  it("shows the http binding url + method for the http kind", () => {
    render(<Harness initial={{ actionType: "function", implKind: "http" }} />);
    expect(screen.getByTestId("command-binding-url")).toBeInTheDocument();
    expect(screen.queryByTestId("command-source-select")).not.toBeInTheDocument();
  });

  it("shows the grpc target + method for the grpc kind", () => {
    render(<Harness initial={{ actionType: "function", implKind: "grpc" }} />);
    expect(screen.getByTestId("command-binding-target")).toBeInTheDocument();
    expect(screen.getByTestId("command-binding-grpc-method")).toBeInTheDocument();
  });

  it("shows the script argv for the script kind", () => {
    render(<Harness initial={{ actionType: "function", implKind: "script" }} />);
    expect(screen.getByTestId("command-binding-argv")).toBeInTheDocument();
  });

  it("shows the python callable for the python kind", () => {
    render(<Harness initial={{ actionType: "function", implKind: "python" }} />);
    expect(screen.getByTestId("command-binding-callable")).toBeInTheDocument();
  });

  it("asks a statement command for its SQL and nothing its statement declares", () => {
    // REQ-2001: the statement declares the kind, the written table and the output.
    render(
      <Harness
        initial={{
          actionType: "function",
          implKind: "statement",
          arguments: [{ name: "since", type: "String", argKind: "column_value" }],
        }}
      />,
    );
    expect(screen.getByTestId("command-binding-sql")).toBeInTheDocument();
    expect(screen.queryByTestId("command-writes-table-select")).not.toBeInTheDocument();
    expect(screen.queryByTestId("command-return-type-select")).not.toBeInTheDocument();
    expect(screen.queryByTestId("command-materialize-switch")).not.toBeInTheDocument();
    expect(screen.queryByText("Kind")).not.toBeInTheDocument();
    expect(screen.queryByLabelText("Argument kind")).not.toBeInTheDocument();
    expect(
      screen.getByRole("img", { name: /Write an argument as \{\{name\}\}/ }),
    ).toBeInTheDocument();
  });

  it("edits a statement's SQL in the code editor, highlighted as SQL (REQ-1984)", () => {
    render(<Harness initial={{ actionType: "function", implKind: "statement" }} />);
    const view = editorOf("command-binding-sql");
    expect(languageOf(view)).toBe("sql");
    typeInto(view, "DELETE FROM kb.docs WHERE id = {{id}}");
    expect(editorOf("command-binding-sql").state.doc.toString()).toBe(
      "DELETE FROM kb.docs WHERE id = {{id}}",
    );
  });

  it("edits a template in the code editor, highlighted as SQL (REQ-1984)", () => {
    render(<Harness initial={{ actionType: "function", implKind: "sql_expression" }} />);
    const view = editorOf("command-binding-template");
    expect(languageOf(view)).toBe("sql");
    typeInto(view, "array_to_string({list}, {delimiter})");
    expect(editorOf("command-binding-template").state.doc.toString()).toBe(
      "array_to_string({list}, {delimiter})",
    );
  });

  it("exposes the materialize identity switch for hosted kinds", () => {
    render(<Harness initial={{ actionType: "function", implKind: "http" }} />);
    expect(screen.getByTestId("command-materialize-switch")).toBeInTheDocument();
  });

  it("exposes a relation-argument-kind selector for hosted-kind arguments", () => {
    render(
      <Harness
        initial={{
          actionType: "function",
          implKind: "http",
          arguments: [{ name: "tbl", type: "String", argKind: "table_ref" }],
        }}
      />,
    );
    expect(screen.getByTestId("command-arg-kind-0")).toBeInTheDocument();
  });

  // REQ-1159: IR-typed dataset contract editors
  it("shows the input-columns editor for a dataset (result_set) argument", () => {
    render(
      <Harness
        initial={{
          actionType: "function",
          implKind: "grpc",
          arguments: [{ name: "input", type: "String", argKind: "result_set" }],
        }}
      />,
    );
    expect(screen.getByTestId("dataset-columns-0")).toBeInTheDocument();
  });

  it("hides the input-columns editor for a scalar (column_value) argument", () => {
    render(
      <Harness
        initial={{
          actionType: "function",
          implKind: "grpc",
          arguments: [{ name: "n", type: "Int", argKind: "column_value" }],
        }}
      />,
    );
    expect(screen.queryByTestId("dataset-columns-0")).not.toBeInTheDocument();
  });

  it("shows the output-columns editor for a hosted command in dataset mode", () => {
    render(
      <Harness
        initial={{ actionType: "function", implKind: "grpc", returnSchemaMode: "dataset" }}
      />,
    );
    expect(screen.getByTestId("output-columns")).toBeInTheDocument();
  });

  it("hides the output-columns editor when the output is a registered table", () => {
    render(
      <Harness initial={{ actionType: "function", implKind: "grpc", returnSchemaMode: "table" }} />,
    );
    expect(screen.queryByTestId("output-columns")).not.toBeInTheDocument();
  });
});

describe("CommandFormFields — response governance (REQ-1679)", () => {
  it("leaves each output column's governance to the Columns tab", () => {
    render(
      <Harness
        initial={{
          actionType: "function",
          implKind: "python",
          returnSchemaMode: "dataset",
          outputColumns: [{ name: "ssn", type: "varchar" }],
        }}
      />,
    );
    expect(screen.getByTestId("output-column-0")).toBeInTheDocument();
    expect(
      within(screen.getByTestId("output-column-0")).queryByRole("textbox", { name: "Visible to" }),
    ).not.toBeInTheDocument();
  });

  it("offers the same controls on each inline return field of a webhook", () => {
    render(
      <Harness
        initial={{
          actionType: "webhook",
          returns: "",
          inlineReturnType: [{ name: "id", type: "Int" }],
        }}
      />,
    );
    expect(
      within(screen.getByTestId("inline-field-governance-0")).getByRole("textbox", {
        name: "Visible to",
      }),
    ).toBeInTheDocument();
  });
});

// REQ-1924: a remote source's write operations are picked, one at a time, and passed through as
// is; what the command takes and answers follows from the operation, not from the form.
function RemoteHarness({ initial }: { initial: Partial<FormState> }) {
  const [form, setForm] = useState<FormState>({ ...EMPTY_FORM, ...initial });
  const sources = [
    { id: "gh", type: "graphql_remote" },
    { id: "pg", type: "postgresql" },
  ] as unknown as Parameters<typeof CommandFormFields>[0]["sources"];
  return (
    <CommandFormFields
      form={form}
      setForm={setForm}
      sources={sources}
      tables={[]}
      domainHints={[]}
      availableFunctions={[{ name: "createIssue", comment: "Creates a new issue." }]}
      loadingFunctions={false}
      roleOptions={[]}
    />
  );
}

describe("CommandFormFields — a remote source's write operations (REQ-1924)", () => {
  it("offers the source's operations to pick, under the source's schema", () => {
    render(
      <RemoteHarness
        initial={{
          actionType: "function",
          implKind: "source_operation",
          sourceId: "gh",
          schemaName: "graphql",
        }}
      />,
    );
    expect(screen.getByTestId("command-function-select")).toBeInTheDocument();
    expect(screen.queryByTestId("command-function-input")).not.toBeInTheDocument();
    expect(screen.getByTestId("command-schema-input")).toHaveAttribute("readonly");
  });

  it("leaves the arguments, kind and answer to the operation", () => {
    render(
      <RemoteHarness
        initial={{ actionType: "function", implKind: "source_operation", sourceId: "gh" }}
      />,
    );
    expect(screen.queryByText("Arguments")).not.toBeInTheDocument();
    expect(screen.queryByTestId("command-return-type-select")).not.toBeInTheDocument();
    expect(screen.queryByTestId("command-materialize-switch")).not.toBeInTheDocument();
  });

  it("asks a database source for a function name, as before", () => {
    render(
      <RemoteHarness
        initial={{ actionType: "function", implKind: "source_procedure", sourceId: "pg" }}
      />,
    );
    expect(screen.getByText("Arguments")).toBeInTheDocument();
    expect(screen.getByTestId("command-function-input")).toBeInTheDocument();
    expect(screen.getByTestId("command-return-type-select")).toBeInTheDocument();
  });
});

describe("CommandFormFields — a code source's column transforms (REQ-2131)", () => {
  function CodeHarness({ initial }: { initial: Partial<FormState> }) {
    const [form, setForm] = useState<FormState>({ ...EMPTY_FORM, ...initial });
    return (
      <>
        <span data-testid="impl-kind">{form.implKind}</span>
        <span data-testid="schema">{form.schemaName}</span>
        <CommandFormFields
          form={form}
          setForm={setForm}
          sources={[{ id: "addresses", type: "python" } as never]}
          tables={[]}
          domainHints={[]}
          availableFunctions={[
            { name: "clean_address", comment: "raw_address -> street, city, country" },
          ]}
          loadingFunctions={false}
          roleOptions={[]}
        />
      </>
    );
  }

  it("offers the code's @columns functions to pick, and leaves their columns to the function", () => {
    render(
      <CodeHarness
        initial={{
          actionType: "function",
          implKind: "code_columns",
          sourceId: "addresses",
          schemaName: "code",
        }}
      />,
    );
    expect(screen.getByTestId("command-function-select")).toBeInTheDocument();
    expect(screen.getByTestId("command-schema-input")).toHaveAttribute("readonly");
    expect(screen.queryByText("Arguments")).not.toBeInTheDocument();
    expect(screen.queryByTestId("output-columns")).not.toBeInTheDocument();
  });

  it("registers a function picked from a code source as a column transform", async () => {
    render(<CodeHarness initial={{ actionType: "function", implKind: "source_procedure" }} />);
    fireEvent.click(screen.getByTestId("command-source-select"));
    fireEvent.click(
      await screen.findByRole("option", { name: "addresses (python)", hidden: true }),
    );
    expect(screen.getByTestId("impl-kind")).toHaveTextContent("code_columns");
    expect(screen.getByTestId("schema")).toHaveTextContent("code");
  });
});

describe("CommandFormFields — a call that needs approval (REQ-1924)", () => {
  it("lets the steward require approval of each call", async () => {
    const { default: userEvent } = await import("@testing-library/user-event");
    render(<Harness initial={{ actionType: "function", implKind: "source_procedure" }} />);
    const toggle = screen.getByTestId("command-requires-approval-switch");
    expect(toggle).not.toBeChecked();
    await userEvent.click(toggle);
    expect(toggle).toBeChecked();
  });
});

describe("CommandFormFields — a function's table (REQ-1986)", () => {
  it("lets the steward turn a registered reading operation's table on and off", async () => {
    const { default: userEvent } = await import("@testing-library/user-event");
    render(
      <RemoteHarness
        initial={{
          actionType: "function",
          implKind: "source_operation",
          sourceId: "gh",
          kind: "query",
          exposedAsTable: true,
        }}
      />,
    );
    const toggle = screen.getByTestId("command-exposed-as-table-switch");
    expect(toggle).toBeChecked();
    expect(screen.getByText("Also a table")).toBeInTheDocument();
    await userEvent.click(toggle);
    expect(toggle).not.toBeChecked();
  });

  it("offers no switch to a function with no table form, or one not yet registered", () => {
    render(<Harness initial={{ actionType: "function", implKind: "source_procedure" }} />);
    expect(screen.queryByTestId("command-exposed-as-table-switch")).not.toBeInTheDocument();
  });
});

describe("CommandFormFields — the table a command writes (REQ-1924, REQ-871)", () => {
  it("offers the selected source's registered tables", () => {
    render(
      <RemoteHarness
        initial={{ actionType: "function", implKind: "source_operation", sourceId: "gh" }}
      />,
    );
    expect(screen.getByTestId("command-writes-table-select")).toBeInTheDocument();
  });
});

describe("CommandFormFields — a database source's routines on offer (REQ-887)", () => {
  it("suggests the schema's routines, and the one picked becomes the function name", async () => {
    const { default: userEvent } = await import("@testing-library/user-event");
    function DbHarness() {
      const [form, setForm] = useState<FormState>({
        ...EMPTY_FORM,
        actionType: "function",
        implKind: "source_procedure",
        sourceId: "pg",
        schemaName: "public",
      });
      return (
        <>
          <CommandFormFields
            form={form}
            setForm={setForm}
            sources={[{ id: "pg", type: "postgresql" } as never]}
            tables={[]}
            domainHints={[]}
            availableFunctions={[{ name: "get_customers_by_region", comment: "query" }]}
            loadingFunctions={false}
            roleOptions={[]}
          />
          <output data-testid="picked">{form.functionName}</output>
        </>
      );
    }
    render(<DbHarness />);
    expect(screen.getByText("1 routine in this schema is on offer")).toBeInTheDocument();
    await userEvent.type(screen.getByTestId("command-function-input"), "get_");
    await userEvent.click(await screen.findByText("get_customers_by_region"));
    expect(screen.getByTestId("picked")).toHaveTextContent("get_customers_by_region");
  });
});

// REQ-2230: a Snowflake source offers operations among its routines; one picked is called as it
// is, while a routine is still described by the form.
describe("CommandFormFields — a Snowflake source's operations", () => {
  function SnowflakeHarness({ functionName }: { functionName: string }) {
    const [form, setForm] = useState<FormState>({
      ...EMPTY_FORM,
      actionType: "function",
      implKind: "source_procedure",
      sourceId: "sf",
      schemaName: "ANALYTICS",
      functionName,
    });
    return (
      <CommandFormFields
        form={form}
        setForm={setForm}
        sources={[{ id: "sf", type: "snowflake" } as never]}
        tables={[]}
        domainHints={[]}
        availableFunctions={[{ name: functionName, comment: "" }]}
        loadingFunctions={false}
        roleOptions={[]}
      />
    );
  }

  it.each([
    "refresh_dynamic_table.DAILY_SALES",
    "suspend_dynamic_table.DAILY_SALES",
    "resume_dynamic_table.DAILY_SALES",
    "execute_task.NIGHTLY",
    "suspend_task.NIGHTLY",
    "resume_task.NIGHTLY",
  ])("leaves the arguments and answer of %s to the operation", (functionName) => {
    render(<SnowflakeHarness functionName={functionName} />);
    expect(screen.queryByText("Arguments")).not.toBeInTheDocument();
    expect(screen.queryByTestId("command-return-type-select")).not.toBeInTheDocument();
  });

  it("still asks for the arguments and answer of a routine", () => {
    render(<SnowflakeHarness functionName="MY_PROC" />);
    expect(screen.getByText("Arguments")).toBeInTheDocument();
    expect(screen.getByTestId("command-return-type-select")).toBeInTheDocument();
  });

  it("leaves the schema for the steward to name", () => {
    render(<SnowflakeHarness functionName="execute_task.NIGHTLY" />);
    expect(screen.getByTestId("command-schema-input")).not.toHaveAttribute("readonly");
  });
});

describe("CommandFormFields — lists laid out as the shared form lays out a list", () => {
  it("sets each output column apart as one entry", () => {
    render(
      <Harness
        initial={{
          actionType: "function",
          implKind: "script",
          returnSchemaMode: "dataset",
          outputColumns: [
            { name: "id", type: "integer" },
            { name: "name", type: "text" },
          ],
        }}
      />,
    );
    for (const i of [0, 1]) {
      const entry = screen.getByTestId(`output-column-${i}`);
      expect(entry).toHaveClass("form-entry");
    }
    expect(screen.getByText("Output Dataset Columns")).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "+ Add Output Column" })).toBeInTheDocument();
  });

  it("keeps an argument's remove on its row, beside its boxes", () => {
    render(
      <Harness
        initial={{
          actionType: "function",
          implKind: "script",
          arguments: [{ name: "name", type: "String" }],
        }}
      />,
    );
    const remove = screen.getByRole("button", { name: /Remove argument name/i });
    expect(remove.parentElement).toHaveClass("form-field-group--bare");
    expect(remove.parentElement).toContainElement(screen.getByDisplayValue("name"));
  });
});

// REQ-1988: a command holding native SQL for a relational source asks for the source it runs on
// and its SQL; its kind, written table and output are declared as for any command.
describe("CommandFormFields — native SQL on a relational source (REQ-1988)", () => {
  it("offers the relational sources it runs on and asks for the SQL", () => {
    render(
      <RemoteHarness
        initial={{
          actionType: "function",
          implKind: "source_query",
          arguments: [{ name: "id", type: "Int" }],
        }}
      />,
    );
    expect(screen.getByTestId("command-binding-native-sql")).toBeInTheDocument();
    // An argument is a value bound as a parameter: it takes no relation.
    expect(screen.queryByTestId("command-arg-kind-0")).not.toBeInTheDocument();
    expect(screen.getByTestId("command-writes-table-select")).toBeInTheDocument();
    expect(screen.getByTestId("command-return-type-select")).toBeInTheDocument();
    expect(screen.getByText("Kind")).toBeInTheDocument();
    expect(screen.queryByTestId("command-materialize-switch")).not.toBeInTheDocument();
    expect(screen.queryByTestId("command-function-input")).not.toBeInTheDocument();
    fireEvent.click(screen.getByTestId("command-native-sql-source-select"));
    expect(screen.getByRole("option", { name: "pg (postgresql)" })).toBeInTheDocument();
    expect(screen.queryByRole("option", { name: "gh (graphql_remote)" })).not.toBeInTheDocument();
    expect(
      screen.getByRole("img", { name: /Write an argument as \{\{name\}\}/ }),
    ).toBeInTheDocument();
  });

  it("keeps the SQL in the binding as it is typed", () => {
    render(<RemoteHarness initial={{ actionType: "function", implKind: "source_query" }} />);
    const sql = "DELETE FROM app.notes WHERE id = {{id}}; SELECT count(*) FROM app.notes";
    typeInto(editorOf("command-binding-native-sql"), sql);
    expect(editorOf("command-binding-native-sql").state.doc.toString()).toBe(sql);
  });

  it("edits the SQL in the code editor, highlighted as SQL (REQ-1984)", () => {
    render(<RemoteHarness initial={{ actionType: "function", implKind: "source_query" }} />);
    expect(languageOf(editorOf("command-binding-native-sql"))).toBe("sql");
  });

  it("is not offered for a MongoDB pipeline command", () => {
    render(
      <RemoteHarness
        initial={{
          actionType: "function",
          implKind: "source_query",
          binding: { inputCollection: "movies", pipeline: [] },
        }}
      />,
    );
    expect(screen.queryByTestId("command-binding-native-sql")).not.toBeInTheDocument();
  });
});

// REQ-1159 (amended 2026-11-27): a command written as entered code (REQ-1993). Its code is typed
// in the editor code sources use; its kind, arguments and columns are its declaration's, shown
// read-only and never typed by hand.
describe("CommandFormFields — Code (entered)", () => {
  function EnteredHarness({ initial }: { initial: Partial<FormState> }) {
    const [form, setForm] = useState<FormState>({ ...EMPTY_FORM, ...initial });
    return (
      <>
        <CommandFormFields
          form={form}
          setForm={setForm}
          sources={[{ id: "pg", type: "postgresql" } as never]}
          tables={[]}
          domainHints={[]}
          availableFunctions={[]}
          loadingFunctions={false}
          roleOptions={[]}
        />
        <output data-testid="state">{JSON.stringify(form)}</output>
      </>
    );
  }
  const state = () => JSON.parse(screen.getByTestId("state").textContent!) as FormState;

  it("is offered beside Python (in-process), which stays", async () => {
    render(<EnteredHarness initial={{ actionType: "function" }} />);
    fireEvent.click(screen.getByTestId("command-impl-kind-select"));
    expect(
      await screen.findByRole("option", { name: "Code (entered)", hidden: true }),
    ).toBeInTheDocument();
    expect(
      screen.getByRole("option", { name: "Python (in-process)", hidden: true }),
    ).toBeInTheDocument();
  });

  it("chosen, opens the Python code editor, its dependencies and the function it calls", async () => {
    render(
      <EnteredHarness
        initial={{
          actionType: "function",
          implKind: "source_procedure",
          sourceId: "pg",
          kind: "mutation",
          arguments: [{ name: "id", type: "Int" }],
          outputColumns: [{ name: "ok", type: "boolean" }],
        }}
      />,
    );
    fireEvent.click(screen.getByTestId("command-impl-kind-select"));
    fireEvent.click(await screen.findByRole("option", { name: "Code (entered)", hidden: true }));
    expect(languageOf(editorOf("command-entered-code"))).toBe("python");
    expect(screen.getByTestId("command-entered-dependencies")).toBeInTheDocument();
    expect(screen.getByTestId("command-entered-function")).toBeInTheDocument();
    expect(screen.queryByTestId("command-binding-callable")).not.toBeInTheDocument();
    expect(screen.queryByTestId("command-source-select")).not.toBeInTheDocument();
    // What another implementation held is not the declaration's: none of it is kept.
    expect(state()).toMatchObject({
      implKind: "code_entered",
      sourceId: "",
      kind: "",
      arguments: [],
      outputColumns: [],
    });
  });

  it("keeps the code and the function as they are typed", async () => {
    render(<EnteredHarness initial={{ actionType: "function", implKind: "code_entered" }} />);
    fireEvent.change(screen.getByTestId("command-entered-function"), {
      target: { value: "greet" },
    });
    fireEvent.change(screen.getByTestId("command-entered-dependencies"), {
      target: { value: "httpx==0.27.0" },
    });
    const code = 'def greet(name: str) -> str:\n    """@query"""\n    return name\n';
    typeInto(editorOf("command-entered-code"), code);
    await vi.waitFor(() =>
      expect(state()).toMatchObject({
        enteredCode: code,
        functionName: "greet",
        enteredDependencies: "httpx==0.27.0",
      }),
    );
  });

  it("shows its kind, arguments and columns read-only, as declared", () => {
    render(
      <EnteredHarness
        initial={{
          actionType: "function",
          implKind: "code_entered",
          kind: "query",
          functionName: "greet",
          arguments: [
            { name: "name", type: "text", argKind: "column_value" },
            { name: "times", type: "bigint", argKind: "column_value" },
          ],
          outputColumns: [
            { name: "name", type: "text" },
            { name: "times", type: "bigint" },
          ],
        }}
      />,
    );
    expect(screen.getByTestId("command-entered-kind")).toHaveTextContent("Query");
    expect(screen.getByTestId("command-entered-arguments")).toHaveTextContent(
      "name: text (value), times: bigint (value)",
    );
    expect(screen.getByTestId("command-entered-columns")).toHaveTextContent(
      "name: text, times: bigint",
    );
    // Nothing to type them into: no argument or column editor, no kind, return type or written
    // table.
    expect(screen.queryByTestId("command-arguments")).not.toBeInTheDocument();
    expect(screen.queryByTestId("output-columns")).not.toBeInTheDocument();
    expect(screen.queryByRole("button", { name: "+ Add Argument" })).not.toBeInTheDocument();
    expect(screen.queryByTestId("command-return-type-select")).not.toBeInTheDocument();
    expect(screen.queryByTestId("command-writes-table-select")).not.toBeInTheDocument();
    expect(screen.queryByTestId("command-materialize-switch")).not.toBeInTheDocument();
  });

  it("shows none of them before the command is saved", () => {
    render(
      <EnteredHarness initial={{ actionType: "function", implKind: "code_entered", kind: "" }} />,
    );
    for (const id of [
      "command-entered-kind",
      "command-entered-arguments",
      "command-entered-columns",
    ])
      expect(screen.getByTestId(id)).toHaveTextContent("—");
  });
});
