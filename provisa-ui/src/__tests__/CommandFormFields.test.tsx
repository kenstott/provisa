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
import { describe, it, expect } from "vitest";
import { render, screen } from "../test-utils/render";
import { CommandFormFields } from "../pages/commands/CommandFormFields";
import { EMPTY_FORM } from "../pages/commands/types";
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
      dataProducts={[]}
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
  it("offers visibility and mask controls on each output column", () => {
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
    expect(screen.getByTestId("output-column-governance-0-visible-to")).toBeInTheDocument();
    expect(screen.getByTestId("output-column-governance-0-mask-type")).toBeInTheDocument();
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
    expect(screen.getByTestId("inline-field-governance-0-visible-to")).toBeInTheDocument();
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
      dataProducts={[]}
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
