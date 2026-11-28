// Copyright (c) 2026 Kenneth Stott
// Canary: 096746ce-1fd7-4b38-b608-3a5a31d8a782
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// A command's panel shows every value of the function or webhook under its form's labels, the
// implementation's own fields for that implementation, and who sees each response column and how
// it is masked.

import { describe, it, expect, vi } from "vitest";
// REQ-2277 rule 6: names link to their panels for a viewer who opens them (EntityLink).
vi.mock("../../../context/AuthContext", () => ({
  useAuth: () => ({
    capabilities: ["access_config", "org_settings", "source_registration", "table_registration"],
  }),
}));
import { render, screen, within } from "../../../test-utils/render";
import type { TrackedFunction, TrackedWebhook } from "../../../api/actions";
import { FunctionRead, WebhookRead } from "../CommandRead";
import { EditorView } from "@codemirror/view";
import { editorOf, languageOf } from "../../../test-utils/codeEditor";

function fields(testId: string): [string, string][] {
  return within(screen.getByTestId(testId))
    .queryAllByRole("term")
    .map((dt) => [dt.textContent ?? "", dt.nextElementSibling?.textContent ?? ""]);
}

const fn: TrackedFunction = {
  name: "refund_order",
  sourceId: "sales-pg",
  schemaName: "public",
  functionName: "refund",
  returns: "",
  arguments: [{ name: "order_id", type: "Int" }],
  visibleTo: ["support"],
  domainId: "sales",
  description: "Refunds an order",
  kind: "mutation",
  implKind: "http",
  binding: { url: "https://pay.example.com/refund", method: "POST" },
  materialize: true,
  requiresApproval: true,
  writesTable: "public.orders",
  productIds: ["p1", "p2"],
  outputColumns: [
    {
      name: "card",
      type: "string",
      visible_to: ["finance"],
      masks: [
        { to: ["auditor"], mask: null },
        { to: null, mask: { type: "regex", pattern: "\\d{12}", replace: "****" } },
      ],
    },
    { name: "status", type: "string" },
  ],
};

const wh: TrackedWebhook = {
  name: "notify_ops",
  url: "https://hooks.example.com/ops",
  method: "POST",
  timeoutMs: 5000,
  returns: null,
  inlineReturnType: [
    {
      name: "ok",
      type: "Boolean",
      masks: [{ to: null, mask: { type: "constant", value: "hidden" } }],
    },
  ],
  arguments: [],
  visibleTo: [],
  domainId: "ops",
  description: null,
  kind: "query",
  requiresApproval: false,
  approved: false,
};

describe("a function's read view", () => {
  it("shows every value, with the fields of its implementation", () => {
    render(<FunctionRead fn={fn} productNames={["Refunds", "Payouts"]} entered={null} />);
    expect(fields("function-read-fields")).toEqual([
      ["Name", "refund_order"],
      ["Type", "DB Function"],
      ["Kind", "Mutation"],
      ["Domain", "sales"],
      ["Data products", "Refunds, Payouts"],
      ["Implementation", "HTTP endpoint"],
      ["Endpoint URL", "https://pay.example.com/refund"],
      ["Method", "POST"],
      ["Materialize (admin/definer identity)", "Yes"],
      ["Return Type", "Own columns"],
      ["Returns", "card: string, status: string"],
      ["Visible To (roles, comma-separated)", "support"],
      ["Writes table", "public.orders"],
      ["Requires approval", "Yes"],
      ["Description", "Refunds an order"],
      ["Arguments", "order_id: Int (value)"],
    ]);
    // A dataset's columns are on the Columns tab, not under the command's own fields.
    expect(screen.queryByTestId("function-read-columns-card")).toBeNull();
  });

  it("shows a source procedure's source, schema and function, and no binding", () => {
    render(
      <FunctionRead
        fn={{ ...fn, implKind: "source_procedure", binding: {}, returns: "public.refunds" }}
        productNames={[]}
        entered={null}
      />,
    );
    const shown = Object.fromEntries(fields("function-read-fields"));
    expect(shown["Source"]).toBe("sales-pg");
    expect(shown["Schema"]).toBe("public");
    expect(shown["Data products"]).toBe("—");
    expect(shown["Return Type"]).toBe("Registered table");
    expect(shown["Returns"]).toBe("public.refunds");
    expect(shown["Endpoint URL"]).toBeUndefined();
    expect(shown["Materialize (admin/definer identity)"]).toBeUndefined();
    expect(screen.queryByTestId("function-read-columns-card")).toBeNull();
  });
});

describe("a function's table (REQ-1986)", () => {
  it("says whether a reading source operation is also a table", () => {
    render(
      <FunctionRead
        fn={{ ...fn, implKind: "source_operation", kind: "query", exposedAsTable: false }}
        productNames={[]}
        entered={null}
      />,
    );
    expect(Object.fromEntries(fields("function-read-fields"))["Also a table"]).toBe("No");
  });

  it("says nothing of a table for a function with no table form", () => {
    render(<FunctionRead fn={fn} productNames={[]} entered={null} />);
    expect(Object.fromEntries(fields("function-read-fields"))["Also a table"]).toBeUndefined();
  });
});

describe("a webhook's read view", () => {
  it("shows every value, and its inline response fields' governance", () => {
    render(<WebhookRead wh={wh} />);
    expect(fields("webhook-read-fields")).toEqual([
      ["Name", "notify_ops"],
      ["Type", "Webhook"],
      ["Kind", "Query"],
      ["Domain", "ops"],
      ["Status", "Pending approval"],
      ["URL", "https://hooks.example.com/ops"],
      ["Method", "POST"],
      ["Timeout (ms)", "5000"],
      ["Returns (table, optional)", "—"],
      ["Visible To (roles, comma-separated)", "all"],
      ["Requires approval", "No"],
      ["Description", "—"],
      ["Arguments", "—"],
    ]);
    expect(fields("webhook-read-columns-ok")).toEqual([
      ["Visible to (roles, comma-separated; blank = everyone)", "all"],
      ["Mask", "1. Everyone: constant hidden"],
    ]);
  });
});

describe("a statement command's panel (REQ-2001)", () => {
  const contract = {
    version: "8359d5f7e33b2980",
    statement_hash: "0ba0",
    kind: "mutation",
    writes: "kb.docs",
    upsert: true,
    reads: ["support.tickets"],
    outputs: [{ name: "rows_affected", type: "bigint" }],
    arguments: [{ name: "since", type: "timestamp" }],
    lineage: { title: ["support.tickets.subject"] },
    write_select: "SELECT subject AS title FROM support.tickets",
    broken: null,
  };
  const statement: TrackedFunction = {
    ...fn,
    name: "embed_tickets",
    implKind: "statement",
    binding: { sql: "INSERT INTO kb.docs (title) SELECT subject FROM support.tickets" },
    writesTable: "kb.docs",
    outputColumns: null,
    contract,
  };

  it("shows what its statement declares", () => {
    render(<FunctionRead fn={statement} productNames={[]} entered={null} />);
    const shown = Object.fromEntries(fields("function-read-fields"));
    expect(shown["Declares"]).toBe(
      "reads support.tickets, writes kb.docs (upsert), returns a row count",
    );
    expect(shown["Argument types"]).toBe("since: timestamp");
    expect(shown["Contract version"]).toBe("8359d5f7e33b2980");
    expect(shown["Broken"]).toBeUndefined();
  });

  it("says why one that no longer resolves is broken", () => {
    const broken = {
      ...statement,
      contract: { ...contract, broken: "Column 'subject' could not be resolved" },
    };
    render(<FunctionRead fn={broken} productNames={[]} entered={null} />);
    const shown = Object.fromEntries(fields("function-read-fields"));
    expect(shown["Broken"]).toBe("Column 'subject' could not be resolved");
    expect(shown["Declares"]).toBeUndefined();
  });
});

describe("a command's SQL in the code editor (REQ-1984)", () => {
  const cases: [string, Partial<TrackedFunction>, string, string][] = [
    [
      "a statement's SQL",
      { implKind: "statement", binding: { sql: "DELETE FROM kb.docs" }, contract: undefined },
      "function-read-sql",
      "DELETE FROM kb.docs",
    ],
    [
      "a source query's native SQL",
      { implKind: "source_query", binding: { sql: "SELECT 1;\nSELECT 2" } },
      "function-read-native-sql",
      "SELECT 1;\nSELECT 2",
    ],
    [
      "a SQL expression's template",
      { implKind: "sql_expression", binding: { sql: "upper({{name}})" } },
      "function-read-template",
      "upper({{name}})",
    ],
  ];

  it.each(cases)("shows %s read-only, highlighted as SQL", (_, over, testId, sql) => {
    render(<FunctionRead fn={{ ...fn, ...over }} productNames={[]} entered={null} />);
    const view = editorOf(testId);
    expect(view.state.doc.toString()).toBe(sql);
    expect(languageOf(view)).toBe("sql");
    expect(view.state.readOnly).toBe(true);
    expect(view.state.facet(EditorView.editable)).toBe(false);
  });

  it("shows the empty mark when there is no SQL", () => {
    render(
      <FunctionRead
        fn={{ ...fn, implKind: "sql_expression", binding: {} }}
        productNames={[]}
        entered={null}
      />,
    );
    expect(screen.queryByTestId("function-read-template")).toBeNull();
    expect(Object.fromEntries(fields("function-read-fields"))["SQL expression"]).toBe("—");
  });
});

// REQ-1159 (amended 2026-11-27): a command written as entered code reads as the form edits it.
describe("a command written as entered code", () => {
  const code = "@query\ndef greet(name: str) -> str:\n    return name";
  const entered = { enteredCode: code, enteredDependencies: "requests\nnumpy" };

  it("shows its code read-only in the Python editor, its dependencies and its function", () => {
    render(
      <FunctionRead
        fn={{ ...fn, implKind: "code_command", schemaName: "code", functionName: "greet" }}
        productNames={[]}
        entered={entered}
      />,
    );
    const view = editorOf("function-read-entered-code");
    expect(view.state.doc.toString()).toBe(code);
    expect(languageOf(view)).toBe("python");
    expect(view.state.readOnly).toBe(true);
    const shown = Object.fromEntries(fields("function-read-fields"));
    expect(shown["Implementation"]).toBe("Code (entered)");
    expect(shown["Dependencies"]).toBe("requests\nnumpy");
    expect(shown["Function Name"]).toBe("greet");
    expect(shown["Return Type"]).toBeUndefined();
    expect(shown["Materialize (admin/definer identity)"]).toBeUndefined();
  });
});

// REQ-1159 (amended 2026-11-28): Returns is the shape of the dataset the command returns.
describe("a function's Returns", () => {
  const returns = (over: Partial<TrackedFunction>) => {
    render(<FunctionRead fn={{ ...fn, ...over }} productNames={[]} entered={null} />);
    return Object.fromEntries(fields("function-read-fields"))["Returns"];
  };

  it("is a code source's declared columns, each with its type", () => {
    expect(
      returns({
        implKind: "code_command",
        schemaName: "code",
        functionName: "greet",
        kind: "query",
        outputColumns: [
          { name: "name", type: "text" },
          { name: "times", type: "bigint" },
        ],
      }),
    ).toBe("name: text, times: bigint");
  });

  it("is a statement mutation's result columns, as its statement declares them", () => {
    expect(
      returns({
        implKind: "statement",
        binding: { sql: "UPDATE orders SET status = 'x' RETURNING id, status" },
        outputColumns: null,
        contract: {
          version: "1",
          statement_hash: "h",
          kind: "mutation",
          writes: "orders",
          upsert: false,
          reads: [],
          outputs: [
            { name: "id", type: "bigint" },
            { name: "status", type: "text" },
          ],
          arguments: [],
          lineage: {},
          write_select: null,
          broken: null,
        },
      }),
    ).toBe("id: bigint, status: text");
  });

  it("is empty for a command that declares no columns", () => {
    expect(returns({ outputColumns: [] })).toBe("—");
  });
});
