// Copyright (c) 2026 Kenneth Stott
// Canary: 1b7f3c92-5d46-4a08-9e21-c8a4d6f0b357
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1159 (amended 2026-11-27): choosing a surface on a command's Query it line first asks for its
// arguments. A scalar is an input; a relation argument is given by picking a table the role reads,
// writing a query, or pasting a few rows (CSV or JSON), capped to a small row count. A missing
// required argument is refused by name on press (REQ-1968).

import { describe, expect, it, vi } from "vitest";
import userEvent from "@testing-library/user-event";
import { fireEvent, render, screen, within } from "../../../test-utils/render";
import type { ActionArg } from "../../../api/actions";
import { CommandArgumentsModal } from "../CommandArgumentsModal";
import type { CommandCall } from "../commandQueries";
import { RELATION_ROW_CAP, fittingTables, readsAs, type ArgTable } from "../relationArg";

const col = (computedSqlAlias: string, dataType: string | null) => ({ computedSqlAlias, dataType });
const ORDERS: ArgTable = {
  domainId: "sales",
  tableName: "orders",
  alias: null,
  columns: [col("id", "bigint"), col("region", "text"), col("total", "numeric(10,2)")],
};
const PEOPLE: ArgTable = {
  domainId: "hr",
  tableName: "people",
  alias: "staff",
  columns: [col("id", "integer"), col("name", "text")],
};

const input: ActionArg = { name: "input", type: "String", argKind: "result_set" };
const factor: ActionArg = { name: "factor", type: "Float" };
const note: ActionArg = { name: "note", type: "String", default: "none" };

function open(call: CommandCall, tables: readonly ArgTable[] = [ORDERS, PEOPLE]) {
  const onOpen = vi.fn();
  render(
    <CommandArgumentsModal
      call={call}
      surface="sql"
      tables={tables}
      onOpen={onOpen}
      onClose={vi.fn()}
    />,
  );
  return onOpen;
}

const ENRICH: CommandCall = { name: "enrich", kind: "query", arguments: [input, factor, note] };
const press = () => userEvent.click(screen.getByTestId("command-args-open"));
const source = (name: string, label: string) =>
  userEvent.click(within(screen.getByTestId(`command-arg-${name}-source`)).getByText(label));

describe("the argument form", () => {
  it("is titled by the command and the surface it opens", () => {
    open(ENRICH);
    expect(screen.getByRole("dialog")).toHaveTextContent("enrich in SQL");
  });

  it("refuses a missing required argument by name on press, and opens nothing", async () => {
    const onOpen = open(ENRICH);
    await press();
    expect(onOpen).not.toHaveBeenCalled();
    const problem = screen.getByTestId("form-problem");
    expect(problem).toHaveTextContent("input");
    expect(problem).toHaveTextContent("factor");
    // An argument with a default may be left out.
    expect(problem).not.toHaveTextContent("note");
  });

  it("takes a scalar as an input, typed as the argument is", async () => {
    const onOpen = open({ name: "scale", kind: "query", arguments: [factor] });
    await userEvent.type(screen.getByTestId("command-arg-factor"), "1.5");
    await press();
    expect(onOpen).toHaveBeenCalledWith({ factor: { kind: "scalar", value: 1.5 } });
  });

  it("refuses a number argument that is not a number, by name", async () => {
    const onOpen = open({ name: "scale", kind: "query", arguments: [factor] });
    await userEvent.type(screen.getByTestId("command-arg-factor"), "lots");
    await press();
    expect(onOpen).not.toHaveBeenCalled();
    expect(screen.getByTestId("form-problem")).toHaveTextContent("factor");
  });

  it("gives a relation by picking a table the role reads, read to the row cap", async () => {
    const onOpen = open({ name: "enrich", kind: "query", arguments: [input] });
    await source("input", "Table");
    await userEvent.click(screen.getByTestId("command-arg-input-table"));
    await userEvent.click(
      await screen.findByRole("option", { name: "sales.orders", hidden: true }),
    );
    await press();
    expect(onOpen).toHaveBeenCalledWith({
      input: {
        kind: "relation",
        text: `select * from "sales"."orders" limit ${RELATION_ROW_CAP}`,
        rows: null,
      },
    });
  });

  it("reads only the argument's declared columns from a picked table", async () => {
    const typed: ActionArg = { ...input, columns: [{ name: "id", type: "integer" }] };
    const onOpen = open({ name: "enrich", kind: "query", arguments: [typed] });
    await source("input", "Table");
    await userEvent.click(screen.getByTestId("command-arg-input-table"));
    await userEvent.click(await screen.findByRole("option", { name: "hr.staff", hidden: true }));
    await press();
    expect(onOpen.mock.calls[0][0].input.text).toBe(
      `select "id" from "hr"."staff" limit ${RELATION_ROW_CAP}`,
    );
  });

  it("gives a relation by a written query, held to the row cap", async () => {
    const onOpen = open({ name: "enrich", kind: "query", arguments: [input] });
    await source("input", "Query");
    fireEvent.change(screen.getByTestId("command-arg-input-query"), {
      target: { value: "select id from sales.orders;" },
    });
    await press();
    expect(onOpen).toHaveBeenCalledWith({
      input: {
        kind: "relation",
        text: `select * from (select id from sales.orders) as given limit ${RELATION_ROW_CAP}`,
        rows: null,
      },
    });
  });

  it("gives a relation by pasted JSON rows", async () => {
    const onOpen = open({ name: "enrich", kind: "query", arguments: [input] });
    await source("input", "Rows");
    const rows = [{ id: 1, name: "a" }];
    fireEvent.change(screen.getByTestId("command-arg-input-rows"), {
      target: { value: JSON.stringify(rows) },
    });
    await press();
    expect(onOpen).toHaveBeenCalledWith({
      input: { kind: "relation", text: JSON.stringify(rows), rows },
    });
  });

  it("gives a relation by pasted CSV rows, typed by the declared columns", async () => {
    const typed: ActionArg = {
      ...input,
      columns: [
        { name: "id", type: "integer" },
        { name: "name", type: "text" },
      ],
    };
    const onOpen = open({ name: "enrich", kind: "query", arguments: [typed] });
    await source("input", "Rows");
    fireEvent.change(screen.getByTestId("command-arg-input-rows"), {
      target: { value: "id,name\n1,ada\n2,bob" },
    });
    await press();
    expect(onOpen.mock.calls[0][0].input.rows).toEqual([
      { id: 1, name: "ada" },
      { id: 2, name: "bob" },
    ]);
  });

  it("refuses more rows than the cap", async () => {
    const onOpen = open({ name: "enrich", kind: "query", arguments: [input] });
    await source("input", "Rows");
    const many = Array.from({ length: RELATION_ROW_CAP + 1 }, (_, i) => ({ id: i }));
    fireEvent.change(screen.getByTestId("command-arg-input-rows"), {
      target: { value: JSON.stringify(many) },
    });
    await press();
    expect(onOpen).not.toHaveBeenCalled();
    expect(screen.getByTestId("form-problem")).toHaveTextContent(String(RELATION_ROW_CAP));
  });

  it("refuses pasted text that is neither JSON rows nor CSV with a header", async () => {
    const onOpen = open({ name: "enrich", kind: "query", arguments: [input] });
    await source("input", "Rows");
    fireEvent.change(screen.getByTestId("command-arg-input-rows"), {
      target: { value: "[1, 2]" },
    });
    await press();
    expect(onOpen).not.toHaveBeenCalled();
    expect(screen.getByTestId("form-problem")).toHaveTextContent("input");
  });

  it("gives a table_ref argument only by picking a table: its service reads the table itself", async () => {
    const ref: ActionArg = { name: "src", type: "String", argKind: "table_ref" };
    const onOpen = open({ name: "load", kind: "query", arguments: [ref] });
    expect(screen.queryByTestId("command-arg-src-source")).toBeNull();
    await userEvent.click(screen.getByTestId("command-arg-src-table"));
    await userEvent.click(
      await screen.findByRole("option", { name: "sales.orders", hidden: true }),
    );
    await press();
    expect(onOpen).toHaveBeenCalledWith({
      src: { kind: "relation", text: `"sales"."orders"`, rows: null },
    });
  });

  it("lists every table the role reads for a result_set that declares no columns", async () => {
    open({ name: "enrich", kind: "query", arguments: [input] });
    await source("input", "Table");
    await userEvent.click(screen.getByTestId("command-arg-input-table"));
    expect(await tableOptions()).toEqual(["sales.orders", "hr.staff"]);
  });

  it("lists only the tables that have every declared column, by name and type", async () => {
    const typed: ActionArg = {
      ...input,
      columns: [
        { name: "id", type: "integer" },
        { name: "region", type: "text" },
      ],
    };
    const onOpen = open({ name: "enrich_grpc_set", kind: "query", arguments: [typed] });
    await source("input", "Table");
    await userEvent.click(screen.getByTestId("command-arg-input-table"));
    // hr.staff has no region column.
    expect(await tableOptions()).toEqual(["sales.orders"]);
    await userEvent.click(
      await screen.findByRole("option", { name: "sales.orders", hidden: true }),
    );
    await press();
    expect(onOpen.mock.calls[0][0].input.text).toBe(
      `select "id", "region" from "sales"."orders" limit ${RELATION_ROW_CAP}`,
    );
  });

  it("says no table fits and names the declared columns when none has them", async () => {
    const typed: ActionArg = {
      ...input,
      columns: [
        { name: "id", type: "integer" },
        { name: "country", type: "text" },
      ],
    };
    open({ name: "enrich", kind: "query", arguments: [typed] });
    await source("input", "Table");
    const picker = screen.getByTestId("command-arg-input-table");
    expect(picker).toBeDisabled();
    const dialog = screen.getByRole("dialog");
    expect(dialog).toHaveTextContent(
      "No table you can read has the columns id integer, country text",
    );
    // The written query and pasted rows are still offered.
    await source("input", "Query");
    expect(screen.getByTestId("command-arg-input-query")).toBeEnabled();
  });

  it("lists every table for a table_ref argument, whatever columns it declares", async () => {
    const ref: ActionArg = {
      name: "src",
      type: "String",
      argKind: "table_ref",
      columns: [{ name: "country", type: "text" }],
    };
    open({ name: "load", kind: "query", arguments: [ref] });
    await userEvent.click(screen.getByTestId("command-arg-src-table"));
    expect(await tableOptions()).toEqual(["sales.orders", "hr.staff"]);
  });
});

async function tableOptions(): Promise<string[]> {
  const options = await screen.findAllByRole("option", { hidden: true });
  return options.map((o) => o.textContent ?? "");
}

describe("the tables a result_set argument fits", () => {
  const arg = (columns: { name: string; type: string }[]): ActionArg => ({
    ...input,
    columns,
  });

  it("matches a column by its name as the catalog spells it", () => {
    expect(fittingTables(arg([{ name: "Region", type: "text" }]), [ORDERS])).toEqual([]);
    expect(fittingTables(arg([{ name: "region", type: "text" }]), [ORDERS])).toEqual([ORDERS]);
  });

  it("matches a column whose type reads as the declared type", () => {
    // An integer reads as an integer of any width, and as a decimal.
    expect(fittingTables(arg([{ name: "id", type: "integer" }]), [ORDERS, PEOPLE])).toEqual([
      ORDERS,
      PEOPLE,
    ]);
    expect(fittingTables(arg([{ name: "id", type: "double" }]), [ORDERS])).toEqual([ORDERS]);
    // A decimal does not read as an integer; text reads as neither.
    expect(fittingTables(arg([{ name: "total", type: "integer" }]), [ORDERS])).toEqual([]);
    expect(fittingTables(arg([{ name: "region", type: "integer" }]), [ORDERS])).toEqual([]);
    expect(fittingTables(arg([{ name: "total", type: "numeric" }]), [ORDERS])).toEqual([ORDERS]);
  });

  it("does not fit a column whose type the catalog does not record", () => {
    const untyped: ArgTable = { ...PEOPLE, columns: [col("id", null)] };
    expect(fittingTables(arg([{ name: "id", type: "integer" }]), [untyped])).toEqual([]);
  });

  it("reads types without case or parameters", () => {
    expect(readsAs("NUMERIC(12,4)", "decimal")).toBe(true);
    expect(readsAs("BOOL", "boolean")).toBe(true);
    expect(readsAs("text", "boolean")).toBe(false);
    expect(readsAs("timestamp", "timestamp")).toBe(true);
    expect(readsAs("date", "timestamp")).toBe(false);
  });
});
