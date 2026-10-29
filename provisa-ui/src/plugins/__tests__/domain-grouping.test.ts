// Copyright (c) 2026 Kenneth Stott
// Canary: fdd9bd61-c19a-4c36-a126-7c71888d33a9
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { describe, it, expect, vi, beforeEach } from "vitest";
// @ts-expect-error -- CJS fork with no type declarations
import { _renderGroupedFields } from "graphiql-explorer";

describe("_renderGroupedFields", () => {
  const renderField = vi.fn((name: string) => ({ field: name }));

  function calledFieldNames() {
    return renderField.mock.calls.map((c: unknown[]) => c[0]);
  }

  beforeEach(() => {
    renderField.mockClear();
  });

  it('groups fields by domain prefix (split on "__")', () => {
    const fields: Record<string, unknown> = {
      sales__orders: {},
      sales__returns: {},
      product__items: {},
    };

    const result = _renderGroupedFields(fields, renderField);

    // Two domain folders: product, sales (sorted)
    expect(result).toHaveLength(2);
    expect(renderField).toHaveBeenCalledTimes(3);
    expect(calledFieldNames()).toEqual(["product__items", "sales__orders", "sales__returns"]);
  });

  it('places ungrouped fields (no "__") after domain folders', () => {
    const fields: Record<string, unknown> = {
      sales__orders: {},
      health: {},
      version: {},
    };

    const result = _renderGroupedFields(fields, renderField);

    // 1 domain folder + 2 ungrouped fields
    expect(result).toHaveLength(3);
    expect(calledFieldNames()).toEqual(["sales__orders", "health", "version"]);
  });

  it('returns flat list when no fields have "__" separator', () => {
    const fields: Record<string, unknown> = {
      health: {},
      version: {},
      status: {},
    };

    const result = _renderGroupedFields(fields, renderField);

    expect(result).toHaveLength(3);
    expect(renderField).toHaveBeenCalledTimes(3);
  });

  it("handles empty fields object", () => {
    const result = _renderGroupedFields({}, renderField);

    expect(result).toHaveLength(0);
    expect(renderField).not.toHaveBeenCalled();
  });

  it("sorts domains alphabetically", () => {
    const fields: Record<string, unknown> = {
      zebra__a: {},
      alpha__b: {},
      middle__c: {},
    };

    const result = _renderGroupedFields(fields, renderField);

    expect(result).toHaveLength(3);
    expect(calledFieldNames()).toEqual(["alpha__b", "middle__c", "zebra__a"]);
  });

  it("groups multiple fields under the same domain", () => {
    const fields: Record<string, unknown> = {
      crm__contacts: {},
      crm__accounts: {},
      crm__deals: {},
    };

    const result = _renderGroupedFields(fields, renderField);

    // 1 domain folder containing 3 fields
    expect(result).toHaveLength(1);
    expect(renderField).toHaveBeenCalledTimes(3);
  });

  it('does not split on leading "__"', () => {
    const fields: Record<string, unknown> = {
      __internal: {},
      sales__orders: {},
    };

    const result = _renderGroupedFields(fields, renderField);

    // __internal has sepIdx === 0, so it's ungrouped
    // 1 domain folder (sales) + 1 ungrouped (__internal)
    expect(result).toHaveLength(2);
    expect(calledFieldNames()).toEqual(["sales__orders", "__internal"]);
  });

  it("opens domain folder when a field in that domain is selected", () => {
    const fields: Record<string, unknown> = {
      sales__orders: {},
      sales__returns: {},
      product__items: {},
    };
    const selections = [{ name: { value: "sales__orders" } }];

    const result = _renderGroupedFields(fields, renderField, selections);

    // sales folder should be open, product should not
    const salesFolder = result.find(
      (r: { props?: { open?: boolean; className?: string } }) =>
        r?.props?.className === "graphiql-explorer-domain" && r?.props?.open === true,
    );
    const productFolder = result.find(
      (r: { props?: { open?: boolean; className?: string } }) =>
        r?.props?.className === "graphiql-explorer-domain" && r?.props?.open === false,
    );
    expect(salesFolder).toBeDefined();
    expect(productFolder).toBeDefined();
  });

  it("all folders closed when no selections provided", () => {
    const fields: Record<string, unknown> = {
      sales__orders: {},
      product__items: {},
    };

    const result = _renderGroupedFields(fields, renderField);

    const openFolders = result.filter(
      (r: { props?: { open?: boolean; className?: string } }) =>
        r?.props?.className === "graphiql-explorer-domain" && r?.props?.open === true,
    );
    expect(openFolders).toHaveLength(0);
  });

  it("opens domain folder when any field in it is selected", () => {
    const fields: Record<string, unknown> = {
      crm__contacts: {},
      crm__accounts: {},
    };
    const selections = [{ name: { value: "crm__accounts" } }];

    const result = _renderGroupedFields(fields, renderField, selections);

    expect(result[0]?.props?.open).toBe(true);
  });
});

describe("_renderGroupedFields system schemas (REQ-2163)", () => {
  const prefixes = { ontology: "on", ops: "op" };
  const typed = (typeName: string) => ({ type: { ofType: { ofType: { name: typeName } } } });
  const fields: Record<string, unknown> = {
    m__resolve: typed("M__Resolve"),
    ps__pets: typed("PS__Pets"),
    on_terms: typed("ON_Terms"),
    on_termEdges: typed("ON_TermEdges"),
    op_queries: typed("OP_Queries"),
    op_traces: typed("OP_Traces"),
  };
  const renderField = (name: string) => ({ field: name });
  type Folder = { key: string; props: { open: boolean }; children?: unknown };

  function folderFields(el: { props: { children: unknown[] } }) {
    const body = el.props.children[1] as { props: { children: { field: string }[] } };
    return body.props.children.map((c) => c.field);
  }

  it("puts on_* in an ontology folder and op_* in an ops folder, none loose", () => {
    const labels = { ontology: "ontology", ops: "ops" };
    const result = _renderGroupedFields(fields, renderField, [], labels, prefixes);
    expect(result).toHaveLength(4);
    expect(result.every((r: Folder) => r.props.open === false)).toBe(true);
    const byLabel = Object.fromEntries(
      result.map((r: { props: { children: unknown[] } }) => [
        (r.props.children[0] as { props: { children: unknown[] } }).props.children[1],
        folderFields(r),
      ]),
    );
    expect(byLabel.ontology).toEqual(["on_termEdges", "on_terms"]);
    expect(byLabel.ops).toEqual(["op_queries", "op_traces"]);
    expect(byLabel.m).toEqual(["m__resolve"]);
    expect(byLabel.ps).toEqual(["ps__pets"]);
  });

  it("opens the folder of a selected system field", () => {
    const result = _renderGroupedFields(
      fields, renderField, [{ name: { value: "op_traces" } }], {}, prefixes,
    );
    const open = result.filter((r: Folder) => r.props.open);
    expect(open).toHaveLength(1);
    expect(folderFields(open[0])).toEqual(["op_queries", "op_traces"]);
  });

  it("leaves a field that is not of a system schema type loose", () => {
    const result = _renderGroupedFields(
      { on_other: typed("Other"), health: typed("Boolean") }, renderField, [], {}, prefixes,
    );
    expect(result).toEqual([{ field: "health" }, { field: "on_other" }]);
  });
});

