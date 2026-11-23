// Copyright (c) 2026 Kenneth Stott
// Canary: 8f3d2a61-4c7e-4b19-9e05-d2a6b1c8f074
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2249: a code resolver is picked in the add form as one thing -- the resolver, the columns of
// its matches holding each table's key -- and a relationship
// that is part of one says so, offering to change or retire the set rather than itself.

import { describe, it, expect, vi } from "vitest";
import { fireEvent, render, screen } from "../../../test-utils/render";
import type { TrackedFunction } from "../../../api/actions";
import type { IncrementalOption, ListedTable, Relationship } from "../../../types/admin";
import { CodeResolverIncrementalQuery } from "../../../hooks/admin.graphql";
import { AddRelationshipForm } from "../AddRelationshipForm";
import { RelationshipDetail } from "../RelationshipDetail";
import { EMPTY_FORM, isCodeResolver, type RelForm } from "../relationship-types";

function command(name: string, more: Partial<TrackedFunction>): TrackedFunction {
  return {
    name,
    sourceId: "ledger",
    schemaName: "code",
    functionName: name,
    returns: "",
    arguments: [
      { name: "orders", type: "String", argKind: "table_ref" },
      { name: "clips", type: "String", argKind: "table_ref" },
    ],
    visibleTo: [],
    domainId: "fx",
    description: null,
    kind: "query",
    implKind: "code_command",
    outputColumns: [
      { name: "order_id", type: "bigint" },
      { name: "clip_id", type: "bigint" },
    ],
    ...more,
  } as TrackedFunction;
}

const resolver = command("match_region", {});
const writes = command("relabel", { kind: "mutation" });
const oneTable = command("read_lean", {
  arguments: [{ name: "orders", type: "String", argKind: "table_ref" }],
});

describe("isCodeResolver", () => {
  it("is a code query over exactly two tables returning columns", () => {
    expect(isCodeResolver(resolver)).toBe(true);
    expect(isCodeResolver(writes)).toBe(false);
    expect(isCodeResolver(oneTable)).toBe(false);
    expect(isCodeResolver(command("x", { implKind: "source_procedure" }))).toBe(false);
    expect(isCodeResolver(command("x", { outputColumns: [] }))).toBe(false);
  });
});

describe("the add form's code resolver target", () => {
  it("asks for the resolver and the columns of its matches holding each key", () => {
    render(
      <AddRelationshipForm
        form={{ ...EMPTY_FORM, targetType: "resolver", resolver: "match_region" }}
        setForm={vi.fn()}
        tables={[] as ListedTable[]}
        functions={[resolver, writes, oneTable]}
        saving={null}
        problem=""
        onSave={vi.fn()}
        onCancel={vi.fn()}
      />,
    );
    expect(screen.getByTestId("rel-form-resolver")).toBeInTheDocument();
    expect(screen.getByTestId("rel-form-source-match")).toBeInTheDocument();
    expect(screen.getByTestId("rel-form-target-match")).toBeInTheDocument();
    // A table target's own fields are not asked.
    expect(screen.queryByTestId("rel-form-cardinality")).toBeNull();
    expect(screen.queryByTestId("rel-form-function")).toBeNull();
  });

  it("keeps a set's id and kind while the set is changed", () => {
    render(
      <AddRelationshipForm
        form={{
          ...EMPTY_FORM,
          id: "order_clips",
          originalId: "order_clips",
          targetType: "resolver",
        }}
        setForm={vi.fn()}
        tables={[] as ListedTable[]}
        functions={[resolver]}
        saving={null}
        problem=""
        onSave={vi.fn()}
        onCancel={vi.fn()}
        changingResolver
      />,
    );
    expect(screen.getByTestId("rel-form-id")).toBeDisabled();
  });
});

// REQ-2249: the set's incremental setting, which the server says may or may not be turned on.
function option(resolverName: string, id: string, answer: IncrementalOption) {
  return {
    request: { query: CodeResolverIncrementalQuery, variables: { resolver: resolverName, id } },
    result: { data: { codeResolverIncremental: answer } },
  };
}

const AVAILABLE: IncrementalOption = { available: true, code: null, reason: null };
const NO_CHANGE: IncrementalOption = {
  available: false,
  code: "schema.incremental_no_change",
  reason: "server english text",
};

function resolverForm(more: Partial<RelForm>, answer: IncrementalOption, setForm = vi.fn()) {
  render(
    <AddRelationshipForm
      form={{
        ...EMPTY_FORM,
        id: "order_clips",
        targetType: "resolver",
        resolver: "match_region",
        ...more,
      }}
      setForm={setForm}
      tables={[] as ListedTable[]}
      functions={[resolver]}
      saving={null}
      problem=""
      onSave={vi.fn()}
      onCancel={vi.fn()}
    />,
    { mocks: [option("match_region", "order_clips", answer)] },
  );
  return setForm;
}

describe("a code resolver's incremental setting", () => {
  it("can be turned on when the resolver's source has a @change function", async () => {
    const setForm = resolverForm({}, AVAILABLE);
    const tick = await screen.findByTestId("rel-form-incremental");
    expect(tick).toBeEnabled();
    expect(tick).not.toBeChecked();
    // Off, no interval between whole rebuilds is asked.
    expect(screen.queryByTestId("rel-form-rebuild-every")).toBeNull();
    fireEvent.click(tick);
    expect(setForm).toHaveBeenCalledWith(expect.objectContaining({ incremental: true }));
  });

  it("is disabled, with the catalog's reason at its (?), when the source has none", async () => {
    resolverForm({}, NO_CHANGE);
    const tick = await screen.findByTestId("rel-form-incremental");
    expect(tick).toBeDisabled();
    // Its (?) says why, in place of what the setting does.
    expect(tick).toHaveAccessibleDescription(
      expect.stringContaining("The code source has no @change function for order_clips."),
    );
    // The catalog words it; the server's English text is not shown.
    expect(screen.queryByText("server english text", { exact: false })).toBeNull();
  });

  it("requires the interval between whole rebuilds while it is on", async () => {
    const setForm = resolverForm({ incremental: true }, AVAILABLE);
    const every = await screen.findByTestId("rel-form-rebuild-every");
    expect(every).toBeRequired();
    fireEvent.change(every, { target: { value: "3600" } });
    expect(setForm).toHaveBeenCalledWith(expect.objectContaining({ rebuildEvery: "3600" }));
  });

  it("stays reachable when it is on and the source lost its @change function", async () => {
    resolverForm({ incremental: true, rebuildEvery: "3600" }, NO_CHANGE);
    const tick = await screen.findByTestId("rel-form-incremental");
    expect(tick).toBeChecked();
    expect(tick).toBeEnabled();
    expect(tick).toHaveAccessibleDescription(
      expect.stringContaining("has no @change function for order_clips"),
    );
  });

  it("is not asked before the set's id and resolver are given", () => {
    render(
      <AddRelationshipForm
        form={{ ...EMPTY_FORM, targetType: "resolver" }}
        setForm={vi.fn()}
        tables={[] as ListedTable[]}
        functions={[resolver]}
        saving={null}
        problem=""
        onSave={vi.fn()}
        onCancel={vi.fn()}
      />,
    );
    expect(screen.queryByTestId("rel-form-incremental")).toBeNull();
  });
});

const member = {
  id: "order_clips",
  sourceTableId: 1,
  targetTableId: 2,
  sourceTableName: "orders",
  sourceDomainId: "fx",
  targetTableName: "clips",
  sourceColumn: "id",
  targetColumn: "id",
  cardinality: "one-to-many",
  materialize: false,
  refreshInterval: 300,
  targetFunctionName: null,
  functionArg: null,
  alias: "ORDER_CLIPS",
  graphqlAlias: "order_clips",
  physicalName: "order_clips",
  computedCypherAlias: null,
  cypherType: null,
  autoSuggested: false,
  disableCypher: false,
  sameEntity: false,
  parameters: [],
  viaTableId: 3,
  viaTableName: "order_clips",
  shape: "junction",
  properties: ["score"],
  viaSourceColumn: "order_id",
  viaTargetColumn: "clip_id",
  viaTypeColumn: null,
  viaTypeValue: null,
  viaLabelSource: "fixed",
  codeResolver: "order_clips",
} as unknown as Relationship;

describe("a relationship that is part of a code resolver", () => {
  it("names its set and offers to change or retire the set, not itself", () => {
    const change = vi.fn();
    render(
      <RelationshipDetail
        rel={member}
        editingRel={null}
        setEditingRel={vi.fn()}
        canManage
        onStartEdit={vi.fn()}
        onDuplicate={vi.fn()}
        onDelete={vi.fn()}
        onEditSave={vi.fn()}
        saving={null}
        problem=""
        tables={[]}
        refetchTables={() => Promise.resolve()}
        functions={[]}
        tableDomainById={{}}
        domainsEnabled={false}
        actingRole={null}
        onChangeResolver={change}
        onRetireResolver={vi.fn()}
      />,
    );
    expect(screen.getByTestId("relationship-detail-resolver-note")).toHaveTextContent(
      "order_clips",
    );
    expect(screen.queryByTestId("relationship-detail-delete")).toBeNull();
    expect(screen.queryByTestId("relationship-detail-duplicate")).toBeNull();
    screen.getByTestId("relationship-detail-change-resolver").click();
    expect(change).toHaveBeenCalledWith("order_clips");
  });
});
