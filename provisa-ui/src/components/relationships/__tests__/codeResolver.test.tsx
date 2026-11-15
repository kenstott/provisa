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
import { render, screen } from "../../../test-utils/render";
import type { TrackedFunction } from "../../../api/actions";
import type { ListedTable, Relationship } from "../../../types/admin";
import { AddRelationshipForm } from "../AddRelationshipForm";
import { RelationshipDetail } from "../RelationshipDetail";
import { EMPTY_FORM, isCodeResolver } from "../relationship-types";

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

const member = {
  id: "order_clips_matches",
  sourceTableId: 1,
  targetTableId: 3,
  sourceTableName: "orders",
  sourceDomainId: "fx",
  targetTableName: "order_clips",
  sourceColumn: "id",
  targetColumn: "order_id",
  cardinality: "one-to-many",
  materialize: false,
  refreshInterval: 300,
  targetFunctionName: null,
  functionArg: null,
  alias: null,
  graphqlAlias: "order_clips",
  physicalName: "order_clips_matches",
  computedCypherAlias: null,
  cypherType: null,
  autoSuggested: false,
  disableCypher: false,
  sameEntity: false,
  parameters: [],
  viaTableId: null,
  viaTableName: null,
  viaSourceColumn: null,
  viaTargetColumn: null,
  viaTypeColumn: null,
  viaTypeValue: null,
  viaLabelSource: null,
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
        functions={[]}
        tableDomainById={{}}
        domainsEnabled={false}
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
