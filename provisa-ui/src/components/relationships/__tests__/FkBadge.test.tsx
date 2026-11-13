// Copyright (c) 2026 Kenneth Stott
// Canary: d53a69e6-0264-443a-b27d-34baa857e887
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// A relationship made from a foreign key the source declares shows a "From FK" badge by its name,
// with the tip "Created from a foreign key in the source", and only then. The Yes/No row and the
// word "Auto-tracked" are gone from the UI.

import { describe, it, expect, vi } from "vitest";
import { fireEvent, render, screen } from "../../../test-utils/render";
import type { Relationship } from "../../../types/admin";
import { RelationshipDetail } from "../RelationshipDetail";
import { RelationshipRow } from "../RelationshipRow";
import { ListTable } from "../../list/ListTable";

const rel: Relationship = {
  id: 7,
  sourceTableId: 1,
  targetTableId: 2,
  sourceTableName: "pets",
  sourceDomainId: "zoo",
  targetTableName: "owners",
  sourceColumn: "owner_id",
  targetColumn: "id",
  cardinality: "many-to-one",
  materialize: false,
  refreshInterval: 300,
  targetFunctionName: null,
  functionArg: null,
  alias: null,
  graphqlAlias: "owner",
  physicalName: "pets_owner",
  computedCypherAlias: "HAS_OWNER",
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
  cypherType: null,
};

function panel(r: Relationship) {
  render(
    <RelationshipDetail
      rel={r}
      editingRel={null}
      setEditingRel={vi.fn()}
      canManage
      onStartEdit={vi.fn()}
      onDuplicate={vi.fn()}
      onReverse={vi.fn()}
      onDelete={vi.fn()}
      onEditSave={vi.fn()}
      saving={null}
      problem=""
      tables={[]}
      functions={[]}
      tableDomainById={{}}
      domainsEnabled={false}
    />,
  );
}

function row(r: Relationship) {
  render(
    <ListTable>
      <RelationshipRow
        rel={r}
        selected={false}
        onToggle={vi.fn()}
        tableDomainById={{}}
        domainsEnabled={false}
      />
    </ListTable>,
  );
}

describe("the From FK badge", () => {
  it("shows in the panel of a relationship created from a foreign key", () => {
    panel({ ...rel, autoSuggested: true });
    expect(screen.getByText("From FK")).toBeInTheDocument();
    expect(screen.queryByText(/Auto-tracked/)).toBeNull();
  });

  it("shows in the list row of one, and carries the tip", async () => {
    row({ ...rel, autoSuggested: true });
    const badge = screen.getByText("From FK");
    expect(badge.closest("[title]")).toBeNull();
    fireEvent.mouseEnter(badge);
    expect(await screen.findByRole("tooltip")).toHaveTextContent(
      "Created from a foreign key in the source",
    );
    expect(screen.queryByText(/Auto-tracked/)).toBeNull();
  });

  it("carries the tip in the panel too", async () => {
    panel({ ...rel, autoSuggested: true });
    fireEvent.mouseEnter(screen.getByText("From FK"));
    expect(await screen.findByRole("tooltip")).toHaveTextContent(
      "Created from a foreign key in the source",
    );
  });

  it("shows nowhere for a relationship not created from one", () => {
    panel(rel);
    expect(screen.queryByText("From FK")).toBeNull();
    expect(screen.queryByText(/Auto-tracked/)).toBeNull();
  });

  it("is absent from the list row of a relationship not created from one", () => {
    row(rel);
    expect(screen.queryByText("From FK")).toBeNull();
    expect(screen.queryByText("FK")).toBeNull();
  });
});
