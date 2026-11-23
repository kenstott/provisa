// Copyright (c) 2026 Kenneth Stott
// Canary: 1e6c9b47-3a2d-4f58-8b70-c5d9e2a4f613
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1586 (amended 2026-11-22): every relationship shows its model type -- fk (a direct foreign
// key), jct (through a junction table) or dim (through a shared dimension) -- in the
// Relationships list and the Cypher relationship detail, and a "prop" badge only where it has
// properties, its tip naming them. The old "props" badge every junction carried is gone.

import { describe, it, expect, vi } from "vitest";
import { fireEvent, render, screen, within } from "../../../test-utils/render";
import type { Relationship } from "../../../types/admin";
import { RelationshipRow } from "../RelationshipRow";
import { ListTable } from "../../list/ListTable";
import { Inspector } from "../../graph/Inspector";
import type { GEdge, GNode } from "../../graph/graph-model";

const direct: Relationship = {
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
  withdrawn: false,
  parameters: [],
  viaTableId: null,
  viaTableName: null,
  viaSourceColumn: null,
  viaTargetColumn: null,
  viaTypeColumn: null,
  viaTypeValue: null,
  viaLabelSource: null,
  shape: "direct",
  properties: [],
  cypherType: "HAS_OWNER",
};

const junction: Relationship = {
  ...direct,
  id: 8,
  cardinality: "one-to-many",
  viaTableId: 9,
  viaTableName: "pet_companions",
  viaSourceColumn: "pet_id",
  viaTargetColumn: "companion_id",
  viaLabelSource: "table",
  shape: "junction",
  properties: ["since", "bond"],
  cypherType: "PET_COMPANIONS",
};

const dimension: Relationship = {
  ...direct,
  id: 10,
  viaTableId: 11,
  viaTableName: "neighborhood",
  viaSourceColumn: "neighborhood",
  viaTargetColumn: "neighborhood",
  viaLabelSource: "table",
  shape: "shared_dimension",
  properties: [],
  cypherType: "NEIGHBORHOOD",
};

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

async function tipOf(text: string) {
  fireEvent.mouseEnter(screen.getByText(text));
  return screen.findByRole("tooltip");
}

describe("the Rel type in the Relationships list", () => {
  it("names a direct foreign key fk", async () => {
    row(direct);
    expect(await tipOf("fk")).toHaveTextContent("Direct foreign key");
  });

  it("names a junction jct, with its table in the tip", async () => {
    row(junction);
    expect(await tipOf("jct")).toHaveTextContent("Through junction pet_companions");
  });

  it("names a shared dimension dim, with its table in the tip", async () => {
    row(dimension);
    expect(await tipOf("dim")).toHaveTextContent("Through shared dimension neighborhood");
  });
});

describe("the prop badge in the Relationships list", () => {
  it("shows where the relationship has properties, its tip naming them", async () => {
    row(junction);
    expect(await tipOf("prop")).toHaveTextContent("Properties: since, bond");
  });

  it("is absent from a junction holding only its two keys", () => {
    row({ ...junction, properties: [] });
    expect(screen.queryByText("prop")).toBeNull();
  });

  it("is absent from a direct relationship", () => {
    row(direct);
    expect(screen.queryByText("prop")).toBeNull();
  });

  it("replaces the old props badge every junction carried", () => {
    row({ ...junction, properties: [] });
    expect(screen.queryByText("props")).toBeNull();
  });
});

const from: GNode = { id: 1, label: "Pet", tableLabel: "pets", properties: {} };
const to: GNode = { id: 2, label: "Pet", tableLabel: "pets", properties: {} };

function inspect(r: Relationship) {
  const edge: GEdge = {
    identity: "e1",
    start: 1,
    end: 2,
    type: r.cypherType ?? "",
    properties: {},
    startNode: from,
    endNode: to,
  };
  render(
    <Inspector
      selected={{ kind: "edge", data: edge }}
      colorOverrides={{}}
      onColorChange={() => {}}
      onClose={() => {}}
      width={320}
      onResizeStart={() => {}}
      relationships={[direct, junction, dimension]}
      labelToTableId={{}}
    />,
  );
  return screen.getByTestId("inspector-rel-type");
}

describe("the Cypher relationship detail", () => {
  it("shows the Rel type field and the prop badge of a junction with properties", async () => {
    const field = inspect(junction);
    expect(field).toHaveTextContent("Rel type");
    expect(within(field).getByText("jct")).toBeInTheDocument();
    expect(await tipOf("prop")).toHaveTextContent("Properties: since, bond");
  });

  it("shows dim and no prop badge for a dimension without properties", () => {
    const field = inspect(dimension);
    expect(within(field).getByText("dim")).toBeInTheDocument();
    expect(within(field).queryByText("prop")).toBeNull();
  });

  it("shows fk for a direct relationship", () => {
    expect(within(inspect(direct)).getByText("fk")).toBeInTheDocument();
  });
});
