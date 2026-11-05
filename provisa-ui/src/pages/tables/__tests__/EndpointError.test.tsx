// Copyright (c) 2026 Kenneth Stott
// Canary: 691cb69a-813c-4144-9e80-ddb5fcccb06d
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// A table of an OpenAPI source whose document could not be read is listed all the same, and says
// why it names no endpoint: a warning mark in its list row, and the reason beside the endpoint in
// its read and edit views.

import { describe, it, expect, vi } from "vitest";
import { render, renderInForm, screen } from "../../../test-utils/render";
import type { RegisteredTable, Source } from "../../../types/admin";
import { EndpointErrorMark } from "../EndpointErrorMark";
import { IdentityRead } from "../TableSectionsRead";
import { TableIdentitySection } from "../TableIdentitySection";
import { draftFromRegistration } from "../draftTable";

const WHY =
  "the API document of source 'petstore-api' could not be read: [Errno -3] Temporary failure in name resolution";

const TABLE = {
  id: 7,
  sourceId: "petstore-api",
  schemaName: "default",
  tableName: "get_inventory",
  domainId: "ops",
  alias: null,
  gqlNamingConvention: null,
  description: null,
  draft: false,
  region: null,
  apiEndpoint: null,
  apiEndpointError: WHY,
} as unknown as RegisteredTable;

describe("a table whose source's API document could not be read", () => {
  it("is marked in its list row, the reason on hover", () => {
    render(<EndpointErrorMark error={WHY} testId="mark" />);
    expect(
      screen.getByRole("img", { name: "Its source's API document could not be read" }),
    ).toHaveAttribute("data-testid", "mark");
  });

  it("is not marked when its document was read", () => {
    render(<EndpointErrorMark error={null} testId="mark" />);
    expect(screen.queryByTestId("mark")).not.toBeInTheDocument();
  });

  it("reads the reason beside the endpoint", () => {
    render(
      <IdentityRead table={TABLE} source={{ id: "petstore-api", type: "openapi" } as Source} />,
    );
    expect(screen.getByTestId("table-api-endpoint-error")).toHaveTextContent(`⚠ ${WHY}`);
  });

  it("shows the reason on the edit form, where the endpoint is", () => {
    const table = {
      ...draftFromRegistration({
        input: { sourceId: "petstore-api", domainId: "ops", schemaName: "default", tableName: "x" },
        pagination: null,
        pagingKind: null,
        pagingCeilingRows: null,
        rowRules: [],
      }),
      apiEndpointError: WHY,
    };
    renderInForm(
      <TableIdentitySection editingTable={table} setEditingTable={vi.fn()} isView={false} />,
    );
    const field = screen.getByTestId("table-api-endpoint-error") as HTMLInputElement;
    expect(field.value).toBe("Its source's API document could not be read");
    expect(screen.getByText(WHY)).toBeInTheDocument();
  });
});
