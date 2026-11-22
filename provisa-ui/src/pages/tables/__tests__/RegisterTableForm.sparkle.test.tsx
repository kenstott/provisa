// Copyright (c) 2026 Kenneth Stott
// Canary: 0d5c9a37-e81b-4f26-b4a3-1f6e8c2d7b95
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// The Register table form's table description and each column's description carry the AI describe
// (the sparkle) before the table is registered: it is written from what the form holds -- the
// source, schema, table name and the columns picked.

import { beforeEach, describe, expect, it, vi } from "vitest";
import userEvent from "@testing-library/user-event";
import { fireEvent, render, screen, waitFor, within } from "../../../test-utils/render";

vi.mock("../../../hooks/useAdminQueries", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../../hooks/useAdminQueries")>()),
  useAvailableSchemas: () => ({ schemas: [] as string[], loading: false }),
  useAvailableTables: () => ({ tables: [] as { name: string }[], loading: false }),
}));
const preview = vi.fn();
vi.mock("../../../hooks/useQueryPreview", () => ({ useQueryPreview: () => ({ preview }) }));
const describeTable = vi.fn();
const describeColumn = vi.fn();
vi.mock("../../../hooks/useDescribeUnregistered", () => ({
  useDescribeUnregistered: () => ({ describeTable, describeColumn }),
}));

const { RegisterTableForm } = await import("../RegisterTableForm");

const CYPHER = "MATCH (a:Adopter) RETURN a.adopter_id AS adopter_id, a.name AS name";
const ADOPTER = {
  sourceId: "neo4j-demo",
  schemaName: "neo4j",
  tableName: "adopter",
  columns: ["adopter_id", "name"],
};

function renderForm() {
  const setError = vi.fn();
  render(
    <RegisterTableForm
      sources={[{ id: "neo4j-demo", type: "neo4j", allowedDomains: [] } as never]}
      domainHints={["shelter"]}
      domainAccess={["*"]}
      checkedDomains={new Set<string>()}
      domainsEnabled
      tables={[]}
      roles={[{ id: "org_admin" } as never]}
      suggestTableAlias={vi.fn().mockResolvedValue("")}
      onContinue={vi.fn()}
      onCancel={() => {}}
      onProductsRegistered={vi.fn()}
      onRegisteredMany={vi.fn()}
      setError={setError}
    />,
  );
  return { setError };
}

/** The neo4j table named and previewed: the form names the table and holds its columns. */
async function nameAndPreview() {
  const user = userEvent.setup();
  await user.selectOptions(screen.getByTestId("register-table-source-select"), "neo4j-demo");
  await user.selectOptions(screen.getByTestId("register-table-domain-select"), "shelter");
  fireEvent.change(await screen.findByTestId("register-table-neo4j-table-name"), {
    target: { value: "adopter" },
  });
  fireEvent.change(screen.getByTestId("register-table-neo4j-cypher"), {
    target: { value: CYPHER },
  });
  await user.click(screen.getByTestId("register-table-neo4j-preview"));
  await waitFor(() => expect(screen.getByTestId("register-table-col-selected-name")).toBeChecked());
  return user;
}

const sparkleIn = (testId: string) =>
  within(screen.getByTestId(testId)).getByTestId("description-field-generate");

beforeEach(() => {
  vi.clearAllMocks();
  preview.mockResolvedValue({
    rows: [{ adopter_id: 1, name: "Sara Kim" }],
    columns: [
      { name: "adopter_id", dataType: "integer" },
      { name: "name", dataType: "text" },
    ],
    error: null,
  });
});

describe("RegisterTableForm describes a table before it is registered", () => {
  it("fills the table description from the AI describe of what the form holds", async () => {
    describeTable.mockResolvedValue("People who adopted a pet.");
    renderForm();
    const user = await nameAndPreview();
    await user.click(sparkleIn("register-table-description-field"));
    await waitFor(() =>
      expect(
        within(screen.getByTestId("register-table-description-field")).getByRole("textbox"),
      ).toHaveValue("People who adopted a pet."),
    );
    expect(describeTable).toHaveBeenCalledWith(ADOPTER);
  });

  it("describes only the columns picked", async () => {
    describeTable.mockResolvedValue("x");
    renderForm();
    const user = await nameAndPreview();
    await user.click(screen.getByTestId("register-table-col-selected-adopter_id"));
    await user.click(sparkleIn("register-table-description-field"));
    await waitFor(() =>
      expect(describeTable).toHaveBeenCalledWith({ ...ADOPTER, columns: ["name"] }),
    );
  });

  it("fills a column's description from the AI describe of that column", async () => {
    describeColumn.mockResolvedValue("The adopter's full name.");
    renderForm();
    const user = await nameAndPreview();
    await user.click(sparkleIn("register-table-col-description-name"));
    await waitFor(() =>
      expect(
        within(screen.getByTestId("register-table-col-description-name")).getByRole("textbox"),
      ).toHaveValue("The adopter's full name."),
    );
    expect(describeColumn).toHaveBeenCalledWith(ADOPTER, "name");
    expect(
      within(screen.getByTestId("register-table-col-description-adopter_id")).getByRole("textbox"),
    ).toHaveValue("");
  });

  it("has no sparkle until the form names the table", async () => {
    renderForm();
    const user = userEvent.setup();
    await user.selectOptions(screen.getByTestId("register-table-source-select"), "neo4j-demo");
    await user.selectOptions(screen.getByTestId("register-table-domain-select"), "shelter");
    await screen.findByTestId("register-table-neo4j-table-name");
    expect(
      within(screen.getByTestId("register-table-description-field")).queryByTestId(
        "description-field-generate",
      ),
    ).toBeNull();
  });

  it("says why a describe was refused", async () => {
    describeTable.mockRejectedValue(new Error("No access to source 'neo4j-demo'"));
    const { setError } = renderForm();
    const user = await nameAndPreview();
    await user.click(sparkleIn("register-table-description-field"));
    await waitFor(() => expect(setError).toHaveBeenCalledWith("No access to source 'neo4j-demo'"));
  });
});
