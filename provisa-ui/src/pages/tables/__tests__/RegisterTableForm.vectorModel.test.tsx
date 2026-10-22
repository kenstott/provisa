// Copyright (c) 2026 Kenneth Stott
// Canary: 64bc1451-27a2-41c9-b79f-18b6894b0584
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

/**
 * REQ-2000: registering a Qdrant collection, a vector column names the registered model it is an
 * embedding of, chosen from the models of the vector's size; naming it registers the vector.
 */

import { describe, expect, it, vi } from "vitest";
import userEvent from "@testing-library/user-event";
import { fireEvent, render, screen, waitFor, within } from "../../../test-utils/render";

const useVectorModels = vi.fn();

vi.mock("../../../hooks/useAdminQueries", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../../hooks/useAdminQueries")>()),
  useAvailableSchemas: () => ({ schemas: ["qdrant"], loading: false }),
  useAvailableTables: () => ({ tables: [{ name: "docs" }], loading: false }),
}));
vi.mock("../useVectorModels", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../useVectorModels")>()),
  useVectorModels: (...args: unknown[]) => useVectorModels(...args),
}));

const { RegisterTableForm } = await import("../RegisterTableForm");

function renderForm() {
  const onContinue = vi.fn();
  render(
    <RegisterTableForm
      sources={[{ id: "vec", type: "qdrant", database: "", allowedDomains: [] } as never]}
      domainHints={["kb"]}
      domainAccess={["*"]}
      checkedDomains={new Set<string>()}
      domainsEnabled
      tables={[]}
      roles={[{ id: "analyst" } as never]}
      getAvailableColumnsMetadata={vi.fn().mockResolvedValue([
        { name: "id", dataType: "text", path: "@id", isPrimaryKey: true },
        { name: "title", dataType: "text", path: "title" },
        { name: "embedding", dataType: "vector(384)", path: "@vector" },
        { name: "image", dataType: "vector(512)", path: "@vector.image" },
      ])}
      suggestTableAlias={vi.fn().mockResolvedValue("")}
      onContinue={onContinue}
      onCancel={() => {}}
      setError={vi.fn()}
    />,
  );
  return onContinue;
}

describe("RegisterTableForm on a Qdrant collection: a vector's model (REQ-2000)", () => {
  it("offers each vector the models of its size and registers the one named", async () => {
    useVectorModels.mockReturnValue({ "": ["minilm"], image: [] });
    const onContinue = renderForm();
    const user = userEvent.setup();
    await user.selectOptions(screen.getByTestId("register-table-source-select"), "vec");
    await user.selectOptions(await screen.findByTestId("register-table-table-select"), "docs");
    const vectors = await screen.findByTestId("register-table-vector-models");
    expect(useVectorModels).toHaveBeenLastCalledWith("vec", "docs", true);
    // Only vector columns are asked; one with no model of its size says so.
    expect(within(vectors).queryByText("title")).toBeNull();
    expect(within(vectors).getByTestId("vector-model-none-image")).toBeInTheDocument();
    const input = within(vectors).getByTestId("vector-model-embedding");
    fireEvent.click(input);
    // Mantine keeps the drop-down out of the accessibility tree in jsdom: reach it through the
    // input's aria-controls listbox.
    const listbox = document.getElementById(input.getAttribute("aria-controls") as string);
    fireEvent.click(within(listbox as HTMLElement).getByRole("option", { hidden: true }));
    fireEvent.click(screen.getByTestId("register-table-submit"));
    await waitFor(() => expect(onContinue).toHaveBeenCalledTimes(1));
    const columns = onContinue.mock.calls[0][0].input.columns as { name: string }[];
    expect(columns.find((c) => c.name === "embedding")).toMatchObject({
      embedding: true,
      embeddingModel: "minilm",
      path: "@vector",
    });
    // A vector left without a model stays unregistered, as offered.
    expect(columns.find((c) => c.name === "image")).toBeUndefined();
  });
});
