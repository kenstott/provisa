// Copyright (c) 2026 Kenneth Stott
// Canary: 6db7cc68-3ef2-4442-be87-3a252ea1b60c
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1921: a new table starts in its source's region, else the region the operator is
// connected to; the operator may change it or choose No region. With no regions declared there
// is no field, and the table is registered with none.

import { beforeEach, describe, expect, it, vi } from "vitest";
import userEvent from "@testing-library/user-event";
import { render, screen, waitFor } from "../../../test-utils/render";

const choices = vi.hoisted(() => ({
  value: { regions: ["eu", "us"], connected: "us" as string | null },
}));

vi.mock("../../../hooks/useAdminQueries", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../../hooks/useAdminQueries")>()),
  useAvailableSchemas: () => ({ schemas: ["public"], loading: false }),
  useAvailableTables: () => ({
    tables: [{ name: "orders", comment: null, pagingKind: null, pagination: null }],
    loading: false,
  }),
}));
vi.mock("../../../hooks/useRegionQueries", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../../hooks/useRegionQueries")>()),
  useRegionChoices: () => choices.value,
}));

const { RegisterTableForm } = await import("../RegisterTableForm");

const SOURCES = [
  { id: "crm", type: "postgresql", allowedDomains: [], region: "eu" },
  { id: "erp", type: "postgresql", allowedDomains: [], region: null },
];

function renderForm() {
  const onContinue = vi.fn();
  render(
    <RegisterTableForm
      sources={SOURCES as never}
      domainHints={[]}
      domainAccess={["*"]}
      checkedDomains={new Set<string>()}
      domainsEnabled={false}
      tables={[]}
      roles={[{ id: "admin" } as never]}
      getAvailableColumnsMetadata={vi.fn().mockResolvedValue([{ name: "id", dataType: "integer" }])}
      suggestTableAlias={vi.fn().mockResolvedValue("")}
      onContinue={onContinue}
      onCancel={() => {}}
      setError={vi.fn()}
    />,
  );
  return { onContinue };
}

async function pick(sourceId: string) {
  const user = userEvent.setup();
  await user.selectOptions(screen.getByTestId("register-table-source-select"), sourceId);
  await user.selectOptions(await screen.findByTestId("register-table-table-select"), "orders");
  // Discovery is done when the short step counts the columns it registers.
  await screen.findByTestId("register-table-columns-summary");
  return user;
}

describe("RegisterTableForm region (REQ-1921)", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    choices.value = { regions: ["eu", "us"], connected: "us" };
  });

  // The short step does not ask the region: the table starts where its source is, and the region
  // is changed on its Identity tab.
  it("starts a table in its source's region", async () => {
    const { onContinue } = renderForm();
    const user = await pick("crm");
    await user.click(screen.getByTestId("register-table-submit"));
    await waitFor(() => expect(onContinue).toHaveBeenCalled());
    expect(onContinue.mock.calls[0][0].input.region).toBe("eu");
  });

  it("starts a table of a source naming none in the connected region", async () => {
    const { onContinue } = renderForm();
    const user = await pick("erp");
    await user.click(screen.getByTestId("register-table-submit"));
    await waitFor(() => expect(onContinue).toHaveBeenCalled());
    expect(onContinue.mock.calls[0][0].input.region).toBe("us");
  });

  it("does not ask the region in the short step", async () => {
    renderForm();
    await pick("crm");
    expect(screen.queryByTestId("register-table-region-select")).toBeNull();
  });

  it("shows no region and sends none when the platform declares no regions", async () => {
    choices.value = { regions: [], connected: null };
    const { onContinue } = renderForm();
    const user = await pick("crm");
    expect(screen.queryByTestId("register-table-region-select")).toBeNull();
    await user.click(screen.getByTestId("register-table-submit"));
    await waitFor(() => expect(onContinue).toHaveBeenCalled());
    expect("region" in onContinue.mock.calls[0][0].input).toBe(false);
  });
});
