// Copyright (c) 2026 Kenneth Stott
// Canary: ed348491-54da-4c25-9f29-5d2dc3ca7eb7
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1907 (#208): a table that will be a replica, registered under a ttl signal, needs a Cache
// TTL -- its own or its source's -- or the server refuses it. The form asks for the table's own
// exactly when it is needed: the server says the kind's tables land here, and the source states
// none. The registration carries it.

import { beforeEach, describe, expect, it, vi } from "vitest";
import userEvent from "@testing-library/user-event";
import { render, screen, waitFor } from "../../../test-utils/render";

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
  useRegionChoices: () => ({ regions: [], connected: null }),
}));

const { RegisterTableForm } = await import("../RegisterTableForm");

// mongodb: the server says its tables land; postgresql: read in place.
const LANDS: Record<string, boolean> = { mongodb: true, postgresql: false };
const SOURCES = [
  { id: "docs", type: "mongodb", allowedDomains: [], changeSignal: "ttl", cacheTtl: null },
  { id: "docs-ttl", type: "mongodb", allowedDomains: [], changeSignal: "ttl", cacheTtl: 300 },
  { id: "docs-push", type: "mongodb", allowedDomains: [], changeSignal: "native", cacheTtl: null },
  { id: "pg", type: "postgresql", allowedDomains: [], changeSignal: "ttl", cacheTtl: null },
];

function renderForm() {
  const onContinue = vi.fn();
  const setError = vi.fn();
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
      setError={setError}
    />,
  );
  return { onContinue, setError };
}

async function pick(sourceId: string) {
  const user = userEvent.setup();
  await user.selectOptions(screen.getByTestId("register-table-source-select"), sourceId);
  await user.selectOptions(await screen.findByTestId("register-table-table-select"), "orders");
  // Discovery is done when the short step counts the columns it registers.
  await screen.findByTestId("register-table-columns-summary");
  return user;
}

describe("RegisterTableForm Cache TTL", () => {
  beforeEach(() => {
    vi.restoreAllMocks();
    vi.spyOn(globalThis, "fetch").mockImplementation(async (url) => {
      const kind = decodeURIComponent(String(url).split("/admin/source-kinds/")[1]);
      return new Response(
        JSON.stringify({
          cache_ttl_required: false,
          sentinel_path: true,
          tables_land: LANDS[kind] ?? false,
        }),
      );
    });
  });

  it("asks for it, refuses without it, and registers with it", async () => {
    const { onContinue, setError } = renderForm();
    const user = await pick("docs");
    const field = await screen.findByTestId("register-table-cache-ttl");
    expect(field).toBeRequired();
    await user.click(screen.getByTestId("register-table-submit"));
    expect(setError).toHaveBeenLastCalledWith(
      "Set a Cache TTL: this table will be a replica, and its source states none.",
    );
    expect(onContinue).not.toHaveBeenCalled();
    await user.type(field, "300");
    await user.click(screen.getByTestId("register-table-submit"));
    await waitFor(() => expect(onContinue).toHaveBeenCalled());
    expect(onContinue.mock.calls[0][0].input.cacheTtl).toBe(300);
  });

  it.each([
    ["its source states a Cache TTL", "docs-ttl"],
    ["its source's signal has its own clock", "docs-push"],
    ["the engine reads the source in place", "pg"],
  ])("does not ask when %s, and sends none", async (_why, sourceId) => {
    const { onContinue } = renderForm();
    const user = await pick(sourceId);
    await waitFor(() => expect(globalThis.fetch).toHaveBeenCalled());
    expect(screen.queryByTestId("register-table-cache-ttl")).toBeNull();
    await user.click(screen.getByTestId("register-table-submit"));
    await waitFor(() => expect(onContinue).toHaveBeenCalled());
    expect(onContinue.mock.calls[0][0].input.cacheTtl).toBeUndefined();
  });
});
