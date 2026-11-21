// Copyright (c) 2026 Kenneth Stott
// Canary: 5c3e8a14-9d27-4f60-b1a8-6e2d7f90c4b3
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

/**
 * The Register table source picker offers only sources a person registers tables on: not the
 * virtual-views sentinel (views come from + View) and not a source Provisa runs for itself
 * (Source.isSystem), and every other source stays.
 */

import { describe, expect, it, vi } from "vitest";
import { render, screen } from "../../../test-utils/render";

vi.mock("../../../hooks/useAdminQueries", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../../hooks/useAdminQueries")>()),
  useAvailableSchemas: () => ({ schemas: [] as string[], loading: false }),
  useAvailableTables: () => ({ tables: [] as { name: string }[], loading: false }),
}));

const { RegisterTableForm } = await import("../RegisterTableForm");

const source = (id: string, isSystem: boolean) =>
  ({ id, type: "postgresql", allowedDomains: [], isSystem }) as never;

describe("RegisterTableForm source picker", () => {
  it("omits __derived__ and is_system sources and keeps user sources", () => {
    render(
      <RegisterTableForm
        sources={[
          source("pet-store-sqlite", false),
          source("__derived__", false),
          source("dq-checker", true),
          source("provisa-admin", true),
          source("orders-pg", false),
        ]}
        domainHints={[]}
        domainAccess={["*"]}
        checkedDomains={new Set<string>()}
        domainsEnabled
        tables={[]}
        roles={[]}
        suggestTableAlias={vi.fn().mockResolvedValue("x")}
        onContinue={vi.fn()}
        onCancel={() => {}}
        onProductsRegistered={vi.fn()}
        onRegisteredMany={vi.fn()}
        setError={vi.fn()}
      />,
    );
    const select = screen.getByTestId("register-table-source-select") as HTMLSelectElement;
    const offered = Array.from(select.options)
      .map((o) => o.value)
      .filter((v) => v !== "");
    expect(offered).toEqual(["pet-store-sqlite", "orders-pg"]);
  });
});
