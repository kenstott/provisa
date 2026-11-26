// Copyright (c) 2026 Kenneth Stott
// Canary: cfa3f9ad-ce76-4347-89ba-b8565ac9611b
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1968: Generate, Run and Extract joins are never disabled by an empty question or empty SQL.
// Pressing one marks what it needs and does nothing else.

import { describe, it, expect, vi } from "vitest";
import { fireEvent, render, screen } from "../../test-utils/render";
import i18n from "../../i18n";

const nlToSql = vi.fn();
const runSql = vi.fn();
vi.mock("../../api/admin", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../api/admin")>()),
  nlToSql: (...a: unknown[]) => nlToSql(...a),
  runSql: (...a: unknown[]) => runSql(...a),
}));
vi.mock("../../context/AuthContext", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../context/AuthContext")>()),
  useAuth: () => ({ actingRoles: "analyst" }),
}));
vi.mock("../../hooks/useAdminQueries", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../hooks/useAdminQueries")>()),
  useRoles: () => ({ roles: [{ id: "analyst" }] }),
  useDomains: () => ({ domains: [] }),
}));

import { SqlModelingModal } from "../SqlModelingModal";

const modal = () =>
  render(
    <SqlModelingModal
      tables={[]}
      existingRels={[]}
      onClose={() => {}}
      onPromote={async () => {}}
    />,
  );

describe("SqlModelingModal required on press", () => {
  it("marks an empty question when Generate is pressed, and asks nothing", () => {
    modal();
    const generate = screen.getByTestId("sql-modeling-generate-sql");
    expect(generate).toBeEnabled();
    fireEvent.click(generate);
    expect(nlToSql).not.toHaveBeenCalled();
    expect(screen.getByTestId("sql-modeling-nl-input")).toHaveAttribute("aria-invalid", "true");
  });

  it("says SQL is needed when Run or Extract joins is pressed on an empty editor", () => {
    modal();
    expect(screen.queryByTestId("sql-modeling-sql-required")).toBeNull();
    const extract = screen.getByTestId("sql-modeling-extract-joins");
    expect(extract).toBeEnabled();
    fireEvent.click(extract);
    expect(screen.getByTestId("sql-modeling-sql-required")).toHaveTextContent(
      i18n.t("common.required"),
    );
    fireEvent.click(screen.getByTestId("sql-modeling-run"));
    expect(runSql).not.toHaveBeenCalled();
  });
});
